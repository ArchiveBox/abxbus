package abxbus

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type OtelTracingMiddlewareOptions struct {
	Tracer trace.Tracer
}

type OtelTracingMiddleware struct {
	tracer         trace.Tracer
	mu             sync.Mutex
	eventContexts  map[string]context.Context
	eventSpans     map[string]trace.Span
	handlerSpans   map[string]trace.Span
	handlerContext map[string]context.Context
}

func NewOtelTracingMiddleware(options *OtelTracingMiddlewareOptions) *OtelTracingMiddleware {
	tracer := trace.Tracer(nil)
	if options != nil {
		tracer = options.Tracer
	}
	if tracer == nil {
		tracer = otel.Tracer("abxbus")
	}
	return &OtelTracingMiddleware{
		tracer:         tracer,
		eventContexts:  map[string]context.Context{},
		eventSpans:     map[string]trace.Span{},
		handlerSpans:   map[string]trace.Span{},
		handlerContext: map[string]context.Context{},
	}
}

func (m *OtelTracingMiddleware) OnEventChange(eventbus *EventBus, event *BaseEvent, status string) {
	switch status {
	case "started":
		m.startEventSpan(eventbus, event)
	case "completed":
		m.completeEventSpan(eventbus, event)
	}
}

func (m *OtelTracingMiddleware) OnEventResultChange(eventbus *EventBus, event *BaseEvent, eventResult *EventResult, status string) {
	switch status {
	case "started":
		m.startHandlerSpan(eventbus, event, eventResult)
	case "completed":
		m.completeHandlerSpan(eventbus, event, eventResult)
	}
}

func (m *OtelTracingMiddleware) startEventSpan(eventbus *EventBus, event *BaseEvent) trace.Span {
	m.mu.Lock()
	defer m.mu.Unlock()
	if span := m.eventSpans[event.EventID]; span != nil {
		return span
	}
	parent := context.Background()
	if event.EventParentID != nil {
		if parentCtx := m.eventContexts[*event.EventParentID]; parentCtx != nil {
			parent = parentCtx
		}
	}
	ctx, span := m.tracer.Start(parent, fmt.Sprintf("%s.%s", eventbus.Name, event.EventType), trace.WithAttributes(eventAttributes(eventbus, event)...))
	m.eventContexts[event.EventID] = ctx
	m.eventSpans[event.EventID] = span
	return span
}

func (m *OtelTracingMiddleware) completeEventSpan(eventbus *EventBus, event *BaseEvent) {
	m.mu.Lock()
	span := m.eventSpans[event.EventID]
	if span == nil {
		m.mu.Unlock()
		span = m.startEventSpan(eventbus, event)
		m.mu.Lock()
	}
	delete(m.eventContexts, event.EventID)
	delete(m.eventSpans, event.EventID)
	m.mu.Unlock()

	if err := firstEventError(event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.SetAttributes(eventAttributes(eventbus, event)...)
	span.End()
}

func (m *OtelTracingMiddleware) startHandlerSpan(eventbus *EventBus, event *BaseEvent, result *EventResult) trace.Span {
	key := handlerSpanKey(result.EventID, result.HandlerID)
	m.mu.Lock()
	defer m.mu.Unlock()
	if span := m.handlerSpans[key]; span != nil {
		return span
	}
	parent := m.eventContexts[event.EventID]
	if parent == nil {
		parent = context.Background()
	}
	ctx, span := m.tracer.Start(parent, fmt.Sprintf("%s.%s.%s", eventbus.Name, event.EventType, result.HandlerName), trace.WithAttributes(handlerAttributes(eventbus, event, result)...))
	m.handlerContext[key] = ctx
	m.handlerSpans[key] = span
	return span
}

func (m *OtelTracingMiddleware) completeHandlerSpan(eventbus *EventBus, event *BaseEvent, result *EventResult) {
	key := handlerSpanKey(result.EventID, result.HandlerID)
	m.mu.Lock()
	span := m.handlerSpans[key]
	delete(m.handlerContext, key)
	delete(m.handlerSpans, key)
	m.mu.Unlock()
	if span == nil {
		return
	}
	if result.Error != nil {
		err := errors.New(toErrorString(result.Error))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.SetAttributes(handlerAttributes(eventbus, event, result)...)
	span.End()
}

func handlerSpanKey(eventID string, handlerID string) string {
	return eventID + ":" + handlerID
}

func eventAttributes(eventbus *EventBus, event *BaseEvent) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("abxbus.eventbus.name", eventbus.Name),
		attribute.String("abxbus.eventbus.id", eventbus.ID),
		attribute.String("abxbus.event.id", event.EventID),
		attribute.String("abxbus.event.type", event.EventType),
		attribute.String("abxbus.event.status", event.EventStatus),
	}
	if event.EventParentID != nil {
		attrs = append(attrs, attribute.String("abxbus.event.parent_id", *event.EventParentID))
	}
	return attrs
}

func handlerAttributes(eventbus *EventBus, event *BaseEvent, result *EventResult) []attribute.KeyValue {
	attrs := eventAttributes(eventbus, event)
	attrs = append(attrs,
		attribute.String("abxbus.handler.id", result.HandlerID),
		attribute.String("abxbus.handler.name", result.HandlerName),
		attribute.String("abxbus.result.id", result.ID),
		attribute.String("abxbus.result.status", string(result.Status)),
	)
	return attrs
}

func firstEventError(event *BaseEvent) error {
	for _, result := range event.sortedEventResults() {
		if result.Error != nil {
			return errors.New(toErrorString(result.Error))
		}
	}
	return nil
}
