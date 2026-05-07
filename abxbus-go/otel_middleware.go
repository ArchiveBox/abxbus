package abxbus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type OtelTracingMiddlewareOptions struct {
	Tracer             trace.Tracer
	RootSpanAttributes []attribute.KeyValue
}

type OtelTracingMiddleware struct {
	tracer         trace.Tracer
	mu             sync.Mutex
	eventContexts  map[string]context.Context
	eventSpans     map[string]trace.Span
	handlerSpans   map[string]trace.Span
	handlerContext map[string]context.Context
	rootAttributes []attribute.KeyValue
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
		rootAttributes: append([]attribute.KeyValue{}, optionsRootAttributes(options)...),
	}
}

func optionsRootAttributes(options *OtelTracingMiddlewareOptions) []attribute.KeyValue {
	if options == nil {
		return nil
	}
	return options.RootSpanAttributes
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
	if event.EventParentID != nil && event.EventEmittedByHandlerID != nil {
		if parentCtx := m.handlerContext[handlerSpanKey(*event.EventParentID, *event.EventEmittedByHandlerID)]; parentCtx != nil {
			parent = parentCtx
		}
	} else if event.EventParentID != nil {
		if parentCtx := m.eventContexts[*event.EventParentID]; parentCtx != nil {
			parent = parentCtx
		}
	}
	ctx, span := m.tracer.Start(parent, eventSpanName(eventbus, event), trace.WithAttributes(m.eventAttributes(eventbus, event)...))
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
	span.SetAttributes(m.eventAttributes(eventbus, event)...)
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
	ctx, span := m.tracer.Start(parent, handlerSpanName(event, result), trace.WithAttributes(handlerAttributes(eventbus, event, result)...))
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

func eventSpanName(eventbus *EventBus, event *BaseEvent) string {
	return fmt.Sprintf("%s.emit(%s)", eventbus.Name, event.EventType)
}

func handlerSpanName(event *BaseEvent, result *EventResult) string {
	return fmt.Sprintf("%s(%s)", result.HandlerName, event.EventType)
}

func (m *OtelTracingMiddleware) eventAttributes(eventbus *EventBus, event *BaseEvent) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("abxbus.event_bus.id", eventbus.ID),
		attribute.String("abxbus.event_bus.name", eventbus.Name),
		attribute.String("abxbus.event_id", event.EventID),
		attribute.String("abxbus.event_type", event.EventType),
		attribute.String("abxbus.event_version", event.EventVersion),
		attribute.String("abxbus.event_path", strings.Join(event.EventPath, " ")),
		attribute.String("abxbus.event_status", event.EventStatus),
	}
	if event.EventParentID == nil {
		attrs = append(attrs, m.rootAttributes...)
		attrs = append(attrs, attribute.Bool("abxbus.trace.root", true))
	}
	if event.EventParentID != nil {
		attrs = append(attrs, attribute.String("abxbus.event_parent_id", *event.EventParentID))
	}
	if event.EventEmittedByHandlerID != nil {
		attrs = append(attrs, attribute.String("abxbus.event_emitted_by_handler_id", *event.EventEmittedByHandlerID))
	}
	if sessionID, ok := event.Payload["session_id"].(string); ok {
		attrs = append(attrs, attribute.String("abxbus.session_id", sessionID))
	}
	return attrs
}

func handlerAttributes(eventbus *EventBus, event *BaseEvent, result *EventResult) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("abxbus.event_bus.id", eventbus.ID),
		attribute.String("abxbus.event_bus.name", eventbus.Name),
		attribute.String("abxbus.event_id", event.EventID),
		attribute.String("abxbus.event_type", event.EventType),
	}
	attrs = append(attrs,
		attribute.String("abxbus.handler_id", result.HandlerID),
		attribute.String("abxbus.handler_name", result.HandlerName),
		attribute.String("abxbus.handler_event_pattern", result.HandlerEventPattern),
		attribute.String("abxbus.event_result_id", result.ID),
		attribute.String("abxbus.event_result_status", string(result.Status)),
	)
	if result.HandlerFilePath != nil {
		attrs = append(attrs, attribute.String("abxbus.handler_file_path", *result.HandlerFilePath))
	}
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
