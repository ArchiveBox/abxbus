package abxbus_test

import (
	"context"
	"errors"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestOtelTracingMiddlewareCreatesEventAndHandlerSpans(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	middleware := abxbus.NewOtelTracingMiddleware(&abxbus.OtelTracingMiddlewareOptions{
		Tracer: provider.Tracer("abxbus-test"),
		RootSpanAttributes: []attribute.KeyValue{
			attribute.String("stagehand.session_id", "session-123"),
		},
	})
	bus := abxbus.NewEventBus("OtelBus", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})
	bus.On("OtelEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("OtelEvent", map[string]any{"session_id": "event-session-456", "value": "x"}))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}

	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("expected event and handler spans, got %d", len(spans))
	}

	names := map[string]bool{}
	for _, span := range spans {
		names[span.Name()] = true
	}
	if !names["OtelBus.emit(OtelEvent)"] {
		t.Fatalf("missing event span: %#v", names)
	}
	if !names["handler(OtelEvent)"] {
		t.Fatalf("missing handler span: %#v", names)
	}
	eventSpan := findSpan(t, spans, "OtelBus.emit(OtelEvent)")
	handlerSpan := findSpan(t, spans, "handler(OtelEvent)")
	eventAttrs := spanAttributes(eventSpan)
	handlerAttrs := spanAttributes(handlerSpan)
	if eventAttrs["abxbus.event_bus.name"] != "OtelBus" || eventAttrs["abxbus.event_type"] != "OtelEvent" {
		t.Fatalf("event span attributes did not match abxbus schema: %#v", eventAttrs)
	}
	if eventAttrs["abxbus.trace.root"] != true {
		t.Fatalf("top-level event span should be marked as trace root: %#v", eventAttrs)
	}
	if eventAttrs["stagehand.session_id"] != "session-123" || eventAttrs["abxbus.session_id"] != "event-session-456" {
		t.Fatalf("missing root/session attrs: %#v", eventAttrs)
	}
	if handlerAttrs["abxbus.handler_name"] != "handler" || handlerAttrs["abxbus.event_result_status"] != "completed" {
		t.Fatalf("handler span attributes did not match abxbus schema: %#v", handlerAttrs)
	}
}

func TestOtelTracingMiddlewareNamesEventAndHandlerSpansForDisplay(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	middleware := abxbus.NewOtelTracingMiddleware(&abxbus.OtelTracingMiddlewareOptions{
		Tracer: provider.Tracer("abxbus-test"),
	})
	bus := abxbus.NewEventBus("StagehandExtensionBackground", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})
	t.Cleanup(bus.Destroy)
	bus.On("CDPConnect", "DebuggerClient.on_CDPConnect", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "connected", nil
	}, nil)

	eventTimeout := 0.2
	event := abxbus.NewBaseEvent("CDPConnect", nil)
	event.EventTimeout = &eventTimeout
	if _, err := bus.Emit(event).Now(); err != nil {
		t.Fatal(err)
	}

	_ = findSpan(t, recorder.Ended(), "StagehandExtensionBackground.emit(CDPConnect)")
	_ = findSpan(t, recorder.Ended(), "DebuggerClient.on_CDPConnect(CDPConnect)")
}

func TestOtelTracingMiddlewareParentsChildEventToEmittingHandlerSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	middleware := abxbus.NewOtelTracingMiddleware(&abxbus.OtelTracingMiddlewareOptions{
		Tracer: provider.Tracer("abxbus-test"),
	})
	bus := abxbus.NewEventBus("OtelParentBus", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})
	bus.On("ParentOtelEvent", "parent", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		child := event.Emit(abxbus.NewBaseEvent("ChildOtelEvent", nil))
		if _, err := child.Now(); err != nil {
			return nil, err
		}
		return "parent", nil
	}, nil)
	bus.On("ChildOtelEvent", "child", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "child", nil
	}, nil)

	parent := bus.Emit(abxbus.NewBaseEvent("ParentOtelEvent", nil))
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}

	spans := recorder.Ended()
	parentEventSpan := findSpan(t, spans, "OtelParentBus.emit(ParentOtelEvent)")
	parentHandlerSpan := findSpan(t, spans, "parent(ParentOtelEvent)")
	childEventSpan := findSpan(t, spans, "OtelParentBus.emit(ChildOtelEvent)")
	childHandlerSpan := findSpan(t, spans, "child(ChildOtelEvent)")

	if parentHandlerSpan.Parent().SpanID() != parentEventSpan.SpanContext().SpanID() {
		t.Fatalf("parent handler should be child of parent event")
	}
	if childEventSpan.Parent().SpanID() != parentHandlerSpan.SpanContext().SpanID() {
		t.Fatalf("child event should be child of emitting handler")
	}
	if childHandlerSpan.Parent().SpanID() != childEventSpan.SpanContext().SpanID() {
		t.Fatalf("child handler should be child of child event")
	}
	childAttrs := spanAttributes(childEventSpan)
	if childAttrs["abxbus.event_parent_id"] != parent.EventID {
		t.Fatalf("child event span missing parent id attr: %#v", childAttrs)
	}
	if _, ok := childAttrs["abxbus.event_emitted_by_handler_id"].(string); !ok {
		t.Fatalf("child event span missing emitted-by handler attr: %#v", childAttrs)
	}
}

func TestOtelTracingMiddlewareWaitsUntilTopLevelEventCompletionBeforeEndingSpans(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	middleware := abxbus.NewOtelTracingMiddleware(&abxbus.OtelTracingMiddlewareOptions{
		Tracer: provider.Tracer("abxbus-test"),
	})
	bus := abxbus.NewEventBus("OtelRootStartBus", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})
	t.Cleanup(bus.Destroy)

	started := make(chan struct{})
	release := make(chan struct{})
	bus.On("OtelRootStartEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		close(started)
		select {
		case <-release:
			return "done", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)

	eventTimeout := 0.5
	event := abxbus.NewBaseEvent("OtelRootStartEvent", nil)
	event.EventTimeout = &eventTimeout
	emitted := bus.Emit(event)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}
	if ended := recorder.Ended(); len(ended) != 0 {
		t.Fatalf("spans should not be ended/exportable before event completion, got %d", len(ended))
	}

	close(release)
	if _, err := emitted.Now(); err != nil {
		t.Fatal(err)
	}

	eventSpan := findSpan(t, recorder.Ended(), "OtelRootStartBus.emit(OtelRootStartEvent)")
	handlerSpan := findSpan(t, recorder.Ended(), "handler(OtelRootStartEvent)")
	if eventSpan.Parent().IsValid() {
		t.Fatalf("top-level event span should be root, got parent=%s", eventSpan.Parent().SpanID())
	}
	if handlerSpan.Parent().SpanID() != eventSpan.SpanContext().SpanID() {
		t.Fatalf("handler span should be child of event span")
	}
	if !eventSpan.EndTime().After(eventSpan.StartTime()) || !handlerSpan.EndTime().After(handlerSpan.StartTime()) {
		t.Fatalf("ended spans should have non-zero duration")
	}
}

func TestOtelTracingMiddlewareRecordsHandlerErrors(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	middleware := abxbus.NewOtelTracingMiddleware(&abxbus.OtelTracingMiddlewareOptions{
		Tracer: provider.Tracer("abxbus-test"),
	})
	bus := abxbus.NewEventBus("OtelErrorBus", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})
	handlerErr := errors.New("handler failed")
	bus.On("OtelErrorEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return nil, handlerErr
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("OtelErrorEvent", nil))
	if _, err := event.Wait(); err != nil {
		t.Fatal(err)
	}

	eventSpan := findSpan(t, recorder.Ended(), "OtelErrorBus.emit(OtelErrorEvent)")
	handlerSpan := findSpan(t, recorder.Ended(), "handler(OtelErrorEvent)")
	if eventSpan.Status().Code != codes.Error {
		t.Fatalf("event span should be error, got %#v", eventSpan.Status())
	}
	if handlerSpan.Status().Code != codes.Error {
		t.Fatalf("handler span should be error, got %#v", handlerSpan.Status())
	}
}

func findSpan(t *testing.T, spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, span := range spans {
		if span.Name() == name {
			return span
		}
	}
	t.Fatalf("missing span %q", name)
	return nil
}

func spanAttributes(span sdktrace.ReadOnlySpan) map[string]any {
	attrs := map[string]any{}
	for _, attr := range span.Attributes() {
		attrs[string(attr.Key)] = attr.Value.AsInterface()
	}
	return attrs
}
