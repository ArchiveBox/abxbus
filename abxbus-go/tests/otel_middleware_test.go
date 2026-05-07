package abxbus_test

import (
	"context"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestOtelTracingMiddlewareCreatesEventAndHandlerSpans(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	middleware := abxbus.NewOtelTracingMiddleware(&abxbus.OtelTracingMiddlewareOptions{
		Tracer: provider.Tracer("abxbus-test"),
	})
	bus := abxbus.NewEventBus("OtelBus", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})
	bus.On("OtelEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("OtelEvent", map[string]any{"value": "x"}))
	if _, err := event.Done(context.Background()); err != nil {
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
	if !names["OtelBus.OtelEvent"] {
		t.Fatalf("missing event span: %#v", names)
	}
	if !names["OtelBus.OtelEvent.handler"] {
		t.Fatalf("missing handler span: %#v", names)
	}
}
