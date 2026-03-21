package bubus_test

import (
	"context"
	"sync/atomic"
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestOnOffByEntryByIDAndRemoveAll(t *testing.T) {
	bus := bubus.NewEventBus("OnOffBus", nil)
	var eventCalls atomic.Int32
	var wildcardCalls atomic.Int32

	h1 := bus.On("Evt", "h1", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		eventCalls.Add(1)
		return "h1", nil
	}, nil)
	h2 := bus.On("Evt", "h2", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		eventCalls.Add(1)
		return "h2", nil
	}, nil)
	bus.On("*", "all", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		wildcardCalls.Add(1)
		return "all", nil
	}, nil)

	e1 := bus.Emit(bubus.NewBaseEvent("Evt", nil))
	if _, err := e1.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if eventCalls.Load() != 2 || wildcardCalls.Load() != 1 {
		t.Fatalf("unexpected first call counts event=%d wildcard=%d", eventCalls.Load(), wildcardCalls.Load())
	}
	if len(e1.EventResults) != 3 {
		t.Fatalf("expected 3 handler results before off, got %d", len(e1.EventResults))
	}

	bus.Off("Evt", h1)
	e2 := bus.Emit(bubus.NewBaseEvent("Evt", nil))
	if _, err := e2.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if eventCalls.Load() != 3 || wildcardCalls.Load() != 2 {
		t.Fatalf("unexpected second call counts event=%d wildcard=%d", eventCalls.Load(), wildcardCalls.Load())
	}
	if len(e2.EventResults) != 2 {
		t.Fatalf("expected 2 handler results after removing one handler, got %d", len(e2.EventResults))
	}

	bus.Off("Evt", h2.ID)
	e3 := bus.Emit(bubus.NewBaseEvent("Evt", nil))
	if _, err := e3.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if eventCalls.Load() != 3 || wildcardCalls.Load() != 3 {
		t.Fatalf("unexpected third call counts event=%d wildcard=%d", eventCalls.Load(), wildcardCalls.Load())
	}
	if len(e3.EventResults) != 1 {
		t.Fatalf("expected wildcard-only results after removing event handlers, got %d", len(e3.EventResults))
	}

	bus.Off("Evt", nil)
	e4 := bus.Emit(bubus.NewBaseEvent("Evt", nil))
	if _, err := e4.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if eventCalls.Load() != 3 || wildcardCalls.Load() != 4 {
		t.Fatalf("unexpected fourth call counts event=%d wildcard=%d", eventCalls.Load(), wildcardCalls.Load())
	}

	bus.Off("*", nil)
	e5 := bus.Emit(bubus.NewBaseEvent("Evt", nil))
	if _, err := e5.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if eventCalls.Load() != 3 || wildcardCalls.Load() != 4 {
		t.Fatalf("no handlers should run after removing all, event=%d wildcard=%d", eventCalls.Load(), wildcardCalls.Load())
	}
	if len(e5.EventResults) != 0 {
		t.Fatalf("expected zero handler results after removing all handlers, got %d", len(e5.EventResults))
	}
}
