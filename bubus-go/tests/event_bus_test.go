package bubus_test

import (
	"context"
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestEmitAndDispatchUseDefaultBehavior(t *testing.T) {
	bus := bubus.NewEventBus("DefaultsBus", nil)
	if bus.EventConcurrency != bubus.EventConcurrencyBusSerial {
		t.Fatalf("unexpected default event concurrency: %s", bus.EventConcurrency)
	}
	if bus.EventHandlerConcurrency != bubus.EventHandlerConcurrencySerial {
		t.Fatalf("unexpected default handler concurrency: %s", bus.EventHandlerConcurrency)
	}
	if bus.EventHandlerCompletion != bubus.EventHandlerCompletionAll {
		t.Fatalf("unexpected default handler completion: %s", bus.EventHandlerCompletion)
	}

	calls := []string{}
	bus.On("CreateUserEvent", "first", func(ctx context.Context, event *bubus.BaseEvent) (any, error) {
		calls = append(calls, "first")
		return "first", nil
	}, nil)
	bus.On("CreateUserEvent", "second", func(ctx context.Context, event *bubus.BaseEvent) (any, error) {
		calls = append(calls, "second")
		return map[string]any{"user_id": "abc"}, nil
	}, nil)

	e := bus.Dispatch(bubus.NewBaseEvent("CreateUserEvent", map[string]any{"email": "a@b.com"}))
	if _, err := e.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if e.EventStatus != "completed" {
		t.Fatalf("expected completed event status, got %s", e.EventStatus)
	}
	if len(calls) != 2 || calls[0] != "first" || calls[1] != "second" {
		t.Fatalf("expected serial handler order, got %v", calls)
	}
	if len(e.EventResults) != 2 {
		t.Fatalf("expected 2 event results, got %d", len(e.EventResults))
	}

	values, err := e.EventResultsList(context.Background(), nil, &bubus.EventResultsListOptions{RaiseIfAny: false, RaiseIfNone: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 {
		t.Fatalf("expected two non-nil result values, got %#v", values)
	}
}

func TestEventResultReturnsFirstCompletedResult(t *testing.T) {
	bus := bubus.NewEventBus("SimpleBus", nil)
	bus.On("ResultEvent", "on_create", func(ctx context.Context, event *bubus.BaseEvent) (any, error) {
		return map[string]any{"user_id": "abc"}, nil
	}, nil)
	e := bus.Emit(bubus.NewBaseEvent("ResultEvent", map[string]any{"email": "a@b.com"}))
	result, err := e.EventResult(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map result, got %#v", result)
	}
	if m["user_id"] != "abc" {
		t.Fatalf("unexpected result %#v", result)
	}
}
