package bubus_test

import (
	"context"
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestEventBusDefaults(t *testing.T) {
	bus := bubus.NewEventBus("DefaultsBus", nil)
	if bus.Name != "DefaultsBus" {
		t.Fatal("bad name")
	}
	if bus.EventConcurrency != bubus.EventConcurrencyBusSerial {
		t.Fatal("bad concurrency")
	}
	if bus.EventHandlerConcurrency != bubus.EventHandlerConcurrencySerial {
		t.Fatal("bad handler concurrency")
	}
	if bus.EventHandlerCompletion != bubus.EventHandlerCompletionAll {
		t.Fatal("bad completion")
	}
}

func TestEmitDoneAndEventResult(t *testing.T) {
	bus := bubus.NewEventBus("SimpleBus", nil)
	bus.On("CreateUserEvent", "on_create", func(ctx context.Context, event *bubus.BaseEvent) (any, error) {
		return map[string]any{"user_id": "abc"}, nil
	}, nil)
	e := bus.Emit(bubus.NewBaseEvent("CreateUserEvent", map[string]any{"email": "a@b.com"}))
	result, err := e.EventResult(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	if m["user_id"] != "abc" {
		t.Fatalf("unexpected result %#v", result)
	}
}
