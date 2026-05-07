package abxbus_test

import (
	"context"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestDispatchLeavesEventConcurrencyDefaultsUnsetOnEvent(t *testing.T) {
	bus := abxbus.NewEventBus("DispatchDefaultsBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyParallel,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
	})
	bus.On("DefaultsEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("DefaultsEvent", nil))
	if event.EventConcurrency != "" || event.EventHandlerConcurrency != "" || event.EventHandlerCompletion != "" {
		t.Fatalf("bus defaults should not be copied onto event at dispatch: %#v", event)
	}
	if _, err := event.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if event.EventConcurrency != "" || event.EventHandlerConcurrency != "" || event.EventHandlerCompletion != "" {
		t.Fatalf("bus defaults should remain resolved at processing time only: %#v", event)
	}
}

func TestEventDispatchOverridesBeatBusDefaults(t *testing.T) {
	bus := abxbus.NewEventBus("DispatchOverrideBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyParallel,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
	})
	bus.On("OverrideEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, nil)

	event := abxbus.NewBaseEvent("OverrideEvent", nil)
	event.EventConcurrency = abxbus.EventConcurrencyBusSerial
	event.EventHandlerConcurrency = abxbus.EventHandlerConcurrencySerial
	event.EventHandlerCompletion = abxbus.EventHandlerCompletionFirst
	if _, err := bus.Emit(event).Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if event.EventConcurrency != abxbus.EventConcurrencyBusSerial ||
		event.EventHandlerConcurrency != abxbus.EventHandlerConcurrencySerial ||
		event.EventHandlerCompletion != abxbus.EventHandlerCompletionFirst {
		t.Fatalf("event-level overrides should be preserved, got %#v", event)
	}
}
