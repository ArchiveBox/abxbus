package abxbus_test

import (
	"context"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"
)

type dispatchDefaultsConcurrencyOverrideEvent struct {
	EventType        string
	EventConcurrency abxbus.EventConcurrencyMode
}

type dispatchDefaultsHandlerOverrideEvent struct {
	EventType               string
	EventHandlerConcurrency abxbus.EventHandlerConcurrencyMode
	EventHandlerCompletion  abxbus.EventHandlerCompletionMode
}

type dispatchDefaultsConfiguredEvent struct {
	Value                       int
	EventType                   string
	EventVersion                string
	EventTimeout                float64
	EventSlowTimeout            float64
	EventHandlerTimeout         float64
	EventHandlerSlowTimeout     float64
	EventBlocksParentCompletion bool
}

func TestEventConcurrencyRemainsUnsetOnDispatchAndResolvesDuringProcessing(t *testing.T) {
	bus := abxbus.NewEventBus("EventConcurrencyDefaultBus", &abxbus.EventBusOptions{
		EventConcurrency: abxbus.EventConcurrencyParallel,
	})
	defer bus.Destroy()
	bus.On("PropagationEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, nil)

	implicit := bus.Emit(abxbus.NewBaseEvent("PropagationEvent", nil))
	explicitNone := abxbus.NewBaseEvent("PropagationEvent", nil)
	explicitNone.EventConcurrency = ""
	explicitNone = bus.Emit(explicitNone)

	if implicit.EventConcurrency != "" {
		t.Fatalf("implicit event_concurrency should stay unset, got %q", implicit.EventConcurrency)
	}
	if explicitNone.EventConcurrency != "" {
		t.Fatalf("explicit empty event_concurrency should stay unset, got %q", explicitNone.EventConcurrency)
	}
	if _, err := implicit.Now(); err != nil {
		t.Fatal(err)
	}
	if _, err := explicitNone.Now(); err != nil {
		t.Fatal(err)
	}
	if implicit.EventConcurrency != "" || explicitNone.EventConcurrency != "" {
		t.Fatalf("bus defaults should not be written onto event after processing: %#v %#v", implicit, explicitNone)
	}
}

func TestEventConcurrencyClassOverrideBeatsBusDefault(t *testing.T) {
	bus := abxbus.NewEventBus("EventConcurrencyOverrideBus", &abxbus.EventBusOptions{
		EventConcurrency: abxbus.EventConcurrencyParallel,
	})
	defer bus.Destroy()
	bus.On("ConcurrencyOverrideEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, nil)

	event := bus.Emit(dispatchDefaultsConcurrencyOverrideEvent{
		EventType:        "ConcurrencyOverrideEvent",
		EventConcurrency: abxbus.EventConcurrencyGlobalSerial,
	})
	if event.EventConcurrency != abxbus.EventConcurrencyGlobalSerial {
		t.Fatalf("typed event default should beat bus default, got %q", event.EventConcurrency)
	}
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
}

func TestEventInstanceOverrideBeatsEventClassDefaults(t *testing.T) {
	bus := abxbus.NewEventBus("EventInstanceOverrideBus", nil)
	defer bus.Destroy()

	classDefault := bus.Emit(dispatchDefaultsConcurrencyOverrideEvent{
		EventType:        "ConcurrencyOverrideEvent",
		EventConcurrency: abxbus.EventConcurrencyGlobalSerial,
	})
	if classDefault.EventConcurrency != abxbus.EventConcurrencyGlobalSerial {
		t.Fatalf("typed event default mismatch: %q", classDefault.EventConcurrency)
	}

	instanceOverride := bus.Emit(dispatchDefaultsConcurrencyOverrideEvent{
		EventType:        "ConcurrencyOverrideEvent",
		EventConcurrency: abxbus.EventConcurrencyParallel,
	})
	if instanceOverride.EventConcurrency != abxbus.EventConcurrencyParallel {
		t.Fatalf("instance override mismatch: %q", instanceOverride.EventConcurrency)
	}
}

func TestHandlerDefaultsRemainUnsetOnDispatchAndResolveDuringProcessing(t *testing.T) {
	bus := abxbus.NewEventBus("HandlerDefaultsBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionFirst,
	})
	defer bus.Destroy()
	bus.On("PropagationEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, nil)

	implicit := bus.Emit(abxbus.NewBaseEvent("PropagationEvent", nil))
	explicitNone := abxbus.NewBaseEvent("PropagationEvent", nil)
	explicitNone.EventHandlerConcurrency = ""
	explicitNone.EventHandlerCompletion = ""
	explicitNone = bus.Emit(explicitNone)

	if implicit.EventHandlerConcurrency != "" || implicit.EventHandlerCompletion != "" {
		t.Fatalf("implicit handler defaults should stay unset, got %#v", implicit)
	}
	if explicitNone.EventHandlerConcurrency != "" || explicitNone.EventHandlerCompletion != "" {
		t.Fatalf("explicit empty handler defaults should stay unset, got %#v", explicitNone)
	}
	if _, err := implicit.Now(); err != nil {
		t.Fatal(err)
	}
	if _, err := explicitNone.Now(); err != nil {
		t.Fatal(err)
	}
	if implicit.EventHandlerConcurrency != "" || implicit.EventHandlerCompletion != "" ||
		explicitNone.EventHandlerConcurrency != "" || explicitNone.EventHandlerCompletion != "" {
		t.Fatalf("bus defaults should not be written onto events after processing: %#v %#v", implicit, explicitNone)
	}
}

func TestHandlerClassOverrideBeatsBusDefault(t *testing.T) {
	bus := abxbus.NewEventBus("HandlerDefaultsOverrideBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionFirst,
	})
	defer bus.Destroy()
	bus.On("HandlerOverrideEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, nil)

	event := bus.Emit(dispatchDefaultsHandlerOverrideEvent{
		EventType:               "HandlerOverrideEvent",
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
	})
	if event.EventHandlerConcurrency != abxbus.EventHandlerConcurrencySerial {
		t.Fatalf("handler concurrency default mismatch: %q", event.EventHandlerConcurrency)
	}
	if event.EventHandlerCompletion != abxbus.EventHandlerCompletionAll {
		t.Fatalf("handler completion default mismatch: %q", event.EventHandlerCompletion)
	}
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
}

func TestHandlerInstanceOverrideBeatsEventClassDefaults(t *testing.T) {
	bus := abxbus.NewEventBus("HandlerInstanceOverrideBus", nil)
	defer bus.Destroy()

	classDefault := bus.Emit(dispatchDefaultsHandlerOverrideEvent{
		EventType:               "HandlerOverrideEvent",
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
	})
	if classDefault.EventHandlerConcurrency != abxbus.EventHandlerConcurrencySerial ||
		classDefault.EventHandlerCompletion != abxbus.EventHandlerCompletionAll {
		t.Fatalf("handler class defaults mismatch: %#v", classDefault)
	}

	instanceOverride := bus.Emit(dispatchDefaultsHandlerOverrideEvent{
		EventType:               "HandlerOverrideEvent",
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionFirst,
	})
	if instanceOverride.EventHandlerConcurrency != abxbus.EventHandlerConcurrencyParallel ||
		instanceOverride.EventHandlerCompletion != abxbus.EventHandlerCompletionFirst {
		t.Fatalf("handler instance override mismatch: %#v", instanceOverride)
	}
}

func TestTypedEventConfigDefaultsPopulateBaseEventFields(t *testing.T) {
	bus := abxbus.NewEventBus("ConfiguredEventDefaultsBus", nil)
	defer bus.Destroy()

	event := bus.Emit(dispatchDefaultsConfiguredEvent{
		Value:                       1,
		EventType:                   "ConfiguredEvent",
		EventVersion:                "2.0.0",
		EventTimeout:                12,
		EventSlowTimeout:            30,
		EventHandlerTimeout:         3,
		EventHandlerSlowTimeout:     4,
		EventBlocksParentCompletion: true,
	})
	if event.EventVersion != "2.0.0" {
		t.Fatalf("event_version mismatch: %s", event.EventVersion)
	}
	if event.EventTimeout == nil || *event.EventTimeout != 12 {
		t.Fatalf("event_timeout mismatch: %#v", event.EventTimeout)
	}
	if event.EventSlowTimeout == nil || *event.EventSlowTimeout != 30 {
		t.Fatalf("event_slow_timeout mismatch: %#v", event.EventSlowTimeout)
	}
	if event.EventHandlerTimeout == nil || *event.EventHandlerTimeout != 3 {
		t.Fatalf("event_handler_timeout mismatch: %#v", event.EventHandlerTimeout)
	}
	if event.EventHandlerSlowTimeout == nil || *event.EventHandlerSlowTimeout != 4 {
		t.Fatalf("event_handler_slow_timeout mismatch: %#v", event.EventHandlerSlowTimeout)
	}
	if !event.EventBlocksParentCompletion {
		t.Fatalf("event_blocks_parent_completion mismatch: %#v", event.EventBlocksParentCompletion)
	}
}
