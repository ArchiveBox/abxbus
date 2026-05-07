package abxbus_test

import (
	"context"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestBaseEventCarriesEventBusReferenceDuringDispatch(t *testing.T) {
	bus := abxbus.NewEventBus("ProxyBus", nil)
	var seenBus *abxbus.EventBus
	bus.On("ProxyEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		seenBus = event.Bus
		return event.Bus.Name, nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("ProxyEvent", nil))
	result, err := event.EventResult(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if seenBus != bus || event.Bus != bus || result != "ProxyBus" {
		t.Fatalf("event bus reference mismatch: seen=%p event=%p bus=%p result=%#v", seenBus, event.Bus, bus, result)
	}
}
