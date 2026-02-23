package bubus_test

import (
	"context"
	"encoding/json"
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestEventHandlerJSONRoundtrip(t *testing.T) {
	h := bubus.NewEventHandler("Bus", "bus-id", "Evt", "h", func(ctx context.Context, event *bubus.BaseEvent) (any, error) { return "ok", nil })
	data, err := h.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	if wire["id"] != h.ID || wire["event_pattern"] != h.EventPattern || wire["handler_name"] != h.HandlerName {
		t.Fatalf("unexpected wire payload values: %#v", wire)
	}
	if wire["eventbus_name"] != h.EventBusName || wire["eventbus_id"] != h.EventBusID {
		t.Fatalf("event bus metadata mismatch in wire payload: %#v", wire)
	}

	round, err := bubus.EventHandlerFromJSON(data, nil)
	if err != nil {
		t.Fatal(err)
	}
	if round.ID != h.ID {
		t.Fatalf("id mismatch: %s vs %s", round.ID, h.ID)
	}
	if round.EventPattern != h.EventPattern {
		t.Fatalf("event pattern mismatch: %s vs %s", round.EventPattern, h.EventPattern)
	}
	if round.EventBusName != h.EventBusName || round.EventBusID != h.EventBusID {
		t.Fatalf("event bus fields mismatch after roundtrip")
	}
	if round.HandlerName != h.HandlerName {
		t.Fatalf("handler name mismatch: %s vs %s", round.HandlerName, h.HandlerName)
	}
	if round.HandlerRegisteredAt != h.HandlerRegisteredAt {
		t.Fatalf("registered_at mismatch: %s vs %s", round.HandlerRegisteredAt, h.HandlerRegisteredAt)
	}
}
