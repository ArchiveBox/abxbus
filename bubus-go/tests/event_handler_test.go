package bubus_test

import (
	"context"
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestEventHandlerJSONRoundtrip(t *testing.T) {
	h := bubus.NewEventHandler("Bus", "bus-id", "Evt", "h", func(ctx context.Context, event *bubus.BaseEvent) (any, error) { return "ok", nil })
	data, err := h.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	round, err := bubus.EventHandlerFromJSON(data, nil)
	if err != nil {
		t.Fatal(err)
	}
	if round.ID != h.ID || round.EventPattern != h.EventPattern {
		t.Fatal("handler roundtrip mismatch")
	}
}
