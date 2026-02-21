package bubus_test

import (
	"context"
	"testing"
	"time"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestEventResultJSONRoundtrip(t *testing.T) {
	bus := bubus.NewEventBus("ResultBus", nil)
	h := bubus.NewEventHandler(bus.Name, bus.ID, "Evt", "h", nil)
	e := bubus.NewBaseEvent("Evt", nil)
	r := bubus.NewEventResult(e, h)
	data, err := r.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	round, err := bubus.EventResultFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	if round.HandlerID != h.ID || round.EventID != e.EventID {
		t.Fatal("result roundtrip mismatch")
	}
}

func TestEventResultWaitTimeout(t *testing.T) {
	bus := bubus.NewEventBus("ResultBus", nil)
	h := bubus.NewEventHandler(bus.Name, bus.ID, "Evt", "h", nil)
	e := bubus.NewBaseEvent("Evt", nil)
	r := bubus.NewEventResult(e, h)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := r.Wait(ctx); err == nil {
		t.Fatal("expected timeout")
	}
}
