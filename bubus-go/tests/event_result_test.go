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
	r.Status = bubus.EventResultCompleted
	r.Result = "ok"
	r.Error = "boom"
	now := "2026-02-21T00:00:00.000000000Z"
	r.StartedAt = &now
	r.CompletedAt = &now

	data, err := r.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	round, err := bubus.EventResultFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	if round.ID != r.ID {
		t.Fatalf("id mismatch: %s vs %s", round.ID, r.ID)
	}
	if round.Status != r.Status {
		t.Fatalf("status mismatch: %s vs %s", round.Status, r.Status)
	}
	if round.HandlerID != h.ID || round.EventID != e.EventID {
		t.Fatal("handler/event ID roundtrip mismatch")
	}
	if round.HandlerName != h.HandlerName || round.EventBusName != h.EventBusName || round.EventBusID != h.EventBusID {
		t.Fatal("handler/event bus metadata mismatch")
	}
	if round.Result != "ok" || round.Error != "boom" {
		t.Fatalf("result/error mismatch after roundtrip: %#v %#v", round.Result, round.Error)
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
