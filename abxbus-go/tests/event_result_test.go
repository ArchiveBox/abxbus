package abxbus_test

import (
	"context"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestEventResultJSONRoundtrip(t *testing.T) {
	bus := abxbus.NewEventBus("ResultBus", nil)
	h := abxbus.NewEventHandler(bus.Name, bus.ID, "Evt", "h", nil)
	e := abxbus.NewBaseEvent("Evt", nil)
	r := abxbus.NewEventResult(e, h)
	r.Status = abxbus.EventResultCompleted
	r.Result = "ok"
	r.Error = "boom"
	now := "2026-02-21T00:00:00.000000000Z"
	r.StartedAt = &now
	r.CompletedAt = &now

	data, err := r.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	round, err := abxbus.EventResultFromJSON(data)
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
	bus := abxbus.NewEventBus("ResultBus", nil)
	h := abxbus.NewEventHandler(bus.Name, bus.ID, "Evt", "h", nil)
	e := abxbus.NewBaseEvent("Evt", nil)
	r := abxbus.NewEventResult(e, h)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := r.Wait(ctx); err == nil {
		t.Fatal("expected timeout")
	}
}

func TestEventResultUpdateKeepsConsistentOrderingSemanticsForStatusResultError(t *testing.T) {
	bus := abxbus.NewEventBus("StandaloneResultUpdateBus", nil)
	handler := abxbus.NewEventHandler(bus.Name, bus.ID, "StandaloneEvent", "handler", nil)
	event := abxbus.NewBaseEvent("StandaloneEvent", nil)
	result := abxbus.NewEventResult(event, handler)
	result.Error = "RuntimeError: existing"

	result.Update(&abxbus.EventResultUpdateOptions{Status: abxbus.EventResultCompleted})
	if result.Status != abxbus.EventResultCompleted {
		t.Fatalf("expected completed status, got %s", result.Status)
	}
	if result.Error != "RuntimeError: existing" {
		t.Fatalf("status-only update should preserve existing error, got %#v", result.Error)
	}

	result.Update(&abxbus.EventResultUpdateOptions{
		Status: abxbus.EventResultError,
		Result: "seeded",
	})
	if result.Result != "seeded" {
		t.Fatalf("result update should preserve seeded result, got %#v", result.Result)
	}
	if result.Status != abxbus.EventResultError {
		t.Fatalf("explicit status should apply after result, got %s", result.Status)
	}
}
