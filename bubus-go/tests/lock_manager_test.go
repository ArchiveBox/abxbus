package bubus_test

import (
	"context"
	"sync/atomic"
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestWaitUntilIdleBehavesCorrectly(t *testing.T) {
	bus := bubus.NewEventBus("IdleBus", nil)
	var calls atomic.Int32
	bus.On("Evt", "h", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		calls.Add(1)
		return "ok", nil
	}, nil)

	short := 0.01
	if !bus.WaitUntilIdle(&short) {
		t.Fatal("bus should be idle before any events")
	}

	e := bus.Emit(bubus.NewBaseEvent("Evt", nil))
	if !bus.WaitUntilIdle(&short) {
		t.Fatal("bus should become idle after event completion")
	}
	if calls.Load() != 1 {
		t.Fatalf("expected one handler invocation, got %d", calls.Load())
	}
	if e.EventStatus != "completed" {
		t.Fatalf("expected completed event status, got %s", e.EventStatus)
	}
	if len(e.EventResults) != 1 {
		t.Fatalf("expected 1 event result, got %d", len(e.EventResults))
	}
	for _, result := range e.EventResults {
		if result.Status != bubus.EventResultCompleted {
			t.Fatalf("expected completed result, got %s", result.Status)
		}
		if result.Result != "ok" {
			t.Fatalf("unexpected handler result: %#v", result.Result)
		}
	}
}
