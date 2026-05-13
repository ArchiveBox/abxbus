package abxbus_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestStopClearsRuntimeStateAndCancelsPendingFinds(t *testing.T) {
	bus := abxbus.NewEventBus("StopBus", nil)
	var calls atomic.Int32
	bus.On("Evt", "h", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		calls.Add(1)
		return "ok", nil
	}, nil)

	e := bus.Emit(abxbus.NewBaseEvent("Evt", nil))
	if _, err := e.Now(); err != nil {
		t.Fatal(err)
	}

	timeout := 1.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("expected bus to become idle before stop")
	}

	stopped := make(chan struct{})
	go func() {
		time.Sleep(20 * time.Millisecond)
		bus.Stop()
		close(stopped)
	}()
	match, err := bus.Find("NeverHappens", nil, &abxbus.FindOptions{Past: false, Future: true})
	if err != nil {
		t.Fatal(err)
	}
	if match != nil {
		t.Fatalf("stop should resolve pending find with nil, got %#v", match)
	}
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stop")
	}

	if bus.EventHistory.Size() != 0 {
		t.Fatal("expected empty history after stop")
	}
	if !bus.IsIdleAndQueueEmpty() {
		t.Fatal("expected idle after stop")
	}

	e2 := bus.Emit(abxbus.NewBaseEvent("Evt", nil))
	if _, err := e2.Now(); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("stopped handlers should not fire after stop, calls=%d", calls.Load())
	}
	if len(e2.EventResults) != 0 {
		t.Fatalf("expected no old handlers after stop, got %d results", len(e2.EventResults))
	}

	bus.On("Evt", "new", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		calls.Add(1)
		return "new", nil
	}, nil)
	e3 := bus.Emit(abxbus.NewBaseEvent("Evt", nil))
	if _, err := e3.Now(); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("bus should accept new handlers after stop, calls=%d", calls.Load())
	}
}
