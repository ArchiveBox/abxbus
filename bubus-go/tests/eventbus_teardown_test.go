package bubus_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestDestroyClearsRuntimeStateAndCancelsPendingFinds(t *testing.T) {
	bus := bubus.NewEventBus("DestroyBus", nil)
	var calls atomic.Int32
	bus.On("Evt", "h", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		calls.Add(1)
		return "ok", nil
	}, nil)

	e := bus.Emit(bubus.NewBaseEvent("Evt", nil))
	if _, err := e.Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	timeout := 1.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("expected bus to become idle before destroy")
	}

	destroyed := make(chan struct{})
	go func() {
		time.Sleep(20 * time.Millisecond)
		bus.Destroy()
		close(destroyed)
	}()
	match, err := bus.Find("NeverHappens", nil, &bubus.FindOptions{Past: false, Future: true})
	if err != nil {
		t.Fatal(err)
	}
	if match != nil {
		t.Fatalf("destroy should resolve pending find with nil, got %#v", match)
	}
	select {
	case <-destroyed:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for destroy")
	}

	if bus.EventHistory.Size() != 0 {
		t.Fatal("expected empty history after destroy")
	}
	if !bus.IsIdleAndQueueEmpty() {
		t.Fatal("expected idle after destroy")
	}

	e2 := bus.Emit(bubus.NewBaseEvent("Evt", nil))
	if _, err := e2.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("destroyed handlers should not fire after destroy, calls=%d", calls.Load())
	}
	if len(e2.EventResults) != 0 {
		t.Fatalf("expected no old handlers after destroy, got %d results", len(e2.EventResults))
	}

	bus.On("Evt", "new", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		calls.Add(1)
		return "new", nil
	}, nil)
	e3 := bus.Emit(bubus.NewBaseEvent("Evt", nil))
	if _, err := e3.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("bus should accept new handlers after destroy, calls=%d", calls.Load())
	}
}
