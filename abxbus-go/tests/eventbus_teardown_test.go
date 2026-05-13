package abxbus_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestDestroyClearFalsePreservesHandlersAndHistoryAndResumes(t *testing.T) {
	bus := abxbus.NewEventBus("DestroyBus", nil)
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
		t.Fatal("expected bus to become idle before destroy")
	}
	if bus.EventHistory.Size() != 1 {
		t.Fatalf("expected one event in history before destroy, got %d", bus.EventHistory.Size())
	}

	destroyed := make(chan struct{})
	go func() {
		time.Sleep(20 * time.Millisecond)
		bus.DestroyWithOptions(&abxbus.EventBusDestroyOptions{Clear: abxbus.Ptr(false)})
		close(destroyed)
	}()
	match, err := bus.Find("NeverHappens", nil, &abxbus.FindOptions{Past: false, Future: true})
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

	if bus.IsDestroyed() {
		t.Fatal("clear=false destroy should leave the bus reusable")
	}
	if bus.EventHistory.Size() != 1 {
		t.Fatalf("clear=false destroy should preserve history, got %d events", bus.EventHistory.Size())
	}
	if !bus.IsIdleAndQueueEmpty() {
		t.Fatal("expected idle after destroy")
	}

	e2 := bus.Emit(abxbus.NewBaseEvent("Evt", nil))
	if _, err := e2.Now(); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("clear=false destroy should preserve handlers, calls=%d", calls.Load())
	}
	if len(e2.EventResults) != 1 {
		t.Fatalf("expected preserved handler after clear=false destroy, got %d results", len(e2.EventResults))
	}
}

func TestDestroyWithTimeoutWaitsBeforeClearingRuntime(t *testing.T) {
	bus := abxbus.NewEventBus("DestroyTimeoutBus", nil)
	var calls atomic.Int32
	bus.On("SlowEvt", "slow", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		time.Sleep(30 * time.Millisecond)
		calls.Add(1)
		return "ok", nil
	}, nil)

	bus.Emit(abxbus.NewBaseEvent("SlowEvt", nil))
	start := time.Now()
	bus.DestroyWithOptions(&abxbus.EventBusDestroyOptions{Timeout: abxbus.Ptr(1.0), Clear: abxbus.Ptr(false)})
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("Destroy(timeout) should wait for in-flight work, elapsed=%s", elapsed)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected slow handler to finish before destroy returns, calls=%d", calls.Load())
	}
	if bus.EventHistory.Size() != 1 {
		t.Fatalf("clear=false destroy should preserve history after waiting, got %d events", bus.EventHistory.Size())
	}
	if bus.IsDestroyed() {
		t.Fatal("clear=false destroy should not terminally destroy the bus")
	}
}

func TestDestroyDefaultClearIsTerminalAndFreesBusState(t *testing.T) {
	bus := abxbus.NewEventBus("TerminalDestroyBus", nil)

	bus.On("Evt", "h", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, nil)
	event := bus.Emit(abxbus.NewBaseEvent("Evt", nil))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}

	bus.Destroy()
	if !bus.IsDestroyed() {
		t.Fatal("Destroy() should mark the bus destroyed")
	}
	if bus.EventHistory.Size() != 0 {
		t.Fatalf("Destroy() should clear history by default, got %d events", bus.EventHistory.Size())
	}
	if !bus.IsIdleAndQueueEmpty() {
		t.Fatal("destroyed bus should be idle with an empty queue")
	}

	assertDestroyedPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			recovered := recover()
			if recovered == nil {
				t.Fatalf("%s should panic after terminal destroy", name)
			}
			err, ok := recovered.(error)
			if !ok {
				t.Fatalf("%s panic should be an error, got %#v", name, recovered)
			}
			var destroyed *abxbus.EventBusDestroyedError
			if !errors.As(err, &destroyed) || !errors.Is(err, abxbus.ErrEventBusDestroyed) {
				t.Fatalf("%s panic should expose EventBusDestroyedError, got %T %v", name, err, err)
			}
			if destroyed.Operation != name {
				t.Fatalf("expected operation %s, got %s", name, destroyed.Operation)
			}
			if !strings.Contains(err.Error(), "TerminalDestroyBus#") || !strings.Contains(err.Error(), "event bus has been destroyed") {
				t.Fatalf("unexpected destroyed error shape: %v", err)
			}
		}()
		fn()
	}
	assertDestroyedPanic("Emit", func() { bus.Emit(abxbus.NewBaseEvent("Evt", nil)) })
	assertDestroyedPanic("On", func() {
		bus.On("Evt", "new", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) { return nil, nil }, nil)
	})

	if _, err := bus.Find("Evt", nil, nil); !errors.Is(err, abxbus.ErrEventBusDestroyed) {
		t.Fatalf("Find should reject with ErrEventBusDestroyed, got %v", err)
	}
	if _, err := bus.Filter("Evt", nil, nil); !errors.Is(err, abxbus.ErrEventBusDestroyed) {
		t.Fatalf("Filter should reject with ErrEventBusDestroyed, got %v", err)
	}
}

func TestDestroyingOneBusDoesNotBreakSharedHandlersOrForwardTargets(t *testing.T) {
	source := abxbus.NewEventBus("DestroySharedSourceBus", nil)
	target := abxbus.NewEventBus("DestroySharedTargetBus", nil)
	defer target.Destroy()

	var seen atomic.Int32
	shared := func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		seen.Add(1)
		return "shared", nil
	}
	source.On("SharedDestroyEvent", "shared_source", shared, nil)
	source.On("*", "forward", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return target.Emit(event), nil
	}, nil)
	target.On("SharedDestroyEvent", "shared_target", shared, nil)

	forwarded := source.Emit(abxbus.NewBaseEvent("SharedDestroyEvent", nil))
	if _, err := forwarded.Now(); err != nil {
		t.Fatal(err)
	}
	if seen.Load() != 2 {
		t.Fatalf("expected shared handler on source and target, got %d calls", seen.Load())
	}

	source.Destroy()

	direct := target.Emit(abxbus.NewBaseEvent("SharedDestroyEvent", nil))
	if result, err := direct.EventResult(); err != nil || result != "shared" {
		t.Fatalf("destroying source should not affect target; result=%#v err=%v", result, err)
	}
	if seen.Load() != 3 {
		t.Fatalf("target handler should still run after source destroy, calls=%d", seen.Load())
	}
}
