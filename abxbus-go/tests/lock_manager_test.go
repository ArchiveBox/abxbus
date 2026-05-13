package abxbus_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"
)

func lockManagerUpdateMax(maxActive *atomic.Int32, value int32) {
	for {
		current := maxActive.Load()
		if value <= current || maxActive.CompareAndSwap(current, value) {
			return
		}
	}
}

func registerActiveLockManagerHandler(bus *abxbus.EventBus, eventType string, handlerName string, active *atomic.Int32, maxActive *atomic.Int32) {
	bus.OnEventName(eventType, handlerName, func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		nowActive := active.Add(1)
		lockManagerUpdateMax(maxActive, nowActive)
		time.Sleep(20 * time.Millisecond)
		active.Add(-1)
		return "ok", nil
	}, nil)
}

func TestAsyncLock1ReleasingToQueuedWaiterDoesNotAllowNewAcquireToSlipIn(t *testing.T) {
	lock := abxbus.NewAsyncLock(1)
	if err := lock.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	waiterAcquired := make(chan struct{})
	releaseWaiter := make(chan struct{})
	go func() {
		if err := lock.Acquire(context.Background()); err != nil {
			t.Errorf("waiter acquire: %v", err)
			return
		}
		close(waiterAcquired)
		<-releaseWaiter
		lock.Release()
	}()

	time.Sleep(10 * time.Millisecond)
	lock.Release()

	contenderAcquired := make(chan struct{})
	go func() {
		if err := lock.Acquire(context.Background()); err != nil {
			t.Errorf("contender acquire: %v", err)
			return
		}
		close(contenderAcquired)
		lock.Release()
	}()

	select {
	case <-waiterAcquired:
	case <-time.After(time.Second):
		t.Fatal("queued waiter should receive the released permit first")
	}

	select {
	case <-contenderAcquired:
		t.Fatal("new contender acquired before queued waiter released")
	default:
	}

	close(releaseWaiter)
	select {
	case <-contenderAcquired:
	case <-time.After(time.Second):
		t.Fatal("contender should acquire after waiter releases")
	}
}

func TestAsyncLockSizeGreaterThanOneEnforcesSemaphoreConcurrencyLimit(t *testing.T) {
	lock := abxbus.NewAsyncLock(2)
	var active atomic.Int32
	var maxActive atomic.Int32
	done := make(chan struct{})

	for range 6 {
		go func() {
			if err := lock.Acquire(context.Background()); err != nil {
				t.Errorf("worker acquire: %v", err)
				done <- struct{}{}
				return
			}
			nowActive := active.Add(1)
			lockManagerUpdateMax(&maxActive, nowActive)
			time.Sleep(5 * time.Millisecond)
			active.Add(-1)
			lock.Release()
			done <- struct{}{}
		}()
	}

	for range 6 {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("worker timed out")
		}
	}
	if maxActive.Load() != 2 {
		t.Fatalf("expected max active workers to be 2, got %d", maxActive.Load())
	}
	if active.Load() != 0 {
		t.Fatalf("expected no active workers after completion, got %d", active.Load())
	}
}

func TestWaitUntilIdleBehavesCorrectly(t *testing.T) {
	bus := abxbus.NewEventBus("IdleBus", nil)
	defer bus.Destroy()
	var calls atomic.Int32
	bus.OnEventName("Evt", "h", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		calls.Add(1)
		return "ok", nil
	}, nil)

	short := 0.01
	if !bus.WaitUntilIdle(&short) {
		t.Fatal("bus should be idle before any events")
	}

	e := bus.EmitEventName("Evt", nil)
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
		if result.Status != abxbus.EventResultCompleted {
			t.Fatalf("expected completed result, got %s", result.Status)
		}
		if result.Result != "ok" {
			t.Fatalf("unexpected handler result: %#v", result.Result)
		}
	}
}

func TestLockManagerGetLockForEventModes(t *testing.T) {
	var active atomic.Int32
	var maxActive atomic.Int32
	timeout := 1.0

	busSerial := abxbus.NewEventBus("LockManagerEventModesBusSerial", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	registerActiveLockManagerHandler(busSerial, "LockModesEvent", "handler", &active, &maxActive)
	busSerial.EmitEventName("LockModesEvent", nil)
	busSerial.EmitEventName("LockModesEvent", nil)
	if !busSerial.WaitUntilIdle(&timeout) {
		t.Fatal("bus-serial bus did not become idle")
	}
	if maxActive.Load() != 1 {
		t.Fatalf("expected bus-serial events to serialize, got max active %d", maxActive.Load())
	}
	busSerial.Destroy()

	active.Store(0)
	maxActive.Store(0)
	parallelOverrideBus := abxbus.NewEventBus("LockManagerEventModesParallelOverrideBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	registerActiveLockManagerHandler(parallelOverrideBus, "LockModesEvent", "handler", &active, &maxActive)
	first := abxbus.NewBaseEvent("LockModesEvent", nil)
	first.EventConcurrency = abxbus.EventConcurrencyParallel
	second := abxbus.NewBaseEvent("LockModesEvent", nil)
	second.EventConcurrency = abxbus.EventConcurrencyParallel
	parallelOverrideBus.Emit(first)
	parallelOverrideBus.Emit(second)
	if !parallelOverrideBus.WaitUntilIdle(&timeout) {
		t.Fatal("parallel override bus did not become idle")
	}
	if maxActive.Load() != 2 {
		t.Fatalf("expected parallel event override to bypass bus lock, got max active %d", maxActive.Load())
	}
	parallelOverrideBus.Destroy()

	active.Store(0)
	maxActive.Store(0)
	busA := abxbus.NewEventBus("LockManagerGlobalEventModesBusA", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyParallel,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	busB := abxbus.NewEventBus("LockManagerGlobalEventModesBusB", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyParallel,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	registerActiveLockManagerHandler(busA, "LockModesEvent", "handler_a", &active, &maxActive)
	registerActiveLockManagerHandler(busB, "LockModesEvent", "handler_b", &active, &maxActive)
	globalA := abxbus.NewBaseEvent("LockModesEvent", nil)
	globalA.EventConcurrency = abxbus.EventConcurrencyGlobalSerial
	globalB := abxbus.NewBaseEvent("LockModesEvent", nil)
	globalB.EventConcurrency = abxbus.EventConcurrencyGlobalSerial
	busA.Emit(globalA)
	busB.Emit(globalB)
	if !busA.WaitUntilIdle(&timeout) {
		t.Fatal("global serial bus A did not become idle")
	}
	if !busB.WaitUntilIdle(&timeout) {
		t.Fatal("global serial bus B did not become idle")
	}
	if maxActive.Load() != 1 {
		t.Fatalf("expected global-serial events to serialize across buses, got max active %d", maxActive.Load())
	}
	busA.Destroy()
	busB.Destroy()
}

func TestLockManagerGetLockForEventHandlerModes(t *testing.T) {
	var active atomic.Int32
	var maxActive atomic.Int32
	timeout := 1.0

	serialBus := abxbus.NewEventBus("LockManagerHandlerModesSerialBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	registerActiveLockManagerHandler(serialBus, "LockHandlerModesEvent", "handler_a", &active, &maxActive)
	registerActiveLockManagerHandler(serialBus, "LockHandlerModesEvent", "handler_b", &active, &maxActive)
	serialBus.EmitEventName("LockHandlerModesEvent", nil)
	if !serialBus.WaitUntilIdle(&timeout) {
		t.Fatal("serial handler bus did not become idle")
	}
	if maxActive.Load() != 1 {
		t.Fatalf("expected serial handlers to serialize, got max active %d", maxActive.Load())
	}
	serialBus.Destroy()

	active.Store(0)
	maxActive.Store(0)
	parallelOverrideBus := abxbus.NewEventBus("LockManagerHandlerModesParallelOverrideBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	registerActiveLockManagerHandler(parallelOverrideBus, "LockHandlerModesEvent", "handler_a", &active, &maxActive)
	registerActiveLockManagerHandler(parallelOverrideBus, "LockHandlerModesEvent", "handler_b", &active, &maxActive)
	event := abxbus.NewBaseEvent("LockHandlerModesEvent", nil)
	event.EventHandlerConcurrency = abxbus.EventHandlerConcurrencyParallel
	parallelOverrideBus.Emit(event)
	if !parallelOverrideBus.WaitUntilIdle(&timeout) {
		t.Fatal("parallel handler override bus did not become idle")
	}
	if maxActive.Load() != 2 {
		t.Fatalf("expected parallel handler override to bypass handler lock, got max active %d", maxActive.Load())
	}
	parallelOverrideBus.Destroy()
}

func TestRunWithEventLockAndHandlerLockRespectParallelBypass(t *testing.T) {
	var active atomic.Int32
	var maxActive atomic.Int32
	timeout := 1.0

	parallelOverrideBus := abxbus.NewEventBus("LockManagerBypassBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	registerActiveLockManagerHandler(parallelOverrideBus, "ParallelBypassEvent", "handler_a", &active, &maxActive)
	registerActiveLockManagerHandler(parallelOverrideBus, "ParallelBypassEvent", "handler_b", &active, &maxActive)
	first := abxbus.NewBaseEvent("ParallelBypassEvent", nil)
	first.EventConcurrency = abxbus.EventConcurrencyParallel
	first.EventHandlerConcurrency = abxbus.EventHandlerConcurrencyParallel
	second := abxbus.NewBaseEvent("ParallelBypassEvent", nil)
	second.EventConcurrency = abxbus.EventConcurrencyParallel
	second.EventHandlerConcurrency = abxbus.EventHandlerConcurrencyParallel
	parallelOverrideBus.Emit(first)
	parallelOverrideBus.Emit(second)
	if !parallelOverrideBus.WaitUntilIdle(&timeout) {
		t.Fatal("parallel bypass bus did not become idle")
	}
	if maxActive.Load() != 4 {
		t.Fatalf("expected parallel event and handler locks to bypass, got max active %d", maxActive.Load())
	}
	parallelOverrideBus.Destroy()

	active.Store(0)
	maxActive.Store(0)
	serialBus := abxbus.NewEventBus("LockManagerSerialAcquireBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	registerActiveLockManagerHandler(serialBus, "SerialAcquireEvent", "handler_a", &active, &maxActive)
	registerActiveLockManagerHandler(serialBus, "SerialAcquireEvent", "handler_b", &active, &maxActive)
	serialBus.EmitEventName("SerialAcquireEvent", nil)
	serialBus.EmitEventName("SerialAcquireEvent", nil)
	if !serialBus.WaitUntilIdle(&timeout) {
		t.Fatal("serial acquire bus did not become idle")
	}
	if maxActive.Load() != 1 {
		t.Fatalf("expected serial event and handler locks to serialize, got max active %d", maxActive.Load())
	}
	serialBus.Destroy()
}
