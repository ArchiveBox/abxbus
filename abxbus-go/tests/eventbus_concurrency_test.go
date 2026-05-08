package abxbus_test

import (
	"context"
	"sync"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestHandlerConcurrencyParallelStartsBoth(t *testing.T) {
	bus := abxbus.NewEventBus("ParallelBus", &abxbus.EventBusOptions{EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel})
	var mu sync.Mutex
	count := 0
	gate := make(chan struct{})
	started := make(chan struct{}, 2)

	for i := 0; i < 2; i++ {
		bus.On("Evt", "h", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
			mu.Lock()
			count++
			mu.Unlock()
			started <- struct{}{}
			<-gate
			return nil, nil
		}, nil)
	}

	e := bus.Emit(abxbus.NewBaseEvent("Evt", nil))
	deadline := time.After(2 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-deadline:
			close(gate)
			t.Fatalf("timed out waiting for parallel handlers to start")
		}
	}

	mu.Lock()
	c := count
	mu.Unlock()
	if c != 2 {
		close(gate)
		t.Fatalf("expected 2 starts, got %d", c)
	}

	close(gate)
	if _, err := e.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestEventHandlerConcurrencyPerEventOverrideControlsExecutionMode(t *testing.T) {
	bus := abxbus.NewEventBus("HandlerConcurrencyOverrideBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	var mu sync.Mutex
	inFlight := 0
	maxInFlightByMode := map[string]int{}
	release := make(chan struct{})
	started := make(chan string, 4)

	handler := func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		mode := e.Payload["mode"].(string)
		mu.Lock()
		inFlight++
		if inFlight > maxInFlightByMode[mode] {
			maxInFlightByMode[mode] = inFlight
		}
		mu.Unlock()
		started <- mode
		<-release
		mu.Lock()
		inFlight--
		mu.Unlock()
		return mode, nil
	}
	bus.On("Evt", "first", handler, nil)
	bus.On("Evt", "second", handler, nil)

	parallelEvent := abxbus.NewBaseEvent("Evt", map[string]any{"mode": "parallel"})
	parallelEvent.EventHandlerConcurrency = abxbus.EventHandlerConcurrencyParallel
	emittedParallel := bus.Emit(parallelEvent)
	for i := 0; i < 2; i++ {
		select {
		case mode := <-started:
			if mode != "parallel" {
				close(release)
				t.Fatalf("unexpected mode %s", mode)
			}
		case <-time.After(2 * time.Second):
			close(release)
			t.Fatal("timed out waiting for parallel handlers")
		}
	}

	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := emittedParallel.Done(ctx); err != nil {
		t.Fatal(err)
	}
	if maxInFlightByMode["parallel"] < 2 {
		t.Fatalf("expected per-event parallel override to run handlers concurrently, max=%d", maxInFlightByMode["parallel"])
	}

	release = make(chan struct{})
	started = make(chan string, 4)
	inFlight = 0
	serialEvent := abxbus.NewBaseEvent("Evt", map[string]any{"mode": "serial"})
	serialEvent.EventHandlerConcurrency = abxbus.EventHandlerConcurrencySerial
	emittedSerial := bus.Emit(serialEvent)
	select {
	case mode := <-started:
		if mode != "serial" {
			close(release)
			t.Fatalf("unexpected mode %s", mode)
		}
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("timed out waiting for serial handler")
	}
	select {
	case <-started:
		close(release)
		t.Fatal("second serial handler should not start before first is released")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if _, err := emittedSerial.Done(ctx); err != nil {
		t.Fatal(err)
	}
	if maxInFlightByMode["serial"] != 1 {
		t.Fatalf("expected per-event serial override to keep max in-flight at 1, got %d", maxInFlightByMode["serial"])
	}
}
