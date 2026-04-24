package bubus_test

import (
	"context"
	"sync"
	"testing"
	"time"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestHandlerConcurrencyParallelStartsBoth(t *testing.T) {
	bus := bubus.NewEventBus("ParallelBus", &bubus.EventBusOptions{EventHandlerConcurrency: bubus.EventHandlerConcurrencyParallel})
	var mu sync.Mutex
	count := 0
	gate := make(chan struct{})
	started := make(chan struct{}, 2)

	for i := 0; i < 2; i++ {
		bus.On("Evt", "h", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
			mu.Lock()
			count++
			mu.Unlock()
			started <- struct{}{}
			<-gate
			return nil, nil
		}, nil)
	}

	e := bus.Emit(bubus.NewBaseEvent("Evt", nil))
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
