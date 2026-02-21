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
	for i := 0; i < 2; i++ {
		bus.On("Evt", "h", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
			mu.Lock()
			count++
			mu.Unlock()
			<-gate
			return nil, nil
		}, nil)
	}
	_ = bus.Emit(bubus.NewBaseEvent("Evt", nil))
	time.Sleep(15 * time.Millisecond)
	mu.Lock()
	c := count
	mu.Unlock()
	if c != 2 {
		close(gate)
		t.Fatalf("expected 2 starts, got %d", c)
	}
	close(gate)
}
