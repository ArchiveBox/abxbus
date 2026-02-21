package bubus_test

import (
	"context"
	"testing"
	"time"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestFirstReturnsFastResult(t *testing.T) {
	bus := bubus.NewEventBus("FirstBus", &bubus.EventBusOptions{EventHandlerCompletion: bubus.EventHandlerCompletionFirst, EventHandlerConcurrency: bubus.EventHandlerConcurrencyParallel})
	bus.On("Evt", "slow", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		select {
		case <-time.After(200 * time.Millisecond):
			return "slow", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	bus.On("Evt", "fast", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return "fast", nil }, nil)
	result, err := bus.Emit(bubus.NewBaseEvent("Evt", nil)).First(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result != "fast" {
		t.Fatalf("expected fast, got %#v", result)
	}
}
