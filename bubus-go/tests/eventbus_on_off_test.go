package bubus_test

import (
	"context"
	"sync/atomic"
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestOnOffAndWildcard(t *testing.T) {
	bus := bubus.NewEventBus("OnOffBus", nil)
	var count atomic.Int32
	h := bus.On("Evt", "h", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { count.Add(1); return nil, nil }, nil)
	bus.On("*", "all", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { count.Add(1); return nil, nil }, nil)
	_, _ = bus.Emit(bubus.NewBaseEvent("Evt", nil)).Done(context.Background())
	if count.Load() != 2 {
		t.Fatalf("expected 2 calls, got %d", count.Load())
	}
	bus.Off("Evt", h)
	_, _ = bus.Emit(bubus.NewBaseEvent("Evt", nil)).Done(context.Background())
	if count.Load() != 3 {
		t.Fatalf("expected wildcard-only call, got %d", count.Load())
	}
}
