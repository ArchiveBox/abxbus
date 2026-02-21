package bubus_test

import (
	"context"
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestDestroyClearsRuntimeState(t *testing.T) {
	bus := bubus.NewEventBus("DestroyBus", nil)
	bus.On("Evt", "h", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return nil, nil }, nil)
	_ = bus.Emit(bubus.NewBaseEvent("Evt", nil))
	timeout := 1.0
	_ = bus.WaitUntilIdle(&timeout)
	bus.Destroy()
	if bus.EventHistory.Size() != 0 {
		t.Fatal("expected empty history after destroy")
	}
	if !bus.IsIdleAndQueueEmpty() {
		t.Fatal("expected idle after destroy")
	}
}
