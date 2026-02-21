package bubus_test

import (
	"context"
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestWaitUntilIdle(t *testing.T) {
	bus := bubus.NewEventBus("IdleBus", nil)
	bus.On("Evt", "h", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return "ok", nil }, nil)
	_ = bus.Emit(bubus.NewBaseEvent("Evt", nil))
	timeout := 1.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("expected idle true")
	}
}
