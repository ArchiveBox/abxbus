package bubus_test

import (
	"context"
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestDispatchAlias(t *testing.T) {
	bus := bubus.NewEventBus("DispatchBus", nil)
	bus.On("Evt", "h", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return "ok", nil }, nil)
	result, err := bus.Dispatch(bubus.NewBaseEvent("Evt", nil)).EventResult(context.Background())
	if err != nil || result != "ok" {
		t.Fatalf("bad dispatch result %#v err=%v", result, err)
	}
}
