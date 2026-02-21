package bubus_test

import (
	"context"
	"testing"
	"time"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestTimeoutPrecedenceEventOverBus(t *testing.T) {
	busTimeout := 5.0
	eventTimeout := 0.01
	bus := bubus.NewEventBus("TimeoutBus", &bubus.EventBusOptions{EventTimeout: &busTimeout})
	bus.On("Evt", "slow", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, nil)
	e := bubus.NewBaseEvent("Evt", nil)
	e.EventTimeout = &eventTimeout
	_, err := bus.Emit(e).EventResult(context.Background())
	if err == nil {
		t.Fatal("expected timeout")
	}
}

func TestNilTimeoutAllowsSlowHandler(t *testing.T) {
	bus := bubus.NewEventBus("NoTimeoutBus", &bubus.EventBusOptions{EventTimeout: nil})
	bus.EventTimeout = nil
	bus.On("Evt", "slow", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		time.Sleep(20 * time.Millisecond)
		return "ok", nil
	}, nil)
	result, err := bus.Emit(bubus.NewBaseEvent("Evt", nil)).EventResult(context.Background())
	if err != nil || result != "ok" {
		t.Fatalf("expected ok, got %#v err=%v", result, err)
	}
}
