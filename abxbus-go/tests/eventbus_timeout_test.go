package abxbus_test

import (
	"context"
	"strings"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestTimeoutPrecedenceEventOverBus(t *testing.T) {
	busTimeout := 5.0
	eventTimeout := 0.01
	bus := abxbus.NewEventBus("TimeoutBus", &abxbus.EventBusOptions{EventTimeout: &busTimeout})
	bus.On("Evt", "slow", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, nil)
	e := abxbus.NewBaseEvent("Evt", nil)
	e.EventTimeout = &eventTimeout

	started := time.Now()
	_, err := bus.Emit(e).EventResult(context.Background())
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("expected timeout")
	}
	if elapsed > time.Second {
		t.Fatalf("expected event timeout (~10ms) to win over bus timeout (5s), elapsed=%s", elapsed)
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected timeout error message, got %v", err)
	}
}

func TestNilTimeoutAllowsSlowHandler(t *testing.T) {
	bus := abxbus.NewEventBus("NoTimeoutBus", &abxbus.EventBusOptions{EventTimeout: nil})
	bus.EventTimeout = nil
	bus.On("Evt", "slow", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		time.Sleep(20 * time.Millisecond)
		return "ok", nil
	}, nil)
	result, err := bus.Emit(abxbus.NewBaseEvent("Evt", nil)).EventResult(context.Background())
	if err != nil || result != "ok" {
		t.Fatalf("expected ok, got %#v err=%v", result, err)
	}
}
