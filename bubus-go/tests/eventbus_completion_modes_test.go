package bubus_test

import (
	"context"
	"testing"
	"time"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestCompletionModeAllWaitsForAllHandlers(t *testing.T) {
	bus := bubus.NewEventBus("AllModeBus", &bubus.EventBusOptions{
		EventHandlerCompletion:  bubus.EventHandlerCompletionAll,
		EventHandlerConcurrency: bubus.EventHandlerConcurrencyParallel,
	})
	slow_done := false
	bus.On("Evt", "fast", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return "fast", nil }, nil)
	bus.On("Evt", "slow", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		time.Sleep(30 * time.Millisecond)
		slow_done = true
		return "slow", nil
	}, nil)
	e := bus.Emit(bubus.NewBaseEvent("Evt", nil))
	_, err := e.Done(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !slow_done {
		t.Fatal("completion=all should wait for slow handler")
	}
}

func TestCompletionModeFirstSerialStopsAfterFirstNonNil(t *testing.T) {
	bus := bubus.NewEventBus("FirstSerialBus", &bubus.EventBusOptions{
		EventHandlerCompletion:  bubus.EventHandlerCompletionFirst,
		EventHandlerConcurrency: bubus.EventHandlerConcurrencySerial,
	})
	second_called := false
	bus.On("Evt", "first", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return "first", nil }, nil)
	bus.On("Evt", "second", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		second_called = true
		return "second", nil
	}, nil)
	result, err := bus.Emit(bubus.NewBaseEvent("Evt", nil)).First(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result != "first" {
		t.Fatalf("expected first result, got %#v", result)
	}
	if second_called {
		t.Fatal("serial first mode should not call second handler after non-nil first result")
	}
}
