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
	slowDone := false
	bus.On("Evt", "fast", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return "fast", nil }, nil)
	bus.On("Evt", "slow", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		time.Sleep(30 * time.Millisecond)
		slowDone = true
		return "slow", nil
	}, nil)
	e := bus.Emit(bubus.NewBaseEvent("Evt", nil))
	if _, err := e.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slowDone {
		t.Fatal("completion=all should wait for slow handler")
	}
}

func TestCompletionModeFirstSerialStopsAfterFirstNonNil(t *testing.T) {
	bus := bubus.NewEventBus("FirstSerialBus", &bubus.EventBusOptions{
		EventHandlerCompletion:  bubus.EventHandlerCompletionFirst,
		EventHandlerConcurrency: bubus.EventHandlerConcurrencySerial,
	})
	secondCalled := false
	bus.On("Evt", "first", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return "first", nil }, nil)
	bus.On("Evt", "second", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		secondCalled = true
		return "second", nil
	}, nil)
	e := bus.Emit(bubus.NewBaseEvent("Evt", nil))
	result, err := e.First(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result != "first" {
		t.Fatalf("expected first result, got %#v", result)
	}
	if secondCalled {
		t.Fatal("serial first mode should not call second handler after non-nil first result")
	}
	for _, eventResult := range e.EventResults {
		if eventResult.Status == bubus.EventResultPending {
			t.Fatal("serial first mode should not leave skipped handlers in pending state")
		}
	}
}

func TestCompletionModeFirstParallelReturnsFastAndCancelsSlow(t *testing.T) {
	bus := bubus.NewEventBus("FirstParallelBus", &bubus.EventBusOptions{
		EventHandlerCompletion:  bubus.EventHandlerCompletionFirst,
		EventHandlerConcurrency: bubus.EventHandlerConcurrencyParallel,
	})
	slowStarted := make(chan struct{}, 1)
	slowExited := make(chan struct{}, 1)
	bus.On("Evt", "slow", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		slowStarted <- struct{}{}
		select {
		case <-time.After(500 * time.Millisecond):
			slowExited <- struct{}{}
			return "slow", nil
		case <-ctx.Done():
			slowExited <- struct{}{}
			return nil, ctx.Err()
		}
	}, nil)
	bus.On("Evt", "fast", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return "fast", nil }, nil)

	result, err := bus.Emit(bubus.NewBaseEvent("Evt", nil)).First(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result != "fast" {
		t.Fatalf("expected fast result, got %#v", result)
	}
	select {
	case <-slowStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("expected slow handler to start in parallel first-completion mode")
	}
	select {
	case <-slowExited:
	case <-time.After(2 * time.Second):
		t.Fatal("expected slow handler to exit (cancel or complete) after fast first result")
	}
}
