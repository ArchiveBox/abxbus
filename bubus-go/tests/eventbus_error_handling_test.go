package bubus_test

import (
	"context"
	"errors"
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestEventResultPropagatesHandlerError(t *testing.T) {
	bus := bubus.NewEventBus("ErrBus", nil)
	bus.On("ErrEvent", "boom", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		return nil, errors.New("boom")
	}, nil)
	e := bus.Emit(bubus.NewBaseEvent("ErrEvent", nil))
	_, err := e.EventResult(context.Background())
	if err == nil || err.Error() != "boom" {
		t.Fatalf("expected boom error, got %v", err)
	}
}

func TestEventCompletesWhenOneHandlerErrorsAndAnotherSucceeds(t *testing.T) {
	bus := bubus.NewEventBus("ErrMixedBus", &bubus.EventBusOptions{EventHandlerConcurrency: bubus.EventHandlerConcurrencyParallel})
	bus.On("MixedEvent", "ok", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		return "ok", nil
	}, nil)
	bus.On("MixedEvent", "boom", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		return nil, errors.New("boom")
	}, nil)
	e := bus.Emit(bubus.NewBaseEvent("MixedEvent", nil))
	_, _ = e.Done(context.Background())
	if e.EventStatus != "completed" {
		t.Fatalf("event should be completed despite handler error, got %s", e.EventStatus)
	}
	if len(e.EventResults) != 2 {
		t.Fatalf("expected 2 event results, got %d", len(e.EventResults))
	}
	seen_error := false
	seen_success := false
	for _, r := range e.EventResults {
		if r.Status == bubus.EventResultError {
			seen_error = true
		}
		if r.Status == bubus.EventResultCompleted {
			seen_success = true
		}
	}
	if !seen_error || !seen_success {
		t.Fatalf("expected both success and error results, got success=%v error=%v", seen_success, seen_error)
	}
}
