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
	if _, err := e.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if e.EventStatus != "completed" {
		t.Fatalf("event should be completed despite handler error, got %s", e.EventStatus)
	}
	if len(e.EventResults) != 2 {
		t.Fatalf("expected 2 event results, got %d", len(e.EventResults))
	}

	seenError := false
	seenSuccess := false
	for _, r := range e.EventResults {
		switch r.HandlerName {
		case "boom":
			seenError = true
			if r.Status != bubus.EventResultError {
				t.Fatalf("expected boom handler to error, got %s", r.Status)
			}
			if r.Error != "boom" {
				t.Fatalf("expected boom error value, got %#v", r.Error)
			}
		case "ok":
			seenSuccess = true
			if r.Status != bubus.EventResultCompleted {
				t.Fatalf("expected ok handler to complete, got %s", r.Status)
			}
			if r.Result != "ok" {
				t.Fatalf("expected ok result value, got %#v", r.Result)
			}
		}
	}
	if !seenError || !seenSuccess {
		t.Fatalf("expected both success and error results, got success=%v error=%v", seenSuccess, seenError)
	}
}
