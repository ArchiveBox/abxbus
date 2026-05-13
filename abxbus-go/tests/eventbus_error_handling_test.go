package abxbus_test

import (
	"context"
	"errors"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"
)

func TestEventResultPropagatesHandlerError(t *testing.T) {
	bus := abxbus.NewEventBus("ErrBus", nil)
	bus.OnEventName("ErrEvent", "boom", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, errors.New("boom")
	}, nil)
	e := bus.EmitEventName("ErrEvent", nil)
	if _, err := e.Now(); err != nil {
		t.Fatal(err)
	}
	_, err := e.EventResult()
	if err == nil || err.Error() != "boom" {
		t.Fatalf("expected boom error, got %v", err)
	}
}

func TestNowRaiseIfAnyOptions(t *testing.T) {
	bus := abxbus.NewEventBus("NowRaiseIfAnyBus", nil)
	bus.OnEventName("NowErrorEvent", "boom", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, errors.New("boom")
	}, nil)

	event := bus.EmitEventName("NowErrorEvent", nil)
	if _, err := event.Now(); err != nil {
		t.Fatalf("Now should wait for completion without surfacing handler errors, got %v", err)
	}
	if event.EventStatus != "completed" {
		t.Fatalf("event should be completed despite handler error, got %s", event.EventStatus)
	}
	if _, err := event.Now(); err != nil {
		t.Fatalf("RaiseIfAny=false should only wait for completion, got %v", err)
	}
}

func TestEventCompletesWhenOneHandlerErrorsAndAnotherSucceeds(t *testing.T) {
	bus := abxbus.NewEventBus("ErrMixedBus", &abxbus.EventBusOptions{EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel})
	bus.OnEventName("MixedEvent", "ok", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, nil)
	bus.OnEventName("MixedEvent", "boom", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, errors.New("boom")
	}, nil)
	e := bus.EmitEventName("MixedEvent", nil)
	if _, err := e.Now(); err != nil {
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
			if r.Status != abxbus.EventResultError {
				t.Fatalf("expected boom handler to error, got %s", r.Status)
			}
			if r.Error != "boom" {
				t.Fatalf("expected boom error value, got %#v", r.Error)
			}
		case "ok":
			seenSuccess = true
			if r.Status != abxbus.EventResultCompleted {
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

func TestSerialHandlerErrorDoesNotPreventLaterHandlers(t *testing.T) {
	bus := abxbus.NewEventBus("SerialErrorIsolationBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
	})
	calls := []string{}
	bus.OnEventName("MixedEvent", "failing", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		calls = append(calls, "failing")
		return nil, errors.New("expected failure")
	}, nil)
	bus.OnEventName("MixedEvent", "working", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		calls = append(calls, "working")
		return "worked", nil
	}, nil)

	event := bus.EmitEventName("MixedEvent", nil)
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	if event.EventStatus != "completed" {
		t.Fatalf("event should complete despite handler error, got %s", event.EventStatus)
	}
	if len(calls) != 2 || calls[0] != "failing" || calls[1] != "working" {
		t.Fatalf("serial handlers should continue after an error, got calls=%v", calls)
	}

	seenError := false
	seenSuccess := false
	for _, result := range event.EventResults {
		switch result.HandlerName {
		case "failing":
			seenError = result.Status == abxbus.EventResultError && result.Error == "expected failure"
		case "working":
			seenSuccess = result.Status == abxbus.EventResultCompleted && result.Result == "worked"
		}
	}
	if !seenError || !seenSuccess {
		t.Fatalf("expected one captured error and one successful result, got %#v", event.EventResults)
	}
}
