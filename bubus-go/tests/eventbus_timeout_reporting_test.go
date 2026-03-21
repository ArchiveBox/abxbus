package bubus_test

import (
	"context"
	"strings"
	"testing"
	"time"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestEventTimeoutMarksAbortedAndCancelledHandlers(t *testing.T) {
	event_timeout := 0.02
	bus := bubus.NewEventBus("TimeoutReportingBus", &bubus.EventBusOptions{
		EventTimeout:            nil,
		EventHandlerConcurrency: bubus.EventHandlerConcurrencySerial,
		EventHandlerCompletion:  bubus.EventHandlerCompletionAll,
		EventHandlerSlowTimeout: nil,
		EventSlowTimeout:        nil,
	})
	bus.EventTimeout = nil
	bus.On("Evt", "slow_first", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		select {
		case <-time.After(250 * time.Millisecond):
			return "late", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	bus.On("Evt", "pending_second", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		return "never", nil
	}, nil)

	e := bubus.NewBaseEvent("Evt", nil)
	e.EventTimeout = &event_timeout
	e = bus.Emit(e)
	_, _ = e.Done(context.Background())
	if len(e.EventResults) != 2 {
		t.Fatalf("expected 2 results, got %d", len(e.EventResults))
	}
	seen_aborted := false
	seen_cancelled := false
	errors_seen := []string{}
	for _, r := range e.EventResults {
		if r.Status != bubus.EventResultError {
			t.Fatalf("expected error status for all results, got %s", r.Status)
		}
		err_s := ""
		if s, ok := r.Error.(string); ok {
			err_s = s
		}
		errors_seen = append(errors_seen, err_s)
		if strings.Contains(err_s, "Aborted running handler") {
			seen_aborted = true
		}
		if strings.Contains(err_s, "Cancelled pending handler") {
			seen_cancelled = true
		}
	}
	if !seen_aborted || !seen_cancelled {
		t.Fatalf("expected aborted+cancelled error reporting, got aborted=%v cancelled=%v errors=%v", seen_aborted, seen_cancelled, errors_seen)
	}
}

func TestHandlerTimeoutUsesTimedOutErrorMessage(t *testing.T) {
	handler_timeout := 0.01
	bus_timeout := 5.0
	bus := bubus.NewEventBus("HandlerTimeoutMessageBus", &bubus.EventBusOptions{EventTimeout: &bus_timeout})
	overrides := &bubus.EventHandler{HandlerTimeout: &handler_timeout}
	bus.On("Evt", "slow", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		select {
		case <-time.After(200 * time.Millisecond):
			return "late", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, overrides)
	e := bus.Emit(bubus.NewBaseEvent("Evt", nil))
	_, err := e.EventResult(context.Background())
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected timeout message, got %v", err)
	}
}
