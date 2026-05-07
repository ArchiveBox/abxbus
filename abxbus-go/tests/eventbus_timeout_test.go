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

func TestHandlerTimeoutResolutionMatchesPrecedence(t *testing.T) {
	busTimeout := 0.2
	eventHandlerTimeout := 0.05
	handlerTimeout := 0.12
	detectPaths := false
	bus := abxbus.NewEventBus("TimeoutPrecedenceBus", &abxbus.EventBusOptions{
		EventTimeout:                &busTimeout,
		EventHandlerConcurrency:     abxbus.EventHandlerConcurrencyParallel,
		EventHandlerCompletion:      abxbus.EventHandlerCompletionAll,
		EventHandlerDetectFilePaths: &detectPaths,
	})
	bus.On("TimeoutDefaultsEvent", "default_handler", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		sleepFor := 80 * time.Millisecond
		if e.Payload["scenario"] == "event-cap" {
			sleepFor = 150 * time.Millisecond
		}
		select {
		case <-time.After(sleepFor):
			return "default", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	bus.On("TimeoutDefaultsEvent", "overridden_handler", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		sleepFor := 80 * time.Millisecond
		if e.Payload["scenario"] == "event-cap" {
			sleepFor = 150 * time.Millisecond
		}
		select {
		case <-time.After(sleepFor):
			return "override", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, &abxbus.EventHandler{HandlerTimeout: &handlerTimeout})

	event := abxbus.NewBaseEvent("TimeoutDefaultsEvent", nil)
	event.EventTimeout = &busTimeout
	event.EventHandlerTimeout = &eventHandlerTimeout
	event = bus.Emit(event)
	_, _ = event.Done(context.Background())

	var defaultResult *abxbus.EventResult
	var overriddenResult *abxbus.EventResult
	for _, result := range event.EventResults {
		switch result.HandlerName {
		case "default_handler":
			defaultResult = result
		case "overridden_handler":
			overriddenResult = result
		}
	}
	if defaultResult == nil || overriddenResult == nil {
		t.Fatalf("missing expected handler results: %#v", event.EventResults)
	}
	if defaultResult.Status != abxbus.EventResultError {
		t.Fatalf("default handler should use event_handler_timeout and time out, got %s result=%#v", defaultResult.Status, defaultResult.Result)
	}
	if overriddenResult.Status != abxbus.EventResultCompleted || overriddenResult.Result != "override" {
		t.Fatalf("handler override should beat event_handler_timeout and complete, status=%s result=%#v error=%#v", overriddenResult.Status, overriddenResult.Result, overriddenResult.Error)
	}

	tighterEventTimeout := 0.08
	longEventHandlerTimeout := 0.2
	tighter := abxbus.NewBaseEvent("TimeoutDefaultsEvent", map[string]any{"scenario": "event-cap"})
	tighter.EventTimeout = &tighterEventTimeout
	tighter.EventHandlerTimeout = &longEventHandlerTimeout
	tighter = bus.Emit(tighter)
	_, _ = tighter.Done(context.Background())
	for _, result := range tighter.EventResults {
		if result.Status != abxbus.EventResultError {
			t.Fatalf("event timeout should cap every handler timeout, got %s for %s", result.Status, result.HandlerName)
		}
	}
}

func TestEventHandlerDetectFilePathsToggle(t *testing.T) {
	detectPaths := false
	bus := abxbus.NewEventBus("NoDetectPathsBus", &abxbus.EventBusOptions{EventHandlerDetectFilePaths: &detectPaths})
	entry := bus.On("TimeoutDefaultsEvent", "handler", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, nil)
	if entry.HandlerFilePath != nil {
		t.Fatalf("expected nil handler_file_path when detection disabled, got %s", *entry.HandlerFilePath)
	}
}
