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
	if !strings.Contains(err.Error(), "timed out") && !strings.Contains(err.Error(), "event timeout") {
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

func TestForwardedEventTimeoutAbortsTargetBusHandler(t *testing.T) {
	busA := abxbus.NewEventBus("TimeoutForwardA", &abxbus.EventBusOptions{EventConcurrency: abxbus.EventConcurrencyBusSerial})
	busB := abxbus.NewEventBus("TimeoutForwardB", &abxbus.EventBusOptions{EventConcurrency: abxbus.EventConcurrencyBusSerial})
	t.Cleanup(busA.Destroy)
	t.Cleanup(busB.Destroy)

	busA.On("TimeoutForwardEvent", "forward", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return busB.Emit(event), nil
	}, nil)
	busB.On("TimeoutForwardEvent", "slow_target", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		select {
		case <-time.After(50 * time.Millisecond):
			return "slow", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)

	eventTimeout := 0.01
	event := abxbus.NewBaseEvent("TimeoutForwardEvent", nil)
	event.EventTimeout = &eventTimeout
	if _, err := busA.Emit(event).Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	var targetResult *abxbus.EventResult
	for _, result := range event.EventResults {
		if result.EventBusID == busB.ID {
			targetResult = result
			break
		}
	}
	if targetResult == nil {
		t.Fatalf("missing target bus result in %#v", event.EventResults)
	}
	if targetResult.Status != abxbus.EventResultError {
		t.Fatalf("target bus handler should be aborted by event timeout, got status=%s result=%#v error=%#v", targetResult.Status, targetResult.Result, targetResult.Error)
	}
	errorText, _ := targetResult.Error.(string)
	if !strings.Contains(errorText, "Aborted running handler") && !strings.Contains(errorText, "timed out") {
		t.Fatalf("target timeout error should describe abort/timeout, got %#v", targetResult.Error)
	}
	if len(event.EventPath) != 2 || event.EventPath[0] != busA.Label() || event.EventPath[1] != busB.Label() {
		t.Fatalf("forwarded timeout event_path mismatch: %v", event.EventPath)
	}
}

func TestQueueJumpAwaitedChildTimeoutAbortsAcrossBuses(t *testing.T) {
	busA := abxbus.NewEventBus("TimeoutQueueJumpA", &abxbus.EventBusOptions{EventConcurrency: abxbus.EventConcurrencyGlobalSerial})
	busB := abxbus.NewEventBus("TimeoutQueueJumpB", &abxbus.EventBusOptions{EventConcurrency: abxbus.EventConcurrencyGlobalSerial})
	t.Cleanup(busA.Destroy)
	t.Cleanup(busB.Destroy)

	var childRef *abxbus.BaseEvent
	busB.On("TimeoutChildEvent", "slow_child", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		select {
		case <-time.After(50 * time.Millisecond):
			return "slow", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	busA.On("TimeoutParentEvent", "parent", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		childTimeout := 0.01
		child := event.Emit(abxbus.NewBaseEvent("TimeoutChildEvent", nil))
		child.EventTimeout = &childTimeout
		busB.Emit(child)
		childRef = child
		if _, err := child.Done(ctx); err != nil {
			return nil, err
		}
		return "parent", nil
	}, nil)

	parentTimeout := 0.5
	parent := abxbus.NewBaseEvent("TimeoutParentEvent", nil)
	parent.EventTimeout = &parentTimeout
	if _, err := busA.Emit(parent).Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if childRef == nil {
		t.Fatal("parent handler did not capture child event")
	}
	foundTimeoutError := false
	for _, result := range childRef.EventResults {
		if result.Status != abxbus.EventResultError {
			continue
		}
		errorText, _ := result.Error.(string)
		if strings.Contains(errorText, "Aborted running handler") || strings.Contains(errorText, "timed out") {
			foundTimeoutError = true
		}
	}
	if !foundTimeoutError {
		t.Fatalf("expected child timeout/abort result across buses, got %#v", childRef.EventResults)
	}
}

func TestForwardedTimeoutPathDoesNotStallFollowupEvents(t *testing.T) {
	busA := abxbus.NewEventBus("TimeoutForwardRecoveryA", nil)
	busB := abxbus.NewEventBus("TimeoutForwardRecoveryB", nil)
	t.Cleanup(busA.Destroy)
	t.Cleanup(busB.Destroy)

	busATailRuns := 0
	busBTailRuns := 0
	var childRef *abxbus.BaseEvent

	busA.On("TimeoutRecoveryParentEvent", "parent", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		childTimeout := 0.01
		child := event.Emit(abxbus.NewBaseEvent("TimeoutRecoveryChildEvent", nil))
		child.EventTimeout = &childTimeout
		childRef = child
		if _, err := child.Done(ctx); err != nil {
			return nil, err
		}
		return "parent_done", nil
	}, nil)
	busA.On("TimeoutRecoveryTailEvent", "tail_a", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		busATailRuns++
		return "tail_a", nil
	}, nil)
	busA.On("*", "forward_to_b", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return busB.Emit(event), nil
	}, nil)
	busB.On("TimeoutRecoveryChildEvent", "slow_child", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		select {
		case <-time.After(50 * time.Millisecond):
			return "child_done", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	busB.On("TimeoutRecoveryTailEvent", "tail_b", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		busBTailRuns++
		return "tail_b", nil
	}, nil)

	parentTimeout := 1.0
	parent := abxbus.NewBaseEvent("TimeoutRecoveryParentEvent", nil)
	parent.EventTimeout = &parentTimeout
	if _, err := busA.Emit(parent).Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if childRef == nil {
		t.Fatal("parent handler did not emit child")
	}
	parentResult := firstEventResult(parent)
	if parentResult == nil || parentResult.Status != abxbus.EventResultCompleted {
		t.Fatalf("parent should complete after awaited child event completion, got %#v", parentResult)
	}
	foundChildTimeoutError := false
	for _, result := range childRef.EventResults {
		if result.Status != abxbus.EventResultError {
			continue
		}
		errorText, _ := result.Error.(string)
		if strings.Contains(errorText, "Aborted running handler") || strings.Contains(errorText, "timed out") {
			foundChildTimeoutError = true
		}
	}
	if !foundChildTimeoutError {
		t.Fatalf("expected child timeout/abort result, got %#v", childRef.EventResults)
	}

	tail := busA.Emit(abxbus.NewBaseEvent("TimeoutRecoveryTailEvent", nil))
	if _, err := tail.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	to := 2.0
	if !busA.WaitUntilIdle(&to) {
		t.Fatal("source bus did not become idle after forwarded timeout")
	}
	if !busB.WaitUntilIdle(&to) {
		t.Fatal("target bus did not become idle after forwarded timeout")
	}
	if tail.EventStatus != "completed" || busATailRuns != 1 || busBTailRuns != 1 {
		t.Fatalf("follow-up tail did not run on both buses: status=%s busA=%d busB=%d", tail.EventStatus, busATailRuns, busBTailRuns)
	}
}
