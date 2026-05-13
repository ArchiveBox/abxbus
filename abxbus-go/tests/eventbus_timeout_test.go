package abxbus_test

import (
	"context"
	"strings"
	"sync"
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
	_, err := bus.Emit(e).EventResult()
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

func TestZeroTimeoutAllowsSlowHandler(t *testing.T) {
	busTimeout := 0.01
	bus := abxbus.NewEventBus("NoTimeoutBus", &abxbus.EventBusOptions{EventTimeout: &busTimeout})
	bus.On("Evt", "slow", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		time.Sleep(20 * time.Millisecond)
		return "ok", nil
	}, nil)
	event := abxbus.NewBaseEvent("Evt", nil)
	noTimeout := 0.0
	event.EventTimeout = &noTimeout
	result, err := bus.Emit(event).EventResult()
	if err != nil || result != "ok" {
		t.Fatalf("expected ok, got %#v err=%v", result, err)
	}
}

func TestProcessingTimeTimeoutDefaultsResolveAtExecutionTime(t *testing.T) {
	eventTimeout := 12.0
	eventSlowTimeout := 34.0
	eventHandlerSlowTimeout := 56.0
	bus := abxbus.NewEventBus("TimeoutDefaultsResolveBus", &abxbus.EventBusOptions{
		EventTimeout:            &eventTimeout,
		EventSlowTimeout:        &eventSlowTimeout,
		EventHandlerSlowTimeout: &eventHandlerSlowTimeout,
	})
	t.Cleanup(bus.Destroy)

	bus.On("TimeoutEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("TimeoutEvent", nil))
	if event.EventTimeout != nil {
		t.Fatalf("expected nil event_timeout, got %#v", event.EventTimeout)
	}
	if event.EventHandlerTimeout != nil {
		t.Fatalf("expected nil event_handler_timeout, got %#v", event.EventHandlerTimeout)
	}
	if event.EventHandlerSlowTimeout != nil {
		t.Fatalf("expected nil event_handler_slow_timeout, got %#v", event.EventHandlerSlowTimeout)
	}
	if event.EventSlowTimeout != nil {
		t.Fatalf("expected nil event_slow_timeout, got %#v", event.EventSlowTimeout)
	}
	if event.EventConcurrency != "" {
		t.Fatalf("expected empty event_concurrency, got %s", event.EventConcurrency)
	}
	if event.EventHandlerConcurrency != "" {
		t.Fatalf("expected empty event_handler_concurrency, got %s", event.EventHandlerConcurrency)
	}
	if event.EventHandlerCompletion != "" {
		t.Fatalf("expected empty event_handler_completion, got %s", event.EventHandlerCompletion)
	}

	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	if event.EventTimeout != nil || event.EventHandlerSlowTimeout != nil || event.EventSlowTimeout != nil ||
		event.EventConcurrency != "" || event.EventHandlerConcurrency != "" || event.EventHandlerCompletion != "" {
		t.Fatalf("bus defaults should not be written onto event after processing: %#v", event)
	}
	result := eventResultByHandlerName(event, "handler")
	if result == nil || result.HandlerTimeout == nil || *result.HandlerTimeout != eventTimeout {
		t.Fatalf("expected handler timeout %v, got %#v", eventTimeout, result)
	}
}

func TestEventTimeoutNilUsesBusDefaultTimeoutAtExecution(t *testing.T) {
	busTimeout := 0.01
	bus := abxbus.NewEventBus("TimeoutNilUsesBusDefault", &abxbus.EventBusOptions{EventTimeout: &busTimeout})
	t.Cleanup(bus.Destroy)

	bus.On("TimeoutDefaultsEvent", "slow", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		select {
		case <-time.After(20 * time.Millisecond):
			return "slow", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("TimeoutDefaultsEvent", nil))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	result := eventResultByHandlerName(event, "slow")
	if event.EventTimeout != nil {
		t.Fatalf("expected nil event_timeout, got %#v", event.EventTimeout)
	}
	if result == nil || result.Status != abxbus.EventResultError {
		t.Fatalf("expected timeout error result, got %#v", result)
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
	_, _ = event.Now()

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
	_, _ = tighter.Now()
	for _, result := range tighter.EventResults {
		if result.Status != abxbus.EventResultError {
			t.Fatalf("event timeout should cap every handler timeout, got %s for %s", result.Status, result.HandlerName)
		}
	}
}

func TestHandlerTimeoutIgnoresLateHandlerResultAndLateEmits(t *testing.T) {
	eventTimeout := 0.2
	handlerTimeout := 0.01
	bus := abxbus.NewEventBus("TimeoutIgnoresLateHandlerBus", &abxbus.EventBusOptions{
		EventTimeout: &eventTimeout,
	})
	t.Cleanup(bus.Destroy)

	lateAttempt := make(chan struct{}, 1)
	lateHandlerRan := false

	bus.On("TimeoutIgnoresLateHandlerEvent", "slow_handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		time.Sleep(40 * time.Millisecond)
		lateAttempt <- struct{}{}
		event.Emit(abxbus.NewBaseEvent("LateAfterTimeoutEvent", nil))
		return "late success", nil
	}, &abxbus.EventHandler{HandlerTimeout: &handlerTimeout})
	bus.On("LateAfterTimeoutEvent", "late_handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		lateHandlerRan = true
		return "late child", nil
	}, nil)

	event := abxbus.NewBaseEvent("TimeoutIgnoresLateHandlerEvent", nil)
	event.EventTimeout = &eventTimeout
	event.EventHandlerTimeout = &handlerTimeout
	event = bus.Emit(event)
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lateAttempt:
	case <-time.After(time.Second):
		t.Fatal("timed-out handler should still reach late emit attempt")
	}
	time.Sleep(30 * time.Millisecond)
	idleTimeout := 1.0
	_ = bus.WaitUntilIdle(&idleTimeout)

	slowResult := eventResultByHandlerName(event, "slow_handler")
	if slowResult == nil || slowResult.Status != abxbus.EventResultError || !timeoutResultErrorContains(slowResult, "timed out") {
		t.Fatalf("slow handler should keep timeout error and no late result, got %#v", slowResult)
	}
	if slowResult.Result != nil {
		t.Fatalf("slow handler late result should be ignored, got %#v", slowResult.Result)
	}
	found, err := bus.Find("LateAfterTimeoutEvent", nil, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if found != nil {
		t.Fatalf("late emit from timed-out handler should not be queued or recorded, got %#v", found)
	}
	if lateHandlerRan {
		t.Fatal("late handler should not run after source handler timed out")
	}
}

func TestEventTimeoutIgnoresLateHandlerResultAndLateEmits(t *testing.T) {
	eventTimeout := 0.01
	bus := abxbus.NewEventBus("TimeoutIgnoresLateEventBus", nil)
	t.Cleanup(bus.Destroy)

	lateAttempt := make(chan struct{}, 1)
	lateHandlerRan := false

	bus.On("TimeoutIgnoresLateEvent", "slow_handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		time.Sleep(40 * time.Millisecond)
		lateAttempt <- struct{}{}
		event.Emit(abxbus.NewBaseEvent("LateAfterEventTimeoutEvent", nil))
		return "late success", nil
	}, nil)
	bus.On("LateAfterEventTimeoutEvent", "late_handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		lateHandlerRan = true
		return "late child", nil
	}, nil)

	event := abxbus.NewBaseEvent("TimeoutIgnoresLateEvent", nil)
	event.EventTimeout = &eventTimeout
	event = bus.Emit(event)
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lateAttempt:
	case <-time.After(time.Second):
		t.Fatal("timed-out handler should still reach late emit attempt")
	}
	time.Sleep(30 * time.Millisecond)
	idleTimeout := 1.0
	_ = bus.WaitUntilIdle(&idleTimeout)

	slowResult := eventResultByHandlerName(event, "slow_handler")
	if slowResult == nil || slowResult.Status != abxbus.EventResultError || !timeoutResultErrorContains(slowResult, "Aborted running handler") {
		t.Fatalf("slow handler should keep event timeout abort and no late result, got %#v", slowResult)
	}
	if slowResult.Result != nil {
		t.Fatalf("slow handler late result should be ignored, got %#v", slowResult.Result)
	}
	found, err := bus.Find("LateAfterEventTimeoutEvent", nil, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if found != nil {
		t.Fatalf("late emit from event-timed-out handler should not be queued or recorded, got %#v", found)
	}
	if lateHandlerRan {
		t.Fatal("late handler should not run after source handler timed out")
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

func TestHandlerSlowWarningUsesEventHandlerSlowTimeout(t *testing.T) {
	eventTimeout := 0.5
	slowBusDefault := 0.5
	handlerSlowTimeout := 0.01
	bus := abxbus.NewEventBus("SlowHandlerWarnBus", &abxbus.EventBusOptions{
		EventTimeout:            &eventTimeout,
		EventSlowTimeout:        &slowBusDefault,
		EventHandlerSlowTimeout: &slowBusDefault,
	})
	t.Cleanup(bus.Destroy)

	var mu sync.Mutex
	messages := []string{}
	originalLogger := abxbus.SlowWarningLogger
	abxbus.SlowWarningLogger = func(message string) {
		mu.Lock()
		defer mu.Unlock()
		messages = append(messages, message)
	}
	defer func() { abxbus.SlowWarningLogger = originalLogger }()

	bus.On("TimeoutDefaultsEvent", "slow_handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		time.Sleep(30 * time.Millisecond)
		mu.Lock()
		messages = append(messages, "slow warning child handler finishing")
		mu.Unlock()
		return "ok", nil
	}, nil)

	event := abxbus.NewBaseEvent("TimeoutDefaultsEvent", nil)
	event.EventHandlerSlowTimeout = &handlerSlowTimeout
	if _, err := bus.Emit(event).Now(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	slowIndex := indexContains(messages, "Slow event handler")
	finishIndex := indexContains(messages, "slow warning child handler finishing")
	if slowIndex < 0 || finishIndex < 0 || slowIndex > finishIndex {
		t.Fatalf("slow handler warning should fire while handler is still running, got %#v", messages)
	}
	if indexContains(messages, "Slow event processing") >= 0 {
		t.Fatalf("handler-only slow warning should not also emit event warning, got %#v", messages)
	}
}

func TestEventSlowWarningUsesEventSlowTimeout(t *testing.T) {
	eventTimeout := 0.5
	slowBusDefault := 0.5
	eventSlowTimeout := 0.01
	bus := abxbus.NewEventBus("SlowEventWarnBus", &abxbus.EventBusOptions{
		EventTimeout:            &eventTimeout,
		EventSlowTimeout:        &slowBusDefault,
		EventHandlerSlowTimeout: &slowBusDefault,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
	})
	t.Cleanup(bus.Destroy)

	var mu sync.Mutex
	messages := []string{}
	originalLogger := abxbus.SlowWarningLogger
	abxbus.SlowWarningLogger = func(message string) {
		mu.Lock()
		defer mu.Unlock()
		messages = append(messages, message)
	}
	defer func() { abxbus.SlowWarningLogger = originalLogger }()

	bus.On("TimeoutDefaultsEvent", "slow_handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		time.Sleep(30 * time.Millisecond)
		mu.Lock()
		messages = append(messages, "slow warning child handler finishing")
		mu.Unlock()
		return "ok", nil
	}, nil)

	event := abxbus.NewBaseEvent("TimeoutDefaultsEvent", nil)
	event.EventSlowTimeout = &eventSlowTimeout
	if _, err := bus.Emit(event).Now(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	slowIndex := indexContains(messages, "Slow event processing")
	finishIndex := indexContains(messages, "slow warning child handler finishing")
	if slowIndex < 0 || finishIndex < 0 || slowIndex > finishIndex {
		t.Fatalf("slow event warning should fire while event is still running, got %#v", messages)
	}
	if indexContains(messages, "Slow event handler") >= 0 {
		t.Fatalf("event-only slow warning should not also emit handler warning, got %#v", messages)
	}
}

func TestSlowHandlerAndSlowEventWarningsCanBothFire(t *testing.T) {
	eventTimeout := 0.5
	slowTimeout := 0.01
	slowBusDefault := 0.5
	bus := abxbus.NewEventBus("SlowComboWarnBus", &abxbus.EventBusOptions{
		EventTimeout:            &eventTimeout,
		EventSlowTimeout:        &slowBusDefault,
		EventHandlerSlowTimeout: &slowBusDefault,
	})
	t.Cleanup(bus.Destroy)

	var mu sync.Mutex
	messages := []string{}
	originalLogger := abxbus.SlowWarningLogger
	abxbus.SlowWarningLogger = func(message string) {
		mu.Lock()
		defer mu.Unlock()
		messages = append(messages, message)
	}
	defer func() { abxbus.SlowWarningLogger = originalLogger }()

	bus.On("TimeoutDefaultsEvent", "slow_handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		time.Sleep(30 * time.Millisecond)
		return "ok", nil
	}, nil)

	event := abxbus.NewBaseEvent("TimeoutDefaultsEvent", nil)
	event.EventSlowTimeout = &slowTimeout
	event.EventHandlerSlowTimeout = &slowTimeout
	if _, err := bus.Emit(event).Now(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if indexContains(messages, "Slow event handler") < 0 {
		t.Fatalf("expected slow handler warning, got %#v", messages)
	}
	if indexContains(messages, "Slow event processing") < 0 {
		t.Fatalf("expected slow event warning, got %#v", messages)
	}
}

func TestZeroSlowWarningThresholdsDisableEventAndHandlerSlowWarnings(t *testing.T) {
	eventTimeout := 0.5
	noSlowWarning := 0.0
	bus := abxbus.NewEventBus("NoSlowWarnBus", &abxbus.EventBusOptions{
		EventTimeout:            &eventTimeout,
		EventSlowTimeout:        &noSlowWarning,
		EventHandlerSlowTimeout: &noSlowWarning,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
	})
	t.Cleanup(bus.Destroy)

	var mu sync.Mutex
	messages := []string{}
	originalLogger := abxbus.SlowWarningLogger
	abxbus.SlowWarningLogger = func(message string) {
		mu.Lock()
		defer mu.Unlock()
		messages = append(messages, message)
	}
	defer func() { abxbus.SlowWarningLogger = originalLogger }()

	bus.On("TimeoutDefaultsEvent", "slow_handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		time.Sleep(30 * time.Millisecond)
		mu.Lock()
		messages = append(messages, "slow warning child handler finishing")
		mu.Unlock()
		return "ok", nil
	}, nil)

	if _, err := bus.Emit(abxbus.NewBaseEvent("TimeoutDefaultsEvent", nil)).Now(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if indexContains(messages, "Slow event handler") >= 0 {
		t.Fatalf("handler slow warning should be disabled, got %#v", messages)
	}
	if indexContains(messages, "Slow event processing") >= 0 {
		t.Fatalf("event slow warning should be disabled, got %#v", messages)
	}
	if indexContains(messages, "slow warning child handler finishing") < 0 {
		t.Fatalf("handler should still run to completion, got %#v", messages)
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
	if _, err := busA.Emit(event).Now(); err != nil {
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
		if _, err := child.Now(); err != nil {
			return nil, err
		}
		return "parent", nil
	}, nil)

	parentTimeout := 0.5
	parent := abxbus.NewBaseEvent("TimeoutParentEvent", nil)
	parent.EventTimeout = &parentTimeout
	if _, err := busA.Emit(parent).Now(); err != nil {
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
		childEvent := abxbus.NewBaseEvent("TimeoutRecoveryChildEvent", nil)
		childEvent.EventTimeout = &childTimeout
		child := event.Emit(childEvent)
		childRef = child
		if _, err := child.Now(); err != nil {
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
	parent = busA.Emit(parent)
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}
	if childRef == nil {
		t.Fatal("parent handler did not emit child")
	}
	parentResult := eventResultByHandlerName(parent, "parent")
	if parentResult == nil || parentResult.Status != abxbus.EventResultCompleted {
		t.Fatalf("parent should complete after awaited child timeout is recorded on child, got %#v", parentResult)
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
	if _, err := tail.Now(); err != nil {
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

func TestParentTimeoutDoesNotCancelUnawaitedChildHandlerResultsUnderSerialHandlerLock(t *testing.T) {
	bus := abxbus.NewEventBus("TimeoutCancelBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	t.Cleanup(bus.Destroy)

	childRuns := 0
	bus.On("TimeoutCancelChildEvent", "child_first", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		childRuns++
		time.Sleep(30 * time.Millisecond)
		return "first", nil
	}, nil)
	bus.On("TimeoutCancelChildEvent", "child_second", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		childRuns++
		time.Sleep(10 * time.Millisecond)
		return "second", nil
	}, nil)
	bus.On("TimeoutCancelParentEvent", "parent", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		childTimeout := 0.2
		child := abxbus.NewBaseEvent("TimeoutCancelChildEvent", nil)
		child.EventTimeout = &childTimeout
		event.Emit(child)
		select {
		case <-time.After(50 * time.Millisecond):
			return "parent", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)

	parentTimeout := 0.01
	parent := abxbus.NewBaseEvent("TimeoutCancelParentEvent", nil)
	parent.EventTimeout = &parentTimeout
	parent = bus.Emit(parent)
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}
	idleTimeout := 2.0
	if !bus.WaitUntilIdle(&idleTimeout) {
		t.Fatal("bus did not become idle")
	}

	children := parentChildEvents(parent)
	if len(children) != 1 {
		t.Fatalf("expected one child event, got %#v", children)
	}
	child := children[0]
	if child.EventParentID == nil || *child.EventParentID != parent.EventID {
		t.Fatalf("child parent mismatch: got %#v want %s", child.EventParentID, parent.EventID)
	}
	if child.EventBlocksParentCompletion {
		t.Fatal("unawaited child should not block parent completion")
	}
	if childRuns != 2 {
		t.Fatalf("expected both child handlers to run, got %d", childRuns)
	}
	assertTimeoutTestAllResultsCompleted(t, child)
}

func TestMultiLevelTimeoutCascadeWithMixedCancellations(t *testing.T) {
	bus := abxbus.NewEventBus("TimeoutCascadeBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	t.Cleanup(bus.Destroy)

	var queuedChild *abxbus.BaseEvent
	var awaitedChild *abxbus.BaseEvent
	var immediateGrandchild *abxbus.BaseEvent
	var queuedGrandchild *abxbus.BaseEvent
	queuedChildRuns := 0
	immediateGrandchildRuns := 0
	queuedGrandchildRuns := 0

	bus.On("TimeoutCascadeQueuedChild", "queued_child_fast", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		queuedChildRuns++
		time.Sleep(5 * time.Millisecond)
		return "queued_fast", nil
	}, nil)
	bus.On("TimeoutCascadeQueuedChild", "queued_child_slow", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		queuedChildRuns++
		time.Sleep(50 * time.Millisecond)
		return "queued_slow", nil
	}, nil)
	bus.On("TimeoutCascadeAwaitedChild", "awaited_child_fast", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		time.Sleep(5 * time.Millisecond)
		return "awaited_fast", nil
	}, nil)
	bus.On("TimeoutCascadeAwaitedChild", "awaited_child_slow", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		queuedTimeout := 0.2
		queuedGrandchild = abxbus.NewBaseEvent("TimeoutCascadeQueuedGrandchild", nil)
		queuedGrandchild.EventTimeout = &queuedTimeout
		queuedGrandchild = event.Emit(queuedGrandchild)

		immediateTimeout := 0.2
		immediateGrandchild = abxbus.NewBaseEvent("TimeoutCascadeImmediateGrandchild", nil)
		immediateGrandchild.EventTimeout = &immediateTimeout
		immediateGrandchild = event.Emit(immediateGrandchild)
		if _, err := immediateGrandchild.Now(); err != nil {
			return nil, err
		}
		select {
		case <-time.After(100 * time.Millisecond):
			return "awaited_slow", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	bus.On("TimeoutCascadeImmediateGrandchild", "immediate_grandchild_slow", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		immediateGrandchildRuns++
		select {
		case <-time.After(50 * time.Millisecond):
			return "immediate_grandchild_slow", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	bus.On("TimeoutCascadeImmediateGrandchild", "immediate_grandchild_fast", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		immediateGrandchildRuns++
		time.Sleep(10 * time.Millisecond)
		return "immediate_grandchild_fast", nil
	}, nil)
	bus.On("TimeoutCascadeQueuedGrandchild", "queued_grandchild_slow", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		queuedGrandchildRuns++
		time.Sleep(50 * time.Millisecond)
		return "queued_grandchild_slow", nil
	}, nil)
	bus.On("TimeoutCascadeQueuedGrandchild", "queued_grandchild_fast", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		queuedGrandchildRuns++
		time.Sleep(10 * time.Millisecond)
		return "queued_grandchild_fast", nil
	}, nil)
	bus.On("TimeoutCascadeTop", "top", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		queuedTimeout := 0.2
		queuedChild = abxbus.NewBaseEvent("TimeoutCascadeQueuedChild", nil)
		queuedChild.EventTimeout = &queuedTimeout
		queuedChild = event.Emit(queuedChild)

		awaitedTimeout := 0.03
		awaitedChild = abxbus.NewBaseEvent("TimeoutCascadeAwaitedChild", nil)
		awaitedChild.EventTimeout = &awaitedTimeout
		awaitedChild = event.Emit(awaitedChild)
		if _, err := awaitedChild.Now(); err != nil {
			return nil, err
		}
		select {
		case <-time.After(80 * time.Millisecond):
			return "top", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)

	topTimeout := 0.04
	top := abxbus.NewBaseEvent("TimeoutCascadeTop", nil)
	top.EventTimeout = &topTimeout
	top = bus.Emit(top)
	if _, err := top.Now(); err != nil {
		t.Fatal(err)
	}
	idleTimeout := 2.0
	if !bus.WaitUntilIdle(&idleTimeout) {
		t.Fatal("bus did not become idle")
	}

	topResult := firstEventResult(top)
	if topResult == nil || topResult.Status != abxbus.EventResultError || !timeoutResultErrorContains(topResult, "Aborted running handler") {
		t.Fatalf("top handler should be aborted by event timeout, got %#v", topResult)
	}
	if queuedChild == nil || queuedChild.EventBlocksParentCompletion {
		t.Fatalf("queued child should be emitted and non-blocking, got %#v", queuedChild)
	}
	if queuedChildRuns != 2 {
		t.Fatalf("queued child should run both handlers independently, got %d", queuedChildRuns)
	}
	assertTimeoutTestAllResultsCompleted(t, queuedChild)

	if awaitedChild == nil {
		t.Fatal("awaited child was not emitted")
	}
	awaitedCompleted, awaitedErrored := timeoutTestCountCompletedAndErrored(awaitedChild)
	if awaitedCompleted != 1 || awaitedErrored != 1 {
		t.Fatalf("awaited child should have one completed and one error result, completed=%d errored=%d results=%#v", awaitedCompleted, awaitedErrored, awaitedChild.EventResults)
	}

	if immediateGrandchild == nil {
		t.Fatal("immediate grandchild was not emitted")
	}
	immediateCompleted, immediateErrored := timeoutTestCountCompletedAndErrored(immediateGrandchild)
	if immediateGrandchildRuns < 1 || immediateCompleted+immediateErrored != 2 || immediateErrored == 0 {
		t.Fatalf("immediate grandchild should have timeout/cancellation results, runs=%d completed=%d errored=%d results=%#v", immediateGrandchildRuns, immediateCompleted, immediateErrored, immediateGrandchild.EventResults)
	}

	if queuedGrandchild == nil || queuedGrandchild.EventBlocksParentCompletion {
		t.Fatalf("queued grandchild should be emitted and non-blocking, got %#v", queuedGrandchild)
	}
	if queuedGrandchildRuns != 2 {
		t.Fatalf("queued grandchild should run independently after parent timeout, got %d", queuedGrandchildRuns)
	}
	assertTimeoutTestAllResultsCompleted(t, queuedGrandchild)
}

func TestUnawaitedDescendantPreservesLineageAndIsNotCancelledByAncestorTimeout(t *testing.T) {
	bus := abxbus.NewEventBus("ErrorChainBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	t.Cleanup(bus.Destroy)

	var innerRef *abxbus.BaseEvent
	var deepRef *abxbus.BaseEvent
	bus.On("ErrorChainDeep", "deep", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		time.Sleep(200 * time.Millisecond)
		return "deep_done", nil
	}, nil)
	bus.On("ErrorChainInner", "inner", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		deepTimeout := 0.5
		deepRef = abxbus.NewBaseEvent("ErrorChainDeep", nil)
		deepRef.EventTimeout = &deepTimeout
		deepRef = event.Emit(deepRef)
		select {
		case <-time.After(200 * time.Millisecond):
			return "inner_done", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	bus.On("ErrorChainOuter", "outer", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		innerTimeout := 0.04
		innerRef = abxbus.NewBaseEvent("ErrorChainInner", nil)
		innerRef.EventTimeout = &innerTimeout
		innerRef = event.Emit(innerRef)
		if _, err := innerRef.Now(); err != nil {
			return nil, err
		}
		select {
		case <-time.After(200 * time.Millisecond):
			return "outer_done", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)

	outerTimeout := 0.15
	outer := abxbus.NewBaseEvent("ErrorChainOuter", nil)
	outer.EventTimeout = &outerTimeout
	outer = bus.Emit(outer)
	if _, err := outer.Now(); err != nil {
		t.Fatal(err)
	}
	idleTimeout := 2.0
	if !bus.WaitUntilIdle(&idleTimeout) {
		t.Fatal("bus did not become idle")
	}

	outerResult := firstEventResult(outer)
	if outerResult == nil || outerResult.Status != abxbus.EventResultError || !timeoutResultErrorContains(outerResult, "Aborted running handler") {
		t.Fatalf("outer handler should be aborted by event timeout, got %#v", outerResult)
	}
	if innerRef == nil {
		t.Fatal("inner event was not emitted")
	}
	innerResult := firstEventResult(innerRef)
	if innerResult == nil || innerResult.Status != abxbus.EventResultError || !timeoutResultErrorContains(innerResult, "Aborted running handler") {
		t.Fatalf("inner handler should be aborted by its own event timeout, got %#v", innerResult)
	}
	if deepRef == nil {
		t.Fatal("deep event was not emitted")
	}
	if deepRef.EventParentID == nil || *deepRef.EventParentID != innerRef.EventID {
		t.Fatalf("deep parent mismatch: got %#v want %s", deepRef.EventParentID, innerRef.EventID)
	}
	if deepRef.EventBlocksParentCompletion {
		t.Fatal("unawaited deep event should not block parent completion")
	}
	deepResult := firstEventResult(deepRef)
	if deepResult == nil || deepResult.Status != abxbus.EventResultCompleted || deepResult.Result != "deep_done" {
		t.Fatalf("deep event should complete independently, got %#v", deepResult)
	}
}

func TestParentTimeoutDoesNotCancelUnawaitedChildrenThatHaveNoTimeoutOfTheirOwn(t *testing.T) {
	noTimeout := 0.0
	bus := abxbus.NewEventBus("TimeoutBoundaryBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventTimeout:            &noTimeout,
	})
	t.Cleanup(bus.Destroy)

	var childRef *abxbus.BaseEvent
	childHandlerRan := false
	bus.On("TimeoutBoundaryChild", "child_slow", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		childHandlerRan = true
		time.Sleep(80 * time.Millisecond)
		return "child_done", nil
	}, nil)
	bus.On("TimeoutBoundaryParent", "parent", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		childRef = abxbus.NewBaseEvent("TimeoutBoundaryChild", nil)
		childRef.EventTimeout = nil
		childRef = event.Emit(childRef)
		select {
		case <-time.After(200 * time.Millisecond):
			return "parent_done", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)

	parentTimeout := 0.03
	parent := abxbus.NewBaseEvent("TimeoutBoundaryParent", nil)
	parent.EventTimeout = &parentTimeout
	parent = bus.Emit(parent)
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}
	idleTimeout := 2.0
	if !bus.WaitUntilIdle(&idleTimeout) {
		t.Fatal("bus did not become idle")
	}

	parentResult := firstEventResult(parent)
	if parentResult == nil || parentResult.Status != abxbus.EventResultError || !timeoutResultErrorContains(parentResult, "Aborted running handler") {
		t.Fatalf("parent should time out, got %#v", parentResult)
	}
	if childRef == nil {
		t.Fatal("child event was not emitted")
	}
	if childRef.EventStatus != "completed" {
		t.Fatalf("child should complete independently, got %s", childRef.EventStatus)
	}
	if childRef.EventParentID == nil || *childRef.EventParentID != parent.EventID {
		t.Fatalf("child parent mismatch: got %#v want %s", childRef.EventParentID, parent.EventID)
	}
	if childRef.EventBlocksParentCompletion {
		t.Fatal("unawaited child should not block parent completion")
	}
	if !childHandlerRan {
		t.Fatal("child handler should run independently")
	}
	childResult := firstEventResult(childRef)
	if childResult == nil || childResult.Status != abxbus.EventResultCompleted || childResult.Result != "child_done" {
		t.Fatalf("child should complete independently, got %#v", childResult)
	}
}

func timeoutResultErrorContains(result *abxbus.EventResult, text string) bool {
	errorText, _ := result.Error.(string)
	return strings.Contains(errorText, text)
}

func timeoutTestCountCompletedAndErrored(event *abxbus.BaseEvent) (int, int) {
	completed := 0
	errored := 0
	for _, result := range event.EventResults {
		switch result.Status {
		case abxbus.EventResultCompleted:
			completed++
		case abxbus.EventResultError:
			errored++
		}
	}
	return completed, errored
}

func assertTimeoutTestAllResultsCompleted(t *testing.T, event *abxbus.BaseEvent) {
	t.Helper()
	if len(event.EventResults) == 0 {
		t.Fatalf("expected handler results for %s", event.EventType)
	}
	for _, result := range event.EventResults {
		if result.Status != abxbus.EventResultCompleted {
			t.Fatalf("%s handler %s should complete, got %s error=%#v", event.EventType, result.HandlerName, result.Status, result.Error)
		}
	}
}

func indexContains(messages []string, text string) int {
	for index, message := range messages {
		if strings.Contains(message, text) {
			return index
		}
	}
	return -1
}
