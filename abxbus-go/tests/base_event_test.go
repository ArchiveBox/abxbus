package abxbus_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestBaseEventNowWithoutBus(t *testing.T) {
	e := abxbus.NewBaseEvent("NoBus", nil)
	if _, err := e.Now(); err == nil || !strings.Contains(err.Error(), "no bus attached") {
		t.Fatalf("expected missing bus error, got %v", err)
	}
	if e.EventStatus != "pending" {
		t.Fatalf("Now without a bus should not mutate event status, got %s", e.EventStatus)
	}
}

func TestBaseEventNowAllowsCompletedRestoredEventWithoutBus(t *testing.T) {
	raw := []byte(`{
		"event_type": "RestoredCompletedEvent",
		"event_version": "0.0.1",
		"event_timeout": null,
		"event_slow_timeout": null,
		"event_concurrency": null,
		"event_handler_timeout": null,
		"event_handler_slow_timeout": null,
		"event_handler_concurrency": null,
		"event_handler_completion": null,
		"event_blocks_parent_completion": false,
		"event_result_type": null,
		"event_id": "00000000-0000-5000-8000-000000000101",
		"event_path": [],
		"event_parent_id": null,
		"event_emitted_by_handler_id": null,
		"event_pending_bus_count": 0,
		"event_created_at": "2026-01-01T00:00:00.000Z",
		"event_status": "completed",
		"event_started_at": "2026-01-01T00:00:00.001Z",
		"event_completed_at": "2026-01-01T00:00:00.002Z",
		"event_results": {}
	}`)

	event, err := abxbus.BaseEventFromJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := event.Now(); err != nil {
		t.Fatalf("completed restored event should not require a live bus: %v", err)
	}
}

func TestBaseEventNowInsideHandlerNoArgs(t *testing.T) {
	bus := abxbus.NewEventBus("BaseEventNowInsideNoArgsBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	order := []string{}

	bus.On("NowInsideNoArgsParent", "parent", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "parent_start")
		event.Bus.Emit(abxbus.NewBaseEvent("NowInsideNoArgsSibling", nil))
		child := event.Emit(abxbus.NewBaseEvent("NowInsideNoArgsChild", nil))
		if _, err := child.Now(); err != nil {
			return nil, err
		}
		order = append(order, "parent_end")
		return nil, nil
	}, nil)
	bus.On("NowInsideNoArgsChild", "child", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "child")
		return nil, nil
	}, nil)
	bus.On("NowInsideNoArgsSibling", "sibling", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "sibling")
		return nil, nil
	}, nil)

	parent := bus.Emit(abxbus.NewBaseEvent("NowInsideNoArgsParent", nil))
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}
	timeout := 2.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("bus did not become idle")
	}
	if strings.Join(order, ",") != "parent_start,child,parent_end,sibling" {
		t.Fatalf("unexpected order: %v", order)
	}
}

func TestBaseEventNowInsideHandlerWithArgs(t *testing.T) {
	bus := abxbus.NewEventBus("BaseEventNowInsideArgsBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	order := []string{}
	var child *abxbus.BaseEvent

	bus.On("NowInsideArgsParent", "parent", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "parent_start")
		event.Bus.Emit(abxbus.NewBaseEvent("NowInsideArgsSibling", nil))
		child = event.Emit(abxbus.NewBaseEvent("NowInsideArgsChild", nil))
		if _, err := child.Now(); err != nil {
			return nil, err
		}
		order = append(order, "parent_end")
		return nil, nil
	}, nil)
	bus.On("NowInsideArgsChild", "child", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "child")
		return nil, errors.New("child failure")
	}, nil)
	bus.On("NowInsideArgsSibling", "sibling", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "sibling")
		return nil, nil
	}, nil)

	parent := bus.Emit(abxbus.NewBaseEvent("NowInsideArgsParent", nil))
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}
	timeout := 2.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("bus did not become idle")
	}
	if strings.Join(order, ",") != "parent_start,child,parent_end,sibling" {
		t.Fatalf("unexpected order: %v", order)
	}
	if child == nil || child.EventStatus != "completed" {
		t.Fatalf("child should complete, got %#v", child)
	}
	if _, err := child.EventResult(); err == nil || err.Error() != "child failure" {
		t.Fatalf("default EventResult should surface child failure after completion, got %v", err)
	}
}

func TestWaitOutsideHandlerPreservesNormalQueueOrder(t *testing.T) {
	bus := abxbus.NewEventBus("WaitOutsideHandlerQueueOrderBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	order := []string{}
	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})

	bus.On("WaitOutsideHandlerBlockerEvent", "blocker", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "blocker_start")
		close(blockerStarted)
		<-releaseBlocker
		order = append(order, "blocker_end")
		return nil, nil
	}, nil)
	bus.On("WaitOutsideHandlerTargetEvent", "target", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "target")
		return nil, nil
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	bus.Emit(abxbus.NewBaseEvent("WaitOutsideHandlerBlockerEvent", nil))
	select {
	case <-blockerStarted:
	case <-ctx.Done():
		t.Fatal("blocker did not start")
	}
	target := bus.Emit(abxbus.NewBaseEvent("WaitOutsideHandlerTargetEvent", nil))
	doneCh := make(chan error, 1)
	go func() {
		_, err := target.Wait()
		doneCh <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if strings.Join(order, ",") != "blocker_start" {
		t.Fatalf("Wait outside handlers should not queue-jump target, got %v", order)
	}
	close(releaseBlocker)
	if err := <-doneCh; err != nil {
		t.Fatal(err)
	}
	timeout := 2.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("bus did not become idle")
	}
	if strings.Join(order, ",") != "blocker_start,blocker_end,target" {
		t.Fatalf("unexpected order: %v", order)
	}
}

func TestNowOutsideHandlerAllowsNormalParallelProcessing(t *testing.T) {
	bus := abxbus.NewEventBus("NowOutsideHandlerParallelQueueOrderBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	order := []string{}
	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})

	bus.On("NowOutsideHandlerParallelBlockerEvent", "blocker", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "blocker_start")
		close(blockerStarted)
		<-releaseBlocker
		order = append(order, "blocker_end")
		return nil, nil
	}, nil)
	bus.On("NowOutsideHandlerParallelTargetEvent", "target", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "target")
		return nil, nil
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	bus.Emit(abxbus.NewBaseEvent("NowOutsideHandlerParallelBlockerEvent", nil))
	select {
	case <-blockerStarted:
	case <-ctx.Done():
		t.Fatal("blocker did not start")
	}
	target := abxbus.NewBaseEvent("NowOutsideHandlerParallelTargetEvent", nil)
	target.EventConcurrency = abxbus.EventConcurrencyParallel
	target = bus.Emit(target)
	doneCh := make(chan error, 1)
	go func() {
		_, err := target.Now()
		doneCh <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if strings.Join(order, ",") != "blocker_start,target" {
		t.Fatalf("parallel target should process normally while blocker is still running, got %v", order)
	}
	close(releaseBlocker)
	if err := <-doneCh; err != nil {
		t.Fatal(err)
	}
	timeout := 2.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("bus did not become idle")
	}
	if strings.Join(order, ",") != "blocker_start,target,blocker_end" {
		t.Fatalf("unexpected order: %v", order)
	}
}

func TestWaitReturnsEventWithoutForcingQueuedExecution(t *testing.T) {
	bus := abxbus.NewEventBus("WaitPassiveQueueOrderBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	order := []string{}
	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})

	bus.On("WaitPassiveBlockerEvent", "blocker", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "blocker_start")
		close(blockerStarted)
		<-releaseBlocker
		order = append(order, "blocker_end")
		return nil, nil
	}, nil)
	bus.On("WaitPassiveTargetEvent", "target", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "target")
		return "target", nil
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	bus.Emit(abxbus.NewBaseEvent("WaitPassiveBlockerEvent", nil))
	select {
	case <-blockerStarted:
	case <-ctx.Done():
		t.Fatal("blocker did not start")
	}
	target := bus.Emit(abxbus.NewBaseEvent("WaitPassiveTargetEvent", nil))
	waitedEvent := make(chan *abxbus.BaseEvent, 1)
	waitErr := make(chan error, 1)
	timeout := 1.0
	go func() {
		event, err := target.Wait(&abxbus.EventWaitOptions{Timeout: &timeout})
		waitedEvent <- event
		waitErr <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if strings.Join(order, ",") != "blocker_start" {
		t.Fatalf("Wait should not queue-jump target, got %v", order)
	}
	close(releaseBlocker)
	if err := <-waitErr; err != nil {
		t.Fatal(err)
	}
	if <-waitedEvent != target {
		t.Fatal("Wait should return the event")
	}
	if strings.Join(order, ",") != "blocker_start,blocker_end,target" {
		t.Fatalf("unexpected order: %v", order)
	}
}

func TestNowReturnsEventAndQueueJumpsQueuedExecution(t *testing.T) {
	bus := abxbus.NewEventBus("NowActiveQueueJumpBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	order := []string{}
	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})

	bus.On("NowActiveBlockerEvent", "blocker", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "blocker_start")
		close(blockerStarted)
		<-releaseBlocker
		order = append(order, "blocker_end")
		return nil, nil
	}, nil)
	bus.On("NowActiveTargetEvent", "target", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "target")
		return "target", nil
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	bus.Emit(abxbus.NewBaseEvent("NowActiveBlockerEvent", nil))
	select {
	case <-blockerStarted:
	case <-ctx.Done():
		t.Fatal("blocker did not start")
	}
	target := bus.Emit(abxbus.NewBaseEvent("NowActiveTargetEvent", nil))
	processedEvent := make(chan *abxbus.BaseEvent, 1)
	nowErr := make(chan error, 1)
	timeout := 1.0
	go func() {
		event, err := target.Now(&abxbus.EventWaitOptions{Timeout: &timeout})
		processedEvent <- event
		nowErr <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if strings.Join(order, ",") != "blocker_start,target" {
		t.Fatalf("Now should queue-jump target, got %v", order)
	}
	if err := <-nowErr; err != nil {
		t.Fatal(err)
	}
	if <-processedEvent != target {
		t.Fatal("Now should return the event")
	}
	close(releaseBlocker)
	timeoutWait := 2.0
	if !bus.WaitUntilIdle(&timeoutWait) {
		t.Fatal("bus did not become idle")
	}
	if strings.Join(order, ",") != "blocker_start,target,blocker_end" {
		t.Fatalf("unexpected order: %v", order)
	}
}

func TestWaitFirstResultReturnsBeforeEventCompletion(t *testing.T) {
	noTimeout := 0.0
	bus := abxbus.NewEventBus("WaitFirstResultBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyParallel,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
		EventTimeout:            &noTimeout,
	})
	slowFinished := make(chan struct{})
	bus.On("WaitFirstResultEvent", "medium", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		time.Sleep(30 * time.Millisecond)
		return "medium", nil
	}, nil)
	bus.On("WaitFirstResultEvent", "fast", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		time.Sleep(10 * time.Millisecond)
		return "fast", nil
	}, nil)
	bus.On("WaitFirstResultEvent", "slow", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		time.Sleep(250 * time.Millisecond)
		close(slowFinished)
		return "slow", nil
	}, nil)

	target := abxbus.NewBaseEvent("WaitFirstResultEvent", nil)
	target.EventConcurrency = abxbus.EventConcurrencyParallel
	event := bus.Emit(target)
	timeout := 1.0
	completed, err := event.Wait(&abxbus.EventWaitOptions{Timeout: &timeout, FirstResult: true})
	if err != nil {
		t.Fatal(err)
	}
	if completed != event {
		t.Fatal("Wait should return the event")
	}
	value, err := event.EventResult(&abxbus.EventResultOptions{RaiseIfAny: abxbus.Ptr(false)})
	if err != nil || value != "fast" {
		t.Fatalf("expected first current result, got %#v err=%v", value, err)
	}
	time.Sleep(50 * time.Millisecond)
	values, err := event.EventResultsList(&abxbus.EventResultOptions{RaiseIfAny: abxbus.Ptr(false)})
	if err != nil || len(values) != 2 || values[0] != "medium" || values[1] != "fast" {
		t.Fatalf("expected current result subset in registration order, got %#v err=%v", values, err)
	}
	select {
	case <-slowFinished:
		t.Fatal("first_result wait should return before slow handler finishes")
	default:
	}
	if event.EventStatus == "completed" {
		t.Fatal("event should still be running after first_result wait")
	}
	select {
	case <-slowFinished:
	case <-time.After(time.Second):
		t.Fatal("slow handler did not finish")
	}
}

func TestNowFirstResultReturnsBeforeEventCompletion(t *testing.T) {
	noTimeout := 0.0
	bus := abxbus.NewEventBus("NowFirstResultBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
		EventTimeout:            &noTimeout,
	})
	slowStarted := make(chan struct{})
	slowFinished := make(chan struct{})
	slowCanceled := make(chan struct{})
	releaseSlow := make(chan struct{})
	bus.On("NowFirstResultEvent", "medium", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		time.Sleep(30 * time.Millisecond)
		return "medium", nil
	}, nil)
	bus.On("NowFirstResultEvent", "fast", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		select {
		case <-slowStarted:
		case <-time.After(time.Second):
			return nil, errors.New("slow handler did not start")
		}
		time.Sleep(10 * time.Millisecond)
		return "fast", nil
	}, nil)
	bus.On("NowFirstResultEvent", "slow", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		close(slowStarted)
		select {
		case <-releaseSlow:
			close(slowFinished)
			return "slow", nil
		case <-ctx.Done():
			close(slowCanceled)
			return nil, ctx.Err()
		}
	}, nil)

	target := abxbus.NewBaseEvent("NowFirstResultEvent", nil)
	target.EventConcurrency = abxbus.EventConcurrencyParallel
	event := bus.Emit(target)
	timeout := 1.0
	completed, err := event.Now(&abxbus.EventWaitOptions{Timeout: &timeout, FirstResult: true})
	if err != nil {
		t.Fatal(err)
	}
	if completed != event {
		t.Fatal("Now should return the event")
	}
	value, err := event.EventResult(&abxbus.EventResultOptions{RaiseIfAny: abxbus.Ptr(false)})
	if err != nil || value != "fast" {
		t.Fatalf("expected first current result, got %#v err=%v", value, err)
	}
	time.Sleep(50 * time.Millisecond)
	values, err := event.EventResultsList(&abxbus.EventResultOptions{RaiseIfAny: abxbus.Ptr(false)})
	if err != nil || len(values) != 2 || values[0] != "medium" || values[1] != "fast" {
		t.Fatalf("expected current result subset in registration order, got %#v err=%v", values, err)
	}
	select {
	case <-slowFinished:
		t.Fatal("first_result now should return before slow handler finishes")
	case <-slowCanceled:
		t.Fatal("first_result now should not cancel background handlers")
	default:
	}
	if event.EventStatus == "completed" {
		t.Fatal("event should still be running after first_result now")
	}
	close(releaseSlow)
	select {
	case <-slowFinished:
	case <-time.After(time.Second):
		t.Fatal("slow handler did not finish after first_result now")
	}
}

func TestEventResultStartsNeverStartedEventAndReturnsFirstResult(t *testing.T) {
	bus := abxbus.NewEventBus("EventResultShortcutQueueJumpBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	order := []string{}
	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})
	bus.On("EventResultShortcutBlockerEvent", "blocker", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "blocker_start")
		close(blockerStarted)
		<-releaseBlocker
		order = append(order, "blocker_end")
		return nil, nil
	}, nil)
	bus.On("EventResultShortcutTargetEvent", "target", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "target")
		return "target", nil
	}, nil)

	bus.Emit(abxbus.NewBaseEvent("EventResultShortcutBlockerEvent", nil))
	<-blockerStarted
	target := bus.Emit(abxbus.NewBaseEvent("EventResultShortcutTargetEvent", nil))
	resultCh := make(chan any, 1)
	errCh := make(chan error, 1)
	go func() {
		result, err := target.EventResult()
		resultCh <- result
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if strings.Join(order, ",") != "blocker_start,target" {
		t.Fatalf("EventResult should queue-jump never-started event, got %v", order)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if result := <-resultCh; result != "target" {
		t.Fatalf("expected target result, got %#v", result)
	}
	close(releaseBlocker)
}

func TestEventResultsListStartsNeverStartedEventAndReturnsAllResults(t *testing.T) {
	bus := abxbus.NewEventBus("EventResultsShortcutQueueJumpBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	order := []string{}
	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})
	bus.On("EventResultsShortcutBlockerEvent", "blocker", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "blocker_start")
		close(blockerStarted)
		<-releaseBlocker
		order = append(order, "blocker_end")
		return nil, nil
	}, nil)
	bus.On("EventResultsShortcutTargetEvent", "first", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "first")
		return "first", nil
	}, nil)
	bus.On("EventResultsShortcutTargetEvent", "second", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		order = append(order, "second")
		return "second", nil
	}, nil)

	bus.Emit(abxbus.NewBaseEvent("EventResultsShortcutBlockerEvent", nil))
	<-blockerStarted
	target := bus.Emit(abxbus.NewBaseEvent("EventResultsShortcutTargetEvent", nil))
	resultsCh := make(chan []any, 1)
	errCh := make(chan error, 1)
	go func() {
		results, err := target.EventResultsList()
		resultsCh <- results
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if strings.Join(order, ",") != "blocker_start,first,second" {
		t.Fatalf("EventResultsList should queue-jump never-started event, got %v", order)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	results := <-resultsCh
	if len(results) != 2 || results[0] != "first" || results[1] != "second" {
		t.Fatalf("unexpected results: %#v", results)
	}
	if len(target.EventResults) != 2 {
		t.Fatalf("expected event_results mapping to contain 2 records, got %#v", target.EventResults)
	}
	resultValues := map[any]bool{}
	for _, eventResult := range target.EventResults {
		resultValues[eventResult.Result] = true
	}
	if !resultValues["first"] || !resultValues["second"] {
		t.Fatalf("event_results mapping did not contain expected records: %#v", target.EventResults)
	}
	close(releaseBlocker)
}

func TestEventResultHelpersDoNotWaitForStartedEvent(t *testing.T) {
	noTimeout := 0.0
	bus := abxbus.NewEventBus("EventResultHelpersStartedBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyParallel,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
		EventTimeout:            &noTimeout,
	})
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	bus.On("EventResultHelpersStartedEvent", "slow", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		close(handlerStarted)
		<-releaseHandler
		return "late", nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("EventResultHelpersStartedEvent", nil))
	<-handlerStarted

	if event.EventStatus != "started" {
		t.Fatalf("expected started event, got %s", event.EventStatus)
	}
	resultCh := make(chan any, 1)
	resultErrCh := make(chan error, 1)
	go func() {
		result, err := event.EventResult(&abxbus.EventResultOptions{RaiseIfNone: abxbus.Ptr(false)})
		resultCh <- result
		resultErrCh <- err
	}()
	select {
	case err := <-resultErrCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("EventResult should not wait for a started event")
	}
	if result := <-resultCh; result != nil {
		t.Fatalf("expected nil current result, got %#v", result)
	}

	resultsCh := make(chan []any, 1)
	resultsErrCh := make(chan error, 1)
	go func() {
		results, err := event.EventResultsList(&abxbus.EventResultOptions{RaiseIfNone: abxbus.Ptr(false)})
		resultsCh <- results
		resultsErrCh <- err
	}()
	select {
	case err := <-resultsErrCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("EventResultsList should not wait for a started event")
	}
	if results := <-resultsCh; len(results) != 0 {
		t.Fatalf("expected no current results, got %#v", results)
	}
	if event.EventStatus != "started" {
		t.Fatalf("result helpers should not complete the event, got %s", event.EventStatus)
	}
	close(releaseHandler)
	timeout := 1.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("timed out waiting for bus")
	}
}

func TestNowOnAlreadyExecutingEventWaitsWithoutDuplicateExecution(t *testing.T) {
	noTimeout := 0.0
	bus := abxbus.NewEventBus("NowAlreadyExecutingBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventTimeout:            &noTimeout,
	})
	started := make(chan struct{})
	release := make(chan struct{})
	runCount := 0
	bus.On("NowAlreadyExecutingEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		runCount++
		close(started)
		<-release
		return "done", nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("NowAlreadyExecutingEvent", nil))
	<-started
	nowCh := make(chan *abxbus.BaseEvent, 1)
	errCh := make(chan error, 1)
	timeout := 1.0
	go func() {
		completed, err := event.Now(&abxbus.EventWaitOptions{Timeout: &timeout})
		nowCh <- completed
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if runCount != 1 {
		t.Fatalf("already executing event should not be duplicated, ran %d times", runCount)
	}
	close(release)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if <-nowCh != event {
		t.Fatal("Now should return the event")
	}
	result, err := event.EventResult()
	if err != nil || result != "done" {
		t.Fatalf("unexpected result %#v err=%v", result, err)
	}
	if runCount != 1 {
		t.Fatalf("already executing event should not be duplicated, ran %d times", runCount)
	}
}

func TestEventResultOptionsApplyToCurrentResults(t *testing.T) {
	noTimeout := 0.0
	bus := abxbus.NewEventBus("EventResultOptionsCurrentResultsBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
		EventTimeout:            &noTimeout,
	})
	releaseSlow := make(chan struct{})
	bus.On("EventResultOptionsCurrentResultsEvent", "fail", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return nil, errors.New("option boom")
	}, nil)
	bus.On("EventResultOptionsCurrentResultsEvent", "keep", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		time.Sleep(10 * time.Millisecond)
		return "keep", nil
	}, nil)
	bus.On("EventResultOptionsCurrentResultsEvent", "slow", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		<-releaseSlow
		return "late", nil
	}, nil)

	timeout := 1.0
	event, err := bus.Emit(abxbus.NewBaseEvent("EventResultOptionsCurrentResultsEvent", nil)).Now(
		&abxbus.EventWaitOptions{Timeout: &timeout, FirstResult: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := event.EventResult(&abxbus.EventResultOptions{RaiseIfAny: abxbus.Ptr(false)})
	if err != nil || result != "keep" {
		t.Fatalf("expected keep result, got %#v err=%v", result, err)
	}
	if _, err := event.EventResult(&abxbus.EventResultOptions{RaiseIfAny: abxbus.Ptr(true)}); err == nil || !strings.Contains(err.Error(), "option boom") {
		t.Fatalf("expected option boom, got %v", err)
	}
	results, err := event.EventResultsList(&abxbus.EventResultOptions{
		Include: func(result any, eventResult *abxbus.EventResult) bool {
			return result == "missing"
		},
		RaiseIfAny:  abxbus.Ptr(false),
		RaiseIfNone: abxbus.Ptr(false),
	})
	if err != nil || len(results) != 0 {
		t.Fatalf("expected empty filtered results, got %#v err=%v", results, err)
	}
	close(releaseSlow)
}

func TestBaseEventNowOutsideHandlerNoArgs(t *testing.T) {
	bus := abxbus.NewEventBus("BaseEventNowOutsideNoArgsBus", nil)
	bus.On("NowOutsideNoArgsEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return nil, errors.New("outside failure")
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("NowOutsideNoArgsEvent", nil))
	if _, err := event.Now(); err != nil {
		t.Fatalf("Now should wait without surfacing handler errors, got %v", err)
	}
	if _, err := event.EventResult(); err == nil || err.Error() != "outside failure" {
		t.Fatalf("default EventResult should surface outside failure, got %v", err)
	}
	if event.EventStatus != "completed" {
		t.Fatalf("event should be completed, got %s", event.EventStatus)
	}
}

func TestBaseEventNowOutsideHandlerWithArgs(t *testing.T) {
	bus := abxbus.NewEventBus("BaseEventNowOutsideArgsBus", nil)
	bus.On("NowOutsideArgsEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return nil, errors.New("outside suppressed failure")
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("NowOutsideArgsEvent", nil))
	if _, err := event.Now(); err != nil {
		t.Fatalf("RaiseIfAny=false should only wait for completion, got %v", err)
	}
	if event.EventStatus != "completed" {
		t.Fatalf("event should be completed, got %s", event.EventStatus)
	}
}

func TestBaseEventJSONFlattenedPayload(t *testing.T) {
	e := abxbus.NewBaseEvent("JSONEvent", map[string]any{"x": 1})
	data, err := e.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil {
		t.Fatal(err)
	}
	if _, ok := obj["payload"]; ok {
		t.Fatal("payload must be flattened")
	}
	if obj["x"].(float64) != 1 {
		t.Fatal("payload key x missing")
	}
	if _, ok := obj["event_id"]; !ok {
		t.Fatal("missing event_id")
	}
}

func TestBaseEventEventResultUpdateCreatesAndUpdatesTypedHandlerResults(t *testing.T) {
	bus := abxbus.NewEventBus("BaseEventEventResultUpdateBus", nil)
	event := abxbus.NewBaseEvent("BaseEventEventResultUpdateEvent", nil)
	event.EventResultType = map[string]any{"type": "string"}
	handlerEntry := bus.On("BaseEventEventResultUpdateEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, nil)

	pending := event.EventResultUpdate(handlerEntry, &abxbus.BaseEventResultUpdateOptions{
		EventBus: bus,
		EventResultUpdateOptions: abxbus.EventResultUpdateOptions{
			Status: abxbus.EventResultPending,
		},
	})
	if event.EventResults[handlerEntry.ID] != pending {
		t.Fatal("event_result_update should store the pending result by handler id")
	}
	if pending.Status != abxbus.EventResultPending {
		t.Fatalf("expected pending result, got %s", pending.Status)
	}

	completed := event.EventResultUpdate(handlerEntry, &abxbus.BaseEventResultUpdateOptions{
		EventBus: bus,
		EventResultUpdateOptions: abxbus.EventResultUpdateOptions{
			Status: abxbus.EventResultCompleted,
			Result: "seeded",
		},
	})
	if completed != pending {
		t.Fatal("event_result_update should update the existing handler result")
	}
	if completed.Status != abxbus.EventResultCompleted || completed.Result != "seeded" {
		t.Fatalf("expected completed seeded result, got status=%s result=%#v", completed.Status, completed.Result)
	}
	if completed.StartedAt == nil || completed.CompletedAt == nil {
		t.Fatalf("completed update should set started_at and completed_at: %#v", completed)
	}
}

func TestBaseEventEventResultUpdateStatusOnlyPreservesExistingErrorAndResult(t *testing.T) {
	bus := abxbus.NewEventBus("BaseEventEventResultUpdateStatusOnlyBus", nil)
	event := abxbus.NewBaseEvent("BaseEventEventResultUpdateStatusOnlyEvent", nil)
	event.EventResultType = map[string]any{"type": "string"}
	handlerEntry := bus.On("BaseEventEventResultUpdateStatusOnlyEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, nil)

	errored := event.EventResultUpdate(handlerEntry, &abxbus.BaseEventResultUpdateOptions{
		EventBus: bus,
		EventResultUpdateOptions: abxbus.EventResultUpdateOptions{
			Error: errors.New("seeded error"),
		},
	})
	if errored.Status != abxbus.EventResultError || errored.Error != "seeded error" {
		t.Fatalf("expected seeded error result, got status=%s error=%#v", errored.Status, errored.Error)
	}

	statusOnly := event.EventResultUpdate(handlerEntry, &abxbus.BaseEventResultUpdateOptions{
		EventBus: bus,
		EventResultUpdateOptions: abxbus.EventResultUpdateOptions{
			Status: abxbus.EventResultPending,
		},
	})
	if statusOnly.Status != abxbus.EventResultPending {
		t.Fatalf("expected status-only update to set pending, got %s", statusOnly.Status)
	}
	if statusOnly.Error != "seeded error" {
		t.Fatalf("status-only update should preserve existing error, got %#v", statusOnly.Error)
	}
	if statusOnly.Result != nil {
		t.Fatalf("status-only update should not synthesize a result, got %#v", statusOnly.Result)
	}
}

func TestBaseEventEventResultUpdateValidatesDeclaredResultSchema(t *testing.T) {
	bus := abxbus.NewEventBus("BaseEventEventResultUpdateSchemaBus", nil)
	event := abxbus.NewBaseEvent("BaseEventEventResultUpdateSchemaEvent", nil)
	event.EventResultType = map[string]any{"type": "string"}
	handlerEntry := bus.On("BaseEventEventResultUpdateSchemaEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, nil)

	result := event.EventResultUpdate(handlerEntry, &abxbus.BaseEventResultUpdateOptions{
		EventBus: bus,
		EventResultUpdateOptions: abxbus.EventResultUpdateOptions{
			Result: 123,
		},
	})
	if result.Status != abxbus.EventResultError {
		t.Fatalf("invalid seeded result should mark handler error, got %s", result.Status)
	}
	if !strings.Contains(result.Error.(string), "EventHandlerResultSchemaError") {
		t.Fatalf("expected schema error, got %#v", result.Error)
	}
	if result.Result != nil {
		t.Fatalf("invalid seeded result should not be stored, got %#v", result.Result)
	}
}

func TestWaitWaitsInQueueOrderInsideHandler(t *testing.T) {
	bus := abxbus.NewEventBus("QueueOrderEventCompletedBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyParallel,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	order := make([]string, 0, 6)
	orderCh := make(chan string, 8)
	siblingStarted := make(chan struct{}, 1)
	var child *abxbus.BaseEvent

	record := func(label string) {
		orderCh <- label
	}

	bus.On("Parent", "parent", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		record("parent_start")
		bus.Emit(abxbus.NewBaseEvent("Sibling", nil))
		select {
		case <-siblingStarted:
		case <-time.After(time.Second):
			return nil, errors.New("timed out waiting for sibling to start")
		}
		child = e.Emit(abxbus.NewBaseEvent("Child", nil))
		if _, err := child.Wait(); err != nil {
			return nil, err
		}
		record("parent_end")
		return "parent", nil
	}, nil)
	bus.On("Child", "child", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		record("child_start")
		time.Sleep(time.Millisecond)
		record("child_end")
		return "child", nil
	}, nil)
	bus.On("Sibling", "sibling", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		record("sibling_start")
		siblingStarted <- struct{}{}
		time.Sleep(time.Millisecond)
		record("sibling_end")
		return "sibling", nil
	}, nil)

	parent := bus.Emit(abxbus.NewBaseEvent("Parent", nil))
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}
	waitTimeout := 2.0
	if !bus.WaitUntilIdle(&waitTimeout) {
		t.Fatal("timed out waiting for bus to become idle")
	}
	close(orderCh)
	for label := range orderCh {
		order = append(order, label)
	}

	if baseEventIndexOf(order, "sibling_start") >= baseEventIndexOf(order, "child_start") {
		t.Fatalf("wait should wait in queue order, got %#v", order)
	}
	if baseEventIndexOf(order, "child_end") >= baseEventIndexOf(order, "parent_end") {
		t.Fatalf("parent should wait for child completion, got %#v", order)
	}
	if child == nil {
		t.Fatal("expected child event")
	}
	if child.EventBlocksParentCompletion {
		t.Fatalf("wait should not queue-jump or mark child as parent-blocking")
	}
}

func TestWaitIsPassiveInsideHandlersAndTimesOutForSerialEvents(t *testing.T) {
	bus := abxbus.NewEventBus("PassiveSerialEventCompletedBus", &abxbus.EventBusOptions{
		EventConcurrency: abxbus.EventConcurrencyBusSerial,
	})
	order := []string{}
	var orderMu sync.Mutex
	record := func(label string) {
		orderMu.Lock()
		defer orderMu.Unlock()
		order = append(order, label)
	}
	snapshot := func() []string {
		orderMu.Lock()
		defer orderMu.Unlock()
		return append([]string{}, order...)
	}

	bus.On("PassiveSerialParentEvent", "parent", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		record("parent_start")
		emitted := e.Emit(abxbus.NewBaseEvent("PassiveSerialEmittedEvent", nil))
		foundSource := e.Emit(abxbus.NewBaseEvent("PassiveSerialFoundEvent", nil))
		found, err := bus.Find("PassiveSerialFoundEvent", nil, &abxbus.FindOptions{Past: true, Future: false})
		if err != nil {
			return nil, err
		}
		if found == nil || found.EventID != foundSource.EventID {
			return nil, errors.New("expected to find queued serial event")
		}

		timeout := 0.02
		if _, err := emitted.Wait(&abxbus.EventWaitOptions{Timeout: &timeout}); err == nil || !strings.Contains(err.Error(), "deadline") {
			return nil, errors.New("emitted serial wait should time out")
		}
		record("emitted_timeout")
		if _, err := found.Wait(&abxbus.EventWaitOptions{Timeout: &timeout}); err == nil || !strings.Contains(err.Error(), "deadline") {
			return nil, errors.New("found serial wait should time out")
		}
		record("found_timeout")
		seen := strings.Join(snapshot(), ",")
		if strings.Contains(seen, "emitted_start") || strings.Contains(seen, "found_start") {
			return nil, errors.New("serial wait should not force child execution")
		}
		if emitted.EventBlocksParentCompletion || found.EventBlocksParentCompletion {
			return nil, errors.New("wait should not mark children as parent-blocking")
		}
		record("parent_end")
		return "parent", nil
	}, nil)
	bus.On("PassiveSerialEmittedEvent", "emitted", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		record("emitted_start")
		return "emitted", nil
	}, nil)
	bus.On("PassiveSerialFoundEvent", "found", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		record("found_start")
		return "found", nil
	}, nil)

	if _, err := bus.Emit(abxbus.NewBaseEvent("PassiveSerialParentEvent", nil)).Now(); err != nil {
		t.Fatal(err)
	}
	waitTimeout := 2.0
	if !bus.WaitUntilIdle(&waitTimeout) {
		t.Fatal("timed out waiting for bus to become idle")
	}
	expected := []string{"parent_start", "emitted_timeout", "found_timeout", "parent_end", "emitted_start", "found_start"}
	if got := snapshot(); strings.Join(got, ",") != strings.Join(expected, ",") {
		t.Fatalf("unexpected order: got %#v want %#v", got, expected)
	}
}

func TestWaitSerialWaitInsideHandlerTimesOutAndWarnsAboutSlowHandler(t *testing.T) {
	handlerSlowTimeout := 0.01
	noEventSlowWarning := 0.0
	bus := abxbus.NewEventBus("EventCompletedSerialDeadlockWarningBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventSlowTimeout:        &noEventSlowWarning,
		EventHandlerSlowTimeout: &handlerSlowTimeout,
	})
	order := []string{}
	var orderMu sync.Mutex
	record := func(label string) {
		orderMu.Lock()
		defer orderMu.Unlock()
		order = append(order, label)
	}
	snapshot := func() []string {
		orderMu.Lock()
		defer orderMu.Unlock()
		return append([]string{}, order...)
	}

	var warningMu sync.Mutex
	warnings := []string{}
	original := abxbus.SlowWarningLogger
	abxbus.SlowWarningLogger = func(message string) {
		warningMu.Lock()
		warnings = append(warnings, message)
		warningMu.Unlock()
	}
	defer func() { abxbus.SlowWarningLogger = original }()

	bus.On("EventCompletedSerialDeadlockWarningParentEvent", "parent", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		record("parent_start")
		child := e.Emit(abxbus.NewBaseEvent("EventCompletedSerialDeadlockWarningChildEvent", nil))
		found, err := bus.Find("EventCompletedSerialDeadlockWarningChildEvent", nil, &abxbus.FindOptions{Past: true, Future: false})
		if err != nil {
			return nil, err
		}
		if found == nil || found != child {
			return nil, errors.New("expected to find queued serial child event")
		}
		timeout := 0.05
		if _, err := found.Wait(&abxbus.EventWaitOptions{Timeout: &timeout}); err == nil || !strings.Contains(err.Error(), "deadline") {
			return nil, errors.New("serial child wait should time out")
		}
		record("child_timeout")
		if strings.Contains(strings.Join(snapshot(), ","), "child_start") {
			return nil, errors.New("serial wait should not force child execution")
		}
		if found.EventBlocksParentCompletion {
			return nil, errors.New("wait should not mark child as parent-blocking")
		}
		record("parent_end")
		return "parent", nil
	}, nil)
	bus.On("EventCompletedSerialDeadlockWarningChildEvent", "child", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		record("child_start")
		return "child", nil
	}, nil)

	if _, err := bus.Emit(abxbus.NewBaseEvent("EventCompletedSerialDeadlockWarningParentEvent", nil)).Now(); err != nil {
		t.Fatal(err)
	}
	waitTimeout := 2.0
	if !bus.WaitUntilIdle(&waitTimeout) {
		t.Fatal("timed out waiting for bus to become idle")
	}
	expected := []string{"parent_start", "child_timeout", "parent_end", "child_start"}
	if got := snapshot(); strings.Join(got, ",") != strings.Join(expected, ",") {
		t.Fatalf("unexpected order: got %#v want %#v", got, expected)
	}
	warningMu.Lock()
	defer warningMu.Unlock()
	if !baseEventContainsString(warnings, "Slow event handler") {
		t.Fatalf("expected slow handler warning, got %#v", warnings)
	}
}

func TestWaitWaitsForNormalParallelProcessingInsideHandlers(t *testing.T) {
	bus := abxbus.NewEventBus("PassiveParallelEventCompletedBus", &abxbus.EventBusOptions{
		EventConcurrency: abxbus.EventConcurrencyBusSerial,
	})
	order := []string{}
	var orderMu sync.Mutex
	record := func(label string) {
		orderMu.Lock()
		defer orderMu.Unlock()
		order = append(order, label)
	}
	snapshot := func() []string {
		orderMu.Lock()
		defer orderMu.Unlock()
		return append([]string{}, order...)
	}

	bus.On("PassiveParallelParentEvent", "parent", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		record("parent_start")
		emittedEvent := abxbus.NewBaseEvent("PassiveParallelEmittedEvent", nil)
		emittedEvent.EventConcurrency = abxbus.EventConcurrencyParallel
		emitted := e.Emit(emittedEvent)
		foundEvent := abxbus.NewBaseEvent("PassiveParallelFoundEvent", nil)
		foundEvent.EventConcurrency = abxbus.EventConcurrencyParallel
		foundSource := e.Emit(foundEvent)
		found, err := bus.Find("PassiveParallelFoundEvent", nil, &abxbus.FindOptions{Past: true, Future: false})
		if err != nil {
			return nil, err
		}
		if found == nil || found.EventID != foundSource.EventID {
			return nil, errors.New("expected to find queued parallel event")
		}

		timeout := 1.0
		if _, err := emitted.Wait(&abxbus.EventWaitOptions{Timeout: &timeout}); err != nil {
			return nil, err
		}
		record("emitted_completed")
		if _, err := found.Wait(&abxbus.EventWaitOptions{Timeout: &timeout}); err != nil {
			return nil, err
		}
		record("found_completed")
		if emitted.EventBlocksParentCompletion || found.EventBlocksParentCompletion {
			return nil, errors.New("wait should not mark children as parent-blocking")
		}
		record("parent_end")
		return "parent", nil
	}, nil)
	bus.On("PassiveParallelEmittedEvent", "emitted", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		record("emitted_start")
		time.Sleep(time.Millisecond)
		record("emitted_end")
		return "emitted", nil
	}, nil)
	bus.On("PassiveParallelFoundEvent", "found", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		record("found_start")
		time.Sleep(time.Millisecond)
		record("found_end")
		return "found", nil
	}, nil)

	if _, err := bus.Emit(abxbus.NewBaseEvent("PassiveParallelParentEvent", nil)).Now(); err != nil {
		t.Fatal(err)
	}
	waitTimeout := 2.0
	if !bus.WaitUntilIdle(&waitTimeout) {
		t.Fatal("timed out waiting for bus to become idle")
	}
	order = snapshot()
	if baseEventIndexOf(order, "emitted_end") >= baseEventIndexOf(order, "emitted_completed") {
		t.Fatalf("emitted parallel event should complete before parent resumes, got %#v", order)
	}
	if baseEventIndexOf(order, "found_end") >= baseEventIndexOf(order, "found_completed") {
		t.Fatalf("found parallel event should complete before parent resumes, got %#v", order)
	}
	if order[len(order)-1] != "parent_end" {
		t.Fatalf("parent should resume after parallel event completion, got %#v", order)
	}
}

func TestWaitWaitsForFutureParallelEventFoundAfterHandlerStarts(t *testing.T) {
	bus := abxbus.NewEventBus("FutureParallelEventCompletedBus", &abxbus.EventBusOptions{
		EventConcurrency: abxbus.EventConcurrencyBusSerial,
	})
	otherStarted := make(chan struct{})
	releaseFind := make(chan struct{})
	parallelStarted := make(chan struct{})
	continued := make(chan struct{})
	waitedFor := make(chan time.Duration, 1)

	bus.On("FutureParallelSomeOtherEvent", "other", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		close(otherStarted)
		<-releaseFind
		found, err := bus.Find("FutureParallelEvent", nil, &abxbus.FindOptions{Past: true, Future: false})
		if err != nil {
			return nil, err
		}
		if found == nil {
			return nil, errors.New("expected to find pending parallel event")
		}
		startedAt := time.Now()
		timeout := 1.0
		if _, err := found.Wait(&abxbus.EventWaitOptions{Timeout: &timeout}); err != nil {
			return nil, err
		}
		waitedFor <- time.Since(startedAt)
		close(continued)
		return "other", nil
	}, nil)
	bus.On("FutureParallelEvent", "parallel", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		close(parallelStarted)
		time.Sleep(250 * time.Millisecond)
		return "parallel", nil
	}, nil)

	other := bus.Emit(abxbus.NewBaseEvent("FutureParallelSomeOtherEvent", nil))
	select {
	case <-otherStarted:
	case <-time.After(time.Second):
		t.Fatal("other handler did not start")
	}
	parallel := abxbus.NewBaseEvent("FutureParallelEvent", nil)
	parallel.EventConcurrency = abxbus.EventConcurrencyParallel
	bus.Emit(parallel)
	select {
	case <-parallelStarted:
	case <-time.After(time.Second):
		t.Fatal("parallel handler did not start")
	}
	close(releaseFind)
	select {
	case <-continued:
	case <-time.After(time.Second):
		t.Fatal("other handler did not continue after parallel event completion")
	}
	if _, err := other.Now(); err != nil {
		t.Fatal(err)
	}
	waitTimeout := 2.0
	if !bus.WaitUntilIdle(&waitTimeout) {
		t.Fatal("timed out waiting for bus to become idle")
	}
	if waited := <-waitedFor; waited < 150*time.Millisecond {
		t.Fatalf("wait returned too early; waited %s", waited)
	}
}

func TestWaitReturnsEventAcceptsTimeoutAndRejectsUnattachedPendingEvent(t *testing.T) {
	timeout := 0.01
	if _, err := abxbus.NewBaseEvent("EventCompletedPendingNoBusEvent", nil).Wait(
		&abxbus.EventWaitOptions{Timeout: &timeout},
	); err == nil || !strings.Contains(err.Error(), "no bus attached") {
		t.Fatalf("Wait should reject unattached pending events, got %v", err)
	}

	completed := abxbus.NewBaseEvent("EventCompletedCompletedNoBusEvent", nil)
	completed.EventStatus = "completed"
	if event, err := completed.Wait(&abxbus.EventWaitOptions{Timeout: &timeout}); err != nil || event != completed {
		t.Fatalf("Wait should return completed event without bus; event=%#v err=%v", event, err)
	}

	bus := abxbus.NewEventBus("EventCompletedTimeoutBus", &abxbus.EventBusOptions{
		EventConcurrency: abxbus.EventConcurrencyBusSerial,
	})
	releaseHandler := make(chan struct{})
	bus.On("EventCompletedTimeoutEvent", "slow", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		<-releaseHandler
		return nil, nil
	}, nil)
	event := bus.Emit(abxbus.NewBaseEvent("EventCompletedTimeoutEvent", nil))
	if _, err := event.Wait(&abxbus.EventWaitOptions{Timeout: &timeout}); err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("Wait should time out, got %v", err)
	}
	close(releaseHandler)
	longTimeout := 1.0
	if completedEvent, err := event.Wait(&abxbus.EventWaitOptions{Timeout: &longTimeout}); err != nil || completedEvent != event {
		t.Fatalf("Wait should return event after completion; event=%#v err=%v", completedEvent, err)
	}
}

func baseEventIndexOf(values []string, needle string) int {
	for idx, value := range values {
		if value == needle {
			return idx
		}
	}
	return len(values)
}

func baseEventContainsString(values []string, needle string) bool {
	for _, value := range values {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}
