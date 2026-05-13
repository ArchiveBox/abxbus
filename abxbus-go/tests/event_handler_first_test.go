package abxbus_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"
)

func TestEventHandlerCompletionBusDefaultFirstSerial(t *testing.T) {
	bus := abxbus.NewEventBus("CompletionDefaultFirstBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionFirst,
	})
	secondHandlerCalled := false

	bus.On("CompletionDefaultFirstEvent", "first_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "first", nil
	}, nil)
	bus.On("CompletionDefaultFirstEvent", "second_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		secondHandlerCalled = true
		return "second", nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("CompletionDefaultFirstEvent", nil))
	if event.EventHandlerCompletion != "" {
		t.Fatalf("event_handler_completion should be unset on the event, got %s", event.EventHandlerCompletion)
	}
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	if event.EventHandlerCompletion != "" {
		t.Fatalf("event_handler_completion should stay unset on the event, got %s", event.EventHandlerCompletion)
	}
	if secondHandlerCalled {
		t.Fatal("bus default first completion should skip the second serial handler")
	}
	result, err := event.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false, RaiseIfNone: false})
	if err != nil || result != "first" {
		t.Fatalf("expected first result, got %#v err=%v", result, err)
	}
	if firstResult := firstSliceEventResultByHandlerName(t, event, "first_handler"); firstResult.Status != abxbus.EventResultCompleted {
		t.Fatalf("first handler should complete, got %#v", firstResult)
	}
	if secondResult := firstSliceEventResultByHandlerName(t, event, "second_handler"); secondResult.Status != abxbus.EventResultError {
		t.Fatalf("second handler should be cancelled, got %#v", secondResult)
	}
}

func TestEventHandlerCompletionExplicitOverrideBeatsBusDefault(t *testing.T) {
	bus := abxbus.NewEventBus("CompletionOverrideBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionFirst,
	})
	secondHandlerCalled := false

	bus.On("CompletionOverrideEvent", "first_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "first", nil
	}, nil)
	bus.On("CompletionOverrideEvent", "second_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		secondHandlerCalled = true
		return "second", nil
	}, nil)

	event := abxbus.NewBaseEvent("CompletionOverrideEvent", nil)
	event.EventHandlerCompletion = abxbus.EventHandlerCompletionAll
	emitted := bus.Emit(event)
	if emitted.EventHandlerCompletion != abxbus.EventHandlerCompletionAll {
		t.Fatalf("expected explicit completion=all, got %s", emitted.EventHandlerCompletion)
	}
	if _, err := emitted.Now(); err != nil {
		t.Fatal(err)
	}
	if !secondHandlerCalled {
		t.Fatal("explicit event completion=all should beat bus default first")
	}
}

func TestEventParallelFirstRacesAndCancelsNonWinners(t *testing.T) {
	bus := abxbus.NewEventBus("CompletionParallelFirstBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
	})
	var slowStarted atomic.Bool

	bus.On("CompletionParallelFirstEvent", "slow_handler_started", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		slowStarted.Store(true)
		select {
		case <-time.After(500 * time.Millisecond):
			return "slow-started", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	bus.On("CompletionParallelFirstEvent", "fast_winner", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(10 * time.Millisecond)
		return "winner", nil
	}, nil)
	bus.On("CompletionParallelFirstEvent", "slow_handler_pending_or_started", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		select {
		case <-time.After(500 * time.Millisecond):
			return "slow-other", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)

	event := abxbus.NewBaseEvent("CompletionParallelFirstEvent", nil)
	event.EventHandlerConcurrency = abxbus.EventHandlerConcurrencyParallel
	event.EventHandlerCompletion = abxbus.EventHandlerCompletionFirst
	emitted := bus.Emit(event)

	started := time.Now()
	if _, err := emitted.Now(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed >= 200*time.Millisecond {
		t.Fatalf("first completion should finish quickly, elapsed=%s", elapsed)
	}
	if !slowStarted.Load() {
		t.Fatal("parallel first mode should start the slow handler before cancelling non-winners")
	}
	timeout := 1.0
	if _, err := emitted.Wait(&abxbus.EventWaitOptions{Timeout: &timeout}); err != nil {
		t.Fatal(err)
	}
	if !waitForFirstSliceErrorResults(emitted, 2, 200*time.Millisecond) {
		t.Fatalf("expected two loser error results, got %#v", emitted.EventResults)
	}

	winnerResult := firstSliceEventResultByHandlerName(t, emitted, "fast_winner")
	if winnerResult.Status != abxbus.EventResultCompleted || winnerResult.Result != "winner" || winnerResult.Error != nil {
		t.Fatalf("unexpected winner result: %#v", winnerResult)
	}
	for _, loser := range firstSliceEventResultsExceptHandlerName(emitted, "fast_winner") {
		if loser.Status != abxbus.EventResultError {
			t.Fatalf("loser should be cancelled with error status, got %#v", loser)
		}
	}
	result, err := emitted.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false, RaiseIfNone: true})
	if err != nil || result != "winner" {
		t.Fatalf("expected winner result after loser cancellation; result=%#v err=%v", result, err)
	}
}

func TestEventHandlerCompletionExplicitFirstCancelsParallelLosers(t *testing.T) {
	bus := abxbus.NewEventBus("CompletionFirstShortcutBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
	})
	var slowHandlerCompleted atomic.Bool

	bus.On("CompletionFirstShortcutEvent", "fast_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(10 * time.Millisecond)
		return "fast", nil
	}, nil)
	bus.On("CompletionFirstShortcutEvent", "slow_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		select {
		case <-time.After(500 * time.Millisecond):
			slowHandlerCompleted.Store(true)
			return "slow", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)

	event := abxbus.NewBaseEvent("CompletionFirstShortcutEvent", nil)
	event.EventHandlerCompletion = abxbus.EventHandlerCompletionFirst
	emitted := bus.Emit(event)
	if emitted.EventHandlerCompletion != abxbus.EventHandlerCompletionFirst {
		t.Fatalf("expected explicit completion=first, got %s", emitted.EventHandlerCompletion)
	}
	if _, err := emitted.Now(&abxbus.EventWaitOptions{FirstResult: true}); err != nil {
		t.Fatal(err)
	}
	result, err := emitted.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false})
	if err != nil || result != "fast" {
		t.Fatalf("expected fast result, got %#v err=%v", result, err)
	}
	timeout := 1.0
	if _, err := emitted.Wait(&abxbus.EventWaitOptions{Timeout: &timeout}); err != nil {
		t.Fatal(err)
	}
	if slowHandlerCompleted.Load() {
		t.Fatal("slow handler should be cancelled before completing")
	}
	if !waitForFirstSliceErrorResults(emitted, 1, 200*time.Millisecond) {
		t.Fatal("first completion should leave cancelled loser error results")
	}
}

func TestEventHandlerCompletionFirstPreservesFalsyResults(t *testing.T) {
	bus := abxbus.NewEventBus("CompletionFalsyBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
	})
	secondHandlerCalled := false

	bus.On("IntCompletionEvent", "zero_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return 0, nil
	}, nil)
	bus.On("IntCompletionEvent", "second_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		secondHandlerCalled = true
		return 99, nil
	}, nil)

	event := abxbus.NewBaseEvent("IntCompletionEvent", nil)
	event.EventHandlerCompletion = abxbus.EventHandlerCompletionFirst
	emitted := bus.Emit(event)
	if _, err := emitted.Now(&abxbus.EventWaitOptions{FirstResult: true}); err != nil {
		t.Fatal(err)
	}
	result, err := emitted.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false})
	if err != nil || result != 0 {
		t.Fatalf("expected zero result, got %#v err=%v", result, err)
	}
	if secondHandlerCalled {
		t.Fatal("zero should count as a first result and skip the later handler")
	}
}

func TestEventHandlerCompletionFirstPreservesFalseAndEmptyStringResults(t *testing.T) {
	boolBus := abxbus.NewEventBus("CompletionFalsyFalseBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
	})
	boolSecondHandlerCalled := false
	boolBus.On("BoolCompletionEvent", "bool_first_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return false, nil
	}, nil)
	boolBus.On("BoolCompletionEvent", "bool_second_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		boolSecondHandlerCalled = true
		return true, nil
	}, nil)

	boolEvent := abxbus.NewBaseEvent("BoolCompletionEvent", nil)
	boolEvent.EventHandlerCompletion = abxbus.EventHandlerCompletionFirst
	emittedBool := boolBus.Emit(boolEvent)
	if _, err := emittedBool.Now(&abxbus.EventWaitOptions{FirstResult: true}); err != nil {
		t.Fatal(err)
	}
	boolResult, err := emittedBool.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false})
	if err != nil || boolResult != false {
		t.Fatalf("expected false result, got %#v err=%v", boolResult, err)
	}
	if boolSecondHandlerCalled {
		t.Fatal("false should count as a first result and skip the later handler")
	}

	strBus := abxbus.NewEventBus("CompletionFalsyEmptyStringBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
	})
	strSecondHandlerCalled := false
	strBus.On("StrCompletionEvent", "str_first_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "", nil
	}, nil)
	strBus.On("StrCompletionEvent", "str_second_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		strSecondHandlerCalled = true
		return "second", nil
	}, nil)

	strEvent := abxbus.NewBaseEvent("StrCompletionEvent", nil)
	strEvent.EventHandlerCompletion = abxbus.EventHandlerCompletionFirst
	emittedStr := strBus.Emit(strEvent)
	if _, err := emittedStr.Now(&abxbus.EventWaitOptions{FirstResult: true}); err != nil {
		t.Fatal(err)
	}
	strResult, err := emittedStr.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false})
	if err != nil || strResult != "" {
		t.Fatalf("expected empty string result, got %#v err=%v", strResult, err)
	}
	if strSecondHandlerCalled {
		t.Fatal("empty string should count as a first result and skip the later handler")
	}
}

func TestEventHandlerCompletionFirstSkipsNoneResultAndUsesNextWinner(t *testing.T) {
	bus := abxbus.NewEventBus("CompletionNoneSkipBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
	})
	thirdHandlerCalled := false
	bus.On("CompletionNoneSkipEvent", "none_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, nil
	}, nil)
	bus.On("CompletionNoneSkipEvent", "winner_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "winner", nil
	}, nil)
	bus.On("CompletionNoneSkipEvent", "third_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		thirdHandlerCalled = true
		return "third", nil
	}, nil)

	event := abxbus.NewBaseEvent("CompletionNoneSkipEvent", nil)
	event.EventHandlerCompletion = abxbus.EventHandlerCompletionFirst
	emitted := bus.Emit(event)
	if _, err := emitted.Now(&abxbus.EventWaitOptions{FirstResult: true}); err != nil {
		t.Fatal(err)
	}
	result, err := emitted.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false})
	if err != nil || result != "winner" {
		t.Fatalf("expected first non-nil result, got %#v err=%v", result, err)
	}
	if thirdHandlerCalled {
		t.Fatal("event_handler_completion=first should skip later serial handlers after the first non-nil result")
	}
	noneResult := firstSliceEventResultByHandlerName(t, emitted, "none_handler")
	winnerResult := firstSliceEventResultByHandlerName(t, emitted, "winner_handler")
	if noneResult.Status != abxbus.EventResultCompleted || noneResult.Result != nil {
		t.Fatalf("nil handler should complete with nil result, got %#v", noneResult)
	}
	if winnerResult.Status != abxbus.EventResultCompleted || winnerResult.Result != "winner" {
		t.Fatalf("winner handler should complete with winner result, got %#v", winnerResult)
	}
}

func TestEventHandlerCompletionFirstSkipsBaseEventResultAndUsesNextWinner(t *testing.T) {
	bus := abxbus.NewEventBus("CompletionBaseEventSkipBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
	})
	thirdHandlerCalled := false
	bus.On("CompletionBaseEventSkipEvent", "baseevent_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return abxbus.NewBaseEvent("ChildCompletionEvent", nil), nil
	}, nil)
	bus.On("CompletionBaseEventSkipEvent", "winner_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "winner", nil
	}, nil)
	bus.On("CompletionBaseEventSkipEvent", "third_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		thirdHandlerCalled = true
		return "third", nil
	}, nil)

	event := abxbus.NewBaseEvent("CompletionBaseEventSkipEvent", nil)
	event.EventHandlerCompletion = abxbus.EventHandlerCompletionFirst
	emitted := bus.Emit(event)
	if _, err := emitted.Now(&abxbus.EventWaitOptions{FirstResult: true}); err != nil {
		t.Fatal(err)
	}
	result, err := emitted.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false})
	if err != nil || result != "winner" {
		t.Fatalf("expected BaseEvent result to be skipped; result=%#v err=%v", result, err)
	}
	if thirdHandlerCalled {
		t.Fatal("event_handler_completion=first should skip later serial handlers after the scalar winner")
	}
	if baseEventResult := firstSliceEventResultByHandlerName(t, emitted, "baseevent_handler"); !isFirstSliceBaseEventResult(baseEventResult.Result) {
		t.Fatalf("raw event result should keep the BaseEvent value, got %#v", baseEventResult.Result)
	}
}

func TestNowRunsAllHandlersAndEventResultReturnsFirstValidResult(t *testing.T) {
	bus := abxbus.NewEventBus("CompletionNowAllBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionFirst,
	})
	lateCalled := false
	bus.On("CompletionNowAllEvent", "baseevent_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return abxbus.NewBaseEvent("NowAllChildEvent", nil), nil
	}, nil)
	bus.On("CompletionNowAllEvent", "none_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, nil
	}, nil)
	bus.On("CompletionNowAllEvent", "winner_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "winner", nil
	}, nil)
	bus.On("CompletionNowAllEvent", "late_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		lateCalled = true
		return "late", nil
	}, nil)

	event := abxbus.NewBaseEvent("CompletionNowAllEvent", nil)
	event.EventHandlerCompletion = abxbus.EventHandlerCompletionAll
	completed, err := bus.Emit(event).Now()
	if err != nil {
		t.Fatal(err)
	}
	result, err := completed.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false})
	if err != nil || result != "winner" {
		t.Fatalf("expected first valid result, got %#v err=%v", result, err)
	}
	if completed.EventHandlerCompletion != abxbus.EventHandlerCompletionAll {
		t.Fatalf("Now should not change event_handler_completion, got %s", completed.EventHandlerCompletion)
	}
	if !lateCalled {
		t.Fatal("Now should let later handlers run")
	}
}

func TestEventNowDefaultErrorPolicy(t *testing.T) {
	noHandlerBus := abxbus.NewEventBus("CompletionNowNoHandlerBus", nil)
	noHandlerEvent, err := noHandlerBus.Emit(abxbus.NewBaseEvent("CompletionNowErrorPolicyEvent", nil)).Now()
	if err != nil {
		t.Fatal(err)
	}
	if result, err := noHandlerEvent.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false, RaiseIfNone: false}); err != nil || result != nil {
		t.Fatalf("no-handler result should be nil without errors; result=%#v err=%v", result, err)
	}

	noneBus := abxbus.NewEventBus("CompletionNowNoneBus", nil)
	noneBus.On("CompletionNowErrorPolicyEvent", "none_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, nil
	}, nil)
	noneEvent, err := noneBus.Emit(abxbus.NewBaseEvent("CompletionNowErrorPolicyEvent", nil)).Now()
	if err != nil {
		t.Fatal(err)
	}
	if result, err := noneEvent.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false, RaiseIfNone: false}); err != nil || result != nil {
		t.Fatalf("none result should be nil without errors; result=%#v err=%v", result, err)
	}

	allErrorBus := abxbus.NewEventBus("CompletionNowAllErrorBus", nil)
	allErrorBus.On("CompletionNowErrorPolicyEvent", "fail_one", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, assertErr("now boom 1")
	}, nil)
	allErrorBus.On("CompletionNowErrorPolicyEvent", "fail_two", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, assertErr("now boom 2")
	}, nil)
	allErrorEvent, err := allErrorBus.Emit(abxbus.NewBaseEvent("CompletionNowErrorPolicyEvent", nil)).Now()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := allErrorEvent.EventResult(); err == nil || !strings.Contains(err.Error(), "2 handler error") {
		t.Fatalf("default EventResult should surface all handler errors, got %v", err)
	}

	mixedValidBus := abxbus.NewEventBus("CompletionNowMixedValidBus", nil)
	mixedValidBus.On("CompletionNowErrorPolicyEvent", "fail_one", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, assertErr("now boom 1")
	}, nil)
	mixedValidBus.On("CompletionNowErrorPolicyEvent", "winner", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "winner", nil
	}, nil)
	mixedValidEvent, err := mixedValidBus.Emit(abxbus.NewBaseEvent("CompletionNowErrorPolicyEvent", nil)).Now()
	if err != nil {
		t.Fatal(err)
	}
	if result, err := mixedValidEvent.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false}); err != nil || result != "winner" {
		t.Fatalf("RaiseIfAny=false should return the valid result; result=%#v err=%v", result, err)
	}

	mixedNoneBus := abxbus.NewEventBus("CompletionNowMixedNoneBus", nil)
	mixedNoneBus.On("CompletionNowErrorPolicyEvent", "fail_one", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, assertErr("now boom 1")
	}, nil)
	mixedNoneBus.On("CompletionNowErrorPolicyEvent", "none_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, nil
	}, nil)
	mixedNoneEvent, err := mixedNoneBus.Emit(abxbus.NewBaseEvent("CompletionNowErrorPolicyEvent", nil)).Now()
	if err != nil {
		t.Fatal(err)
	}
	if result, err := mixedNoneEvent.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false, RaiseIfNone: false}); err != nil || result != nil {
		t.Fatalf("mixed error/none should return nil when errors are suppressed; result=%#v err=%v", result, err)
	}
}

func TestEventResultOptionsMatchEventResultsShape(t *testing.T) {
	bus := abxbus.NewEventBus("CompletionNowOptionsBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	bus.On("CompletionNowOptionsEvent", "fail_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, assertErr("now option boom")
	}, nil)
	bus.On("CompletionNowOptionsEvent", "first_valid", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "first", nil
	}, nil)
	bus.On("CompletionNowOptionsEvent", "second_valid", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "second", nil
	}, nil)

	event, err := bus.Emit(abxbus.NewBaseEvent("CompletionNowOptionsEvent", nil)).Now()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := event.EventResult(&abxbus.EventResultOptions{RaiseIfAny: true}); err == nil || !strings.Contains(err.Error(), "now option boom") {
		t.Fatalf("RaiseIfAny=true should surface handler errors, got %v", err)
	}

	filteredEvent, err := bus.Emit(abxbus.NewBaseEvent("CompletionNowOptionsEvent", nil)).Now()
	if err != nil {
		t.Fatal(err)
	}
	result, err := filteredEvent.EventResult(&abxbus.EventResultOptions{
		RaiseIfAny: false,
		Include: func(result any, eventResult *abxbus.EventResult) bool {
			if result != eventResult.Result {
				t.Fatalf("include should receive matching result and EventResult, got %#v vs %#v", result, eventResult.Result)
			}
			return eventResult.Status == abxbus.EventResultCompleted && eventResult.Result == "second"
		},
	})
	if err != nil || result != "second" {
		t.Fatalf("expected Include filter to select second result; result=%#v err=%v", result, err)
	}
}

func TestEventResultReturnsFirstValidResultByRegistrationOrderAfterNow(t *testing.T) {
	bus := abxbus.NewEventBus("CompletionNowRegistrationOrderBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	bus.On("CompletionNowRegistrationOrderEvent", "slow_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(50 * time.Millisecond)
		return "slow", nil
	}, nil)
	bus.On("CompletionNowRegistrationOrderEvent", "fast_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(time.Millisecond)
		return "fast", nil
	}, nil)

	event, err := bus.Emit(abxbus.NewBaseEvent("CompletionNowRegistrationOrderEvent", nil)).Now()
	if err != nil {
		t.Fatal(err)
	}
	result, err := event.EventResult()
	if err != nil || result != "slow" {
		t.Fatalf("expected first registered valid result, result=%#v err=%v", result, err)
	}
}

func TestEventHandlerCompletionFirstReturnsNoneWhenAllHandlersFail(t *testing.T) {
	bus := abxbus.NewEventBus("CompletionAllFailBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	bus.On("CompletionAllFailEvent", "fail_fast", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, assertErr("boom1")
	}, nil)
	bus.On("CompletionAllFailEvent", "fail_slow", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(10 * time.Millisecond)
		return nil, assertErr("boom2")
	}, nil)

	event := abxbus.NewBaseEvent("CompletionAllFailEvent", nil)
	event.EventHandlerCompletion = abxbus.EventHandlerCompletionFirst
	emitted := bus.Emit(event)
	if _, err := emitted.Now(&abxbus.EventWaitOptions{FirstResult: true}); err != nil {
		t.Fatal(err)
	}
	result, err := emitted.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false, RaiseIfNone: false})
	if err != nil || result != nil {
		t.Fatalf("all-fail first completion should return nil with suppressed errors; result=%#v err=%v", result, err)
	}
}

func TestEventHandlerCompletionFirstResultOptionsMatchEventResultOptions(t *testing.T) {
	bus := abxbus.NewEventBus("CompletionFirstOptionsBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	bus.On("CompletionFirstOptionsEvent", "fail_fast", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, assertErr("first option boom")
	}, nil)
	bus.On("CompletionFirstOptionsEvent", "slow_winner", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(10 * time.Millisecond)
		return "winner", nil
	}, nil)

	event := abxbus.NewBaseEvent("CompletionFirstOptionsEvent", nil)
	event.EventHandlerCompletion = abxbus.EventHandlerCompletionFirst
	emitted := bus.Emit(event)
	if _, err := emitted.Now(&abxbus.EventWaitOptions{FirstResult: true}); err != nil {
		t.Fatal(err)
	}
	result, err := emitted.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false})
	if err != nil || result != "winner" {
		t.Fatalf("RaiseIfAny=false should return winner; result=%#v err=%v", result, err)
	}

	errorEvent := abxbus.NewBaseEvent("CompletionFirstOptionsEvent", nil)
	errorEvent.EventHandlerCompletion = abxbus.EventHandlerCompletionFirst
	emittedError := bus.Emit(errorEvent)
	if _, err := emittedError.Now(&abxbus.EventWaitOptions{FirstResult: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := emittedError.EventResult(&abxbus.EventResultOptions{RaiseIfAny: true}); err == nil || !strings.Contains(err.Error(), "first option boom") {
		t.Fatalf("RaiseIfAny=true should surface first option boom, got %v", err)
	}

	noneBus := abxbus.NewEventBus("CompletionFirstRaiseNoneBus", nil)
	noneBus.On("CompletionFirstRaiseNoneEvent", "none_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, nil
	}, nil)
	noneEvent := abxbus.NewBaseEvent("CompletionFirstRaiseNoneEvent", nil)
	noneEvent.EventHandlerCompletion = abxbus.EventHandlerCompletionFirst
	emittedNone := noneBus.Emit(noneEvent)
	if _, err := emittedNone.Now(&abxbus.EventWaitOptions{FirstResult: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := emittedNone.EventResult(&abxbus.EventResultOptions{RaiseIfNone: true}); err == nil || !strings.Contains(err.Error(), "Expected at least one handler") {
		t.Fatalf("RaiseIfNone=true should reject no-result first completion, got %v", err)
	}
}

func TestNowFirstResultTimeoutLimitsProcessingWait(t *testing.T) {
	noTimeout := 0.0
	bus := abxbus.NewEventBus("CompletionFirstTimeoutBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventTimeout:            &noTimeout,
	})
	bus.On("CompletionFirstTimeoutEvent", "slow_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(500 * time.Millisecond)
		return "slow", nil
	}, nil)

	event := abxbus.NewBaseEvent("CompletionFirstTimeoutEvent", nil)
	event.EventHandlerCompletion = abxbus.EventHandlerCompletionFirst
	timeout := 0.01
	_, err := bus.Emit(event).Now(&abxbus.EventWaitOptions{Timeout: &timeout, FirstResult: true})
	if err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("Now(first_result=true) should time out while waiting for processing, got %v", err)
	}
}

func TestEventResultIncludeCallbackReceivesResultAndEventResult(t *testing.T) {
	bus := abxbus.NewEventBus("CompletionFirstIncludeBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	seenHandlerNames := []string{}
	seenResults := []any{}
	bus.On("CompletionFirstIncludeEvent", "none_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, nil
	}, nil)
	bus.On("CompletionFirstIncludeEvent", "second_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "second", nil
	}, nil)

	event := abxbus.NewBaseEvent("CompletionFirstIncludeEvent", nil)
	event.EventHandlerCompletion = abxbus.EventHandlerCompletionFirst
	emitted := bus.Emit(event)
	if _, err := emitted.Now(&abxbus.EventWaitOptions{FirstResult: true}); err != nil {
		t.Fatal(err)
	}
	result, err := emitted.EventResult(&abxbus.EventResultOptions{
		RaiseIfAny: false,
		Include: func(result any, eventResult *abxbus.EventResult) bool {
			if result != eventResult.Result {
				t.Fatalf("include should receive the unwrapped result and matching EventResult, got %#v vs %#v", result, eventResult.Result)
			}
			seenResults = append(seenResults, result)
			seenHandlerNames = append(seenHandlerNames, eventResult.HandlerName)
			return eventResult.Status == abxbus.EventResultCompleted && result == "second"
		},
	})
	if err != nil || result != "second" {
		t.Fatalf("expected include filter to select second result; result=%#v err=%v", result, err)
	}
	if len(seenResults) < 2 || seenResults[len(seenResults)-2] != nil || seenResults[len(seenResults)-1] != "second" {
		t.Fatalf("include should see nil then second results, got %v", seenResults)
	}
	if strings.Join(seenHandlerNames[len(seenHandlerNames)-2:], ",") != "none_handler,second_handler" {
		t.Fatalf("include should receive EventResult in handler order, got %v", seenHandlerNames)
	}
}

func TestEventResultsIncludeCallbackReceivesResultAndEventResult(t *testing.T) {
	bus := abxbus.NewEventBus("CompletionResultsListIncludeBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	seenPairs := []string{}
	bus.On("CompletionResultsListIncludeEvent", "keep_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "keep", nil
	}, nil)
	bus.On("CompletionResultsListIncludeEvent", "drop_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "drop", nil
	}, nil)

	event, err := bus.Emit(abxbus.NewBaseEvent("CompletionResultsListIncludeEvent", nil)).Now()
	if err != nil {
		t.Fatal(err)
	}
	results, err := event.EventResultsList(&abxbus.EventResultOptions{
		RaiseIfAny:  false,
		RaiseIfNone: true,
		Include: func(result any, eventResult *abxbus.EventResult) bool {
			if result != eventResult.Result {
				t.Fatalf("include should receive the unwrapped result and matching EventResult, got %#v vs %#v", result, eventResult.Result)
			}
			seenPairs = append(seenPairs, fmt.Sprintf("%v:%s", result, eventResult.HandlerName))
			return result == "keep"
		},
	})
	if err != nil || len(results) != 1 || results[0] != "keep" {
		t.Fatalf("expected only keep result; results=%#v err=%v", results, err)
	}
	if strings.Join(seenPairs, ",") != "keep:keep_handler,drop:drop_handler" {
		t.Fatalf("include should see both results in order, got %v", seenPairs)
	}
}

func TestEventResultReturnsFirstCurrentResultWithFirstResultWait(t *testing.T) {
	bus := abxbus.NewEventBus("CompletionFirstCurrentResultBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	bus.On("CompletionFirstCurrentResultEvent", "slow_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(50 * time.Millisecond)
		return "slow", nil
	}, nil)
	bus.On("CompletionFirstCurrentResultEvent", "fast_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(time.Millisecond)
		return "fast", nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("CompletionFirstCurrentResultEvent", nil))
	if _, err := event.Now(&abxbus.EventWaitOptions{FirstResult: true}); err != nil {
		t.Fatal(err)
	}
	result, err := event.EventResult()
	if err != nil || result != "fast" {
		t.Fatalf("expected first current result after first_result wait; result=%#v err=%v", result, err)
	}
}

func TestEventResultRaiseIfAnyIncludesFirstModeControlErrors(t *testing.T) {
	bus := abxbus.NewEventBus("CompletionFirstControlErrorBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
	})
	bus.On("CompletionFirstControlErrorEvent", "fast_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(10 * time.Millisecond)
		return "fast", nil
	}, nil)
	bus.On("CompletionFirstControlErrorEvent", "slow_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		select {
		case <-time.After(500 * time.Millisecond):
			return "slow", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)

	event := abxbus.NewBaseEvent("CompletionFirstControlErrorEvent", nil)
	event.EventHandlerCompletion = abxbus.EventHandlerCompletionFirst
	emitted := bus.Emit(event)
	if _, err := emitted.Now(&abxbus.EventWaitOptions{FirstResult: true}); err != nil {
		t.Fatal(err)
	}
	if !waitForFirstSliceErrorResults(emitted, 1, 200*time.Millisecond) {
		t.Fatalf("expected first-mode loser error result, got %#v", emitted.EventResults)
	}
	if _, err := emitted.EventResult(&abxbus.EventResultOptions{RaiseIfAny: true}); err == nil || !strings.Contains(strings.ToLower(err.Error()), "first") {
		t.Fatalf("RaiseIfAny=true should include first-mode cancellation errors, got %v", err)
	}
	result, err := emitted.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false, RaiseIfNone: true})
	if err != nil || result != "fast" {
		t.Fatalf("RaiseIfAny=false should still return fast; result=%#v err=%v", result, err)
	}
}

func firstSliceEventResultByHandlerName(t *testing.T, event *abxbus.BaseEvent, handlerName string) *abxbus.EventResult {
	t.Helper()
	for _, result := range event.EventResults {
		if result.HandlerName == handlerName {
			return result
		}
	}
	t.Fatalf("missing event result for handler %q; results=%#v", handlerName, event.EventResults)
	return nil
}

func firstSliceEventResultsExceptHandlerName(event *abxbus.BaseEvent, handlerName string) []*abxbus.EventResult {
	results := []*abxbus.EventResult{}
	for _, result := range event.EventResults {
		if result.HandlerName != handlerName {
			results = append(results, result)
		}
	}
	return results
}

func firstSliceEventHasErrorResult(event *abxbus.BaseEvent) bool {
	for _, result := range event.EventResults {
		if result.Status == abxbus.EventResultError {
			return true
		}
	}
	return false
}

func waitForFirstSliceErrorResults(event *abxbus.BaseEvent, expected int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		count := 0
		for _, result := range event.EventResults {
			if result.Status == abxbus.EventResultError {
				count++
			}
		}
		if count >= expected {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
}

func isFirstSliceBaseEventResult(result any) bool {
	if _, ok := result.(*abxbus.BaseEvent); ok {
		return true
	}
	if object, ok := result.(map[string]any); ok {
		_, hasEventType := object["event_type"]
		_, hasEventID := object["event_id"]
		return hasEventType && hasEventID
	}
	return false
}

type assertErr string

func (e assertErr) Error() string {
	return string(e)
}
