package abxbus_test

import (
	"context"
	"strings"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestEventHandlerCompletionFirstStopsAfterFirstValidResult(t *testing.T) {
	bus := abxbus.NewEventBus("EventHandlerCompletionFirstShortcutBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	lateCalled := false
	bus.On("EventHandlerCompletionFirstShortcutEvent", "empty", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return nil, nil
	}, nil)
	bus.On("EventHandlerCompletionFirstShortcutEvent", "winner", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "winner", nil
	}, nil)
	bus.On("EventHandlerCompletionFirstShortcutEvent", "late", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		lateCalled = true
		return "late", nil
	}, nil)

	event := abxbus.NewBaseEvent("EventHandlerCompletionFirstShortcutEvent", nil)
	event.EventHandlerCompletion = abxbus.EventHandlerCompletionFirst
	emitted := bus.Emit(event)
	if _, err := emitted.Now(); err != nil {
		t.Fatal(err)
	}
	result, err := emitted.EventResult(&abxbus.EventResultOptions{RaiseIfAny: abxbus.Ptr(false)})
	if err != nil || result != "winner" {
		t.Fatalf("expected first non-nil result, got %#v err=%v", result, err)
	}
	if emitted.EventHandlerCompletion != abxbus.EventHandlerCompletionFirst {
		t.Fatalf("event_handler_completion should remain first, got %s", emitted.EventHandlerCompletion)
	}
	if lateCalled {
		t.Fatal("event_handler_completion=first should skip later serial handlers after the first non-nil result")
	}
}

func TestNowRunsAllHandlersAndEventResultReturnsFirstValidResult(t *testing.T) {
	bus := abxbus.NewEventBus("NowAllBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	lateCalled := false
	bus.On("NowAllEvent", "base-event", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return abxbus.NewBaseEvent("NowAllChildEvent", nil), nil
	}, nil)
	bus.On("NowAllEvent", "none", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return nil, nil
	}, nil)
	bus.On("NowAllEvent", "winner", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "winner", nil
	}, nil)
	bus.On("NowAllEvent", "late", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		lateCalled = true
		return "late", nil
	}, nil)

	event, err := bus.Emit(abxbus.NewBaseEvent("NowAllEvent", nil)).Now()
	if err != nil {
		t.Fatal(err)
	}
	result, err := event.EventResult()
	if err != nil || result != "winner" {
		t.Fatalf("expected first valid result, got %#v err=%v", result, err)
	}
	if event.EventHandlerCompletion != "" {
		t.Fatalf("Now should not change event_handler_completion, got %s", event.EventHandlerCompletion)
	}
	if !lateCalled {
		t.Fatal("Now should let later handlers run")
	}
}

func TestEventResultDefaultErrorPolicyRaisesHandlerErrors(t *testing.T) {
	bus := abxbus.NewEventBus("EventResultErrorPolicyBus", nil)
	bus.On("EventResultErrorPolicyEvent", "fail", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return nil, assertErr("event result boom")
	}, nil)
	bus.On("EventResultErrorPolicyEvent", "winner", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "winner", nil
	}, nil)

	event, err := bus.Emit(abxbus.NewBaseEvent("EventResultErrorPolicyEvent", nil)).Now()
	if err != nil {
		t.Fatal(err)
	}
	_, err = event.EventResult()
	if err == nil || !strings.Contains(err.Error(), "event result boom") {
		t.Fatalf("default EventResult should surface handler errors, got %v", err)
	}
	result, err := event.EventResult(&abxbus.EventResultOptions{RaiseIfAny: abxbus.Ptr(false)})
	if err != nil || result != "winner" {
		t.Fatalf("RaiseIfAny=false should return the valid result; result=%#v err=%v", result, err)
	}
}

func TestEventResultOptionsCanRaiseAndFilterResults(t *testing.T) {
	bus := abxbus.NewEventBus("EventResultOptionsBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	bus.On("EventResultOptionsEvent", "fail", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return nil, assertErr("event result option boom")
	}, nil)
	bus.On("EventResultOptionsEvent", "first", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "first", nil
	}, nil)
	bus.On("EventResultOptionsEvent", "second", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "second", nil
	}, nil)

	event, err := bus.Emit(abxbus.NewBaseEvent("EventResultOptionsEvent", nil)).Now()
	if err != nil {
		t.Fatal(err)
	}
	_, err = event.EventResult(&abxbus.EventResultOptions{RaiseIfAny: abxbus.Ptr(true)})
	if err == nil || !strings.Contains(err.Error(), "event result option boom") {
		t.Fatalf("RaiseIfAny=true should surface handler errors, got %v", err)
	}
	result, err := event.EventResult(&abxbus.EventResultOptions{
		RaiseIfAny: abxbus.Ptr(false),
		Include: func(result any, eventResult *abxbus.EventResult) bool {
			return eventResult.Status == abxbus.EventResultCompleted && result == "second"
		},
	})
	if err != nil || result != "second" {
		t.Fatalf("expected Include filter to select second result; result=%#v err=%v", result, err)
	}
}

func TestWaitAndNowFirstResultTimeoutLimitsProcessingWait(t *testing.T) {
	noTimeout := 0.0
	bus := abxbus.NewEventBus("FirstResultTimeoutBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventTimeout:            &noTimeout,
	})
	bus.On("FirstResultTimeoutEvent", "slow", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		time.Sleep(500 * time.Millisecond)
		return "slow", nil
	}, nil)

	timeout := 0.01
	_, err := bus.Emit(abxbus.NewBaseEvent("FirstResultTimeoutEvent", nil)).Now(
		&abxbus.EventWaitOptions{Timeout: &timeout, FirstResult: true},
	)
	if err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("Now(first_result=true) should time out while waiting for processing, got %v", err)
	}
}

func TestEventResultIncludeCallbackReceivesResultAndEventResult(t *testing.T) {
	bus := abxbus.NewEventBus("EventResultIncludeBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	seenHandlerNames := []string{}
	bus.On("EventResultIncludeEvent", "none_handler", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return nil, nil
	}, nil)
	bus.On("EventResultIncludeEvent", "second_handler", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "second", nil
	}, nil)

	event, err := bus.Emit(abxbus.NewBaseEvent("EventResultIncludeEvent", nil)).Now()
	if err != nil {
		t.Fatal(err)
	}
	result, err := event.EventResult(&abxbus.EventResultOptions{
		Include: func(result any, eventResult *abxbus.EventResult) bool {
			seenHandlerNames = append(seenHandlerNames, eventResult.HandlerName)
			if result != eventResult.Result {
				t.Fatalf("include should receive the unwrapped result and matching EventResult, got %#v vs %#v", result, eventResult.Result)
			}
			return eventResult.Status == abxbus.EventResultCompleted && result == "second"
		},
	})
	if err != nil || result != "second" {
		t.Fatalf("expected include filter to select second result; result=%#v err=%v", result, err)
	}
	if strings.Join(seenHandlerNames, ",") != "none_handler,second_handler" {
		t.Fatalf("include should receive EventResult in handler order, got %v", seenHandlerNames)
	}
}

func TestEventResultReturnsFirstValidResultByHandlerOrder(t *testing.T) {
	bus := abxbus.NewEventBus("EventResultHandlerOrderBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	bus.On("EventResultHandlerOrderEvent", "slow", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		time.Sleep(50 * time.Millisecond)
		return "slow", nil
	}, nil)
	bus.On("EventResultHandlerOrderEvent", "fast", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		time.Sleep(time.Millisecond)
		return "fast", nil
	}, nil)

	event, err := bus.Emit(abxbus.NewBaseEvent("EventResultHandlerOrderEvent", nil)).Now()
	if err != nil {
		t.Fatal(err)
	}
	result, err := event.EventResult()
	if err != nil || result != "slow" {
		t.Fatalf("expected first registered valid result, result=%#v err=%v", result, err)
	}
}

func TestEventResultAndNowRejectWhenEventHasNoBusAttached(t *testing.T) {
	event := abxbus.NewBaseEvent("NoBusEvent", nil)
	if _, err := event.Now(); err == nil || !strings.Contains(err.Error(), "no bus attached") {
		t.Fatalf("Now without bus should fail with no bus attached, got %v", err)
	}
	if _, err := event.EventResult(); err == nil || !strings.Contains(err.Error(), "no bus attached") {
		t.Fatalf("EventResult without bus should fail with no bus attached, got %v", err)
	}
}

func TestNowAndEventResultAllowCompletedEventWithNoBusAttached(t *testing.T) {
	event := abxbus.NewBaseEvent("CompletedNoBusEvent", nil)
	event.EventStatus = "completed"
	if completed, err := event.Now(); err != nil || completed != event {
		t.Fatalf("Now should allow completed event without bus; completed=%#v err=%v", completed, err)
	}
	if result, err := event.EventResult(); err != nil || result != nil {
		t.Fatalf("EventResult should allow completed event without bus and no results; result=%#v err=%v", result, err)
	}
}

func TestEventResultIgnoresBaseEventReturnWhenChoosingWinner(t *testing.T) {
	bus := abxbus.NewEventBus("EventResultBaseEventWinnerBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	bus.On("EventResultBaseEventWinnerEvent", "base-event", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return abxbus.NewBaseEvent("ForwardedEvent", nil), nil
	}, nil)
	bus.On("EventResultBaseEventWinnerEvent", "scalar", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "winner", nil
	}, nil)

	event, err := bus.Emit(abxbus.NewBaseEvent("EventResultBaseEventWinnerEvent", nil)).Now()
	if err != nil {
		t.Fatal(err)
	}
	result, err := event.EventResult()
	if err != nil || result != "winner" {
		t.Fatalf("expected EventResult to skip BaseEvent result and return scalar winner; result=%#v err=%v", result, err)
	}
}

type assertErr string

func (e assertErr) Error() string {
	return string(e)
}
