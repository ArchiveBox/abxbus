package abxbus_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEmitAndDispatchUseDefaultBehavior(t *testing.T) {
	bus := abxbus.NewEventBus("DefaultsBus", nil)
	if bus.EventHistory.MaxHistorySize == nil || *bus.EventHistory.MaxHistorySize != abxbus.DefaultMaxHistorySize {
		t.Fatalf("unexpected default max history size: %#v", bus.EventHistory.MaxHistorySize)
	}
	if bus.EventConcurrency != abxbus.EventConcurrencyBusSerial {
		t.Fatalf("unexpected default event concurrency: %s", bus.EventConcurrency)
	}
	if bus.EventHandlerConcurrency != abxbus.EventHandlerConcurrencySerial {
		t.Fatalf("unexpected default handler concurrency: %s", bus.EventHandlerConcurrency)
	}
	if bus.EventHandlerCompletion != abxbus.EventHandlerCompletionAll {
		t.Fatalf("unexpected default handler completion: %s", bus.EventHandlerCompletion)
	}

	calls := []string{}
	bus.On("CreateUserEvent", "first", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		calls = append(calls, "first")
		return "first", nil
	}, nil)
	bus.On("CreateUserEvent", "second", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		calls = append(calls, "second")
		return map[string]any{"user_id": "abc"}, nil
	}, nil)

	e := bus.Dispatch(abxbus.NewBaseEvent("CreateUserEvent", map[string]any{"email": "a@b.com"}))
	if _, err := e.Now(); err != nil {
		t.Fatal(err)
	}
	if e.EventStatus != "completed" {
		t.Fatalf("expected completed event status, got %s", e.EventStatus)
	}
	if len(calls) != 2 || calls[0] != "first" || calls[1] != "second" {
		t.Fatalf("expected serial handler order, got %v", calls)
	}
	if len(e.EventResults) != 2 {
		t.Fatalf("expected 2 event results, got %d", len(e.EventResults))
	}

	values, err := e.EventResultsList(&abxbus.EventResultOptions{RaiseIfAny: false, RaiseIfNone: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 {
		t.Fatalf("expected two non-nil result values, got %#v", values)
	}
	if values[0] != "first" || !reflect.DeepEqual(values[1], map[string]any{"user_id": "abc"}) {
		t.Fatalf("unexpected result values/order: %#v", values)
	}
}

func TestUnboundedHistoryDisablesHistoryRejection(t *testing.T) {
	unlimitedHistorySize := abxbus.UnlimitedHistorySize
	bus := abxbus.NewEventBus("UnlimitedHistBus", &abxbus.EventBusOptions{
		MaxHistorySize: &unlimitedHistorySize,
		MaxHistoryDrop: false,
	})
	if bus.EventHistory.MaxHistorySize != nil {
		t.Fatalf("unbounded history should store nil max size, got %#v", bus.EventHistory.MaxHistorySize)
	}

	for i := 0; i < abxbus.DefaultMaxHistorySize+10; i++ {
		event := abxbus.NewBaseEvent("HistoryEvent", map[string]any{"index": i})
		event.EventStatus = "completed"
		bus.EventHistory.AddEvent(event)
	}
	if bus.EventHistory.Size() != abxbus.DefaultMaxHistorySize+10 {
		t.Fatalf("unbounded history should keep all events, got %d", bus.EventHistory.Size())
	}
}

func TestMaxHistoryDropFalseRejectsNewDispatchWhenHistoryIsFull(t *testing.T) {
	maxHistorySize := 2
	bus := abxbus.NewEventBus("NoDropHistBus", &abxbus.EventBusOptions{MaxHistorySize: &maxHistorySize})
	bus.On("NoDropEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, nil)

	for i := 1; i <= 2; i++ {
		event := bus.Emit(abxbus.NewBaseEvent("NoDropEvent", map[string]any{"seq": i}))
		if _, err := event.Now(); err != nil {
			t.Fatal(err)
		}
	}
	if bus.EventHistory.Size() != 2 {
		t.Fatalf("expected history size 2, got %d", bus.EventHistory.Size())
	}

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected history limit panic")
		}
		if !strings.Contains(recovered.(string), "history limit reached (2/2)") {
			t.Fatalf("unexpected panic: %v", recovered)
		}
		if bus.EventHistory.Size() != 2 {
			t.Fatalf("history should remain capped after rejected emit, got %d", bus.EventHistory.Size())
		}
	}()
	bus.Emit(abxbus.NewBaseEvent("NoDropEvent", map[string]any{"seq": 3}))
}

func TestZeroHistorySizeKeepsInflightAndDropsOnCompletion(t *testing.T) {
	zeroHistorySize := 0
	bus := abxbus.NewEventBus("ZeroHistoryBus", &abxbus.EventBusOptions{MaxHistorySize: &zeroHistorySize})
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	bus.On("SlowEvent", "slow", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return "ok", nil
	}, nil)

	first := bus.Emit(abxbus.NewBaseEvent("SlowEvent", nil))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("timed out waiting for first handler to start")
	}
	second := bus.Emit(abxbus.NewBaseEvent("SlowEvent", nil))
	if !bus.EventHistory.Has(first.EventID) || !bus.EventHistory.Has(second.EventID) {
		close(release)
		t.Fatalf("zero history should keep in-flight events, size=%d", bus.EventHistory.Size())
	}

	close(release)
	for _, event := range []*abxbus.BaseEvent{first, second} {
		if _, err := event.Now(); err != nil {
			t.Fatal(err)
		}
	}
	timeout := 2.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("bus did not become idle")
	}
	if bus.EventHistory.Size() != 0 {
		t.Fatalf("zero history should drop completed events, got size=%d", bus.EventHistory.Size())
	}
}

func TestZeroHistoryNoDropAllowsBurstQueueingAndDropsCompletedEvents(t *testing.T) {
	zeroHistorySize := 0
	bus := abxbus.NewEventBus("ZeroHistNoDropBus", &abxbus.EventBusOptions{MaxHistorySize: &zeroHistorySize, MaxHistoryDrop: false})
	release := make(chan struct{})
	bus.On("BurstEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		<-release
		return "ok", nil
	}, nil)

	events := make([]*abxbus.BaseEvent, 0, 25)
	for i := 0; i < 25; i++ {
		events = append(events, bus.Emit(abxbus.NewBaseEvent("BurstEvent", map[string]any{"seq": i})))
	}
	if bus.EventHistory.Size() == 0 {
		close(release)
		t.Fatal("zero history should retain pending/in-flight events before completion")
	}

	close(release)
	for _, event := range events {
		if _, err := event.Now(); err != nil {
			t.Fatal(err)
		}
	}
	timeout := 2.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("bus did not become idle")
	}
	if bus.EventHistory.Size() != 0 {
		t.Fatalf("zero history should drop all completed burst events, got size=%d", bus.EventHistory.Size())
	}
}

func TestEventResultReturnsFirstCompletedResult(t *testing.T) {
	bus := abxbus.NewEventBus("SimpleBus", nil)
	bus.On("ResultEvent", "on_create", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return map[string]any{"user_id": "abc"}, nil
	}, nil)
	e := bus.Emit(abxbus.NewBaseEvent("ResultEvent", map[string]any{"email": "a@b.com"}))
	result, err := e.EventResult()
	if err != nil {
		t.Fatal(err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map result, got %#v", result)
	}
	if m["user_id"] != "abc" {
		t.Fatalf("unexpected result %#v", result)
	}
}

func TestEventResultsListDefaultsFilterEmptyValuesRaiseErrorsAndOptionsOverride(t *testing.T) {
	bus := abxbus.NewEventBus("EventResultsOptionsBus", nil)
	defer bus.Destroy()

	bus.On("ResultOptionsDefaultEvent", "ok", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, nil)
	bus.On("ResultOptionsDefaultEvent", "nil", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, nil
	}, nil)
	bus.On("ResultOptionsDefaultEvent", "forwarded", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return abxbus.NewBaseEvent("ForwardedResultEvent", nil), nil
	}, nil)
	defaultEvent := bus.Emit(abxbus.NewBaseEvent("ResultOptionsDefaultEvent", nil))
	if _, err := defaultEvent.Now(); err != nil {
		t.Fatal(err)
	}
	defaultValues, err := defaultEvent.EventResultsList()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(defaultValues, []any{"ok"}) {
		t.Fatalf("default result filtering mismatch: %#v", defaultValues)
	}

	bus.On("ResultOptionsErrorEvent", "ok", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, nil)
	bus.On("ResultOptionsErrorEvent", "boom", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, errors.New("boom")
	}, nil)
	errorEvent := bus.Emit(abxbus.NewBaseEvent("ResultOptionsErrorEvent", nil))
	if _, err := errorEvent.Now(); err != nil {
		t.Fatal(err)
	}
	if _, err := errorEvent.EventResultsList(); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("default raise_if_any should surface handler error, got %v", err)
	}
	valuesWithoutErrors, err := errorEvent.EventResultsList(&abxbus.EventResultOptions{RaiseIfAny: false, RaiseIfNone: true})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(valuesWithoutErrors, []any{"ok"}) {
		t.Fatalf("raise_if_any=false values mismatch: %#v", valuesWithoutErrors)
	}

	bus.On("ResultOptionsEmptyEvent", "nil", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, nil
	}, nil)
	emptyEvent := bus.Emit(abxbus.NewBaseEvent("ResultOptionsEmptyEvent", nil))
	if _, err := emptyEvent.Now(); err != nil {
		t.Fatal(err)
	}
	if _, err := emptyEvent.EventResultsList(&abxbus.EventResultOptions{RaiseIfNone: true}); err == nil {
		t.Fatal("raise_if_none=true should fail when every handler result is filtered out")
	}
	emptyValues, err := emptyEvent.EventResultsList(&abxbus.EventResultOptions{RaiseIfAny: false, RaiseIfNone: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(emptyValues) != 0 {
		t.Fatalf("raise_if_none=false should allow empty result list, got %#v", emptyValues)
	}

	bus.On("ResultOptionsIncludeEvent", "keep", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "keep", nil
	}, nil)
	bus.On("ResultOptionsIncludeEvent", "drop", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "drop", nil
	}, nil)
	seenHandlerNames := []string{}
	includeEvent := bus.Emit(abxbus.NewBaseEvent("ResultOptionsIncludeEvent", nil))
	if _, err := includeEvent.Now(); err != nil {
		t.Fatal(err)
	}
	filteredValues, err := includeEvent.EventResultsList(&abxbus.EventResultOptions{
		RaiseIfAny:  false,
		RaiseIfNone: true,
		Include: func(result any, eventResult *abxbus.EventResult) bool {
			seenHandlerNames = append(seenHandlerNames, eventResult.HandlerName)
			return result == "keep"
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(filteredValues, []any{"keep"}) || len(seenHandlerNames) != 2 {
		t.Fatalf("include filter mismatch values=%#v seen=%#v", filteredValues, seenHandlerNames)
	}
}

// Folded from event_history_store_test.go to keep test layout class-based.
func TestEventHistory(t *testing.T) {
	max := 2
	h := abxbus.NewEventHistory(&max, false)
	if h.Size() != 0 {
		t.Fatalf("new history should start empty, got %d", h.Size())
	}

	e1 := abxbus.NewBaseEvent("A", nil)
	e2 := abxbus.NewBaseEvent("B", nil)
	e3 := abxbus.NewBaseEvent("C", nil)
	e1.EventStatus = "completed"
	e2.EventStatus = "started"
	e3.EventStatus = "completed"

	h.AddEvent(e1)
	h.AddEvent(e2)
	h.AddEvent(e3)

	if h.Size() != 2 {
		t.Fatalf("expected size 2 after trimming, got %d", h.Size())
	}
	if h.Has(e1.EventID) {
		t.Fatalf("expected oldest completed event to be trimmed")
	}
	if !h.Has(e2.EventID) || !h.Has(e3.EventID) {
		t.Fatalf("expected newer events to remain after trim")
	}
	if h.GetEvent("missing") != nil || h.Has("missing") {
		t.Fatalf("missing IDs should not be found")
	}

	vals := h.Values()
	if len(vals) != 2 || vals[0].EventID != e2.EventID || vals[1].EventID != e3.EventID {
		t.Fatalf("expected stable order after trim, got %#v", vals)
	}

	h.AddEvent(e2)
	if h.Size() != 2 {
		t.Fatalf("duplicate add should not change size, got %d", h.Size())
	}

	if !h.RemoveEvent(e2.EventID) {
		t.Fatalf("expected remove existing event to succeed")
	}
	if h.RemoveEvent("missing") {
		t.Fatalf("remove missing event should return false")
	}
	if h.Size() != 1 || !h.Has(e3.EventID) {
		t.Fatalf("unexpected final remove state")
	}
	if removed := h.TrimEventHistory(nil); removed != 0 {
		t.Fatalf("trim under max should remove 0 events, removed=%d", removed)
	}

	unbounded := abxbus.NewEventHistory(nil, false)
	for i := 0; i < 3; i++ {
		event := abxbus.NewBaseEvent("Unlimited", map[string]any{"index": i})
		event.EventStatus = "completed"
		unbounded.AddEvent(event)
	}
	if unbounded.MaxHistorySize != nil {
		t.Fatalf("nil max_history_size should mean unbounded history")
	}
	if unbounded.Size() != 3 {
		t.Fatalf("unbounded history should keep every event, got %d", unbounded.Size())
	}
	if removed := unbounded.TrimEventHistory(nil); removed != 0 {
		t.Fatalf("unbounded history should not trim events, removed=%d", removed)
	}

	maxOne := 1
	noDrop := abxbus.NewEventHistory(&maxOne, false)
	p1 := abxbus.NewBaseEvent("P1", nil)
	p2 := abxbus.NewBaseEvent("P2", nil)
	p1.EventStatus = "started"
	p2.EventStatus = "pending"
	noDrop.AddEvent(p1)
	noDrop.AddEvent(p2)
	if noDrop.Size() != 2 {
		t.Fatalf("max_history_drop=false should not force-drop in-progress events")
	}

	drop := abxbus.NewEventHistory(&maxOne, true)
	d1 := abxbus.NewBaseEvent("D1", nil)
	d2 := abxbus.NewBaseEvent("D2", nil)
	d1.EventStatus = "started"
	d2.EventStatus = "pending"
	drop.AddEvent(d1)
	drop.AddEvent(d2)
	if drop.Size() != 2 || !drop.Has(d1.EventID) || !drop.Has(d2.EventID) {
		t.Fatalf("max_history_drop=true should not force-drop in-progress events")
	}

	drop.Clear()
	if drop.Size() != 0 || len(drop.Values()) != 0 {
		t.Fatalf("clear should reset history")
	}
}

// Folded from eventbus_completion_modes_test.go to keep test layout class-based.
func TestCompletionModeAllWaitsForAllHandlers(t *testing.T) {
	bus := abxbus.NewEventBus("AllModeBus", &abxbus.EventBusOptions{
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	slowDone := false
	bus.On("Evt", "fast", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) { return "fast", nil }, nil)
	bus.On("Evt", "slow", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(30 * time.Millisecond)
		slowDone = true
		return "slow", nil
	}, nil)
	e := bus.Emit(abxbus.NewBaseEvent("Evt", nil))
	if _, err := e.Now(); err != nil {
		t.Fatal(err)
	}
	if !slowDone {
		t.Fatal("completion=all should wait for slow handler")
	}
}

func TestCompletionModeFirstSerialStopsAfterFirstNonNil(t *testing.T) {
	bus := abxbus.NewEventBus("FirstSerialBus", &abxbus.EventBusOptions{
		EventHandlerCompletion:  abxbus.EventHandlerCompletionFirst,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	secondCalled := false
	bus.On("Evt", "first", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) { return "first", nil }, nil)
	bus.On("Evt", "second", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		secondCalled = true
		return "second", nil
	}, nil)
	e := bus.Emit(abxbus.NewBaseEvent("Evt", nil))
	if _, err := e.Now(); err != nil {
		t.Fatal(err)
	}
	result, err := e.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false})
	if err != nil {
		t.Fatal(err)
	}
	if result != "first" {
		t.Fatalf("expected first result, got %#v", result)
	}
	if secondCalled {
		t.Fatal("serial first mode should not call second handler after non-nil first result")
	}
	for _, eventResult := range e.EventResults {
		if eventResult.Status == abxbus.EventResultPending {
			t.Fatal("serial first mode should not leave skipped handlers in pending state")
		}
	}
}

func TestCompletionModeFirstParallelReturnsFastAndCancelsSlow(t *testing.T) {
	bus := abxbus.NewEventBus("FirstParallelBus", &abxbus.EventBusOptions{
		EventHandlerCompletion:  abxbus.EventHandlerCompletionFirst,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	slowStarted := make(chan struct{}, 1)
	slowExited := make(chan struct{}, 1)
	bus.On("Evt", "slow", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		slowStarted <- struct{}{}
		select {
		case <-time.After(500 * time.Millisecond):
			slowExited <- struct{}{}
			return "slow", nil
		case <-ctx.Done():
			slowExited <- struct{}{}
			return nil, ctx.Err()
		}
	}, nil)
	bus.On("Evt", "fast", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) { return "fast", nil }, nil)

	event := abxbus.NewBaseEvent("Evt", nil)
	event.EventHandlerCompletion = abxbus.EventHandlerCompletionFirst
	emitted := bus.Emit(event)
	if _, err := emitted.Now(); err != nil {
		t.Fatal(err)
	}
	result, err := emitted.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false})
	if err != nil {
		t.Fatal(err)
	}
	if result != "fast" {
		t.Fatalf("expected fast result, got %#v", result)
	}
	select {
	case <-slowStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("expected slow handler to start in parallel first-completion mode")
	}
	select {
	case <-slowExited:
	case <-time.After(2 * time.Second):
		t.Fatal("expected slow handler to exit (cancel or complete) after fast first result")
	}
}

// Folded from eventbus_edge_cases_test.go to keep test layout class-based.
func TestWaitUntilIdleTimeoutAndRecovery(t *testing.T) {
	bus := abxbus.NewEventBus("IdleTimeoutBus", nil)
	release := make(chan struct{})
	bus.On("Evt", "slow", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		<-release
		return nil, nil
	}, nil)
	_ = bus.Emit(abxbus.NewBaseEvent("Evt", nil))

	tShort := 0.01
	if bus.WaitUntilIdle(&tShort) {
		close(release)
		t.Fatal("expected false due to in-flight work")
	}
	close(release)
	tLong := 1.0
	if !bus.WaitUntilIdle(&tLong) {
		t.Fatal("expected true after releasing handler")
	}
}

func TestEventResetCreatesFreshPendingEventForCrossBusDispatch(t *testing.T) {
	busA := abxbus.NewEventBus("ResetCoverageBusA", nil)
	busB := abxbus.NewEventBus("ResetCoverageBusB", nil)
	seenA := []string{}
	seenB := []string{}

	busA.On("ResetCoverageEvent", "record_a", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		if label, ok := event.Payload["label"].(string); ok {
			seenA = append(seenA, label)
		}
		return "a:" + event.Payload["label"].(string), nil
	}, nil)
	busB.On("ResetCoverageEvent", "record_b", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		if label, ok := event.Payload["label"].(string); ok {
			seenB = append(seenB, label)
		}
		return "b:" + event.Payload["label"].(string), nil
	}, nil)

	completed := busA.Emit(abxbus.NewBaseEvent("ResetCoverageEvent", map[string]any{"label": "hello"}))
	if _, err := completed.Now(); err != nil {
		t.Fatal(err)
	}
	if completed.EventStatus != "completed" || len(completed.EventResults) != 1 {
		t.Fatalf("expected completed event with one result, got status=%s results=%d", completed.EventStatus, len(completed.EventResults))
	}

	fresh, err := completed.EventReset()
	if err != nil {
		t.Fatal(err)
	}
	if fresh.EventID == completed.EventID {
		t.Fatal("reset event should have a fresh event_id")
	}
	if fresh.EventStatus != "pending" || fresh.EventStartedAt != nil || fresh.EventCompletedAt != nil {
		t.Fatalf("reset event should be pending with no lifecycle timestamps: %#v", fresh)
	}
	if len(fresh.EventResults) != 0 || fresh.EventPendingBusCount != 0 || fresh.Bus != nil {
		t.Fatalf("reset event should clear runtime state: %#v", fresh)
	}

	forwarded := busB.Emit(fresh)
	if _, err := forwarded.Now(); err != nil {
		t.Fatal(err)
	}
	if forwarded.EventStatus != "completed" {
		t.Fatalf("expected forwarded reset event to complete, got %s", forwarded.EventStatus)
	}
	if len(seenA) != 1 || seenA[0] != "hello" || len(seenB) != 1 || seenB[0] != "hello" {
		t.Fatalf("unexpected handler observations: seenA=%#v seenB=%#v", seenA, seenB)
	}
	hasBusA := false
	hasBusB := false
	for _, entry := range forwarded.EventPath {
		if len(entry) >= len("ResetCoverageBusA#") && entry[:len("ResetCoverageBusA#")] == "ResetCoverageBusA#" {
			hasBusA = true
		}
		if len(entry) >= len("ResetCoverageBusB#") && entry[:len("ResetCoverageBusB#")] == "ResetCoverageBusB#" {
			hasBusB = true
		}
	}
	if !hasBusA || !hasBusB {
		t.Fatalf("reset event should preserve previous path and append new bus path: %#v", forwarded.EventPath)
	}
}

func TestIsIdleAndQueueEmptyStates(t *testing.T) {
	bus := abxbus.NewEventBus("IdleStateBus", nil)
	if !bus.IsIdleAndQueueEmpty() {
		t.Fatal("new bus should be idle and queue-empty")
	}

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	bus.On("Evt", "slow", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		started <- struct{}{}
		<-release
		return nil, nil
	}, nil)
	_ = bus.Emit(abxbus.NewBaseEvent("Evt", nil))

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for handler start")
	}
	if bus.IsIdleAndQueueEmpty() {
		t.Fatal("bus should not be idle while work is pending/running")
	}

	close(release)
	tWait := 1.0
	if !bus.WaitUntilIdle(&tWait) {
		t.Fatal("bus should become idle")
	}
	if !bus.IsIdleAndQueueEmpty() {
		t.Fatal("bus should be idle/queue-empty after completion")
	}
}

// Folded from eventbus_middleware_test.go to keep test layout class-based.
type middlewareRecord struct {
	Middleware    string
	Hook          string
	BusID         string
	EventID       string
	Status        string
	ResultStatus  abxbus.EventResultStatus
	HandlerID     string
	HandlerName   string
	EventPattern  string
	Registered    bool
	HandlerResult any
	HandlerError  any
}

type recordingMiddleware struct {
	name    string
	records []middlewareRecord
	seq     *[]string
	mu      sync.Mutex
}

func newRecordingMiddleware(name string, seq *[]string) *recordingMiddleware {
	return &recordingMiddleware{name: name, seq: seq}
}

func (m *recordingMiddleware) OnEventChange(bus *abxbus.EventBus, event *abxbus.BaseEvent, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, middlewareRecord{
		Middleware: m.name,
		Hook:       "event",
		BusID:      bus.ID,
		EventID:    event.EventID,
		Status:     status,
	})
	if m.seq != nil {
		*m.seq = append(*m.seq, fmt.Sprintf("%s:event:%s", m.name, status))
	}
}

func (m *recordingMiddleware) OnEventResultChange(bus *abxbus.EventBus, event *abxbus.BaseEvent, result *abxbus.EventResult, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record := middlewareRecord{
		Middleware:   m.name,
		Hook:         "result",
		BusID:        bus.ID,
		EventID:      event.EventID,
		Status:       status,
		ResultStatus: result.Status,
		HandlerID:    result.HandlerID,
		HandlerName:  result.HandlerName,
		HandlerError: result.Error,
	}
	if status == "completed" {
		record.HandlerResult = result.Result
	}
	m.records = append(m.records, record)
	if m.seq != nil {
		*m.seq = append(*m.seq, fmt.Sprintf("%s:result:%s:%s", m.name, result.HandlerName, status))
	}
}

func (m *recordingMiddleware) OnBusHandlersChange(bus *abxbus.EventBus, handler *abxbus.EventHandler, registered bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, middlewareRecord{
		Middleware:   m.name,
		Hook:         "handler",
		BusID:        bus.ID,
		HandlerID:    handler.ID,
		HandlerName:  handler.HandlerName,
		EventPattern: handler.EventPattern,
		Registered:   registered,
	})
	if m.seq != nil {
		state := "unregistered"
		if registered {
			state = "registered"
		}
		*m.seq = append(*m.seq, fmt.Sprintf("%s:handler:%s:%s", m.name, handler.HandlerName, state))
	}
}

func (m *recordingMiddleware) snapshot() []middlewareRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]middlewareRecord{}, m.records...)
}

func recordsByHook(records []middlewareRecord, hook string) []middlewareRecord {
	filtered := []middlewareRecord{}
	for _, record := range records {
		if record.Hook == hook {
			filtered = append(filtered, record)
		}
	}
	return filtered
}

func statuses(records []middlewareRecord) []string {
	values := make([]string, 0, len(records))
	for _, record := range records {
		values = append(values, record.Status)
	}
	return values
}

func resultStatuses(records []middlewareRecord) []abxbus.EventResultStatus {
	values := make([]abxbus.EventResultStatus, 0, len(records))
	for _, record := range records {
		values = append(values, record.ResultStatus)
	}
	return values
}

func assertRecordStatuses(t *testing.T, records []middlewareRecord, expected []string) {
	t.Helper()
	if !reflect.DeepEqual(statuses(records), expected) {
		t.Fatalf("unexpected hook statuses: got %#v want %#v", statuses(records), expected)
	}
}

func TestEventBusMiddlewareReceivesPendingStartedCompletedLifecycleHooks(t *testing.T) {
	middleware := newRecordingMiddleware("single", nil)
	bus := abxbus.NewEventBus("MiddlewareBus", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})
	handler := bus.On("MiddlewareEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, nil)
	bus.Off("MiddlewareEvent", handler)
	handler = bus.On("MiddlewareEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, &abxbus.EventHandler{
		ID:                  handler.ID,
		HandlerRegisteredAt: handler.HandlerRegisteredAt,
	})

	event := bus.Emit(abxbus.NewBaseEvent("MiddlewareEvent", nil))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}

	records := middleware.snapshot()
	handlerChanges := recordsByHook(records, "handler")
	if len(handlerChanges) != 3 {
		t.Fatalf("unexpected handler middleware changes: %#v", handlerChanges)
	}
	if handlerChanges[0].HandlerName != "handler" || !handlerChanges[0].Registered ||
		handlerChanges[1].HandlerName != "handler" || handlerChanges[1].Registered ||
		handlerChanges[2].HandlerName != "handler" || !handlerChanges[2].Registered {
		t.Fatalf("unexpected handler middleware changes: %#v", handlerChanges)
	}

	assertRecordStatuses(t, recordsByHook(records, "event"), []string{"pending", "started", "completed"})

	resultRecords := recordsByHook(records, "result")
	assertRecordStatuses(t, resultRecords, []string{"pending", "started", "completed"})
	if !reflect.DeepEqual(resultStatuses(resultRecords), []abxbus.EventResultStatus{
		abxbus.EventResultPending,
		abxbus.EventResultStarted,
		abxbus.EventResultCompleted,
	}) {
		t.Fatalf("unexpected runtime result statuses: %#v", resultStatuses(resultRecords))
	}
	if resultRecords[2].HandlerResult != "ok" {
		t.Fatalf("completed middleware hook did not observe handler result: %#v", resultRecords[2])
	}
}

func TestEventBusMiddlewareHooksExecuteInRegistrationOrder(t *testing.T) {
	sequence := []string{}
	first := newRecordingMiddleware("first", &sequence)
	second := newRecordingMiddleware("second", &sequence)
	bus := abxbus.NewEventBus("MiddlewareOrderBus", &abxbus.EventBusOptions{
		Middlewares: []abxbus.EventBusMiddleware{first, second},
	})
	bus.On("MiddlewareOrderEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, nil)

	if _, err := bus.Emit(abxbus.NewBaseEvent("MiddlewareOrderEvent", nil)).Now(); err != nil {
		t.Fatal(err)
	}

	orderedPairs := [][2]string{
		{"first:event:pending", "second:event:pending"},
		{"first:event:started", "second:event:started"},
		{"first:result:handler:pending", "second:result:handler:pending"},
		{"first:result:handler:started", "second:result:handler:started"},
		{"first:result:handler:completed", "second:result:handler:completed"},
		{"first:event:completed", "second:event:completed"},
	}
	for _, pair := range orderedPairs {
		firstIndex, secondIndex := middlewareIndexOf(sequence, pair[0]), middlewareIndexOf(sequence, pair[1])
		if firstIndex < 0 || secondIndex < 0 || firstIndex > secondIndex {
			t.Fatalf("middleware order mismatch for %v in sequence %#v", pair, sequence)
		}
	}
}

func TestEventBusMiddlewareNoHandlerEventLifecycle(t *testing.T) {
	middleware := newRecordingMiddleware("single", nil)
	bus := abxbus.NewEventBus("MiddlewareNoHandlerBus", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})

	event := bus.Emit(abxbus.NewBaseEvent("MiddlewareNoHandlerEvent", nil))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}

	records := middleware.snapshot()
	assertRecordStatuses(t, recordsByHook(records, "event"), []string{"pending", "started", "completed"})
	if resultRecords := recordsByHook(records, "result"); len(resultRecords) != 0 {
		t.Fatalf("no-handler event should not emit result hooks: %#v", resultRecords)
	}
}

func TestEventBusMiddlewareEventLifecycleOrderingIsDeterministicPerEvent(t *testing.T) {
	middleware := newRecordingMiddleware("deterministic", nil)
	historySize := 0
	bus := abxbus.NewEventBus("MiddlewareDeterministicBus", &abxbus.EventBusOptions{
		Middlewares:    []abxbus.EventBusMiddleware{middleware},
		MaxHistorySize: &historySize,
	})
	bus.On("MiddlewareDeterministicEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(time.Millisecond)
		return "ok", nil
	}, nil)

	batchCount := 5
	eventsPerBatch := 50
	seenEvents := map[string]bool{}
	for batchIndex := 0; batchIndex < batchCount; batchIndex++ {
		events := make([]*abxbus.BaseEvent, 0, eventsPerBatch)
		for eventIndex := 0; eventIndex < eventsPerBatch; eventIndex++ {
			eventTimeout := 0.2
			event := abxbus.NewBaseEvent("MiddlewareDeterministicEvent", nil)
			event.EventTimeout = &eventTimeout
			events = append(events, bus.Emit(event))
		}
		for _, event := range events {
			if _, err := event.Now(); err != nil {
				t.Fatal(err)
			}
			seenEvents[event.EventID] = true
		}

		recordsByEventID := map[string][]middlewareRecord{}
		for _, record := range recordsByHook(middleware.snapshot(), "event") {
			recordsByEventID[record.EventID] = append(recordsByEventID[record.EventID], record)
		}
		for _, event := range events {
			assertRecordStatuses(t, recordsByEventID[event.EventID], []string{"pending", "started", "completed"})
		}
	}
	if len(seenEvents) != batchCount*eventsPerBatch {
		t.Fatalf("unexpected deterministic event count: got %d want %d", len(seenEvents), batchCount*eventsPerBatch)
	}
}

func TestEventBusMiddlewareHooksObserveHandlerErrorsWithoutErrorHookStatus(t *testing.T) {
	middleware := newRecordingMiddleware("errors", nil)
	bus := abxbus.NewEventBus("MiddlewareErrorBus", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})
	bus.On("MiddlewareErrorEvent", "failing", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, errors.New("boom")
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("MiddlewareErrorEvent", nil))
	_, err := event.EventResult()
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected handler error from event result, got %v", err)
	}

	records := middleware.snapshot()
	assertRecordStatuses(t, recordsByHook(records, "event"), []string{"pending", "started", "completed"})
	resultRecords := recordsByHook(records, "result")
	assertRecordStatuses(t, resultRecords, []string{"pending", "started", "completed"})
	if resultRecords[len(resultRecords)-1].ResultStatus != abxbus.EventResultError {
		t.Fatalf("completed hook should observe runtime error status: %#v", resultRecords)
	}
	if resultRecords[len(resultRecords)-1].HandlerError == nil {
		t.Fatalf("completed hook should observe handler error: %#v", resultRecords)
	}
}

func TestEventBusMiddlewareHooksRemainMonotonicOnEventTimeout(t *testing.T) {
	middleware := newRecordingMiddleware("timeout", nil)
	bus := abxbus.NewEventBus("MiddlewareTimeoutBus", &abxbus.EventBusOptions{
		Middlewares:             []abxbus.EventBusMiddleware{middleware},
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	bus.On("MiddlewareTimeoutEvent", "slow", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		select {
		case <-time.After(100 * time.Millisecond):
			return "late", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	bus.On("MiddlewareTimeoutEvent", "pending", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "pending", nil
	}, nil)
	timeout := 0.02
	event := abxbus.NewBaseEvent("MiddlewareTimeoutEvent", nil)
	event.EventTimeout = &timeout

	_, _ = bus.Emit(event).Wait()

	records := middleware.snapshot()
	assertRecordStatuses(t, recordsByHook(records, "event"), []string{"pending", "started", "completed"})
	resultRecords := recordsByHook(records, "result")
	if len(resultRecords) != 5 {
		t.Fatalf("expected pending/started/completed for slow plus pending/completed for queued handler, got %#v", resultRecords)
	}
	byHandler := map[string][]middlewareRecord{}
	for _, record := range resultRecords {
		byHandler[record.HandlerName] = append(byHandler[record.HandlerName], record)
	}
	assertRecordStatuses(t, byHandler["slow"], []string{"pending", "started", "completed"})
	assertRecordStatuses(t, byHandler["pending"], []string{"pending", "completed"})
	if byHandler["slow"][2].ResultStatus != abxbus.EventResultError || byHandler["pending"][1].ResultStatus != abxbus.EventResultError {
		t.Fatalf("timeout hooks should observe runtime error status: %#v", byHandler)
	}
}

func TestEventBusMiddlewareHardEventTimeoutFinalizesImmediatelyWithoutWaitingForInFlightHandlers(t *testing.T) {
	bus := abxbus.NewEventBus("MiddlewareHardTimeoutBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	started := make(chan struct{}, 2)
	for _, handlerName := range []string{"slow_1", "slow_2"} {
		handlerName := handlerName
		bus.On("MiddlewareHardTimeoutEvent", handlerName, func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
			started <- struct{}{}
			time.Sleep(200 * time.Millisecond)
			return "late:" + handlerName, nil
		}, nil)
	}

	timeout := 0.01
	event := abxbus.NewBaseEvent("MiddlewareHardTimeoutEvent", nil)
	event.EventTimeout = &timeout
	startedAt := time.Now()
	dispatched := bus.Emit(event)
	if _, err := dispatched.Wait(); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(startedAt)
	if elapsed > 100*time.Millisecond {
		t.Fatalf("event timeout should finalize without waiting for slow handlers, elapsed=%s", elapsed)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		default:
			t.Fatalf("expected both parallel handlers to start before hard timeout, got %d", i)
		}
	}

	initialSnapshot := map[string]abxbus.EventResultStatus{}
	for id, result := range dispatched.EventResults {
		initialSnapshot[id] = result.Status
		if result.Status != abxbus.EventResultError {
			t.Fatalf("hard timeout should finalize handler result as error, got %#v", result)
		}
	}
	time.Sleep(250 * time.Millisecond)
	for id, status := range initialSnapshot {
		result := dispatched.EventResults[id]
		if result.Status != status {
			t.Fatalf("late handler completion reversed result status for %s: got %s want %s", id, result.Status, status)
		}
		if result.Result != nil {
			t.Fatalf("late handler result should not overwrite timeout error for %s: %#v", id, result)
		}
	}
}

func TestEventBusMiddlewareTimeoutCancelAbortAndResultSchemaTaxonomyRemainsExplicit(t *testing.T) {
	serialBus := abxbus.NewEventBus("MiddlewareTaxonomySerialBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	parallelBus := abxbus.NewEventBus("MiddlewareTaxonomyParallelBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})

	serialBus.On("MiddlewareSchemaEvent", "bad_schema", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "not-a-number", nil
	}, nil)
	schemaEvent := abxbus.NewBaseEvent("MiddlewareSchemaEvent", nil)
	schemaEvent.EventResultType = map[string]any{"type": "number"}
	schemaEvent = serialBus.Emit(schemaEvent)
	if _, err := schemaEvent.Wait(); err != nil {
		t.Fatal(err)
	}
	schemaResults := schemaEvent.EventResults
	if len(schemaResults) != 1 {
		t.Fatalf("schema event should have one handler result, got %#v", schemaResults)
	}
	for _, result := range schemaResults {
		if result.Status != abxbus.EventResultError || !strings.Contains(fmt.Sprint(result.Error), "EventHandlerResultSchemaError") {
			t.Fatalf("schema mismatch should remain an explicit result-schema error, got %#v", result)
		}
	}

	serialBus.On("MiddlewareSerialTimeoutEvent", "slow_1", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		select {
		case <-time.After(100 * time.Millisecond):
			return "slow", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	serialBus.On("MiddlewareSerialTimeoutEvent", "slow_2", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		select {
		case <-time.After(100 * time.Millisecond):
			return "slow-2", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	serialTimeout := 0.01
	serialEvent := abxbus.NewBaseEvent("MiddlewareSerialTimeoutEvent", nil)
	serialEvent.EventTimeout = &serialTimeout
	serialEvent = serialBus.Emit(serialEvent)
	if _, err := serialEvent.Wait(); err != nil {
		t.Fatal(err)
	}
	serialErrors := eventResultErrorStrings(serialEvent)
	if !containsErrorText(serialErrors, "Cancelled pending handler") {
		t.Fatalf("serial event timeout should cancel pending handlers explicitly, got %#v", serialErrors)
	}
	if !containsErrorText(serialErrors, "Aborted running handler") && !containsErrorText(serialErrors, "timed out") {
		t.Fatalf("serial event timeout should abort or time out a running handler explicitly, got %#v", serialErrors)
	}

	parallelBus.On("MiddlewareParallelTimeoutEvent", "slow_1", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		select {
		case <-time.After(100 * time.Millisecond):
			return "slow", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	parallelBus.On("MiddlewareParallelTimeoutEvent", "slow_2", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		select {
		case <-time.After(100 * time.Millisecond):
			return "slow-2", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	parallelTimeout := 0.01
	parallelEvent := abxbus.NewBaseEvent("MiddlewareParallelTimeoutEvent", nil)
	parallelEvent.EventTimeout = &parallelTimeout
	parallelEvent = parallelBus.Emit(parallelEvent)
	if _, err := parallelEvent.Wait(); err != nil {
		t.Fatal(err)
	}
	parallelErrors := eventResultErrorStrings(parallelEvent)
	if !containsErrorText(parallelErrors, "Aborted running handler") && !containsErrorText(parallelErrors, "timed out") {
		t.Fatalf("parallel event timeout should abort or time out running handlers explicitly, got %#v", parallelErrors)
	}
	if containsErrorText(parallelErrors, "Cancelled pending handler") {
		t.Fatalf("parallel event timeout should not cancel pending handlers when all handlers have started, got %#v", parallelErrors)
	}
}

func eventResultErrorStrings(event *abxbus.BaseEvent) []string {
	errors := []string{}
	for _, result := range event.EventResults {
		if result.Error != nil {
			errors = append(errors, fmt.Sprint(result.Error))
		}
	}
	return errors
}

func containsErrorText(errors []string, text string) bool {
	for _, err := range errors {
		if strings.Contains(err, text) {
			return true
		}
	}
	return false
}

func TestEventBusMiddlewareHooksArePerBusOnForwardedEvents(t *testing.T) {
	middlewareA := newRecordingMiddleware("a", nil)
	middlewareB := newRecordingMiddleware("b", nil)
	busA := abxbus.NewEventBus("MiddlewareForwardA", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middlewareA}})
	busB := abxbus.NewEventBus("MiddlewareForwardB", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middlewareB}})
	handlerA := busA.On("MiddlewareForwardEvent", "forward", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		busB.Emit(event)
		return "forwarded", nil
	}, nil)
	handlerB := busB.On("MiddlewareForwardEvent", "target", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "target", nil
	}, nil)

	if _, err := busA.Emit(abxbus.NewBaseEvent("MiddlewareForwardEvent", nil)).Now(); err != nil {
		t.Fatal(err)
	}

	recordsA := recordsByHook(middlewareA.snapshot(), "result")
	recordsB := recordsByHook(middlewareB.snapshot(), "result")
	if !recordsContainHandler(recordsA, handlerA.ID) || recordsContainHandler(recordsA, handlerB.ID) {
		t.Fatalf("source middleware should only observe source handler, got %#v", recordsA)
	}
	if !recordsContainHandler(recordsB, handlerB.ID) {
		t.Fatalf("target middleware should observe target handler, got %#v", recordsB)
	}
}

func TestEventBusMiddlewareHooksCoverStringAndWildcardPatterns(t *testing.T) {
	middleware := newRecordingMiddleware("patterns", nil)
	bus := abxbus.NewEventBus("MiddlewarePatternBus", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})
	stringHandler := bus.On("MiddlewarePatternEvent", "string", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "string:" + event.EventType, nil
	}, nil)
	wildcardHandler := bus.OnEventName("*", "wildcard", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "wildcard:" + event.EventType, nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("MiddlewarePatternEvent", nil))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	bus.Off("MiddlewarePatternEvent", stringHandler)
	bus.Off("*", wildcardHandler)

	records := middleware.snapshot()
	handlerRecords := recordsByHook(records, "handler")
	expectedPatternByID := map[string]string{
		stringHandler.ID:   "MiddlewarePatternEvent",
		wildcardHandler.ID: "*",
	}
	registered := map[string]bool{}
	unregistered := map[string]bool{}
	for _, record := range handlerRecords {
		expectedPattern, ok := expectedPatternByID[record.HandlerID]
		if !ok {
			t.Fatalf("unexpected handler record: %#v", record)
		}
		if record.EventPattern != expectedPattern {
			t.Fatalf("handler pattern changed: %#v", record)
		}
		if record.Registered {
			registered[record.HandlerID] = true
		} else {
			unregistered[record.HandlerID] = true
		}
	}
	for handlerID := range expectedPatternByID {
		if !registered[handlerID] || !unregistered[handlerID] {
			t.Fatalf("expected register/unregister records for %s, got %#v", handlerID, handlerRecords)
		}
	}

	resultRecords := recordsByHook(records, "result")
	if !recordsContainHandler(resultRecords, stringHandler.ID) || !recordsContainHandler(resultRecords, wildcardHandler.ID) {
		t.Fatalf("expected both string and wildcard handlers in result hooks, got %#v", resultRecords)
	}
	if event.EventResults[stringHandler.ID].Result != "string:MiddlewarePatternEvent" {
		t.Fatalf("unexpected string handler result: %#v", event.EventResults[stringHandler.ID])
	}
	if event.EventResults[wildcardHandler.ID].Result != "wildcard:MiddlewarePatternEvent" {
		t.Fatalf("unexpected wildcard handler result: %#v", event.EventResults[wildcardHandler.ID])
	}
}

func recordsContainHandler(records []middlewareRecord, handlerID string) bool {
	for _, record := range records {
		if record.HandlerID == handlerID {
			return true
		}
	}
	return false
}

func middlewareIndexOf(values []string, needle string) int {
	for i, value := range values {
		if value == needle {
			return i
		}
	}
	return -1
}

// Folded from eventbus_name_conflict_gc_test.go to keep test layout class-based.
func TestSameNameEventBusesKeepIndependentIDsHandlersAndHistory(t *testing.T) {
	first := abxbus.NewEventBus("DuplicateNameBus", nil)
	second := abxbus.NewEventBus("DuplicateNameBus", nil)
	defer first.Destroy()
	defer second.Destroy()
	if first.ID == second.ID {
		t.Fatal("same-name buses should still have distinct ids")
	}

	first.On("NameConflictEvent", "first", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "first", nil
	}, nil)
	second.On("NameConflictEvent", "second", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "second", nil
	}, nil)

	firstResult, err := first.Emit(abxbus.NewBaseEvent("NameConflictEvent", nil)).EventResult()
	if err != nil {
		t.Fatal(err)
	}
	secondResult, err := second.Emit(abxbus.NewBaseEvent("NameConflictEvent", nil)).EventResult()
	if err != nil {
		t.Fatal(err)
	}
	if firstResult != "first" || secondResult != "second" {
		t.Fatalf("same-name bus handlers crossed: first=%#v second=%#v", firstResult, secondResult)
	}
	if first.EventHistory.Size() != 1 || second.EventHistory.Size() != 1 {
		t.Fatalf("same-name bus histories should remain isolated, got %d and %d", first.EventHistory.Size(), second.EventHistory.Size())
	}
}

// Folded from eventbus_teardown_test.go to keep test layout class-based.
func TestDestroyClearFalsePreservesHandlersAndHistoryResolvesWaitersAndIsTerminal(t *testing.T) {
	bus := abxbus.NewEventBus("DestroyClearFalseTerminalBus", nil)
	var calls atomic.Int32
	bus.On("Evt", "h", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		calls.Add(1)
		return "ok", nil
	}, nil)

	e := bus.Emit(abxbus.NewBaseEvent("Evt", nil))
	if _, err := e.Now(); err != nil {
		t.Fatal(err)
	}

	timeout := 1.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("expected bus to become idle before destroy")
	}
	if bus.EventHistory.Size() != 1 {
		t.Fatalf("expected one event in history before destroy, got %d", bus.EventHistory.Size())
	}

	destroyed := make(chan struct{})
	go func() {
		time.Sleep(20 * time.Millisecond)
		bus.DestroyWithOptions(&abxbus.EventBusDestroyOptions{Clear: false})
		close(destroyed)
	}()
	match, err := bus.FindEventName("NeverHappens", nil, &abxbus.FindOptions{Past: false, Future: true})
	if err != nil {
		t.Fatal(err)
	}
	if match != nil {
		t.Fatalf("destroy should resolve pending find with nil, got %#v", match)
	}
	select {
	case <-destroyed:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for destroy")
	}

	if !bus.IsDestroyed() {
		t.Fatal("clear=false destroy should mark the bus destroyed")
	}
	if bus.EventHistory.Size() != 1 {
		t.Fatalf("clear=false destroy should preserve history, got %d events", bus.EventHistory.Size())
	}
	if !bus.IsIdleAndQueueEmpty() {
		t.Fatal("expected idle after destroy")
	}
	payloadBytes, err := bus.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload["handlers"].(map[string]any)) != 1 || len(payload["event_history"].(map[string]any)) != 1 {
		t.Fatalf("clear=false should preserve handlers and history payload: %#v", payload)
	}

	assertDestroyedPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			recovered := recover()
			if recovered == nil {
				t.Fatalf("%s should panic after clear=false destroy", name)
			}
			if !errors.Is(recovered.(error), abxbus.ErrEventBusDestroyed) {
				t.Fatalf("expected destroyed error panic, got %#v", recovered)
			}
		}()
		fn()
	}
	assertDestroyedPanic("Emit", func() { bus.Emit(abxbus.NewBaseEvent("Evt", nil)) })
	assertDestroyedPanic("On", func() {
		bus.On("Evt", "again", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
			return nil, nil
		}, nil)
	})
	assertDestroyedPanic("On", func() {
		type ClearFalseDestroyedEvent struct{}
		bus.On(func(payload ClearFalseDestroyedEvent) {})
	})
	if _, err := bus.FindEventName("Evt", nil, nil); !errors.Is(err, abxbus.ErrEventBusDestroyed) {
		t.Fatalf("Find should reject with ErrEventBusDestroyed, got %v", err)
	}
}

func TestDestroyIsImmediateAndRejectsLateHandlerEmits(t *testing.T) {
	bus := abxbus.NewEventBus("DestroyImmediateBus", nil)
	started := make(chan struct{})
	release := make(chan struct{})
	lateEmitRejected := make(chan bool, 1)
	var startOnce sync.Once

	bus.On("SlowEvt", "slow", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		startOnce.Do(func() { close(started) })
		<-release
		rejected := false
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					if err, ok := recovered.(error); ok && errors.Is(err, abxbus.ErrEventBusDestroyed) {
						rejected = true
					}
				}
			}()
			bus.Emit(abxbus.NewBaseEvent("LateEvt", nil))
		}()
		lateEmitRejected <- rejected
		return "slow", nil
	}, nil)

	bus.Emit(abxbus.NewBaseEvent("SlowEvt", nil))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for slow handler to start")
	}

	start := time.Now()
	bus.DestroyWithOptions(&abxbus.EventBusDestroyOptions{Clear: false})
	if elapsed := time.Since(start); elapsed >= 50*time.Millisecond {
		t.Fatalf("Destroy should be immediate, elapsed=%s", elapsed)
	}
	if !bus.IsDestroyed() {
		t.Fatal("destroy should mark bus destroyed")
	}

	func() {
		defer func() {
			recovered := recover()
			if recovered == nil {
				t.Fatal("outside Emit should panic after destroy")
			}
			if !errors.Is(recovered.(error), abxbus.ErrEventBusDestroyed) {
				t.Fatalf("expected destroyed error panic, got %#v", recovered)
			}
		}()
		bus.Emit(abxbus.NewBaseEvent("OutsideEvt", nil))
	}()

	close(release)
	select {
	case rejected := <-lateEmitRejected:
		if !rejected {
			t.Fatal("late handler emit should reject after destroy")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for late handler emit")
	}
}

func TestDestroyDefaultClearIsTerminalAndFreesBusState(t *testing.T) {
	bus := abxbus.NewEventBus("TerminalDestroyBus", nil)

	bus.On("Evt", "h", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, nil)
	event := bus.Emit(abxbus.NewBaseEvent("Evt", nil))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}

	bus.Destroy()
	if !bus.IsDestroyed() {
		t.Fatal("Destroy() should mark the bus destroyed")
	}
	if bus.EventHistory.Size() != 0 {
		t.Fatalf("Destroy() should clear history by default, got %d events", bus.EventHistory.Size())
	}
	if !bus.IsIdleAndQueueEmpty() {
		t.Fatal("destroyed bus should be idle with an empty queue")
	}
	payloadBytes, err := bus.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]any{
		"handlers":            payload["handlers"],
		"handlers_by_key":     payload["handlers_by_key"],
		"event_history":       payload["event_history"],
		"pending_event_queue": payload["pending_event_queue"],
	} {
		switch typed := value.(type) {
		case map[string]any:
			if len(typed) != 0 {
				t.Fatalf("Destroy() should clear %s, got %#v", key, typed)
			}
		case []any:
			if len(typed) != 0 {
				t.Fatalf("Destroy() should clear %s, got %#v", key, typed)
			}
		default:
			t.Fatalf("unexpected %s payload shape: %#v", key, value)
		}
	}

	assertDestroyedPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			recovered := recover()
			if recovered == nil {
				t.Fatalf("%s should panic after terminal destroy", name)
			}
			err, ok := recovered.(error)
			if !ok {
				t.Fatalf("%s panic should be an error, got %#v", name, recovered)
			}
			var destroyed *abxbus.EventBusDestroyedError
			if !errors.As(err, &destroyed) || !errors.Is(err, abxbus.ErrEventBusDestroyed) {
				t.Fatalf("%s panic should expose EventBusDestroyedError, got %T %v", name, err, err)
			}
			if destroyed.Operation != name {
				t.Fatalf("expected operation %s, got %s", name, destroyed.Operation)
			}
			if !strings.Contains(err.Error(), "TerminalDestroyBus#") || !strings.Contains(err.Error(), "event bus has been destroyed") {
				t.Fatalf("unexpected destroyed error shape: %v", err)
			}
		}()
		fn()
	}
	assertDestroyedPanic("Emit", func() { bus.Emit(abxbus.NewBaseEvent("Evt", nil)) })
	assertDestroyedPanic("On", func() {
		bus.On("Evt", "new", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) { return nil, nil }, nil)
	})
	assertDestroyedPanic("On", func() {
		type TerminalDestroyedEvent struct{}
		bus.On(func(payload TerminalDestroyedEvent) {})
	})

	if _, err := bus.FindEventName("Evt", nil, nil); !errors.Is(err, abxbus.ErrEventBusDestroyed) {
		t.Fatalf("Find should reject with ErrEventBusDestroyed, got %v", err)
	}
	if _, err := bus.FilterEventName("Evt", nil, nil); !errors.Is(err, abxbus.ErrEventBusDestroyed) {
		t.Fatalf("Filter should reject with ErrEventBusDestroyed, got %v", err)
	}
}

func TestDestroyingOneBusDoesNotBreakSharedHandlersOrForwardTargets(t *testing.T) {
	source := abxbus.NewEventBus("DestroySharedSourceBus", nil)
	target := abxbus.NewEventBus("DestroySharedTargetBus", nil)
	defer target.Destroy()

	var seen atomic.Int32
	shared := func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		seen.Add(1)
		return "shared", nil
	}
	source.On("SharedDestroyEvent", "shared_source", shared, nil)
	source.OnEventName("*", "forward", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return target.Emit(event), nil
	}, nil)
	target.On("SharedDestroyEvent", "shared_target", shared, nil)

	forwarded := source.Emit(abxbus.NewBaseEvent("SharedDestroyEvent", nil))
	if _, err := forwarded.Now(); err != nil {
		t.Fatal(err)
	}
	if seen.Load() != 2 {
		t.Fatalf("expected shared handler on source and target, got %d calls", seen.Load())
	}

	source.Destroy()

	direct := target.Emit(abxbus.NewBaseEvent("SharedDestroyEvent", nil))
	completedDirect, err := direct.Now()
	if err != nil {
		t.Fatalf("target should still process after source destroy: %v", err)
	}
	if result, err := completedDirect.EventResult(); err != nil || result != "shared" {
		t.Fatalf("destroying source should not affect target; result=%#v err=%v", result, err)
	}
	if seen.Load() != 3 {
		t.Fatalf("target handler should still run after source destroy, calls=%d", seen.Load())
	}
}

// Folded from optional_dependencies_test.go to keep test layout class-based.
func TestGoIntegrationSurfaceOnlyIncludesSupportedBridgeAndMiddleware(t *testing.T) {
	var _ = abxbus.NewJSONLEventBridge
	var _ abxbus.EventBusMiddleware = abxbus.NewOtelTracingMiddleware(nil)

	goRoot := goPackageRoot(t)
	entries, err := os.ReadDir(goRoot)
	if err != nil {
		t.Fatal(err)
	}

	bridgeFiles := map[string]bool{}
	middlewareFiles := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		switch {
		case strings.HasSuffix(entry.Name(), "_bridge.go"):
			bridgeFiles[entry.Name()] = true
		case strings.HasSuffix(entry.Name(), "_middleware.go") || entry.Name() == "middleware.go":
			middlewareFiles[entry.Name()] = true
		}
	}

	if len(bridgeFiles) != 1 || !bridgeFiles["jsonl_bridge.go"] {
		t.Fatalf("Go should only implement JSONL bridge for now, got %v", bridgeFiles)
	}
	expectedMiddlewareFiles := map[string]bool{"middleware.go": true, "otel_middleware.go": true}
	if len(middlewareFiles) != len(expectedMiddlewareFiles) {
		t.Fatalf("unexpected Go middleware files: %v", middlewareFiles)
	}
	for expected := range expectedMiddlewareFiles {
		if !middlewareFiles[expected] {
			t.Fatalf("missing expected Go middleware file %s, got %v", expected, middlewareFiles)
		}
	}
}

func TestGoUnsupportedBridgeAPIsAndDependenciesAreAbsent(t *testing.T) {
	goRoot := goPackageRoot(t)
	sourceFiles, err := filepath.Glob(filepath.Join(goRoot, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	unsupportedAPI := regexp.MustCompile(`\b(?:New)?(?:HTTP|Socket|SQLite|Redis|NATS|Postgres|Tachyon)EventBridge\b`)
	for _, path := range sourceFiles {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if unsupportedAPI.Match(source) {
			t.Fatalf("unsupported Go bridge API leaked into %s", filepath.Base(path))
		}
	}

	for _, filename := range []string{"go.mod", "go.sum"} {
		data, err := os.ReadFile(filepath.Join(goRoot, filename))
		if err != nil {
			t.Fatal(err)
		}
		text := string(data)
		for _, forbidden := range []string{
			"github.com/jackc/pgx",
			"github.com/lib/pq",
			"github.com/redis",
			"github.com/nats-io",
			"modernc.org/sqlite",
			"github.com/mattn/go-sqlite3",
			"tachyon",
		} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s unexpectedly references unsupported optional integration dependency %q", filename, forbidden)
			}
		}
	}
}

func goPackageRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(filepath.Dir(filename))
}

// Folded from otel_tracing_middleware_test.go to keep test layout class-based.
func TestOtelTracingMiddlewareCreatesEventAndHandlerSpans(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	middleware := abxbus.NewOtelTracingMiddleware(&abxbus.OtelTracingMiddlewareOptions{
		Tracer: provider.Tracer("abxbus-test"),
		RootSpanAttributes: []attribute.KeyValue{
			attribute.String("stagehand.session_id", "session-123"),
		},
	})
	bus := abxbus.NewEventBus("OtelBus", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})
	bus.On("OtelEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("OtelEvent", map[string]any{"session_id": "event-session-456", "value": "x"}))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}

	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("expected event and handler spans, got %d", len(spans))
	}

	names := map[string]bool{}
	for _, span := range spans {
		names[span.Name()] = true
	}
	if !names["OtelBus.emit(OtelEvent)"] {
		t.Fatalf("missing event span: %#v", names)
	}
	if !names["handler(OtelEvent)"] {
		t.Fatalf("missing handler span: %#v", names)
	}
	eventSpan := findSpan(t, spans, "OtelBus.emit(OtelEvent)")
	handlerSpan := findSpan(t, spans, "handler(OtelEvent)")
	eventAttrs := spanAttributes(eventSpan)
	handlerAttrs := spanAttributes(handlerSpan)
	if eventAttrs["abxbus.event_bus.name"] != "OtelBus" || eventAttrs["abxbus.event_type"] != "OtelEvent" {
		t.Fatalf("event span attributes did not match abxbus schema: %#v", eventAttrs)
	}
	if eventAttrs["abxbus.trace.root"] != true {
		t.Fatalf("top-level event span should be marked as trace root: %#v", eventAttrs)
	}
	if eventAttrs["stagehand.session_id"] != "session-123" || eventAttrs["abxbus.session_id"] != "event-session-456" {
		t.Fatalf("missing root/session attrs: %#v", eventAttrs)
	}
	if handlerAttrs["abxbus.handler_name"] != "handler" || handlerAttrs["abxbus.event_result_status"] != "completed" {
		t.Fatalf("handler span attributes did not match abxbus schema: %#v", handlerAttrs)
	}
}

func TestOtelTracingMiddlewareNamesEventAndHandlerSpansForDisplay(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	middleware := abxbus.NewOtelTracingMiddleware(&abxbus.OtelTracingMiddlewareOptions{
		Tracer: provider.Tracer("abxbus-test"),
	})
	bus := abxbus.NewEventBus("StagehandExtensionBackground", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})
	t.Cleanup(bus.Destroy)
	bus.On("CDPConnect", "DebuggerClient.on_CDPConnect", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "connected", nil
	}, nil)

	eventTimeout := 0.2
	event := abxbus.NewBaseEvent("CDPConnect", nil)
	event.EventTimeout = &eventTimeout
	if _, err := bus.Emit(event).Now(); err != nil {
		t.Fatal(err)
	}

	_ = findSpan(t, recorder.Ended(), "StagehandExtensionBackground.emit(CDPConnect)")
	_ = findSpan(t, recorder.Ended(), "DebuggerClient.on_CDPConnect(CDPConnect)")
}

func TestOtelTracingMiddlewareParentsChildEventToEmittingHandlerSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	middleware := abxbus.NewOtelTracingMiddleware(&abxbus.OtelTracingMiddlewareOptions{
		Tracer: provider.Tracer("abxbus-test"),
	})
	bus := abxbus.NewEventBus("OtelParentBus", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})
	bus.On("ParentOtelEvent", "parent", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		child := event.Emit(abxbus.NewBaseEvent("ChildOtelEvent", nil))
		if _, err := child.Now(); err != nil {
			return nil, err
		}
		return "parent", nil
	}, nil)
	bus.On("ChildOtelEvent", "child", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "child", nil
	}, nil)

	parent := bus.Emit(abxbus.NewBaseEvent("ParentOtelEvent", nil))
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}

	spans := recorder.Ended()
	parentEventSpan := findSpan(t, spans, "OtelParentBus.emit(ParentOtelEvent)")
	parentHandlerSpan := findSpan(t, spans, "parent(ParentOtelEvent)")
	childEventSpan := findSpan(t, spans, "OtelParentBus.emit(ChildOtelEvent)")
	childHandlerSpan := findSpan(t, spans, "child(ChildOtelEvent)")

	if parentHandlerSpan.Parent().SpanID() != parentEventSpan.SpanContext().SpanID() {
		t.Fatalf("parent handler should be child of parent event")
	}
	if childEventSpan.Parent().SpanID() != parentHandlerSpan.SpanContext().SpanID() {
		t.Fatalf("child event should be child of emitting handler")
	}
	if childHandlerSpan.Parent().SpanID() != childEventSpan.SpanContext().SpanID() {
		t.Fatalf("child handler should be child of child event")
	}
	childAttrs := spanAttributes(childEventSpan)
	if childAttrs["abxbus.event_parent_id"] != parent.EventID {
		t.Fatalf("child event span missing parent id attr: %#v", childAttrs)
	}
	if _, ok := childAttrs["abxbus.event_emitted_by_handler_id"].(string); !ok {
		t.Fatalf("child event span missing emitted-by handler attr: %#v", childAttrs)
	}
}

func TestOtelTracingMiddlewareWaitsUntilTopLevelEventCompletionBeforeEndingSpans(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	middleware := abxbus.NewOtelTracingMiddleware(&abxbus.OtelTracingMiddlewareOptions{
		Tracer: provider.Tracer("abxbus-test"),
	})
	bus := abxbus.NewEventBus("OtelRootStartBus", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})
	t.Cleanup(bus.Destroy)

	started := make(chan struct{})
	release := make(chan struct{})
	bus.On("OtelRootStartEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		close(started)
		select {
		case <-release:
			return "done", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)

	eventTimeout := 0.5
	event := abxbus.NewBaseEvent("OtelRootStartEvent", nil)
	event.EventTimeout = &eventTimeout
	emitted := bus.Emit(event)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}
	if ended := recorder.Ended(); len(ended) != 0 {
		t.Fatalf("spans should not be ended/exportable before event completion, got %d", len(ended))
	}

	close(release)
	if _, err := emitted.Now(); err != nil {
		t.Fatal(err)
	}

	eventSpan := findSpan(t, recorder.Ended(), "OtelRootStartBus.emit(OtelRootStartEvent)")
	handlerSpan := findSpan(t, recorder.Ended(), "handler(OtelRootStartEvent)")
	if eventSpan.Parent().IsValid() {
		t.Fatalf("top-level event span should be root, got parent=%s", eventSpan.Parent().SpanID())
	}
	if handlerSpan.Parent().SpanID() != eventSpan.SpanContext().SpanID() {
		t.Fatalf("handler span should be child of event span")
	}
	if !eventSpan.EndTime().After(eventSpan.StartTime()) || !handlerSpan.EndTime().After(handlerSpan.StartTime()) {
		t.Fatalf("ended spans should have non-zero duration")
	}
}

func TestOtelTracingMiddlewareRecordsHandlerErrors(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	middleware := abxbus.NewOtelTracingMiddleware(&abxbus.OtelTracingMiddlewareOptions{
		Tracer: provider.Tracer("abxbus-test"),
	})
	bus := abxbus.NewEventBus("OtelErrorBus", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})
	handlerErr := errors.New("handler failed")
	bus.On("OtelErrorEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, handlerErr
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("OtelErrorEvent", nil))
	if _, err := event.Wait(); err != nil {
		t.Fatal(err)
	}

	eventSpan := findSpan(t, recorder.Ended(), "OtelErrorBus.emit(OtelErrorEvent)")
	handlerSpan := findSpan(t, recorder.Ended(), "handler(OtelErrorEvent)")
	if eventSpan.Status().Code != codes.Error {
		t.Fatalf("event span should be error, got %#v", eventSpan.Status())
	}
	if handlerSpan.Status().Code != codes.Error {
		t.Fatalf("handler span should be error, got %#v", handlerSpan.Status())
	}
}

func findSpan(t *testing.T, spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, span := range spans {
		if span.Name() == name {
			return span
		}
	}
	t.Fatalf("missing span %q", name)
	return nil
}

func spanAttributes(span sdktrace.ReadOnlySpan) map[string]any {
	attrs := map[string]any{}
	for _, attr := range span.Attributes() {
		attrs[string(attr.Key)] = attr.Value.AsInterface()
	}
	return attrs
}
