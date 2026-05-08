package abxbus_test

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
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
	bus.On("CreateUserEvent", "first", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		calls = append(calls, "first")
		return "first", nil
	}, nil)
	bus.On("CreateUserEvent", "second", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		calls = append(calls, "second")
		return map[string]any{"user_id": "abc"}, nil
	}, nil)

	e := bus.Dispatch(abxbus.NewBaseEvent("CreateUserEvent", map[string]any{"email": "a@b.com"}))
	if _, err := e.Done(context.Background()); err != nil {
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

	values, err := e.EventResultsList(context.Background(), nil, &abxbus.EventResultsListOptions{RaiseIfAny: false, RaiseIfNone: false})
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
	bus.On("NoDropEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, nil)

	for i := 1; i <= 2; i++ {
		event := bus.Emit(abxbus.NewBaseEvent("NoDropEvent", map[string]any{"seq": i}))
		if _, err := event.Done(context.Background()); err != nil {
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
	bus.On("SlowEvent", "slow", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
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
		if _, err := event.Done(context.Background()); err != nil {
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
	bus.On("BurstEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
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
		if _, err := event.Done(context.Background()); err != nil {
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
	bus.On("ResultEvent", "on_create", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return map[string]any{"user_id": "abc"}, nil
	}, nil)
	e := bus.Emit(abxbus.NewBaseEvent("ResultEvent", map[string]any{"email": "a@b.com"}))
	result, err := e.EventResult(context.Background())
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
