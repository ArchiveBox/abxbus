package abxbus_test

import (
	"context"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

const debounceTargetID1 = "9b447756-908c-7b75-8a51-4a2c2b4d9b14"
const debounceTargetID2 = "194870e1-fa02-70a4-8101-d10d57c3449c"

func debounceEmitFallback(bus *abxbus.EventBus, eventType string, payload map[string]any, found *abxbus.BaseEvent) *abxbus.BaseEvent {
	if found != nil {
		return found
	}
	return bus.Emit(abxbus.NewBaseEvent(eventType, payload))
}

func TestSimpleDebounceWithChildOfReusesRecentEvent(t *testing.T) {
	bus := abxbus.NewEventBus("DebounceBus", nil)
	bus.On("ScreenshotEvent", "complete_screenshot", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "screenshot_done", nil
	}, nil)

	parent := bus.Emit(abxbus.NewBaseEvent("ParentEvent", nil))
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}
	child := parent.Emit(abxbus.NewBaseEvent("ScreenshotEvent", map[string]any{"target_id": debounceTargetID1}))
	if _, err := child.Now(); err != nil {
		t.Fatal(err)
	}

	found, err := bus.Find("ScreenshotEvent", nil, &abxbus.FindOptions{
		Past:    10.0,
		Future:  false,
		ChildOf: parent,
	})
	if err != nil {
		t.Fatal(err)
	}
	reused := debounceEmitFallback(bus, "ScreenshotEvent", map[string]any{"target_id": debounceTargetID2}, found)
	if _, err := reused.Now(); err != nil {
		t.Fatal(err)
	}

	if reused.EventID != child.EventID {
		t.Fatalf("expected debounce to reuse child %s, got %s", child.EventID, reused.EventID)
	}
	if reused.EventParentID == nil || *reused.EventParentID != parent.EventID {
		t.Fatalf("expected reused child parent id %s, got %v", parent.EventID, reused.EventParentID)
	}
}

func TestSimpleDebounceUsesRecentHistoryOrDispatchesNew(t *testing.T) {
	TestSimpleDebounceWithChildOfReusesRecentEvent(t)
}

func TestDebounceUsesFutureMatchBeforeDispatchFallback(t *testing.T) {
	bus := abxbus.NewEventBus("AdvancedDebounceBus", nil)
	pending := make(chan *abxbus.BaseEvent, 1)
	errs := make(chan error, 1)

	go func() {
		found, err := bus.Find("SyncEvent", nil, &abxbus.FindOptions{Past: false, Future: 0.5})
		if err != nil {
			errs <- err
			return
		}
		pending <- found
	}()
	go func() {
		time.Sleep(50 * time.Millisecond)
		bus.Emit(abxbus.NewBaseEvent("SyncEvent", nil))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	historyMatch, err := bus.Find("SyncEvent", nil, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	var futureMatch *abxbus.BaseEvent
	select {
	case err := <-errs:
		t.Fatal(err)
	case futureMatch = <-pending:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	resolved := historyMatch
	if resolved == nil {
		resolved = futureMatch
	}
	if resolved == nil {
		resolved = bus.Emit(abxbus.NewBaseEvent("SyncEvent", nil))
	}
	if _, err := resolved.Now(); err != nil {
		t.Fatal(err)
	}
	if resolved.EventType != "SyncEvent" {
		t.Fatalf("expected SyncEvent, got %s", resolved.EventType)
	}
}

func TestAdvancedDebouncePrefersHistoryThenWaitsFutureThenDispatches(t *testing.T) {
	TestDebounceUsesFutureMatchBeforeDispatchFallback(t)
}

func TestAdvancedDebouncePrefersHistoryThenWaitsForFutureThenDispatches(t *testing.T) {
	TestDebounceUsesFutureMatchBeforeDispatchFallback(t)
}

func TestDebouncePrefersRecentHistory(t *testing.T) {
	bus := abxbus.NewEventBus("DebounceFreshBus", nil)
	bus.On("ScreenshotEvent", "complete", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "done", nil
	}, nil)

	original := bus.Emit(abxbus.NewBaseEvent("ScreenshotEvent", map[string]any{"target_id": debounceTargetID1}))
	if _, err := original.Now(); err != nil {
		t.Fatal(err)
	}

	isFresh := func(event *abxbus.BaseEvent) bool {
		if event.EventCompletedAt == nil {
			return false
		}
		completedAt, err := time.Parse(time.RFC3339Nano, *event.EventCompletedAt)
		if err != nil {
			t.Fatal(err)
		}
		return time.Since(completedAt) < 5*time.Second
	}
	found, err := bus.Find("ScreenshotEvent", func(event *abxbus.BaseEvent) bool {
		return event.Payload["target_id"] == debounceTargetID1 && isFresh(event)
	}, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	result := debounceEmitFallback(bus, "ScreenshotEvent", map[string]any{"target_id": debounceTargetID1}, found)
	if _, err := result.Now(); err != nil {
		t.Fatal(err)
	}
	if result.EventID != original.EventID {
		t.Fatalf("expected debounce to reuse %s, got %s", original.EventID, result.EventID)
	}
}

func TestReturnsExistingFreshEvent(t *testing.T) {
	TestDebouncePrefersRecentHistory(t)
}

func TestDebounceReturnsExistingFreshEvent(t *testing.T) {
	TestDebouncePrefersRecentHistory(t)
}

func TestDebounceDispatchesWhenRecentMissing(t *testing.T) {
	bus := abxbus.NewEventBus("DebounceNoMatchBus", nil)
	bus.On("ScreenshotEvent", "complete", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "done", nil
	}, nil)

	found, err := bus.Find("ScreenshotEvent", func(event *abxbus.BaseEvent) bool {
		return event.Payload["target_id"] == debounceTargetID1
	}, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	result := debounceEmitFallback(bus, "ScreenshotEvent", map[string]any{"target_id": debounceTargetID1}, found)
	if _, err := result.Now(); err != nil {
		t.Fatal(err)
	}
	if result.Payload["target_id"] != debounceTargetID1 {
		t.Fatalf("expected target_id=%s, got %#v", debounceTargetID1, result.Payload["target_id"])
	}
	if result.EventStatus != "completed" {
		t.Fatalf("expected completed event, got %s", result.EventStatus)
	}
}

func TestDispatchesNewWhenNoMatch(t *testing.T) {
	TestDebounceDispatchesWhenRecentMissing(t)
}

func TestDebounceDispatchesNewWhenNoMatch(t *testing.T) {
	TestDebounceDispatchesWhenRecentMissing(t)
}

func TestDispatchesNewWhenStale(t *testing.T) {
	bus := abxbus.NewEventBus("DebounceStaleBus", nil)
	bus.On("ScreenshotEvent", "complete", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "done", nil
	}, nil)

	original := bus.Emit(abxbus.NewBaseEvent("ScreenshotEvent", map[string]any{"target_id": debounceTargetID1}))
	if _, err := original.Now(); err != nil {
		t.Fatal(err)
	}
	found, err := bus.Find("ScreenshotEvent", func(event *abxbus.BaseEvent) bool {
		return event.Payload["target_id"] == debounceTargetID2
	}, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	result := debounceEmitFallback(bus, "ScreenshotEvent", map[string]any{"target_id": debounceTargetID1}, found)
	if _, err := result.Now(); err != nil {
		t.Fatal(err)
	}

	count := 0
	for _, event := range bus.EventHistory.Values() {
		if event.EventType == "ScreenshotEvent" {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("expected stale debounce fallback to create second screenshot event, got %d", count)
	}
}

func TestDebounceDispatchesNewWhenExistingIsStale(t *testing.T) {
	TestDispatchesNewWhenStale(t)
}

func TestFindPastOnlyReturnsImmediatelyWithoutWaiting(t *testing.T) {
	bus := abxbus.NewEventBus("DebouncePastOnlyBus", nil)
	start := time.Now()
	result, err := bus.Find("ParentEvent", nil, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if result != nil {
		t.Fatalf("expected no past result, got %#v", result)
	}
	if elapsed >= 50*time.Millisecond {
		t.Fatalf("past-only empty find should return immediately, elapsed=%s", elapsed)
	}
}

func TestDebouncePastOnlyAndPastWindowLookupsReturnImmediatelyWhenEmpty(t *testing.T) {
	TestFindPastOnlyReturnsImmediatelyWithoutWaiting(t)
	TestFindPastFloatReturnsImmediatelyWithoutWaiting(t)
}

func TestFindPastFloatReturnsImmediatelyWithoutWaiting(t *testing.T) {
	bus := abxbus.NewEventBus("DebouncePastWindowBus", nil)
	start := time.Now()
	result, err := bus.Find("ParentEvent", nil, &abxbus.FindOptions{Past: 5.0, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if result != nil {
		t.Fatalf("expected no past-window result, got %#v", result)
	}
	if elapsed >= 50*time.Millisecond {
		t.Fatalf("past-window empty find should return immediately, elapsed=%s", elapsed)
	}
}

func TestOrChainWithoutWaitingFindsExisting(t *testing.T) {
	bus := abxbus.NewEventBus("DebounceOrChainExistingBus", nil)
	bus.On("ScreenshotEvent", "complete", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "done", nil
	}, nil)

	original := bus.Emit(abxbus.NewBaseEvent("ScreenshotEvent", map[string]any{"target_id": debounceTargetID1}))
	if _, err := original.Now(); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	found, err := bus.Find("ScreenshotEvent", func(event *abxbus.BaseEvent) bool {
		return event.Payload["target_id"] == debounceTargetID1
	}, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	result := debounceEmitFallback(bus, "ScreenshotEvent", map[string]any{"target_id": debounceTargetID1}, found)
	if _, err := result.Now(); err != nil {
		t.Fatal(err)
	}
	if result.EventID != original.EventID {
		t.Fatalf("expected existing event %s, got %s", original.EventID, result.EventID)
	}
	if elapsed := time.Since(start); elapsed >= 100*time.Millisecond {
		t.Fatalf("existing debounce lookup should not block, elapsed=%s", elapsed)
	}
}

func TestOrChainWithoutWaitingDispatchesWhenNoMatch(t *testing.T) {
	bus := abxbus.NewEventBus("DebounceOrChainNoMatchBus", nil)
	bus.On("ScreenshotEvent", "complete", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "done", nil
	}, nil)

	start := time.Now()
	found, err := bus.Find("ScreenshotEvent", func(event *abxbus.BaseEvent) bool {
		return event.Payload["target_id"] == debounceTargetID1
	}, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	result := debounceEmitFallback(bus, "ScreenshotEvent", map[string]any{"target_id": debounceTargetID1}, found)
	if _, err := result.Now(); err != nil {
		t.Fatal(err)
	}
	if result.Payload["target_id"] != debounceTargetID1 {
		t.Fatalf("expected target_id=%s, got %#v", debounceTargetID1, result.Payload["target_id"])
	}
	if elapsed := time.Since(start); elapsed >= 100*time.Millisecond {
		t.Fatalf("missing debounce lookup should dispatch without blocking, elapsed=%s", elapsed)
	}
}

func TestOrChainMultipleSequentialLookups(t *testing.T) {
	bus := abxbus.NewEventBus("DebounceSequentialBus", nil)
	bus.On("ScreenshotEvent", "complete", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "done", nil
	}, nil)

	start := time.Now()
	found1, err := bus.Find("ScreenshotEvent", func(event *abxbus.BaseEvent) bool {
		return event.Payload["target_id"] == debounceTargetID1
	}, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	result1 := debounceEmitFallback(bus, "ScreenshotEvent", map[string]any{"target_id": debounceTargetID1}, found1)

	found2, err := bus.Find("ScreenshotEvent", func(event *abxbus.BaseEvent) bool {
		return event.Payload["target_id"] == debounceTargetID1
	}, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	result2 := debounceEmitFallback(bus, "ScreenshotEvent", map[string]any{"target_id": debounceTargetID1}, found2)

	found3, err := bus.Find("ScreenshotEvent", func(event *abxbus.BaseEvent) bool {
		return event.Payload["target_id"] == debounceTargetID2
	}, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	result3 := debounceEmitFallback(bus, "ScreenshotEvent", map[string]any{"target_id": debounceTargetID2}, found3)

	for _, result := range []*abxbus.BaseEvent{result1, result2, result3} {
		if _, err := result.Now(); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed >= 200*time.Millisecond {
		t.Fatalf("sequential debounce lookups should not block, elapsed=%s", elapsed)
	}
	if result1.EventID != result2.EventID {
		t.Fatalf("expected sequential lookup to reuse first event, got %s and %s", result1.EventID, result2.EventID)
	}
	if result1.EventID == result3.EventID {
		t.Fatalf("expected second target to dispatch separately")
	}
	if result3.Payload["target_id"] != debounceTargetID2 {
		t.Fatalf("expected target_id=%s, got %#v", debounceTargetID2, result3.Payload["target_id"])
	}
}

func TestDebounceOrChainHandlesSequentialLookupsWithoutBlocking(t *testing.T) {
	TestOrChainMultipleSequentialLookups(t)
}
