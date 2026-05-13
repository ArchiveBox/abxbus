package abxbus_test

import (
	"context"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"
)

type TypedFindEvent struct {
	RequestID string `json:"request_id"`
	Count     int    `json:"count"`
}

func TestFindHistoryAndFuture(t *testing.T) {
	bus := abxbus.NewEventBus("FindBus", nil)
	seed := bus.EmitEventName("ResponseEvent", map[string]any{"request_id": "abc"})
	if _, err := seed.Now(); err != nil {
		t.Fatal(err)
	}

	match, err := bus.FindEventName("ResponseEvent", func(e *abxbus.BaseEvent) bool {
		return e.Payload["request_id"] == "abc"
	}, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if match == nil || match.EventID != seed.EventID {
		t.Fatal("expected history find to match seeded event")
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		bus.EmitEventName("FutureEvent", map[string]any{"request_id": "future"})
	}()
	future, err := bus.FindEventName("FutureEvent", nil, &abxbus.FindOptions{Past: false, Future: 1.0})
	if err != nil {
		t.Fatal(err)
	}
	if future == nil || future.EventType != "FutureEvent" {
		t.Fatalf("expected future find to resolve FutureEvent, got %#v", future)
	}
}

func TestFindAndFilterDefaultToTypedEvents(t *testing.T) {
	bus := abxbus.NewEventBus("TypedFindFilterBus", nil)
	first := bus.Emit(TypedFindEvent{RequestID: "one", Count: 1})
	second := bus.Emit(TypedFindEvent{RequestID: "two", Count: 2})
	if _, err := first.Now(); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Now(); err != nil {
		t.Fatal(err)
	}

	found, err := bus.Find(TypedFindEvent{}, func(payload TypedFindEvent) bool {
		return payload.RequestID == "two"
	}, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if found == nil || found.EventID != second.EventID {
		t.Fatalf("expected typed find to match second event, got %#v", found)
	}

	matches, err := bus.Filter(TypedFindEvent{}, func(payload TypedFindEvent) bool {
		return payload.Count >= 1
	}, &abxbus.FilterOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 || matches[0].EventID != second.EventID || matches[1].EventID != first.EventID {
		t.Fatalf("expected typed filter to return newest-first matches, got %#v", matches)
	}
}

func TestEmitEventNameCoversRawStringEmission(t *testing.T) {
	bus := abxbus.NewEventBus("EmitEventNameBus", nil)
	event := bus.EmitEventName("RawStringEvent", map[string]any{"ok": true})
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	if event.EventType != "RawStringEvent" || event.Payload["ok"] != true {
		t.Fatalf("unexpected raw event emission: %#v", event)
	}
}

func TestFindReturnsNilWhenNoMatch(t *testing.T) {
	bus := abxbus.NewEventBus("FindNilBus", nil)
	match, err := bus.FindEventName("MissingEvent", nil, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if match != nil {
		t.Fatalf("expected nil when no event matches, got %#v", match)
	}
}

func TestFindDefaultPastOnlyNoFutureWait(t *testing.T) {
	bus := abxbus.NewEventBus("FindDefaultBus", nil)
	seed := bus.EmitEventName("DefaultEvent", nil)
	if _, err := seed.Now(); err != nil {
		t.Fatal(err)
	}
	match, err := bus.FindEventName("DefaultEvent", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if match == nil || match.EventID != seed.EventID {
		t.Fatalf("expected default find to return past match")
	}
}

func TestFindFutureIgnoresPastEvents(t *testing.T) {
	bus := abxbus.NewEventBus("FindFutureIgnoresPastBus", nil)
	prior := bus.EmitEventName("ParentEvent", nil)
	if _, err := prior.Now(); err != nil {
		t.Fatal(err)
	}

	found, err := bus.FindEventName("ParentEvent", nil, &abxbus.FindOptions{Past: false, Future: 0.03})
	if err != nil {
		t.Fatal(err)
	}
	if found != nil {
		t.Fatalf("future-only find should ignore past events, got %#v", found)
	}
}

func TestFindPastFalseFutureFalseReturnsNilImmediately(t *testing.T) {
	bus := abxbus.NewEventBus("FindNeitherBus", nil)
	start := time.Now()
	found, err := bus.FindEventName("ParentEvent", nil, &abxbus.FindOptions{Past: false, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if found != nil {
		t.Fatalf("past=false future=false should return nil, got %#v", found)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("past=false future=false should not wait, elapsed=%s", elapsed)
	}
}

func TestFindPastAndFutureWindowsAreIndependent(t *testing.T) {
	bus := abxbus.NewEventBus("FindWindowIndependentBus", nil)
	oldEvent := bus.EmitEventName("ParentEvent", nil)
	if _, err := oldEvent.Now(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)

	start := time.Now()
	found, err := bus.FindEventName("ParentEvent", nil, &abxbus.FindOptions{Past: 0.03, Future: 0.03})
	if err != nil {
		t.Fatal(err)
	}
	if found != nil {
		t.Fatalf("old event outside past window should not match, got %#v", found)
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("future window should be waited independently after past miss, elapsed=%s", elapsed)
	}
}

func TestFindPastWindowAndEqualsFiltering(t *testing.T) {
	bus := abxbus.NewEventBus("FindWindowBus", nil)

	oldEvent := abxbus.NewBaseEvent("WindowEvent", map[string]any{"request_id": "old"})
	oldEvent.EventCreatedAt = time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339Nano)
	if _, err := bus.Emit(oldEvent).Now(); err != nil {
		t.Fatal(err)
	}

	newEvent := abxbus.NewBaseEvent("WindowEvent", map[string]any{"request_id": "new"})
	if _, err := bus.Emit(newEvent).Now(); err != nil {
		t.Fatal(err)
	}

	recent, err := bus.FindEventName("WindowEvent", nil, &abxbus.FindOptions{Past: 0.5, Future: false, Equals: map[string]any{"event_type": "WindowEvent", "event_status": "completed"}})
	if err != nil {
		t.Fatal(err)
	}
	if recent == nil || recent.EventID != newEvent.EventID {
		t.Fatalf("expected past-window filter to return recent event, got %#v", recent)
	}

	equalsMatch, err := bus.FindEventName("WindowEvent", nil, &abxbus.FindOptions{Past: true, Future: false, Equals: map[string]any{"request_id": "new"}})
	if err != nil {
		t.Fatal(err)
	}
	if equalsMatch == nil || equalsMatch.EventID != newEvent.EventID {
		t.Fatalf("expected equals filter to match payload value, got %#v", equalsMatch)
	}
}

func TestFindSupportsMetadataAndPayloadEqualityFilters(t *testing.T) {
	bus := abxbus.NewEventBus("FindEventFieldFilterBus", nil)
	eventA := abxbus.NewBaseEvent("FieldFilterEvent", map[string]any{"action": "logout", "user_id": "user-2"})
	eventTimeoutA := 11.0
	eventA.EventTimeout = &eventTimeoutA
	eventB := abxbus.NewBaseEvent("FieldFilterEvent", map[string]any{"action": "login", "user_id": "user-1"})
	eventTimeoutB := 22.0
	eventB.EventTimeout = &eventTimeoutB
	for _, event := range []*abxbus.BaseEvent{bus.Emit(eventA), bus.Emit(eventB)} {
		if _, err := event.Now(); err != nil {
			t.Fatal(err)
		}
	}

	foundA, err := bus.FindEventName("FieldFilterEvent", nil, &abxbus.FindOptions{
		Past:   true,
		Future: false,
		Equals: map[string]any{
			"event_id":      eventA.EventID,
			"event_timeout": 11,
			"event_status":  "completed",
			"action":        "logout",
			"user_id":       "user-2",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if foundA == nil || foundA.EventID != eventA.EventID {
		t.Fatalf("expected metadata and payload filters to match event A, got %#v", foundA)
	}

	mismatch, err := bus.FindEventName("FieldFilterEvent", nil, &abxbus.FindOptions{
		Past:   true,
		Future: false,
		Equals: map[string]any{
			"event_id":      eventA.EventID,
			"event_timeout": 22,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mismatch != nil {
		t.Fatalf("expected mismatched metadata filters to return nil, got %#v", mismatch)
	}

	foundPayload, err := bus.FindEventName("FieldFilterEvent", nil, &abxbus.FindOptions{
		Past:   true,
		Future: false,
		Equals: map[string]any{
			"action":  "login",
			"user_id": "user-1",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if foundPayload == nil || foundPayload.EventID != eventB.EventID {
		t.Fatalf("expected payload filters to match newest login event, got %#v", foundPayload)
	}
}

func TestFindWherePredicateAndBusScopedHistory(t *testing.T) {
	busA := abxbus.NewEventBus("FindBusA", nil)
	busB := abxbus.NewEventBus("FindBusB", nil)
	matchA := busA.EmitEventName("ScopedEvent", map[string]any{"source": "A", "value": 1})
	matchB := busB.EmitEventName("ScopedEvent", map[string]any{"source": "B", "value": 2})
	if _, err := matchA.Now(); err != nil {
		t.Fatal(err)
	}
	if _, err := matchB.Now(); err != nil {
		t.Fatal(err)
	}

	foundA, err := busA.FindEventName("ScopedEvent", func(event *abxbus.BaseEvent) bool {
		return event.Payload["source"] == "A" && event.Payload["value"] == 1
	}, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if foundA == nil || foundA.EventID != matchA.EventID {
		t.Fatalf("expected bus A to find only its own event, got %#v", foundA)
	}

	foundB, err := busB.FindEventName("ScopedEvent", func(event *abxbus.BaseEvent) bool {
		return event.Payload["source"] == "B"
	}, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if foundB == nil || foundB.EventID != matchB.EventID {
		t.Fatalf("expected bus B to find only its own event, got %#v", foundB)
	}
}

func TestFindChildOfFilteringAndLineageTraversal(t *testing.T) {
	bus := abxbus.NewEventBus("FindChildBus", nil)

	parent := bus.EmitEventName("Parent", nil)
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}

	child := abxbus.NewBaseEvent("Child", nil)
	child.EventParentID = &parent.EventID
	child = bus.Emit(child)
	if _, err := child.Now(); err != nil {
		t.Fatal(err)
	}

	grandchild := abxbus.NewBaseEvent("Grandchild", nil)
	grandchild.EventParentID = &child.EventID
	grandchild = bus.Emit(grandchild)
	if _, err := grandchild.Now(); err != nil {
		t.Fatal(err)
	}

	if !bus.EventIsChildOf(child, parent) {
		t.Fatal("expected direct child relation")
	}
	if !bus.EventIsChildOf(grandchild, parent) {
		t.Fatal("expected grandchild relation")
	}
	if !bus.EventIsParentOf(parent, child) {
		t.Fatal("expected parent relation")
	}
	if bus.EventIsChildOf(parent, child) {
		t.Fatal("parent should not be child of child")
	}
	if bus.EventIsChildOf(parent, parent) {
		t.Fatal("event should not be child of itself")
	}

	found, err := bus.FindEventName("Grandchild", nil, &abxbus.FindOptions{Past: true, Future: false, ChildOf: parent})
	if err != nil {
		t.Fatal(err)
	}
	if found == nil || found.EventType != "Grandchild" || found.EventParentID == nil || *found.EventParentID != child.EventID {
		t.Fatalf("expected child_of filter to return true descendant, got %#v", found)
	}
}

func TestFindCanSeeInProgressEventInHistory(t *testing.T) {
	bus := abxbus.NewEventBus("FindInProgressBus", nil)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	bus.OnEventName("SlowFindEvent", "slow", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		started <- struct{}{}
		<-release
		return "ok", nil
	}, nil)

	e := bus.EmitEventName("SlowFindEvent", nil)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("timed out waiting for slow handler start")
	}

	match, err := bus.FindEventName("SlowFindEvent", nil, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	if match == nil || match.EventID != e.EventID {
		close(release)
		t.Fatalf("expected in-progress event to be discoverable in history, got %#v", match)
	}

	close(release)
	if _, err := e.Now(); err != nil {
		t.Fatal(err)
	}
}

func TestFindFutureIgnoresAlreadyDispatchedInFlightEventsWhenPastFalse(t *testing.T) {
	bus := abxbus.NewEventBus("FindFutureIgnoresInflightBus", nil)
	t.Cleanup(bus.Destroy)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	bus.OnEventName("FutureInflightEvent", "slow", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return "ok", nil
	}, nil)

	event := bus.EmitEventName("FutureInflightEvent", nil)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("timed out waiting for in-flight event")
	}

	match, err := bus.FindEventName("FutureInflightEvent", nil, &abxbus.FindOptions{Past: false, Future: 0.03})
	close(release)
	if err != nil {
		t.Fatal(err)
	}
	if match != nil {
		t.Fatalf("future-only find should ignore already-dispatched in-flight events, got %#v", match)
	}
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
}

func TestFindFutureResolvesOnDispatchBeforeHandlersComplete(t *testing.T) {
	bus := abxbus.NewEventBus("FindFutureDispatchVisibilityBus", nil)
	t.Cleanup(bus.Destroy)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	bus.OnEventName("DispatchVisibleEvent", "slow", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return "ok", nil
	}, nil)

	go func() {
		time.Sleep(20 * time.Millisecond)
		bus.EmitEventName("DispatchVisibleEvent", nil)
	}()
	match, err := bus.FindEventName("DispatchVisibleEvent", nil, &abxbus.FindOptions{Past: false, Future: 1.0})
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	if match == nil {
		close(release)
		t.Fatal("future find should resolve when event is dispatched")
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("timed out waiting for matched event handler to start")
	}
	if match.EventStatus == "completed" {
		close(release)
		t.Fatalf("future find should resolve before handler completion, got status %s", match.EventStatus)
	}
	close(release)
	if _, err := match.Now(); err != nil {
		t.Fatal(err)
	}
}

func TestMultipleConcurrentFutureFindWaitersResolveCorrectEvents(t *testing.T) {
	bus := abxbus.NewEventBus("FindConcurrentWaitersBus", nil)
	t.Cleanup(bus.Destroy)
	resultA := make(chan *abxbus.BaseEvent, 1)
	resultB := make(chan *abxbus.BaseEvent, 1)
	errs := make(chan error, 2)

	go func() {
		event, err := bus.FindEventName("ConcurrentFindA", nil, &abxbus.FindOptions{Past: false, Future: 1.0})
		if err != nil {
			errs <- err
			return
		}
		resultA <- event
	}()
	go func() {
		event, err := bus.FindEventName("ConcurrentFindB", nil, &abxbus.FindOptions{Past: false, Future: 1.0})
		if err != nil {
			errs <- err
			return
		}
		resultB <- event
	}()

	time.Sleep(20 * time.Millisecond)
	eventB := bus.EmitEventName("ConcurrentFindB", nil)
	eventA := bus.EmitEventName("ConcurrentFindA", nil)

	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
	select {
	case gotA := <-resultA:
		if gotA == nil || gotA.EventID != eventA.EventID {
			t.Fatalf("waiter A resolved wrong event: got %#v want %s", gotA, eventA.EventID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for waiter A")
	}
	select {
	case gotB := <-resultB:
		if gotB == nil || gotB.EventID != eventB.EventID {
			t.Fatalf("waiter B resolved wrong event: got %#v want %s", gotB, eventB.EventID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for waiter B")
	}
	if _, err := eventA.Now(); err != nil {
		t.Fatal(err)
	}
	if _, err := eventB.Now(); err != nil {
		t.Fatal(err)
	}
}

func TestMaxHistorySizeZeroDisablesPastSearchButFutureFindStillResolves(t *testing.T) {
	zeroHistorySize := 0
	bus := abxbus.NewEventBus("FindZeroHistoryBus", &abxbus.EventBusOptions{MaxHistorySize: &zeroHistorySize})
	bus.OnEventName("ZeroHistoryEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok:" + event.Payload["value"].(string), nil
	}, nil)

	first := bus.EmitEventName("ZeroHistoryEvent", map[string]any{"value": "first"})
	if _, err := first.Now(); err != nil {
		t.Fatal(err)
	}
	if bus.EventHistory.Size() != 0 {
		t.Fatalf("zero history should drop completed event, got size=%d", bus.EventHistory.Size())
	}
	past, err := bus.FindEventName("ZeroHistoryEvent", nil, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if past != nil {
		t.Fatalf("past find should not see completed event in zero history, got %#v", past)
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		bus.EmitEventName("ZeroHistoryEvent", map[string]any{"value": "future"})
	}()
	future, err := bus.FindEventName("ZeroHistoryEvent", func(event *abxbus.BaseEvent) bool {
		return event.Payload["value"] == "future"
	}, &abxbus.FindOptions{Past: false, Future: 1.0})
	if err != nil {
		t.Fatal(err)
	}
	if future == nil || future.Payload["value"] != "future" {
		t.Fatalf("future find should resolve before zero history pruning, got %#v", future)
	}
	if _, err := future.Now(); err != nil {
		t.Fatal(err)
	}
	if bus.EventHistory.Size() != 0 {
		t.Fatalf("zero history should stay empty after future match completion, got size=%d", bus.EventHistory.Size())
	}
}

func TestFindReturnsFirstFilterResult(t *testing.T) {
	bus := abxbus.NewEventBus("FindFilterFirstBus", nil)
	first := bus.EmitEventName("ParentEvent", nil)
	second := bus.EmitEventName("ParentEvent", nil)
	if _, err := first.Now(); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Now(); err != nil {
		t.Fatal(err)
	}

	found, err := bus.FindEventName("ParentEvent", nil, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	limit := 1
	filtered, err := bus.FilterEventName("ParentEvent", nil, &abxbus.FilterOptions{Past: true, Future: false, Limit: &limit})
	if err != nil {
		t.Fatal(err)
	}
	if found == nil || len(filtered) != 1 {
		t.Fatalf("expected find and filter to return one newest match, found=%#v filtered=%#v", found, filtered)
	}
	if found.EventID != filtered[0].EventID || found.EventID != second.EventID {
		t.Fatalf("find should return first filter result/newest event, found=%s filtered=%s newest=%s", found.EventID, filtered[0].EventID, second.EventID)
	}
}

func TestFindSupportsPayloadFieldNamedLimitViaEquals(t *testing.T) {
	bus := abxbus.NewEventBus("FindLimitFieldBus", nil)
	noMatch := bus.EmitEventName("LimitFieldEvent", map[string]any{"limit": 3})
	target := bus.EmitEventName("LimitFieldEvent", map[string]any{"limit": 5})
	if _, err := noMatch.Now(); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Now(); err != nil {
		t.Fatal(err)
	}

	match, err := bus.FindEventName("LimitFieldEvent", nil, &abxbus.FindOptions{
		Past:   true,
		Future: false,
		Equals: map[string]any{"limit": 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	if match == nil || match.EventID != target.EventID {
		t.Fatalf("expected payload field named limit to match target event, got %#v", match)
	}
	if match.EventID == noMatch.EventID {
		t.Fatal("find matched the wrong limit payload")
	}
}

func TestFilterLimitZeroAndNegativeReturnImmediatelyWithoutFutureWait(t *testing.T) {
	bus := abxbus.NewEventBus("FilterLimitImmediateBus", nil)
	t.Cleanup(bus.Destroy)
	for _, limit := range []int{0, -1} {
		start := time.Now()
		matches, err := bus.FilterEventName("NeverDispatched", nil, &abxbus.FilterOptions{Past: false, Future: 1.0, Limit: &limit})
		if err != nil {
			t.Fatal(err)
		}
		if len(matches) != 0 {
			t.Fatalf("limit=%d should return no matches, got %#v", limit, matches)
		}
		if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
			t.Fatalf("limit=%d should not wait for future events, elapsed=%s", limit, elapsed)
		}
	}
}

func TestFilterFutureOnlyTimesOutToEmptyList(t *testing.T) {
	bus := abxbus.NewEventBus("FilterFutureTimeoutBus", nil)
	t.Cleanup(bus.Destroy)
	start := time.Now()
	matches, err := bus.FilterEventName("MissingFutureFilterEvent", nil, &abxbus.FilterOptions{Past: false, Future: 0.03})
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if len(matches) != 0 {
		t.Fatalf("future-only filter should time out to empty list, got %#v", matches)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("future-only filter timeout took too long: %s", elapsed)
	}
}

func TestFilterReturnsEmptyArrayWhenNoMatches(t *testing.T) {
	bus := abxbus.NewEventBus("FilterEmptyBus", nil)
	matches, err := bus.FilterEventName("ParentEvent", nil, &abxbus.FilterOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("expected empty match list, got %#v", matches)
	}
}

func TestFilterReturnsPastMatchesNewestFirstAndRespectsLimit(t *testing.T) {
	bus := abxbus.NewEventBus("FilterPastBus", nil)
	first := bus.EmitEventName("Work", map[string]any{"n": 1})
	second := bus.EmitEventName("Work", map[string]any{"n": 2})
	third := bus.EmitEventName("Work", map[string]any{"n": 3})
	for _, event := range []*abxbus.BaseEvent{first, second, third} {
		if _, err := event.Now(); err != nil {
			t.Fatal(err)
		}
	}

	limit := 2
	matches, err := bus.FilterEventName("Work", nil, &abxbus.FilterOptions{Past: true, Future: false, Limit: &limit})
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 || matches[0].EventID != third.EventID || matches[1].EventID != second.EventID {
		t.Fatalf("expected two newest matches [third, second], got %#v", matches)
	}
}

func TestFilterRespectsWherePredicateNewestFirst(t *testing.T) {
	bus := abxbus.NewEventBus("FilterWhereBus", nil)
	first := bus.EmitEventName("ScreenshotEvent", map[string]any{"target_id": "same"})
	other := bus.EmitEventName("ScreenshotEvent", map[string]any{"target_id": "other"})
	second := bus.EmitEventName("ScreenshotEvent", map[string]any{"target_id": "same"})
	for _, event := range []*abxbus.BaseEvent{first, other, second} {
		if _, err := event.Now(); err != nil {
			t.Fatal(err)
		}
	}

	matches, err := bus.FilterEventName("ScreenshotEvent", func(event *abxbus.BaseEvent) bool {
		return event.Payload["target_id"] == "same"
	}, &abxbus.FilterOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 || matches[0].EventID != second.EventID || matches[1].EventID != first.EventID {
		t.Fatalf("expected where-filtered newest-first matches [second, first], got %#v", matches)
	}
}

func TestFilterWildcardMatchesAllEventTypesNewestFirst(t *testing.T) {
	bus := abxbus.NewEventBus("FilterWildcardBus", nil)
	userEvent := bus.EmitEventName("UserActionEvent", map[string]any{"action": "login"})
	systemEvent := bus.EmitEventName("SystemEvent", nil)
	if _, err := userEvent.Now(); err != nil {
		t.Fatal(err)
	}
	if _, err := systemEvent.Now(); err != nil {
		t.Fatal(err)
	}

	matches, err := bus.FilterEventName("*", nil, &abxbus.FilterOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) < 2 || matches[0].EventID != systemEvent.EventID || matches[1].EventID != userEvent.EventID {
		t.Fatalf("expected wildcard newest-first matches [system, user], got %#v", matches)
	}
}

func TestFilterPastWindowFiltersByAge(t *testing.T) {
	bus := abxbus.NewEventBus("FilterPastWindowBus", nil)
	oldEvent := bus.EmitEventName("ParentEvent", nil)
	if _, err := oldEvent.Now(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)
	newEvent := bus.EmitEventName("ParentEvent", nil)
	if _, err := newEvent.Now(); err != nil {
		t.Fatal(err)
	}

	matches, err := bus.FilterEventName("ParentEvent", nil, &abxbus.FilterOptions{Past: 0.1, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 || matches[0].EventID != newEvent.EventID {
		t.Fatalf("expected only recent event inside past window, got %#v", matches)
	}
}

func TestFilterFutureAppendsMatchAfterPastResults(t *testing.T) {
	bus := abxbus.NewEventBus("FilterFutureAppendBus", nil)
	pastEvent := bus.EmitEventName("ParentEvent", nil)
	if _, err := pastEvent.Now(); err != nil {
		t.Fatal(err)
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		bus.EmitEventName("ParentEvent", nil)
	}()
	matches, err := bus.FilterEventName("ParentEvent", nil, &abxbus.FilterOptions{Past: true, Future: 0.5})
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 {
		t.Fatalf("expected past match plus future match, got %#v", matches)
	}
	if matches[0].EventID != pastEvent.EventID {
		t.Fatalf("future match should be appended after past results, got %#v", matches)
	}
}

func TestFilterSupportsWhereEqualsWildcardChildAndFuture(t *testing.T) {
	bus := abxbus.NewEventBus("FilterOptionsBus", nil)
	parent := bus.EmitEventName("Parent", nil)
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}
	child := abxbus.NewBaseEvent("Child", map[string]any{"kind": "target"})
	child.EventParentID = &parent.EventID
	child = bus.Emit(child)
	if _, err := child.Now(); err != nil {
		t.Fatal(err)
	}
	bus.EmitEventName("Other", map[string]any{"kind": "target"})

	childMatches, err := bus.FilterEventName("*", func(event *abxbus.BaseEvent) bool {
		return event.Payload["kind"] == "target"
	}, &abxbus.FilterOptions{Past: true, Future: false, ChildOf: parent, Equals: map[string]any{"kind": "target"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(childMatches) != 1 || childMatches[0].EventID != child.EventID {
		t.Fatalf("expected child match only, got %#v", childMatches)
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		bus.EmitEventName("FutureWork", map[string]any{"kind": "future"})
	}()
	futureMatches, err := bus.FilterEventName("FutureWork", nil, &abxbus.FilterOptions{
		Past:   false,
		Future: 1.0,
		Equals: map[string]any{"kind": "future"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(futureMatches) != 1 || futureMatches[0].EventType != "FutureWork" {
		t.Fatalf("expected one future match, got %#v", futureMatches)
	}

	none, err := bus.FilterEventName("Missing", nil, &abxbus.FilterOptions{Past: false, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(none) != 0 {
		t.Fatalf("expected no matches when past=false and future=false, got %#v", none)
	}
}

func TestFilterSupportsMetadataEqualityAndFutureLimitShortCircuit(t *testing.T) {
	bus := abxbus.NewEventBus("FilterEventFieldBus", nil)
	eventA := abxbus.NewBaseEvent("NumberedEvent", map[string]any{"value": 1})
	timeoutA := 11.0
	eventA.EventTimeout = &timeoutA
	eventB := abxbus.NewBaseEvent("NumberedEvent", map[string]any{"value": 2})
	timeoutB := 22.0
	eventB.EventTimeout = &timeoutB
	for _, event := range []*abxbus.BaseEvent{bus.Emit(eventA), bus.Emit(eventB)} {
		if _, err := event.Now(); err != nil {
			t.Fatal(err)
		}
	}

	matches, err := bus.FilterEventName("NumberedEvent", nil, &abxbus.FilterOptions{
		Past:   true,
		Future: false,
		Equals: map[string]any{
			"event_timeout": 22,
			"value":         2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 || matches[0].EventID != eventB.EventID {
		t.Fatalf("expected metadata and payload filters to match event B, got %#v", matches)
	}

	limit := 1
	start := time.Now()
	limited, err := bus.FilterEventName("NumberedEvent", nil, &abxbus.FilterOptions{Past: true, Future: 2.0, Limit: &limit})
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 1 || limited[0].EventID != eventB.EventID {
		t.Fatalf("expected newest event from limit short-circuit, got %#v", limited)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("filter should short-circuit future wait after hitting limit, elapsed=%s", elapsed)
	}
}

func TestEventHistoryFindCoversFiltersAndEdgeCases(t *testing.T) {
	maxHistorySize := 100
	h := abxbus.NewEventHistory(&maxHistorySize, false)
	parent := abxbus.NewBaseEvent("ParentEvent", map[string]any{"kind": "parent"})
	parent.EventStatus = "completed"
	child := abxbus.NewBaseEvent("ChildEvent", map[string]any{"kind": "child", "k": "v"})
	child.EventParentID = &parent.EventID
	child.EventStatus = "completed"
	child.EventCreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	oldChild := abxbus.NewBaseEvent("ChildEvent", map[string]any{"kind": "child", "k": "old"})
	oldChild.EventParentID = &parent.EventID
	oldChild.EventStatus = "completed"
	oldChild.EventCreatedAt = time.Now().Add(-3 * time.Second).UTC().Format(time.RFC3339Nano)
	other := abxbus.NewBaseEvent("OtherEvent", map[string]any{"kind": "other"})
	other.EventStatus = "started"

	h.AddEvent(parent)
	h.AddEvent(oldChild)
	h.AddEvent(child)
	h.AddEvent(other)

	foundChild := h.Find("ChildEvent", nil, &abxbus.EventHistoryFindOptions{Past: true, ChildOf: parent})
	if foundChild == nil || foundChild.EventID != child.EventID {
		t.Fatalf("expected newest matching child, got %#v", foundChild)
	}

	recentChild := h.Find("ChildEvent", nil, &abxbus.EventHistoryFindOptions{Past: 1.0, ChildOf: parent, Equals: map[string]any{"k": "v"}})
	if recentChild == nil || recentChild.EventID != child.EventID {
		t.Fatalf("expected recent child match with equals filter, got %#v", recentChild)
	}

	whereMatch := h.Find("*", func(event *abxbus.BaseEvent) bool {
		return event.Payload["kind"] == "other" && event.EventStatus == "started"
	}, &abxbus.EventHistoryFindOptions{Past: true})
	if whereMatch == nil || whereMatch.EventID != other.EventID {
		t.Fatalf("expected wildcard+where filter to find other event, got %#v", whereMatch)
	}

	eventTypeMatch := h.Find("*", nil, &abxbus.EventHistoryFindOptions{Past: true, Equals: map[string]any{"event_type": "ParentEvent", "event_status": "completed"}})
	if eventTypeMatch == nil || eventTypeMatch.EventID != parent.EventID {
		t.Fatalf("expected event_type/event_status equals match, got %#v", eventTypeMatch)
	}

	notFound := h.Find("ChildEvent", nil, &abxbus.EventHistoryFindOptions{Past: false})
	if notFound != nil {
		t.Fatalf("expected nil when past=false, got %#v", notFound)
	}
}
