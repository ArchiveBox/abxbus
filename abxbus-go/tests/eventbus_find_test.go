package abxbus_test

import (
	"context"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestFindHistoryAndFuture(t *testing.T) {
	bus := abxbus.NewEventBus("FindBus", nil)
	seed := bus.Emit(abxbus.NewBaseEvent("ResponseEvent", map[string]any{"request_id": "abc"}))
	if _, err := seed.Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	match, err := bus.Find("ResponseEvent", func(e *abxbus.BaseEvent) bool {
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
		bus.Emit(abxbus.NewBaseEvent("FutureEvent", map[string]any{"request_id": "future"}))
	}()
	future, err := bus.Find("FutureEvent", nil, &abxbus.FindOptions{Past: false, Future: 1.0})
	if err != nil {
		t.Fatal(err)
	}
	if future == nil || future.EventType != "FutureEvent" {
		t.Fatalf("expected future find to resolve FutureEvent, got %#v", future)
	}
}

func TestFindReturnsNilWhenNoMatch(t *testing.T) {
	bus := abxbus.NewEventBus("FindNilBus", nil)
	match, err := bus.Find("MissingEvent", nil, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if match != nil {
		t.Fatalf("expected nil when no event matches, got %#v", match)
	}
}

func TestFindDefaultPastOnlyNoFutureWait(t *testing.T) {
	bus := abxbus.NewEventBus("FindDefaultBus", nil)
	seed := bus.Emit(abxbus.NewBaseEvent("DefaultEvent", nil))
	if _, err := seed.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	match, err := bus.Find("DefaultEvent", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if match == nil || match.EventID != seed.EventID {
		t.Fatalf("expected default find to return past match")
	}
}

func TestFindPastWindowAndEqualsFiltering(t *testing.T) {
	bus := abxbus.NewEventBus("FindWindowBus", nil)

	oldEvent := abxbus.NewBaseEvent("WindowEvent", map[string]any{"request_id": "old"})
	oldEvent.EventCreatedAt = time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339Nano)
	if _, err := bus.Emit(oldEvent).Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	newEvent := abxbus.NewBaseEvent("WindowEvent", map[string]any{"request_id": "new"})
	if _, err := bus.Emit(newEvent).Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	recent, err := bus.Find("WindowEvent", nil, &abxbus.FindOptions{Past: 0.5, Future: false, Equals: map[string]any{"event_type": "WindowEvent", "event_status": "completed"}})
	if err != nil {
		t.Fatal(err)
	}
	if recent == nil || recent.EventID != newEvent.EventID {
		t.Fatalf("expected past-window filter to return recent event, got %#v", recent)
	}

	equalsMatch, err := bus.Find("WindowEvent", nil, &abxbus.FindOptions{Past: true, Future: false, Equals: map[string]any{"request_id": "new"}})
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
		if _, err := event.Done(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	foundA, err := bus.Find("FieldFilterEvent", nil, &abxbus.FindOptions{
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

	mismatch, err := bus.Find("FieldFilterEvent", nil, &abxbus.FindOptions{
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

	foundPayload, err := bus.Find("FieldFilterEvent", nil, &abxbus.FindOptions{
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
	matchA := busA.Emit(abxbus.NewBaseEvent("ScopedEvent", map[string]any{"source": "A", "value": 1}))
	matchB := busB.Emit(abxbus.NewBaseEvent("ScopedEvent", map[string]any{"source": "B", "value": 2}))
	if _, err := matchA.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := matchB.Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	foundA, err := busA.Find("ScopedEvent", func(event *abxbus.BaseEvent) bool {
		return event.Payload["source"] == "A" && event.Payload["value"] == 1
	}, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if foundA == nil || foundA.EventID != matchA.EventID {
		t.Fatalf("expected bus A to find only its own event, got %#v", foundA)
	}

	foundB, err := busB.Find("ScopedEvent", func(event *abxbus.BaseEvent) bool {
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

	parent := bus.Emit(abxbus.NewBaseEvent("Parent", nil))
	if _, err := parent.Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	child := abxbus.NewBaseEvent("Child", nil)
	child.EventParentID = &parent.EventID
	child = bus.Emit(child)
	if _, err := child.Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	grandchild := abxbus.NewBaseEvent("Grandchild", nil)
	grandchild.EventParentID = &child.EventID
	grandchild = bus.Emit(grandchild)
	if _, err := grandchild.Done(context.Background()); err != nil {
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

	found, err := bus.Find("Grandchild", nil, &abxbus.FindOptions{Past: true, Future: false, ChildOf: parent})
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
	bus.On("SlowFindEvent", "slow", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		started <- struct{}{}
		<-release
		return "ok", nil
	}, nil)

	e := bus.Emit(abxbus.NewBaseEvent("SlowFindEvent", nil))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("timed out waiting for slow handler start")
	}

	match, err := bus.Find("SlowFindEvent", nil, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	if match == nil || match.EventID != e.EventID {
		close(release)
		t.Fatalf("expected in-progress event to be discoverable in history, got %#v", match)
	}

	close(release)
	if _, err := e.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFindFutureIgnoresAlreadyDispatchedInFlightEventsWhenPastFalse(t *testing.T) {
	bus := abxbus.NewEventBus("FindFutureIgnoresInflightBus", nil)
	t.Cleanup(bus.Destroy)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	bus.On("FutureInflightEvent", "slow", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return "ok", nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("FutureInflightEvent", nil))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("timed out waiting for in-flight event")
	}

	match, err := bus.Find("FutureInflightEvent", nil, &abxbus.FindOptions{Past: false, Future: 0.03})
	close(release)
	if err != nil {
		t.Fatal(err)
	}
	if match != nil {
		t.Fatalf("future-only find should ignore already-dispatched in-flight events, got %#v", match)
	}
	if _, err := event.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFindFutureResolvesOnDispatchBeforeHandlersComplete(t *testing.T) {
	bus := abxbus.NewEventBus("FindFutureDispatchVisibilityBus", nil)
	t.Cleanup(bus.Destroy)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	bus.On("DispatchVisibleEvent", "slow", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
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
		bus.Emit(abxbus.NewBaseEvent("DispatchVisibleEvent", nil))
	}()
	match, err := bus.Find("DispatchVisibleEvent", nil, &abxbus.FindOptions{Past: false, Future: 1.0})
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
	if _, err := match.Done(context.Background()); err != nil {
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
		event, err := bus.Find("ConcurrentFindA", nil, &abxbus.FindOptions{Past: false, Future: 1.0})
		if err != nil {
			errs <- err
			return
		}
		resultA <- event
	}()
	go func() {
		event, err := bus.Find("ConcurrentFindB", nil, &abxbus.FindOptions{Past: false, Future: 1.0})
		if err != nil {
			errs <- err
			return
		}
		resultB <- event
	}()

	time.Sleep(20 * time.Millisecond)
	eventB := bus.Emit(abxbus.NewBaseEvent("ConcurrentFindB", nil))
	eventA := bus.Emit(abxbus.NewBaseEvent("ConcurrentFindA", nil))

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
	if _, err := eventA.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := eventB.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMaxHistorySizeZeroDisablesPastSearchButFutureFindStillResolves(t *testing.T) {
	zeroHistorySize := 0
	bus := abxbus.NewEventBus("FindZeroHistoryBus", &abxbus.EventBusOptions{MaxHistorySize: &zeroHistorySize})
	bus.On("ZeroHistoryEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "ok:" + event.Payload["value"].(string), nil
	}, nil)

	first := bus.Emit(abxbus.NewBaseEvent("ZeroHistoryEvent", map[string]any{"value": "first"}))
	if _, err := first.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if bus.EventHistory.Size() != 0 {
		t.Fatalf("zero history should drop completed event, got size=%d", bus.EventHistory.Size())
	}
	past, err := bus.Find("ZeroHistoryEvent", nil, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if past != nil {
		t.Fatalf("past find should not see completed event in zero history, got %#v", past)
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		bus.Emit(abxbus.NewBaseEvent("ZeroHistoryEvent", map[string]any{"value": "future"}))
	}()
	future, err := bus.Find("ZeroHistoryEvent", func(event *abxbus.BaseEvent) bool {
		return event.Payload["value"] == "future"
	}, &abxbus.FindOptions{Past: false, Future: 1.0})
	if err != nil {
		t.Fatal(err)
	}
	if future == nil || future.Payload["value"] != "future" {
		t.Fatalf("future find should resolve before zero history pruning, got %#v", future)
	}
	if _, err := future.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if bus.EventHistory.Size() != 0 {
		t.Fatalf("zero history should stay empty after future match completion, got size=%d", bus.EventHistory.Size())
	}
}

func TestFilterLimitZeroAndNegativeReturnImmediatelyWithoutFutureWait(t *testing.T) {
	bus := abxbus.NewEventBus("FilterLimitImmediateBus", nil)
	t.Cleanup(bus.Destroy)
	for _, limit := range []int{0, -1} {
		start := time.Now()
		matches, err := bus.Filter("NeverDispatched", nil, &abxbus.FilterOptions{Past: false, Future: 1.0, Limit: &limit})
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
	matches, err := bus.Filter("MissingFutureFilterEvent", nil, &abxbus.FilterOptions{Past: false, Future: 0.03})
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

func TestFilterReturnsPastMatchesNewestFirstAndRespectsLimit(t *testing.T) {
	bus := abxbus.NewEventBus("FilterPastBus", nil)
	first := bus.Emit(abxbus.NewBaseEvent("Work", map[string]any{"n": 1}))
	second := bus.Emit(abxbus.NewBaseEvent("Work", map[string]any{"n": 2}))
	third := bus.Emit(abxbus.NewBaseEvent("Work", map[string]any{"n": 3}))
	for _, event := range []*abxbus.BaseEvent{first, second, third} {
		if _, err := event.Done(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	limit := 2
	matches, err := bus.Filter("Work", nil, &abxbus.FilterOptions{Past: true, Future: false, Limit: &limit})
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 || matches[0].EventID != third.EventID || matches[1].EventID != second.EventID {
		t.Fatalf("expected two newest matches [third, second], got %#v", matches)
	}
}

func TestFilterSupportsWhereEqualsWildcardChildAndFuture(t *testing.T) {
	bus := abxbus.NewEventBus("FilterOptionsBus", nil)
	parent := bus.Emit(abxbus.NewBaseEvent("Parent", nil))
	if _, err := parent.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	child := abxbus.NewBaseEvent("Child", map[string]any{"kind": "target"})
	child.EventParentID = &parent.EventID
	child = bus.Emit(child)
	if _, err := child.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	bus.Emit(abxbus.NewBaseEvent("Other", map[string]any{"kind": "target"}))

	childMatches, err := bus.Filter("*", func(event *abxbus.BaseEvent) bool {
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
		bus.Emit(abxbus.NewBaseEvent("FutureWork", map[string]any{"kind": "future"}))
	}()
	futureMatches, err := bus.Filter("FutureWork", nil, &abxbus.FilterOptions{
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

	none, err := bus.Filter("Missing", nil, &abxbus.FilterOptions{Past: false, Future: false})
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
		if _, err := event.Done(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	matches, err := bus.Filter("NumberedEvent", nil, &abxbus.FilterOptions{
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
	limited, err := bus.Filter("NumberedEvent", nil, &abxbus.FilterOptions{Past: true, Future: 2.0, Limit: &limit})
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
