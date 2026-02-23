package bubus_test

import (
	"context"
	"testing"
	"time"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestFindHistoryAndFuture(t *testing.T) {
	bus := bubus.NewEventBus("FindBus", nil)
	seed := bus.Emit(bubus.NewBaseEvent("ResponseEvent", map[string]any{"request_id": "abc"}))
	if _, err := seed.Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	match, err := bus.Find("ResponseEvent", func(e *bubus.BaseEvent) bool {
		return e.Payload["request_id"] == "abc"
	}, &bubus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if match == nil || match.EventID != seed.EventID {
		t.Fatal("expected history find to match seeded event")
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		bus.Emit(bubus.NewBaseEvent("FutureEvent", map[string]any{"request_id": "future"}))
	}()
	future, err := bus.Find("FutureEvent", nil, &bubus.FindOptions{Past: false, Future: 1.0})
	if err != nil {
		t.Fatal(err)
	}
	if future == nil || future.EventType != "FutureEvent" {
		t.Fatalf("expected future find to resolve FutureEvent, got %#v", future)
	}
}

func TestFindReturnsNilWhenNoMatch(t *testing.T) {
	bus := bubus.NewEventBus("FindNilBus", nil)
	match, err := bus.Find("MissingEvent", nil, &bubus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if match != nil {
		t.Fatalf("expected nil when no event matches, got %#v", match)
	}
}

func TestFindDefaultPastOnlyNoFutureWait(t *testing.T) {
	bus := bubus.NewEventBus("FindDefaultBus", nil)
	seed := bus.Emit(bubus.NewBaseEvent("DefaultEvent", nil))
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
	bus := bubus.NewEventBus("FindWindowBus", nil)

	oldEvent := bubus.NewBaseEvent("WindowEvent", map[string]any{"request_id": "old"})
	oldEvent.EventCreatedAt = time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339Nano)
	if _, err := bus.Emit(oldEvent).Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	newEvent := bubus.NewBaseEvent("WindowEvent", map[string]any{"request_id": "new"})
	if _, err := bus.Emit(newEvent).Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	recent, err := bus.Find("WindowEvent", nil, &bubus.FindOptions{Past: 0.5, Future: false, Equals: map[string]any{"event_type": "WindowEvent", "event_status": "completed"}})
	if err != nil {
		t.Fatal(err)
	}
	if recent == nil || recent.EventID != newEvent.EventID {
		t.Fatalf("expected past-window filter to return recent event, got %#v", recent)
	}

	equalsMatch, err := bus.Find("WindowEvent", nil, &bubus.FindOptions{Past: true, Future: false, Equals: map[string]any{"request_id": "new"}})
	if err != nil {
		t.Fatal(err)
	}
	if equalsMatch == nil || equalsMatch.EventID != newEvent.EventID {
		t.Fatalf("expected equals filter to match payload value, got %#v", equalsMatch)
	}
}

func TestFindWherePredicateAndBusScopedHistory(t *testing.T) {
	busA := bubus.NewEventBus("FindBusA", nil)
	busB := bubus.NewEventBus("FindBusB", nil)
	matchA := busA.Emit(bubus.NewBaseEvent("ScopedEvent", map[string]any{"source": "A", "value": 1}))
	matchB := busB.Emit(bubus.NewBaseEvent("ScopedEvent", map[string]any{"source": "B", "value": 2}))
	if _, err := matchA.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := matchB.Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	foundA, err := busA.Find("ScopedEvent", func(event *bubus.BaseEvent) bool {
		return event.Payload["source"] == "A" && event.Payload["value"] == 1
	}, &bubus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if foundA == nil || foundA.EventID != matchA.EventID {
		t.Fatalf("expected bus A to find only its own event, got %#v", foundA)
	}

	foundB, err := busB.Find("ScopedEvent", func(event *bubus.BaseEvent) bool {
		return event.Payload["source"] == "B"
	}, &bubus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if foundB == nil || foundB.EventID != matchB.EventID {
		t.Fatalf("expected bus B to find only its own event, got %#v", foundB)
	}
}

func TestFindChildOfFilteringAndLineageTraversal(t *testing.T) {
	bus := bubus.NewEventBus("FindChildBus", nil)

	parent := bus.Emit(bubus.NewBaseEvent("Parent", nil))
	if _, err := parent.Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	child := bubus.NewBaseEvent("Child", nil)
	child.EventParentID = &parent.EventID
	child = bus.Emit(child)
	if _, err := child.Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	grandchild := bubus.NewBaseEvent("Grandchild", nil)
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

	found, err := bus.Find("Grandchild", nil, &bubus.FindOptions{Past: true, Future: false, ChildOf: parent})
	if err != nil {
		t.Fatal(err)
	}
	if found == nil || found.EventType != "Grandchild" || found.EventParentID == nil || *found.EventParentID != child.EventID {
		t.Fatalf("expected child_of filter to return true descendant, got %#v", found)
	}
}

func TestFindCanSeeInProgressEventInHistory(t *testing.T) {
	bus := bubus.NewEventBus("FindInProgressBus", nil)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	bus.On("SlowFindEvent", "slow", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		started <- struct{}{}
		<-release
		return "ok", nil
	}, nil)

	e := bus.Emit(bubus.NewBaseEvent("SlowFindEvent", nil))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("timed out waiting for slow handler start")
	}

	match, err := bus.Find("SlowFindEvent", nil, &bubus.FindOptions{Past: true, Future: false})
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
