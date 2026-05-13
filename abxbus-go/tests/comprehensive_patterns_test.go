package abxbus_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"
)

func appendLocked(mu *sync.Mutex, entries *[]string, entry string) {
	mu.Lock()
	defer mu.Unlock()
	*entries = append(*entries, entry)
}

func snapshotLocked(mu *sync.Mutex, entries *[]string) []string {
	mu.Lock()
	defer mu.Unlock()
	return append([]string{}, (*entries)...)
}

func indexOf(entries []string, needle string) int {
	for i, entry := range entries {
		if entry == needle {
			return i
		}
	}
	return -1
}

func requireIndex(t *testing.T, entries []string, needle string) int {
	t.Helper()
	idx := indexOf(entries, needle)
	if idx < 0 {
		t.Fatalf("missing %q in execution order: %v", needle, entries)
	}
	return idx
}

func countEntries(entries []string, needle string) int {
	count := 0
	for _, entry := range entries {
		if entry == needle {
			count++
		}
	}
	return count
}

func waitForEntry(t *testing.T, mu *sync.Mutex, entries *[]string, needle string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if indexOf(snapshotLocked(mu, entries), needle) >= 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q; got %v", needle, snapshotLocked(mu, entries))
}

func assertWaitIdle(t *testing.T, bus *abxbus.EventBus) {
	t.Helper()
	timeout := 2.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatalf("%s did not become idle", bus.Name)
	}
}

func TestComprehensivePatternsForwardingDispatchAndParentTracking(t *testing.T) {
	bus1 := abxbus.NewEventBus("bus1", nil)
	bus2 := abxbus.NewEventBus("bus2", nil)

	var mu sync.Mutex
	results := []string{}
	sequence := 0
	next := func(label string) {
		mu.Lock()
		defer mu.Unlock()
		sequence++
		results = append(results, fmt.Sprintf("%04d:%s", sequence, label))
	}

	bus2.OnEventName("*", "child_bus2_event_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		eventTypeShort := strings.TrimSuffix(e.EventType, "Event")
		next("bus2_handler_" + eventTypeShort)
		return "forwarded bus result", nil
	}, nil)
	bus1.OnEventName("*", "emit", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return bus2.Emit(e), nil
	}, nil)

	var asyncChild *abxbus.BaseEvent
	var syncChild *abxbus.BaseEvent
	bus1.OnEventName("ParentEvent", "parent_bus1_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		next("parent_start")

		asyncChild = e.EmitEventName("QueuedChildEvent", nil)
		if asyncChild.EventStatus == "completed" {
			t.Fatalf("unawaited child should not be completed inside parent handler")
		}

		syncChild = e.EmitEventName("ImmediateChildEvent", nil)
		if _, err := syncChild.Now(); err != nil {
			return nil, err
		}
		if syncChild.EventStatus != "completed" {
			t.Fatalf("awaited child should be completed inside parent handler, got %s", syncChild.EventStatus)
		}
		if syncChild.EventParentID == nil || *syncChild.EventParentID != e.EventID {
			t.Fatalf("awaited child parent mismatch")
		}
		if asyncChild.EventParentID == nil || *asyncChild.EventParentID != e.EventID {
			t.Fatalf("unawaited child parent mismatch")
		}

		next("parent_end")
		return "parent_done", nil
	}, nil)

	parent := bus1.EmitEventName("ParentEvent", nil)
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}
	assertWaitIdle(t, bus1)
	assertWaitIdle(t, bus2)

	if asyncChild == nil || syncChild == nil {
		t.Fatalf("expected both child events to be emitted")
	}
	if syncChild.EventParentID == nil || *syncChild.EventParentID != parent.EventID {
		t.Fatalf("awaited child parent mismatch after completion")
	}
	if asyncChild.EventParentID == nil || *asyncChild.EventParentID != parent.EventID {
		t.Fatalf("unawaited child parent mismatch after completion")
	}
	if len(parent.EventResults) == 0 {
		t.Fatalf("expected parent handler results")
	}
	for _, result := range parent.EventResults {
		if result.Status == abxbus.EventResultError {
			t.Fatalf("parent handler errored: %#v", result.Error)
		}
	}

	orderWithSeq := snapshotLocked(&mu, &results)
	order := make([]string, 0, len(orderWithSeq))
	for _, entry := range orderWithSeq {
		order = append(order, strings.SplitN(entry, ":", 2)[1])
	}
	if order[0] != "parent_start" {
		t.Fatalf("expected parent to start first, got %v", order)
	}
	if countEntries(order, "bus2_handler_ImmediateChild") != 1 {
		t.Fatalf("expected one forwarded immediate child handler, got %v", order)
	}
	if countEntries(order, "bus2_handler_QueuedChild") != 1 {
		t.Fatalf("expected one forwarded queued child handler, got %v", order)
	}
	if countEntries(order, "bus2_handler_Parent") != 1 {
		t.Fatalf("expected one forwarded parent handler, got %v", order)
	}
	if parentEnd := indexOf(order, "parent_end"); parentEnd >= 0 && parentEnd <= 1 {
		t.Fatalf("parent_end should happen after child work starts, got %v", order)
	}
}

func TestComprehensiveRaceConditionStress(t *testing.T) {
	bus1 := abxbus.NewEventBus("RaceBus1", nil)
	bus2 := abxbus.NewEventBus("RaceBus2", nil)

	var mu sync.Mutex
	results := []string{}

	bus1.OnEventName("*", "forward_to_bus2", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return bus2.Emit(e), nil
	}, nil)

	for _, bus := range []*abxbus.EventBus{bus1, bus2} {
		bus := bus
		for _, pattern := range []string{"QueuedChildEvent", "ImmediateChildEvent"} {
			pattern := pattern
			bus.OnEventName(pattern, pattern+"_child_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
				appendLocked(&mu, &results, "child_"+bus.Label())
				time.Sleep(time.Millisecond)
				return "child_done_" + bus.Label(), nil
			}, nil)
		}
	}

	bus1.OnEventName("RootEvent", "parent_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		children := []*abxbus.BaseEvent{}
		for i := 0; i < 3; i++ {
			children = append(children, e.EmitEventName("QueuedChildEvent", nil))
		}
		for i := 0; i < 3; i++ {
			child := e.EmitEventName("ImmediateChildEvent", nil)
			if _, err := child.Now(); err != nil {
				return nil, err
			}
			if child.EventStatus != "completed" {
				t.Fatalf("awaited child should complete, got %s", child.EventStatus)
			}
			children = append(children, child)
		}
		for _, child := range children {
			if child.EventParentID == nil || *child.EventParentID != e.EventID {
				t.Fatalf("child parent mismatch for %s", child.EventType)
			}
		}
		return "parent_done", nil
	}, nil)
	bus1.OnEventName("RootEvent", "bad_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, nil
	}, nil)

	for run := 0; run < 5; run++ {
		mu.Lock()
		results = []string{}
		mu.Unlock()

		event := bus1.EmitEventName("RootEvent", nil)
		if _, err := event.Now(); err != nil {
			t.Fatal(err)
		}
		assertWaitIdle(t, bus1)
		assertWaitIdle(t, bus2)

		snapshot := snapshotLocked(&mu, &results)
		if got := countEntries(snapshot, "child_"+bus1.Label()); got != 6 {
			t.Fatalf("run %d: expected six child handlers on bus1, got %d: %v", run, got, snapshot)
		}
		if got := countEntries(snapshot, "child_"+bus2.Label()); got != 6 {
			t.Fatalf("run %d: expected six child handlers on bus2, got %d: %v", run, got, snapshot)
		}
	}
}

func TestComprehensiveAwaitedChildJumpsQueueWithoutOvershoot(t *testing.T) {
	bus := abxbus.NewEventBus("ComprehensiveNoOvershootBus", nil)
	var mu sync.Mutex
	order := []string{}

	bus.OnEventName("Event1", "event1_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		appendLocked(&mu, &order, "Event1_start")
		child := e.EmitEventName("ChildEvent", nil)
		appendLocked(&mu, &order, "Child_dispatched")
		if _, err := child.Now(); err != nil {
			return nil, err
		}
		appendLocked(&mu, &order, "Child_await_returned")
		appendLocked(&mu, &order, "Event1_end")
		return "event1_done", nil
	}, nil)
	bus.OnEventName("Event2", "event2_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		appendLocked(&mu, &order, "Event2_start")
		appendLocked(&mu, &order, "Event2_end")
		return "event2_done", nil
	}, nil)
	bus.OnEventName("Event3", "event3_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		appendLocked(&mu, &order, "Event3_start")
		appendLocked(&mu, &order, "Event3_end")
		return "event3_done", nil
	}, nil)
	bus.OnEventName("ChildEvent", "child_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		appendLocked(&mu, &order, "Child_start")
		appendLocked(&mu, &order, "Child_end")
		return "child_done", nil
	}, nil)

	event1 := bus.EmitEventName("Event1", nil)
	event2 := bus.EmitEventName("Event2", nil)
	event3 := bus.EmitEventName("Event3", nil)

	if _, err := event1.Now(); err != nil {
		t.Fatal(err)
	}
	assertWaitIdle(t, bus)

	entries := snapshotLocked(&mu, &order)
	childStart := requireIndex(t, entries, "Child_start")
	childEnd := requireIndex(t, entries, "Child_end")
	event1End := requireIndex(t, entries, "Event1_end")
	if childStart > event1End || childEnd > event1End {
		t.Fatalf("child must complete before Event1 returns, got %v", entries)
	}
	event2Start := requireIndex(t, entries, "Event2_start")
	event3Start := requireIndex(t, entries, "Event3_start")
	if event2Start < event1End || event3Start < event1End {
		t.Fatalf("queued siblings must not overshoot awaited child, got %v", entries)
	}
	if event2Start > event3Start {
		t.Fatalf("FIFO should preserve Event2 before Event3, got %v", entries)
	}
	if event1.EventStatus != "completed" || event2.EventStatus != "completed" || event3.EventStatus != "completed" {
		t.Fatalf("expected all events completed, got %s %s %s", event1.EventStatus, event2.EventStatus, event3.EventStatus)
	}
}

func TestComprehensiveDispatchMultipleAwaitOneSkipsOthersUntilAfterHandler(t *testing.T) {
	bus := abxbus.NewEventBus("MultiDispatchBus", nil)
	var mu sync.Mutex
	order := []string{}

	bus.OnEventName("Event1", "event1_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		appendLocked(&mu, &order, "Event1_start")
		e.EmitEventName("ChildA", nil)
		appendLocked(&mu, &order, "ChildA_dispatched")
		childB := e.EmitEventName("ChildB", nil)
		appendLocked(&mu, &order, "ChildB_dispatched")
		e.EmitEventName("ChildC", nil)
		appendLocked(&mu, &order, "ChildC_dispatched")
		if _, err := childB.Now(); err != nil {
			return nil, err
		}
		appendLocked(&mu, &order, "ChildB_await_returned")
		appendLocked(&mu, &order, "Event1_end")
		return "event1_done", nil
	}, nil)
	for _, eventType := range []string{"Event2", "Event3", "ChildA", "ChildB", "ChildC"} {
		eventType := eventType
		bus.OnEventName(eventType, eventType+"_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
			appendLocked(&mu, &order, eventType+"_start")
			appendLocked(&mu, &order, eventType+"_end")
			return strings.ToLower(eventType) + "_done", nil
		}, nil)
	}

	event1 := bus.EmitEventName("Event1", nil)
	bus.EmitEventName("Event2", nil)
	bus.EmitEventName("Event3", nil)

	if _, err := event1.Now(); err != nil {
		t.Fatal(err)
	}
	assertWaitIdle(t, bus)

	entries := snapshotLocked(&mu, &order)
	childBEnd := requireIndex(t, entries, "ChildB_end")
	event1End := requireIndex(t, entries, "Event1_end")
	if childBEnd > event1End {
		t.Fatalf("awaited ChildB should complete before Event1 ends, got %v", entries)
	}
	for _, label := range []string{"ChildA_start", "ChildC_start", "Event2_start", "Event3_start"} {
		if idx := requireIndex(t, entries, label); idx < event1End {
			t.Fatalf("%s should not start before Event1 ends, got %v", label, entries)
		}
	}
	if !(requireIndex(t, entries, "Event2_start") < requireIndex(t, entries, "Event3_start") &&
		requireIndex(t, entries, "Event3_start") < requireIndex(t, entries, "ChildA_start") &&
		requireIndex(t, entries, "ChildA_start") < requireIndex(t, entries, "ChildC_start")) {
		t.Fatalf("FIFO order for remaining events was not preserved: %v", entries)
	}
}

func TestComprehensiveMultiBusQueuesIndependentWhenAwaitingChild(t *testing.T) {
	bus1 := abxbus.NewEventBus("Bus1", nil)
	bus2 := abxbus.NewEventBus("Bus2", nil)
	var mu sync.Mutex
	order := []string{}
	bus2Started := make(chan struct{})
	closeBus2Started := sync.Once{}

	bus1.OnEventName("Event1", "event1_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		appendLocked(&mu, &order, "Bus1_Event1_start")
		child := e.EmitEventName("ChildEvent", nil)
		appendLocked(&mu, &order, "Child_dispatched_to_Bus1")
		if _, err := child.Now(); err != nil {
			return nil, err
		}
		appendLocked(&mu, &order, "Child_await_returned")
		appendLocked(&mu, &order, "Bus1_Event1_end")
		return "event1_done", nil
	}, nil)
	bus1.OnEventName("Event2", "event2_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		appendLocked(&mu, &order, "Bus1_Event2_start")
		appendLocked(&mu, &order, "Bus1_Event2_end")
		return "event2_done", nil
	}, nil)
	bus1.OnEventName("ChildEvent", "child_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		appendLocked(&mu, &order, "Child_start")
		select {
		case <-bus2Started:
		case <-time.After(2 * time.Second):
			return nil, fmt.Errorf("timed out waiting for Bus2 to process independently")
		}
		appendLocked(&mu, &order, "Child_end")
		return "child_done", nil
	}, nil)
	for _, eventType := range []string{"Event3", "Event4"} {
		eventType := eventType
		bus2.OnEventName(eventType, eventType+"_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
			appendLocked(&mu, &order, "Bus2_"+eventType+"_start")
			if eventType == "Event3" {
				closeBus2Started.Do(func() { close(bus2Started) })
			}
			appendLocked(&mu, &order, "Bus2_"+eventType+"_end")
			return strings.ToLower(eventType) + "_done", nil
		}, nil)
	}

	event1 := bus1.EmitEventName("Event1", nil)
	bus1.EmitEventName("Event2", nil)
	bus2.EmitEventName("Event3", nil)
	bus2.EmitEventName("Event4", nil)

	waitForEntry(t, &mu, &order, "Bus2_Event3_start")
	if _, err := event1.Now(); err != nil {
		t.Fatal(err)
	}
	assertWaitIdle(t, bus1)
	assertWaitIdle(t, bus2)

	entries := snapshotLocked(&mu, &order)
	childEnd := requireIndex(t, entries, "Child_end")
	event1End := requireIndex(t, entries, "Bus1_Event1_end")
	if childEnd > event1End {
		t.Fatalf("child should complete before Event1 ends, got %v", entries)
	}
	if idx := requireIndex(t, entries, "Bus1_Event2_start"); idx < event1End {
		t.Fatalf("Bus1 Event2 must not overshoot Event1 handler, got %v", entries)
	}
	if requireIndex(t, entries, "Bus2_Event3_start") > event1End {
		t.Fatalf("Bus2 should process independently while Bus1 awaits child, got %v", entries)
	}
	requireIndex(t, entries, "Bus2_Event4_start")
}
