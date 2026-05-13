package abxbus_test

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestEventsForwardBetweenBusesWithoutDuplication(t *testing.T) {
	busA := abxbus.NewEventBus("BusA", nil)
	busB := abxbus.NewEventBus("BusB", nil)
	busC := abxbus.NewEventBus("BusC", nil)
	seenA := []string{}
	seenB := []string{}
	seenC := []string{}

	busA.On("PingEvent", "seen_a", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		seenA = append(seenA, event.EventID)
		return "a", nil
	}, nil)
	busB.On("PingEvent", "seen_b", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		seenB = append(seenB, event.EventID)
		return "b", nil
	}, nil)
	busC.On("PingEvent", "seen_c", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		seenC = append(seenC, event.EventID)
		return "c", nil
	}, nil)
	busA.On("*", "forward_to_b", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		busB.Emit(event)
		return nil, nil
	}, nil)
	busB.On("*", "forward_to_c", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		busC.Emit(event)
		return nil, nil
	}, nil)

	event := busA.Emit(abxbus.NewBaseEvent("PingEvent", map[string]any{"value": 1}))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	waitAllIdle(t, busA, busB, busC)

	if !reflect.DeepEqual(seenA, []string{event.EventID}) ||
		!reflect.DeepEqual(seenB, []string{event.EventID}) ||
		!reflect.DeepEqual(seenC, []string{event.EventID}) {
		t.Fatalf("event should be seen exactly once per bus, got A=%v B=%v C=%v", seenA, seenB, seenC)
	}
	expectedPath := []string{busA.Label(), busB.Label(), busC.Label()}
	if !reflect.DeepEqual(event.EventPath, expectedPath) {
		t.Fatalf("unexpected forwarding path: got %v want %v", event.EventPath, expectedPath)
	}
}

func TestForwardingDisambiguatesBusesThatShareTheSameName(t *testing.T) {
	busA := abxbus.NewEventBus("SharedName", nil)
	busB := abxbus.NewEventBus("SharedName", nil)
	seenA := []string{}
	seenB := []string{}

	busA.On("PingEvent", "seen_a", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		seenA = append(seenA, event.EventID)
		return "a", nil
	}, nil)
	busB.On("PingEvent", "seen_b", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		seenB = append(seenB, event.EventID)
		return "b", nil
	}, nil)
	busA.On("*", "forward_to_b", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		busB.Emit(event)
		return nil, nil
	}, nil)

	event := busA.Emit(abxbus.NewBaseEvent("PingEvent", map[string]any{"value": 99}))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	waitAllIdle(t, busA, busB)

	if !reflect.DeepEqual(seenA, []string{event.EventID}) || !reflect.DeepEqual(seenB, []string{event.EventID}) {
		t.Fatalf("same-name buses should each see event once, got A=%v B=%v", seenA, seenB)
	}
	if busA.Label() == busB.Label() {
		t.Fatalf("same-name buses should have distinct labels, got %q", busA.Label())
	}
	expectedPath := []string{busA.Label(), busB.Label()}
	if !reflect.DeepEqual(event.EventPath, expectedPath) {
		t.Fatalf("unexpected same-name forwarding path: got %v want %v", event.EventPath, expectedPath)
	}
}

func TestAwaitNowWaitsForHandlersOnForwardedBuses(t *testing.T) {
	busA := abxbus.NewEventBus("ForwardWaitA", nil)
	busB := abxbus.NewEventBus("ForwardWaitB", nil)
	busC := abxbus.NewEventBus("ForwardWaitC", nil)
	var mu sync.Mutex
	completionLog := []string{}
	record := func(value string) {
		mu.Lock()
		defer mu.Unlock()
		completionLog = append(completionLog, value)
	}

	busA.On("PingEvent", "a", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		time.Sleep(10 * time.Millisecond)
		record("A")
		return "a", nil
	}, nil)
	busB.On("PingEvent", "b", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		time.Sleep(30 * time.Millisecond)
		record("B")
		return "b", nil
	}, nil)
	busC.On("PingEvent", "c", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		time.Sleep(50 * time.Millisecond)
		record("C")
		return "c", nil
	}, nil)
	busA.On("*", "forward_to_b", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		busB.Emit(event)
		return nil, nil
	}, nil)
	busB.On("*", "forward_to_c", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		busC.Emit(event)
		return nil, nil
	}, nil)

	event := busA.Emit(abxbus.NewBaseEvent("PingEvent", map[string]any{"value": 2}))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	waitAllIdle(t, busA, busB, busC)

	mu.Lock()
	defer mu.Unlock()
	if len(completionLog) != 3 || !containsAll(completionLog, []string{"A", "B", "C"}) {
		t.Fatalf("event.Now should wait for all forwarded handlers, got %v", completionLog)
	}
	if event.EventPendingBusCount != 0 {
		t.Fatalf("event pending bus count should be zero, got %d", event.EventPendingBusCount)
	}
}

func TestCircularForwardingDoesNotLoop(t *testing.T) {
	peer1 := abxbus.NewEventBus("Peer1", nil)
	peer2 := abxbus.NewEventBus("Peer2", nil)
	peer3 := abxbus.NewEventBus("Peer3", nil)
	seen1 := []string{}
	seen2 := []string{}
	seen3 := []string{}

	peer1.On("PingEvent", "seen_1", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		seen1 = append(seen1, event.EventID)
		return "p1", nil
	}, nil)
	peer2.On("PingEvent", "seen_2", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		seen2 = append(seen2, event.EventID)
		return "p2", nil
	}, nil)
	peer3.On("PingEvent", "seen_3", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		seen3 = append(seen3, event.EventID)
		return "p3", nil
	}, nil)
	peer1.On("*", "forward_to_2", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		peer2.Emit(event)
		return nil, nil
	}, nil)
	peer2.On("*", "forward_to_3", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		peer3.Emit(event)
		return nil, nil
	}, nil)
	peer3.On("*", "forward_to_1", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		peer1.Emit(event)
		return nil, nil
	}, nil)

	event := peer1.Emit(abxbus.NewBaseEvent("PingEvent", map[string]any{"value": 42}))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	waitAllIdle(t, peer1, peer2, peer3)

	if !reflect.DeepEqual(seen1, []string{event.EventID}) ||
		!reflect.DeepEqual(seen2, []string{event.EventID}) ||
		!reflect.DeepEqual(seen3, []string{event.EventID}) {
		t.Fatalf("cycle should see first event once per peer, got p1=%v p2=%v p3=%v", seen1, seen2, seen3)
	}
	expectedPath := []string{peer1.Label(), peer2.Label(), peer3.Label()}
	if !reflect.DeepEqual(event.EventPath, expectedPath) {
		t.Fatalf("unexpected cycle path from peer1: got %v want %v", event.EventPath, expectedPath)
	}

	seen1, seen2, seen3 = []string{}, []string{}, []string{}
	event2 := peer2.Emit(abxbus.NewBaseEvent("PingEvent", map[string]any{"value": 99}))
	if _, err := event2.Now(); err != nil {
		t.Fatal(err)
	}
	waitAllIdle(t, peer1, peer2, peer3)

	if !reflect.DeepEqual(seen1, []string{event2.EventID}) ||
		!reflect.DeepEqual(seen2, []string{event2.EventID}) ||
		!reflect.DeepEqual(seen3, []string{event2.EventID}) {
		t.Fatalf("cycle should see second event once per peer, got p1=%v p2=%v p3=%v", seen1, seen2, seen3)
	}
	expectedPath = []string{peer2.Label(), peer3.Label(), peer1.Label()}
	if !reflect.DeepEqual(event2.EventPath, expectedPath) {
		t.Fatalf("unexpected cycle path from peer2: got %v want %v", event2.EventPath, expectedPath)
	}
}

func TestForwardingDoesNotSetSelfParentOnSameEvent(t *testing.T) {
	origin := abxbus.NewEventBus("Origin", nil)
	target := abxbus.NewEventBus("Target", nil)
	origin.On("*", "forward", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) { return target.Emit(e), nil }, nil)

	e := origin.Emit(abxbus.NewBaseEvent("SelfParentForwardEvent", nil))
	if _, err := e.Now(); err != nil {
		t.Fatal(err)
	}
	if e.EventParentID != nil {
		t.Fatalf("expected nil parent for forwarded same event, got %v", *e.EventParentID)
	}
	if len(e.EventPath) != 2 {
		t.Fatalf("expected both buses in event_path, got %v", e.EventPath)
	}
}

func TestForwardedEventUsesProcessingBusDefaults(t *testing.T) {
	busA := abxbus.NewEventBus("ForwardDefaultsA", &abxbus.EventBusOptions{EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial})
	busB := abxbus.NewEventBus("ForwardDefaultsB", &abxbus.EventBusOptions{EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel})

	var mu sync.Mutex
	entries := []string{}
	appendEntry := func(v string) {
		mu.Lock()
		defer mu.Unlock()
		entries = append(entries, v)
	}

	h1StartedInherited := make(chan struct{}, 1)
	h1StartedOverride := make(chan struct{}, 1)
	h2StartedInherited := make(chan struct{}, 1)
	releaseInherited := make(chan struct{})
	releaseOverride := make(chan struct{})

	h1 := func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		mode := e.Payload["mode"].(string)
		appendEntry(mode + ":b1_start")
		switch mode {
		case "inherited":
			h1StartedInherited <- struct{}{}
			<-releaseInherited
		case "override":
			h1StartedOverride <- struct{}{}
			<-releaseOverride
		}
		appendEntry(mode + ":b1_end")
		return "b1", nil
	}
	h2 := func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		mode := e.Payload["mode"].(string)
		appendEntry(mode + ":b2_start")
		if mode == "inherited" {
			h2StartedInherited <- struct{}{}
		}
		appendEntry(mode + ":b2_end")
		return "b2", nil
	}
	trigger := func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		inherited := busA.Emit(abxbus.NewBaseEvent("ForwardedDefaultsChildEvent", map[string]any{"mode": "inherited"}))
		busB.Emit(inherited)
		if _, err := inherited.Now(); err != nil {
			return nil, err
		}

		override := busA.Emit(abxbus.NewBaseEvent("ForwardedDefaultsChildEvent", map[string]any{"mode": "override"}))
		override.EventHandlerConcurrency = abxbus.EventHandlerConcurrencySerial
		busB.Emit(override)
		if _, err := override.Now(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	busA.On("ForwardedDefaultsTriggerEvent", "trigger", trigger, nil)
	busB.On("ForwardedDefaultsChildEvent", "h1", h1, nil)
	busB.On("ForwardedDefaultsChildEvent", "h2", h2, nil)

	top := busA.Emit(abxbus.NewBaseEvent("ForwardedDefaultsTriggerEvent", nil))
	select {
	case <-h1StartedInherited:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for inherited h1 start")
	}
	select {
	case <-h2StartedInherited:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for inherited h2 start before h1 release")
	}
	close(releaseInherited)

	close(releaseOverride)

	if _, err := top.Now(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h1StartedOverride:
	default:
		t.Fatal("override h1 did not start")
	}
	to := 2.0
	if !busA.WaitUntilIdle(&to) {
		t.Fatal("busA did not become idle")
	}
	if !busB.WaitUntilIdle(&to) {
		t.Fatal("busB did not become idle")
	}

	idx := func(s string) int {
		for i, v := range entries {
			if v == s {
				return i
			}
		}
		return -1
	}
	requireIndex := func(label string) int {
		i := idx(label)
		if i < 0 {
			t.Fatalf("missing required log entry %q, log=%v", label, entries)
		}
		return i
	}

	inheritedB2Start := requireIndex("inherited:b2_start")
	inheritedB1End := requireIndex("inherited:b1_end")
	if !(inheritedB2Start < inheritedB1End) {
		t.Fatalf("expected inherited mode parallel on processing bus, log=%v", entries)
	}
	overrideB1End := requireIndex("override:b1_end")
	overrideB2Start := requireIndex("override:b2_start")
	if !(overrideB1End < overrideB2Start) {
		t.Fatalf("expected override mode serial, log=%v", entries)
	}
}

func waitAllIdle(t *testing.T, buses ...*abxbus.EventBus) {
	t.Helper()
	for _, bus := range buses {
		timeout := 2.0
		if !bus.WaitUntilIdle(&timeout) {
			t.Fatalf("%s did not become idle", bus.Name)
		}
	}
}

func containsAll(values []string, expected []string) bool {
	seen := map[string]bool{}
	for _, value := range values {
		seen[value] = true
	}
	for _, value := range expected {
		if !seen[value] {
			return false
		}
	}
	return true
}
