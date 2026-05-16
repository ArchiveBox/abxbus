package abxbus_test

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/v2/abxbus-go"
)

func TestEventsForwardBetweenBusesWithoutDuplication(t *testing.T) {
	busA := abxbus.NewEventBus("BusA", nil)
	busB := abxbus.NewEventBus("BusB", nil)
	busC := abxbus.NewEventBus("BusC", nil)
	defer busA.Destroy()
	defer busB.Destroy()
	defer busC.Destroy()

	seenA := []string{}
	seenB := []string{}
	seenC := []string{}
	busA.On("PingEvent", "seen_a", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		seenA = append(seenA, event.EventID)
		return "a", nil
	}, nil)
	busB.On("PingEvent", "seen_b", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		seenB = append(seenB, event.EventID)
		return "b", nil
	}, nil)
	busC.On("PingEvent", "seen_c", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		seenC = append(seenC, event.EventID)
		return "c", nil
	}, nil)
	busA.OnEventName("*", "forward_to_b", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		busB.Emit(event)
		return nil, nil
	}, nil)
	busB.OnEventName("*", "forward_to_c", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
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
	if event.EventPendingBusCount != 0 {
		t.Fatalf("event pending bus count should be zero, got %d", event.EventPendingBusCount)
	}
}

func TestTreeLevelHierarchyBubbling(t *testing.T) {
	parentBus := abxbus.NewEventBus("ParentBus", nil)
	childBus := abxbus.NewEventBus("ChildBus", nil)
	subchildBus := abxbus.NewEventBus("SubchildBus", nil)
	defer parentBus.Destroy()
	defer childBus.Destroy()
	defer subchildBus.Destroy()

	seenParent := []string{}
	seenChild := []string{}
	seenSubchild := []string{}
	parentBus.On("PingEvent", "parent_seen", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		seenParent = append(seenParent, event.EventID)
		return nil, nil
	}, nil)
	childBus.On("PingEvent", "child_seen", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		seenChild = append(seenChild, event.EventID)
		return nil, nil
	}, nil)
	subchildBus.On("PingEvent", "subchild_seen", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		seenSubchild = append(seenSubchild, event.EventID)
		return nil, nil
	}, nil)
	childBus.OnEventName("*", "forward_to_parent", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		parentBus.Emit(event)
		return nil, nil
	}, nil)
	subchildBus.OnEventName("*", "forward_to_child", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		childBus.Emit(event)
		return nil, nil
	}, nil)

	bottom := subchildBus.Emit(abxbus.NewBaseEvent("PingEvent", map[string]any{"value": 1}))
	if _, err := bottom.Now(); err != nil {
		t.Fatal(err)
	}
	waitAllIdle(t, subchildBus, childBus, parentBus)

	if !reflect.DeepEqual(seenSubchild, []string{bottom.EventID}) ||
		!reflect.DeepEqual(seenChild, []string{bottom.EventID}) ||
		!reflect.DeepEqual(seenParent, []string{bottom.EventID}) {
		t.Fatalf("bottom event should bubble once through all buses, got subchild=%v child=%v parent=%v", seenSubchild, seenChild, seenParent)
	}
	expectedPath := []string{subchildBus.Label(), childBus.Label(), parentBus.Label()}
	if !reflect.DeepEqual(bottom.EventPath, expectedPath) {
		t.Fatalf("unexpected bottom path: got %v want %v", bottom.EventPath, expectedPath)
	}

	seenParent, seenChild, seenSubchild = []string{}, []string{}, []string{}
	middle := childBus.Emit(abxbus.NewBaseEvent("PingEvent", map[string]any{"value": 2}))
	if _, err := middle.Now(); err != nil {
		t.Fatal(err)
	}
	waitAllIdle(t, childBus, parentBus)

	if len(seenSubchild) != 0 || !reflect.DeepEqual(seenChild, []string{middle.EventID}) || !reflect.DeepEqual(seenParent, []string{middle.EventID}) {
		t.Fatalf("middle event should bubble to parent only, got subchild=%v child=%v parent=%v", seenSubchild, seenChild, seenParent)
	}
	expectedPath = []string{childBus.Label(), parentBus.Label()}
	if !reflect.DeepEqual(middle.EventPath, expectedPath) {
		t.Fatalf("unexpected middle path: got %v want %v", middle.EventPath, expectedPath)
	}
}

func TestForwardingDisambiguatesBusesThatShareTheSameName(t *testing.T) {
	busA := abxbus.NewEventBus("SharedName", nil)
	busB := abxbus.NewEventBus("SharedName", nil)
	defer busA.Destroy()
	defer busB.Destroy()

	seenA := []string{}
	seenB := []string{}
	busA.On("PingEvent", "seen_a", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		seenA = append(seenA, event.EventID)
		return "a", nil
	}, nil)
	busB.On("PingEvent", "seen_b", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		seenB = append(seenB, event.EventID)
		return "b", nil
	}, nil)
	busA.OnEventName("*", "forward_to_b", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
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

func TestAwaitEventNowWaitsForHandlersOnForwardedBuses(t *testing.T) {
	busA := abxbus.NewEventBus("ForwardWaitA", nil)
	busB := abxbus.NewEventBus("ForwardWaitB", nil)
	busC := abxbus.NewEventBus("ForwardWaitC", nil)
	defer busA.Destroy()
	defer busB.Destroy()
	defer busC.Destroy()

	var mu sync.Mutex
	completionLog := []string{}
	record := func(value string) {
		mu.Lock()
		defer mu.Unlock()
		completionLog = append(completionLog, value)
	}

	busA.On("PingEvent", "a", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(10 * time.Millisecond)
		record("A")
		return "a", nil
	}, nil)
	busB.On("PingEvent", "b", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(30 * time.Millisecond)
		record("B")
		return "b", nil
	}, nil)
	busC.On("PingEvent", "c", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(50 * time.Millisecond)
		record("C")
		return "c", nil
	}, nil)
	busA.OnEventName("*", "forward_to_b", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		busB.Emit(event)
		return nil, nil
	}, nil)
	busB.OnEventName("*", "forward_to_c", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
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
	expectedPath := []string{busA.Label(), busB.Label(), busC.Label()}
	if !reflect.DeepEqual(event.EventPath, expectedPath) {
		t.Fatalf("unexpected forwarded wait path: got %v want %v", event.EventPath, expectedPath)
	}
}

func TestCircularForwardingFromFirstPeerDoesNotLoop(t *testing.T) {
	peer1 := abxbus.NewEventBus("Peer1", nil)
	peer2 := abxbus.NewEventBus("Peer2", nil)
	peer3 := abxbus.NewEventBus("Peer3", nil)
	defer peer1.Destroy()
	defer peer2.Destroy()
	defer peer3.Destroy()

	seen1, seen2, seen3 := registerCycle(t, peer1, peer2, peer3)
	event := peer1.Emit(abxbus.NewBaseEvent("PingEvent", map[string]any{"value": 42}))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	waitAllIdle(t, peer1, peer2, peer3)

	if !reflect.DeepEqual(*seen1, []string{event.EventID}) ||
		!reflect.DeepEqual(*seen2, []string{event.EventID}) ||
		!reflect.DeepEqual(*seen3, []string{event.EventID}) {
		t.Fatalf("cycle should see event once per peer, got p1=%v p2=%v p3=%v", *seen1, *seen2, *seen3)
	}
	expectedPath := []string{peer1.Label(), peer2.Label(), peer3.Label()}
	if !reflect.DeepEqual(event.EventPath, expectedPath) {
		t.Fatalf("unexpected cycle path from peer1: got %v want %v", event.EventPath, expectedPath)
	}
}

func TestCircularForwardingFromMiddlePeerDoesNotLoop(t *testing.T) {
	peer1 := abxbus.NewEventBus("RacePeer1", nil)
	peer2 := abxbus.NewEventBus("RacePeer2", nil)
	peer3 := abxbus.NewEventBus("RacePeer3", nil)
	defer peer1.Destroy()
	defer peer2.Destroy()
	defer peer3.Destroy()

	seen1, seen2, seen3 := registerCycle(t, peer1, peer2, peer3)
	warmup := peer1.Emit(abxbus.NewBaseEvent("PingEvent", map[string]any{"value": 42}))
	if _, err := warmup.Now(); err != nil {
		t.Fatal(err)
	}
	waitAllIdle(t, peer1, peer2, peer3)
	*seen1, *seen2, *seen3 = []string{}, []string{}, []string{}

	event := peer2.Emit(abxbus.NewBaseEvent("PingEvent", map[string]any{"value": 99}))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	waitAllIdle(t, peer1, peer2, peer3)

	if !reflect.DeepEqual(*seen1, []string{event.EventID}) ||
		!reflect.DeepEqual(*seen2, []string{event.EventID}) ||
		!reflect.DeepEqual(*seen3, []string{event.EventID}) {
		t.Fatalf("cycle should see event once per peer, got p1=%v p2=%v p3=%v", *seen1, *seen2, *seen3)
	}
	expectedPath := []string{peer2.Label(), peer3.Label(), peer1.Label()}
	if !reflect.DeepEqual(event.EventPath, expectedPath) {
		t.Fatalf("unexpected cycle path from peer2: got %v want %v", event.EventPath, expectedPath)
	}
	if event.EventStatus != "completed" {
		t.Fatalf("event should be completed, got %s", event.EventStatus)
	}
}

func TestAwaitEventNowWaitsWhenForwardingHandlerIsAsyncDelayed(t *testing.T) {
	busA := abxbus.NewEventBus("BusADelayedForward", nil)
	busB := abxbus.NewEventBus("BusBDelayedForward", nil)
	defer busA.Destroy()
	defer busB.Destroy()

	busADone := false
	busBDone := false
	busA.On("PingEvent", "handler_a", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(20 * time.Millisecond)
		busADone = true
		return nil, nil
	}, nil)
	busB.On("PingEvent", "handler_b", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(10 * time.Millisecond)
		busBDone = true
		return nil, nil
	}, nil)
	busA.OnEventName("*", "delayed_forward_to_b", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(30 * time.Millisecond)
		busB.Emit(event)
		return nil, nil
	}, nil)

	event := busA.Emit(abxbus.NewBaseEvent("PingEvent", map[string]any{"value": 3}))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}

	if !busADone || !busBDone {
		t.Fatalf("event.Now should wait for delayed forwarding handlers, busADone=%v busBDone=%v", busADone, busBDone)
	}
	if event.EventPendingBusCount != 0 {
		t.Fatalf("event pending bus count should be zero, got %d", event.EventPendingBusCount)
	}
	expectedPath := []string{busA.Label(), busB.Label()}
	if !reflect.DeepEqual(event.EventPath, expectedPath) {
		t.Fatalf("unexpected delayed forwarding path: got %v want %v", event.EventPath, expectedPath)
	}
}

func TestForwardingSameEventDoesNotSetSelfParentID(t *testing.T) {
	origin := abxbus.NewEventBus("SelfParentOrigin", nil)
	target := abxbus.NewEventBus("SelfParentTarget", nil)
	defer origin.Destroy()
	defer target.Destroy()

	origin.On("SelfParentForwardEvent", "origin_handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "origin-ok", nil
	}, nil)
	target.On("SelfParentForwardEvent", "target_handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "target-ok", nil
	}, nil)
	origin.OnEventName("*", "forward_to_target", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		target.Emit(event)
		return nil, nil
	}, nil)

	event := origin.Emit(abxbus.NewBaseEvent("SelfParentForwardEvent", nil))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	waitAllIdle(t, origin, target)

	if event.EventParentID != nil {
		t.Fatalf("expected nil parent for forwarded same event, got %v", *event.EventParentID)
	}
	expectedPath := []string{origin.Label(), target.Label()}
	if !reflect.DeepEqual(event.EventPath, expectedPath) {
		t.Fatalf("unexpected self-parent forwarding path: got %v want %v", event.EventPath, expectedPath)
	}
}

func TestForwardedEventUsesProcessingBusDefaults(t *testing.T) {
	busATimeout := 1.5
	busBTimeout := 2.5
	busA := abxbus.NewEventBus("ForwardDefaultsA", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventTimeout:            &busATimeout,
	})
	busB := abxbus.NewEventBus("ForwardDefaultsB", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
		EventTimeout:            &busBTimeout,
	})
	defer busA.Destroy()
	defer busB.Destroy()

	var mu sync.Mutex
	entries := []string{}
	appendEntry := func(v string) {
		mu.Lock()
		defer mu.Unlock()
		entries = append(entries, v)
	}
	h1Started := make(chan struct{}, 1)
	h2Started := make(chan struct{}, 1)
	release := make(chan struct{})
	var inheritedRef *abxbus.BaseEvent

	busB.On("ForwardedDefaultsChildEvent", "h1", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		if e.EventTimeout != nil || e.EventHandlerConcurrency != "" || e.EventHandlerCompletion != "" {
			t.Fatalf("forwarded event should keep defaults unset in handler: %#v", e)
		}
		mode := e.Payload["mode"].(string)
		appendEntry(mode + ":b1_start")
		h1Started <- struct{}{}
		<-release
		appendEntry(mode + ":b1_end")
		return "b1", nil
	}, nil)
	busB.On("ForwardedDefaultsChildEvent", "h2", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		if e.EventTimeout != nil || e.EventHandlerConcurrency != "" || e.EventHandlerCompletion != "" {
			t.Fatalf("forwarded event should keep defaults unset in handler: %#v", e)
		}
		mode := e.Payload["mode"].(string)
		appendEntry(mode + ":b2_start")
		h2Started <- struct{}{}
		appendEntry(mode + ":b2_end")
		return "b2", nil
	}, nil)
	busA.On("ForwardedDefaultsTriggerEvent", "trigger", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		inherited := e.Emit(abxbus.NewBaseEvent("ForwardedDefaultsChildEvent", map[string]any{"mode": "inherited"}))
		inheritedRef = inherited
		busB.Emit(inherited)
		if _, err := inherited.Now(); err != nil {
			return nil, err
		}
		return nil, nil
	}, nil)

	top := busA.Emit(abxbus.NewBaseEvent("ForwardedDefaultsTriggerEvent", nil))
	select {
	case <-h1Started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for inherited h1 start")
	}
	select {
	case <-h2Started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for inherited h2 start before h1 release")
	}
	close(release)
	if _, err := top.Now(); err != nil {
		t.Fatal(err)
	}
	waitAllIdle(t, busA, busB)

	mu.Lock()
	defer mu.Unlock()
	if !(forwardingIndexOf(entries, "inherited:b2_start") < forwardingIndexOf(entries, "inherited:b1_end")) {
		t.Fatalf("expected inherited mode parallel on processing bus, log=%v", entries)
	}
	if inheritedRef == nil {
		t.Fatal("missing inherited event reference")
	}
	if inheritedRef.EventTimeout != nil || inheritedRef.EventHandlerConcurrency != "" || inheritedRef.EventHandlerCompletion != "" {
		t.Fatalf("forwarded event should keep defaults unset after processing: %#v", inheritedRef)
	}
	busBResults := 0
	for _, result := range inheritedRef.EventResults {
		if result.EventBusID != busB.ID {
			continue
		}
		busBResults++
		if result.HandlerTimeout == nil || *result.HandlerTimeout != busBTimeout {
			t.Fatalf("target bus default timeout should be resolved on handler result, got %#v", result.HandlerTimeout)
		}
	}
	if busBResults == 0 {
		t.Fatal("expected busB handler results")
	}
}

func TestForwardedEventPreservesExplicitHandlerConcurrencyOverride(t *testing.T) {
	busA := abxbus.NewEventBus("ForwardOverrideA", &abxbus.EventBusOptions{EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel})
	busB := abxbus.NewEventBus("ForwardOverrideB", &abxbus.EventBusOptions{EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel})
	defer busA.Destroy()
	defer busB.Destroy()

	var mu sync.Mutex
	entries := []string{}
	appendEntry := func(v string) {
		mu.Lock()
		defer mu.Unlock()
		entries = append(entries, v)
	}
	h1Started := make(chan struct{}, 1)
	h2Started := make(chan struct{}, 1)
	release := make(chan struct{})

	busB.On("ForwardedDefaultsChildEvent", "h1", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		mode := e.Payload["mode"].(string)
		appendEntry(mode + ":b1_start")
		h1Started <- struct{}{}
		<-release
		appendEntry(mode + ":b1_end")
		return "b1", nil
	}, nil)
	busB.On("ForwardedDefaultsChildEvent", "h2", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		mode := e.Payload["mode"].(string)
		appendEntry(mode + ":b2_start")
		h2Started <- struct{}{}
		appendEntry(mode + ":b2_end")
		return "b2", nil
	}, nil)
	busA.On("ForwardedDefaultsTriggerEvent", "trigger", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		override := e.Emit(abxbus.NewBaseEvent("ForwardedDefaultsChildEvent", map[string]any{"mode": "override"}))
		override.EventHandlerConcurrency = abxbus.EventHandlerConcurrencySerial
		busB.Emit(override)
		if _, err := override.Now(); err != nil {
			return nil, err
		}
		return nil, nil
	}, nil)

	top := busA.Emit(abxbus.NewBaseEvent("ForwardedDefaultsTriggerEvent", nil))
	select {
	case <-h1Started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for override h1 start")
	}
	select {
	case <-h2Started:
		t.Fatal("override h2 started before h1 release; explicit serial override was ignored")
	default:
	}
	close(release)
	if _, err := top.Now(); err != nil {
		t.Fatal(err)
	}
	waitAllIdle(t, busA, busB)

	mu.Lock()
	defer mu.Unlock()
	if !(forwardingIndexOf(entries, "override:b1_end") < forwardingIndexOf(entries, "override:b2_start")) {
		t.Fatalf("expected override mode serial, log=%v", entries)
	}
}

func TestForwardedFirstModeUsesProcessingBusHandlerConcurrencyDefaults(t *testing.T) {
	busA := abxbus.NewEventBus("ForwardedFirstDefaultsA", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
	})
	busB := abxbus.NewEventBus("ForwardedFirstDefaultsB", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionFirst,
	})
	defer busA.Destroy()
	defer busB.Destroy()

	var mu sync.Mutex
	log := []string{}
	appendLog := func(v string) {
		mu.Lock()
		defer mu.Unlock()
		log = append(log, v)
	}

	busA.OnEventName("*", "forward_to_b", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		busB.Emit(event)
		return nil, nil
	}, nil)
	busB.On("ForwardedFirstDefaultsEvent", "slow", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		appendLog("slow_start")
		time.Sleep(20 * time.Millisecond)
		appendLog("slow_end")
		return "slow", nil
	}, nil)
	busB.On("ForwardedFirstDefaultsEvent", "fast", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		appendLog("fast_start")
		time.Sleep(time.Millisecond)
		appendLog("fast_end")
		return "fast", nil
	}, nil)

	event := busA.Emit(abxbus.NewBaseEvent("ForwardedFirstDefaultsEvent", nil))
	if _, err := event.Now(&abxbus.EventWaitOptions{FirstResult: true}); err != nil {
		t.Fatal(err)
	}
	result, err := event.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false})
	if err != nil {
		t.Fatal(err)
	}
	waitAllIdle(t, busA, busB)

	mu.Lock()
	defer mu.Unlock()
	if result != "fast" {
		t.Fatalf("first-mode on processing bus should pick fast handler, got %v log=%v", result, log)
	}
	if !containsAll(log, []string{"slow_start", "fast_start"}) {
		t.Fatalf("both handlers should start under parallel first-mode, log=%v", log)
	}
}

func TestProxyDispatchAutoLinksChildEventsLikeEmit(t *testing.T) {
	bus := abxbus.NewEventBus("ProxyDispatchAutoLinkBus", nil)
	defer bus.Destroy()

	bus.On("ProxyDispatchRootEvent", "root_handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		event.Emit(abxbus.NewBaseEvent("ProxyDispatchChildEvent", nil))
		return "root", nil
	}, nil)
	bus.On("ProxyDispatchChildEvent", "child_handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "child", nil
	}, nil)

	root := bus.Emit(abxbus.NewBaseEvent("ProxyDispatchRootEvent", nil))
	if _, err := root.Now(); err != nil {
		t.Fatal(err)
	}
	waitAllIdle(t, bus)

	children := eventChildren(root)
	if len(children) != 1 {
		t.Fatalf("expected one child event, got %d", len(children))
	}
	if children[0].EventParentID == nil || *children[0].EventParentID != root.EventID {
		t.Fatalf("child parent id should be root event id, got %v want %s", children[0].EventParentID, root.EventID)
	}
}

func TestProxyDispatchOfSameEventDoesNotSelfParentOrSelfLinkChild(t *testing.T) {
	bus := abxbus.NewEventBus("ProxyDispatchSameEventBus", nil)
	defer bus.Destroy()

	bus.On("ProxyDispatchRootEvent", "root_handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		event.Emit(event)
		return "root", nil
	}, nil)

	root := bus.Emit(abxbus.NewBaseEvent("ProxyDispatchRootEvent", nil))
	if _, err := root.Now(); err != nil {
		t.Fatal(err)
	}
	waitAllIdle(t, bus)

	if root.EventParentID != nil {
		t.Fatalf("expected nil parent for same-event dispatch, got %v", *root.EventParentID)
	}
	if children := eventChildren(root); len(children) != 0 {
		t.Fatalf("expected no self-linked children, got %d", len(children))
	}
}

func TestEventsAreProcessedInFIFOOrder(t *testing.T) {
	bus := abxbus.NewEventBus("FifoBus", nil)
	defer bus.Destroy()

	processedOrders := []int{}
	handlerStartTimes := []time.Time{}
	bus.On("OrderEvent", "order_handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		handlerStartTimes = append(handlerStartTimes, time.Now())
		order := event.Payload["order"].(int)
		if order%2 == 0 {
			time.Sleep(30 * time.Millisecond)
		} else {
			time.Sleep(5 * time.Millisecond)
		}
		processedOrders = append(processedOrders, order)
		return nil, nil
	}, nil)

	for order := 0; order < 10; order++ {
		bus.Emit(abxbus.NewBaseEvent("OrderEvent", map[string]any{"order": order}))
	}
	waitAllIdle(t, bus)

	expected := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	if !reflect.DeepEqual(processedOrders, expected) {
		t.Fatalf("events should be processed in FIFO order, got %v want %v", processedOrders, expected)
	}
	for i := 1; i < len(handlerStartTimes); i++ {
		if handlerStartTimes[i].Before(handlerStartTimes[i-1]) {
			t.Fatalf("handler start times should be monotonic, got %v", handlerStartTimes)
		}
	}
}

func registerCycle(t *testing.T, peer1, peer2, peer3 *abxbus.EventBus) (*[]string, *[]string, *[]string) {
	t.Helper()
	seen1 := []string{}
	seen2 := []string{}
	seen3 := []string{}
	peer1.On("PingEvent", "seen_1", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		seen1 = append(seen1, event.EventID)
		return "p1", nil
	}, nil)
	peer2.On("PingEvent", "seen_2", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		seen2 = append(seen2, event.EventID)
		return "p2", nil
	}, nil)
	peer3.On("PingEvent", "seen_3", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		seen3 = append(seen3, event.EventID)
		return "p3", nil
	}, nil)
	peer1.OnEventName("*", "forward_to_2", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		peer2.Emit(event)
		return nil, nil
	}, nil)
	peer2.OnEventName("*", "forward_to_3", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		peer3.Emit(event)
		return nil, nil
	}, nil)
	peer3.OnEventName("*", "forward_to_1", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		peer1.Emit(event)
		return nil, nil
	}, nil)
	return &seen1, &seen2, &seen3
}

func eventChildren(event *abxbus.BaseEvent) []*abxbus.BaseEvent {
	children := []*abxbus.BaseEvent{}
	for _, result := range event.EventResults {
		children = append(children, result.EventChildren...)
	}
	return children
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

func forwardingIndexOf(values []string, expected string) int {
	for i, value := range values {
		if value == expected {
			return i
		}
	}
	return -1
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
