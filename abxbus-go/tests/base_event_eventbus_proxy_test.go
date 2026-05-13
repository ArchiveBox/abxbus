package abxbus_test

import (
	"context"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestBaseEventCarriesEventBusReferenceDuringDispatch(t *testing.T) {
	bus := abxbus.NewEventBus("ProxyBus", nil)
	var seenBus *abxbus.EventBus
	bus.On("ProxyEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		seenBus = event.Bus
		return event.Bus.Name, nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("ProxyEvent", nil))
	result, err := event.EventResult()
	if err != nil {
		t.Fatal(err)
	}
	if seenBus != bus || event.Bus != bus || result != "ProxyBus" {
		t.Fatalf("event bus reference mismatch: seen=%p event=%p bus=%p result=%#v", seenBus, event.Bus, bus, result)
	}
}

func TestBaseEventBusReferenceReflectsForwardedProcessingBus(t *testing.T) {
	source := abxbus.NewEventBus("ProxySourceBus", nil)
	target := abxbus.NewEventBus("ProxyTargetBus", nil)
	source.On("*", "forward", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		target.Emit(event)
		return "forwarded", nil
	}, nil)

	var targetSeenBus *abxbus.EventBus
	target.On("ProxyForwardEvent", "target", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		targetSeenBus = event.Bus
		return event.Bus.Name, nil
	}, nil)

	event := source.Emit(abxbus.NewBaseEvent("ProxyForwardEvent", nil))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	timeout := 2.0
	if !target.WaitUntilIdle(&timeout) {
		t.Fatal("target bus did not become idle")
	}

	if targetSeenBus != target {
		t.Fatalf("forwarded handler should see target bus, got %p want %p", targetSeenBus, target)
	}
	if event.Bus != source {
		t.Fatalf("source event bus reference should be restored after forwarded processing, got %p want %p", event.Bus, source)
	}
	if len(event.EventPath) != 2 || event.EventPath[0] != source.Label() || event.EventPath[1] != target.Label() {
		t.Fatalf("unexpected forwarded event path: %#v", event.EventPath)
	}
}

func TestEventEmitFromForwardedHandlerDispatchesChildOnTargetBus(t *testing.T) {
	source := abxbus.NewEventBus("ProxyChildSourceBus", nil)
	target := abxbus.NewEventBus("ProxyChildTargetBus", nil)
	source.On("*", "forward", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		target.Emit(event)
		return "forwarded", nil
	}, nil)

	var child *abxbus.BaseEvent
	var childSeenBus *abxbus.EventBus
	targetHandler := target.On("ProxyParentEvent", "target_parent", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		if event.Bus != target {
			t.Fatalf("target parent handler should see target bus, got %p want %p", event.Bus, target)
		}
		child = event.Emit(abxbus.NewBaseEvent("ProxyChildEvent", nil))
		if _, err := child.Now(); err != nil {
			return nil, err
		}
		return "parent", nil
	}, nil)
	target.On("ProxyChildEvent", "target_child", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		childSeenBus = event.Bus
		return "child", nil
	}, nil)

	parent := source.Emit(abxbus.NewBaseEvent("ProxyParentEvent", nil))
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}
	timeout := 2.0
	if !target.WaitUntilIdle(&timeout) {
		t.Fatal("target bus did not become idle")
	}

	if child == nil {
		t.Fatal("expected forwarded handler to emit child")
	}
	if child.Bus != target || childSeenBus != target {
		t.Fatalf("child should be dispatched and processed on target bus, child.Bus=%p seen=%p target=%p", child.Bus, childSeenBus, target)
	}
	if len(child.EventPath) != 1 || child.EventPath[0] != target.Label() {
		t.Fatalf("child emitted from forwarded handler should stay on target bus, path=%#v", child.EventPath)
	}
	if child.EventParentID == nil || *child.EventParentID != parent.EventID {
		t.Fatalf("child parent ID should link to forwarded parent")
	}
	if child.EventEmittedByHandlerID == nil || *child.EventEmittedByHandlerID != targetHandler.ID {
		t.Fatalf("child emitted-by handler should be target handler %s, got %#v", targetHandler.ID, child.EventEmittedByHandlerID)
	}
}
