package abxbus_test

import (
	"context"
	"errors"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/v2/abxbus-go"
)

func TestQueueJumpProcessesChildInsideParentHandler(t *testing.T) {
	bus := abxbus.NewEventBus("QueueJumpBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	var capturedChild *abxbus.BaseEvent
	childProcessedBeforeParentReturn := false

	bus.On("Parent", "on_parent", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		capturedChild = e.Emit(abxbus.NewBaseEvent("Child", nil))
		if _, err := capturedChild.Now(); err != nil {
			return nil, err
		}
		if capturedChild.EventStatus == "completed" {
			childProcessedBeforeParentReturn = true
		}
		return "parent", nil
	}, nil)
	bus.On("Child", "on_child", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "child", nil
	}, nil)

	parent := bus.Emit(abxbus.NewBaseEvent("Parent", nil))
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}
	if !childProcessedBeforeParentReturn {
		t.Fatal("expected child queue-jump processing to complete inside parent handler")
	}
	if parent.EventStatus != "completed" {
		t.Fatalf("expected parent completed status, got %s", parent.EventStatus)
	}
	if capturedChild == nil {
		t.Fatal("expected child event to be emitted")
	}
	if capturedChild.EventStatus != "completed" {
		t.Fatalf("expected child completed status, got %s", capturedChild.EventStatus)
	}
	if capturedChild.EventParentID == nil || *capturedChild.EventParentID != parent.EventID {
		t.Fatalf("expected child parent ID to link to parent event")
	}
	if capturedChild.EventEmittedByHandlerID == nil {
		t.Fatalf("expected child emitted-by handler id to be set")
	}
	if !capturedChild.EventBlocksParentCompletion {
		t.Fatalf("expected awaited child to block parent completion")
	}

	childResult, err := capturedChild.EventResult()
	if err != nil {
		t.Fatal(err)
	}
	if childResult != "child" {
		t.Fatalf("expected child result value, got %#v", childResult)
	}
	parentResult, err := parent.EventResult()
	if err != nil {
		t.Fatal(err)
	}
	if parentResult != "parent" {
		t.Fatalf("expected parent result value, got %#v", parentResult)
	}
}

func TestEventEmitWithoutAwaitTracksChildButDoesNotBlockParentCompletion(t *testing.T) {
	bus := abxbus.NewEventBus("UnawaitedEventEmitBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	childStarted := make(chan struct{}, 1)
	releaseChild := make(chan struct{})
	var capturedChild *abxbus.BaseEvent

	bus.On("Parent", "on_parent", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		capturedChild = e.Emit(abxbus.NewBaseEvent("Child", map[string]any{"mode": "unawaited"}))
		return "parent", nil
	}, nil)
	bus.On("Child", "on_child", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		childStarted <- struct{}{}
		<-releaseChild
		return "child", nil
	}, nil)

	parent := bus.Emit(abxbus.NewBaseEvent("Parent", nil))
	if _, err := parent.Now(); err != nil {
		close(releaseChild)
		t.Fatal(err)
	}
	if capturedChild == nil {
		close(releaseChild)
		t.Fatal("expected child event")
	}
	if capturedChild.EventParentID == nil || *capturedChild.EventParentID != parent.EventID {
		close(releaseChild)
		t.Fatalf("expected event.emit child parent ID to link to parent event")
	}
	if capturedChild.EventEmittedByHandlerID == nil {
		close(releaseChild)
		t.Fatalf("expected event.emit child emitted-by handler id")
	}
	if capturedChild.EventBlocksParentCompletion {
		close(releaseChild)
		t.Fatalf("unawaited event.emit child should not block parent completion")
	}
	if parent.EventStatus != "completed" {
		close(releaseChild)
		t.Fatalf("parent should complete without waiting for unawaited child, got %s", parent.EventStatus)
	}

	select {
	case <-childStarted:
	case <-time.After(2 * time.Second):
		close(releaseChild)
		t.Fatal("timed out waiting for child to start after parent completion")
	}
	if capturedChild.EventStatus == "completed" {
		close(releaseChild)
		t.Fatalf("child should still be blocked after parent completion")
	}
	close(releaseChild)
	if _, err := capturedChild.Now(); err != nil {
		t.Fatal(err)
	}
}

func TestBusEmitInsideHandlerIsUntrackedBackgroundEvent(t *testing.T) {
	bus := abxbus.NewEventBus("BackgroundBusEmitBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	bgStarted := make(chan struct{}, 1)
	releaseBg := make(chan struct{})
	var background *abxbus.BaseEvent

	bus.On("Parent", "on_parent", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		background = bus.Emit(abxbus.NewBaseEvent("Background", map[string]any{"mode": "untracked"}))
		return "parent", nil
	}, nil)
	bus.On("Background", "on_background", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		bgStarted <- struct{}{}
		<-releaseBg
		return "background", nil
	}, nil)

	parent := bus.Emit(abxbus.NewBaseEvent("Parent", nil))
	if _, err := parent.Now(); err != nil {
		close(releaseBg)
		t.Fatal(err)
	}
	if background == nil {
		close(releaseBg)
		t.Fatal("expected background event")
	}
	if background.EventParentID != nil {
		close(releaseBg)
		t.Fatalf("bus.Emit inside handler should not set parent ID, got %s", *background.EventParentID)
	}
	if background.EventEmittedByHandlerID != nil {
		close(releaseBg)
		t.Fatalf("bus.Emit inside handler should not set emitted-by handler id")
	}
	if background.EventBlocksParentCompletion {
		close(releaseBg)
		t.Fatalf("unawaited bus.Emit event should not block parent completion")
	}
	for _, result := range parent.EventResults {
		if len(result.EventChildren) != 0 {
			close(releaseBg)
			t.Fatalf("bus.Emit background event should not be listed in parent event_children")
		}
	}

	select {
	case <-bgStarted:
	case <-time.After(2 * time.Second):
		close(releaseBg)
		t.Fatal("timed out waiting for background event to start")
	}
	close(releaseBg)
	if _, err := background.Now(); err != nil {
		t.Fatal(err)
	}
}

func TestAwaitedBusEmitInsideHandlerQueueJumpsButStaysUntrackedRootEvent(t *testing.T) {
	bus := abxbus.NewEventBus("AwaitedBackgroundBusEmitBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	var background *abxbus.BaseEvent
	backgroundCompletedBeforeParentReturn := false

	bus.On("Parent", "on_parent", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		background = bus.Emit(abxbus.NewBaseEvent("Background", map[string]any{"mode": "awaited"}))
		if _, err := background.Now(); err != nil {
			return nil, err
		}
		backgroundCompletedBeforeParentReturn = background.EventStatus == "completed"
		return "parent", nil
	}, nil)
	bus.On("Background", "on_background", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "background", nil
	}, nil)

	parent := bus.Emit(abxbus.NewBaseEvent("Parent", nil))
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}
	if background == nil {
		t.Fatal("expected background event")
	}
	if !backgroundCompletedBeforeParentReturn {
		t.Fatalf("awaited bus.Emit event should queue-jump and complete inside parent handler")
	}
	if background.EventParentID != nil {
		t.Fatalf("awaited bus.Emit inside handler should not set parent ID, got %s", *background.EventParentID)
	}
	if background.EventEmittedByHandlerID != nil {
		t.Fatalf("awaited bus.Emit inside handler should not set emitted-by handler id")
	}
	if background.EventBlocksParentCompletion {
		t.Fatalf("awaited bus.Emit root event should not become parent-blocking")
	}
	for _, result := range parent.EventResults {
		if len(result.EventChildren) != 0 {
			t.Fatalf("awaited bus.Emit root event should not be listed in parent event_children")
		}
	}
}

func TestErroringParentHandlersStillTrackChildrenAndContinue(t *testing.T) {
	bus := abxbus.NewEventBus("ErrorParentTrackingBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
	})
	childEvents := []*abxbus.BaseEvent{}

	bus.On("Parent", "failing", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		child := e.Emit(abxbus.NewBaseEvent("Child", map[string]any{"source": "failing"}))
		childEvents = append(childEvents, child)
		return nil, errors.New("expected parent handler failure")
	}, nil)
	bus.On("Parent", "success", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		child := e.Emit(abxbus.NewBaseEvent("Child", map[string]any{"source": "success"}))
		childEvents = append(childEvents, child)
		return "success", nil
	}, nil)
	bus.On("Child", "child", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "child", nil
	}, nil)

	parent := bus.Emit(abxbus.NewBaseEvent("Parent", nil))
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}
	if len(childEvents) != 2 {
		t.Fatalf("expected both parent handlers to run and emit children, got %d", len(childEvents))
	}
	for _, child := range childEvents {
		if child.EventParentID == nil || *child.EventParentID != parent.EventID {
			t.Fatalf("child should link to parent after handler error path, got %#v", child.EventParentID)
		}
		if child.EventEmittedByHandlerID == nil {
			t.Fatalf("child should record emitting handler id")
		}
	}
	if len(parentChildEvents(parent)) != 2 {
		t.Fatalf("parent event results should track both children, got %#v", parentChildEvents(parent))
	}
}

func TestEventChildrenTrackDirectAndNestedDescendants(t *testing.T) {
	bus := abxbus.NewEventBus("NestedChildrenTrackingBus", nil)
	var child *abxbus.BaseEvent
	var grandchild *abxbus.BaseEvent

	bus.On("Parent", "parent", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		child = e.Emit(abxbus.NewBaseEvent("Child", map[string]any{"level": 1}))
		return "parent", nil
	}, nil)
	bus.On("Child", "child", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		grandchild = e.Emit(abxbus.NewBaseEvent("Grandchild", map[string]any{"level": 2}))
		return "child", nil
	}, nil)
	bus.On("Grandchild", "grandchild", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "grandchild", nil
	}, nil)

	parent := bus.Emit(abxbus.NewBaseEvent("Parent", nil))
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}
	timeout := 2.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("bus did not become idle")
	}
	if child == nil || grandchild == nil {
		t.Fatalf("expected child and grandchild to be emitted, child=%#v grandchild=%#v", child, grandchild)
	}
	parentChildren := parentChildEvents(parent)
	if len(parentChildren) != 1 || parentChildren[0].EventID != child.EventID {
		t.Fatalf("parent should track direct child only, got %#v", parentChildren)
	}
	childChildren := parentChildEvents(child)
	if len(childChildren) != 1 || childChildren[0].EventID != grandchild.EventID {
		t.Fatalf("child should track direct grandchild only, got %#v", childChildren)
	}
	if !bus.EventIsChildOf(grandchild, parent) {
		t.Fatalf("grandchild should be a descendant of parent")
	}
}

func parentChildEvents(event *abxbus.BaseEvent) []*abxbus.BaseEvent {
	children := []*abxbus.BaseEvent{}
	seen := map[string]bool{}
	for _, result := range event.EventResults {
		for _, child := range result.EventChildren {
			if seen[child.EventID] {
				continue
			}
			seen[child.EventID] = true
			children = append(children, child)
		}
	}
	return children
}
