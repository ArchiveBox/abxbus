package abxbus_test

import (
	"context"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestQueueJumpProcessesChildInsideParentHandler(t *testing.T) {
	bus := abxbus.NewEventBus("QueueJumpBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	var capturedChild *abxbus.BaseEvent
	childProcessedBeforeParentReturn := false

	bus.On("Parent", "on_parent", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		capturedChild = e.Emit(abxbus.NewBaseEvent("Child", nil))
		if _, err := capturedChild.Done(ctx); err != nil {
			return nil, err
		}
		if capturedChild.EventStatus == "completed" {
			childProcessedBeforeParentReturn = true
		}
		return "parent", nil
	}, nil)
	bus.On("Child", "on_child", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "child", nil
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	parent := bus.Emit(abxbus.NewBaseEvent("Parent", nil))
	if _, err := parent.Done(ctx); err != nil {
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

	childResult, err := capturedChild.EventResult(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if childResult != "child" {
		t.Fatalf("expected child result value, got %#v", childResult)
	}
	parentResult, err := parent.EventResult(ctx)
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

	bus.On("Parent", "on_parent", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		capturedChild = e.Emit(abxbus.NewBaseEvent("Child", map[string]any{"mode": "unawaited"}))
		return "parent", nil
	}, nil)
	bus.On("Child", "on_child", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		childStarted <- struct{}{}
		<-releaseChild
		return "child", nil
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	parent := bus.Emit(abxbus.NewBaseEvent("Parent", nil))
	if _, err := parent.Done(ctx); err != nil {
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
	if _, err := capturedChild.Done(ctx); err != nil {
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

	bus.On("Parent", "on_parent", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		background = bus.Emit(abxbus.NewBaseEvent("Background", map[string]any{"mode": "untracked"}))
		return "parent", nil
	}, nil)
	bus.On("Background", "on_background", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		bgStarted <- struct{}{}
		<-releaseBg
		return "background", nil
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	parent := bus.Emit(abxbus.NewBaseEvent("Parent", nil))
	if _, err := parent.Done(ctx); err != nil {
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
	if _, err := background.Done(ctx); err != nil {
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

	bus.On("Parent", "on_parent", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		background = bus.Emit(abxbus.NewBaseEvent("Background", map[string]any{"mode": "awaited"}))
		if _, err := background.Done(ctx); err != nil {
			return nil, err
		}
		backgroundCompletedBeforeParentReturn = background.EventStatus == "completed"
		return "parent", nil
	}, nil)
	bus.On("Background", "on_background", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "background", nil
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	parent := bus.Emit(abxbus.NewBaseEvent("Parent", nil))
	if _, err := parent.Done(ctx); err != nil {
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
