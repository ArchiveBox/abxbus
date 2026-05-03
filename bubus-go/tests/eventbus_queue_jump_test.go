package bubus_test

import (
	"context"
	"testing"
	"time"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestQueueJumpProcessesChildInsideParentHandler(t *testing.T) {
	bus := bubus.NewEventBus("QueueJumpBus", &bubus.EventBusOptions{
		EventHandlerConcurrency: bubus.EventHandlerConcurrencySerial,
	})
	var capturedChild *bubus.BaseEvent
	childProcessedBeforeParentReturn := false

	bus.On("Parent", "on_parent", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		capturedChild = bus.Emit(bubus.NewBaseEvent("Child", nil))
		if _, err := capturedChild.Done(ctx); err != nil {
			return nil, err
		}
		if capturedChild.EventStatus == "completed" {
			childProcessedBeforeParentReturn = true
		}
		return "parent", nil
	}, nil)
	bus.On("Child", "on_child", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		return "child", nil
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	parent := bus.Emit(bubus.NewBaseEvent("Parent", nil))
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
