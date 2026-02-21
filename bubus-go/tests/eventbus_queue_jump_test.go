package bubus_test

import (
	"context"
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestQueueJumpProcessesChildInsideParentHandler(t *testing.T) {
	bus := bubus.NewEventBus("QueueJumpBus", &bubus.EventBusOptions{
		EventHandlerConcurrency: bubus.EventHandlerConcurrencySerial,
	})
	child_processed_before_parent_return := false
	bus.On("Parent", "on_parent", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		child := bus.Emit(bubus.NewBaseEvent("Child", nil))
		if _, err := child.Done(ctx); err != nil {
			return nil, err
		}
		if child.EventStatus == "completed" {
			child_processed_before_parent_return = true
		}
		return "parent", nil
	}, nil)
	bus.On("Child", "on_child", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		return "child", nil
	}, nil)

	parent := bus.Emit(bubus.NewBaseEvent("Parent", nil))
	_, err := parent.Done(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !child_processed_before_parent_return {
		t.Fatal("expected child queue-jump processing to complete inside parent handler")
	}
}
