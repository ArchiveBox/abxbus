package bubus_test

import (
	"context"
	"testing"
	"time"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestWaitUntilIdleTimeoutAndRecovery(t *testing.T) {
	bus := bubus.NewEventBus("IdleTimeoutBus", nil)
	release := make(chan struct{})
	bus.On("Evt", "slow", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		<-release
		return nil, nil
	}, nil)
	_ = bus.Emit(bubus.NewBaseEvent("Evt", nil))

	t_short := 0.01
	if bus.WaitUntilIdle(&t_short) {
		close(release)
		t.Fatal("expected false due to in-flight work")
	}
	close(release)
	t_long := 1.0
	if !bus.WaitUntilIdle(&t_long) {
		t.Fatal("expected true after releasing handler")
	}
}

func TestIsIdleAndQueueEmptyStates(t *testing.T) {
	bus := bubus.NewEventBus("IdleStateBus", nil)
	if !bus.IsIdleAndQueueEmpty() {
		t.Fatal("new bus should be idle and queue-empty")
	}
	bus.On("Evt", "slow", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		time.Sleep(20 * time.Millisecond)
		return nil, nil
	}, nil)
	_ = bus.Emit(bubus.NewBaseEvent("Evt", nil))
	if bus.IsIdleAndQueueEmpty() {
		t.Fatal("bus should not be idle while work is pending/running")
	}
	t_wait := 1.0
	if !bus.WaitUntilIdle(&t_wait) {
		t.Fatal("bus should become idle")
	}
	if !bus.IsIdleAndQueueEmpty() {
		t.Fatal("bus should be idle/queue-empty after completion")
	}
}
