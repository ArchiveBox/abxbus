package abxbus_test

import (
	"context"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestWaitUntilIdleTimeoutAndRecovery(t *testing.T) {
	bus := abxbus.NewEventBus("IdleTimeoutBus", nil)
	release := make(chan struct{})
	bus.On("Evt", "slow", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		<-release
		return nil, nil
	}, nil)
	_ = bus.Emit(abxbus.NewBaseEvent("Evt", nil))

	tShort := 0.01
	if bus.WaitUntilIdle(&tShort) {
		close(release)
		t.Fatal("expected false due to in-flight work")
	}
	close(release)
	tLong := 1.0
	if !bus.WaitUntilIdle(&tLong) {
		t.Fatal("expected true after releasing handler")
	}
}

func TestIsIdleAndQueueEmptyStates(t *testing.T) {
	bus := abxbus.NewEventBus("IdleStateBus", nil)
	if !bus.IsIdleAndQueueEmpty() {
		t.Fatal("new bus should be idle and queue-empty")
	}

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	bus.On("Evt", "slow", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		started <- struct{}{}
		<-release
		return nil, nil
	}, nil)
	_ = bus.Emit(abxbus.NewBaseEvent("Evt", nil))

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for handler start")
	}
	if bus.IsIdleAndQueueEmpty() {
		t.Fatal("bus should not be idle while work is pending/running")
	}

	close(release)
	tWait := 1.0
	if !bus.WaitUntilIdle(&tWait) {
		t.Fatal("bus should become idle")
	}
	if !bus.IsIdleAndQueueEmpty() {
		t.Fatal("bus should be idle/queue-empty after completion")
	}
}
