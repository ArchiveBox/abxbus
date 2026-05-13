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

func TestEventResetCreatesFreshPendingEventForCrossBusDispatch(t *testing.T) {
	busA := abxbus.NewEventBus("ResetCoverageBusA", nil)
	busB := abxbus.NewEventBus("ResetCoverageBusB", nil)
	seenA := []string{}
	seenB := []string{}

	busA.On("ResetCoverageEvent", "record_a", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		if label, ok := event.Payload["label"].(string); ok {
			seenA = append(seenA, label)
		}
		return "a:" + event.Payload["label"].(string), nil
	}, nil)
	busB.On("ResetCoverageEvent", "record_b", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		if label, ok := event.Payload["label"].(string); ok {
			seenB = append(seenB, label)
		}
		return "b:" + event.Payload["label"].(string), nil
	}, nil)

	completed := busA.Emit(abxbus.NewBaseEvent("ResetCoverageEvent", map[string]any{"label": "hello"}))
	if _, err := completed.Now(); err != nil {
		t.Fatal(err)
	}
	if completed.EventStatus != "completed" || len(completed.EventResults) != 1 {
		t.Fatalf("expected completed event with one result, got status=%s results=%d", completed.EventStatus, len(completed.EventResults))
	}

	fresh, err := completed.EventReset()
	if err != nil {
		t.Fatal(err)
	}
	if fresh.EventID == completed.EventID {
		t.Fatal("reset event should have a fresh event_id")
	}
	if fresh.EventStatus != "pending" || fresh.EventStartedAt != nil || fresh.EventCompletedAt != nil {
		t.Fatalf("reset event should be pending with no lifecycle timestamps: %#v", fresh)
	}
	if len(fresh.EventResults) != 0 || fresh.EventPendingBusCount != 0 || fresh.Bus != nil {
		t.Fatalf("reset event should clear runtime state: %#v", fresh)
	}

	forwarded := busB.Emit(fresh)
	if _, err := forwarded.Now(); err != nil {
		t.Fatal(err)
	}
	if forwarded.EventStatus != "completed" {
		t.Fatalf("expected forwarded reset event to complete, got %s", forwarded.EventStatus)
	}
	if len(seenA) != 1 || seenA[0] != "hello" || len(seenB) != 1 || seenB[0] != "hello" {
		t.Fatalf("unexpected handler observations: seenA=%#v seenB=%#v", seenA, seenB)
	}
	hasBusA := false
	hasBusB := false
	for _, entry := range forwarded.EventPath {
		if len(entry) >= len("ResetCoverageBusA#") && entry[:len("ResetCoverageBusA#")] == "ResetCoverageBusA#" {
			hasBusA = true
		}
		if len(entry) >= len("ResetCoverageBusB#") && entry[:len("ResetCoverageBusB#")] == "ResetCoverageBusB#" {
			hasBusB = true
		}
	}
	if !hasBusA || !hasBusB {
		t.Fatalf("reset event should preserve previous path and append new bus path: %#v", forwarded.EventPath)
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
