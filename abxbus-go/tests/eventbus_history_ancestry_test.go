package abxbus_test

import (
	"context"
	abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"
	"testing"
)

func TestHistoryPressurePreservesCompletedParentOfPendingChild(t *testing.T) {
	limit := 2
	bus := abxbus.NewEventBus("HistoryAncestry", &abxbus.EventBusOptions{MaxHistorySize: &limit, MaxHistoryDrop: true})
	release := make(chan struct{})
	defer bus.Destroy()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	bus.On("HeldChild", "hold", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) { <-release; return "ok", nil }, nil)
	parent := bus.Emit(abxbus.NewBaseEvent("Parent", nil))
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.Emit(abxbus.NewBaseEvent("Extra", nil)).Now(); err != nil {
		t.Fatal(err)
	}
	child := abxbus.NewBaseEvent("HeldChild", nil)
	child.EventParentID = &parent.EventID
	bus.Emit(child)
	for i := 0; i < 8; i++ {
		bus.Emit(abxbus.NewBaseEvent("Extra", nil))
	}
	if !bus.EventHistory.Has(parent.EventID) || !bus.EventHistory.Has(child.EventID) {
		t.Fatal("history pressure removed pending child's ancestry")
	}
	close(release)
	if _, err := child.Now(); err != nil {
		t.Fatal(err)
	}
	timeout := 2.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("bus did not finish admitted work")
	}
	if bus.EventHistory.Size() > limit {
		t.Fatal("completed history was not trimmed")
	}
}
