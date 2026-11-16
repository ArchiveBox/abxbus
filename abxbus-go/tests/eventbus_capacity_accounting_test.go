package abxbus_test

import (
	"context"
	"fmt"
	abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"
	"strings"
	"testing"
)

func TestCapacityDoesNotDoubleCountQueuedEvents(t *testing.T) {
	for _, limit := range []int{50, 100, 150} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			bus := abxbus.NewEventBus("CapacityAccounting", &abxbus.EventBusOptions{MaxHistorySize: &limit})
			release := make(chan struct{})
			defer bus.Destroy()
			bus.On("CapacityEvent", "blocked_handler", func(_ *abxbus.BaseEvent, ctx context.Context) (any, error) {
				select {
				case <-release:
					return nil, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}, nil)
			events := make([]*abxbus.BaseEvent, 0, limit)
			for i := 0; i < limit; i++ {
				events = append(events, bus.Emit(abxbus.NewBaseEvent("CapacityEvent", nil)))
			}
			if bus.EventHistory.Size() != limit {
				t.Fatalf("history count=%d, want %d", bus.EventHistory.Size(), limit)
			}
			func() {
				defer func() {
					got := recover()
					if !strings.Contains(fmt.Sprint(got), fmt.Sprintf("history limit reached (%d/%d)", limit, limit)) {
						t.Fatalf("unexpected capacity error: %v", got)
					}
				}()
				bus.Emit(abxbus.NewBaseEvent("CapacityEvent", nil))
			}()
			if bus.EventHistory.Size() != limit {
				t.Fatal("rejected event changed history")
			}
			close(release)
			timeout := 5.0
			if !bus.WaitUntilIdle(&timeout) {
				t.Fatal("accepted events did not finish")
			}
			for _, event := range events {
				if event.EventStatus != "completed" {
					t.Fatal("accepted event did not complete")
				}
			}
		})
	}
}
