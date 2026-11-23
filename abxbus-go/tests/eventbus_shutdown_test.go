package abxbus_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"
)

func TestDestroyReleasesIdleWaitAndStaysTerminal(t *testing.T) {
	for _, clear := range []bool{false, true} {
		t.Run(fmt.Sprintf("clear=%t", clear), func(t *testing.T) {
			bus := abxbus.NewEventBus("ShutdownIdleWait", nil)
			started := make(chan struct{})
			release := make(chan struct{})
			defer close(release)
			defer bus.Destroy()
			bus.On("Slow", "blocked", func(_ *abxbus.BaseEvent, _ context.Context) (any, error) {
				close(started)
				<-release
				return nil, nil
			}, nil)
			bus.Emit(abxbus.NewBaseEvent("Slow", nil))
			bus.Emit(abxbus.NewBaseEvent("Queued", nil))
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("handler never started")
			}
			waiting := make(chan bool, 1)
			entered := make(chan struct{})
			go func() {
				close(entered)
				waiting <- bus.WaitUntilIdle(nil)
			}()
			<-entered
			select {
			case <-waiting:
				t.Fatal("idle wait returned with active work")
			case <-time.After(20 * time.Millisecond):
			}
			bus.DestroyWithOptions(&abxbus.EventBusDestroyOptions{Clear: clear})
			select {
			case idle := <-waiting:
				if !idle {
					t.Fatal("destroy should release existing idle wait")
				}
			case <-time.After(time.Second):
				t.Fatal("destroy stranded idle waiter")
			}
			for name, operation := range map[string]func(){
				"emit":      func() { bus.Emit(abxbus.NewBaseEvent("Late", nil)) },
				"idle wait": func() { bus.WaitUntilIdle(nil) },
			} {
				func() {
					defer func() {
						got := recover()
						err, ok := got.(error)
						if !ok || !errors.Is(err, abxbus.ErrEventBusDestroyed) {
							t.Errorf("%s: expected destroyed error, got %v", name, got)
						}
					}()
					operation()
				}()
			}
			if !bus.IsDestroyed() {
				t.Fatal("destroyed bus restarted")
			}
			serialized, err := bus.ToJSON()
			if err != nil {
				t.Fatal(err)
			}
			var state struct {
				PendingEventQueue []string `json:"pending_event_queue"`
			}
			if err := json.Unmarshal(serialized, &state); err != nil {
				t.Fatal(err)
			}
			if len(state.PendingEventQueue) != 0 {
				t.Fatal("destroyed bus retained queued work")
			}
		})
	}
}
