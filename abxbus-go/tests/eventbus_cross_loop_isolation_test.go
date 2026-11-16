package abxbus_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"
)

// Go has one scheduler rather than loop-affine queues. Preserve the equivalent
// contract: awaiting one event must not drain unrelated bus queues.
func TestNowDoesNotDrainUnrelatedBus(t *testing.T) {
	bus := abxbus.NewEventBus("AwaitingBus", nil)
	other := abxbus.NewEventBus("UnrelatedBus", nil)
	defer bus.Destroy()
	defer other.Destroy()
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var seen atomic.Int32
	var ownSeen atomic.Int32
	bus.On("Own", "own", func(_ *abxbus.BaseEvent, _ context.Context) (any, error) {
		ownSeen.Add(1)
		return nil, nil
	}, nil)
	other.On("Blocker", "blocker", func(_ *abxbus.BaseEvent, _ context.Context) (any, error) {
		close(started)
		<-release
		return nil, nil
	}, nil)
	other.On("Probe", "probe", func(_ *abxbus.BaseEvent, _ context.Context) (any, error) {
		seen.Add(1)
		return nil, nil
	}, nil)
	other.Emit(abxbus.NewBaseEvent("Blocker", nil))
	queued := other.Emit(abxbus.NewBaseEvent("Probe", nil))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("blocker never started")
	}
	timeout := 2.0
	if _, err := bus.Emit(abxbus.NewBaseEvent("Own", nil)).Now(&abxbus.EventWaitOptions{Timeout: &timeout}); err != nil {
		t.Fatal(err)
	}
	if seen.Load() != 0 {
		t.Fatal("now drained unrelated queue")
	}
	if ownSeen.Load() != 1 {
		t.Fatal("own handler should run exactly once")
	}
	unblock()
	if _, err := queued.Wait(&abxbus.EventWaitOptions{Timeout: &timeout}); err != nil {
		t.Fatal(err)
	}
	if seen.Load() != 1 {
		t.Fatal("probe should run exactly once")
	}
}
