package bubus_test

import (
	"context"
	"testing"
	"time"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestGlobalSerialAcrossBuses(t *testing.T) {
	b1 := bubus.NewEventBus("B1", &bubus.EventBusOptions{EventConcurrency: bubus.EventConcurrencyGlobalSerial})
	b2 := bubus.NewEventBus("B2", &bubus.EventBusOptions{EventConcurrency: bubus.EventConcurrencyGlobalSerial})
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	h := func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		started <- struct{}{}
		<-release
		return nil, nil
	}
	b1.On("Evt", "h1", h, nil)
	b2.On("Evt", "h2", h, nil)
	_ = b1.Emit(bubus.NewBaseEvent("Evt", nil))
	_ = b2.Emit(bubus.NewBaseEvent("Evt", nil))
	<-started
	select {
	case <-started:
		t.Fatal("global serial should not start both immediately")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
}
