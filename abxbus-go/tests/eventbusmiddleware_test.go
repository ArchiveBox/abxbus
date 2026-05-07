package abxbus_test

import (
	"context"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

type recordingMiddleware struct {
	eventStatuses  []string
	resultStatuses []string
}

func (m *recordingMiddleware) OnEventChange(bus *abxbus.EventBus, event *abxbus.BaseEvent, status string) {
	m.eventStatuses = append(m.eventStatuses, status)
}

func (m *recordingMiddleware) OnEventResultChange(bus *abxbus.EventBus, event *abxbus.BaseEvent, result *abxbus.EventResult, status string) {
	m.resultStatuses = append(m.resultStatuses, status)
}

func TestEventBusMiddlewareReceivesEventAndResultLifecycleHooks(t *testing.T) {
	middleware := &recordingMiddleware{}
	bus := abxbus.NewEventBus("MiddlewareBus", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})
	bus.On("MiddlewareEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, nil)

	if _, err := bus.Emit(abxbus.NewBaseEvent("MiddlewareEvent", nil)).Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(middleware.eventStatuses) != 2 || middleware.eventStatuses[0] != "started" || middleware.eventStatuses[1] != "completed" {
		t.Fatalf("unexpected event middleware statuses: %#v", middleware.eventStatuses)
	}
	if len(middleware.resultStatuses) != 2 || middleware.resultStatuses[0] != "started" || middleware.resultStatuses[1] != "completed" {
		t.Fatalf("unexpected result middleware statuses: %#v", middleware.resultStatuses)
	}
}
