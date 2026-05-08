package abxbus_test

import (
	"context"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func mustJSON(t *testing.T, event *abxbus.BaseEvent) []byte {
	t.Helper()
	data, err := event.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestBaseEventRuntimeStateTransitionsAndJSON(t *testing.T) {
	bus := abxbus.NewEventBus("RuntimeStateBus", nil)
	bus.On("RuntimeStateEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		if event.EventStatus != "started" {
			t.Fatalf("handler should see started status, got %s", event.EventStatus)
		}
		if event.EventStartedAt == nil {
			t.Fatal("event_started_at should be set before handler runs")
		}
		return "ok", nil
	}, nil)

	event := abxbus.NewBaseEvent("RuntimeStateEvent", nil)
	if event.EventStatus != "pending" {
		t.Fatalf("new event should start pending, got %s", event.EventStatus)
	}
	if event.EventCompletedAt != nil {
		t.Fatal("new event should not have event_completed_at")
	}
	if _, err := bus.Emit(event).Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if event.EventStatus != "completed" {
		t.Fatalf("completed event status mismatch: %s", event.EventStatus)
	}
	if event.EventCompletedAt == nil {
		t.Fatal("completed event should have event_completed_at")
	}

	restored, err := abxbus.BaseEventFromJSON(mustJSON(t, event))
	if err != nil {
		t.Fatal(err)
	}
	if restored.EventStatus != "completed" || restored.EventCompletedAt == nil || len(restored.EventResults) != 1 {
		t.Fatalf("runtime JSON state did not roundtrip: %#v", restored)
	}
}
