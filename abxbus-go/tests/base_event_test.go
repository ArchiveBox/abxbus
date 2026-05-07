package abxbus_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestBaseEventDoneWithoutBus(t *testing.T) {
	e := abxbus.NewBaseEvent("NoBus", nil)
	if _, err := e.Done(context.Background()); err == nil {
		t.Fatal("expected error")
	}
}

func TestBaseEventDoneAllowsCompletedRestoredEventWithoutBus(t *testing.T) {
	raw := []byte(`{
		"event_type": "RestoredCompletedEvent",
		"event_version": "0.0.1",
		"event_timeout": null,
		"event_slow_timeout": null,
		"event_concurrency": null,
		"event_handler_timeout": null,
		"event_handler_slow_timeout": null,
		"event_handler_concurrency": null,
		"event_handler_completion": null,
		"event_blocks_parent_completion": false,
		"event_result_type": null,
		"event_id": "00000000-0000-5000-8000-000000000101",
		"event_path": [],
		"event_parent_id": null,
		"event_emitted_by_handler_id": null,
		"event_pending_bus_count": 0,
		"event_created_at": "2026-01-01T00:00:00.000Z",
		"event_status": "completed",
		"event_started_at": "2026-01-01T00:00:00.001Z",
		"event_completed_at": "2026-01-01T00:00:00.002Z",
		"event_results": {}
	}`)

	event, err := abxbus.BaseEventFromJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := event.Done(context.Background()); err != nil {
		t.Fatalf("completed restored event should not require a live bus: %v", err)
	}
}

func TestBaseEventJSONFlattenedPayload(t *testing.T) {
	e := abxbus.NewBaseEvent("JSONEvent", map[string]any{"x": 1})
	data, err := e.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil {
		t.Fatal(err)
	}
	if _, ok := obj["payload"]; ok {
		t.Fatal("payload must be flattened")
	}
	if obj["x"].(float64) != 1 {
		t.Fatal("payload key x missing")
	}
	if _, ok := obj["event_id"]; !ok {
		t.Fatal("missing event_id")
	}
}

func TestEventCompletedWaitsInQueueOrderInsideHandler(t *testing.T) {
	bus := abxbus.NewEventBus("QueueOrderEventCompletedBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyParallel,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	order := make([]string, 0, 6)
	orderCh := make(chan string, 8)
	var child *abxbus.BaseEvent

	record := func(label string) {
		orderCh <- label
	}

	bus.On("Parent", "parent", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		record("parent_start")
		bus.Emit(abxbus.NewBaseEvent("Sibling", nil))
		child = e.Emit(abxbus.NewBaseEvent("Child", nil))
		if err := child.EventCompleted(ctx); err != nil {
			return nil, err
		}
		record("parent_end")
		return "parent", nil
	}, nil)
	bus.On("Child", "child", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		record("child_start")
		time.Sleep(time.Millisecond)
		record("child_end")
		return "child", nil
	}, nil)
	bus.On("Sibling", "sibling", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		record("sibling_start")
		time.Sleep(time.Millisecond)
		record("sibling_end")
		return "sibling", nil
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	parent := bus.Emit(abxbus.NewBaseEvent("Parent", nil))
	if _, err := parent.Done(ctx); err != nil {
		t.Fatal(err)
	}
	waitTimeout := 2.0
	if !bus.WaitUntilIdle(&waitTimeout) {
		t.Fatal("timed out waiting for bus to become idle")
	}
	close(orderCh)
	for label := range orderCh {
		order = append(order, label)
	}

	if baseEventIndexOf(order, "sibling_start") >= baseEventIndexOf(order, "child_start") {
		t.Fatalf("event_completed should wait in queue order, got %#v", order)
	}
	if baseEventIndexOf(order, "child_end") >= baseEventIndexOf(order, "parent_end") {
		t.Fatalf("parent should wait for child completion, got %#v", order)
	}
	if child == nil {
		t.Fatal("expected child event")
	}
	if child.EventBlocksParentCompletion {
		t.Fatalf("event_completed should not queue-jump or mark child as parent-blocking")
	}
}

func baseEventIndexOf(values []string, needle string) int {
	for idx, value := range values {
		if value == needle {
			return idx
		}
	}
	return len(values)
}
