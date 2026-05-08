package abxbus_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestBaseEventDoneWithoutBus(t *testing.T) {
	e := abxbus.NewBaseEvent("NoBus", nil)
	if _, err := e.Done(context.Background()); err == nil || !strings.Contains(err.Error(), "no bus attached") {
		t.Fatalf("expected missing bus error, got %v", err)
	}
	if e.EventStatus != "pending" {
		t.Fatalf("Done without a bus should not mutate event status, got %s", e.EventStatus)
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

func TestBaseEventEventResultUpdateCreatesAndUpdatesTypedHandlerResults(t *testing.T) {
	bus := abxbus.NewEventBus("BaseEventEventResultUpdateBus", nil)
	event := abxbus.NewBaseEvent("BaseEventEventResultUpdateEvent", nil)
	event.EventResultType = map[string]any{"type": "string"}
	handlerEntry := bus.On("BaseEventEventResultUpdateEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, nil)

	pending := event.EventResultUpdate(handlerEntry, &abxbus.BaseEventResultUpdateOptions{
		EventBus: bus,
		EventResultUpdateOptions: abxbus.EventResultUpdateOptions{
			Status: abxbus.EventResultPending,
		},
	})
	if event.EventResults[handlerEntry.ID] != pending {
		t.Fatal("event_result_update should store the pending result by handler id")
	}
	if pending.Status != abxbus.EventResultPending {
		t.Fatalf("expected pending result, got %s", pending.Status)
	}

	completed := event.EventResultUpdate(handlerEntry, &abxbus.BaseEventResultUpdateOptions{
		EventBus: bus,
		EventResultUpdateOptions: abxbus.EventResultUpdateOptions{
			Status: abxbus.EventResultCompleted,
			Result: "seeded",
		},
	})
	if completed != pending {
		t.Fatal("event_result_update should update the existing handler result")
	}
	if completed.Status != abxbus.EventResultCompleted || completed.Result != "seeded" {
		t.Fatalf("expected completed seeded result, got status=%s result=%#v", completed.Status, completed.Result)
	}
	if completed.StartedAt == nil || completed.CompletedAt == nil {
		t.Fatalf("completed update should set started_at and completed_at: %#v", completed)
	}
}

func TestBaseEventEventResultUpdateStatusOnlyPreservesExistingErrorAndResult(t *testing.T) {
	bus := abxbus.NewEventBus("BaseEventEventResultUpdateStatusOnlyBus", nil)
	event := abxbus.NewBaseEvent("BaseEventEventResultUpdateStatusOnlyEvent", nil)
	event.EventResultType = map[string]any{"type": "string"}
	handlerEntry := bus.On("BaseEventEventResultUpdateStatusOnlyEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, nil)

	errored := event.EventResultUpdate(handlerEntry, &abxbus.BaseEventResultUpdateOptions{
		EventBus: bus,
		EventResultUpdateOptions: abxbus.EventResultUpdateOptions{
			Error: errors.New("seeded error"),
		},
	})
	if errored.Status != abxbus.EventResultError || errored.Error != "seeded error" {
		t.Fatalf("expected seeded error result, got status=%s error=%#v", errored.Status, errored.Error)
	}

	statusOnly := event.EventResultUpdate(handlerEntry, &abxbus.BaseEventResultUpdateOptions{
		EventBus: bus,
		EventResultUpdateOptions: abxbus.EventResultUpdateOptions{
			Status: abxbus.EventResultPending,
		},
	})
	if statusOnly.Status != abxbus.EventResultPending {
		t.Fatalf("expected status-only update to set pending, got %s", statusOnly.Status)
	}
	if statusOnly.Error != "seeded error" {
		t.Fatalf("status-only update should preserve existing error, got %#v", statusOnly.Error)
	}
	if statusOnly.Result != nil {
		t.Fatalf("status-only update should not synthesize a result, got %#v", statusOnly.Result)
	}
}

func TestBaseEventEventResultUpdateValidatesDeclaredResultSchema(t *testing.T) {
	bus := abxbus.NewEventBus("BaseEventEventResultUpdateSchemaBus", nil)
	event := abxbus.NewBaseEvent("BaseEventEventResultUpdateSchemaEvent", nil)
	event.EventResultType = map[string]any{"type": "string"}
	handlerEntry := bus.On("BaseEventEventResultUpdateSchemaEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, nil)

	result := event.EventResultUpdate(handlerEntry, &abxbus.BaseEventResultUpdateOptions{
		EventBus: bus,
		EventResultUpdateOptions: abxbus.EventResultUpdateOptions{
			Result: 123,
		},
	})
	if result.Status != abxbus.EventResultError {
		t.Fatalf("invalid seeded result should mark handler error, got %s", result.Status)
	}
	if !strings.Contains(result.Error.(string), "EventHandlerResultSchemaError") {
		t.Fatalf("expected schema error, got %#v", result.Error)
	}
	if result.Result != nil {
		t.Fatalf("invalid seeded result should not be stored, got %#v", result.Result)
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
