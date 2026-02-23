package bubus_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestEventBusSerializationRoundtripPreservesConfigHandlersHistory(t *testing.T) {
	maxHistory := 5
	eventTimeout := 2.5
	eventSlowTimeout := 0.75
	handlerSlowTimeout := 0.33
	bus := bubus.NewEventBus("SerBus", &bubus.EventBusOptions{
		ID:                      "serbus-1234",
		MaxHistorySize:          &maxHistory,
		MaxHistoryDrop:          true,
		EventConcurrency:        bubus.EventConcurrencyParallel,
		EventTimeout:            &eventTimeout,
		EventSlowTimeout:        &eventSlowTimeout,
		EventHandlerConcurrency: bubus.EventHandlerConcurrencyParallel,
		EventHandlerCompletion:  bubus.EventHandlerCompletionAll,
		EventHandlerSlowTimeout: &handlerSlowTimeout,
	})
	h := bus.On("Evt", "h", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return "ok", nil }, nil)
	e := bus.Emit(bubus.NewBaseEvent("Evt", map[string]any{"k": "v"}))
	if _, err := e.Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	data, err := bus.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var payload bubus.EventBusJSON
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.ID != bus.ID || payload.Name != bus.Name {
		t.Fatalf("id/name mismatch in json payload: %#v", payload)
	}
	if payload.MaxHistorySize == nil || *payload.MaxHistorySize != maxHistory {
		t.Fatalf("max_history_size mismatch in json payload: %#v", payload.MaxHistorySize)
	}
	if !payload.MaxHistoryDrop {
		t.Fatalf("expected max_history_drop=true in json payload")
	}
	if payload.EventConcurrency != bubus.EventConcurrencyParallel || payload.EventHandlerConcurrency != bubus.EventHandlerConcurrencyParallel {
		t.Fatalf("concurrency fields mismatch in payload")
	}
	if len(payload.Handlers) != 1 || payload.Handlers[h.ID] == nil {
		t.Fatalf("handler map mismatch in payload: %#v", payload.Handlers)
	}
	if len(payload.HandlersByKey["Evt"]) != 1 || payload.HandlersByKey["Evt"][0] != h.ID {
		t.Fatalf("handlers_by_key mismatch in payload: %#v", payload.HandlersByKey)
	}
	if payload.EventHistory[e.EventID] == nil {
		t.Fatalf("event history missing emitted event id=%s", e.EventID)
	}

	restored, err := bubus.EventBusFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	if restored.ID != bus.ID || restored.Name != bus.Name {
		t.Fatalf("id/name mismatch after roundtrip")
	}
	if restored.EventTimeout == nil || *restored.EventTimeout != eventTimeout {
		t.Fatalf("event timeout mismatch after roundtrip")
	}
	if restored.EventSlowTimeout == nil || *restored.EventSlowTimeout != eventSlowTimeout {
		t.Fatalf("event slow timeout mismatch after roundtrip")
	}
	if restored.EventHandlerSlowTimeout == nil || *restored.EventHandlerSlowTimeout != handlerSlowTimeout {
		t.Fatalf("handler slow timeout mismatch after roundtrip")
	}
	if restored.EventHistory.Size() != 1 {
		t.Fatalf("expected one history entry after roundtrip, got %d", restored.EventHistory.Size())
	}
	restoredEvent := restored.EventHistory.GetEvent(e.EventID)
	if restoredEvent == nil || restoredEvent.Payload["k"] != "v" {
		t.Fatalf("restored history payload mismatch")
	}
	if len(restoredEvent.EventResults) != 1 {
		t.Fatalf("expected one restored event result, got %d", len(restoredEvent.EventResults))
	}
	for _, result := range restoredEvent.EventResults {
		if result.Handler == nil {
			t.Fatalf("restored event result should reference restored handler object")
		}
		if result.HandlerID != result.Handler.ID {
			t.Fatalf("restored handler linkage mismatch")
		}
	}
	if !restored.IsIdleAndQueueEmpty() {
		t.Fatalf("restored idle bus should start with clean runtime state")
	}

	restored.On("Evt2", "h2", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return "ok2", nil }, nil)
	v, err := restored.Emit(bubus.NewBaseEvent("Evt2", nil)).EventResult(context.Background())
	if err != nil || v != "ok2" {
		t.Fatalf("restored bus should remain functional, result=%#v err=%v", v, err)
	}
}

func TestEventBusSerializationPreservesPendingQueueIDs(t *testing.T) {
	bus := bubus.NewEventBus("PendingSerBus", &bubus.EventBusOptions{EventHandlerConcurrency: bubus.EventHandlerConcurrencySerial})
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	bus.On("BlockedEvt", "block", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		started <- struct{}{}
		<-release
		return "done", nil
	}, nil)

	first := bus.Emit(bubus.NewBaseEvent("BlockedEvt", nil))
	<-started
	second := bus.Emit(bubus.NewBaseEvent("BlockedEvt", nil))

	data, err := bus.ToJSON()
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	var payload bubus.EventBusJSON
	if err := json.Unmarshal(data, &payload); err != nil {
		close(release)
		t.Fatal(err)
	}
	if len(payload.PendingEventQueue) == 0 {
		close(release)
		t.Fatalf("expected at least one pending event id in serialization payload")
	}
	foundSecond := false
	for _, eventID := range payload.PendingEventQueue {
		if eventID == second.EventID {
			foundSecond = true
			break
		}
	}
	if !foundSecond {
		close(release)
		t.Fatalf("expected queued second event id in pending_event_queue, got %v", payload.PendingEventQueue)
	}

	restored, err := bubus.EventBusFromJSON(data)
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	restoredData, err := restored.ToJSON()
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	var restoredPayload bubus.EventBusJSON
	if err := json.Unmarshal(restoredData, &restoredPayload); err != nil {
		close(release)
		t.Fatal(err)
	}
	foundSecondAfterRestore := false
	for _, eventID := range restoredPayload.PendingEventQueue {
		if eventID == second.EventID {
			foundSecondAfterRestore = true
			break
		}
	}
	if !foundSecondAfterRestore {
		close(release)
		t.Fatalf("restored pending_event_queue missing second event id")
	}

	timedCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	restoredQueued := restored.EventHistory.GetEvent(second.EventID)
	if restoredQueued == nil {
		close(release)
		t.Fatalf("restored history missing queued event")
	}
	if _, err := restoredQueued.Done(timedCtx); err != nil {
		close(release)
		t.Fatalf("restored queued event should still be processable: %v", err)
	}

	close(release)
	if _, err := first.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
}
