package abxbus_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func assertJSONKeyBefore(t *testing.T, data []byte, firstKey string, secondKey string) {
	t.Helper()
	firstNeedle := append(append([]byte{'"'}, []byte(firstKey)...), []byte{'"', ':'}...)
	secondNeedle := append(append([]byte{'"'}, []byte(secondKey)...), []byte{'"', ':'}...)
	firstIndex := bytes.Index(data, firstNeedle)
	secondIndex := bytes.Index(data, secondNeedle)
	if firstIndex < 0 || secondIndex < 0 {
		t.Fatalf("expected JSON keys %q and %q in payload: %s", firstKey, secondKey, string(data))
	}
	if firstIndex > secondIndex {
		t.Fatalf("expected JSON key %q before %q in payload: %s", firstKey, secondKey, string(data))
	}
}

func TestEventBusSerializationRoundtripPreservesConfigHandlersHistory(t *testing.T) {
	maxHistory := 5
	eventTimeout := 2.5
	eventSlowTimeout := 0.75
	handlerSlowTimeout := 0.33
	bus := abxbus.NewEventBus("SerBus", &abxbus.EventBusOptions{
		ID:                      "serbus-1234",
		MaxHistorySize:          &maxHistory,
		MaxHistoryDrop:          true,
		EventConcurrency:        abxbus.EventConcurrencyParallel,
		EventTimeout:            &eventTimeout,
		EventSlowTimeout:        &eventSlowTimeout,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
		EventHandlerCompletion:  abxbus.EventHandlerCompletionAll,
		EventHandlerSlowTimeout: &handlerSlowTimeout,
	})
	h := bus.On("Evt", "h", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) { return "ok", nil }, nil)
	e := bus.Emit(abxbus.NewBaseEvent("Evt", map[string]any{"k": "v"}))
	if _, err := e.Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	data, err := bus.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var payload abxbus.EventBusJSON
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
	if payload.EventConcurrency != abxbus.EventConcurrencyParallel || payload.EventHandlerConcurrency != abxbus.EventHandlerConcurrencyParallel {
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

	restored, err := abxbus.EventBusFromJSON(data)
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

	restored.On("Evt2", "h2", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) { return "ok2", nil }, nil)
	v, err := restored.Emit(abxbus.NewBaseEvent("Evt2", nil)).EventResult(context.Background())
	if err != nil || v != "ok2" {
		t.Fatalf("restored bus should remain functional, result=%#v err=%v", v, err)
	}
}

func TestEventBusSerializationPreservesUnboundedHistoryNull(t *testing.T) {
	unlimitedHistorySize := abxbus.UnlimitedHistorySize
	bus := abxbus.NewEventBus("UnlimitedSerBus", &abxbus.EventBusOptions{
		MaxHistorySize: &unlimitedHistorySize,
		MaxHistoryDrop: false,
	})
	data, err := bus.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte(`"max_history_size":null`)) {
		t.Fatalf("expected max_history_size to serialize as null: %s", string(data))
	}

	var payload abxbus.EventBusJSON
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.MaxHistorySize != nil {
		t.Fatalf("expected unmarshaled max_history_size null, got %#v", payload.MaxHistorySize)
	}

	restored, err := abxbus.EventBusFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	if restored.EventHistory.MaxHistorySize != nil {
		t.Fatalf("expected restored history to remain unbounded, got %#v", restored.EventHistory.MaxHistorySize)
	}
}

func TestEventBusFromJSONPreservesNullEventTimeout(t *testing.T) {
	data := []byte(`{"id":"timeout-null-bus","name":"TimeoutNullBus","max_history_size":100,"max_history_drop":false,"event_concurrency":"bus-serial","event_timeout":null,"event_slow_timeout":null,"event_handler_concurrency":"serial","event_handler_completion":"all","event_handler_slow_timeout":null,"event_handler_detect_file_paths":false,"handlers":{},"handlers_by_key":{},"event_history":{},"pending_event_queue":[]}`)
	restored, err := abxbus.EventBusFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	if restored.EventTimeout != nil {
		t.Fatalf("JSON event_timeout:null should disable the bus timeout, got %#v", restored.EventTimeout)
	}
	roundtripped, err := restored.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(roundtripped, []byte(`"event_timeout":null`)) {
		t.Fatalf("expected event_timeout null to survive roundtrip: %s", string(roundtripped))
	}
}

func TestEventBusSerializationPreservesHandlerRegistrationOrderThroughJSONAndRestore(t *testing.T) {
	detectPaths := false
	bus := abxbus.NewEventBus("HandlerOrderSourceBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency:     abxbus.EventHandlerConcurrencySerial,
		EventHandlerCompletion:      abxbus.EventHandlerCompletionAll,
		EventHandlerDetectFilePaths: &detectPaths,
	})
	originalOrder := []string{}

	first := bus.On("HandlerOrderEvent", "first", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		originalOrder = append(originalOrder, "first")
		return "first", nil
	}, nil)
	second := bus.On("HandlerOrderEvent", "second", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		originalOrder = append(originalOrder, "second")
		return "second", nil
	}, nil)
	expectedIDs := []string{first.ID, second.ID}

	data, err := bus.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	assertJSONKeyBefore(t, data, first.ID, second.ID)
	var payload abxbus.EventBusJSON
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatal(err)
	}
	if got := payload.HandlersByKey["HandlerOrderEvent"]; len(got) != 2 || got[0] != expectedIDs[0] || got[1] != expectedIDs[1] {
		t.Fatalf("handlers_by_key order mismatch: got %v want %v", got, expectedIDs)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := bus.Emit(abxbus.NewBaseEvent("HandlerOrderEvent", nil)).Done(ctx); err != nil {
		t.Fatal(err)
	}
	if len(originalOrder) != 2 || originalOrder[0] != "first" || originalOrder[1] != "second" {
		t.Fatalf("handler execution order mismatch before restore: got %v", originalOrder)
	}

	restored, err := abxbus.EventBusFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	restoredData, err := restored.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	assertJSONKeyBefore(t, restoredData, first.ID, second.ID)
	var restoredPayload abxbus.EventBusJSON
	if err := json.Unmarshal(restoredData, &restoredPayload); err != nil {
		t.Fatal(err)
	}
	if got := restoredPayload.HandlersByKey["HandlerOrderEvent"]; len(got) != 2 || got[0] != expectedIDs[0] || got[1] != expectedIDs[1] {
		t.Fatalf("restored handlers_by_key order mismatch: got %v want %v", got, expectedIDs)
	}

	restoredOrder := []string{}
	restored.On("HandlerOrderEvent", "first", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		restoredOrder = append(restoredOrder, "first")
		return "first", nil
	}, payload.Handlers[first.ID])
	restored.On("HandlerOrderEvent", "second", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		restoredOrder = append(restoredOrder, "second")
		return "second", nil
	}, payload.Handlers[second.ID])

	restoredData, err = restored.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	assertJSONKeyBefore(t, restoredData, first.ID, second.ID)
	if err := json.Unmarshal(restoredData, &restoredPayload); err != nil {
		t.Fatal(err)
	}
	if got := restoredPayload.HandlersByKey["HandlerOrderEvent"]; len(got) != 2 || got[0] != expectedIDs[0] || got[1] != expectedIDs[1] {
		t.Fatalf("reattached handlers_by_key order mismatch: got %v want %v", got, expectedIDs)
	}

	restoredCtx, restoredCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer restoredCancel()
	if _, err := restored.Emit(abxbus.NewBaseEvent("HandlerOrderEvent", nil)).Done(restoredCtx); err != nil {
		t.Fatal(err)
	}
	if len(restoredOrder) != 2 || restoredOrder[0] != "first" || restoredOrder[1] != "second" {
		t.Fatalf("handler execution order mismatch after restore: got %v", restoredOrder)
	}
}

func TestEventBusSerializationPreservesPendingQueueIDs(t *testing.T) {
	bus := abxbus.NewEventBus("PendingSerBus", &abxbus.EventBusOptions{EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial})
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	bus.On("BlockedEvt", "block", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		started <- struct{}{}
		<-release
		return "done", nil
	}, nil)

	first := bus.Emit(abxbus.NewBaseEvent("BlockedEvt", nil))
	<-started
	second := bus.Emit(abxbus.NewBaseEvent("BlockedEvt", nil))

	data, err := bus.ToJSON()
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	var payload abxbus.EventBusJSON
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

	restored, err := abxbus.EventBusFromJSON(data)
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	restoredData, err := restored.ToJSON()
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	var restoredPayload abxbus.EventBusJSON
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

func TestEventBusFromJSONPreservesEventHistoryObjectOrder(t *testing.T) {
	bus := abxbus.NewEventBus("HistoryOrderBus", nil)
	first := abxbus.NewBaseEvent("HistoryOrderEvent", map[string]any{"label": "first"})
	second := abxbus.NewBaseEvent("HistoryOrderEvent", map[string]any{"label": "second"})
	bus.EventHistory.AddEvent(first)
	bus.EventHistory.AddEvent(second)

	data, err := bus.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	assertJSONKeyBefore(t, data, first.EventID, second.EventID)

	restored, err := abxbus.EventBusFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	restoredData, err := restored.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	assertJSONKeyBefore(t, restoredData, first.EventID, second.EventID)
}
