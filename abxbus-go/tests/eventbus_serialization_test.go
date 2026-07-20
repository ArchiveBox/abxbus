package abxbus_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"
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
	h := bus.On("Evt", "h", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) { return "ok", nil }, nil)
	e := bus.Emit(abxbus.NewBaseEvent("Evt", map[string]any{"k": "v"}))
	if _, err := e.Now(); err != nil {
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
	if restoredEvent == nil || restoredEvent.EventExtraPayload["k"] != "v" {
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

	restored.On("Evt2", "h2", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) { return "ok2", nil }, nil)
	v, err := restored.Emit(abxbus.NewBaseEvent("Evt2", nil)).EventResult()
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

func TestEventBusFromJSONRebuildsTTLIndexesForRestoredCompletedHistory(t *testing.T) {
	resultSource := abxbus.NewEventBus("RestoreResultTTLBus", &abxbus.EventBusOptions{
		MaxHistorySize: nil,
		EventTTL:       ttlPtr(-1),
		EventResultTTL: ttlPtr(0),
	})
	resultSource.EventHistory.MaxHistorySize = nil
	resultSource.On("RestoreTTLProbeEvent", "handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, nil)
	resultEvent := resultSource.Emit(abxbus.NewBaseEvent("RestoreTTLProbeEvent", nil))
	if _, err := resultEvent.Now(); err != nil {
		t.Fatal(err)
	}
	resultData, err := resultSource.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	resultSource.Destroy()

	restoredResults, err := abxbus.EventBusFromJSON(resultData)
	if err != nil {
		t.Fatal(err)
	}
	restoredResults.EventHistory.MaxHistorySize = nil
	if len(restoredResults.EventHistory.GetEvent(resultEvent.EventID).EventResults) == 0 {
		t.Fatal("restored completed event should keep results before TTL trim")
	}
	if _, err := restoredResults.Emit(abxbus.NewBaseEvent("RestoreTTLTouchEvent", nil)).Now(nil); err != nil {
		t.Fatal(err)
	}
	if restoredResults.EventHistory.GetEvent(resultEvent.EventID) == nil {
		t.Fatal("result TTL should keep the restored event")
	}
	if len(restoredResults.EventHistory.GetEvent(resultEvent.EventID).EventResults) != 0 {
		t.Fatalf("restored result TTL should clear results, got %#v", restoredResults.EventHistory.GetEvent(resultEvent.EventID).EventResults)
	}
	restoredResults.Destroy()

	eventSource := abxbus.NewEventBus("RestoreEventTTLBus", &abxbus.EventBusOptions{
		MaxHistorySize: nil,
		EventTTL:       ttlPtr(0),
	})
	eventSource.EventHistory.MaxHistorySize = nil
	eventSource.On("RestoreTTLProbeEvent", "handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, nil)
	expiredEvent := eventSource.Emit(abxbus.NewBaseEvent("RestoreTTLProbeEvent", nil))
	if _, err := expiredEvent.Now(); err != nil {
		t.Fatal(err)
	}
	expiredData, err := eventSource.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	eventSource.Destroy()

	restoredEvents, err := abxbus.EventBusFromJSON(expiredData)
	if err != nil {
		t.Fatal(err)
	}
	restoredEvents.EventHistory.MaxHistorySize = nil
	if restoredEvents.EventHistory.GetEvent(expiredEvent.EventID) == nil {
		t.Fatal("restored event should be present before TTL trim")
	}
	if _, err := restoredEvents.Emit(abxbus.NewBaseEvent("RestoreTTLTouchEvent", nil)).Now(nil); err != nil {
		t.Fatal(err)
	}
	if restoredEvents.EventHistory.GetEvent(expiredEvent.EventID) != nil {
		t.Fatal("restored event TTL should delete expired event on natural trim")
	}
	restoredEvents.Destroy()
}

func TestEventBusFromJSONNullEventTimeoutUsesDefault(t *testing.T) {
	data := []byte(`{"id":"timeout-null-bus","name":"TimeoutNullBus","max_history_size":100,"max_history_drop":false,"event_concurrency":"bus-serial","event_timeout":null,"event_slow_timeout":null,"event_handler_concurrency":"serial","event_handler_completion":"all","event_handler_slow_timeout":null,"event_handler_detect_file_paths":false,"handlers":{},"handlers_by_key":{},"event_history":{},"pending_event_queue":[]}`)
	restored, err := abxbus.EventBusFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	if restored.EventTimeout == nil || *restored.EventTimeout != 60.0 {
		t.Fatalf("JSON event_timeout:null should use the default bus timeout, got %#v", restored.EventTimeout)
	}
	roundtripped, err := restored.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(roundtripped, []byte(`"event_timeout":60`)) {
		t.Fatalf("expected event_timeout default to survive roundtrip: %s", string(roundtripped))
	}
}

func TestEventBusFromJSONRejectsRestoredTTLsBelowMinusOne(t *testing.T) {
	cases := map[string]string{
		"event_ttl":          `{"id":"bad-ttl-bus","name":"BadTTLBus","max_history_size":100,"max_history_drop":false,"event_concurrency":"bus-serial","event_timeout":60,"event_slow_timeout":null,"event_ttl":-2,"event_handler_concurrency":"serial","event_handler_completion":"all","event_handler_slow_timeout":null,"event_handler_detect_file_paths":false,"handlers":{},"handlers_by_key":{},"event_history":{},"pending_event_queue":[]}`,
		"event_result_ttl":   `{"id":"bad-ttl-bus","name":"BadTTLBus","max_history_size":100,"max_history_drop":false,"event_concurrency":"bus-serial","event_timeout":60,"event_slow_timeout":null,"event_result_ttl":-2,"event_handler_concurrency":"serial","event_handler_completion":"all","event_handler_slow_timeout":null,"event_handler_detect_file_paths":false,"handlers":{},"handlers_by_key":{},"event_history":{},"pending_event_queue":[]}`,
		"handler_result_ttl": `{"id":"bad-ttl-bus","name":"BadTTLBus","max_history_size":100,"max_history_drop":false,"event_concurrency":"bus-serial","event_timeout":60,"event_slow_timeout":null,"event_handler_concurrency":"serial","event_handler_completion":"all","event_handler_slow_timeout":null,"event_handler_detect_file_paths":false,"handlers":{"h":{"id":"h","eventbus_name":"BadTTLBus","eventbus_id":"bad-ttl-bus","event_pattern":"Evt","handler_name":"h","handler_file_path":null,"handler_timeout":null,"handler_slow_timeout":null,"handler_result_ttl":-2,"handler_registered_at":"2025-01-02T03:04:05.000000000Z"}},"handlers_by_key":{"Evt":["h"]},"event_history":{},"pending_event_queue":[]}`,
	}
	for name, raw := range cases {
		if _, err := abxbus.EventBusFromJSON([]byte(raw)); err == nil || !strings.Contains(err.Error(), name) {
			t.Fatalf("expected %s restore error, got %v", name, err)
		}
	}

	data := []byte(`{"id":"bad-event-ttl-bus","name":"BadEventTTLBus","max_history_size":100,"max_history_drop":false,"event_concurrency":"bus-serial","event_timeout":60,"event_slow_timeout":null,"event_handler_concurrency":"serial","event_handler_completion":"all","event_handler_slow_timeout":null,"event_handler_detect_file_paths":false,"handlers":{},"handlers_by_key":{},"event_history":{"evt-1":{"event_id":"evt-1","event_created_at":"2025-01-02T03:04:05.000000000Z","event_type":"Evt","event_version":"0.0.1","event_timeout":null,"event_ttl":-2,"event_status":"completed","event_completed_at":"2025-01-02T03:04:06.000000000Z","event_path":[],"event_pending_bus_count":0,"event_blocks_parent_completion":false,"event_results":{}}},"pending_event_queue":[]}`)
	if _, err := abxbus.EventBusFromJSON(data); err == nil || !strings.Contains(err.Error(), "event_ttl") {
		t.Fatalf("expected restored event_ttl error, got %v", err)
	}

	resultData := []byte(`{"id":"bad-result-ttl-bus","name":"BadResultTTLBus","max_history_size":100,"max_history_drop":false,"event_concurrency":"bus-serial","event_timeout":60,"event_slow_timeout":null,"event_handler_concurrency":"serial","event_handler_completion":"all","event_handler_slow_timeout":null,"event_handler_detect_file_paths":false,"handlers":{},"handlers_by_key":{},"event_history":{"evt-1":{"event_id":"evt-1","event_created_at":"2025-01-02T03:04:05.000000000Z","event_type":"Evt","event_version":"0.0.1","event_timeout":null,"event_status":"completed","event_completed_at":"2025-01-02T03:04:06.000000000Z","event_path":[],"event_pending_bus_count":0,"event_blocks_parent_completion":false,"event_results":{"h":{"id":"r","status":"completed","event_id":"evt-1","handler_id":"h","handler_name":"h","handler_file_path":null,"handler_timeout":null,"handler_slow_timeout":null,"handler_result_ttl":-2,"handler_registered_at":"2025-01-02T03:04:05.000000000Z","handler_event_pattern":"Evt","eventbus_name":"BadResultTTLBus","eventbus_id":"bad-result-ttl-bus","started_at":null,"completed_at":"2025-01-02T03:04:06.000000000Z","result":"ok","error":null,"event_children":[]}}}},"pending_event_queue":[]}`)
	if _, err := abxbus.EventBusFromJSON(resultData); err == nil || !strings.Contains(err.Error(), "handler_result_ttl") {
		t.Fatalf("expected restored handler_result_ttl error, got %v", err)
	}
}

func TestEventBusFromJSONDefaultsMissingHandlerMaps(t *testing.T) {
	data := []byte(`{"id":"missing-handler-maps","name":"MissingHandlerMaps","max_history_size":100,"max_history_drop":false,"event_concurrency":"bus-serial","event_timeout":null,"event_slow_timeout":null,"event_handler_concurrency":"serial","event_handler_completion":"all","event_handler_slow_timeout":null,"event_handler_detect_file_paths":false,"event_history":{},"pending_event_queue":[]}`)
	restored, err := abxbus.EventBusFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	restored.On("Evt", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, nil)
	result, err := restored.Emit(abxbus.NewBaseEvent("Evt", nil)).EventResult()
	if err != nil {
		t.Fatal(err)
	}
	if result != "ok" {
		t.Fatalf("restored bus handler did not run after missing maps were defaulted: %#v", result)
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

	first := bus.On("HandlerOrderEvent", "first", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		originalOrder = append(originalOrder, "first")
		return "first", nil
	}, nil)
	second := bus.On("HandlerOrderEvent", "second", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
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

	if _, err := bus.Emit(abxbus.NewBaseEvent("HandlerOrderEvent", nil)).Now(); err != nil {
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
	restored.On("HandlerOrderEvent", "first", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		restoredOrder = append(restoredOrder, "first")
		return "first", nil
	}, payload.Handlers[first.ID])
	restored.On("HandlerOrderEvent", "second", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
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

	if _, err := restored.Emit(abxbus.NewBaseEvent("HandlerOrderEvent", nil)).Now(); err != nil {
		t.Fatal(err)
	}
	if len(restoredOrder) != 2 || restoredOrder[0] != "first" || restoredOrder[1] != "second" {
		t.Fatalf("handler execution order mismatch after restore: got %v", restoredOrder)
	}
}

func TestEventBusFromJSONRecreatesMissingHandlerEntriesFromEventResultMetadata(t *testing.T) {
	bus := abxbus.NewEventBus("MissingHandlerHydrationBus", nil)
	bus.On("SerializableEvent", "handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, nil)
	event := bus.Emit(abxbus.NewBaseEvent("SerializableEvent", nil))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}

	handlerID := ""
	for _, result := range event.EventResults {
		handlerID = result.HandlerID
		break
	}
	data, err := bus.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatal(err)
	}
	payload["handlers"] = map[string]any{}
	payload["handlers_by_key"] = map[string]any{}
	data, err = json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}

	restored, err := abxbus.EventBusFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	restoredEvent := restored.EventHistory.GetEvent(event.EventID)
	if restoredEvent == nil {
		t.Fatalf("restored event missing")
	}
	restoredData, err := restored.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var restoredPayload abxbus.EventBusJSON
	if err := json.Unmarshal(restoredData, &restoredPayload); err != nil {
		t.Fatal(err)
	}
	if _, ok := restoredPayload.Handlers[handlerID]; !ok {
		t.Fatalf("restored handlers missing hydrated handler %s", handlerID)
	}
	restoredResult := restoredEvent.EventResults[handlerID]
	if restoredResult == nil || restoredResult.Handler == nil || restoredResult.Handler.ID != handlerID {
		t.Fatalf("restored result handler linkage mismatch: %#v", restoredResult)
	}
}

func TestBaseEventFromJSONRoundtripsRuntimeJSONShape(t *testing.T) {
	bus := abxbus.NewEventBus("SerializableBaseEventBus", nil)
	defer bus.Destroy()
	bus.On("SerializableBaseEvent", "handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("SerializableBaseEvent", nil))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	payload, err := event.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := abxbus.BaseEventFromJSON(payload)
	if err != nil {
		t.Fatal(err)
	}
	restoredPayload, err := restored.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restoredPayload, payload) {
		t.Fatalf("BaseEvent JSON roundtrip mismatch:\nrestored=%s\noriginal=%s", restoredPayload, payload)
	}
}

func TestEventBusSerializationPreservesPendingQueueIDs(t *testing.T) {
	bus := abxbus.NewEventBus("PendingSerBus", &abxbus.EventBusOptions{EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial})
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	bus.On("BlockedEvt", "block", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
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

	restoredQueued := restored.EventHistory.GetEvent(second.EventID)
	if restoredQueued == nil {
		close(release)
		t.Fatalf("restored history missing queued event")
	}
	if _, err := restoredQueued.Now(); err != nil {
		close(release)
		t.Fatalf("restored queued event should still be processable: %v", err)
	}

	close(release)
	if _, err := first.Now(); err != nil {
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
