package abxbus_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestEventResultOptions(t *testing.T) {
	bus := abxbus.NewEventBus("ResultsListBus", nil)
	bus.On("ListEvent", "ok", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) { return "ok", nil }, nil)
	bus.On("ListEvent", "nil", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) { return nil, nil }, nil)
	bus.On("ListEvent", "err", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) { return nil, errors.New("boom") }, nil)

	e := bus.Emit(abxbus.NewBaseEvent("ListEvent", nil))
	if _, err := e.Now(); err != nil {
		t.Fatal(err)
	}

	if _, err := e.EventResultsList(nil); err == nil || err.Error() != "boom" {
		t.Fatalf("default options should raise first handler error, got %v", err)
	}
	if _, err := e.EventResultsList(&abxbus.EventResultOptions{RaiseIfAny: true, RaiseIfNone: false}); err == nil || err.Error() != "boom" {
		t.Fatalf("RaiseIfAny=true should surface boom, got %v", err)
	}

	vals, err := e.EventResultsList(&abxbus.EventResultOptions{RaiseIfAny: false, RaiseIfNone: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 1 || vals[0] != "ok" {
		t.Fatalf("unexpected values for non-raising mode: %#v", vals)
	}

	onlyNil, err := e.EventResultsList(&abxbus.EventResultOptions{Include: func(result any, eventResult *abxbus.EventResult) bool {
		return result == nil
	}, RaiseIfAny: false, RaiseIfNone: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(onlyNil) != 2 {
		t.Fatalf("expected include predicate to capture nil results from nil+error handlers, got %#v", onlyNil)
	}

	if _, err := e.EventResultsList(&abxbus.EventResultOptions{Include: func(result any, eventResult *abxbus.EventResult) bool {
		return false
	}, RaiseIfAny: false, RaiseIfNone: true}); err == nil {
		t.Fatal("RaiseIfNone=true should fail when include filter rejects all results")
	}

	slowBus := abxbus.NewEventBus("ResultsListTimeoutBus", nil)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	slowBus.On("SlowListEvent", "slow", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		started <- struct{}{}
		<-release
		return "late", nil
	}, nil)
	slow := slowBus.Emit(abxbus.NewBaseEvent("SlowListEvent", nil))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("timed out waiting for slow handler start")
	}
	tiny := 0.01
	if _, err := slow.Now(&abxbus.EventWaitOptions{Timeout: &tiny}); err == nil {
		close(release)
		t.Fatal("expected timeout error from Now with timeout option")
	}
	close(release)
	if _, err := slow.Now(); err != nil {
		t.Fatal(err)
	}
}

func TestAllErrorResultOptionsMatchCrossLanguageMatrix(t *testing.T) {
	bus := abxbus.NewEventBus("AllErrorResultOptionsBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	defer bus.Destroy()

	bus.On("AllErrorResultOptionsEvent", "first", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return nil, errors.New("first failure")
	}, nil)
	bus.On("AllErrorResultOptionsEvent", "second", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return nil, errors.New("second failure")
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("AllErrorResultOptionsEvent", nil))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}

	if _, err := event.EventResult(); err == nil || !strings.Contains(err.Error(), "first failure") {
		t.Fatalf("default EventResult should surface handler errors, got %v", err)
	}
	if _, err := event.EventResultsList(); err == nil || !strings.Contains(err.Error(), "first failure") {
		t.Fatalf("default EventResultsList should surface handler errors, got %v", err)
	}

	value, err := event.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false, RaiseIfNone: false})
	if err != nil || value != nil {
		t.Fatalf("false/false EventResult should return nil without error, got %#v err=%v", value, err)
	}
	values, err := event.EventResultsList(&abxbus.EventResultOptions{RaiseIfAny: false, RaiseIfNone: false})
	if err != nil || len(values) != 0 {
		t.Fatalf("false/false EventResultsList should return empty values without error, got %#v err=%v", values, err)
	}

	if _, err := event.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false, RaiseIfNone: true}); err == nil || !strings.Contains(err.Error(), "no valid handler results") {
		t.Fatalf("false/true EventResult should raise no-result error, got %v", err)
	}
	if _, err := event.EventResultsList(&abxbus.EventResultOptions{RaiseIfAny: false, RaiseIfNone: true}); err == nil || !strings.Contains(err.Error(), "no valid handler results") {
		t.Fatalf("false/true EventResultsList should raise no-result error, got %v", err)
	}

	if _, err := event.EventResult(&abxbus.EventResultOptions{RaiseIfAny: true, RaiseIfNone: false}); err == nil || !strings.Contains(err.Error(), "first failure") {
		t.Fatalf("true/false EventResult should surface handler errors, got %v", err)
	}
	if _, err := event.EventResultsList(&abxbus.EventResultOptions{RaiseIfAny: true, RaiseIfNone: false}); err == nil || !strings.Contains(err.Error(), "first failure") {
		t.Fatalf("true/false EventResultsList should surface handler errors, got %v", err)
	}

	if _, err := event.EventResult(&abxbus.EventResultOptions{RaiseIfAny: true, RaiseIfNone: true}); err == nil || !strings.Contains(err.Error(), "first failure") {
		t.Fatalf("true/true EventResult should surface handler errors, got %v", err)
	}
	if _, err := event.EventResultsList(&abxbus.EventResultOptions{RaiseIfAny: true, RaiseIfNone: true}); err == nil || !strings.Contains(err.Error(), "first failure") {
		t.Fatalf("true/true EventResultsList should surface handler errors, got %v", err)
	}
}

func TestEventResultsListAndFirstUseHandlerRegistrationOrder(t *testing.T) {
	bus := abxbus.NewEventBus("ResultsListOrderBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	bus.On("OrderResultEvent", "null", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return nil, nil
	}, &abxbus.EventHandler{ID: "m-null"})
	bus.On("OrderResultEvent", "winner", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "winner", nil
	}, &abxbus.EventHandler{ID: "z-winner"})
	bus.On("OrderResultEvent", "late", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "late", nil
	}, &abxbus.EventHandler{ID: "a-late"})

	e := bus.Emit(abxbus.NewBaseEvent("OrderResultEvent", nil))
	first, err := e.EventResult()
	if err != nil {
		t.Fatal(err)
	}
	if first != "winner" {
		t.Fatalf("expected EventResult to return first non-nil result in registration order, got %#v", first)
	}

	values, err := e.EventResultsList(&abxbus.EventResultOptions{RaiseIfAny: false, RaiseIfNone: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || values[0] != "winner" || values[1] != "late" {
		t.Fatalf("expected filtered values in registration order, got %#v", values)
	}

	rawValues, err := e.EventResultsList(&abxbus.EventResultOptions{Include: func(result any, eventResult *abxbus.EventResult) bool {
		return true
	}, RaiseIfAny: false, RaiseIfNone: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(rawValues) != 3 || rawValues[0] != nil || rawValues[1] != "winner" || rawValues[2] != "late" {
		t.Fatalf("expected raw values in registration order, got %#v", rawValues)
	}
}

func TestEventResultsListPreservesJSONEventResultsObjectOrder(t *testing.T) {
	nullID := "00000000-0000-5000-8000-00000000000b"
	winnerID := "00000000-0000-5000-8000-00000000000c"
	lateID := "00000000-0000-5000-8000-00000000000a"
	raw := []byte(fmt.Sprintf(`{
		"event_type": "RestoredResultOrderEvent",
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
		"event_id": "00000000-0000-5000-8000-000000000201",
		"event_path": [],
		"event_parent_id": null,
		"event_emitted_by_handler_id": null,
		"event_pending_bus_count": 0,
		"event_created_at": "2026-01-01T00:00:00.000Z",
		"event_status": "completed",
		"event_started_at": "2026-01-01T00:00:00.001Z",
		"event_completed_at": "2026-01-01T00:00:00.002Z",
		"event_results": {
			%q: %s,
			%q: %s,
			%q: %s
		}
	}`, nullID, restoredEventResultJSON(nullID, "null", "null"), winnerID, restoredEventResultJSON(winnerID, "winner", `"winner"`), lateID, restoredEventResultJSON(lateID, "late", `"late"`)))

	event, err := abxbus.BaseEventFromJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	first, err := event.EventResult()
	if err != nil {
		t.Fatal(err)
	}
	if first != "winner" {
		t.Fatalf("expected first non-nil result to follow JSON object/registration order, got %#v", first)
	}

	values, err := event.EventResultsList(&abxbus.EventResultOptions{Include: func(result any, eventResult *abxbus.EventResult) bool {
		return true
	}, RaiseIfAny: false, RaiseIfNone: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 3 || values[0] != nil || values[1] != "winner" || values[2] != "late" {
		t.Fatalf("expected restored raw values in JSON object order, got %#v", values)
	}

	serialized, err := event.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	serializedText := string(serialized)
	nullIndex := strings.Index(serializedText, `"`+nullID+`"`)
	winnerIndex := strings.Index(serializedText, `"`+winnerID+`"`)
	lateIndex := strings.Index(serializedText, `"`+lateID+`"`)
	if nullIndex < 0 || winnerIndex < 0 || lateIndex < 0 {
		t.Fatalf("serialized event missing result IDs: %s", serializedText)
	}
	if !(nullIndex < winnerIndex && winnerIndex < lateIndex) {
		t.Fatalf("serialized event_results should preserve restored order: %s", serializedText)
	}
}

func restoredEventResultJSON(handlerID string, name string, resultJSON string) string {
	return fmt.Sprintf(`{
		"id": "result-%s",
		"status": "completed",
		"event_id": "00000000-0000-5000-8000-000000000201",
		"handler_id": %q,
		"handler_name": %q,
		"handler_file_path": null,
		"handler_timeout": null,
		"handler_slow_timeout": null,
		"handler_registered_at": "2026-01-01T00:00:00.000Z",
		"handler_event_pattern": "RestoredResultOrderEvent",
		"eventbus_name": "RestoredResultOrderBus",
		"eventbus_id": "00000000-0000-5000-8000-000000000202",
		"started_at": "2026-01-01T00:00:00.001Z",
		"completed_at": "2026-01-01T00:00:00.002Z",
		"result": %s,
		"error": null,
		"event_children": []
	}`, handlerID, handlerID, name, resultJSON)
}
