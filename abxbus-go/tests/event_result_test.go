package abxbus_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestEventResultJSONRoundtrip(t *testing.T) {
	bus := abxbus.NewEventBus("ResultBus", nil)
	h := abxbus.NewEventHandler(bus.Name, bus.ID, "Evt", "h", nil)
	e := abxbus.NewBaseEvent("Evt", nil)
	r := abxbus.NewEventResult(e, h)
	r.Status = abxbus.EventResultCompleted
	r.Result = "ok"
	r.Error = "boom"
	now := "2026-02-21T00:00:00.000000000Z"
	r.StartedAt = &now
	r.CompletedAt = &now

	data, err := r.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	round, err := abxbus.EventResultFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	if round.ID != r.ID {
		t.Fatalf("id mismatch: %s vs %s", round.ID, r.ID)
	}
	if round.Status != r.Status {
		t.Fatalf("status mismatch: %s vs %s", round.Status, r.Status)
	}
	if round.HandlerID != h.ID || round.EventID != e.EventID {
		t.Fatal("handler/event ID roundtrip mismatch")
	}
	if round.HandlerName != h.HandlerName || round.EventBusName != h.EventBusName || round.EventBusID != h.EventBusID {
		t.Fatal("handler/event bus metadata mismatch")
	}
	if round.Result != "ok" || round.Error != "boom" {
		t.Fatalf("result/error mismatch after roundtrip: %#v %#v", round.Result, round.Error)
	}
	if round.StartedAt == nil || *round.StartedAt != now || round.CompletedAt == nil || *round.CompletedAt != now {
		t.Fatalf("timestamp mismatch after roundtrip: started=%#v completed=%#v", round.StartedAt, round.CompletedAt)
	}
}

func TestEventResultWaitBlocksUntilSettled(t *testing.T) {
	bus := abxbus.NewEventBus("ResultBus", nil)
	h := abxbus.NewEventHandler(bus.Name, bus.ID, "Evt", "h", nil)
	e := abxbus.NewBaseEvent("Evt", nil)
	r := abxbus.NewEventResult(e, h)

	done := make(chan error, 1)
	go func() { done <- r.Wait() }()
	select {
	case err := <-done:
		t.Fatalf("wait should block while result is pending, got %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	r.Update(&abxbus.EventResultUpdateOptions{Status: abxbus.EventResultCompleted})
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for completed result")
	}
}

func TestEventResultUnmarshalResetsWaitStateForPendingResults(t *testing.T) {
	completed := []byte(`{
		"id":"result-1",
		"status":"completed",
		"event_id":"event-1",
		"handler_id":"handler-1",
		"handler_name":"handler",
		"handler_file_path":null,
		"handler_timeout":null,
		"handler_slow_timeout":null,
		"handler_registered_at":"2026-02-21T00:00:00.000000000Z",
		"handler_event_pattern":"Evt",
		"eventbus_name":"Bus",
		"eventbus_id":"bus-1",
		"started_at":null,
		"completed_at":"2026-02-21T00:00:00.000000000Z",
		"result":"ok",
		"error":null,
		"event_children":[]
	}`)
	pending := []byte(`{
		"id":"result-2",
		"status":"pending",
		"event_id":"event-2",
		"handler_id":"handler-2",
		"handler_name":"handler",
		"handler_file_path":null,
		"handler_timeout":null,
		"handler_slow_timeout":null,
		"handler_registered_at":"2026-02-21T00:00:00.000000000Z",
		"handler_event_pattern":"Evt",
		"eventbus_name":"Bus",
		"eventbus_id":"bus-1",
		"started_at":null,
		"completed_at":null,
		"result":null,
		"error":null,
		"event_children":[]
	}`)

	var result abxbus.EventResult
	if err := result.UnmarshalJSON(completed); err != nil {
		t.Fatal(err)
	}
	if err := result.Wait(); err != nil {
		t.Fatalf("completed result should wait immediately: %v", err)
	}
	if err := result.UnmarshalJSON(pending); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- result.Wait() }()
	select {
	case err := <-done:
		t.Fatalf("pending result should not inherit the closed wait channel from a previous unmarshal, got %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	result.Update(&abxbus.EventResultUpdateOptions{Status: abxbus.EventResultCompleted})
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for completed result")
	}
}

func TestEventResultUpdateKeepsConsistentOrderingSemanticsForStatusResultError(t *testing.T) {
	bus := abxbus.NewEventBus("StandaloneResultUpdateBus", nil)
	handler := abxbus.NewEventHandler(bus.Name, bus.ID, "StandaloneEvent", "handler", nil)
	event := abxbus.NewBaseEvent("StandaloneEvent", nil)
	result := abxbus.NewEventResult(event, handler)
	result.Error = "RuntimeError: existing"

	result.Update(&abxbus.EventResultUpdateOptions{Status: abxbus.EventResultCompleted})
	if result.Status != abxbus.EventResultCompleted {
		t.Fatalf("expected completed status, got %s", result.Status)
	}
	if result.Error != "RuntimeError: existing" {
		t.Fatalf("status-only update should preserve existing error, got %#v", result.Error)
	}

	result.Update(&abxbus.EventResultUpdateOptions{
		Status: abxbus.EventResultError,
		Result: "seeded",
	})
	if result.Result != "seeded" {
		t.Fatalf("result update should preserve seeded result, got %#v", result.Result)
	}
	if result.Status != abxbus.EventResultError {
		t.Fatalf("explicit status should apply after result, got %s", result.Status)
	}
}

func TestEventResultAllErrorOptionsContract(t *testing.T) {
	bus := abxbus.NewEventBus("AllErrorResultOptionsBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	defer bus.Destroy()

	bus.On("AllErrorResultOptionsEvent", "first", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, errors.New("first failure")
	}, nil)
	bus.On("AllErrorResultOptionsEvent", "second", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, errors.New("second failure")
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("AllErrorResultOptionsEvent", nil))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}

	if _, err := event.EventResult(); err == nil || !strings.Contains(err.Error(), "first failure") || !strings.Contains(err.Error(), "second failure") {
		t.Fatalf("default EventResult should surface handler errors, got %v", err)
	}
	if _, err := event.EventResultsList(); err == nil || !strings.Contains(err.Error(), "first failure") || !strings.Contains(err.Error(), "second failure") {
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

	if _, err := event.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false, RaiseIfNone: true}); err == nil || !strings.Contains(err.Error(), "Expected at least one handler") {
		t.Fatalf("false/true EventResult should raise no-result error, got %v", err)
	}
	if _, err := event.EventResultsList(&abxbus.EventResultOptions{RaiseIfAny: false, RaiseIfNone: true}); err == nil || !strings.Contains(err.Error(), "Expected at least one handler") {
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

func TestEventResultDefaultOptionsContract(t *testing.T) {
	errorBus := abxbus.NewEventBus("EventResultDefaultErrorOptionsBus", nil)
	errorBus.On("DefaultErrorOptionsEvent", "boom", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, errors.New("default boom")
	}, nil)

	errorEvent := errorBus.Emit(abxbus.NewBaseEvent("DefaultErrorOptionsEvent", nil))
	if _, err := errorEvent.Now(); err != nil {
		t.Fatal(err)
	}
	if _, err := errorEvent.EventResult(); err == nil || err.Error() != "default boom" {
		t.Fatalf("nil options should raise handler error by default, got %v", err)
	}
	if _, err := errorEvent.EventResultsList(); err == nil || err.Error() != "default boom" {
		t.Fatalf("nil list options should raise handler error by default, got %v", err)
	}
	if _, err := errorEvent.EventResult(&abxbus.EventResultOptions{}); err == nil || err.Error() != "default boom" {
		t.Fatalf("zero-value options should match nil defaults, got %v", err)
	}
	if _, err := errorEvent.EventResult(&abxbus.EventResultOptions{RaiseIfNone: true}); err == nil || err.Error() != "default boom" {
		t.Fatalf("omitted RaiseIfAny should keep default true, got %v", err)
	}
	if value, err := errorEvent.EventResult(&abxbus.EventResultOptions{RaiseIfAny: false}); err != nil || value != nil {
		t.Fatalf("explicit RaiseIfAny=false should suppress handler errors, value=%#v err=%v", value, err)
	}
	values, err := errorEvent.EventResultsList(&abxbus.EventResultOptions{RaiseIfAny: false})
	if err != nil || len(values) != 0 {
		t.Fatalf("explicit RaiseIfAny=false should suppress list handler errors, values=%#v err=%v", values, err)
	}
	errorBus.Destroy()

	emptyBus := abxbus.NewEventBus("EventResultDefaultNoneOptionsBus", nil)
	emptyEvent := emptyBus.Emit(abxbus.NewBaseEvent("DefaultNoneOptionsEvent", nil))
	if _, err := emptyEvent.Now(); err != nil {
		t.Fatal(err)
	}
	if value, err := emptyEvent.EventResult(); err != nil || value != nil {
		t.Fatalf("default RaiseIfNone=false should return nil result without error, value=%#v err=%v", value, err)
	}
	if values, err := emptyEvent.EventResultsList(); err != nil || len(values) != 0 {
		t.Fatalf("default RaiseIfNone=false should return empty list without error, values=%#v err=%v", values, err)
	}
	if _, err := emptyEvent.EventResult(&abxbus.EventResultOptions{RaiseIfNone: true}); err == nil || !strings.Contains(err.Error(), "Expected at least one handler") {
		t.Fatalf("RaiseIfNone=true should reject empty results, got %v", err)
	}
	if _, err := emptyEvent.EventResultsList(&abxbus.EventResultOptions{RaiseIfNone: true}); err == nil || !strings.Contains(err.Error(), "Expected at least one handler") {
		t.Fatalf("RaiseIfNone=true should reject empty result list, got %v", err)
	}
	emptyBus.Destroy()
}

func TestEventResultErrorShapesUseSingleExceptionOrGroup(t *testing.T) {
	bus := abxbus.NewEventBus("EventResultErrorShapeBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	bus.On("SingleErrorEvent", "single", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, errors.New("single shape failure")
	}, nil)
	bus.On("MultiErrorEvent", "first", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, errors.New("first shape failure")
	}, nil)
	bus.On("MultiErrorEvent", "second", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return nil, errors.New("second shape failure")
	}, nil)

	single := bus.Emit(abxbus.NewBaseEvent("SingleErrorEvent", nil))
	if _, err := single.Now(); err != nil {
		t.Fatal(err)
	}
	_, err := single.EventResult()
	if err == nil || err.Error() != "single shape failure" {
		t.Fatalf("single handler failure should return original error shape, got %T %v", err, err)
	}
	var singleHandlerErrors *abxbus.EventHandlerErrors
	if errors.As(err, &singleHandlerErrors) {
		t.Fatalf("single handler failure should not use EventHandlerErrors, got %#v", singleHandlerErrors)
	}

	event := bus.Emit(abxbus.NewBaseEvent("MultiErrorEvent", nil))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	_, err = event.EventResult()
	var handlerErrors *abxbus.EventHandlerErrors
	if !errors.As(err, &handlerErrors) {
		t.Fatalf("multi-handler failure should expose EventHandlerErrors, got %T %v", err, err)
	}
	if handlerErrors.EventType != "MultiErrorEvent" || handlerErrors.EventID != event.EventID || len(handlerErrors.Errors) != 2 {
		t.Fatalf("unexpected EventHandlerErrors payload: %#v", handlerErrors)
	}
	if !strings.Contains(err.Error(), "Event MultiErrorEvent#") ||
		!strings.Contains(err.Error(), "had 2 handler error(s)") ||
		!strings.Contains(err.Error(), "first shape failure") ||
		!strings.Contains(err.Error(), "second shape failure") {
		t.Fatalf("unexpected multi-error shape: %v", err)
	}

	emptyBus := abxbus.NewEventBus("EventResultNoneShapeBus", nil)
	empty := emptyBus.Emit(abxbus.NewBaseEvent("EmptyResultEvent", nil))
	if _, err := empty.Now(); err != nil {
		t.Fatal(err)
	}
	_, err = empty.EventResultsList(&abxbus.EventResultOptions{RaiseIfNone: true})
	var noneErr *abxbus.EventResultNoneError
	if !errors.As(err, &noneErr) {
		t.Fatalf("empty included results should expose EventResultNoneError, got %T %v", err, err)
	}
	if noneErr.EventType != "EmptyResultEvent" || noneErr.EventID != empty.EventID {
		t.Fatalf("unexpected EventResultNoneError payload: %#v", noneErr)
	}
	if !strings.Contains(err.Error(), "Expected at least one handler to return a non-null result") ||
		!strings.Contains(err.Error(), "EmptyResultEvent#") {
		t.Fatalf("unexpected none-error shape: %v", err)
	}
}

func TestEventResultOptions(t *testing.T) {
	bus := abxbus.NewEventBus("ResultsListBus", nil)
	bus.On("ListEvent", "ok", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) { return "ok", nil }, nil)
	bus.On("ListEvent", "nil", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) { return nil, nil }, nil)
	bus.On("ListEvent", "err", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) { return nil, errors.New("boom") }, nil)

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
	slowBus.On("SlowListEvent", "slow", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
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

func TestEventResultAndResultsListUseRegistrationOrderForCurrentResultSubset(t *testing.T) {
	bus := abxbus.NewEventBus("ResultsListOrderBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	completedOrder := make(chan string, 3)
	bus.On("OrderResultEvent", "null", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(30 * time.Millisecond)
		completedOrder <- "null"
		return nil, nil
	}, &abxbus.EventHandler{ID: "m-null"})
	bus.On("OrderResultEvent", "winner", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		time.Sleep(20 * time.Millisecond)
		completedOrder <- "winner"
		return "winner", nil
	}, &abxbus.EventHandler{ID: "z-winner"})
	bus.On("OrderResultEvent", "late", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		completedOrder <- "late"
		return "late", nil
	}, &abxbus.EventHandler{ID: "a-late"})

	e := bus.Emit(abxbus.NewBaseEvent("OrderResultEvent", nil))
	if _, err := e.Now(); err != nil {
		t.Fatal(err)
	}
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
	if got := []string{<-completedOrder, <-completedOrder, <-completedOrder}; !reflect.DeepEqual(got, []string{"late", "winner", "null"}) {
		t.Fatalf("expected completion order to differ from registration order, got %#v", got)
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

// Folded from event_result_handler_metadata_test.go to keep test layout class-based.
func TestEventResultSerializesHandlerMetadataAndDerivedFields(t *testing.T) {
	bus := abxbus.NewEventBus("ResultMetadataBus", nil)
	timeout := 1.5
	slowTimeout := 0.25
	handler := bus.On("MetadataEvent", "metadata_handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, &abxbus.EventHandler{
		HandlerTimeout:     &timeout,
		HandlerSlowTimeout: &slowTimeout,
	})

	event := bus.Emit(abxbus.NewBaseEvent("MetadataEvent", nil))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	result := event.EventResults[handler.ID]
	data, err := result.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"handler_id", "handler_name", "handler_timeout", "handler_slow_timeout", "handler_event_pattern", "eventbus_name", "eventbus_id"} {
		if _, ok := payload[key]; !ok {
			t.Fatalf("missing EventResult metadata key %s in %#v", key, payload)
		}
	}
	if payload["handler_id"] != handler.ID || payload["handler_name"] != "metadata_handler" || payload["handler_event_pattern"] != "MetadataEvent" {
		t.Fatalf("handler metadata mismatch: %#v", payload)
	}
	if _, ok := payload["result_type"]; ok {
		t.Fatalf("EventResult JSON must not duplicate parent event result schema: %#v", payload)
	}
}

// Folded from event_result_typed_results_test.go to keep test layout class-based.
func schemaEvent(eventType string, schema map[string]any) *abxbus.BaseEvent {
	event := abxbus.NewBaseEvent(eventType, nil)
	event.EventResultType = schema
	return event
}

func firstEventResult(event *abxbus.BaseEvent) *abxbus.EventResult {
	for _, result := range event.EventResults {
		return result
	}
	return nil
}

func assertSchemaResult(t *testing.T, name string, schema map[string]any, value any, wantError bool) {
	t.Helper()
	bus := abxbus.NewEventBus(name+"Bus", nil)
	eventType := name + "Event"
	bus.On(eventType, "handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return value, nil
	}, nil)
	event := bus.Emit(schemaEvent(eventType, schema))
	_, err := event.Now()
	if wantError {
		if err != nil {
			t.Fatalf("%s: event completion should collect handler schema errors, got %v", name, err)
		}
		if _, err := event.EventResult(); err == nil || !strings.Contains(err.Error(), "EventHandlerResultSchemaError") {
			t.Fatalf("%s: expected schema error from result accessor, got %v", name, err)
		}
		result := firstEventResult(event)
		if result == nil || result.Status != abxbus.EventResultError {
			t.Fatalf("%s: expected errored result, got %#v", name, result)
		}
		return
	}
	if err != nil {
		t.Fatalf("%s: expected schema to accept result, got %v", name, err)
	}
	result := firstEventResult(event)
	if result == nil || result.Status != abxbus.EventResultCompleted {
		t.Fatalf("%s: expected completed result, got %#v", name, result)
	}
}

func TestTypedResultSchemaValidatesHandlerResult(t *testing.T) {
	bus := abxbus.NewEventBus("TypedResultBus", nil)
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"value": map[string]any{"type": "string"},
			"count": map[string]any{"type": "number"},
		},
		"required": []any{"value", "count"},
	}
	bus.On("TypedResultEvent", "handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return map[string]any{"value": "hello", "count": 42}, nil
	}, nil)

	event := bus.Emit(schemaEvent("TypedResultEvent", schema))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	result := firstEventResult(event)
	if result == nil || result.Status != abxbus.EventResultCompleted {
		t.Fatalf("expected completed result, got %#v", result)
	}
}

func TestBuiltinResultSchemaValidatesHandlerResults(t *testing.T) {
	bus := abxbus.NewEventBus("BuiltinResultSchemaBus", nil)
	bus.On("BuiltinStringResultEvent", "string_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "42", nil
	}, nil)
	bus.On("BuiltinIntResultEvent", "int_handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return 123, nil
	}, nil)

	stringEvent := bus.Emit(schemaEvent("BuiltinStringResultEvent", map[string]any{"type": "string"}))
	if _, err := stringEvent.Now(); err != nil {
		t.Fatal(err)
	}
	stringResult := firstEventResult(stringEvent)
	if stringResult == nil || stringResult.Status != abxbus.EventResultCompleted || stringResult.Result != "42" {
		t.Fatalf("expected completed string result, got %#v", stringResult)
	}

	intEvent := bus.Emit(schemaEvent("BuiltinIntResultEvent", map[string]any{"type": "integer"}))
	if _, err := intEvent.Now(); err != nil {
		t.Fatal(err)
	}
	intResult := firstEventResult(intEvent)
	if intResult == nil || intResult.Status != abxbus.EventResultCompleted || intResult.Result != 123 {
		t.Fatalf("expected completed integer result, got %#v", intResult)
	}
}

func TestInvalidHandlerResultMarksErrorWhenSchemaIsDefined(t *testing.T) {
	bus := abxbus.NewEventBus("InvalidTypedResultBus", nil)
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"value": map[string]any{"type": "string"},
		},
		"required":             []any{"value"},
		"additionalProperties": false,
	}
	bus.On("InvalidTypedResultEvent", "handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return map[string]any{"value": 123, "extra": true}, nil
	}, nil)

	event := bus.Emit(schemaEvent("InvalidTypedResultEvent", schema))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	if _, err := event.EventResult(); err == nil || !strings.Contains(err.Error(), "EventHandlerResultSchemaError") {
		t.Fatalf("expected schema error from result accessor, got %v", err)
	}
	result := firstEventResult(event)
	if result == nil || result.Status != abxbus.EventResultError {
		t.Fatalf("expected errored result, got %#v", result)
	}
}

func TestNoSchemaLeavesRawHandlerResultUntouched(t *testing.T) {
	bus := abxbus.NewEventBus("NoSchemaResultBus", nil)
	raw := map[string]any{"value": 123}
	bus.On("NoSchemaEvent", "handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return raw, nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("NoSchemaEvent", nil))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	result := firstEventResult(event)
	if result == nil || result.Result == nil {
		t.Fatalf("expected raw result, got %#v", result)
	}
	resultMap, ok := result.Result.(map[string]any)
	if !ok || resultMap["value"] != 123 {
		t.Fatalf("raw result changed: %#v", result.Result)
	}
}

func TestComplexResultSchemaValidatesNestedData(t *testing.T) {
	bus := abxbus.NewEventBus("ComplexTypedResultBus", nil)
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"items": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"id":     map[string]any{"type": "integer"},
						"labels": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					},
					"required": []any{"id", "labels"},
				},
			},
		},
		"required": []any{"items"},
	}
	bus.On("ComplexTypedResultEvent", "handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return map[string]any{"items": []any{map[string]any{"id": 1, "labels": []any{"a", "b"}}}}, nil
	}, nil)

	event := bus.Emit(schemaEvent("ComplexTypedResultEvent", schema))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
}

func TestJSONSchemaCombinationKeywordsAreEnforced(t *testing.T) {
	assertSchemaResult(t, "AllOfValid", map[string]any{
		"allOf": []any{
			map[string]any{"type": "object", "required": []any{"kind"}},
			map[string]any{"type": "object", "properties": map[string]any{"kind": map[string]any{"const": "ok"}}},
		},
	}, map[string]any{"kind": "ok"}, false)
	assertSchemaResult(t, "AllOfInvalid", map[string]any{
		"allOf": []any{
			map[string]any{"type": "object", "required": []any{"kind"}},
			map[string]any{"type": "object", "properties": map[string]any{"kind": map[string]any{"const": "ok"}}},
		},
	}, map[string]any{"kind": "bad"}, true)
	assertSchemaResult(t, "OneOfValid", map[string]any{
		"oneOf": []any{
			map[string]any{"type": "string"},
			map[string]any{"type": "integer"},
		},
	}, "ok", false)
	assertSchemaResult(t, "OneOfInvalidAmbiguous", map[string]any{
		"oneOf": []any{
			map[string]any{"type": "number"},
			map[string]any{"type": "integer"},
		},
	}, 7, true)
	assertSchemaResult(t, "NotInvalid", map[string]any{
		"type": "string",
		"not":  map[string]any{"const": "forbidden"},
	}, "forbidden", true)
}

func TestJSONSchemaConstraintKeywordsAreEnforced(t *testing.T) {
	assertSchemaResult(t, "EnumValid", map[string]any{
		"enum": []any{"queued", "done"},
	}, "queued", false)
	assertSchemaResult(t, "EnumInvalid", map[string]any{
		"enum": []any{"queued", "done"},
	}, "other", true)
	assertSchemaResult(t, "StringConstraintsValid", map[string]any{
		"type":      "string",
		"minLength": 3,
		"maxLength": 5,
		"pattern":   "^[a-z]+$",
	}, "abcd", false)
	assertSchemaResult(t, "StringConstraintsInvalid", map[string]any{
		"type":      "string",
		"minLength": 3,
		"maxLength": 5,
		"pattern":   "^[a-z]+$",
	}, "AB", true)
	assertSchemaResult(t, "UnionTypeStillAppliesSiblingConstraints", map[string]any{
		"type":      []any{"string", "null"},
		"minLength": 3,
	}, "ok", true)
	assertSchemaResult(t, "NumericConstraintsValid", map[string]any{
		"type":             "number",
		"minimum":          1,
		"exclusiveMaximum": 10,
		"multipleOf":       0.5,
	}, 4.5, false)
	assertSchemaResult(t, "NumericConstraintsInvalid", map[string]any{
		"type":             "number",
		"minimum":          1,
		"exclusiveMaximum": 10,
		"multipleOf":       0.5,
	}, 10, true)
	assertSchemaResult(t, "ArrayConstraintsValid", map[string]any{
		"type":     "array",
		"minItems": 2,
		"maxItems": 3,
		"items":    map[string]any{"type": "integer"},
	}, []any{1, 2}, false)
	assertSchemaResult(t, "ArrayConstraintsInvalid", map[string]any{
		"type":     "array",
		"minItems": 2,
		"maxItems": 3,
		"items":    map[string]any{"type": "integer"},
	}, []any{1, "two"}, true)
	assertSchemaResult(t, "ObjectConstraintsValid", map[string]any{
		"type":          "object",
		"minProperties": 1,
		"maxProperties": 2,
		"properties":    map[string]any{"id": map[string]any{"type": "integer"}},
	}, map[string]any{"id": 1}, false)
	assertSchemaResult(t, "ObjectConstraintsInvalid", map[string]any{
		"type":          "object",
		"minProperties": 1,
		"maxProperties": 2,
		"properties":    map[string]any{"id": map[string]any{"type": "integer"}},
	}, map[string]any{"id": 1, "name": "a", "extra": true}, true)
}

func TestFromJSONNormalizesEventResultTypeSchemaDraft(t *testing.T) {
	schema := map[string]any{"type": "string"}
	event := schemaEvent("SchemaRoundtripEvent", schema)
	data, err := event.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := abxbus.BaseEventFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	encodedSchema, err := json.Marshal(restored.EventResultType)
	if err != nil {
		t.Fatal(err)
	}
	if string(encodedSchema) != `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string"}` {
		t.Fatalf("unexpected restored schema: %s", string(encodedSchema))
	}
}

func TestFromJSONPreservesCanonicalEventResultTypeSchemaJSON(t *testing.T) {
	data := []byte(`{"event_type":"SchemaRawRoundtripEvent","event_version":"0.0.1","event_timeout":null,"event_slow_timeout":null,"event_concurrency":null,"event_handler_timeout":null,"event_handler_slow_timeout":null,"event_handler_concurrency":null,"event_handler_completion":null,"event_blocks_parent_completion":false,"event_result_type":{"type":"string","$schema":"https://json-schema.org/draft/2020-12/schema"},"event_id":"018f8e40-1234-7000-8000-00000000abcd","event_path":[],"event_parent_id":null,"event_emitted_by_handler_id":null,"event_pending_bus_count":0,"event_created_at":"2026-01-01T00:00:00.000000000Z","event_status":"pending","event_started_at":null,"event_completed_at":null}`)
	event, err := abxbus.BaseEventFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	roundtripped, err := event.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(roundtripped), `"event_result_type":{"type":"string","$schema":"https://json-schema.org/draft/2020-12/schema"}`) {
		t.Fatalf("canonical event_result_type JSON order changed: %s", string(roundtripped))
	}
}

func TestFromJSONPreservesRawEventResultTypeSchemaWithoutDraft(t *testing.T) {
	data := []byte(`{"event_type":"SchemaRawNoDraftRoundtripEvent","event_version":"0.0.1","event_timeout":null,"event_slow_timeout":null,"event_concurrency":null,"event_handler_timeout":null,"event_handler_slow_timeout":null,"event_handler_concurrency":null,"event_handler_completion":null,"event_blocks_parent_completion":false,"event_result_type":{"type":"array","items":{"type":"string"}},"event_id":"018f8e40-1234-7000-8000-00000000abcd","event_path":[],"event_parent_id":null,"event_emitted_by_handler_id":null,"event_pending_bus_count":0,"event_created_at":"2026-01-01T00:00:00.000000000Z","event_status":"pending","event_started_at":null,"event_completed_at":null}`)
	event, err := abxbus.BaseEventFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	roundtripped, err := event.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(roundtripped), `"event_result_type":{"type":"array","items":{"type":"string"}}`) {
		t.Fatalf("raw event_result_type JSON changed: %s", string(roundtripped))
	}
}

func TestSchemaReferencesAndAnyOfAreEnforced(t *testing.T) {
	bus := abxbus.NewEventBus("SchemaRefBus", nil)
	schema := map[string]any{
		"$defs": map[string]any{
			"Payload": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"value": map[string]any{"anyOf": []any{
						map[string]any{"type": "string"},
						map[string]any{"type": "integer"},
					}},
				},
				"required": []any{"value"},
			},
		},
		"$ref": "#/$defs/Payload",
	}
	bus.On("SchemaRefEvent", "handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return map[string]any{"value": 7}, nil
	}, nil)

	event := bus.Emit(schemaEvent("SchemaRefEvent", schema))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
}

// Folded from typed_events_test.go to keep test layout class-based.
type addPayload struct {
	A int `json:"a"`
	B int `json:"b"`
}

type addResult struct {
	Sum int `json:"sum"`
}

type getConfigEvent struct {
	Url              string
	UserID           string
	EventType        string
	EventTimeout     float64
	EventResultType  map[string]any
	EventConcurrency abxbus.EventConcurrencyMode
}

type DerivedNameEvent struct {
	Url string
}

type namedBaseNoContextHandler func(*abxbus.BaseEvent) error
type namedBaseWithContextHandler func(*abxbus.BaseEvent, context.Context) (any, error)
type namedTypedNoContextHandler func(addPayload) addResult
type namedTypedWithContextHandler func(addPayload, context.Context) (addResult, error)

func TestEmitAcceptsTypedStructAndDerivesPayloadAndConfig(t *testing.T) {
	bus := abxbus.NewEventBus("StructEmitBus", nil)
	event := bus.Emit(getConfigEvent{
		Url:              "https://example.com",
		UserID:           "user-1",
		EventType:        "GetConfigEvent",
		EventTimeout:     30,
		EventResultType:  map[string]any{"type": "object"},
		EventConcurrency: abxbus.EventConcurrencyParallel,
	})

	if event.EventType != "GetConfigEvent" {
		t.Fatalf("event type mismatch: %s", event.EventType)
	}
	if event.EventTimeout == nil || *event.EventTimeout != 30 {
		t.Fatalf("event timeout mismatch: %#v", event.EventTimeout)
	}
	if event.EventConcurrency != abxbus.EventConcurrencyParallel {
		t.Fatalf("event concurrency mismatch: %s", event.EventConcurrency)
	}
	if !reflect.DeepEqual(event.EventResultType, map[string]any{"type": "object"}) {
		t.Fatalf("event result type mismatch: %#v", event.EventResultType)
	}
	if event.Payload["url"] != "https://example.com" || event.Payload["user_id"] != "user-1" {
		t.Fatalf("payload casing mismatch: %#v", event.Payload)
	}
	if _, ok := event.Payload["event_timeout"]; ok {
		t.Fatalf("event config leaked into payload: %#v", event.Payload)
	}

	derived := bus.Emit(DerivedNameEvent{Url: "https://example.org"})
	if derived.EventType != "DerivedNameEvent" {
		t.Fatalf("derived event type mismatch: %s", derived.EventType)
	}
	if derived.Payload["url"] != "https://example.org" {
		t.Fatalf("derived payload mismatch: %#v", derived.Payload)
	}
}

func TestTypedEventPayloadAndResultHelpers(t *testing.T) {
	bus := abxbus.NewEventBus("TypedBus", nil)
	abxbus.OnTyped[addPayload, addResult](bus, "AddEvent", "add", func(payload addPayload) (addResult, error) {
		return addResult{Sum: payload.A + payload.B}, nil
	}, nil)

	event := abxbus.MustNewTypedEventWithResult[addPayload, addResult]("AddEvent", addPayload{A: 4, B: 9})
	result, err := bus.Emit(event).EventResult()
	if err != nil {
		t.Fatal(err)
	}
	typedResult, err := abxbus.EventResultAs[addResult](result)
	if err != nil {
		t.Fatal(err)
	}
	if typedResult.Sum != 13 {
		t.Fatalf("expected typed result sum=13, got %#v", typedResult)
	}

	roundtrippedPayload, err := abxbus.EventPayloadAs[addPayload](event)
	if err != nil {
		t.Fatal(err)
	}
	if roundtrippedPayload != (addPayload{A: 4, B: 9}) {
		t.Fatalf("typed payload roundtrip mismatch: %#v", roundtrippedPayload)
	}
}

func TestOnTypedSupportsOptionalContextHandlerSignatures(t *testing.T) {
	bus := abxbus.NewEventBus("TypedOptionalContextBus", nil)
	abxbus.OnTyped[addPayload, addResult](bus, "TypedNoContextEvent", "typed", func(payload addPayload) addResult {
		return addResult{Sum: payload.A + payload.B}
	}, nil)

	gotCtx := false
	abxbus.OnTyped[addPayload, addResult](bus, "TypedWithContextEvent", "typed", func(payload addPayload, ctx context.Context) (addResult, error) {
		gotCtx = ctx != nil
		return addResult{Sum: payload.A + payload.B}, nil
	}, nil)

	noCtxEvent := bus.Emit(abxbus.MustNewTypedEventWithResult[addPayload, addResult]("TypedNoContextEvent", addPayload{A: 2, B: 3}))
	if _, err := noCtxEvent.Now(); err != nil {
		t.Fatal(err)
	}
	noCtxResult, err := noCtxEvent.EventResult()
	if err != nil {
		t.Fatal(err)
	}
	noCtxTypedResult, err := abxbus.EventResultAs[addResult](noCtxResult)
	if err != nil {
		t.Fatal(err)
	}
	if noCtxTypedResult.Sum != 5 {
		t.Fatalf("expected typed no-context result sum=5, got %#v", noCtxTypedResult)
	}

	ctxEvent := bus.Emit(abxbus.MustNewTypedEventWithResult[addPayload, addResult]("TypedWithContextEvent", addPayload{A: 4, B: 6}))
	if _, err := ctxEvent.Now(); err != nil {
		t.Fatal(err)
	}
	ctxResult, err := ctxEvent.EventResult()
	if err != nil {
		t.Fatal(err)
	}
	ctxTypedResult, err := abxbus.EventResultAs[addResult](ctxResult)
	if err != nil {
		t.Fatal(err)
	}
	if ctxTypedResult.Sum != 10 || !gotCtx {
		t.Fatalf("expected typed context result sum=10 and ctx=true, got result=%#v ctx=%v", ctxTypedResult, gotCtx)
	}
}

func TestEventBusOnSupportsNamedHandlerFunctionTypes(t *testing.T) {
	bus := abxbus.NewEventBus("NamedHandlerFunctionTypesBus", nil)
	gotNoContext := false
	gotContext := false

	bus.On("NamedNoContextHandlerEvent", "named_no_context", namedBaseNoContextHandler(func(event *abxbus.BaseEvent) error {
		gotNoContext = event.EventType == "NamedNoContextHandlerEvent"
		return nil
	}), nil)
	bus.On("NamedWithContextHandlerEvent", "named_with_context", namedBaseWithContextHandler(func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		gotContext = event.EventType == "NamedWithContextHandlerEvent" && ctx != nil
		return "named-ok", nil
	}), nil)

	noContextEvent := bus.Emit(abxbus.NewBaseEvent("NamedNoContextHandlerEvent", nil))
	if _, err := noContextEvent.Now(); err != nil {
		t.Fatal(err)
	}
	if !gotNoContext {
		t.Fatal("expected named no-context handler to run")
	}

	contextEvent := bus.Emit(abxbus.NewBaseEvent("NamedWithContextHandlerEvent", nil))
	if _, err := contextEvent.Now(); err != nil {
		t.Fatal(err)
	}
	contextResult, err := contextEvent.EventResult()
	if err != nil {
		t.Fatal(err)
	}
	if contextResult != "named-ok" || !gotContext {
		t.Fatalf("expected named context handler result and ctx=true, got result=%#v ctx=%v", contextResult, gotContext)
	}
}

func TestOnTypedSupportsNamedHandlerFunctionTypes(t *testing.T) {
	bus := abxbus.NewEventBus("TypedNamedHandlerFunctionTypesBus", nil)
	gotCtx := false

	abxbus.OnTyped[addPayload, addResult](bus, "TypedNamedNoContextEvent", "typed", namedTypedNoContextHandler(func(payload addPayload) addResult {
		return addResult{Sum: payload.A + payload.B}
	}), nil)
	abxbus.OnTyped[addPayload, addResult](bus, "TypedNamedWithContextEvent", "typed", namedTypedWithContextHandler(func(payload addPayload, ctx context.Context) (addResult, error) {
		gotCtx = ctx != nil
		return addResult{Sum: payload.A + payload.B}, nil
	}), nil)

	noContextEvent := bus.Emit(abxbus.MustNewTypedEventWithResult[addPayload, addResult]("TypedNamedNoContextEvent", addPayload{A: 3, B: 4}))
	if _, err := noContextEvent.Now(); err != nil {
		t.Fatal(err)
	}
	noContextResult, err := noContextEvent.EventResult()
	if err != nil {
		t.Fatal(err)
	}
	noContextTypedResult, err := abxbus.EventResultAs[addResult](noContextResult)
	if err != nil {
		t.Fatal(err)
	}
	if noContextTypedResult.Sum != 7 {
		t.Fatalf("expected named typed no-context result sum=7, got %#v", noContextTypedResult)
	}

	contextEvent := bus.Emit(abxbus.MustNewTypedEventWithResult[addPayload, addResult]("TypedNamedWithContextEvent", addPayload{A: 5, B: 8}))
	if _, err := contextEvent.Now(); err != nil {
		t.Fatal(err)
	}
	contextResult, err := contextEvent.EventResult()
	if err != nil {
		t.Fatal(err)
	}
	contextTypedResult, err := abxbus.EventResultAs[addResult](contextResult)
	if err != nil {
		t.Fatal(err)
	}
	if contextTypedResult.Sum != 13 || !gotCtx {
		t.Fatalf("expected named typed context result sum=13 and ctx=true, got result=%#v ctx=%v", contextTypedResult, gotCtx)
	}
}

func TestEventPayloadAsRejectsNilEvent(t *testing.T) {
	if _, err := abxbus.EventPayloadAs[addPayload](nil); err == nil || !strings.Contains(err.Error(), "event is nil") {
		t.Fatalf("expected nil event error, got %v", err)
	}
}

func TestTypedEventWithResultSchemaValidatesHandlerReturnAtRuntime(t *testing.T) {
	bus := abxbus.NewEventBus("TypedSchemaBus", nil)
	bus.On("TypedSchemaEvent", "bad", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return map[string]any{"sum": "not-an-int"}, nil
	}, nil)

	event := abxbus.MustNewTypedEventWithResult[addPayload, addResult]("TypedSchemaEvent", addPayload{A: 1, B: 2})
	if _, err := bus.Emit(event).EventResult(); err == nil || !strings.Contains(err.Error(), "EventHandlerResultSchemaError") {
		t.Fatalf("expected typed result schema error, got %v", err)
	}
}

func TestOnTypedValidatesPayloadBeforeCallingHandler(t *testing.T) {
	bus := abxbus.NewEventBus("TypedPayloadSchemaBus", nil)
	called := false
	abxbus.OnTyped[addPayload, addResult](bus, "TypedPayloadSchemaEvent", "typed", func(payload addPayload, ctx context.Context) (addResult, error) {
		called = true
		return addResult{Sum: payload.A + payload.B}, nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("TypedPayloadSchemaEvent", map[string]any{"a": 1}))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("typed handler should not be called when a required payload field is missing")
	}
	if _, err := event.EventResult(); err == nil || !strings.Contains(err.Error(), "EventHandlerPayloadSchemaError") {
		t.Fatalf("expected typed payload schema error, got %v", err)
	}
}

func TestOnTypedRejectsWrongPayloadFieldType(t *testing.T) {
	bus := abxbus.NewEventBus("TypedPayloadTypeBus", nil)
	called := false
	abxbus.OnTyped[addPayload, addResult](bus, "TypedPayloadTypeEvent", "typed", func(payload addPayload, ctx context.Context) (addResult, error) {
		called = true
		return addResult{Sum: payload.A + payload.B}, nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("TypedPayloadTypeEvent", map[string]any{"a": "one", "b": 2}))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("typed handler should not be called when a payload field has the wrong type")
	}
	if _, err := event.EventResult(); err == nil || !strings.Contains(err.Error(), "EventHandlerPayloadSchemaError") {
		t.Fatalf("expected typed payload schema error, got %v", err)
	}
}

func TestJSONSchemaForGoStructUsesJSONTagsAndRequiredFields(t *testing.T) {
	type nestedResult struct {
		Tags []string `json:"tags"`
	}
	type schemaResult struct {
		ID       string            `json:"id"`
		Count    int               `json:"count"`
		Metadata map[string]int    `json:"metadata,omitempty"`
		Nested   *nestedResult     `json:"nested,omitempty"`
		Ignored  string            `json:"-"`
		Any      map[string]string `json:",omitempty"`
	}

	schema := abxbus.JSONSchemaFor[schemaResult]()
	expectedRequired := []any{"id", "count"}
	if schema["$schema"] != "https://json-schema.org/draft/2020-12/schema" || schema["type"] != "object" {
		t.Fatalf("unexpected schema root: %#v", schema)
	}
	if !reflect.DeepEqual(schema["required"], expectedRequired) {
		t.Fatalf("unexpected required fields: %#v", schema["required"])
	}
	properties := schema["properties"].(map[string]any)
	if _, ok := properties["Ignored"]; ok {
		t.Fatalf("json:- field leaked into schema: %#v", properties)
	}
	if properties["id"].(map[string]any)["type"] != "string" || properties["count"].(map[string]any)["type"] != "integer" {
		t.Fatalf("primitive property schemas did not match: %#v", properties)
	}
	if properties["metadata"].(map[string]any)["additionalProperties"].(map[string]any)["type"] != "integer" {
		t.Fatalf("map property schema did not match: %#v", properties["metadata"])
	}
	nestedAnyOf := properties["nested"].(map[string]any)["anyOf"].([]any)
	if nestedAnyOf[0].(map[string]any)["type"] != "object" || nestedAnyOf[1].(map[string]any)["type"] != "null" {
		t.Fatalf("pointer property schema did not match: %#v", properties["nested"])
	}
}

func TestJSONSchemaForMapIncludesAdditionalPropertiesForNonStringKeys(t *testing.T) {
	schema := abxbus.JSONSchemaFor[map[int][]string]()
	if schema["type"] != "object" {
		t.Fatalf("unexpected map schema type: %#v", schema)
	}
	additionalProperties := schema["additionalProperties"].(map[string]any)
	if additionalProperties["type"] != "array" {
		t.Fatalf("map value schema should be array, got %#v", additionalProperties)
	}
	items := additionalProperties["items"].(map[string]any)
	if items["type"] != "string" {
		t.Fatalf("map value item schema should be string, got %#v", items)
	}
}

func TestJSONSchemaForEmbeddedStructFieldsMatchesJSONFlattening(t *testing.T) {
	type embeddedProfile struct {
		Email string `json:"email"`
		Age   int    `json:"age,omitempty"`
	}
	type schemaResult struct {
		embeddedProfile
		Name string `json:"name"`
	}

	schema := abxbus.JSONSchemaFor[schemaResult]()
	properties := schema["properties"].(map[string]any)
	if _, ok := properties["embeddedProfile"]; ok {
		t.Fatalf("anonymous embedded struct should be flattened, got %#v", properties)
	}
	if properties["email"].(map[string]any)["type"] != "string" || properties["age"].(map[string]any)["type"] != "integer" || properties["name"].(map[string]any)["type"] != "string" {
		t.Fatalf("flattened embedded property schemas did not match: %#v", properties)
	}
	expectedRequired := []any{"email", "name"}
	if !reflect.DeepEqual(schema["required"], expectedRequired) {
		t.Fatalf("unexpected required fields for flattened embedded struct: %#v", schema["required"])
	}
}

func TestJSONSchemaForTaggedAnonymousStructFieldStaysNested(t *testing.T) {
	type EmbeddedProfile struct {
		Email string `json:"email"`
	}
	type schemaResult struct {
		EmbeddedProfile `json:"profile"`
	}

	schema := abxbus.JSONSchemaFor[schemaResult]()
	properties := schema["properties"].(map[string]any)
	if _, ok := properties["email"]; ok {
		t.Fatalf("tagged anonymous struct should not be flattened: %#v", properties)
	}
	profile := properties["profile"].(map[string]any)
	if profile["type"] != "object" {
		t.Fatalf("tagged anonymous struct should be nested object, got %#v", profile)
	}
}
