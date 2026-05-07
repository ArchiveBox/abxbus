package abxbus_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

type middlewareRecord struct {
	Middleware    string
	Hook          string
	BusID         string
	EventID       string
	Status        string
	ResultStatus  abxbus.EventResultStatus
	HandlerID     string
	HandlerName   string
	EventPattern  string
	Registered    bool
	HandlerResult any
	HandlerError  any
}

type recordingMiddleware struct {
	name    string
	records []middlewareRecord
	seq     *[]string
	mu      sync.Mutex
}

func newRecordingMiddleware(name string, seq *[]string) *recordingMiddleware {
	return &recordingMiddleware{name: name, seq: seq}
}

func (m *recordingMiddleware) OnEventChange(bus *abxbus.EventBus, event *abxbus.BaseEvent, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, middlewareRecord{
		Middleware: m.name,
		Hook:       "event",
		BusID:      bus.ID,
		EventID:    event.EventID,
		Status:     status,
	})
	if m.seq != nil {
		*m.seq = append(*m.seq, fmt.Sprintf("%s:event:%s", m.name, status))
	}
}

func (m *recordingMiddleware) OnEventResultChange(bus *abxbus.EventBus, event *abxbus.BaseEvent, result *abxbus.EventResult, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record := middlewareRecord{
		Middleware:   m.name,
		Hook:         "result",
		BusID:        bus.ID,
		EventID:      event.EventID,
		Status:       status,
		ResultStatus: result.Status,
		HandlerID:    result.HandlerID,
		HandlerName:  result.HandlerName,
		HandlerError: result.Error,
	}
	if status == "completed" {
		record.HandlerResult = result.Result
	}
	m.records = append(m.records, record)
	if m.seq != nil {
		*m.seq = append(*m.seq, fmt.Sprintf("%s:result:%s:%s", m.name, result.HandlerName, status))
	}
}

func (m *recordingMiddleware) OnBusHandlersChange(bus *abxbus.EventBus, handler *abxbus.EventHandler, registered bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, middlewareRecord{
		Middleware:   m.name,
		Hook:         "handler",
		BusID:        bus.ID,
		HandlerID:    handler.ID,
		HandlerName:  handler.HandlerName,
		EventPattern: handler.EventPattern,
		Registered:   registered,
	})
	if m.seq != nil {
		state := "unregistered"
		if registered {
			state = "registered"
		}
		*m.seq = append(*m.seq, fmt.Sprintf("%s:handler:%s:%s", m.name, handler.HandlerName, state))
	}
}

func (m *recordingMiddleware) snapshot() []middlewareRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]middlewareRecord{}, m.records...)
}

func recordsByHook(records []middlewareRecord, hook string) []middlewareRecord {
	filtered := []middlewareRecord{}
	for _, record := range records {
		if record.Hook == hook {
			filtered = append(filtered, record)
		}
	}
	return filtered
}

func statuses(records []middlewareRecord) []string {
	values := make([]string, 0, len(records))
	for _, record := range records {
		values = append(values, record.Status)
	}
	return values
}

func resultStatuses(records []middlewareRecord) []abxbus.EventResultStatus {
	values := make([]abxbus.EventResultStatus, 0, len(records))
	for _, record := range records {
		values = append(values, record.ResultStatus)
	}
	return values
}

func assertRecordStatuses(t *testing.T, records []middlewareRecord, expected []string) {
	t.Helper()
	if !reflect.DeepEqual(statuses(records), expected) {
		t.Fatalf("unexpected hook statuses: got %#v want %#v", statuses(records), expected)
	}
}

func TestEventBusMiddlewareReceivesPendingStartedCompletedLifecycleHooks(t *testing.T) {
	middleware := newRecordingMiddleware("single", nil)
	bus := abxbus.NewEventBus("MiddlewareBus", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})
	handler := bus.On("MiddlewareEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, nil)
	bus.Off("MiddlewareEvent", handler)
	handler = bus.On("MiddlewareEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, &abxbus.EventHandler{
		ID:                  handler.ID,
		HandlerRegisteredAt: handler.HandlerRegisteredAt,
	})

	event := bus.Emit(abxbus.NewBaseEvent("MiddlewareEvent", nil))
	if _, err := event.Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	records := middleware.snapshot()
	handlerChanges := recordsByHook(records, "handler")
	if len(handlerChanges) != 3 {
		t.Fatalf("unexpected handler middleware changes: %#v", handlerChanges)
	}
	if handlerChanges[0].HandlerName != "handler" || !handlerChanges[0].Registered ||
		handlerChanges[1].HandlerName != "handler" || handlerChanges[1].Registered ||
		handlerChanges[2].HandlerName != "handler" || !handlerChanges[2].Registered {
		t.Fatalf("unexpected handler middleware changes: %#v", handlerChanges)
	}

	assertRecordStatuses(t, recordsByHook(records, "event"), []string{"pending", "started", "completed"})

	resultRecords := recordsByHook(records, "result")
	assertRecordStatuses(t, resultRecords, []string{"pending", "started", "completed"})
	if !reflect.DeepEqual(resultStatuses(resultRecords), []abxbus.EventResultStatus{
		abxbus.EventResultPending,
		abxbus.EventResultStarted,
		abxbus.EventResultCompleted,
	}) {
		t.Fatalf("unexpected runtime result statuses: %#v", resultStatuses(resultRecords))
	}
	if resultRecords[2].HandlerResult != "ok" {
		t.Fatalf("completed middleware hook did not observe handler result: %#v", resultRecords[2])
	}
}

func TestEventBusMiddlewareHooksExecuteInRegistrationOrder(t *testing.T) {
	sequence := []string{}
	first := newRecordingMiddleware("first", &sequence)
	second := newRecordingMiddleware("second", &sequence)
	bus := abxbus.NewEventBus("MiddlewareOrderBus", &abxbus.EventBusOptions{
		Middlewares: []abxbus.EventBusMiddleware{first, second},
	})
	bus.On("MiddlewareOrderEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, nil)

	if _, err := bus.Emit(abxbus.NewBaseEvent("MiddlewareOrderEvent", nil)).Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	orderedPairs := [][2]string{
		{"first:event:pending", "second:event:pending"},
		{"first:event:started", "second:event:started"},
		{"first:result:handler:pending", "second:result:handler:pending"},
		{"first:result:handler:started", "second:result:handler:started"},
		{"first:result:handler:completed", "second:result:handler:completed"},
		{"first:event:completed", "second:event:completed"},
	}
	for _, pair := range orderedPairs {
		firstIndex, secondIndex := middlewareIndexOf(sequence, pair[0]), middlewareIndexOf(sequence, pair[1])
		if firstIndex < 0 || secondIndex < 0 || firstIndex > secondIndex {
			t.Fatalf("middleware order mismatch for %v in sequence %#v", pair, sequence)
		}
	}
}

func TestEventBusMiddlewareNoHandlerEventLifecycle(t *testing.T) {
	middleware := newRecordingMiddleware("single", nil)
	bus := abxbus.NewEventBus("MiddlewareNoHandlerBus", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})

	event := bus.Emit(abxbus.NewBaseEvent("MiddlewareNoHandlerEvent", nil))
	if _, err := event.Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	records := middleware.snapshot()
	assertRecordStatuses(t, recordsByHook(records, "event"), []string{"pending", "started", "completed"})
	if resultRecords := recordsByHook(records, "result"); len(resultRecords) != 0 {
		t.Fatalf("no-handler event should not emit result hooks: %#v", resultRecords)
	}
}

func TestEventBusMiddlewareEventLifecycleOrderingIsDeterministicPerEvent(t *testing.T) {
	middleware := newRecordingMiddleware("deterministic", nil)
	historySize := 0
	bus := abxbus.NewEventBus("MiddlewareDeterministicBus", &abxbus.EventBusOptions{
		Middlewares:    []abxbus.EventBusMiddleware{middleware},
		MaxHistorySize: &historySize,
	})
	bus.On("MiddlewareDeterministicEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		time.Sleep(time.Millisecond)
		return "ok", nil
	}, nil)

	batchCount := 5
	eventsPerBatch := 50
	seenEvents := map[string]bool{}
	for batchIndex := 0; batchIndex < batchCount; batchIndex++ {
		events := make([]*abxbus.BaseEvent, 0, eventsPerBatch)
		for eventIndex := 0; eventIndex < eventsPerBatch; eventIndex++ {
			eventTimeout := 0.2
			event := abxbus.NewBaseEvent("MiddlewareDeterministicEvent", nil)
			event.EventTimeout = &eventTimeout
			events = append(events, bus.Emit(event))
		}
		for _, event := range events {
			if _, err := event.Done(context.Background()); err != nil {
				t.Fatal(err)
			}
			seenEvents[event.EventID] = true
		}

		recordsByEventID := map[string][]middlewareRecord{}
		for _, record := range recordsByHook(middleware.snapshot(), "event") {
			recordsByEventID[record.EventID] = append(recordsByEventID[record.EventID], record)
		}
		for _, event := range events {
			assertRecordStatuses(t, recordsByEventID[event.EventID], []string{"pending", "started", "completed"})
		}
	}
	if len(seenEvents) != batchCount*eventsPerBatch {
		t.Fatalf("unexpected deterministic event count: got %d want %d", len(seenEvents), batchCount*eventsPerBatch)
	}
}

func TestEventBusMiddlewareHooksObserveHandlerErrorsWithoutErrorHookStatus(t *testing.T) {
	middleware := newRecordingMiddleware("errors", nil)
	bus := abxbus.NewEventBus("MiddlewareErrorBus", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})
	bus.On("MiddlewareErrorEvent", "failing", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return nil, errors.New("boom")
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("MiddlewareErrorEvent", nil))
	_, err := event.EventResult(context.Background())
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected handler error from event result, got %v", err)
	}

	records := middleware.snapshot()
	assertRecordStatuses(t, recordsByHook(records, "event"), []string{"pending", "started", "completed"})
	resultRecords := recordsByHook(records, "result")
	assertRecordStatuses(t, resultRecords, []string{"pending", "started", "completed"})
	if resultRecords[len(resultRecords)-1].ResultStatus != abxbus.EventResultError {
		t.Fatalf("completed hook should observe runtime error status: %#v", resultRecords)
	}
	if resultRecords[len(resultRecords)-1].HandlerError == nil {
		t.Fatalf("completed hook should observe handler error: %#v", resultRecords)
	}
}

func TestEventBusMiddlewareHooksRemainMonotonicOnEventTimeout(t *testing.T) {
	middleware := newRecordingMiddleware("timeout", nil)
	bus := abxbus.NewEventBus("MiddlewareTimeoutBus", &abxbus.EventBusOptions{
		Middlewares:             []abxbus.EventBusMiddleware{middleware},
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	bus.On("MiddlewareTimeoutEvent", "slow", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		select {
		case <-time.After(100 * time.Millisecond):
			return "late", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	bus.On("MiddlewareTimeoutEvent", "pending", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "pending", nil
	}, nil)
	timeout := 0.02
	event := abxbus.NewBaseEvent("MiddlewareTimeoutEvent", nil)
	event.EventTimeout = &timeout

	_ = bus.Emit(event).EventCompleted(context.Background())

	records := middleware.snapshot()
	assertRecordStatuses(t, recordsByHook(records, "event"), []string{"pending", "started", "completed"})
	resultRecords := recordsByHook(records, "result")
	if len(resultRecords) != 5 {
		t.Fatalf("expected pending/started/completed for slow plus pending/completed for queued handler, got %#v", resultRecords)
	}
	byHandler := map[string][]middlewareRecord{}
	for _, record := range resultRecords {
		byHandler[record.HandlerName] = append(byHandler[record.HandlerName], record)
	}
	assertRecordStatuses(t, byHandler["slow"], []string{"pending", "started", "completed"})
	assertRecordStatuses(t, byHandler["pending"], []string{"pending", "completed"})
	if byHandler["slow"][2].ResultStatus != abxbus.EventResultError || byHandler["pending"][1].ResultStatus != abxbus.EventResultError {
		t.Fatalf("timeout hooks should observe runtime error status: %#v", byHandler)
	}
}

func TestEventBusMiddlewareHardEventTimeoutFinalizesImmediatelyWithoutWaitingForInFlightHandlers(t *testing.T) {
	bus := abxbus.NewEventBus("MiddlewareHardTimeoutBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	started := make(chan struct{}, 2)
	for _, handlerName := range []string{"slow_1", "slow_2"} {
		handlerName := handlerName
		bus.On("MiddlewareHardTimeoutEvent", handlerName, func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
			started <- struct{}{}
			time.Sleep(200 * time.Millisecond)
			return "late:" + handlerName, nil
		}, nil)
	}

	timeout := 0.01
	event := abxbus.NewBaseEvent("MiddlewareHardTimeoutEvent", nil)
	event.EventTimeout = &timeout
	startedAt := time.Now()
	dispatched := bus.Emit(event)
	if err := dispatched.EventCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(startedAt)
	if elapsed > 100*time.Millisecond {
		t.Fatalf("event timeout should finalize without waiting for slow handlers, elapsed=%s", elapsed)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		default:
			t.Fatalf("expected both parallel handlers to start before hard timeout, got %d", i)
		}
	}

	initialSnapshot := map[string]abxbus.EventResultStatus{}
	for id, result := range dispatched.EventResults {
		initialSnapshot[id] = result.Status
		if result.Status != abxbus.EventResultError {
			t.Fatalf("hard timeout should finalize handler result as error, got %#v", result)
		}
	}
	time.Sleep(250 * time.Millisecond)
	for id, status := range initialSnapshot {
		result := dispatched.EventResults[id]
		if result.Status != status {
			t.Fatalf("late handler completion reversed result status for %s: got %s want %s", id, result.Status, status)
		}
		if result.Result != nil {
			t.Fatalf("late handler result should not overwrite timeout error for %s: %#v", id, result)
		}
	}
}

func TestEventBusMiddlewareTimeoutCancelAbortAndResultSchemaTaxonomyRemainsExplicit(t *testing.T) {
	serialBus := abxbus.NewEventBus("MiddlewareTaxonomySerialBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	parallelBus := abxbus.NewEventBus("MiddlewareTaxonomyParallelBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})

	serialBus.On("MiddlewareSchemaEvent", "bad_schema", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "not-a-number", nil
	}, nil)
	schemaEvent := abxbus.NewBaseEvent("MiddlewareSchemaEvent", nil)
	schemaEvent.EventResultType = map[string]any{"type": "number"}
	schemaEvent = serialBus.Emit(schemaEvent)
	if err := schemaEvent.EventCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	schemaResults := schemaEvent.EventResults
	if len(schemaResults) != 1 {
		t.Fatalf("schema event should have one handler result, got %#v", schemaResults)
	}
	for _, result := range schemaResults {
		if result.Status != abxbus.EventResultError || !strings.Contains(fmt.Sprint(result.Error), "EventHandlerResultSchemaError") {
			t.Fatalf("schema mismatch should remain an explicit result-schema error, got %#v", result)
		}
	}

	serialBus.On("MiddlewareSerialTimeoutEvent", "slow_1", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		select {
		case <-time.After(100 * time.Millisecond):
			return "slow", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	serialBus.On("MiddlewareSerialTimeoutEvent", "slow_2", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		select {
		case <-time.After(100 * time.Millisecond):
			return "slow-2", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	serialTimeout := 0.01
	serialEvent := abxbus.NewBaseEvent("MiddlewareSerialTimeoutEvent", nil)
	serialEvent.EventTimeout = &serialTimeout
	serialEvent = serialBus.Emit(serialEvent)
	if err := serialEvent.EventCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	serialErrors := eventResultErrorStrings(serialEvent)
	if !containsErrorText(serialErrors, "Cancelled pending handler") {
		t.Fatalf("serial event timeout should cancel pending handlers explicitly, got %#v", serialErrors)
	}
	if !containsErrorText(serialErrors, "Aborted running handler") && !containsErrorText(serialErrors, "timed out") {
		t.Fatalf("serial event timeout should abort or time out a running handler explicitly, got %#v", serialErrors)
	}

	parallelBus.On("MiddlewareParallelTimeoutEvent", "slow_1", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		select {
		case <-time.After(100 * time.Millisecond):
			return "slow", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	parallelBus.On("MiddlewareParallelTimeoutEvent", "slow_2", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		select {
		case <-time.After(100 * time.Millisecond):
			return "slow-2", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	parallelTimeout := 0.01
	parallelEvent := abxbus.NewBaseEvent("MiddlewareParallelTimeoutEvent", nil)
	parallelEvent.EventTimeout = &parallelTimeout
	parallelEvent = parallelBus.Emit(parallelEvent)
	if err := parallelEvent.EventCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	parallelErrors := eventResultErrorStrings(parallelEvent)
	if !containsErrorText(parallelErrors, "Aborted running handler") && !containsErrorText(parallelErrors, "timed out") {
		t.Fatalf("parallel event timeout should abort or time out running handlers explicitly, got %#v", parallelErrors)
	}
	if containsErrorText(parallelErrors, "Cancelled pending handler") {
		t.Fatalf("parallel event timeout should not cancel pending handlers when all handlers have started, got %#v", parallelErrors)
	}
}

func eventResultErrorStrings(event *abxbus.BaseEvent) []string {
	errors := []string{}
	for _, result := range event.EventResults {
		if result.Error != nil {
			errors = append(errors, fmt.Sprint(result.Error))
		}
	}
	return errors
}

func containsErrorText(errors []string, text string) bool {
	for _, err := range errors {
		if strings.Contains(err, text) {
			return true
		}
	}
	return false
}

func TestEventBusMiddlewareHooksArePerBusOnForwardedEvents(t *testing.T) {
	middlewareA := newRecordingMiddleware("a", nil)
	middlewareB := newRecordingMiddleware("b", nil)
	busA := abxbus.NewEventBus("MiddlewareForwardA", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middlewareA}})
	busB := abxbus.NewEventBus("MiddlewareForwardB", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middlewareB}})
	handlerA := busA.On("MiddlewareForwardEvent", "forward", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		busB.Emit(event)
		return "forwarded", nil
	}, nil)
	handlerB := busB.On("MiddlewareForwardEvent", "target", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "target", nil
	}, nil)

	if _, err := busA.Emit(abxbus.NewBaseEvent("MiddlewareForwardEvent", nil)).Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	recordsA := recordsByHook(middlewareA.snapshot(), "result")
	recordsB := recordsByHook(middlewareB.snapshot(), "result")
	if !recordsContainHandler(recordsA, handlerA.ID) || recordsContainHandler(recordsA, handlerB.ID) {
		t.Fatalf("source middleware should only observe source handler, got %#v", recordsA)
	}
	if !recordsContainHandler(recordsB, handlerB.ID) {
		t.Fatalf("target middleware should observe target handler, got %#v", recordsB)
	}
}

func TestEventBusMiddlewareHooksCoverStringAndWildcardPatterns(t *testing.T) {
	middleware := newRecordingMiddleware("patterns", nil)
	bus := abxbus.NewEventBus("MiddlewarePatternBus", &abxbus.EventBusOptions{Middlewares: []abxbus.EventBusMiddleware{middleware}})
	stringHandler := bus.On("MiddlewarePatternEvent", "string", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "string:" + event.EventType, nil
	}, nil)
	wildcardHandler := bus.On("*", "wildcard", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "wildcard:" + event.EventType, nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("MiddlewarePatternEvent", nil))
	if _, err := event.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	bus.Off("MiddlewarePatternEvent", stringHandler)
	bus.Off("*", wildcardHandler)

	records := middleware.snapshot()
	handlerRecords := recordsByHook(records, "handler")
	expectedPatternByID := map[string]string{
		stringHandler.ID:   "MiddlewarePatternEvent",
		wildcardHandler.ID: "*",
	}
	registered := map[string]bool{}
	unregistered := map[string]bool{}
	for _, record := range handlerRecords {
		expectedPattern, ok := expectedPatternByID[record.HandlerID]
		if !ok {
			t.Fatalf("unexpected handler record: %#v", record)
		}
		if record.EventPattern != expectedPattern {
			t.Fatalf("handler pattern changed: %#v", record)
		}
		if record.Registered {
			registered[record.HandlerID] = true
		} else {
			unregistered[record.HandlerID] = true
		}
	}
	for handlerID := range expectedPatternByID {
		if !registered[handlerID] || !unregistered[handlerID] {
			t.Fatalf("expected register/unregister records for %s, got %#v", handlerID, handlerRecords)
		}
	}

	resultRecords := recordsByHook(records, "result")
	if !recordsContainHandler(resultRecords, stringHandler.ID) || !recordsContainHandler(resultRecords, wildcardHandler.ID) {
		t.Fatalf("expected both string and wildcard handlers in result hooks, got %#v", resultRecords)
	}
	if event.EventResults[stringHandler.ID].Result != "string:MiddlewarePatternEvent" {
		t.Fatalf("unexpected string handler result: %#v", event.EventResults[stringHandler.ID])
	}
	if event.EventResults[wildcardHandler.ID].Result != "wildcard:MiddlewarePatternEvent" {
		t.Fatalf("unexpected wildcard handler result: %#v", event.EventResults[wildcardHandler.ID])
	}
}

func recordsContainHandler(records []middlewareRecord, handlerID string) bool {
	for _, record := range records {
		if record.HandlerID == handlerID {
			return true
		}
	}
	return false
}

func middlewareIndexOf(values []string, needle string) int {
	for i, value := range values {
		if value == needle {
			return i
		}
	}
	return -1
}
