package abxbus_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func testFloat64Ptr(value float64) *float64 { return &value }

func testWaitForSignal(t *testing.T, ch <-chan struct{}, timeout time.Duration, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func TestQueueJumpPreservesParentChildLineageAndFindVisibility(t *testing.T) {
	bus := abxbus.NewEventBus("ParityQueueJumpBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	t.Cleanup(bus.Destroy)

	var mu sync.Mutex
	executionOrder := []string{}
	childEventID := ""
	appendOrder := func(value string) {
		mu.Lock()
		defer mu.Unlock()
		executionOrder = append(executionOrder, value)
	}

	bus.On("QueueJumpRootEvent", "on_root", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		appendOrder("root:start")
		child := event.Emit(abxbus.NewBaseEvent("QueueJumpChildEvent", nil))
		if _, err := child.Now(); err != nil {
			return nil, err
		}
		appendOrder("root:end")
		return "root-ok", nil
	}, nil)
	bus.On("QueueJumpChildEvent", "on_child", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		mu.Lock()
		childEventID = event.EventID
		mu.Unlock()
		appendOrder("child")
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return "child-ok", nil
	}, nil)
	bus.On("QueueJumpSiblingEvent", "on_sibling", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		appendOrder("sibling")
		return "sibling-ok", nil
	}, nil)

	root := bus.Emit(abxbus.NewBaseEvent("QueueJumpRootEvent", nil))
	sibling := bus.Emit(abxbus.NewBaseEvent("QueueJumpSiblingEvent", nil))
	if _, err := root.Now(); err != nil {
		t.Fatal(err)
	}
	if _, err := sibling.Now(); err != nil {
		t.Fatal(err)
	}
	if !bus.WaitUntilIdle(testFloat64Ptr(2)) {
		t.Fatal("timed out waiting for queue-jump bus to idle")
	}

	mu.Lock()
	gotOrder := append([]string{}, executionOrder...)
	capturedChildID := childEventID
	mu.Unlock()
	wantOrder := []string{"root:start", "child", "root:end", "sibling"}
	if len(gotOrder) != len(wantOrder) {
		t.Fatalf("execution order length mismatch: got %v want %v", gotOrder, wantOrder)
	}
	for idx := range wantOrder {
		if gotOrder[idx] != wantOrder[idx] {
			t.Fatalf("execution order mismatch: got %v want %v", gotOrder, wantOrder)
		}
	}
	if capturedChildID == "" {
		t.Fatal("child handler did not capture child event id")
	}

	foundChild, err := bus.Find("QueueJumpChildEvent", nil, &abxbus.FindOptions{Past: true, Future: false, ChildOf: root})
	if err != nil {
		t.Fatal(err)
	}
	if foundChild == nil || foundChild.EventID != capturedChildID {
		t.Fatalf("expected to find child %s, got %#v", capturedChildID, foundChild)
	}
	if foundChild.EventParentID == nil || *foundChild.EventParentID != root.EventID {
		t.Fatalf("child parent mismatch: got %#v want %s", foundChild.EventParentID, root.EventID)
	}

	var rootResult *abxbus.EventResult
	for _, result := range root.EventResults {
		if result.HandlerName == "on_root" {
			rootResult = result
			break
		}
	}
	if rootResult == nil {
		t.Fatalf("missing root handler result: %#v", root.EventResults)
	}
	foundLineage := false
	for _, childID := range rootResult.EventChildIDs {
		if childID == foundChild.EventID {
			foundLineage = true
			break
		}
	}
	if !foundLineage {
		t.Fatalf("root handler result did not track child %s: %#v", foundChild.EventID, rootResult.EventChildIDs)
	}
}

func TestConcurrencyIntersectionParallelEventsWithSerialHandlers(t *testing.T) {
	bus := abxbus.NewEventBus("ParityConcurrencyIntersectionBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyParallel,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		MaxHistorySize:          nil,
	})
	bus.EventHistory.MaxHistorySize = nil
	t.Cleanup(bus.Destroy)

	var mu sync.Mutex
	currentByEvent := map[string]int{}
	maxByEvent := map[string]int{}
	globalCurrent := 0
	globalMax := 0
	trackedHandler := func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		mu.Lock()
		currentByEvent[event.EventID]++
		if currentByEvent[event.EventID] > maxByEvent[event.EventID] {
			maxByEvent[event.EventID] = currentByEvent[event.EventID]
		}
		globalCurrent++
		if globalCurrent > globalMax {
			globalMax = globalCurrent
		}
		mu.Unlock()

		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		mu.Lock()
		currentByEvent[event.EventID]--
		globalCurrent--
		mu.Unlock()
		return "ok", nil
	}
	bus.On("ConcurrencyIntersectionEvent", "tracked_handler_a", trackedHandler, nil)
	bus.On("ConcurrencyIntersectionEvent", "tracked_handler_b", trackedHandler, nil)

	events := make([]*abxbus.BaseEvent, 0, 8)
	for idx := 0; idx < 8; idx++ {
		events = append(events, bus.Emit(abxbus.NewBaseEvent("ConcurrencyIntersectionEvent", map[string]any{"token": idx})))
	}
	for _, event := range events {
		if _, err := event.Wait(); err != nil {
			t.Fatal(err)
		}
	}
	if !bus.WaitUntilIdle(testFloat64Ptr(2)) {
		t.Fatal("timed out waiting for concurrency-intersection bus to idle")
	}

	mu.Lock()
	defer mu.Unlock()
	for _, event := range events {
		if maxByEvent[event.EventID] != 1 {
			t.Fatalf("handlers for event %s overlapped despite serial handler mode: max=%d", event.EventID, maxByEvent[event.EventID])
		}
		for _, result := range event.EventResults {
			if result.Status != abxbus.EventResultCompleted {
				t.Fatalf("expected completed handler result for %s, got %s error=%#v", event.EventID, result.Status, result.Error)
			}
		}
	}
	if globalMax < 2 {
		t.Fatalf("parallel event mode did not overlap events; global max concurrency=%d", globalMax)
	}
}

func TestTimeoutEnforcementDoesNotBreakFollowupProcessingOrQueueState(t *testing.T) {
	bus := abxbus.NewEventBus("ParityTimeoutEnforcementBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	t.Cleanup(bus.Destroy)
	bus.On("TimeoutEnforcementEvent", "slow_handler_a", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, nil)
	bus.On("TimeoutEnforcementEvent", "slow_handler_b", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, nil)
	bus.On("TimeoutFollowupEvent", "followup_handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "followup-ok", nil
	}, nil)

	timeout := 0.02
	timedOut := abxbus.NewBaseEvent("TimeoutEnforcementEvent", nil)
	timedOut.EventTimeout = &timeout
	timedOut = bus.Emit(timedOut)
	_, _ = timedOut.Now()
	if timedOut.EventStatus != "completed" {
		t.Fatalf("timed-out event should settle completed, got %s", timedOut.EventStatus)
	}
	if len(timedOut.EventResults) == 0 {
		t.Fatal("expected timed-out handler results")
	}
	for _, result := range timedOut.EventResults {
		if result.Status != abxbus.EventResultError {
			t.Fatalf("expected timeout handler result error, got %s result=%#v error=%#v", result.Status, result.Result, result.Error)
		}
	}

	followup := bus.Emit(abxbus.NewBaseEvent("TimeoutFollowupEvent", nil))
	if _, err := followup.Now(); err != nil {
		t.Fatal(err)
	}
	got, err := followup.EventResult()
	if err != nil {
		t.Fatal(err)
	}
	if got != "followup-ok" {
		t.Fatalf("unexpected followup result: %#v", got)
	}
	for _, result := range followup.EventResults {
		if result.Status != abxbus.EventResultCompleted {
			t.Fatalf("expected followup handler completed, got %s", result.Status)
		}
	}
	if !bus.WaitUntilIdle(testFloat64Ptr(2)) {
		t.Fatal("timed out waiting after timeout/followup")
	}
	if !bus.IsIdleAndQueueEmpty() {
		t.Fatal("bus queue state should be idle and empty after timeout/followup")
	}
}

func TestZeroHistoryBackpressureWithFindFutureStillResolvesNewEvents(t *testing.T) {
	maxHistorySize := 0
	bus := abxbus.NewEventBus("ParityZeroHistoryBus", &abxbus.EventBusOptions{
		MaxHistorySize: &maxHistorySize,
		MaxHistoryDrop: false,
	})
	t.Cleanup(bus.Destroy)
	bus.On("ZeroHistoryEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok:" + event.Payload["value"].(string), nil
	}, nil)

	first := bus.Emit(abxbus.NewBaseEvent("ZeroHistoryEvent", map[string]any{"value": "first"}))
	if _, err := first.Now(); err != nil {
		t.Fatal(err)
	}
	if bus.EventHistory.Has(first.EventID) {
		t.Fatal("max_history_size=0 should drop completed events from history")
	}
	past, err := bus.Find("ZeroHistoryEvent", nil, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if past != nil {
		t.Fatalf("zero-history past lookup should not find completed event, got %#v", past)
	}

	capturedFutureID := make(chan string, 1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		futureEvent := bus.Emit(abxbus.NewBaseEvent("ZeroHistoryEvent", map[string]any{"value": "future"}))
		capturedFutureID <- futureEvent.EventID
	}()
	futureMatch, err := bus.Find("ZeroHistoryEvent", func(event *abxbus.BaseEvent) bool {
		return event.Payload["value"] == "future"
	}, &abxbus.FindOptions{Past: false, Future: 1.0})
	if err != nil {
		t.Fatal(err)
	}
	capturedID := <-capturedFutureID
	if futureMatch == nil || futureMatch.Payload["value"] != "future" || futureMatch.EventID != capturedID {
		t.Fatalf("future find mismatch: got %#v captured=%s", futureMatch, capturedID)
	}
	if !bus.WaitUntilIdle(testFloat64Ptr(2)) {
		t.Fatal("timed out waiting for zero-history future event")
	}
	if bus.EventHistory.Size() != 0 {
		t.Fatalf("zero-history bus should drop completed events, got history size %d", bus.EventHistory.Size())
	}
}

type crossRuntimeContextKey string

func TestContextPropagatesThroughForwardingAndChildDispatchWithLineageIntact(t *testing.T) {
	busA := abxbus.NewEventBus("ParityContextForwardA", nil)
	busB := abxbus.NewEventBus("ParityContextForwardB", nil)
	t.Cleanup(busA.Destroy)
	t.Cleanup(busB.Destroy)

	key := crossRuntimeContextKey("request_id")
	capturedParentRequestID := ""
	capturedChildRequestID := ""
	parentEventID := ""
	childParentID := ""

	busA.On("*", "forward_to_b", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return busB.Emit(event), nil
	}, nil)
	busB.On("ContextParentEvent", "on_parent", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		capturedParentRequestID, _ = ctx.Value(key).(string)
		parentEventID = event.EventID
		child := event.Emit(abxbus.NewBaseEvent("ContextChildEvent", nil))
		if _, err := child.Now(); err != nil {
			return nil, err
		}
		return "parent-ok", nil
	}, nil)
	busB.On("ContextChildEvent", "on_child", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		capturedChildRequestID, _ = ctx.Value(key).(string)
		if event.EventParentID != nil {
			childParentID = *event.EventParentID
		}
		return "child-ok", nil
	}, nil)

	requestID := "fc81f432-98cd-7a06-824c-dafed74761bb"
	ctx := context.WithValue(context.Background(), key, requestID)
	parent := busA.EmitWithContext(ctx, abxbus.NewBaseEvent("ContextParentEvent", nil))
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}
	if !busB.WaitUntilIdle(testFloat64Ptr(2)) {
		t.Fatal("timed out waiting for forwarded context bus")
	}
	if capturedParentRequestID != requestID {
		t.Fatalf("forwarded parent handler did not see request id: got %q want %q", capturedParentRequestID, requestID)
	}
	if capturedChildRequestID != requestID {
		t.Fatalf("forwarded child handler did not see request id: got %q want %q", capturedChildRequestID, requestID)
	}
	if parentEventID == "" || childParentID != parentEventID {
		t.Fatalf("child lineage mismatch: parent=%q child_parent=%q", parentEventID, childParentID)
	}
	if len(parent.EventPath) < 2 || !strings.HasPrefix(parent.EventPath[0], "ParityContextForwardA#") {
		t.Fatalf("parent event path did not include source bus first: %#v", parent.EventPath)
	}
	foundTargetBus := false
	for _, label := range parent.EventPath {
		if strings.HasPrefix(label, "ParityContextForwardB#") {
			foundTargetBus = true
			break
		}
	}
	if !foundTargetBus {
		t.Fatalf("parent event path did not include target bus: %#v", parent.EventPath)
	}

	foundChild, err := busB.Find("ContextChildEvent", nil, &abxbus.FindOptions{Past: true, Future: false, ChildOf: parent})
	if err != nil {
		t.Fatal(err)
	}
	if foundChild == nil || foundChild.EventParentID == nil || *foundChild.EventParentID != parent.EventID {
		t.Fatalf("expected forwarded child to be findable by parent, got %#v", foundChild)
	}
}

func TestPendingQueueFindVisibilityTransitionsToCompletedAfterRelease(t *testing.T) {
	bus := abxbus.NewEventBus("ParityPendingFindBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
		MaxHistorySize:          nil,
	})
	bus.EventHistory.MaxHistorySize = nil
	t.Cleanup(bus.Destroy)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	bus.On("PendingVisibilityEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		if event.Payload["tag"] == "blocking" {
			once.Do(func() { close(started) })
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return "ok:" + event.Payload["tag"].(string), nil
	}, nil)

	blocking := bus.Emit(abxbus.NewBaseEvent("PendingVisibilityEvent", map[string]any{"tag": "blocking"}))
	testWaitForSignal(t, started, 2*time.Second, "blocking event start")

	queued := bus.Emit(abxbus.NewBaseEvent("PendingVisibilityEvent", map[string]any{"tag": "queued"}))
	time.Sleep(10 * time.Millisecond)
	foundQueued, err := bus.Find("PendingVisibilityEvent", func(event *abxbus.BaseEvent) bool {
		return event.Payload["tag"] == "queued"
	}, &abxbus.FindOptions{Past: true, Future: false})
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	if foundQueued == nil || foundQueued.EventID != queued.EventID || foundQueued.EventStatus != "pending" {
		close(release)
		t.Fatalf("expected queued pending event to be visible via find, got %#v", foundQueued)
	}

	close(release)
	if _, err := blocking.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err := queued.Wait(); err != nil {
		t.Fatal(err)
	}
	if queued.EventStatus != "completed" {
		t.Fatalf("queued event should transition to completed after release, got %s", queued.EventStatus)
	}
}

func TestHistoryBackpressureRejectsOverflowAndPreservesFindableHistory(t *testing.T) {
	maxHistorySize := 2
	bus := abxbus.NewEventBus("ParityBackpressureBus", &abxbus.EventBusOptions{
		MaxHistorySize: &maxHistorySize,
		MaxHistoryDrop: false,
	})
	t.Cleanup(bus.Destroy)
	bus.On("BackpressureEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok:" + event.Payload["value"].(string), nil
	}, nil)

	first := bus.Emit(abxbus.NewBaseEvent("BackpressureEvent", map[string]any{"value": "first"}))
	second := bus.Emit(abxbus.NewBaseEvent("BackpressureEvent", map[string]any{"value": "second"}))
	if _, err := first.Now(); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Now(); err != nil {
		t.Fatal(err)
	}
	foundFirst, err := bus.Find("BackpressureEvent", nil, &abxbus.FindOptions{
		Past:   true,
		Future: false,
		Equals: map[string]any{"value": "first"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if foundFirst == nil || foundFirst.EventID != first.EventID {
		t.Fatalf("expected first event to remain findable before overflow, got %#v", foundFirst)
	}

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("expected max_history_drop=false overflow to panic")
		}
		if bus.EventHistory.Size() != 2 {
			t.Fatalf("history size should remain capped after rejected overflow, got %d", bus.EventHistory.Size())
		}
	}()
	bus.Emit(abxbus.NewBaseEvent("BackpressureEvent", map[string]any{"value": "overflow"}))
}

func TestEventBusCrossRuntimeJSONFeaturesUseCanonicalShapes(t *testing.T) {
	detectPaths := false
	bus := abxbus.NewEventBus("CrossRuntimeFeatureBus", &abxbus.EventBusOptions{
		EventHandlerDetectFilePaths: &detectPaths,
	})
	handler := bus.On("CrossRuntimeFeatureEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return map[string]any{"ok": true}, nil
	}, nil)
	event := abxbus.NewBaseEvent("CrossRuntimeFeatureEvent", map[string]any{"label": "go"})
	event.EventResultType = map[string]any{
		"type":       "object",
		"properties": map[string]any{"ok": map[string]any{"type": "boolean"}},
	}
	if _, err := bus.Emit(event).Now(); err != nil {
		t.Fatal(err)
	}

	data, err := bus.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatal(err)
	}
	handlers := payload["handlers"].(map[string]any)
	handlersByKey := payload["handlers_by_key"].(map[string]any)
	history := payload["event_history"].(map[string]any)
	if _, ok := handlers[handler.ID]; !ok {
		t.Fatalf("handlers must be id-keyed, got %#v", handlers)
	}
	if ids := handlersByKey["CrossRuntimeFeatureEvent"].([]any); len(ids) != 1 || ids[0] != handler.ID {
		t.Fatalf("handlers_by_key shape mismatch: %#v", handlersByKey)
	}
	if _, ok := history[event.EventID]; !ok {
		t.Fatalf("event_history must be id-keyed, got %#v", history)
	}
}
