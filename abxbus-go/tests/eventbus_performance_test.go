package abxbus_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

const performanceMaxMsPerUnit = 0.3

func assertPerformanceBudget(t *testing.T, scenario string, total int, elapsed time.Duration, unit string) {
	t.Helper()
	msPerUnit := float64(elapsed.Microseconds()) / 1000.0 / float64(total)
	throughput := float64(total) / elapsed.Seconds()
	t.Logf("%s: total=%s latency=%.3fms/%s throughput=%.0f/s", scenario, elapsed, msPerUnit, unit, throughput)
	if msPerUnit > performanceMaxMsPerUnit {
		t.Fatalf("%s exceeded %.3fms/%s budget: %.3fms/%s", scenario, performanceMaxMsPerUnit, unit, msPerUnit, unit)
	}
}

func waitForPerformanceBatch(t *testing.T, events []*abxbus.BaseEvent) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, event := range events {
		if _, err := event.Done(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPerformance50kEvents(t *testing.T) {
	totalEvents := 50_000
	batchSize := 512
	historySize := 512
	bus := abxbus.NewEventBus("Perf50kBus", &abxbus.EventBusOptions{
		MaxHistorySize: &historySize,
		MaxHistoryDrop: true,
	})

	var processed int64
	var checksum int64
	var expectedChecksum int64
	bus.On("PerfSimpleEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		atomic.AddInt64(&processed, 1)
		value, _ := event.Payload["value"].(int)
		batchID, _ := event.Payload["batch_id"].(int)
		atomic.AddInt64(&checksum, int64(value+batchID))
		return nil, nil
	}, nil)

	pending := make([]*abxbus.BaseEvent, 0, batchSize)
	started := time.Now()
	for i := 0; i < totalEvents; i++ {
		payload := map[string]any{"value": i, "batch_id": i % 17}
		expectedChecksum += int64(i + i%17)
		pending = append(pending, bus.Emit(abxbus.NewBaseEvent("PerfSimpleEvent", payload)))
		if len(pending) >= batchSize {
			waitForPerformanceBatch(t, pending)
			pending = pending[:0]
		}
	}
	if len(pending) > 0 {
		waitForPerformanceBatch(t, pending)
	}
	timeout := 10.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("timed out waiting for bus to become idle")
	}
	elapsed := time.Since(started)

	if got := atomic.LoadInt64(&processed); got != int64(totalEvents) {
		t.Fatalf("expected %d processed events, got %d", totalEvents, got)
	}
	if got := atomic.LoadInt64(&checksum); got != expectedChecksum {
		t.Fatalf("checksum mismatch: expected %d, got %d", expectedChecksum, got)
	}
	if bus.EventHistory.Size() > historySize {
		t.Fatalf("history should stay trimmed to %d, got %d", historySize, bus.EventHistory.Size())
	}
	assertPerformanceBudget(t, "50k events", totalEvents, elapsed, "event")
}

func TestPerformanceEphemeralBuses(t *testing.T) {
	totalBuses := 500
	eventsPerBus := 100
	historySize := 128
	var processed int64

	started := time.Now()
	for busIndex := 0; busIndex < totalBuses; busIndex++ {
		bus := abxbus.NewEventBus(fmt.Sprintf("PerfEphemeralBus%d", busIndex), &abxbus.EventBusOptions{
			MaxHistorySize: &historySize,
			MaxHistoryDrop: true,
		})
		bus.On("PerfEphemeralEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
			atomic.AddInt64(&processed, 1)
			return nil, nil
		}, nil)
		pending := make([]*abxbus.BaseEvent, 0, eventsPerBus)
		for eventIndex := 0; eventIndex < eventsPerBus; eventIndex++ {
			pending = append(pending, bus.Emit(abxbus.NewBaseEvent("PerfEphemeralEvent", nil)))
		}
		waitForPerformanceBatch(t, pending)
		timeout := 2.0
		if !bus.WaitUntilIdle(&timeout) {
			t.Fatal("timed out waiting for ephemeral bus")
		}
	}
	elapsed := time.Since(started)
	totalEvents := totalBuses * eventsPerBus
	if got := atomic.LoadInt64(&processed); got != int64(totalEvents) {
		t.Fatalf("expected %d processed events, got %d", totalEvents, got)
	}
	assertPerformanceBudget(t, "500 buses x 100 events", totalEvents, elapsed, "event")
}

func TestPerformanceSingleEventManyParallelHandlers(t *testing.T) {
	totalHandlers := 50_000
	historySize := 128
	bus := abxbus.NewEventBus("PerfFixedHandlersBus", &abxbus.EventBusOptions{
		MaxHistorySize:              &historySize,
		MaxHistoryDrop:              true,
		EventHandlerConcurrency:     abxbus.EventHandlerConcurrencyParallel,
		EventHandlerCompletion:      abxbus.EventHandlerCompletionAll,
		EventHandlerDetectFilePaths: ptrBool(false),
	})

	var handled int64
	for index := 0; index < totalHandlers; index++ {
		handlerID := fmt.Sprintf("perf-fixed-handler-%05d", index)
		bus.On("PerfFixedHandlersEvent", handlerID, func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
			atomic.AddInt64(&handled, 1)
			return nil, nil
		}, &abxbus.EventHandler{ID: handlerID})
	}

	started := time.Now()
	event := bus.Emit(abxbus.NewBaseEvent("PerfFixedHandlersEvent", nil))
	waitForPerformanceBatch(t, []*abxbus.BaseEvent{event})
	timeout := 10.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("timed out waiting for fixed-handler bus")
	}
	elapsed := time.Since(started)

	if got := atomic.LoadInt64(&handled); got != int64(totalHandlers) {
		t.Fatalf("expected %d handler calls, got %d", totalHandlers, got)
	}
	assertPerformanceBudget(t, "1 event x 50k parallel handlers", totalHandlers, elapsed, "handler")
}

func TestPerformanceOnOffChurn(t *testing.T) {
	totalEvents := 50_000
	historySize := 128
	bus := abxbus.NewEventBus("PerfOnOffChurnBus", &abxbus.EventBusOptions{
		MaxHistorySize: &historySize,
		MaxHistoryDrop: true,
	})

	var handled int64
	started := time.Now()
	for index := 0; index < totalEvents; index++ {
		handlerID := fmt.Sprintf("perf-one-off-handler-%05d", index)
		handler := bus.On("PerfOneOffEvent", handlerID, func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
			atomic.AddInt64(&handled, 1)
			return nil, nil
		}, &abxbus.EventHandler{ID: handlerID})
		event := bus.Emit(abxbus.NewBaseEvent("PerfOneOffEvent", nil))
		waitForPerformanceBatch(t, []*abxbus.BaseEvent{event})
		bus.Off("PerfOneOffEvent", handler)
	}
	timeout := 10.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("timed out waiting for one-off bus")
	}
	elapsed := time.Since(started)

	if got := atomic.LoadInt64(&handled); got != int64(totalEvents) {
		t.Fatalf("expected %d handler calls, got %d", totalEvents, got)
	}
	assertPerformanceBudget(t, "50k one-off handlers over 50k events", totalEvents, elapsed, "event")
}

func TestPerformanceWorstCaseForwardingQueueJumpTimeouts(t *testing.T) {
	totalEvents := 2_000
	eventTimeout := 0.0001
	historySize := 128
	parentBus := abxbus.NewEventBus("PerfWorstParentBus", &abxbus.EventBusOptions{
		MaxHistorySize: &historySize,
		MaxHistoryDrop: true,
		EventTimeout:   &eventTimeout,
	})
	childBus := abxbus.NewEventBus("PerfWorstChildBus", &abxbus.EventBusOptions{
		MaxHistorySize: &historySize,
		MaxHistoryDrop: true,
		EventTimeout:   &eventTimeout,
	})

	var parents int64
	var children int64
	var timedOut int64
	parentBus.On("WCParent", "forward", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		atomic.AddInt64(&parents, 1)
		child := childBus.Emit(abxbus.NewBaseEvent("WCChild", map[string]any{"parent": event.EventID, "iteration": event.Payload["iteration"]}))
		_, _ = child.Done(ctx)
		return nil, nil
	}, nil)
	childBus.On("WCChild", "child", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		atomic.AddInt64(&children, 1)
		iteration, _ := event.Payload["iteration"].(int)
		if iteration%10 != 0 {
			return "ok", nil
		}
		select {
		case <-time.After(2 * time.Millisecond):
			return "late", nil
		case <-ctx.Done():
			atomic.AddInt64(&timedOut, 1)
			return nil, ctx.Err()
		}
	}, nil)

	pending := make([]*abxbus.BaseEvent, 0, 128)
	started := time.Now()
	for index := 0; index < totalEvents; index++ {
		pending = append(pending, parentBus.Emit(abxbus.NewBaseEvent("WCParent", map[string]any{"iteration": index})))
		if len(pending) >= cap(pending) {
			waitForPerformanceBatchAllowErrors(t, pending)
			pending = pending[:0]
		}
	}
	if len(pending) > 0 {
		waitForPerformanceBatchAllowErrors(t, pending)
	}
	parentTimeout := 10.0
	childTimeout := 10.0
	if !parentBus.WaitUntilIdle(&parentTimeout) || !childBus.WaitUntilIdle(&childTimeout) {
		t.Fatal("timed out waiting for worst-case buses")
	}
	elapsed := time.Since(started)

	if atomic.LoadInt64(&parents) != int64(totalEvents) {
		t.Fatalf("expected %d parent handler calls, got %d", totalEvents, parents)
	}
	if atomic.LoadInt64(&children) == 0 {
		t.Fatal("expected child handlers to run")
	}
	if atomic.LoadInt64(&timedOut) == 0 {
		t.Fatal("expected at least one child timeout")
	}
	assertPerformanceBudget(t, "worst-case forwarding + timeouts", totalEvents, elapsed, "event")
}

func waitForPerformanceBatchAllowErrors(t *testing.T, events []*abxbus.BaseEvent) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, event := range events {
		_, _ = event.Done(ctx)
	}
}

func ptrBool(value bool) *bool {
	return &value
}

func TestPerformanceParallelFanoutBeatsSerialForIOBoundHandlers(t *testing.T) {
	serialElapsed, serialHandled := runFanoutBenchmark(t, abxbus.EventHandlerConcurrencySerial)
	parallelElapsed, parallelHandled := runFanoutBenchmark(t, abxbus.EventHandlerConcurrencyParallel)
	if serialHandled != parallelHandled {
		t.Fatalf("serial/parallel fanout handled different counts: %d != %d", serialHandled, parallelHandled)
	}
	t.Logf("fanout serial=%s parallel=%s", serialElapsed, parallelElapsed)
	if parallelElapsed >= serialElapsed {
		t.Fatalf("parallel fanout should beat serial for IO-bound handlers: serial=%s parallel=%s", serialElapsed, parallelElapsed)
	}
}

func runFanoutBenchmark(t *testing.T, mode abxbus.EventHandlerConcurrencyMode) (time.Duration, int64) {
	t.Helper()
	totalEvents := 800
	handlersPerEvent := 4
	sleepDuration := 1500 * time.Microsecond
	historySize := 128
	bus := abxbus.NewEventBus("PerfFanoutBus", &abxbus.EventBusOptions{
		MaxHistorySize:              &historySize,
		MaxHistoryDrop:              true,
		EventHandlerConcurrency:     mode,
		EventHandlerDetectFilePaths: ptrBool(false),
	})

	var handled int64
	for index := 0; index < handlersPerEvent; index++ {
		handlerID := fmt.Sprintf("fanout-handler-%d", index)
		bus.On("PerfFanoutEvent", handlerID, func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
			time.Sleep(sleepDuration)
			atomic.AddInt64(&handled, 1)
			return nil, nil
		}, &abxbus.EventHandler{ID: handlerID})
	}

	pending := make([]*abxbus.BaseEvent, 0, 40)
	started := time.Now()
	for index := 0; index < totalEvents; index++ {
		pending = append(pending, bus.Emit(abxbus.NewBaseEvent("PerfFanoutEvent", nil)))
		if len(pending) >= cap(pending) {
			waitForPerformanceBatch(t, pending)
			pending = pending[:0]
		}
	}
	if len(pending) > 0 {
		waitForPerformanceBatch(t, pending)
	}
	timeout := 10.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("timed out waiting for fanout bus")
	}
	return time.Since(started), atomic.LoadInt64(&handled)
}
