package abxbus_test

import (
	"context"
	abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestGlobalSerialAcrossBuses(t *testing.T) {
	b1 := abxbus.NewEventBus("B1", &abxbus.EventBusOptions{EventConcurrency: abxbus.EventConcurrencyGlobalSerial})
	b2 := abxbus.NewEventBus("B2", &abxbus.EventBusOptions{EventConcurrency: abxbus.EventConcurrencyGlobalSerial})

	var mu sync.Mutex
	inFlight := 0
	maxInFlight := 0
	order := []string{}
	h := func(busLabel string) func(*abxbus.BaseEvent, context.Context) (any, error) {
		return func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
			seq := int(e.Payload["n"].(int))
			mu.Lock()
			inFlight++
			if inFlight > maxInFlight {
				maxInFlight = inFlight
			}
			order = append(order, busLabel+":start:"+strconv.Itoa(seq))
			mu.Unlock()

			time.Sleep(5 * time.Millisecond)

			mu.Lock()
			order = append(order, busLabel+":end:"+strconv.Itoa(seq))
			inFlight--
			mu.Unlock()
			return nil, nil
		}
	}

	b1.OnEventName("Evt", "h1", h("b1"), nil)
	b2.OnEventName("Evt", "h2", h("b2"), nil)

	for i := 1; i <= 3; i++ {
		b1.EmitEventName("Evt", map[string]any{"n": i})
		b2.EmitEventName("Evt", map[string]any{"n": i})
	}

	timeout := 2.0
	if !b1.WaitUntilIdle(&timeout) {
		t.Fatal("b1 did not become idle")
	}
	if !b2.WaitUntilIdle(&timeout) {
		t.Fatal("b2 did not become idle")
	}

	if maxInFlight != 1 {
		t.Fatalf("expected strict global serial execution (max in flight=1), got %d, order=%v", maxInFlight, order)
	}

	seenB1 := []int{}
	seenB2 := []int{}
	for _, entry := range order {
		if len(entry) < 9 || entry[3:8] != "start" {
			continue
		}
		if entry[:2] == "b1" {
			seenB1 = append(seenB1, int(entry[len(entry)-1]-'0'))
		}
		if entry[:2] == "b2" {
			seenB2 = append(seenB2, int(entry[len(entry)-1]-'0'))
		}
	}
	if len(seenB1) != 3 || seenB1[0] != 1 || seenB1[1] != 2 || seenB1[2] != 3 {
		t.Fatalf("expected per-bus FIFO order for b1, got %v", seenB1)
	}
	if len(seenB2) != 3 || seenB2[0] != 1 || seenB2[1] != 2 || seenB2[2] != 3 {
		t.Fatalf("expected per-bus FIFO order for b2, got %v", seenB2)
	}

	b1.Destroy()
	b2.Destroy()
}

func TestGlobalSerialAwaitedChildJumpsAheadOfQueuedEventsAcrossBuses(t *testing.T) {
	busA := abxbus.NewEventBus("GlobalSerialParent", &abxbus.EventBusOptions{EventConcurrency: abxbus.EventConcurrencyGlobalSerial})
	busB := abxbus.NewEventBus("GlobalSerialChild", &abxbus.EventBusOptions{EventConcurrency: abxbus.EventConcurrencyGlobalSerial})
	defer busA.Destroy()
	defer busB.Destroy()

	var mu sync.Mutex
	order := []string{}
	record := func(value string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, value)
	}

	busB.OnEventName("ChildEvent", "child", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		record("child_start")
		time.Sleep(5 * time.Millisecond)
		record("child_end")
		return "child", nil
	}, nil)
	busB.OnEventName("QueuedEvent", "queued", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		record("queued_start")
		time.Sleep(time.Millisecond)
		record("queued_end")
		return "queued", nil
	}, nil)
	busA.OnEventName("ParentEvent", "parent", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		record("parent_start")
		busB.EmitEventName("QueuedEvent", nil)
		child := e.EmitEventName("ChildEvent", nil)
		busB.Emit(child)
		record("child_dispatched")
		if _, err := child.Now(); err != nil {
			return nil, err
		}
		record("child_awaited")
		record("parent_end")
		return "parent", nil
	}, nil)

	parent := busA.EmitEventName("ParentEvent", nil)
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}
	timeout := 2.0
	if !busB.WaitUntilIdle(&timeout) {
		t.Fatal("busB did not become idle")
	}

	mu.Lock()
	defer mu.Unlock()
	childStart := indexOfLockingOrder(order, "child_start")
	childEnd := indexOfLockingOrder(order, "child_end")
	queuedStart := indexOfLockingOrder(order, "queued_start")
	if childStart == -1 || childEnd == -1 || queuedStart == -1 {
		t.Fatalf("expected child and queued handlers to run, order=%v", order)
	}
	if !(childStart < queuedStart && childEnd < queuedStart) {
		t.Fatalf("awaited child should queue-jump ahead of older queued event, order=%v", order)
	}
}

func TestEventConcurrencyBusSerialSerializesPerBusButOverlapsAcrossBuses(t *testing.T) {
	busA := abxbus.NewEventBus("BusSerialA", &abxbus.EventBusOptions{EventConcurrency: abxbus.EventConcurrencyBusSerial})
	busB := abxbus.NewEventBus("BusSerialB", &abxbus.EventBusOptions{EventConcurrency: abxbus.EventConcurrencyBusSerial})
	defer busA.Destroy()
	defer busB.Destroy()

	startedA := make(chan struct{}, 2)
	startedB := make(chan struct{}, 2)
	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	var mu sync.Mutex
	inFlightByBus := map[string]int{"a": 0, "b": 0}
	maxByBus := map[string]int{"a": 0, "b": 0}
	globalInFlight := 0
	maxGlobalInFlight := 0

	handler := func(label string, started chan struct{}, release chan struct{}) func(*abxbus.BaseEvent, context.Context) (any, error) {
		return func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
			mu.Lock()
			inFlightByBus[label]++
			if inFlightByBus[label] > maxByBus[label] {
				maxByBus[label] = inFlightByBus[label]
			}
			globalInFlight++
			if globalInFlight > maxGlobalInFlight {
				maxGlobalInFlight = globalInFlight
			}
			mu.Unlock()
			started <- struct{}{}
			<-release
			mu.Lock()
			inFlightByBus[label]--
			globalInFlight--
			mu.Unlock()
			return label, nil
		}
	}
	busA.OnEventName("Evt", "a", handler("a", startedA, releaseA), nil)
	busB.OnEventName("Evt", "b", handler("b", startedB, releaseB), nil)

	firstA := busA.EmitEventName("Evt", map[string]any{"n": 1})
	secondA := busA.EmitEventName("Evt", map[string]any{"n": 2})
	firstB := busB.EmitEventName("Evt", map[string]any{"n": 1})

	select {
	case <-startedA:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first bus A event")
	}
	select {
	case <-startedB:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first bus B event")
	}
	select {
	case <-startedA:
		t.Fatal("second bus A event should not start while first bus A event holds bus-serial lock")
	case <-time.After(30 * time.Millisecond):
	}

	mu.Lock()
	if maxGlobalInFlight < 2 {
		mu.Unlock()
		t.Fatal("bus-serial events on different buses should overlap")
	}
	if maxByBus["a"] != 1 || maxByBus["b"] != 1 {
		mu.Unlock()
		t.Fatalf("bus-serial should keep per-bus max in-flight at 1, got %#v", maxByBus)
	}
	mu.Unlock()

	close(releaseA)
	close(releaseB)
	for _, event := range []*abxbus.BaseEvent{firstA, secondA, firstB} {
		if _, err := event.Now(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEventConcurrencyParallelAllowsSameBusEventsToOverlap(t *testing.T) {
	bus := abxbus.NewEventBus("ParallelEventsBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyParallel,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	defer bus.Destroy()

	started := make(chan int, 2)
	release := make(chan struct{})
	var mu sync.Mutex
	inFlight := 0
	maxInFlight := 0
	bus.OnEventName("Evt", "handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()
		started <- e.Payload["n"].(int)
		<-release
		mu.Lock()
		inFlight--
		mu.Unlock()
		return e.Payload["n"], nil
	}, nil)

	first := bus.EmitEventName("Evt", map[string]any{"n": 1})
	second := bus.EmitEventName("Evt", map[string]any{"n": 2})
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			close(release)
			t.Fatal("timed out waiting for parallel event start")
		}
	}
	mu.Lock()
	maxSeen := maxInFlight
	mu.Unlock()
	if maxSeen < 2 {
		close(release)
		t.Fatalf("expected parallel event overlap, max in-flight=%d", maxSeen)
	}
	close(release)

	if _, err := first.Now(); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Now(); err != nil {
		t.Fatal(err)
	}
}

func TestEventConcurrencyOverrideParallelBeatsBusSerialDefault(t *testing.T) {
	bus := abxbus.NewEventBus("OverrideParallelBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyBusSerial,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	defer bus.Destroy()

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var mu sync.Mutex
	inFlight := 0
	maxInFlight := 0
	bus.OnEventName("Evt", "handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()
		started <- struct{}{}
		<-release
		mu.Lock()
		inFlight--
		mu.Unlock()
		return nil, nil
	}, nil)

	first := abxbus.NewBaseEvent("Evt", map[string]any{"n": 1})
	first.EventConcurrency = abxbus.EventConcurrencyParallel
	second := abxbus.NewBaseEvent("Evt", map[string]any{"n": 2})
	second.EventConcurrency = abxbus.EventConcurrencyParallel
	emittedFirst := bus.Emit(first)
	emittedSecond := bus.Emit(second)

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			close(release)
			t.Fatal("timed out waiting for override-parallel event start")
		}
	}
	mu.Lock()
	maxSeen := maxInFlight
	mu.Unlock()
	if maxSeen < 2 {
		close(release)
		t.Fatalf("event-level parallel should override bus-serial default, max in-flight=%d", maxSeen)
	}
	close(release)

	if _, err := emittedFirst.Now(); err != nil {
		t.Fatal(err)
	}
	if _, err := emittedSecond.Now(); err != nil {
		t.Fatal(err)
	}
}

func TestEventConcurrencyOverrideBusSerialBeatsBusParallelDefault(t *testing.T) {
	bus := abxbus.NewEventBus("OverrideBusSerialBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyParallel,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	defer bus.Destroy()

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var mu sync.Mutex
	inFlight := 0
	maxInFlight := 0
	bus.OnEventName("Evt", "handler", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()
		started <- struct{}{}
		<-release
		mu.Lock()
		inFlight--
		mu.Unlock()
		return nil, nil
	}, nil)

	first := abxbus.NewBaseEvent("Evt", map[string]any{"n": 1})
	first.EventConcurrency = abxbus.EventConcurrencyBusSerial
	second := abxbus.NewBaseEvent("Evt", map[string]any{"n": 2})
	second.EventConcurrency = abxbus.EventConcurrencyBusSerial
	emittedFirst := bus.Emit(first)
	emittedSecond := bus.Emit(second)

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("timed out waiting for first bus-serial override event")
	}
	select {
	case <-started:
		close(release)
		t.Fatal("second bus-serial override event should not overlap first")
	case <-time.After(30 * time.Millisecond):
	}
	mu.Lock()
	maxSeen := maxInFlight
	mu.Unlock()
	if maxSeen != 1 {
		close(release)
		t.Fatalf("event-level bus-serial should override parallel bus default, max in-flight=%d", maxSeen)
	}
	close(release)

	if _, err := emittedFirst.Now(); err != nil {
		t.Fatal(err)
	}
	if _, err := emittedSecond.Now(); err != nil {
		t.Fatal(err)
	}
}

func indexOfLockingOrder(values []string, target string) int {
	for i, value := range values {
		if value == target {
			return i
		}
	}
	return -1
}

// Folded from eventbus_concurrency_test.go to keep test layout class-based.
func TestHandlerConcurrencyParallelStartsBoth(t *testing.T) {
	bus := abxbus.NewEventBus("ParallelBus", &abxbus.EventBusOptions{EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel})
	var mu sync.Mutex
	count := 0
	gate := make(chan struct{})
	started := make(chan struct{}, 2)

	for i := 0; i < 2; i++ {
		bus.OnEventName("Evt", "h", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
			mu.Lock()
			count++
			mu.Unlock()
			started <- struct{}{}
			<-gate
			return nil, nil
		}, nil)
	}

	e := bus.EmitEventName("Evt", nil)
	deadline := time.After(2 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-deadline:
			close(gate)
			t.Fatalf("timed out waiting for parallel handlers to start")
		}
	}

	mu.Lock()
	c := count
	mu.Unlock()
	if c != 2 {
		close(gate)
		t.Fatalf("expected 2 starts, got %d", c)
	}

	close(gate)
	if _, err := e.Now(); err != nil {
		t.Fatal(err)
	}
}

func TestEventHandlerConcurrencyPerEventOverrideControlsExecutionMode(t *testing.T) {
	bus := abxbus.NewEventBus("HandlerConcurrencyOverrideBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	var mu sync.Mutex
	inFlight := 0
	maxInFlightByMode := map[string]int{}
	release := make(chan struct{})
	started := make(chan string, 4)

	handler := func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		mode := e.Payload["mode"].(string)
		mu.Lock()
		inFlight++
		if inFlight > maxInFlightByMode[mode] {
			maxInFlightByMode[mode] = inFlight
		}
		mu.Unlock()
		started <- mode
		<-release
		mu.Lock()
		inFlight--
		mu.Unlock()
		return mode, nil
	}
	bus.OnEventName("Evt", "first", handler, nil)
	bus.OnEventName("Evt", "second", handler, nil)

	parallelEvent := abxbus.NewBaseEvent("Evt", map[string]any{"mode": "parallel"})
	parallelEvent.EventHandlerConcurrency = abxbus.EventHandlerConcurrencyParallel
	emittedParallel := bus.Emit(parallelEvent)
	for i := 0; i < 2; i++ {
		select {
		case mode := <-started:
			if mode != "parallel" {
				close(release)
				t.Fatalf("unexpected mode %s", mode)
			}
		case <-time.After(2 * time.Second):
			close(release)
			t.Fatal("timed out waiting for parallel handlers")
		}
	}

	close(release)
	if _, err := emittedParallel.Now(); err != nil {
		t.Fatal(err)
	}
	if maxInFlightByMode["parallel"] < 2 {
		t.Fatalf("expected per-event parallel override to run handlers concurrently, max=%d", maxInFlightByMode["parallel"])
	}

	release = make(chan struct{})
	started = make(chan string, 4)
	inFlight = 0
	serialEvent := abxbus.NewBaseEvent("Evt", map[string]any{"mode": "serial"})
	serialEvent.EventHandlerConcurrency = abxbus.EventHandlerConcurrencySerial
	emittedSerial := bus.Emit(serialEvent)
	select {
	case mode := <-started:
		if mode != "serial" {
			close(release)
			t.Fatalf("unexpected mode %s", mode)
		}
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("timed out waiting for serial handler")
	}
	select {
	case <-started:
		close(release)
		t.Fatal("second serial handler should not start before first is released")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if _, err := emittedSerial.Now(); err != nil {
		t.Fatal(err)
	}
	if maxInFlightByMode["serial"] != 1 {
		t.Fatalf("expected per-event serial override to keep max in-flight at 1, got %d", maxInFlightByMode["serial"])
	}
}
