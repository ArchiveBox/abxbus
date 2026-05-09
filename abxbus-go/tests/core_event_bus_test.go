package abxbus_test

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"
)

type GoCoreEvent struct {
	Value int `json:"value"`
}

type GoCoreErrorEvent struct {
	Value int `json:"value"`
}

type GoCoreBenchEvent struct {
	Value int `json:"value"`
}

func uniqueCoreBusName(prefix string) string {
	return prefix + strconv.FormatInt(time.Now().UnixNano(), 10)
}

func TestRustCoreEventBusRunsGoHandlerViaCoreSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bus, err := abxbus.NewRustCoreEventBus(ctx, uniqueCoreBusName("GoCoreBus"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bus.Stop(ctx) }()

	abxbus.OnTyped[GoCoreEvent, int](bus, "GoCoreEvent", "typed", func(event GoCoreEvent, ctx context.Context) (int, error) {
		return event.Value + 1, nil
	}, nil)

	completed, err := bus.Emit(GoCoreEvent{Value: 41})
	if err != nil {
		t.Fatal(err)
	}
	if completed.EventStatus != "completed" {
		t.Fatalf("unexpected completion state: %+v", completed)
	}
}

func TestRustCoreEventBusCommitsGoHandlerErrorThroughCore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bus, err := abxbus.NewRustCoreEventBus(ctx, uniqueCoreBusName("GoCoreErrorBus"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bus.Stop(ctx) }()

	abxbus.OnTyped[GoCoreErrorEvent, string](bus, "GoCoreErrorEvent", "typed", func(event GoCoreErrorEvent, ctx context.Context) (string, error) {
		return "", errors.New("go boom")
	}, nil)

	completed, err := bus.Emit(GoCoreErrorEvent{Value: 41})
	if err != nil {
		t.Fatal(err)
	}
	if completed.EventStatus != "completed" {
		t.Fatalf("unexpected completion state: %+v", completed)
	}
}

func TestRustCoreEventBusFindAndFilterReadFromCoreSnapshots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bus, err := abxbus.NewRustCoreEventBus(ctx, uniqueCoreBusName("GoCoreQueryBus"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bus.Stop(ctx) }()

	abxbus.OnTyped[GoCoreEvent, int](bus, "GoCoreEvent", "typed", func(event GoCoreEvent, ctx context.Context) (int, error) {
		return event.Value + 10, nil
	}, nil)
	first, err := bus.Emit(GoCoreEvent{Value: 1})
	if err != nil {
		t.Fatal(err)
	}
	latest, err := bus.Emit(GoCoreEvent{Value: 2})
	if err != nil {
		t.Fatal(err)
	}
	found, err := bus.Find("GoCoreEvent", nil, &abxbus.FindOptions{Equals: map[string]any{"value": 2}})
	if err != nil {
		t.Fatal(err)
	}
	limit := 1
	matches, err := bus.Filter("GoCoreEvent", nil, &abxbus.FilterOptions{Limit: &limit})
	if err != nil {
		t.Fatal(err)
	}

	if found == nil || found.EventID != latest.EventID || int(found.Payload["value"].(float64)) != 2 {
		t.Fatalf("unexpected found event: %#v latest=%s first=%s", found, latest.EventID, first.EventID)
	}
	if len(found.EventResults) != 1 {
		t.Fatalf("expected found event result snapshot, got %#v", found.EventResults)
	}
	for _, result := range found.EventResults {
		if int(result.Result.(float64)) != 12 {
			t.Fatalf("unexpected result snapshot: %#v", result)
		}
	}
	if len(matches) != 1 || matches[0].EventID != latest.EventID {
		t.Fatalf("unexpected matches: %#v", matches)
	}
}

func TestRustCoreEventBusSameNameSharesBusWithoutExplicitID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	busName := uniqueCoreBusName("GoCoreSharedRuntimeBus")
	emitter, err := abxbus.NewRustCoreEventBus(ctx, busName, abxbus.RustCoreEventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = emitter.Stop(ctx) }()
	worker, err := abxbus.NewRustCoreEventBus(ctx, busName, abxbus.RustCoreEventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = worker.Disconnect() }()

	if emitter.BusID != worker.BusID {
		t.Fatalf("same bus name should derive same bus id: %s != %s", emitter.BusID, worker.BusID)
	}
	abxbus.OnTyped[GoCoreEvent, int](emitter, "GoCoreEvent", "local", func(event GoCoreEvent, ctx context.Context) (int, error) {
		return event.Value + 1, nil
	}, nil)
	abxbus.OnTyped[GoCoreEvent, int](worker, "GoCoreEvent", "remote", func(event GoCoreEvent, ctx context.Context) (int, error) {
		return event.Value + 10, nil
	}, nil)

	completed, err := emitter.Emit(GoCoreEvent{Value: 5})
	if err != nil {
		t.Fatal(err)
	}
	results := make([]int, 0, len(completed.EventResults))
	for _, result := range completed.EventResults {
		results = append(results, int(result.Result.(float64)))
	}
	sort.Ints(results)
	if !reflect.DeepEqual(results, []int{6, 15}) {
		t.Fatalf("expected local and remote handler results, got %#v", results)
	}
}

func BenchmarkRustCoreEventBusEmit(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bus, err := abxbus.NewRustCoreEventBus(ctx, uniqueCoreBusName("GoCoreBenchBus"))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = bus.Stop(ctx) }()

	abxbus.OnTyped[GoCoreBenchEvent, int](bus, "GoCoreBenchEvent", "typed", func(event GoCoreBenchEvent, ctx context.Context) (int, error) {
		return event.Value + 1, nil
	}, nil)
	for i := 0; i < 20; i++ {
		if _, err := bus.Emit(GoCoreBenchEvent{Value: i}); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bus.Emit(GoCoreBenchEvent{Value: i}); err != nil {
			b.Fatal(err)
		}
	}
}
