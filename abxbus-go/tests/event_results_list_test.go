package abxbus_test

import (
	"context"
	"errors"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestEventResultsListOptions(t *testing.T) {
	bus := abxbus.NewEventBus("ResultsListBus", nil)
	bus.On("ListEvent", "ok", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) { return "ok", nil }, nil)
	bus.On("ListEvent", "nil", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) { return nil, nil }, nil)
	bus.On("ListEvent", "err", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) { return nil, errors.New("boom") }, nil)

	e := bus.Emit(abxbus.NewBaseEvent("ListEvent", nil))
	if _, err := e.Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	if _, err := e.EventResultsList(context.Background(), nil, nil); err == nil || err.Error() != "boom" {
		t.Fatalf("default options should raise first handler error, got %v", err)
	}
	if _, err := e.EventResultsList(context.Background(), nil, &abxbus.EventResultsListOptions{RaiseIfAny: true, RaiseIfNone: false}); err == nil || err.Error() != "boom" {
		t.Fatalf("RaiseIfAny=true should surface boom, got %v", err)
	}

	vals, err := e.EventResultsList(context.Background(), nil, &abxbus.EventResultsListOptions{RaiseIfAny: false, RaiseIfNone: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 1 || vals[0] != "ok" {
		t.Fatalf("unexpected values for non-raising mode: %#v", vals)
	}

	onlyNil, err := e.EventResultsList(context.Background(), func(result any, eventResult *abxbus.EventResult) bool {
		return result == nil
	}, &abxbus.EventResultsListOptions{RaiseIfAny: false, RaiseIfNone: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(onlyNil) != 2 {
		t.Fatalf("expected include predicate to capture nil results from nil+error handlers, got %#v", onlyNil)
	}

	if _, err := e.EventResultsList(context.Background(), func(result any, eventResult *abxbus.EventResult) bool {
		return false
	}, &abxbus.EventResultsListOptions{RaiseIfAny: false, RaiseIfNone: true}); err == nil {
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
	if _, err := slow.EventResultsList(context.Background(), nil, &abxbus.EventResultsListOptions{Timeout: &tiny, RaiseIfAny: false, RaiseIfNone: false}); err == nil {
		close(release)
		t.Fatal("expected timeout error from EventResultsList with timeout option")
	}
	close(release)
	if _, err := slow.Done(context.Background()); err != nil {
		t.Fatal(err)
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
	first, err := e.EventResult(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first != "winner" {
		t.Fatalf("expected EventResult to return first non-nil result in registration order, got %#v", first)
	}

	values, err := e.EventResultsList(context.Background(), nil, &abxbus.EventResultsListOptions{RaiseIfAny: false, RaiseIfNone: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || values[0] != "winner" || values[1] != "late" {
		t.Fatalf("expected filtered values in registration order, got %#v", values)
	}

	rawValues, err := e.EventResultsList(context.Background(), func(result any, eventResult *abxbus.EventResult) bool {
		return true
	}, &abxbus.EventResultsListOptions{RaiseIfAny: false, RaiseIfNone: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(rawValues) != 3 || rawValues[0] != nil || rawValues[1] != "winner" || rawValues[2] != "late" {
		t.Fatalf("expected raw values in registration order, got %#v", rawValues)
	}
}
