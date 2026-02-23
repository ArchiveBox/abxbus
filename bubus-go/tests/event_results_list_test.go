package bubus_test

import (
	"context"
	"errors"
	"testing"
	"time"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestEventResultsListOptions(t *testing.T) {
	bus := bubus.NewEventBus("ResultsListBus", nil)
	bus.On("ListEvent", "ok", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return "ok", nil }, nil)
	bus.On("ListEvent", "nil", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return nil, nil }, nil)
	bus.On("ListEvent", "err", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return nil, errors.New("boom") }, nil)

	e := bus.Emit(bubus.NewBaseEvent("ListEvent", nil))
	if _, err := e.Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	if _, err := e.EventResultsList(context.Background(), nil, nil); err == nil || err.Error() != "boom" {
		t.Fatalf("default options should raise first handler error, got %v", err)
	}
	if _, err := e.EventResultsList(context.Background(), nil, &bubus.EventResultsListOptions{RaiseIfAny: true, RaiseIfNone: false}); err == nil || err.Error() != "boom" {
		t.Fatalf("RaiseIfAny=true should surface boom, got %v", err)
	}

	vals, err := e.EventResultsList(context.Background(), nil, &bubus.EventResultsListOptions{RaiseIfAny: false, RaiseIfNone: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 1 || vals[0] != "ok" {
		t.Fatalf("unexpected values for non-raising mode: %#v", vals)
	}

	onlyNil, err := e.EventResultsList(context.Background(), func(result any, eventResult *bubus.EventResult) bool {
		return result == nil
	}, &bubus.EventResultsListOptions{RaiseIfAny: false, RaiseIfNone: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(onlyNil) != 2 {
		t.Fatalf("expected include predicate to capture nil results from nil+error handlers, got %#v", onlyNil)
	}

	if _, err := e.EventResultsList(context.Background(), func(result any, eventResult *bubus.EventResult) bool {
		return false
	}, &bubus.EventResultsListOptions{RaiseIfAny: false, RaiseIfNone: true}); err == nil {
		t.Fatal("RaiseIfNone=true should fail when include filter rejects all results")
	}

	slowBus := bubus.NewEventBus("ResultsListTimeoutBus", nil)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	slowBus.On("SlowListEvent", "slow", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		started <- struct{}{}
		<-release
		return "late", nil
	}, nil)
	slow := slowBus.Emit(bubus.NewBaseEvent("SlowListEvent", nil))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("timed out waiting for slow handler start")
	}
	tiny := 0.01
	if _, err := slow.EventResultsList(context.Background(), nil, &bubus.EventResultsListOptions{Timeout: &tiny, RaiseIfAny: false, RaiseIfNone: false}); err == nil {
		close(release)
		t.Fatal("expected timeout error from EventResultsList with timeout option")
	}
	close(release)
	if _, err := slow.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
}
