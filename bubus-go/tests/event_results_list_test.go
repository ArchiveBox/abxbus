package bubus

import (
	"context"
	"errors"
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestEventResultsListOptions(t *testing.T) {
	bus := bubus.NewEventBus("ResultsListBus", nil)
	bus.On("ListEvent", "ok", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return "ok", nil }, nil)
	bus.On("ListEvent", "nil", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return nil, nil }, nil)
	bus.On("ListEvent", "err", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return nil, errors.New("boom") }, nil)
	e := bus.Emit(bubus.NewBaseEvent("ListEvent", nil))
	_, _ = e.Done(context.Background())
	vals, err := e.EventResultsList(context.Background(), nil, &bubus.EventResultsListOptions{RaiseIfAny: false, RaiseIfNone: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 1 || vals[0] != "ok" {
		t.Fatalf("unexpected values: %#v", vals)
	}
}
