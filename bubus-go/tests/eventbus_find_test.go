package bubus_test

import (
	"context"
	"testing"
	"time"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestFindHistoryAndFuture(t *testing.T) {
	bus := bubus.NewEventBus("FindBus", nil)
	seed := bus.Emit(bubus.NewBaseEvent("ResponseEvent", map[string]any{"request_id": "abc"}))
	_, _ = seed.Done(context.Background())

	match, err := bus.Find("ResponseEvent", func(e *bubus.BaseEvent) bool {
		return e.Payload["request_id"] == "abc"
	}, &bubus.FindOptions{Past: true, Future: false})
	if err != nil {
		t.Fatal(err)
	}
	if match == nil || match.EventID != seed.EventID {
		t.Fatal("expected history find to match seeded event")
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		bus.Emit(bubus.NewBaseEvent("FutureEvent", nil))
	}()
	future, err := bus.Find("FutureEvent", nil, &bubus.FindOptions{Past: false, Future: 0.5})
	if err != nil {
		t.Fatal(err)
	}
	if future == nil {
		t.Fatal("expected future find to resolve")
	}
}

func TestFindChildOfFiltering(t *testing.T) {
	bus := bubus.NewEventBus("FindChildBus", nil)
	parent := bus.Emit(bubus.NewBaseEvent("Parent", nil))
	bus.On("Parent", "emit_child", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		child := bus.Emit(bubus.NewBaseEvent("Child", nil))
		_, _ = child.Done(ctx)
		return nil, nil
	}, nil)
	_, _ = parent.Done(context.Background())

	child, err := bus.Find("Child", nil, &bubus.FindOptions{Past: true, Future: false, ChildOf: parent})
	if err != nil {
		t.Fatal(err)
	}
	if child == nil {
		t.Fatal("expected child find to match")
	}
}
