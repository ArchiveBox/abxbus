package abxbus_test

import (
	"context"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"
)

type contextKey string

func TestAwaitedDispatchPropagatesContextIntoHandlers(t *testing.T) {
	bus := abxbus.NewEventBus("ContextDispatchBus", nil)
	key := contextKey("request_id")
	seen := ""
	bus.On("ContextEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		seen, _ = ctx.Value(key).(string)
		return "ok", nil
	}, nil)

	ctx := context.WithValue(context.Background(), key, "req-123")
	if _, err := bus.EmitWithContext(ctx, abxbus.NewBaseEvent("ContextEvent", nil)).Now(); err != nil {
		t.Fatal(err)
	}
	if seen != "req-123" {
		t.Fatalf("handler did not receive dispatch context value, got %q", seen)
	}
}

func TestAwaitedChildDispatchPropagatesHandlerContext(t *testing.T) {
	bus := abxbus.NewEventBus("ContextChildBus", nil)
	key := contextKey("trace_id")
	childSeen := ""
	bus.On("ParentContextEvent", "parent", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		child := event.Emit(abxbus.NewBaseEvent("ChildContextEvent", nil))
		if _, err := child.Now(); err != nil {
			return nil, err
		}
		return "parent", nil
	}, nil)
	bus.On("ChildContextEvent", "child", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		childSeen, _ = ctx.Value(key).(string)
		return "child", nil
	}, nil)

	ctx := context.WithValue(context.Background(), key, "trace-456")
	if _, err := bus.EmitWithContext(ctx, abxbus.NewBaseEvent("ParentContextEvent", nil)).Now(); err != nil {
		t.Fatal(err)
	}
	if childSeen != "trace-456" {
		t.Fatalf("child handler did not receive parent handler context, got %q", childSeen)
	}
}

func TestWaitChildDispatchPreservesHandlerContext(t *testing.T) {
	bus := abxbus.NewEventBus("ContextEventCompletedChildBus", &abxbus.EventBusOptions{
		EventConcurrency:        abxbus.EventConcurrencyParallel,
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel,
	})
	key := contextKey("event_completed_trace_id")
	childSeen := ""
	bus.On("EventCompletedParentContextEvent", "parent", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		child := event.Emit(abxbus.NewBaseEvent("EventCompletedChildContextEvent", nil))
		if _, err := child.Wait(); err != nil {
			return nil, err
		}
		return "parent", nil
	}, nil)
	bus.On("EventCompletedChildContextEvent", "child", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		childSeen, _ = ctx.Value(key).(string)
		return "child", nil
	}, nil)

	ctx := context.WithValue(context.Background(), key, "trace-789")
	if _, err := bus.EmitWithContext(ctx, abxbus.NewBaseEvent("EventCompletedParentContextEvent", nil)).Now(); err != nil {
		t.Fatal(err)
	}
	waitTimeout := 2.0
	if !bus.WaitUntilIdle(&waitTimeout) {
		t.Fatal("timed out waiting for bus to become idle")
	}
	if childSeen != "trace-789" {
		t.Fatalf("child handler did not receive parent handler context through wait, got %q", childSeen)
	}
}
