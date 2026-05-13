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
	bus.OnEventName("ContextEvent", "handler", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		seen, _ = ctx.Value(key).(string)
		return "ok", nil
	}, nil)

	ctx := context.WithValue(context.Background(), key, "req-123")
	if _, err := bus.EmitEventNameWithContext(ctx, "ContextEvent", nil).Now(); err != nil {
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
	bus.OnEventName("ParentContextEvent", "parent", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		child := event.EmitEventName("ChildContextEvent", nil)
		if _, err := child.Now(); err != nil {
			return nil, err
		}
		return "parent", nil
	}, nil)
	bus.OnEventName("ChildContextEvent", "child", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		childSeen, _ = ctx.Value(key).(string)
		return "child", nil
	}, nil)

	ctx := context.WithValue(context.Background(), key, "trace-456")
	if _, err := bus.EmitEventNameWithContext(ctx, "ParentContextEvent", nil).Now(); err != nil {
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
	bus.OnEventName("EventCompletedParentContextEvent", "parent", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		child := event.EmitEventName("EventCompletedChildContextEvent", nil)
		if _, err := child.Wait(); err != nil {
			return nil, err
		}
		return "parent", nil
	}, nil)
	bus.OnEventName("EventCompletedChildContextEvent", "child", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		childSeen, _ = ctx.Value(key).(string)
		return "child", nil
	}, nil)

	ctx := context.WithValue(context.Background(), key, "trace-789")
	if _, err := bus.EmitEventNameWithContext(ctx, "EventCompletedParentContextEvent", nil).Now(); err != nil {
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
