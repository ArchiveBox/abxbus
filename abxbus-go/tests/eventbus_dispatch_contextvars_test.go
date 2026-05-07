package abxbus_test

import (
	"context"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

type contextKey string

func TestAwaitedDispatchPropagatesContextIntoHandlers(t *testing.T) {
	bus := abxbus.NewEventBus("ContextDispatchBus", nil)
	key := contextKey("request_id")
	seen := ""
	bus.On("ContextEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		seen, _ = ctx.Value(key).(string)
		return "ok", nil
	}, nil)

	ctx := context.WithValue(context.Background(), key, "req-123")
	if _, err := bus.Emit(abxbus.NewBaseEvent("ContextEvent", nil)).Done(ctx); err != nil {
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
	bus.On("ParentContextEvent", "parent", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		child := event.Emit(abxbus.NewBaseEvent("ChildContextEvent", nil))
		if _, err := child.Done(ctx); err != nil {
			return nil, err
		}
		return "parent", nil
	}, nil)
	bus.On("ChildContextEvent", "child", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		childSeen, _ = ctx.Value(key).(string)
		return "child", nil
	}, nil)

	ctx := context.WithValue(context.Background(), key, "trace-456")
	if _, err := bus.Emit(abxbus.NewBaseEvent("ParentContextEvent", nil)).Done(ctx); err != nil {
		t.Fatal(err)
	}
	if childSeen != "trace-456" {
		t.Fatalf("child handler did not receive parent handler context, got %q", childSeen)
	}
}
