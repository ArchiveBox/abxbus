package bubus

import (
	"context"
	"strings"
	"testing"
	"time"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestLogTreeShowsParentChildAndHandlerResults(t *testing.T) {
	bus := bubus.NewEventBus("TreeBus", nil)
	bus.On("RootEvent", "root", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		child := bus.Emit(bubus.NewBaseEvent("ChildEvent", nil))
		_, _ = child.Done(context.Background())
		return "root-ok", nil
	}, nil)
	bus.On("ChildEvent", "child", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		return "child-ok", nil
	}, nil)

	e := bus.Emit(bubus.NewBaseEvent("RootEvent", nil))
	if _, err := e.Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	out := bus.LogTree()
	if !strings.Contains(out, "RootEvent#") || !strings.Contains(out, "ChildEvent#") {
		t.Fatalf("expected root+child in tree, got:\n%s", out)
	}
	if !strings.Contains(out, "✅") || !strings.Contains(out, "root-ok") {
		t.Fatalf("expected handler result lines in tree, got:\n%s", out)
	}
}

func TestLogTreeIncludesTimedOutResultErrors(t *testing.T) {
	short := 0.01
	bus := bubus.NewEventBus("TimeoutTreeBus", &bubus.EventBusOptions{EventTimeout: &short})
	bus.On("SlowEvent", "slow", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		time.Sleep(30 * time.Millisecond)
		return "too-late", nil
	}, nil)
	e := bus.Emit(bubus.NewBaseEvent("SlowEvent", nil))
	_, _ = e.Done(context.Background())
	out := bus.LogTree()
	if !strings.Contains(out, "SlowEvent#") || !strings.Contains(out, "slow") {
		t.Fatalf("expected slow event/handler lines in tree, got:\n%s", out)
	}
}
