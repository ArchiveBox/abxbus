package bubus_test

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
		if _, err := child.Done(context.Background()); err != nil {
			return nil, err
		}
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
	if !strings.Contains(out, "✅") || !strings.Contains(out, "root-ok") || !strings.Contains(out, "child-ok") {
		t.Fatalf("expected handler result lines in tree, got:\n%s", out)
	}
}

func TestLogTreeIncludesTimedOutResultErrors(t *testing.T) {
	short := 0.01
	bus := bubus.NewEventBus("TimeoutTreeBus", &bubus.EventBusOptions{EventTimeout: &short})
	bus.On("SlowEvent", "slow", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		select {
		case <-time.After(30 * time.Millisecond):
			return "too-late", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil)
	e := bus.Emit(bubus.NewBaseEvent("SlowEvent", nil))
	if _, err := e.EventResult(context.Background()); err == nil {
		t.Fatal("expected timeout error from event result")
	}
	out := bus.LogTree()
	if !strings.Contains(out, "SlowEvent#") || !strings.Contains(out, "slow") {
		t.Fatalf("expected slow event/handler lines in tree, got:\n%s", out)
	}
	if !strings.Contains(out, "❌") {
		t.Fatalf("expected failed handler indicator in tree, got:\n%s", out)
	}
	if !strings.Contains(out, "timed out") && !strings.Contains(out, "Aborted running handler") && !strings.Contains(out, "Cancelled pending handler") {
		t.Fatalf("expected timeout-related error details in tree, got:\n%s", out)
	}
}
