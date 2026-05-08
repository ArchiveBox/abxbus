package abxbus_test

import (
	"context"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestFirstSetsEventHandlerCompletionToFirst(t *testing.T) {
	bus := abxbus.NewEventBus("FirstShortcutBus", &abxbus.EventBusOptions{
		EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial,
	})
	secondCalled := false
	bus.On("FirstShortcutEvent", "empty", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return nil, nil
	}, nil)
	bus.On("FirstShortcutEvent", "winner", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return "winner", nil
	}, nil)
	bus.On("FirstShortcutEvent", "late", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		secondCalled = true
		return "late", nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("FirstShortcutEvent", nil))
	if event.EventHandlerCompletion != "" {
		t.Fatalf("event_handler_completion should start unset on event, got %s", event.EventHandlerCompletion)
	}
	result, err := event.First(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result != "winner" {
		t.Fatalf("expected first non-nil result, got %#v", result)
	}
	if event.EventHandlerCompletion != abxbus.EventHandlerCompletionFirst {
		t.Fatalf("First should set event_handler_completion=first, got %s", event.EventHandlerCompletion)
	}
	if secondCalled {
		t.Fatal("First should skip later serial handlers after the first non-nil result")
	}
}
