package abxbus_test

import (
	"context"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestSameNameEventBusesKeepIndependentIDsHandlersAndHistory(t *testing.T) {
	first := abxbus.NewEventBus("DuplicateNameBus", nil)
	second := abxbus.NewEventBus("DuplicateNameBus", nil)
	if first.ID == second.ID {
		t.Fatal("same-name buses should still have distinct ids")
	}

	first.On("NameConflictEvent", "first", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "first", nil
	}, nil)
	second.On("NameConflictEvent", "second", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "second", nil
	}, nil)

	firstResult, err := first.Emit(abxbus.NewBaseEvent("NameConflictEvent", nil)).EventResult(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	secondResult, err := second.Emit(abxbus.NewBaseEvent("NameConflictEvent", nil)).EventResult(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if firstResult != "first" || secondResult != "second" {
		t.Fatalf("same-name bus handlers crossed: first=%#v second=%#v", firstResult, secondResult)
	}
	if first.EventHistory.Size() != 1 || second.EventHistory.Size() != 1 {
		t.Fatalf("same-name bus histories should remain isolated, got %d and %d", first.EventHistory.Size(), second.EventHistory.Size())
	}
}
