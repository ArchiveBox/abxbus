package abxbus_test

import (
	"context"
	"encoding/json"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestEventBusCrossRuntimeJSONFeaturesUseCanonicalShapes(t *testing.T) {
	detectPaths := false
	bus := abxbus.NewEventBus("CrossRuntimeFeatureBus", &abxbus.EventBusOptions{
		EventHandlerDetectFilePaths: &detectPaths,
	})
	handler := bus.On("CrossRuntimeFeatureEvent", "handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return map[string]any{"ok": true}, nil
	}, nil)
	event := abxbus.NewBaseEvent("CrossRuntimeFeatureEvent", map[string]any{"label": "go"})
	event.EventResultType = map[string]any{
		"type":       "object",
		"properties": map[string]any{"ok": map[string]any{"type": "boolean"}},
	}
	if _, err := bus.Emit(event).Done(context.Background()); err != nil {
		t.Fatal(err)
	}

	data, err := bus.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatal(err)
	}
	handlers := payload["handlers"].(map[string]any)
	handlersByKey := payload["handlers_by_key"].(map[string]any)
	history := payload["event_history"].(map[string]any)
	if _, ok := handlers[handler.ID]; !ok {
		t.Fatalf("handlers must be id-keyed, got %#v", handlers)
	}
	if ids := handlersByKey["CrossRuntimeFeatureEvent"].([]any); len(ids) != 1 || ids[0] != handler.ID {
		t.Fatalf("handlers_by_key shape mismatch: %#v", handlersByKey)
	}
	if _, ok := history[event.EventID]; !ok {
		t.Fatalf("event_history must be id-keyed, got %#v", history)
	}
}
