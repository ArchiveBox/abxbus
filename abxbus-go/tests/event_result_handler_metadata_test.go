package abxbus_test

import (
	"context"
	"encoding/json"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestEventResultSerializesHandlerMetadataAndDerivedFields(t *testing.T) {
	bus := abxbus.NewEventBus("ResultMetadataBus", nil)
	timeout := 1.5
	slowTimeout := 0.25
	handler := bus.On("MetadataEvent", "metadata_handler", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return "ok", nil
	}, &abxbus.EventHandler{
		HandlerTimeout:     &timeout,
		HandlerSlowTimeout: &slowTimeout,
	})

	event := bus.Emit(abxbus.NewBaseEvent("MetadataEvent", nil))
	if _, err := event.Now(); err != nil {
		t.Fatal(err)
	}
	result := event.EventResults[handler.ID]
	data, err := result.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"handler_id", "handler_name", "handler_timeout", "handler_slow_timeout", "handler_event_pattern", "eventbus_name", "eventbus_id"} {
		if _, ok := payload[key]; !ok {
			t.Fatalf("missing EventResult metadata key %s in %#v", key, payload)
		}
	}
	if payload["handler_id"] != handler.ID || payload["handler_name"] != "metadata_handler" || payload["handler_event_pattern"] != "MetadataEvent" {
		t.Fatalf("handler metadata mismatch: %#v", payload)
	}
	if _, ok := payload["result_type"]; ok {
		t.Fatalf("EventResult JSON must not duplicate parent event result schema: %#v", payload)
	}
}
