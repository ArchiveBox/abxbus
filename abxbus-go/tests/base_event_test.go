package abxbus_test

import (
	"context"
	"encoding/json"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestBaseEventDoneWithoutBus(t *testing.T) {
	e := abxbus.NewBaseEvent("NoBus", nil)
	if _, err := e.Done(context.Background()); err == nil {
		t.Fatal("expected error")
	}
}

func TestBaseEventJSONFlattenedPayload(t *testing.T) {
	e := abxbus.NewBaseEvent("JSONEvent", map[string]any{"x": 1})
	data, err := e.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil {
		t.Fatal(err)
	}
	if _, ok := obj["payload"]; ok {
		t.Fatal("payload must be flattened")
	}
	if obj["x"].(float64) != 1 {
		t.Fatal("payload key x missing")
	}
	if _, ok := obj["event_id"]; !ok {
		t.Fatal("missing event_id")
	}
}
