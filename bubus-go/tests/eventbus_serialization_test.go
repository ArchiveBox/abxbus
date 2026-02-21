package bubus_test

import (
	"context"
	"encoding/json"
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestEventBusJSONShapeAndRoundtrip(t *testing.T) {
	bus := bubus.NewEventBus("SerBus", nil)
	bus.On("Evt", "h", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return "ok", nil }, nil)
	_, _ = bus.Emit(bubus.NewBaseEvent("Evt", map[string]any{"k": "v"})).Done(context.Background())
	data, err := bus.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"id", "name", "handlers", "handlers_by_key", "event_history", "pending_event_queue"} {
		if _, ok := obj[k]; !ok {
			t.Fatalf("missing key %s", k)
		}
	}
	round, err := bubus.EventBusFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	if round.Name != bus.Name {
		t.Fatal("name mismatch")
	}
}
