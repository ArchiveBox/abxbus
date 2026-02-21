package bubus_test

import (
	"context"
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestEventBusFromJSONResetsRuntimeExecutionState(t *testing.T) {
	bus := bubus.NewEventBus("RestoreBus", nil)
	bus.On("Evt", "h", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return "ok", nil }, nil)
	e := bus.Emit(bubus.NewBaseEvent("Evt", nil))
	_, _ = e.Done(context.Background())

	data, err := bus.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := bubus.EventBusFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	if !restored.IsIdleAndQueueEmpty() {
		t.Fatal("restored bus should start from clean runtime execution state")
	}

	restored.On("Evt2", "h2", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return "ok2", nil }, nil)
	result, err := restored.Emit(bubus.NewBaseEvent("Evt2", nil)).EventResult(context.Background())
	if err != nil || result != "ok2" {
		t.Fatalf("restored bus should be functional, got result=%#v err=%v", result, err)
	}
}
