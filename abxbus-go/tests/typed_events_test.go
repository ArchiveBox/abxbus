package abxbus_test

import (
	"context"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

type addPayload struct {
	A int `json:"a"`
	B int `json:"b"`
}

type addResult struct {
	Sum int `json:"sum"`
}

func TestTypedEventPayloadAndResultHelpers(t *testing.T) {
	bus := abxbus.NewEventBus("TypedBus", nil)
	bus.On("AddEvent", "add", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		payload, err := abxbus.EventPayloadAs[addPayload](event)
		if err != nil {
			return nil, err
		}
		return addResult{Sum: payload.A + payload.B}, nil
	}, nil)

	event := abxbus.MustNewTypedEvent("AddEvent", addPayload{A: 4, B: 9})
	result, err := bus.Emit(event).EventResult(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	typedResult, err := abxbus.EventResultAs[addResult](result)
	if err != nil {
		t.Fatal(err)
	}
	if typedResult.Sum != 13 {
		t.Fatalf("expected typed result sum=13, got %#v", typedResult)
	}

	roundtrippedPayload, err := abxbus.EventPayloadAs[addPayload](event)
	if err != nil {
		t.Fatal(err)
	}
	if roundtrippedPayload != (addPayload{A: 4, B: 9}) {
		t.Fatalf("typed payload roundtrip mismatch: %#v", roundtrippedPayload)
	}
}
