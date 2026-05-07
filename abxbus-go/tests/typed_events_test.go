package abxbus_test

import (
	"context"
	"reflect"
	"strings"
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
	abxbus.OnTyped[addPayload, addResult](bus, "AddEvent", "add", func(ctx context.Context, payload addPayload) (addResult, error) {
		return addResult{Sum: payload.A + payload.B}, nil
	}, nil)

	event := abxbus.MustNewTypedEventWithResult[addPayload, addResult]("AddEvent", addPayload{A: 4, B: 9})
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

func TestTypedEventWithResultSchemaValidatesHandlerReturnAtRuntime(t *testing.T) {
	bus := abxbus.NewEventBus("TypedSchemaBus", nil)
	bus.On("TypedSchemaEvent", "bad", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return map[string]any{"sum": "not-an-int"}, nil
	}, nil)

	event := abxbus.MustNewTypedEventWithResult[addPayload, addResult]("TypedSchemaEvent", addPayload{A: 1, B: 2})
	if _, err := bus.Emit(event).EventResult(context.Background()); err == nil || !strings.Contains(err.Error(), "EventHandlerResultSchemaError") {
		t.Fatalf("expected typed result schema error, got %v", err)
	}
}

func TestOnTypedValidatesPayloadBeforeCallingHandler(t *testing.T) {
	bus := abxbus.NewEventBus("TypedPayloadSchemaBus", nil)
	called := false
	abxbus.OnTyped[addPayload, addResult](bus, "TypedPayloadSchemaEvent", "typed", func(ctx context.Context, payload addPayload) (addResult, error) {
		called = true
		return addResult{Sum: payload.A + payload.B}, nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("TypedPayloadSchemaEvent", map[string]any{"a": 1}))
	if _, err := event.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("typed handler should not be called when a required payload field is missing")
	}
	if _, err := event.EventResult(context.Background()); err == nil || !strings.Contains(err.Error(), "EventHandlerPayloadSchemaError") {
		t.Fatalf("expected typed payload schema error, got %v", err)
	}
}

func TestOnTypedRejectsWrongPayloadFieldType(t *testing.T) {
	bus := abxbus.NewEventBus("TypedPayloadTypeBus", nil)
	called := false
	abxbus.OnTyped[addPayload, addResult](bus, "TypedPayloadTypeEvent", "typed", func(ctx context.Context, payload addPayload) (addResult, error) {
		called = true
		return addResult{Sum: payload.A + payload.B}, nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("TypedPayloadTypeEvent", map[string]any{"a": "one", "b": 2}))
	if _, err := event.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("typed handler should not be called when a payload field has the wrong type")
	}
	if _, err := event.EventResult(context.Background()); err == nil || !strings.Contains(err.Error(), "EventHandlerPayloadSchemaError") {
		t.Fatalf("expected typed payload schema error, got %v", err)
	}
}

func TestJSONSchemaForGoStructUsesJSONTagsAndRequiredFields(t *testing.T) {
	type nestedResult struct {
		Tags []string `json:"tags"`
	}
	type schemaResult struct {
		ID       string            `json:"id"`
		Count    int               `json:"count"`
		Metadata map[string]int    `json:"metadata,omitempty"`
		Nested   *nestedResult     `json:"nested,omitempty"`
		Ignored  string            `json:"-"`
		Any      map[string]string `json:",omitempty"`
	}

	schema := abxbus.JSONSchemaFor[schemaResult]()
	expectedRequired := []any{"id", "count"}
	if schema["$schema"] != "https://json-schema.org/draft/2020-12/schema" || schema["type"] != "object" {
		t.Fatalf("unexpected schema root: %#v", schema)
	}
	if !reflect.DeepEqual(schema["required"], expectedRequired) {
		t.Fatalf("unexpected required fields: %#v", schema["required"])
	}
	properties := schema["properties"].(map[string]any)
	if _, ok := properties["Ignored"]; ok {
		t.Fatalf("json:- field leaked into schema: %#v", properties)
	}
	if properties["id"].(map[string]any)["type"] != "string" || properties["count"].(map[string]any)["type"] != "integer" {
		t.Fatalf("primitive property schemas did not match: %#v", properties)
	}
	if properties["metadata"].(map[string]any)["additionalProperties"].(map[string]any)["type"] != "integer" {
		t.Fatalf("map property schema did not match: %#v", properties["metadata"])
	}
	nestedAnyOf := properties["nested"].(map[string]any)["anyOf"].([]any)
	if nestedAnyOf[0].(map[string]any)["type"] != "object" || nestedAnyOf[1].(map[string]any)["type"] != "null" {
		t.Fatalf("pointer property schema did not match: %#v", properties["nested"])
	}
}
