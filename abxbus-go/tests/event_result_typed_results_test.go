package abxbus_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func schemaEvent(eventType string, schema map[string]any) *abxbus.BaseEvent {
	event := abxbus.NewBaseEvent(eventType, nil)
	event.EventResultType = schema
	return event
}

func firstEventResult(event *abxbus.BaseEvent) *abxbus.EventResult {
	for _, result := range event.EventResults {
		return result
	}
	return nil
}

func TestTypedResultSchemaValidatesHandlerResult(t *testing.T) {
	bus := abxbus.NewEventBus("TypedResultBus", nil)
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"value": map[string]any{"type": "string"},
			"count": map[string]any{"type": "number"},
		},
		"required": []any{"value", "count"},
	}
	bus.On("TypedResultEvent", "handler", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return map[string]any{"value": "hello", "count": 42}, nil
	}, nil)

	event := bus.Emit(schemaEvent("TypedResultEvent", schema))
	if _, err := event.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	result := firstEventResult(event)
	if result == nil || result.Status != abxbus.EventResultCompleted {
		t.Fatalf("expected completed result, got %#v", result)
	}
}

func TestInvalidHandlerResultMarksErrorWhenSchemaIsDefined(t *testing.T) {
	bus := abxbus.NewEventBus("InvalidTypedResultBus", nil)
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"value": map[string]any{"type": "string"},
		},
		"required":             []any{"value"},
		"additionalProperties": false,
	}
	bus.On("InvalidTypedResultEvent", "handler", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return map[string]any{"value": 123, "extra": true}, nil
	}, nil)

	event := bus.Emit(schemaEvent("InvalidTypedResultEvent", schema))
	if _, err := event.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := event.EventResult(context.Background()); err == nil || !strings.Contains(err.Error(), "EventHandlerResultSchemaError") {
		t.Fatalf("expected schema error from result accessor, got %v", err)
	}
	result := firstEventResult(event)
	if result == nil || result.Status != abxbus.EventResultError {
		t.Fatalf("expected errored result, got %#v", result)
	}
}

func TestNoSchemaLeavesRawHandlerResultUntouched(t *testing.T) {
	bus := abxbus.NewEventBus("NoSchemaResultBus", nil)
	raw := map[string]any{"value": 123}
	bus.On("NoSchemaEvent", "handler", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return raw, nil
	}, nil)

	event := bus.Emit(abxbus.NewBaseEvent("NoSchemaEvent", nil))
	if _, err := event.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	result := firstEventResult(event)
	if result == nil || result.Result == nil {
		t.Fatalf("expected raw result, got %#v", result)
	}
	resultMap, ok := result.Result.(map[string]any)
	if !ok || resultMap["value"] != 123 {
		t.Fatalf("raw result changed: %#v", result.Result)
	}
}

func TestComplexResultSchemaValidatesNestedData(t *testing.T) {
	bus := abxbus.NewEventBus("ComplexTypedResultBus", nil)
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"items": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"id":     map[string]any{"type": "integer"},
						"labels": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					},
					"required": []any{"id", "labels"},
				},
			},
		},
		"required": []any{"items"},
	}
	bus.On("ComplexTypedResultEvent", "handler", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return map[string]any{"items": []any{map[string]any{"id": 1, "labels": []any{"a", "b"}}}}, nil
	}, nil)

	event := bus.Emit(schemaEvent("ComplexTypedResultEvent", schema))
	if _, err := event.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFromJSONNormalizesEventResultTypeSchemaDraft(t *testing.T) {
	schema := map[string]any{"type": "string"}
	event := schemaEvent("SchemaRoundtripEvent", schema)
	data, err := event.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := abxbus.BaseEventFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	encodedSchema, err := json.Marshal(restored.EventResultType)
	if err != nil {
		t.Fatal(err)
	}
	if string(encodedSchema) != `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string"}` {
		t.Fatalf("unexpected restored schema: %s", string(encodedSchema))
	}
}

func TestFromJSONPreservesCanonicalEventResultTypeSchemaJSON(t *testing.T) {
	data := []byte(`{"event_type":"SchemaRawRoundtripEvent","event_version":"0.0.1","event_timeout":null,"event_slow_timeout":null,"event_concurrency":null,"event_handler_timeout":null,"event_handler_slow_timeout":null,"event_handler_concurrency":null,"event_handler_completion":null,"event_blocks_parent_completion":false,"event_result_type":{"type":"string","$schema":"https://json-schema.org/draft/2020-12/schema"},"event_id":"018f8e40-1234-7000-8000-00000000abcd","event_path":[],"event_parent_id":null,"event_emitted_by_handler_id":null,"event_pending_bus_count":0,"event_created_at":"2026-01-01T00:00:00.000000000Z","event_status":"pending","event_started_at":null,"event_completed_at":null}`)
	event, err := abxbus.BaseEventFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	roundtripped, err := event.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(roundtripped), `"event_result_type":{"type":"string","$schema":"https://json-schema.org/draft/2020-12/schema"}`) {
		t.Fatalf("canonical event_result_type JSON order changed: %s", string(roundtripped))
	}
}

func TestSchemaReferencesAndAnyOfAreEnforced(t *testing.T) {
	bus := abxbus.NewEventBus("SchemaRefBus", nil)
	schema := map[string]any{
		"$defs": map[string]any{
			"Payload": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"value": map[string]any{"anyOf": []any{
						map[string]any{"type": "string"},
						map[string]any{"type": "integer"},
					}},
				},
				"required": []any{"value"},
			},
		},
		"$ref": "#/$defs/Payload",
	}
	bus.On("SchemaRefEvent", "handler", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		return map[string]any{"value": 7}, nil
	}, nil)

	event := bus.Emit(schemaEvent("SchemaRefEvent", schema))
	if _, err := event.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
}
