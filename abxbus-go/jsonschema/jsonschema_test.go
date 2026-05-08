package jsonschema_test

import (
	"strings"
	"testing"

	"github.com/ArchiveBox/abxbus/abxbus-go/jsonschema"
)

func TestValidateAcceptsBasicObjectSchema(t *testing.T) {
	schema := map[string]any{"type": "object"}
	if err := jsonschema.Validate(schema, map[string]any{"ok": true}); err != nil {
		t.Fatalf("expected object to validate: %v", err)
	}
	if err := jsonschema.Validate(schema, "not object"); err == nil || !strings.Contains(err.Error(), "expected object") {
		t.Fatalf("expected object type error, got %v", err)
	}
}

func TestValidatePropertiesRequiredAndAdditionalProperties(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{"type": "string"},
			"age":  map[string]any{"type": "integer", "minimum": 0},
		},
		"required":             []any{"name"},
		"additionalProperties": false,
	}
	if err := jsonschema.Validate(schema, map[string]any{"name": "Ada", "age": 37}); err != nil {
		t.Fatalf("expected payload to validate: %v", err)
	}
	if err := jsonschema.Validate(schema, map[string]any{"age": 37}); err == nil || !strings.Contains(err.Error(), ".name is required") {
		t.Fatalf("expected required property error, got %v", err)
	}
	if err := jsonschema.Validate(schema, map[string]any{"name": "Ada", "extra": true}); err == nil || !strings.Contains(err.Error(), ".extra is not allowed") {
		t.Fatalf("expected additional property error, got %v", err)
	}
}

func TestValidateReferencesAndCompositeSchemas(t *testing.T) {
	schema := map[string]any{
		"$defs": map[string]any{
			"id": map[string]any{"type": "string", "pattern": "^[a-z]+$"},
		},
		"type": "object",
		"properties": map[string]any{
			"id": map[string]any{"$ref": "#/$defs/id"},
			"value": map[string]any{
				"anyOf": []any{
					map[string]any{"type": "string"},
					map[string]any{"type": "integer"},
				},
			},
		},
		"required": []any{"id", "value"},
	}
	if err := jsonschema.Validate(schema, map[string]any{"id": "abc", "value": 3}); err != nil {
		t.Fatalf("expected payload to validate: %v", err)
	}
	if err := jsonschema.Validate(schema, map[string]any{"id": "ABC", "value": true}); err == nil {
		t.Fatalf("expected payload to fail")
	}
}
