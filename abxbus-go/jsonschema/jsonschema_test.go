package jsonschema_test

import (
	"strings"
	"testing"

	"github.com/ArchiveBox/abxbus/abxbus-go/v2/jsonschema"
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

func TestSchemaForStructUsesJSONNamesAndValidates(t *testing.T) {
	type Params struct {
		ID    string   `json:"id"`
		Count int      `json:"count,omitempty"`
		Tags  []string `json:"tags,omitempty"`
		Skip  string   `json:"-"`
	}

	schema := jsonschema.SchemaFor[Params]()
	properties := schema["properties"].(map[string]any)
	if _, ok := properties["ID"]; ok {
		t.Fatalf("expected json field names, got %#v", properties)
	}
	if _, ok := properties["skip"]; ok {
		t.Fatalf("expected json:- field to be skipped, got %#v", properties)
	}
	if err := jsonschema.Validate(schema, Params{ID: "abc"}); err != nil {
		t.Fatalf("expected struct value to validate: %v", err)
	}
	if err := jsonschema.Validate(schema, map[string]any{"id": 123}); err == nil || !strings.Contains(err.Error(), "expected string") {
		t.Fatalf("expected generated schema to reject wrong id type, got %v", err)
	}
}

func TestSchemaForRecursiveStructUsesRefsAndValidates(t *testing.T) {
	type Node struct {
		ID       string `json:"id"`
		Children []Node `json:"children,omitempty"`
		Parent   *Node  `json:"parent,omitempty"`
	}

	schema := jsonschema.SchemaFor[Node]()
	if _, ok := schema["$defs"].(map[string]any); !ok {
		t.Fatalf("expected recursive schema defs, got %#v", schema)
	}
	if err := jsonschema.Validate(schema, Node{ID: "root", Children: []Node{{ID: "child"}}}); err != nil {
		t.Fatalf("expected recursive struct value to validate: %v", err)
	}
	payload := map[string]any{
		"id": "root",
		"children": []any{
			map[string]any{"id": 3},
		},
	}
	if err := jsonschema.Validate(schema, payload); err == nil || !strings.Contains(err.Error(), "expected string") {
		t.Fatalf("expected generated recursive schema to reject wrong child id type, got %v", err)
	}
}
