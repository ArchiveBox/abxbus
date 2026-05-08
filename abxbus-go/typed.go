package abxbus

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/ArchiveBox/abxbus/abxbus-go/jsonschema"
)

func Event[T any](payload T) (*BaseEvent, error) {
	return baseEventFromAny(payload)
}

func baseEventFromAny(value any) (*BaseEvent, error) {
	if event, ok := value.(*BaseEvent); ok {
		if event == nil {
			return nil, fmt.Errorf("event is nil")
		}
		return event, nil
	}
	if event, ok := value.(BaseEvent); ok {
		return &event, nil
	}

	raw := reflect.ValueOf(value)
	if !raw.IsValid() {
		return nil, fmt.Errorf("event is nil")
	}
	for raw.Kind() == reflect.Pointer {
		if raw.IsNil() {
			return nil, fmt.Errorf("event is nil")
		}
		raw = raw.Elem()
	}
	if raw.Kind() != reflect.Struct {
		return nil, fmt.Errorf("event must be *BaseEvent or struct, got %T", value)
	}

	eventType := raw.Type().Name()
	if eventType == "" {
		return nil, fmt.Errorf("event struct type must be named")
	}
	payload := map[string]any{}
	event := NewBaseEvent(eventType, payload)

	for i := 0; i < raw.NumField(); i++ {
		field := raw.Type().Field(i)
		if field.PkgPath != "" {
			continue
		}
		if field.Anonymous {
			continue
		}
		name, _, skip, _ := jsonFieldName(field)
		if skip {
			continue
		}
		fieldValue := raw.Field(i)
		if applyEventConfigField(event, field.Name, fieldValue) {
			continue
		}
		payload[name] = normalizeReflectValue(fieldValue)
	}
	return event, nil
}

func applyEventConfigField(event *BaseEvent, name string, value reflect.Value) bool {
	switch name {
	case "EventType":
		if value.Kind() == reflect.String && value.String() != "" {
			event.EventType = value.String()
		}
	case "EventVersion":
		if value.Kind() == reflect.String && value.String() != "" {
			event.EventVersion = value.String()
		}
	case "EventTimeout":
		event.EventTimeout = reflectOptionalFloat(value)
	case "EventSlowTimeout":
		event.EventSlowTimeout = reflectOptionalFloat(value)
	case "EventHandlerTimeout":
		event.EventHandlerTimeout = reflectOptionalFloat(value)
	case "EventHandlerSlowTimeout":
		event.EventHandlerSlowTimeout = reflectOptionalFloat(value)
	case "EventConcurrency":
		if str := reflectString(value); str != "" {
			event.EventConcurrency = EventConcurrencyMode(str)
		}
	case "EventHandlerConcurrency":
		if str := reflectString(value); str != "" {
			event.EventHandlerConcurrency = EventHandlerConcurrencyMode(str)
		}
	case "EventHandlerCompletion":
		if str := reflectString(value); str != "" {
			event.EventHandlerCompletion = EventHandlerCompletionMode(str)
		}
	case "EventBlocksParentCompletion":
		if value.Kind() == reflect.Bool {
			event.EventBlocksParentCompletion = value.Bool()
		}
	case "EventResultType":
		if !value.IsZero() {
			event.EventResultType = normalizeReflectValue(value)
		}
	default:
		return false
	}
	return true
}

func reflectOptionalFloat(value reflect.Value) *float64 {
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}
	switch value.Kind() {
	case reflect.Float32, reflect.Float64:
		if value.Float() == 0 {
			return nil
		}
		f := value.Convert(reflect.TypeOf(float64(0))).Float()
		return &f
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if value.Int() == 0 {
			return nil
		}
		f := float64(value.Int())
		return &f
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if value.Uint() == 0 {
			return nil
		}
		f := float64(value.Uint())
		return &f
	default:
		return nil
	}
}

func reflectString(value reflect.Value) string {
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return ""
		}
		value = value.Elem()
	}
	if value.Kind() == reflect.String {
		return value.String()
	}
	return ""
}

func normalizeReflectValue(value reflect.Value) any {
	if !value.IsValid() {
		return nil
	}
	return value.Interface()
}

func NewTypedEvent[T any](eventType string, payload T) (*BaseEvent, error) {
	normalized := map[string]any{}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if string(data) != "null" {
		if err := json.Unmarshal(data, &normalized); err != nil {
			return nil, err
		}
	}
	return NewBaseEvent(eventType, normalized), nil
}

func NewTypedEventWithResult[TPayload any, TResult any](eventType string, payload TPayload) (*BaseEvent, error) {
	event, err := NewTypedEvent(eventType, payload)
	if err != nil {
		return nil, err
	}
	event.EventResultType = JSONSchemaFor[TResult]()
	return event, nil
}

func MustNewTypedEvent[T any](eventType string, payload T) *BaseEvent {
	event, err := NewTypedEvent(eventType, payload)
	if err != nil {
		panic(err)
	}
	return event
}

func MustNewTypedEventWithResult[TPayload any, TResult any](eventType string, payload TPayload) *BaseEvent {
	event, err := NewTypedEventWithResult[TPayload, TResult](eventType, payload)
	if err != nil {
		panic(err)
	}
	return event
}

func OnTyped[TPayload any, TResult any](
	bus *EventBus,
	eventPattern string,
	handlerName string,
	handler func(context.Context, TPayload) (TResult, error),
	options *EventHandler,
) *EventHandler {
	payloadSchema := JSONSchemaFor[TPayload]()
	return bus.On(eventPattern, handlerName, func(ctx context.Context, event *BaseEvent) (any, error) {
		if err := jsonschema.Validate(payloadSchema, event.Payload); err != nil {
			var zero TResult
			return zero, fmt.Errorf("EventHandlerPayloadSchemaError: Event payload did not match declared handler payload type: %w", err)
		}
		payload, err := EventPayloadAs[TPayload](event)
		if err != nil {
			var zero TResult
			return zero, err
		}
		return handler(ctx, payload)
	}, options)
}

func EventPayloadAs[T any](event *BaseEvent) (T, error) {
	var payload T
	if event == nil {
		return payload, fmt.Errorf("event is nil")
	}
	data, err := json.Marshal(event.Payload)
	if err != nil {
		return payload, err
	}
	err = json.Unmarshal(data, &payload)
	return payload, err
}

func EventResultAs[T any](result any) (T, error) {
	var typed T
	data, err := json.Marshal(result)
	if err != nil {
		return typed, err
	}
	err = json.Unmarshal(data, &typed)
	return typed, err
}

func JSONSchemaFor[T any]() map[string]any {
	var zero T
	return jsonSchemaForType(reflect.TypeOf(zero))
}

func jsonSchemaForType(t reflect.Type) map[string]any {
	if t == nil {
		return map[string]any{"$schema": jsonSchemaDraft202012}
	}
	for t.Kind() == reflect.Pointer {
		return map[string]any{
			"$schema": jsonSchemaDraft202012,
			"anyOf": []any{
				jsonSchemaWithoutDraft(jsonSchemaForType(t.Elem())),
				map[string]any{"type": "null"},
			},
		}
	}
	schema := jsonSchemaWithoutDraft(jsonSchemaForNonPointerType(t))
	schema["$schema"] = jsonSchemaDraft202012
	return schema
}

func jsonSchemaForNonPointerType(t reflect.Type) map[string]any {
	switch t.Kind() {
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.Slice, reflect.Array:
		return map[string]any{"type": "array", "items": jsonSchemaWithoutDraft(jsonSchemaForType(t.Elem()))}
	case reflect.Map:
		schema := map[string]any{"type": "object"}
		schema["additionalProperties"] = jsonSchemaWithoutDraft(jsonSchemaForType(t.Elem()))
		return schema
	case reflect.Struct:
		return jsonSchemaForStruct(t)
	case reflect.Interface:
		return map[string]any{}
	default:
		return map[string]any{}
	}
}

func jsonSchemaForStruct(t reflect.Type) map[string]any {
	properties := map[string]any{}
	required := []any{}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		name, omitempty, skip, explicitName := jsonFieldName(field)
		if skip {
			continue
		}
		fieldType := field.Type
		for fieldType.Kind() == reflect.Pointer {
			fieldType = fieldType.Elem()
		}
		if field.Anonymous && !explicitName && fieldType.Kind() == reflect.Struct {
			embedded := jsonSchemaForStruct(fieldType)
			if embeddedProperties, ok := embedded["properties"].(map[string]any); ok {
				for embeddedName, embeddedSchema := range embeddedProperties {
					properties[embeddedName] = embeddedSchema
				}
			}
			if !omitempty && field.Type.Kind() != reflect.Pointer {
				if embeddedRequired, ok := embedded["required"].([]any); ok {
					required = append(required, embeddedRequired...)
				}
			}
			continue
		}
		if field.PkgPath != "" {
			continue
		}
		properties[name] = jsonSchemaWithoutDraft(jsonSchemaForType(field.Type))
		if !omitempty && field.Type.Kind() != reflect.Pointer {
			required = append(required, name)
		}
	}
	schema := map[string]any{"type": "object", "properties": properties}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func jsonFieldName(field reflect.StructField) (name string, omitempty bool, skip bool, explicitName bool) {
	name = lowerSnakeCase(field.Name)
	tag := field.Tag.Get("json")
	if tag == "-" {
		return "", false, true, false
	}
	if tag == "" {
		return name, false, false, false
	}
	parts := strings.Split(tag, ",")
	if parts[0] != "" {
		name = parts[0]
		explicitName = true
	}
	for _, part := range parts[1:] {
		if part == "omitempty" || part == "omitzero" {
			omitempty = true
			break
		}
	}
	return name, omitempty, false, explicitName
}

var initialismPattern = regexp.MustCompile(`([A-Z]+)([A-Z][a-z])`)
var wordBoundaryPattern = regexp.MustCompile(`([a-z0-9])([A-Z])`)

func lowerSnakeCase(name string) string {
	if name == "" {
		return name
	}
	name = initialismPattern.ReplaceAllString(name, `${1}_${2}`)
	name = wordBoundaryPattern.ReplaceAllString(name, `${1}_${2}`)
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ReplaceAll(name, " ", "_")
	return strings.ToLower(name)
}

func jsonSchemaWithoutDraft(schema map[string]any) map[string]any {
	out := make(map[string]any, len(schema))
	for key, value := range schema {
		if key == "$schema" {
			continue
		}
		out[key] = value
	}
	return out
}
