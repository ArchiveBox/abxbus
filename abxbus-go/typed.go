package abxbus

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/ArchiveBox/abxbus/abxbus-go/v2/jsonschema"
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

	baseEventType := reflect.TypeOf((*BaseEvent)(nil)).Elem()
	rawType := reflect.TypeOf(value)
	if rawType == baseEventType {
		return nil, fmt.Errorf("event must be *BaseEvent, got BaseEvent")
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
		name, _, skip, _ := jsonschema.StructFieldJSONName(field)
		if skip {
			continue
		}
		fieldValue := raw.Field(i)
		if applyEventConfigField(event, field.Name, fieldValue) {
			continue
		}
		if field.Name == "EventExtraPayload" {
			mergeEventExtraPayload(payload, fieldValue)
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
	case "EventTTL":
		event.EventTTL = reflectOptionalFloat(value)
	case "EventResultTTL":
		event.EventResultTTL = reflectOptionalFloat(value)
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
	wasPointer := false
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}
		wasPointer = true
		value = value.Elem()
	}
	switch value.Kind() {
	case reflect.Float32, reflect.Float64:
		if value.Float() == 0 && !wasPointer {
			return nil
		}
		f := value.Convert(reflect.TypeOf(float64(0))).Float()
		return &f
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if value.Int() == 0 && !wasPointer {
			return nil
		}
		f := float64(value.Int())
		return &f
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if value.Uint() == 0 && !wasPointer {
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

type ModelField struct {
	Name       string
	Type       map[string]any
	Default    any
	HasDefault bool
}

type EventClass[T any] struct {
	eventType string
	options   []EventOption
}

func NewEventClass[T any](eventType string, options ...EventOption) EventClass[T] {
	return EventClass[T]{eventType: eventType, options: append([]EventOption(nil), options...)}
}

func (eventClass EventClass[T]) ModelFields() map[string]ModelField {
	return ModelFieldsFor[T](eventClass.options...)
}

func (eventClass EventClass[T]) New(payload T, options ...EventOption) (*BaseEvent, error) {
	mergedOptions := append(append([]EventOption(nil), eventClass.options...), options...)
	return NewEvent(eventClass.eventType, payload, mergedOptions...)
}

func (eventClass EventClass[T]) MustNew(payload T, options ...EventOption) *BaseEvent {
	event, err := eventClass.New(payload, options...)
	if err != nil {
		panic(err)
	}
	return event
}

func ModelFieldsFor[T any](options ...EventOption) map[string]ModelField {
	fields := map[string]ModelField{}
	payloadSchema := JSONSchemaFor[T]()
	properties, _ := payloadSchema["properties"].(map[string]any)
	defaults := defaultPayloadValues[T]()
	for _, name := range payloadModelFieldNames(reflect.TypeOf((*T)(nil)).Elem()) {
		schema, _ := properties[name].(map[string]any)
		defaultValue, hasDefault := defaults[name]
		fields[name] = ModelField{
			Name:       name,
			Type:       schema,
			Default:    defaultValue,
			HasDefault: hasDefault,
		}
	}
	event := NewBaseEvent("", nil)
	for _, option := range options {
		if option != nil {
			option(event)
		}
	}
	if event.EventResultType != nil {
		resultSchema, _ := event.EventResultType.(map[string]any)
		fields["event_result_type"] = ModelField{
			Name:       "event_result_type",
			Type:       resultSchema,
			Default:    event.EventResultType,
			HasDefault: true,
		}
	}
	return fields
}

func defaultPayloadValues[T any]() map[string]any {
	var zero T
	data, err := json.Marshal(zero)
	if err != nil || string(data) == "null" {
		return map[string]any{}
	}
	defaults := map[string]any{}
	if err := json.Unmarshal(data, &defaults); err != nil {
		return map[string]any{}
	}
	return defaults
}

func payloadModelFieldNames(t reflect.Type) []string {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	names := []string{}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" || field.Anonymous || strings.HasPrefix(field.Name, "Event") || strings.HasPrefix(field.Name, "Model") {
			continue
		}
		name, _, skip, _ := jsonschema.StructFieldJSONName(field)
		if skip {
			continue
		}
		names = append(names, name)
	}
	return names
}

func newEventFromPayload[T any](eventType string, payload T) (*BaseEvent, error) {
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
	delete(normalized, "event_extra_payload")
	delete(normalized, "EventExtraPayload")
	mergeEventExtraPayload(normalized, reflect.ValueOf(payload))
	return NewBaseEvent(eventType, normalized), nil
}

type EventOption func(*BaseEvent)

func ResultType[T any]() EventOption {
	return func(event *BaseEvent) {
		event.EventResultType = JSONSchemaFor[T]()
	}
}

func NewEvent[T any](eventType string, payload T, options ...EventOption) (*BaseEvent, error) {
	event, err := newEventFromPayload(eventType, payload)
	if err != nil {
		return nil, err
	}
	for _, option := range options {
		if option != nil {
			option(event)
		}
	}
	return event, nil
}

func MustNewEvent[T any](eventType string, payload T, options ...EventOption) *BaseEvent {
	event, err := NewEvent(eventType, payload, options...)
	if err != nil {
		panic(err)
	}
	return event
}

func EventPayloadAs[T any](event *BaseEvent) (T, error) {
	var payload T
	if event == nil {
		return payload, fmt.Errorf("event is nil")
	}
	eventPayload, err := event.EventPayload()
	if err != nil {
		return payload, err
	}
	data, err := json.Marshal(eventPayload)
	if err != nil {
		return payload, err
	}
	err = json.Unmarshal(data, &payload)
	if err != nil {
		return payload, err
	}
	setEventExtraPayload(&payload, eventPayload)
	return payload, nil
}

// EventExtraPayload is the in-memory field for forward-compatible typed events.
// It stores flat JSON fields that were not declared statically on the Go type.
// The map is always merged into the normal flat event JSON before dumping, and
// split back out when loading; the key "event_extra_payload" is never emitted.
func mergeEventExtraPayload(payload map[string]any, value reflect.Value) {
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return
		}
		value = value.Elem()
	}
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return
	}
	field := value.FieldByName("EventExtraPayload")
	if !field.IsValid() || field.Kind() != reflect.Map || field.Type().Key().Kind() != reflect.String {
		return
	}
	for _, key := range field.MapKeys() {
		payload[key.String()] = normalizeReflectValue(field.MapIndex(key))
	}
}

func setEventExtraPayload(target any, payload map[string]any) {
	value := reflect.ValueOf(target)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return
	}
	value = value.Elem()
	for value.IsValid() && value.Kind() == reflect.Pointer {
		if value.IsNil() {
			value.Set(reflect.New(value.Type().Elem()))
		}
		value = value.Elem()
	}
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return
	}
	field := value.FieldByName("EventExtraPayload")
	if !field.IsValid() || !field.CanSet() || field.Kind() != reflect.Map || field.Type().Key().Kind() != reflect.String {
		return
	}
	extra := reflect.MakeMap(field.Type())
	for key, item := range payload {
		if typedPayloadJSONFieldNames(value.Type())[key] {
			continue
		}
		itemValue := reflect.ValueOf(item)
		if !itemValue.IsValid() {
			itemValue = reflect.Zero(field.Type().Elem())
		} else if !itemValue.Type().AssignableTo(field.Type().Elem()) {
			if !itemValue.Type().ConvertibleTo(field.Type().Elem()) {
				continue
			}
			itemValue = itemValue.Convert(field.Type().Elem())
		}
		extra.SetMapIndex(reflect.ValueOf(key), itemValue)
	}
	if extra.Len() > 0 {
		field.Set(extra)
	}
}

func typedPayloadJSONFieldNames(t reflect.Type) map[string]bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	names := map[string]bool{}
	if t.Kind() != reflect.Struct {
		return names
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" || field.Anonymous || strings.HasPrefix(field.Name, "Event") || strings.HasPrefix(field.Name, "Model") {
			continue
		}
		name, _, skip, _ := jsonschema.StructFieldJSONName(field)
		if !skip {
			names[name] = true
		}
	}
	return names
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
	return jsonschema.SchemaForType(reflect.TypeOf(zero))
}
