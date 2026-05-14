package abxbus

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/ArchiveBox/abxbus/abxbus-go/v2/jsonschema"
)

type EventHandlerCallable func(event *BaseEvent, ctx context.Context) (any, error)

var (
	baseEventPointerType = reflect.TypeOf((*BaseEvent)(nil))
	contextInterfaceType = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorInterfaceType   = reflect.TypeOf((*error)(nil)).Elem()
)

func reflectValueIsNil(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

type EventHandler struct {
	ID                  string   `json:"id"`
	EventBusName        string   `json:"eventbus_name"`
	EventBusID          string   `json:"eventbus_id"`
	EventPattern        string   `json:"event_pattern"`
	HandlerName         string   `json:"handler_name"`
	HandlerFilePath     *string  `json:"handler_file_path"`
	HandlerTimeout      *float64 `json:"handler_timeout"`
	HandlerSlowTimeout  *float64 `json:"handler_slow_timeout"`
	HandlerRegisteredAt string   `json:"handler_registered_at"`

	handler EventHandlerCallable
}

type EventHandlerTimeoutError struct {
	Message        string  `json:"message"`
	TimeoutSeconds float64 `json:"timeout_seconds"`
}

func (e *EventHandlerTimeoutError) Error() string { return e.Message }

type EventTimeoutError struct {
	Message        string  `json:"message"`
	TimeoutSeconds float64 `json:"timeout_seconds"`
}

func (e *EventTimeoutError) Error() string { return e.Message }

type EventHandlerCancelledError struct {
	Message string `json:"message"`
}

func (e *EventHandlerCancelledError) Error() string { return e.Message }

type EventHandlerAbortedError struct {
	Message string `json:"message"`
}

func (e *EventHandlerAbortedError) Error() string { return e.Message }

func NewEventHandler(eventbus_name, eventbus_id, event_pattern, handler_name string, handler EventHandlerCallable) *EventHandler {
	registered := monotonicDatetime()
	id := ComputeHandlerID(eventbus_id, handler_name, nil, registered, event_pattern)
	return &EventHandler{
		ID:                  id,
		EventBusName:        eventbus_name,
		EventBusID:          eventbus_id,
		EventPattern:        event_pattern,
		HandlerName:         handler_name,
		HandlerRegisteredAt: registered,
		handler:             handler,
	}
}

func EventHandlerFromJSON(data []byte, handler EventHandlerCallable) (*EventHandler, error) {
	var parsed EventHandler
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, err
	}
	parsed.handler = handler
	return &parsed, nil
}

func (h *EventHandler) Handle(ctx context.Context, event *BaseEvent) (any, error) {
	if h.handler == nil {
		return nil, nil
	}
	return h.handler(event, ctx)
}

func (h *EventHandler) ToJSON() ([]byte, error) { return json.Marshal(h) }

func normalizeEventHandlerCallable(handler any) (EventHandlerCallable, error) {
	value := reflect.ValueOf(handler)
	if value.IsValid() && value.Kind() == reflect.Func && value.IsNil() {
		return nil, nil
	}
	switch typed := handler.(type) {
	case nil:
		return nil, nil
	case EventHandlerCallable:
		return typed, nil
	case func(*BaseEvent, context.Context) (any, error):
		return typed, nil
	case func(*BaseEvent) (any, error):
		return func(event *BaseEvent, ctx context.Context) (any, error) {
			return typed(event)
		}, nil
	case func(*BaseEvent, context.Context) error:
		return func(event *BaseEvent, ctx context.Context) (any, error) {
			return nil, typed(event, ctx)
		}, nil
	case func(*BaseEvent) error:
		return func(event *BaseEvent, ctx context.Context) (any, error) {
			return nil, typed(event)
		}, nil
	case func(*BaseEvent, context.Context):
		return func(event *BaseEvent, ctx context.Context) (any, error) {
			typed(event, ctx)
			return nil, nil
		}, nil
	case func(*BaseEvent):
		return func(event *BaseEvent, ctx context.Context) (any, error) {
			typed(event)
			return nil, nil
		}, nil
	default:
		if !value.IsValid() || value.Kind() != reflect.Func || value.IsNil() {
			return nil, unsupportedHandlerSignatureError(handler)
		}
		handlerType := value.Type()
		if handlerType.NumIn() != 1 && handlerType.NumIn() != 2 {
			return nil, unsupportedHandlerSignatureError(handler)
		}
		withContext := handlerType.NumIn() == 2
		if withContext && handlerType.In(1) != contextInterfaceType {
			return nil, unsupportedHandlerSignatureError(handler)
		}
		if handlerType.NumOut() > 2 {
			return nil, unsupportedHandlerSignatureError(handler)
		}
		if handlerType.NumOut() == 1 && !handlerType.Out(0).Implements(errorInterfaceType) {
			if handlerType.In(0) == baseEventPointerType {
				return nil, unsupportedHandlerSignatureError(handler)
			}
			return normalizeTypedEventHandlerReflectCallable(value, handlerType), nil
		}
		if handlerType.NumOut() == 2 && !handlerType.Out(1).Implements(errorInterfaceType) {
			return nil, unsupportedHandlerSignatureError(handler)
		}
		if handlerType.In(0) != baseEventPointerType {
			return normalizeTypedEventHandlerReflectCallable(value, handlerType), nil
		}
		return func(event *BaseEvent, ctx context.Context) (any, error) {
			args := []reflect.Value{reflect.ValueOf(event)}
			if withContext {
				args = append(args, reflect.ValueOf(ctx))
			}
			results := value.Call(args)
			switch len(results) {
			case 0:
				return nil, nil
			case 1:
				if reflectValueIsNil(results[0]) {
					return nil, nil
				}
				return nil, results[0].Interface().(error)
			default:
				var err error
				if !reflectValueIsNil(results[1]) {
					err = results[1].Interface().(error)
				}
				return results[0].Interface(), err
			}
		}, nil
	}
}

func normalizeTypedEventHandlerCallable(handler any) (EventHandlerCallable, error) {
	value := reflect.ValueOf(handler)
	if value.IsValid() && value.Kind() == reflect.Func && value.IsNil() {
		return nil, nil
	}
	if !value.IsValid() || value.Kind() != reflect.Func || value.IsNil() {
		return nil, unsupportedHandlerSignatureError(handler)
	}
	handlerType := value.Type()
	if handlerType.NumIn() != 1 && handlerType.NumIn() != 2 {
		return nil, unsupportedHandlerSignatureError(handler)
	}
	if handlerType.In(0) == baseEventPointerType {
		return nil, unsupportedHandlerSignatureError(handler)
	}
	if handlerType.NumIn() == 2 && handlerType.In(1) != contextInterfaceType {
		return nil, unsupportedHandlerSignatureError(handler)
	}
	if handlerType.NumOut() > 2 {
		return nil, unsupportedHandlerSignatureError(handler)
	}
	if handlerType.NumOut() == 2 && !handlerType.Out(1).Implements(errorInterfaceType) {
		return nil, unsupportedHandlerSignatureError(handler)
	}
	return normalizeTypedEventHandlerReflectCallable(value, handlerType), nil
}

func unsupportedHandlerSignatureError(handler any) error {
	return fmt.Errorf("handler must be one of: func(*BaseEvent), func(*BaseEvent) error, func(*BaseEvent) (any, error), a typed payload handler func(TPayload), func(TPayload) error, func(TPayload) TResult, func(TPayload) (TResult, error), or the same forms with context.Context as the second argument; got %T", handler)
}

func normalizeTypedEventHandlerReflectCallable(value reflect.Value, handlerType reflect.Type) EventHandlerCallable {
	payloadType := handlerType.In(0)
	payloadSchema := jsonSchemaForType(payloadType)
	withContext := handlerType.NumIn() == 2
	return func(event *BaseEvent, ctx context.Context) (any, error) {
		if err := jsonschema.Validate(payloadSchema, event.Payload); err != nil {
			return nil, fmt.Errorf("EventHandlerPayloadSchemaError: Event payload did not match declared handler payload type: %w", err)
		}
		payload, err := eventPayloadAsReflectValue(event, payloadType)
		if err != nil {
			return nil, err
		}
		args := []reflect.Value{payload}
		if withContext {
			args = append(args, reflect.ValueOf(ctx))
		}
		results := value.Call(args)
		switch len(results) {
		case 0:
			return nil, nil
		case 1:
			if results[0].Type().Implements(errorInterfaceType) {
				if reflectValueIsNil(results[0]) {
					return nil, nil
				}
				return nil, results[0].Interface().(error)
			}
			return reflectResultInterface(results[0]), nil
		default:
			var err error
			if !reflectValueIsNil(results[1]) {
				err = results[1].Interface().(error)
			}
			return reflectResultInterface(results[0]), err
		}
	}
}

func eventPayloadAsReflectValue(event *BaseEvent, payloadType reflect.Type) (reflect.Value, error) {
	if event == nil {
		return reflect.Value{}, fmt.Errorf("event is nil")
	}
	data, err := json.Marshal(event.Payload)
	if err != nil {
		return reflect.Value{}, err
	}
	payloadPtr := reflect.New(payloadType)
	if err := json.Unmarshal(data, payloadPtr.Interface()); err != nil {
		return reflect.Value{}, err
	}
	return payloadPtr.Elem(), nil
}

func reflectResultInterface(value reflect.Value) any {
	if reflectValueIsNil(value) {
		return nil
	}
	return value.Interface()
}

func normalizeTypedFindPredicate(where any) (func(*BaseEvent) bool, error) {
	if where == nil {
		return nil, nil
	}
	if typed, ok := where.(func(*BaseEvent) bool); ok {
		return typed, nil
	}
	value := reflect.ValueOf(where)
	if !value.IsValid() || value.Kind() != reflect.Func || value.IsNil() {
		return nil, fmt.Errorf("where must be func(*BaseEvent) bool or func(TPayload) bool; got %T", where)
	}
	predicateType := value.Type()
	if predicateType.NumIn() != 1 || predicateType.NumOut() != 1 || predicateType.Out(0).Kind() != reflect.Bool {
		return nil, fmt.Errorf("where must be func(*BaseEvent) bool or func(TPayload) bool; got %T", where)
	}
	if predicateType.In(0) == baseEventPointerType {
		return func(event *BaseEvent) bool {
			return value.Call([]reflect.Value{reflect.ValueOf(event)})[0].Bool()
		}, nil
	}
	payloadType := predicateType.In(0)
	payloadSchema := jsonSchemaForType(payloadType)
	return func(event *BaseEvent) bool {
		if err := jsonschema.Validate(payloadSchema, event.Payload); err != nil {
			return false
		}
		payload, err := eventPayloadAsReflectValue(event, payloadType)
		if err != nil {
			return false
		}
		return value.Call([]reflect.Value{payload})[0].Bool()
	}, nil
}
