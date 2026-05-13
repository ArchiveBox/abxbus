package abxbus

import (
	"context"
	"encoding/json"
	"fmt"
)

type EventHandlerCallable func(event *BaseEvent, ctx context.Context) (any, error)

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
		return nil, fmt.Errorf("handler must be one of: func(*BaseEvent), func(*BaseEvent) error, func(*BaseEvent) (any, error), or the same forms with context.Context as the second argument; got %T", handler)
	}
}
