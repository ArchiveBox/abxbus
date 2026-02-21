package bubus

import (
	"context"
	"encoding/json"
	"fmt"
)

type EventHandlerCallable func(ctx context.Context, event *BaseEvent) (any, error)

type EventHandler struct {
	ID                  string   `json:"id"`
	EventBusName        string   `json:"eventbus_name"`
	EventBusID          string   `json:"eventbus_id"`
	EventPattern        string   `json:"event_pattern"`
	HandlerName         string   `json:"handler_name"`
	HandlerFilePath     *string  `json:"handler_file_path,omitempty"`
	HandlerTimeout      *float64 `json:"handler_timeout,omitempty"`
	HandlerSlowTimeout  *float64 `json:"handler_slow_timeout,omitempty"`
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
	seed := fmt.Sprintf("%s|%s|%s|%s|%s", eventbus_id, handler_name, "unknown", registered, event_pattern)
	id := deterministicUUID(seed)
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
	return h.handler(ctx, event)
}

func (h *EventHandler) ToJSON() ([]byte, error) { return json.Marshal(h) }
