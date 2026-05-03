package bubus

import (
	"context"
	"encoding/json"
	"sync"
)

type EventResultStatus string

const (
	EventResultPending   EventResultStatus = "pending"
	EventResultStarted   EventResultStatus = "started"
	EventResultCompleted EventResultStatus = "completed"
	EventResultError     EventResultStatus = "error"
)

type EventResult struct {
	ID            string            `json:"id"`
	Status        EventResultStatus `json:"status"`
	EventID       string            `json:"event_id"`
	HandlerID     string            `json:"handler_id"`
	HandlerName   string            `json:"handler_name"`
	EventBusName  string            `json:"eventbus_name"`
	EventBusID    string            `json:"eventbus_id"`
	StartedAt     *string           `json:"started_at,omitempty"`
	CompletedAt   *string           `json:"completed_at,omitempty"`
	Result        any               `json:"result,omitempty"`
	Error         any               `json:"error,omitempty"`
	EventChildren []*BaseEvent      `json:"event_children,omitempty"`

	Event   *BaseEvent    `json:"-"`
	Handler *EventHandler `json:"-"`

	mu      sync.Mutex
	done_ch chan struct{}
	once    sync.Once
}

type eventResultJSON struct {
	ID            string            `json:"id"`
	Status        EventResultStatus `json:"status"`
	EventID       string            `json:"event_id"`
	HandlerID     string            `json:"handler_id"`
	HandlerName   string            `json:"handler_name"`
	EventBusName  string            `json:"eventbus_name"`
	EventBusID    string            `json:"eventbus_id"`
	StartedAt     *string           `json:"started_at,omitempty"`
	CompletedAt   *string           `json:"completed_at,omitempty"`
	Result        any               `json:"result,omitempty"`
	Error         any               `json:"error,omitempty"`
	EventChildren []*BaseEvent      `json:"event_children,omitempty"`
}

func NewEventResult(event *BaseEvent, handler *EventHandler) *EventResult {
	return &EventResult{
		ID:           newUUIDv7String(),
		Status:       EventResultPending,
		EventID:      event.EventID,
		HandlerID:    handler.ID,
		HandlerName:  handler.HandlerName,
		EventBusName: handler.EventBusName,
		EventBusID:   handler.EventBusID,
		Event:        event,
		Handler:      handler,
		done_ch:      make(chan struct{}),
	}
}

func EventResultFromJSON(data []byte) (*EventResult, error) {
	var parsed EventResult
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, err
	}
	if parsed.done_ch == nil {
		parsed.done_ch = make(chan struct{})
	}
	if parsed.Status == EventResultCompleted || parsed.Status == EventResultError {
		parsed.once.Do(func() { close(parsed.done_ch) })
	}
	return &parsed, nil
}

func (r *EventResult) markStarted() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.Status == EventResultPending {
		r.Status = EventResultStarted
		now := monotonicDatetime()
		r.StartedAt = &now
	}
}

func (r *EventResult) markCompleted(result any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Status = EventResultCompleted
	r.Result = result
	now := monotonicDatetime()
	r.CompletedAt = &now
	r.once.Do(func() { close(r.done_ch) })
}

func (r *EventResult) markError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Status = EventResultError
	if err != nil {
		r.Error = err.Error()
	}
	now := monotonicDatetime()
	r.CompletedAt = &now
	r.once.Do(func() { close(r.done_ch) })
}

func (r *EventResult) Wait(ctx context.Context) error {
	select {
	case <-r.done_ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *EventResult) snapshot() (status EventResultStatus, result any, err any, startedAt *string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Status, r.Result, r.Error, r.StartedAt
}

func (r *EventResult) addChild(event *BaseEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.EventChildren = append(r.EventChildren, event)
}

func (r *EventResult) childEvents() []*BaseEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*BaseEvent{}, r.EventChildren...)
}

func (r *EventResult) MarshalJSON() ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	payload := eventResultJSON{
		ID:            r.ID,
		Status:        r.Status,
		EventID:       r.EventID,
		HandlerID:     r.HandlerID,
		HandlerName:   r.HandlerName,
		EventBusName:  r.EventBusName,
		EventBusID:    r.EventBusID,
		StartedAt:     r.StartedAt,
		CompletedAt:   r.CompletedAt,
		Result:        r.Result,
		Error:         r.Error,
		EventChildren: append([]*BaseEvent{}, r.EventChildren...),
	}
	return json.Marshal(payload)
}

func (r *EventResult) ToJSON() ([]byte, error) { return json.Marshal(r) }

func (r *EventResult) replaceError(message string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Status = EventResultError
	r.Error = message
	now := monotonicDatetime()
	r.CompletedAt = &now
	r.once.Do(func() { close(r.done_ch) })
}
