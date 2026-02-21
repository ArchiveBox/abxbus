package bubus

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

type BaseEvent struct {
	EventID                 string                      `json:"event_id"`
	EventCreatedAt          string                      `json:"event_created_at"`
	EventType               string                      `json:"event_type"`
	EventVersion            string                      `json:"event_version"`
	EventTimeout            *float64                    `json:"event_timeout"`
	EventSlowTimeout        *float64                    `json:"event_slow_timeout,omitempty"`
	EventHandlerTimeout     *float64                    `json:"event_handler_timeout,omitempty"`
	EventHandlerSlowTimeout *float64                    `json:"event_handler_slow_timeout,omitempty"`
	EventParentID           *string                     `json:"event_parent_id,omitempty"`
	EventPath               []string                    `json:"event_path,omitempty"`
	EventResultType         any                         `json:"event_result_type,omitempty"`
	EventEmittedByHandlerID *string                     `json:"event_emitted_by_handler_id,omitempty"`
	EventPendingBusCount    int                         `json:"event_pending_bus_count"`
	EventStatus             string                      `json:"event_status"`
	EventStartedAt          *string                     `json:"event_started_at,omitempty"`
	EventCompletedAt        *string                     `json:"event_completed_at,omitempty"`
	EventConcurrency        EventConcurrencyMode        `json:"event_concurrency,omitempty"`
	EventHandlerConcurrency EventHandlerConcurrencyMode `json:"event_handler_concurrency,omitempty"`
	EventHandlerCompletion  EventHandlerCompletionMode  `json:"event_handler_completion,omitempty"`

	Payload      map[string]any
	Bus          *EventBus               `json:"-"`
	EventResults map[string]*EventResult `json:"-"`
	dispatchCtx  context.Context         `json:"-"`
	mu           sync.Mutex
	done_ch      chan struct{}
	done_once    sync.Once
}

type EventResultsListOptions struct {
	Timeout     *float64
	RaiseIfAny  bool
	RaiseIfNone bool
}

func NewBaseEvent(event_type string, payload map[string]any) *BaseEvent {
	id := newUUIDv7String()
	if payload == nil {
		payload = map[string]any{}
	}
	return &BaseEvent{
		EventID: id, EventCreatedAt: monotonicDatetime(), EventType: event_type, EventVersion: "0.0.1",
		EventStatus: "pending", EventPath: []string{}, EventResults: map[string]*EventResult{}, EventPendingBusCount: 0,
		Payload: payload, done_ch: make(chan struct{}),
	}
}

func (e *BaseEvent) MarshalJSON() ([]byte, error) {
	record := map[string]any{}
	record["event_id"] = e.EventID
	record["event_created_at"] = e.EventCreatedAt
	record["event_type"] = e.EventType
	record["event_version"] = e.EventVersion
	record["event_timeout"] = e.EventTimeout
	record["event_slow_timeout"] = e.EventSlowTimeout
	record["event_handler_timeout"] = e.EventHandlerTimeout
	record["event_handler_slow_timeout"] = e.EventHandlerSlowTimeout
	record["event_parent_id"] = e.EventParentID
	record["event_path"] = e.EventPath
	record["event_result_type"] = e.EventResultType
	record["event_emitted_by_handler_id"] = e.EventEmittedByHandlerID
	record["event_pending_bus_count"] = e.EventPendingBusCount
	record["event_status"] = e.EventStatus
	record["event_started_at"] = e.EventStartedAt
	record["event_completed_at"] = e.EventCompletedAt
	record["event_concurrency"] = e.EventConcurrency
	record["event_handler_concurrency"] = e.EventHandlerConcurrency
	record["event_handler_completion"] = e.EventHandlerCompletion
	for k, v := range e.Payload {
		record[k] = v
	}
	if len(e.EventResults) > 0 {
		event_results := make([]*EventResult, 0, len(e.EventResults))
		for _, r := range e.EventResults {
			event_results = append(event_results, r)
		}
		record["event_results"] = event_results
	}
	return json.Marshal(record)
}

func (e *BaseEvent) UnmarshalJSON(data []byte) error {
	var record map[string]any
	if err := json.Unmarshal(data, &record); err != nil {
		return err
	}
	type meta struct {
		EventID                 string                      `json:"event_id"`
		EventCreatedAt          string                      `json:"event_created_at"`
		EventType               string                      `json:"event_type"`
		EventVersion            string                      `json:"event_version"`
		EventTimeout            *float64                    `json:"event_timeout"`
		EventSlowTimeout        *float64                    `json:"event_slow_timeout,omitempty"`
		EventHandlerTimeout     *float64                    `json:"event_handler_timeout,omitempty"`
		EventHandlerSlowTimeout *float64                    `json:"event_handler_slow_timeout,omitempty"`
		EventParentID           *string                     `json:"event_parent_id,omitempty"`
		EventPath               []string                    `json:"event_path,omitempty"`
		EventResultType         any                         `json:"event_result_type,omitempty"`
		EventEmittedByHandlerID *string                     `json:"event_emitted_by_handler_id,omitempty"`
		EventPendingBusCount    int                         `json:"event_pending_bus_count"`
		EventStatus             string                      `json:"event_status"`
		EventStartedAt          *string                     `json:"event_started_at,omitempty"`
		EventCompletedAt        *string                     `json:"event_completed_at,omitempty"`
		EventConcurrency        EventConcurrencyMode        `json:"event_concurrency,omitempty"`
		EventHandlerConcurrency EventHandlerConcurrencyMode `json:"event_handler_concurrency,omitempty"`
		EventHandlerCompletion  EventHandlerCompletionMode  `json:"event_handler_completion,omitempty"`
		EventResults            []json.RawMessage           `json:"event_results,omitempty"`
	}
	var m meta
	raw, _ := json.Marshal(record)
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	e.EventID = m.EventID
	e.EventCreatedAt = m.EventCreatedAt
	e.EventType = m.EventType
	e.EventVersion = m.EventVersion
	e.EventTimeout = m.EventTimeout
	e.EventSlowTimeout = m.EventSlowTimeout
	e.EventHandlerTimeout = m.EventHandlerTimeout
	e.EventHandlerSlowTimeout = m.EventHandlerSlowTimeout
	e.EventParentID = m.EventParentID
	e.EventPath = m.EventPath
	e.EventResultType = m.EventResultType
	e.EventEmittedByHandlerID = m.EventEmittedByHandlerID
	e.EventPendingBusCount = m.EventPendingBusCount
	e.EventStatus = m.EventStatus
	e.EventStartedAt = m.EventStartedAt
	e.EventCompletedAt = m.EventCompletedAt
	e.EventConcurrency = m.EventConcurrency
	e.EventHandlerConcurrency = m.EventHandlerConcurrency
	e.EventHandlerCompletion = m.EventHandlerCompletion
	e.Payload = map[string]any{}
	known := map[string]bool{"event_id": true, "event_created_at": true, "event_type": true, "event_version": true, "event_timeout": true, "event_slow_timeout": true, "event_handler_timeout": true, "event_handler_slow_timeout": true, "event_parent_id": true, "event_path": true, "event_result_type": true, "event_emitted_by_handler_id": true, "event_pending_bus_count": true, "event_status": true, "event_started_at": true, "event_completed_at": true, "event_concurrency": true, "event_handler_concurrency": true, "event_handler_completion": true, "event_results": true}
	for k, v := range record {
		if !known[k] {
			e.Payload[k] = v
		}
	}
	e.EventResults = map[string]*EventResult{}
	for _, raw_result := range m.EventResults {
		result, err := EventResultFromJSON(raw_result)
		if err != nil {
			return err
		}
		e.EventResults[result.HandlerID] = result
	}
	if e.done_ch == nil {
		e.done_ch = make(chan struct{})
	}
	if e.EventStatus == "completed" {
		e.done_once.Do(func() { close(e.done_ch) })
	}
	return nil
}

func (e *BaseEvent) ToJSON() ([]byte, error) { return json.Marshal(e) }

func BaseEventFromJSON(data []byte) (*BaseEvent, error) {
	var event BaseEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, err
	}
	if event.EventID == "" {
		return nil, errors.New("event_id required")
	}
	if event.done_ch == nil {
		event.done_ch = make(chan struct{})
	}
	if event.EventResults == nil {
		event.EventResults = map[string]*EventResult{}
	}
	return &event, nil
}

func (e *BaseEvent) markStarted() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.EventStatus == "pending" {
		e.EventStatus = "started"
		now := monotonicDatetime()
		e.EventStartedAt = &now
	}
}

func (e *BaseEvent) markCompleted() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.EventStatus = "completed"
	now := monotonicDatetime()
	e.EventCompletedAt = &now
	e.done_once.Do(func() { close(e.done_ch) })
}

func (e *BaseEvent) EventCompleted(ctx context.Context) error {
	select {
	case <-e.done_ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *BaseEvent) Done(ctx context.Context) (*BaseEvent, error) {
	if e.Bus == nil {
		return nil, errors.New("event has no bus attached")
	}
	_, err := e.Bus.processEventImmediately(ctx, e, nil)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (e *BaseEvent) EventResult(ctx context.Context) (any, error) {
	if _, err := e.Done(ctx); err != nil {
		return nil, err
	}
	for _, result := range e.EventResults {
		if result.Status == EventResultError {
			return nil, errors.New(toErrorString(result.Error))
		}
		if result.Status == EventResultCompleted {
			return result.Result, nil
		}
	}
	return nil, nil
}

func (e *BaseEvent) First(ctx context.Context) (any, error) {
	if _, err := e.Done(ctx); err != nil {
		return nil, err
	}
	for _, result := range e.EventResults {
		if result.Status == EventResultCompleted && result.Result != nil {
			return result.Result, nil
		}
	}
	for _, result := range e.EventResults {
		if result.Status == EventResultError {
			return nil, errors.New(toErrorString(result.Error))
		}
	}
	return nil, nil
}

func (e *BaseEvent) EventResultsList(ctx context.Context, include func(result any, event_result *EventResult) bool, options *EventResultsListOptions) ([]any, error) {
	if options == nil {
		options = &EventResultsListOptions{RaiseIfAny: true, RaiseIfNone: true}
	}
	if !options.RaiseIfAny && !options.RaiseIfNone && options.Timeout == nil {
		// keep defaults explicit for common non-raising mode
	}
	if options.Timeout != nil {
		ctx2, cancel := context.WithTimeout(ctx, time.Duration(*options.Timeout*float64(time.Second)))
		defer cancel()
		ctx = ctx2
	}
	if _, err := e.Done(ctx); err != nil {
		return nil, err
	}
	if include == nil {
		include = func(result any, event_result *EventResult) bool {
			if event_result.Status != EventResultCompleted || result == nil {
				return false
			}
			_, is_event := result.(*BaseEvent)
			return !is_event
		}
	}
	out := make([]any, 0, len(e.EventResults))
	for _, event_result := range e.EventResults {
		if options.RaiseIfAny && event_result.Status == EventResultError {
			return nil, errors.New(toErrorString(event_result.Error))
		}
		if include(event_result.Result, event_result) {
			out = append(out, event_result.Result)
		}
	}
	if options.RaiseIfNone && len(out) == 0 {
		return nil, errors.New("no valid handler results")
	}
	return out, nil
}

func toErrorString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	b, _ := json.Marshal(v)
	return string(b)
}
