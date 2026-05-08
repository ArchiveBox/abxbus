package abxbus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/ArchiveBox/abxbus/abxbus-go/jsonschema"
)

const jsonSchemaDraft202012 = jsonschema.Draft202012

type BaseEvent struct {
	EventID                     string                      `json:"event_id"`
	EventCreatedAt              string                      `json:"event_created_at"`
	EventType                   string                      `json:"event_type"`
	EventVersion                string                      `json:"event_version"`
	EventTimeout                *float64                    `json:"event_timeout"`
	EventSlowTimeout            *float64                    `json:"event_slow_timeout,omitempty"`
	EventHandlerTimeout         *float64                    `json:"event_handler_timeout,omitempty"`
	EventHandlerSlowTimeout     *float64                    `json:"event_handler_slow_timeout,omitempty"`
	EventParentID               *string                     `json:"event_parent_id,omitempty"`
	EventPath                   []string                    `json:"event_path,omitempty"`
	EventResultType             any                         `json:"event_result_type,omitempty"`
	EventEmittedByHandlerID     *string                     `json:"event_emitted_by_handler_id,omitempty"`
	EventPendingBusCount        int                         `json:"event_pending_bus_count"`
	EventStatus                 string                      `json:"event_status"`
	EventStartedAt              *string                     `json:"event_started_at,omitempty"`
	EventCompletedAt            *string                     `json:"event_completed_at,omitempty"`
	EventConcurrency            EventConcurrencyMode        `json:"event_concurrency,omitempty"`
	EventHandlerConcurrency     EventHandlerConcurrencyMode `json:"event_handler_concurrency,omitempty"`
	EventHandlerCompletion      EventHandlerCompletionMode  `json:"event_handler_completion,omitempty"`
	EventBlocksParentCompletion bool                        `json:"event_blocks_parent_completion"`

	Payload            map[string]any
	Bus                *EventBus               `json:"-"`
	EventResults       map[string]*EventResult `json:"-"`
	eventResultOrder   []string
	eventResultTypeRaw json.RawMessage
	dispatchCtx        context.Context `json:"-"`
	mu                 sync.Mutex
	done_ch            chan struct{}
	done_once          sync.Once
}

type EventResultsListOptions struct {
	Timeout     *float64
	RaiseIfAny  bool
	RaiseIfNone bool
}

type BaseEventResultUpdateOptions struct {
	EventBus *EventBus
	Timeout  *float64
	EventResultUpdateOptions
}

func NewBaseEvent(event_type string, payload map[string]any) *BaseEvent {
	id := newUUIDv7String()
	if payload == nil {
		payload = map[string]any{}
	}
	return &BaseEvent{
		EventID: id, EventCreatedAt: monotonicDatetime(), EventType: event_type, EventVersion: "0.0.1",
		EventStatus: "pending", EventPath: []string{}, EventResults: map[string]*EventResult{}, eventResultOrder: []string{}, EventPendingBusCount: 0,
		Payload: payload, done_ch: make(chan struct{}),
	}
}

func (e *BaseEvent) MarshalJSON() ([]byte, error) {
	entries := []jsonObjectEntry{
		{key: "event_type", value: e.EventType},
		{key: "event_version", value: e.EventVersion},
		{key: "event_timeout", value: e.EventTimeout},
		{key: "event_slow_timeout", value: e.EventSlowTimeout},
	}
	if e.EventConcurrency == "" {
		entries = append(entries, jsonObjectEntry{key: "event_concurrency", value: nil})
	} else {
		entries = append(entries, jsonObjectEntry{key: "event_concurrency", value: e.EventConcurrency})
	}
	entries = append(entries,
		jsonObjectEntry{key: "event_handler_timeout", value: e.EventHandlerTimeout},
		jsonObjectEntry{key: "event_handler_slow_timeout", value: e.EventHandlerSlowTimeout},
	)
	if e.EventHandlerConcurrency == "" {
		entries = append(entries, jsonObjectEntry{key: "event_handler_concurrency", value: nil})
	} else {
		entries = append(entries, jsonObjectEntry{key: "event_handler_concurrency", value: e.EventHandlerConcurrency})
	}
	if e.EventHandlerCompletion == "" {
		entries = append(entries, jsonObjectEntry{key: "event_handler_completion", value: nil})
	} else {
		entries = append(entries, jsonObjectEntry{key: "event_handler_completion", value: e.EventHandlerCompletion})
	}
	entries = append(entries,
		jsonObjectEntry{key: "event_blocks_parent_completion", value: e.EventBlocksParentCompletion},
		jsonObjectEntry{key: "event_result_type", value: e.eventResultTypeJSONValue()},
		jsonObjectEntry{key: "event_id", value: e.EventID},
		jsonObjectEntry{key: "event_path", value: e.EventPath},
		jsonObjectEntry{key: "event_parent_id", value: e.EventParentID},
		jsonObjectEntry{key: "event_emitted_by_handler_id", value: e.EventEmittedByHandlerID},
		jsonObjectEntry{key: "event_pending_bus_count", value: e.EventPendingBusCount},
		jsonObjectEntry{key: "event_created_at", value: e.EventCreatedAt},
		jsonObjectEntry{key: "event_status", value: e.EventStatus},
		jsonObjectEntry{key: "event_started_at", value: e.EventStartedAt},
		jsonObjectEntry{key: "event_completed_at", value: e.EventCompletedAt},
	)
	if len(e.EventResults) > 0 {
		resultEntries := make([]jsonObjectEntry, 0, len(e.EventResults))
		for _, result := range e.sortedEventResults() {
			resultEntries = append(resultEntries, jsonObjectEntry{key: result.HandlerID, value: result})
		}
		resultsJSON, err := marshalOrderedObject(resultEntries)
		if err != nil {
			return nil, err
		}
		entries = append(entries, jsonObjectEntry{key: "event_results", value: json.RawMessage(resultsJSON)})
	}
	payloadKeys := make([]string, 0, len(e.Payload))
	for key := range e.Payload {
		payloadKeys = append(payloadKeys, key)
	}
	sort.Strings(payloadKeys)
	for _, key := range payloadKeys {
		entries = append(entries, jsonObjectEntry{key: key, value: e.Payload[key]})
	}
	return marshalOrderedObject(entries)
}

func (e *BaseEvent) UnmarshalJSON(data []byte) error {
	var record map[string]any
	if err := json.Unmarshal(data, &record); err != nil {
		return err
	}
	type meta struct {
		EventID                     string                      `json:"event_id"`
		EventCreatedAt              string                      `json:"event_created_at"`
		EventType                   string                      `json:"event_type"`
		EventVersion                string                      `json:"event_version"`
		EventTimeout                *float64                    `json:"event_timeout"`
		EventSlowTimeout            *float64                    `json:"event_slow_timeout,omitempty"`
		EventHandlerTimeout         *float64                    `json:"event_handler_timeout,omitempty"`
		EventHandlerSlowTimeout     *float64                    `json:"event_handler_slow_timeout,omitempty"`
		EventParentID               *string                     `json:"event_parent_id,omitempty"`
		EventPath                   []string                    `json:"event_path,omitempty"`
		EventResultType             json.RawMessage             `json:"event_result_type,omitempty"`
		EventEmittedByHandlerID     *string                     `json:"event_emitted_by_handler_id,omitempty"`
		EventPendingBusCount        int                         `json:"event_pending_bus_count"`
		EventStatus                 string                      `json:"event_status"`
		EventStartedAt              *string                     `json:"event_started_at,omitempty"`
		EventCompletedAt            *string                     `json:"event_completed_at,omitempty"`
		EventConcurrency            EventConcurrencyMode        `json:"event_concurrency,omitempty"`
		EventHandlerConcurrency     EventHandlerConcurrencyMode `json:"event_handler_concurrency,omitempty"`
		EventHandlerCompletion      EventHandlerCompletionMode  `json:"event_handler_completion,omitempty"`
		EventBlocksParentCompletion bool                        `json:"event_blocks_parent_completion,omitempty"`
		EventResults                json.RawMessage             `json:"event_results,omitempty"`
	}
	var m meta
	if err := json.Unmarshal(data, &m); err != nil {
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
	if len(m.EventResultType) > 0 && string(m.EventResultType) != "null" {
		var resultType any
		if err := json.Unmarshal(m.EventResultType, &resultType); err != nil {
			return err
		}
		e.EventResultType = normalizeEventResultTypeSchema(resultType)
		if rawEventResultTypeIsObject(m.EventResultType) {
			e.eventResultTypeRaw = append(json.RawMessage(nil), m.EventResultType...)
		} else {
			e.eventResultTypeRaw = nil
		}
	} else {
		e.EventResultType = nil
		e.eventResultTypeRaw = nil
	}
	e.EventEmittedByHandlerID = m.EventEmittedByHandlerID
	e.EventPendingBusCount = m.EventPendingBusCount
	e.EventStatus = m.EventStatus
	e.EventStartedAt = m.EventStartedAt
	e.EventCompletedAt = m.EventCompletedAt
	e.EventConcurrency = m.EventConcurrency
	e.EventHandlerConcurrency = m.EventHandlerConcurrency
	e.EventHandlerCompletion = m.EventHandlerCompletion
	e.EventBlocksParentCompletion = m.EventBlocksParentCompletion
	e.Payload = map[string]any{}
	known := map[string]bool{"event_id": true, "event_created_at": true, "event_type": true, "event_version": true, "event_timeout": true, "event_slow_timeout": true, "event_handler_timeout": true, "event_handler_slow_timeout": true, "event_parent_id": true, "event_path": true, "event_result_type": true, "event_emitted_by_handler_id": true, "event_pending_bus_count": true, "event_status": true, "event_started_at": true, "event_completed_at": true, "event_concurrency": true, "event_handler_concurrency": true, "event_handler_completion": true, "event_blocks_parent_completion": true, "event_results": true}
	for k, v := range record {
		if !known[k] {
			e.Payload[k] = v
		}
	}
	e.EventResults = map[string]*EventResult{}
	e.eventResultOrder = []string{}
	if len(m.EventResults) > 0 && string(m.EventResults) != "null" {
		if keyedResults, ok, err := orderedJSONRawObjectEntries(m.EventResults); err != nil {
			return err
		} else if ok {
			for _, entry := range keyedResults {
				result, err := EventResultFromJSON(entry.raw)
				if err != nil {
					return err
				}
				if result.HandlerID == "" {
					result.HandlerID = entry.key
				}
				e.EventResults[result.HandlerID] = result
				e.noteEventResultOrder(result.HandlerID)
			}
		} else {
			var resultList []json.RawMessage
			if err := json.Unmarshal(m.EventResults, &resultList); err != nil {
				return err
			}
			for _, raw_result := range resultList {
				result, err := EventResultFromJSON(raw_result)
				if err != nil {
					return err
				}
				e.EventResults[result.HandlerID] = result
				e.noteEventResultOrder(result.HandlerID)
			}
		}
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

func (e *BaseEvent) EventReset() (*BaseEvent, error) {
	data, err := e.ToJSON()
	if err != nil {
		return nil, err
	}
	fresh, err := BaseEventFromJSON(data)
	if err != nil {
		return nil, err
	}
	fresh.EventID = newUUIDv7String()
	resetInboundEvent(fresh)
	return fresh, nil
}

func (e *BaseEvent) EventResultUpdate(handler *EventHandler, options *BaseEventResultUpdateOptions) *EventResult {
	if handler == nil {
		return nil
	}
	if options == nil {
		options = &BaseEventResultUpdateOptions{}
	}
	e.mu.Lock()
	if e.EventResults == nil {
		e.EventResults = map[string]*EventResult{}
	}

	result := e.EventResults[handler.ID]
	if result == nil {
		status := options.Status
		if status == "" {
			status = EventResultPending
		}
		result = NewEventResult(e, handler)
		result.Status = status
		if options.Timeout != nil {
			result.HandlerTimeout = options.Timeout
		}
		e.EventResults[handler.ID] = result
	}
	e.mu.Unlock()

	result.mu.Lock()
	result.Event = e
	result.Handler = handler
	result.HandlerID = handler.ID
	result.HandlerName = handler.HandlerName
	result.HandlerFilePath = handler.HandlerFilePath
	result.HandlerSlowTimeout = handler.HandlerSlowTimeout
	result.HandlerRegisteredAt = handler.HandlerRegisteredAt
	result.HandlerEventPattern = handler.EventPattern
	result.EventBusName = handler.EventBusName
	result.EventBusID = handler.EventBusID
	if options.Timeout != nil {
		result.HandlerTimeout = options.Timeout
	} else {
		result.HandlerTimeout = handler.HandlerTimeout
	}
	result.mu.Unlock()
	e.noteEventResultOrder(handler.ID)

	result.Update(&options.EventResultUpdateOptions)
	if result.Status == EventResultStarted && result.StartedAt != nil {
		e.EventStatus = "started"
		if e.EventStartedAt == nil {
			e.EventStartedAt = result.StartedAt
		}
	}
	if result.Status == EventResultPending || result.Status == EventResultStarted {
		e.EventCompletedAt = nil
	}
	if options.EventBus != nil {
		e.Bus = options.EventBus
	}
	return result
}

func (e *BaseEvent) eventResultTypeJSONValue() any {
	if e.EventResultType == nil {
		return nil
	}
	if len(e.eventResultTypeRaw) > 0 && rawEventResultTypeMatchesValue(e.eventResultTypeRaw, e.EventResultType) {
		return json.RawMessage(e.eventResultTypeRaw)
	}
	return normalizeEventResultTypeSchema(e.EventResultType)
}

func rawEventResultTypeIsObject(raw json.RawMessage) bool {
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		return false
	}
	return true
}

func rawEventResultTypeMatchesValue(raw json.RawMessage, value any) bool {
	var rawValue any
	if err := json.Unmarshal(raw, &rawValue); err != nil {
		return false
	}
	normalizedRaw := normalizeJSONValue(rawValue)
	normalizedValue := normalizeJSONValue(value)
	if reflect.DeepEqual(normalizedRaw, normalizedValue) {
		return true
	}
	rawSchema, rawOK := normalizedRaw.(map[string]any)
	valueSchema, valueOK := normalizedValue.(map[string]any)
	if !rawOK || !valueOK {
		return false
	}
	if _, rawHasDraftSchema := rawSchema["$schema"]; rawHasDraftSchema {
		return false
	}
	valueSchemaWithoutDraft := make(map[string]any, len(valueSchema))
	for key, entry := range valueSchema {
		if key != "$schema" {
			valueSchemaWithoutDraft[key] = entry
		}
	}
	return reflect.DeepEqual(rawSchema, valueSchemaWithoutDraft)
}

func normalizeEventResultTypeSchema(value any) any {
	normalized := normalizeJSONValue(value)
	schema, ok := normalized.(map[string]any)
	if !ok {
		return normalized
	}
	out := make(map[string]any, len(schema)+1)
	for key, entry := range schema {
		out[key] = entry
	}
	if _, ok := out["$schema"]; !ok {
		out["$schema"] = jsonSchemaDraft202012
	}
	return out
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

func (e *BaseEvent) status() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.EventStatus
}

func (e *BaseEvent) EventCompleted(ctx context.Context) error {
	e.mu.Lock()
	bus := e.Bus
	e.mu.Unlock()
	if bus != nil {
		if active := bus.locks.getActiveHandlerResult(); active != nil {
			active.releaseQueueJumpPauseFor(bus)
		}
		bus.startRunloop()
	}
	select {
	case <-e.done_ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *BaseEvent) Done(ctx context.Context) (*BaseEvent, error) {
	if e.status() == "completed" {
		return e, nil
	}
	e.mu.Lock()
	bus := e.Bus
	if ctx != nil {
		e.dispatchCtx = ctx
	}
	e.mu.Unlock()
	if bus == nil {
		return nil, errors.New("event has no bus attached")
	}
	e.markBlocksParentCompletionIfAwaitedFromEmittingHandler()
	_, err := bus.processEventImmediatelyAcrossBuses(ctx, e)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (e *BaseEvent) Emit(event *BaseEvent) *BaseEvent {
	e.mu.Lock()
	bus := e.Bus
	e.mu.Unlock()
	if bus == nil {
		return event
	}
	if event.EventID != e.EventID {
		if event.EventParentID == nil {
			parentID := e.EventID
			event.EventParentID = &parentID
		}
		if active := bus.locks.getActiveHandlerResult(); active != nil && active.EventID == e.EventID {
			active.ensureQueueJumpPause(bus)
			if event.EventEmittedByHandlerID == nil {
				handlerID := active.HandlerID
				event.EventEmittedByHandlerID = &handlerID
			}
			active.addChild(event)
		}
	}
	return bus.Emit(event)
}

func (e *BaseEvent) markBlocksParentCompletionIfAwaitedFromEmittingHandler() {
	e.mu.Lock()
	bus := e.Bus
	blocksParentCompletion := e.EventBlocksParentCompletion
	parentID := e.EventParentID
	emittedByHandlerID := e.EventEmittedByHandlerID
	e.mu.Unlock()
	if blocksParentCompletion || bus == nil || parentID == nil || emittedByHandlerID == nil {
		return
	}
	active := bus.locks.getActiveHandlerResult()
	if active == nil || active.EventID != *parentID || active.HandlerID != *emittedByHandlerID {
		return
	}
	for _, child := range active.EventChildren {
		if child.EventID == e.EventID {
			e.mu.Lock()
			e.EventBlocksParentCompletion = true
			e.mu.Unlock()
			return
		}
	}
}

func (e *BaseEvent) EventResult(ctx context.Context) (any, error) {
	if _, err := e.Done(ctx); err != nil {
		return nil, err
	}
	for _, result := range e.sortedEventResults() {
		if result.Status == EventResultError {
			return nil, errors.New(toErrorString(result.Error))
		}
	}
	for _, result := range e.sortedEventResults() {
		if result.Status == EventResultCompleted && result.Result != nil && !isBaseEventResult(result.Result) {
			return result.Result, nil
		}
	}
	return nil, nil
}

func (e *BaseEvent) First(ctx context.Context) (any, error) {
	e.EventHandlerCompletion = EventHandlerCompletionFirst
	if _, err := e.Done(ctx); err != nil {
		return nil, err
	}
	for _, result := range e.sortedEventResults() {
		if result.Status == EventResultCompleted && result.Result != nil && !isBaseEventResult(result.Result) {
			return result.Result, nil
		}
	}
	for _, result := range e.sortedEventResults() {
		if result.Status == EventResultError {
			return nil, errors.New(toErrorString(result.Error))
		}
	}
	return nil, nil
}

func isBaseEventResult(result any) bool {
	if _, ok := result.(*BaseEvent); ok {
		return true
	}
	if object, ok := normalizeJSONValue(result).(map[string]any); ok {
		_, hasEventType := object["event_type"]
		_, hasEventID := object["event_id"]
		return hasEventType && hasEventID
	}
	return false
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
			return !isBaseEventResult(result)
		}
	}
	out := make([]any, 0, len(e.EventResults))
	for _, event_result := range e.sortedEventResults() {
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

func (e *BaseEvent) validateResultValue(value any) error {
	if e.EventResultType == nil || value == nil {
		return nil
	}
	if _, isEvent := value.(*BaseEvent); isEvent {
		return nil
	}
	schema, ok := normalizeJSONValue(e.EventResultType).(map[string]any)
	if !ok {
		return nil
	}
	normalized := normalizeJSONValue(value)
	if err := jsonschema.Validate(schema, normalized); err != nil {
		previewBytes, _ := json.Marshal(normalized)
		preview := string(previewBytes)
		if len(preview) > 40 {
			preview = preview[:40]
		}
		return fmt.Errorf("EventHandlerResultSchemaError: Event handler return value %s... did not match event_result_type: %s", preview, err)
	}
	return nil
}

func normalizeJSONValue(value any) any {
	return jsonschema.Normalize(value)
}

func (e *BaseEvent) sortedEventResults() []*EventResult {
	e.mu.Lock()
	if len(e.EventResults) == 0 {
		e.mu.Unlock()
		return nil
	}
	eventResults := make(map[string]*EventResult, len(e.EventResults))
	for handlerID, result := range e.EventResults {
		eventResults[handlerID] = result
	}
	eventResultOrder := append([]string{}, e.eventResultOrder...)
	e.mu.Unlock()

	results := make([]*EventResult, 0, len(eventResults))
	seen := map[string]bool{}
	appendByID := func(handlerID string) {
		if seen[handlerID] {
			return
		}
		if result := eventResults[handlerID]; result != nil {
			results = append(results, result)
			seen[handlerID] = true
		}
	}

	for _, handlerID := range eventResultOrder {
		appendByID(handlerID)
	}

	remaining := make([]*EventResult, 0, len(eventResults)-len(results))
	for handlerID, result := range eventResults {
		if !seen[handlerID] && result != nil {
			remaining = append(remaining, result)
		}
	}
	sort.SliceStable(remaining, func(i, j int) bool {
		return eventResultRegistrationLess(remaining[i], remaining[j])
	})
	results = append(results, remaining...)
	return results
}

func (e *BaseEvent) eventResultsSnapshot() []*EventResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	results := make([]*EventResult, 0, len(e.EventResults))
	for _, result := range e.EventResults {
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}

func (e *BaseEvent) noteEventResultOrder(handlerID string) {
	if handlerID == "" {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, existing := range e.eventResultOrder {
		if existing == handlerID {
			return
		}
	}
	e.eventResultOrder = append(e.eventResultOrder, handlerID)
}

func (e *BaseEvent) eventResultOrderSnapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string{}, e.eventResultOrder...)
}

func (e *BaseEvent) rebuildEventResultOrderByMetadata() {
	results := e.eventResultsSnapshot()
	sort.SliceStable(results, func(i, j int) bool {
		return eventResultRegistrationLess(results[i], results[j])
	})
	e.mu.Lock()
	defer e.mu.Unlock()
	e.eventResultOrder = e.eventResultOrder[:0]
	for _, result := range results {
		e.eventResultOrder = append(e.eventResultOrder, result.HandlerID)
	}
}

func eventResultRegistrationLess(a, b *EventResult) bool {
	if a.HandlerRegisteredAt != b.HandlerRegisteredAt {
		return a.HandlerRegisteredAt < b.HandlerRegisteredAt
	}
	if a.StartedAt != nil && b.StartedAt != nil && *a.StartedAt != *b.StartedAt {
		return *a.StartedAt < *b.StartedAt
	}
	return a.HandlerID < b.HandlerID
}

type orderedJSONRawObjectEntry struct {
	key string
	raw json.RawMessage
}

func orderedJSONRawObjectEntries(data []byte) ([]orderedJSONRawObjectEntry, bool, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return nil, false, err
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		return nil, false, nil
	}
	entries := []orderedJSONRawObjectEntry{}
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, true, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, true, fmt.Errorf("expected JSON object key while decoding event_results")
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return nil, true, err
		}
		entries = append(entries, orderedJSONRawObjectEntry{key: key, raw: raw})
	}
	endToken, err := decoder.Token()
	if err != nil {
		return nil, true, err
	}
	endDelim, ok := endToken.(json.Delim)
	if !ok || endDelim != '}' {
		return nil, true, fmt.Errorf("unterminated JSON object while decoding event_results")
	}
	return entries, true, nil
}

func toErrorString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	if err, ok := v.(error); ok {
		return err.Error()
	}
	b, _ := json.Marshal(v)
	return string(b)
}
