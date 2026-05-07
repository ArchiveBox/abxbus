package abxbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

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

	Payload          map[string]any
	Bus              *EventBus               `json:"-"`
	EventResults     map[string]*EventResult `json:"-"`
	eventResultOrder []string
	dispatchCtx      context.Context `json:"-"`
	mu               sync.Mutex
	done_ch          chan struct{}
	done_once        sync.Once
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
		jsonObjectEntry{key: "event_result_type", value: e.EventResultType},
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
		EventResultType             any                         `json:"event_result_type,omitempty"`
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
	e.EventBlocksParentCompletion = m.EventBlocksParentCompletion
	e.Payload = map[string]any{}
	known := map[string]bool{"event_id": true, "event_created_at": true, "event_type": true, "event_version": true, "event_timeout": true, "event_slow_timeout": true, "event_handler_timeout": true, "event_handler_slow_timeout": true, "event_parent_id": true, "event_path": true, "event_result_type": true, "event_emitted_by_handler_id": true, "event_pending_bus_count": true, "event_status": true, "event_started_at": true, "event_completed_at": true, "event_concurrency": true, "event_handler_concurrency": true, "event_handler_completion": true, "event_blocks_parent_completion": true, "event_results": true}
	for k, v := range record {
		if !known[k] {
			e.Payload[k] = v
		}
	}
	e.EventResults = map[string]*EventResult{}
	if len(m.EventResults) > 0 && string(m.EventResults) != "null" {
		var keyedResults map[string]json.RawMessage
		if err := json.Unmarshal(m.EventResults, &keyedResults); err == nil {
			for handlerID, raw_result := range keyedResults {
				result, err := EventResultFromJSON(raw_result)
				if err != nil {
					return err
				}
				if result.HandlerID == "" {
					result.HandlerID = handlerID
				}
				e.EventResults[result.HandlerID] = result
			}
			e.rebuildEventResultOrderByMetadata()
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
	e.markBlocksParentCompletionIfAwaitedFromEmittingHandler()
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
	e.markBlocksParentCompletionIfAwaitedFromEmittingHandler()
	_, err := e.Bus.processEventImmediately(ctx, e, nil)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (e *BaseEvent) Emit(event *BaseEvent) *BaseEvent {
	if e.Bus == nil {
		return event
	}
	if event.EventID != e.EventID {
		if event.EventParentID == nil {
			parentID := e.EventID
			event.EventParentID = &parentID
		}
		if active := e.Bus.locks.getActiveHandlerResult(); active != nil && active.EventID == e.EventID {
			if event.EventEmittedByHandlerID == nil {
				handlerID := active.HandlerID
				event.EventEmittedByHandlerID = &handlerID
			}
			active.addChild(event)
		}
	}
	return e.Bus.Emit(event)
}

func (e *BaseEvent) markBlocksParentCompletionIfAwaitedFromEmittingHandler() {
	if e.EventBlocksParentCompletion || e.Bus == nil || e.EventParentID == nil || e.EventEmittedByHandlerID == nil {
		return
	}
	active := e.Bus.locks.getActiveHandlerResult()
	if active == nil || active.EventID != *e.EventParentID || active.HandlerID != *e.EventEmittedByHandlerID {
		return
	}
	for _, child := range active.EventChildren {
		if child.EventID == e.EventID {
			e.EventBlocksParentCompletion = true
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
			_, is_event := result.(*BaseEvent)
			return !is_event
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
	if err := validateJSONSchemaValue(schema, schema, normalized, "$"); err != nil {
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
	data, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var normalized any
	if err := json.Unmarshal(data, &normalized); err != nil {
		return value
	}
	return normalized
}

func validateJSONSchemaValue(root map[string]any, schema any, value any, path string) error {
	schemaMap, ok := schema.(map[string]any)
	if !ok {
		return nil
	}
	if ref, ok := schemaMap["$ref"].(string); ok {
		resolved, ok := resolveJSONSchemaRef(root, ref)
		if !ok {
			return fmt.Errorf("%s unresolved schema reference %s", path, ref)
		}
		if err := validateJSONSchemaValue(root, resolved, value, path); err != nil {
			return err
		}
		if len(schemaMap) == 1 {
			return nil
		}
	}
	if anyOf, ok := schemaMap["anyOf"].([]any); ok {
		for _, branch := range anyOf {
			if validateJSONSchemaValue(root, branch, value, path) == nil {
				return nil
			}
		}
		return fmt.Errorf("%s did not match anyOf schema", path)
	}
	if schemaType, ok := schemaMap["type"]; ok {
		if types, ok := schemaType.([]any); ok {
			for _, allowed := range types {
				if jsonSchemaTypeMatches(allowed, value) {
					return validateJSONSchemaChildren(root, schemaMap, value, path)
				}
			}
			return fmt.Errorf("%s did not match any allowed type", path)
		}
		if !jsonSchemaTypeMatches(schemaType, value) {
			if label, ok := schemaType.(string); ok {
				return fmt.Errorf("%s expected %s", path, label)
			}
			return fmt.Errorf("%s expected matching schema type", path)
		}
	} else if schemaMap["properties"] != nil || schemaMap["required"] != nil || schemaMap["additionalProperties"] != nil {
		if _, ok := value.(map[string]any); !ok {
			return fmt.Errorf("%s expected object", path)
		}
	}
	return validateJSONSchemaChildren(root, schemaMap, value, path)
}

func resolveJSONSchemaRef(root map[string]any, ref string) (any, bool) {
	if ref == "#" {
		return root, true
	}
	if !strings.HasPrefix(ref, "#/") {
		return nil, false
	}
	var current any = root
	for _, part := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
		part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func jsonSchemaTypeMatches(schemaType any, value any) bool {
	label, ok := schemaType.(string)
	if !ok {
		return true
	}
	switch label {
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		number, ok := value.(float64)
		return ok && !math.IsNaN(number) && !math.IsInf(number, 0)
	case "integer":
		number, ok := value.(float64)
		return ok && !math.IsNaN(number) && !math.IsInf(number, 0) && math.Trunc(number) == number
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "null":
		return value == nil
	case "array":
		_, ok := value.([]any)
		return ok
	case "object":
		_, ok := value.(map[string]any)
		return ok
	default:
		return true
	}
}

func validateJSONSchemaChildren(root map[string]any, schema map[string]any, value any, path string) error {
	if itemsSchema, ok := schema["items"]; ok {
		if items, ok := value.([]any); ok {
			for idx, item := range items {
				if err := validateJSONSchemaValue(root, itemsSchema, item, fmt.Sprintf("%s[%d]", path, idx)); err != nil {
					return err
				}
			}
		}
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	if required, ok := schema["required"].([]any); ok {
		for _, keyValue := range required {
			key, ok := keyValue.(string)
			if !ok {
				continue
			}
			if _, exists := object[key]; !exists {
				return fmt.Errorf("%s.%s is required", path, key)
			}
		}
	}
	properties, _ := schema["properties"].(map[string]any)
	for key, propertySchema := range properties {
		if propertyValue, exists := object[key]; exists {
			if err := validateJSONSchemaValue(root, propertySchema, propertyValue, path+"."+key); err != nil {
				return err
			}
		}
	}
	switch additional := schema["additionalProperties"].(type) {
	case bool:
		if !additional && properties != nil {
			for key := range object {
				if _, known := properties[key]; !known {
					return fmt.Errorf("%s.%s is not allowed", path, key)
				}
			}
		}
	case map[string]any:
		for key, item := range object {
			if properties != nil {
				if _, known := properties[key]; known {
					continue
				}
			}
			if err := validateJSONSchemaValue(root, additional, item, path+"."+key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *BaseEvent) sortedEventResults() []*EventResult {
	if len(e.EventResults) == 0 {
		return nil
	}

	results := make([]*EventResult, 0, len(e.EventResults))
	seen := map[string]bool{}
	appendByID := func(handlerID string) {
		if seen[handlerID] {
			return
		}
		if result := e.EventResults[handlerID]; result != nil {
			results = append(results, result)
			seen[handlerID] = true
		}
	}

	for _, handlerID := range e.eventResultOrderSnapshot() {
		appendByID(handlerID)
	}

	remaining := make([]*EventResult, 0, len(e.EventResults)-len(results))
	for handlerID, result := range e.EventResults {
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
	results := make([]*EventResult, 0, len(e.EventResults))
	for _, result := range e.EventResults {
		if result != nil {
			results = append(results, result)
		}
	}
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
