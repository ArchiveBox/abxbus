package abxbus

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
	ID                  string            `json:"id"`
	Status              EventResultStatus `json:"status"`
	EventID             string            `json:"event_id"`
	HandlerID           string            `json:"handler_id"`
	HandlerName         string            `json:"handler_name"`
	HandlerFilePath     *string           `json:"handler_file_path,omitempty"`
	HandlerTimeout      *float64          `json:"handler_timeout,omitempty"`
	HandlerSlowTimeout  *float64          `json:"handler_slow_timeout,omitempty"`
	HandlerRegisteredAt string            `json:"handler_registered_at"`
	HandlerEventPattern string            `json:"handler_event_pattern"`
	EventBusName        string            `json:"eventbus_name"`
	EventBusID          string            `json:"eventbus_id"`
	StartedAt           *string           `json:"started_at,omitempty"`
	CompletedAt         *string           `json:"completed_at,omitempty"`
	Result              any               `json:"result,omitempty"`
	Error               any               `json:"error,omitempty"`
	EventChildren       []*BaseEvent      `json:"-"`
	EventChildIDs       []string          `json:"-"`

	Event   *BaseEvent    `json:"-"`
	Handler *EventHandler `json:"-"`

	mu                     sync.Mutex
	queueJumpPauseReleases map[*EventBus]func()
	done_ch                chan struct{}
	once                   sync.Once
}

type EventResultUpdateOptions struct {
	Status    EventResultStatus
	Result    any
	Error     any
	ResultSet bool
	ErrorSet  bool
}

type eventResultJSON struct {
	ID                  string            `json:"id"`
	Status              EventResultStatus `json:"status"`
	EventID             string            `json:"event_id"`
	HandlerID           string            `json:"handler_id"`
	HandlerName         string            `json:"handler_name"`
	HandlerFilePath     *string           `json:"handler_file_path"`
	HandlerTimeout      *float64          `json:"handler_timeout"`
	HandlerSlowTimeout  *float64          `json:"handler_slow_timeout"`
	HandlerRegisteredAt string            `json:"handler_registered_at"`
	HandlerEventPattern string            `json:"handler_event_pattern"`
	EventBusName        string            `json:"eventbus_name"`
	EventBusID          string            `json:"eventbus_id"`
	StartedAt           *string           `json:"started_at"`
	CompletedAt         *string           `json:"completed_at"`
	Result              any               `json:"result"`
	Error               any               `json:"error"`
	EventChildren       []string          `json:"event_children"`
}

func NewEventResult(event *BaseEvent, handler *EventHandler) *EventResult {
	return &EventResult{
		ID:                  newUUIDv7String(),
		Status:              EventResultPending,
		EventID:             event.EventID,
		HandlerID:           handler.ID,
		HandlerName:         handler.HandlerName,
		HandlerFilePath:     handler.HandlerFilePath,
		HandlerTimeout:      handler.HandlerTimeout,
		HandlerSlowTimeout:  handler.HandlerSlowTimeout,
		HandlerRegisteredAt: handler.HandlerRegisteredAt,
		HandlerEventPattern: handler.EventPattern,
		EventBusName:        handler.EventBusName,
		EventBusID:          handler.EventBusID,
		Event:               event,
		Handler:             handler,
		done_ch:             make(chan struct{}),
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

func (r *EventResult) markCompleted(result any) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.Status == EventResultCompleted || r.Status == EventResultError {
		return false
	}
	r.Status = EventResultCompleted
	r.Result = result
	now := monotonicDatetime()
	r.CompletedAt = &now
	r.once.Do(func() { close(r.done_ch) })
	return true
}

func (r *EventResult) markError(err error) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.Status == EventResultCompleted || r.Status == EventResultError {
		return false
	}
	r.Status = EventResultError
	if err != nil {
		r.Error = err.Error()
	}
	now := monotonicDatetime()
	r.CompletedAt = &now
	r.once.Do(func() { close(r.done_ch) })
	return true
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
	r.EventChildIDs = append(r.EventChildIDs, event.EventID)
}

func (r *EventResult) childEvents() []*BaseEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*BaseEvent{}, r.EventChildren...)
}

func (r *EventResult) ensureQueueJumpPause(bus *EventBus) {
	if bus == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.queueJumpPauseReleases == nil {
		r.queueJumpPauseReleases = map[*EventBus]func(){}
	}
	if _, ok := r.queueJumpPauseReleases[bus]; ok {
		return
	}
	r.queueJumpPauseReleases[bus] = bus.locks.requestRunloopPause()
}

func (r *EventResult) releaseQueueJumpPauses() {
	r.mu.Lock()
	releases := r.queueJumpPauseReleases
	r.queueJumpPauseReleases = nil
	r.mu.Unlock()
	for _, release := range releases {
		release()
	}
}

func (r *EventResult) releaseQueueJumpPauseFor(bus *EventBus) {
	if bus == nil {
		return
	}
	r.mu.Lock()
	var release func()
	if r.queueJumpPauseReleases != nil {
		release = r.queueJumpPauseReleases[bus]
		delete(r.queueJumpPauseReleases, bus)
		if len(r.queueJumpPauseReleases) == 0 {
			r.queueJumpPauseReleases = nil
		}
	}
	r.mu.Unlock()
	if release != nil {
		release()
	}
}

func (r *EventResult) MarshalJSON() ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	childIDs := append([]string{}, r.EventChildIDs...)
	if len(childIDs) == 0 {
		for _, child := range r.EventChildren {
			childIDs = append(childIDs, child.EventID)
		}
	}
	payload := eventResultJSON{
		ID:                  r.ID,
		Status:              r.Status,
		EventID:             r.EventID,
		HandlerID:           r.HandlerID,
		HandlerName:         r.HandlerName,
		HandlerFilePath:     r.HandlerFilePath,
		HandlerTimeout:      r.HandlerTimeout,
		HandlerSlowTimeout:  r.HandlerSlowTimeout,
		HandlerRegisteredAt: r.HandlerRegisteredAt,
		HandlerEventPattern: r.HandlerEventPattern,
		EventBusName:        r.EventBusName,
		EventBusID:          r.EventBusID,
		StartedAt:           r.StartedAt,
		CompletedAt:         r.CompletedAt,
		Result:              r.Result,
		Error:               r.Error,
		EventChildren:       childIDs,
	}
	return json.Marshal(payload)
}

func (r *EventResult) UnmarshalJSON(data []byte) error {
	var parsed eventResultJSON
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	r.ID = parsed.ID
	r.Status = parsed.Status
	r.EventID = parsed.EventID
	r.HandlerID = parsed.HandlerID
	r.HandlerName = parsed.HandlerName
	r.HandlerFilePath = parsed.HandlerFilePath
	r.HandlerTimeout = parsed.HandlerTimeout
	r.HandlerSlowTimeout = parsed.HandlerSlowTimeout
	r.HandlerRegisteredAt = parsed.HandlerRegisteredAt
	r.HandlerEventPattern = parsed.HandlerEventPattern
	r.EventBusName = parsed.EventBusName
	r.EventBusID = parsed.EventBusID
	r.StartedAt = parsed.StartedAt
	r.CompletedAt = parsed.CompletedAt
	r.Result = parsed.Result
	r.Error = parsed.Error
	r.EventChildIDs = append([]string{}, parsed.EventChildren...)
	r.EventChildren = nil
	r.done_ch = make(chan struct{})
	r.once = sync.Once{}
	if r.Status == EventResultCompleted || r.Status == EventResultError {
		r.once.Do(func() { close(r.done_ch) })
	}
	return nil
}

func (r *EventResult) ToJSON() ([]byte, error) { return json.Marshal(r) }

func (r *EventResult) Update(options *EventResultUpdateOptions) *EventResult {
	if options == nil {
		return r
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done_ch == nil {
		r.done_ch = make(chan struct{})
	}

	hasResult := options.ResultSet || options.Result != nil
	if hasResult {
		if errValue, ok := options.Result.(error); ok {
			r.Result = nil
			r.Error = errValue.Error()
			r.Status = EventResultError
		} else if r.Event != nil {
			if err := r.Event.validateResultValue(options.Result); err != nil {
				r.Result = nil
				r.Error = err.Error()
				r.Status = EventResultError
			} else {
				r.Result = options.Result
				r.Status = EventResultCompleted
			}
		} else {
			r.Result = options.Result
			r.Status = EventResultCompleted
		}
	}

	hasError := options.ErrorSet || options.Error != nil
	if hasError {
		r.Error = toErrorString(options.Error)
		r.Status = EventResultError
	}

	if options.Status != "" {
		r.Status = options.Status
	}
	if r.Status != EventResultPending && r.StartedAt == nil {
		now := monotonicDatetime()
		r.StartedAt = &now
	}
	if (r.Status == EventResultCompleted || r.Status == EventResultError) && r.CompletedAt == nil {
		now := monotonicDatetime()
		r.CompletedAt = &now
		r.once.Do(func() { close(r.done_ch) })
	}
	return r
}

func (r *EventResult) replaceError(message string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.Status == EventResultCompleted || r.Status == EventResultError {
		return false
	}
	r.Status = EventResultError
	r.Error = message
	now := monotonicDatetime()
	r.CompletedAt = &now
	r.once.Do(func() { close(r.done_ch) })
	return true
}
