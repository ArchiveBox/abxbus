package bubus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

var SlowWarningLogger = func(message string) {
	fmt.Println(message)
}

type EventBusOptions struct {
	ID                          string
	MaxHistorySize              *int
	MaxHistoryDrop              bool
	EventConcurrency            EventConcurrencyMode
	EventTimeout                *float64
	EventSlowTimeout            *float64
	EventHandlerConcurrency     EventHandlerConcurrencyMode
	EventHandlerCompletion      EventHandlerCompletionMode
	EventHandlerSlowTimeout     *float64
	EventHandlerDetectFilePaths bool
}

type FindOptions struct {
	Past    any
	Future  any
	ChildOf *BaseEvent
	Equals  map[string]any
}

type findWaiter struct {
	EventPattern string
	Matches      func(event *BaseEvent) bool
	Resolve      func(event *BaseEvent)
}

type EventBusJSON struct {
	ID                          string                      `json:"id"`
	Name                        string                      `json:"name"`
	MaxHistorySize              *int                        `json:"max_history_size"`
	MaxHistoryDrop              bool                        `json:"max_history_drop"`
	EventConcurrency            EventConcurrencyMode        `json:"event_concurrency"`
	EventTimeout                *float64                    `json:"event_timeout"`
	EventSlowTimeout            *float64                    `json:"event_slow_timeout"`
	EventHandlerConcurrency     EventHandlerConcurrencyMode `json:"event_handler_concurrency"`
	EventHandlerCompletion      EventHandlerCompletionMode  `json:"event_handler_completion"`
	EventHandlerSlowTimeout     *float64                    `json:"event_handler_slow_timeout"`
	EventHandlerDetectFilePaths bool                        `json:"event_handler_detect_file_paths"`
	Handlers                    map[string]*EventHandler    `json:"handlers"`
	HandlersByKey               map[string][]string         `json:"handlers_by_key"`
	EventHistory                map[string]*BaseEvent       `json:"event_history"`
	PendingEventQueue           []string                    `json:"pending_event_queue"`
}

type EventBus struct {
	ID   string
	Name string

	EventTimeout            *float64
	EventConcurrency        EventConcurrencyMode
	EventHandlerConcurrency EventHandlerConcurrencyMode
	EventHandlerCompletion  EventHandlerCompletionMode

	EventHandlerSlowTimeout *float64
	EventSlowTimeout        *float64

	handlers          map[string]*EventHandler
	handlersByKey     map[string][]string
	EventHistory      *EventHistory
	pendingEventQueue []*BaseEvent
	inFlightEventIDs  map[string]bool
	runloopRunning    bool
	locks             *LockManager
	findWaiters       []*findWaiter

	mu          sync.Mutex
	global_lock *AsyncLock
}

func NewEventBus(name string, options *EventBusOptions) *EventBus {
	if name == "" {
		name = "EventBus"
	}
	if options == nil {
		options = &EventBusOptions{}
	}
	id := options.ID
	if id == "" {
		id = newUUIDv7String()
	}
	max_history_size := options.MaxHistorySize
	if max_history_size == nil {
		max_history_size = ptr(100)
	}
	event_timeout := options.EventTimeout
	if event_timeout == nil {
		event_timeout = ptr(60.0)
	}
	bus := &EventBus{
		ID: id, Name: name,
		EventTimeout:            event_timeout,
		EventConcurrency:        options.EventConcurrency,
		EventHandlerConcurrency: options.EventHandlerConcurrency,
		EventHandlerCompletion:  options.EventHandlerCompletion,
		EventHandlerSlowTimeout: options.EventHandlerSlowTimeout,
		EventSlowTimeout:        options.EventSlowTimeout,
		handlers:                map[string]*EventHandler{},
		handlersByKey:           map[string][]string{},
		EventHistory:            NewEventHistory(max_history_size, options.MaxHistoryDrop),
		pendingEventQueue:       []*BaseEvent{},
		inFlightEventIDs:        map[string]bool{},
		findWaiters:             []*findWaiter{},
		global_lock:             shared_global_event_lock,
	}
	if bus.EventConcurrency == "" {
		bus.EventConcurrency = EventConcurrencyBusSerial
	}
	if bus.EventHandlerConcurrency == "" {
		bus.EventHandlerConcurrency = EventHandlerConcurrencySerial
	}
	if bus.EventHandlerCompletion == "" {
		bus.EventHandlerCompletion = EventHandlerCompletionAll
	}
	bus.locks = NewLockManager(bus)
	return bus
}

func (b *EventBus) Label() string { return fmt.Sprintf("%s#%s", b.Name, b.ID[len(b.ID)-4:]) }

func (b *EventBus) On(event_pattern string, handler_name string, handler EventHandlerCallable, options *EventHandler) *EventHandler {
	if event_pattern == "" {
		event_pattern = "*"
	}
	h := NewEventHandler(b.Name, b.ID, event_pattern, handler_name, handler)
	if options != nil {
		h.HandlerTimeout = options.HandlerTimeout
		h.HandlerSlowTimeout = options.HandlerSlowTimeout
		h.HandlerFilePath = options.HandlerFilePath
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[h.ID] = h
	b.handlersByKey[event_pattern] = append(b.handlersByKey[event_pattern], h.ID)
	return h
}

func (b *EventBus) Off(event_pattern string, handler any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	ids := b.handlersByKey[event_pattern]
	if handler == nil {
		for _, id := range ids {
			delete(b.handlers, id)
		}
		delete(b.handlersByKey, event_pattern)
		return
	}
	for i := len(ids) - 1; i >= 0; i-- {
		id := ids[i]
		h := b.handlers[id]
		match := false
		switch v := handler.(type) {
		case string:
			match = id == v
		case *EventHandler:
			match = id == v.ID
		}
		if match {
			ids = append(ids[:i], ids[i+1:]...)
			delete(b.handlers, id)
		} else if h == nil {
			ids = append(ids[:i], ids[i+1:]...)
		}
	}
	if len(ids) == 0 {
		delete(b.handlersByKey, event_pattern)
	} else {
		b.handlersByKey[event_pattern] = ids
	}
}

func (b *EventBus) Emit(event *BaseEvent) *BaseEvent {
	original_event := event
	if event.Bus == nil {
		original_event.Bus = b
	}
	if original_event.dispatchCtx == nil {
		original_event.dispatchCtx = b.locks.getActiveDispatchContext()
		if original_event.dispatchCtx == nil {
			original_event.dispatchCtx = context.Background()
		}
	}
	for _, label := range original_event.EventPath {
		if label == b.Label() {
			return original_event
		}
	}
	original_event.EventPath = append(original_event.EventPath, b.Label())
	if original_event.EventParentID == nil && original_event.EventEmittedByHandlerID == nil {
		if active := b.locks.getActiveHandlerResult(); active != nil {
			if active.EventID != original_event.EventID {
				parent_id := active.EventID
				handler_id := active.HandlerID
				original_event.EventParentID = &parent_id
				original_event.EventEmittedByHandlerID = &handler_id
				active.EventChildren = append(active.EventChildren, original_event)
			}
		}
	}
	b.EventHistory.AddEvent(original_event)
	b.resolveFindWaiters(original_event)
	original_event.EventPendingBusCount++
	b.mu.Lock()
	b.pendingEventQueue = append(b.pendingEventQueue, original_event)
	b.mu.Unlock()
	b.startRunloop()
	return original_event
}

func (b *EventBus) Dispatch(event *BaseEvent) *BaseEvent { return b.Emit(event) }

func (b *EventBus) getHandlersForEvent(event *BaseEvent) []*EventHandler {
	b.mu.Lock()
	defer b.mu.Unlock()
	ids := append([]string{}, b.handlersByKey[event.EventType]...)
	ids = append(ids, b.handlersByKey["*"]...)
	handlers := make([]*EventHandler, 0, len(ids))
	for _, id := range ids {
		if h := b.handlers[id]; h != nil {
			handlers = append(handlers, h)
		}
	}
	return handlers
}

func runWithTimeout(ctx context.Context, timeout_seconds *float64, on_timeout func() error, fn func(context.Context) error) error {
	if timeout_seconds == nil {
		return fn(ctx)
	}
	ctx2, cancel := context.WithTimeout(ctx, time.Duration(*timeout_seconds*float64(time.Second)))
	defer cancel()
	err := fn(ctx2)
	if err != nil && errors.Is(err, context.DeadlineExceeded) && on_timeout != nil {
		return on_timeout()
	}
	return err
}

func (b *EventBus) processEvent(ctx context.Context, event *BaseEvent, bypass_event_locks bool, pre_acquired_lock *AsyncLock) error {
	var event_lock *AsyncLock
	if !bypass_event_locks {
		event_lock = b.locks.getLockForEvent(event)
	}
	if pre_acquired_lock != nil {
		event_lock = pre_acquired_lock
	}
	if event_lock != nil && pre_acquired_lock == nil {
		if err := event_lock.Acquire(ctx); err != nil {
			return err
		}
		defer event_lock.Release()
	}
	defer func() { b.mu.Lock(); delete(b.inFlightEventIDs, event.EventID); b.mu.Unlock() }()
	event.markStarted()
	handlers := b.getHandlersForEvent(event)
	pending_entries := make([]*EventResult, 0, len(handlers))
	for _, h := range handlers {
		result := event.EventResults[h.ID]
		if result == nil {
			result = NewEventResult(event, h)
			event.EventResults[h.ID] = result
		}
		pending_entries = append(pending_entries, result)
	}
	resolved_event_timeout := event.EventTimeout
	if resolved_event_timeout == nil {
		resolved_event_timeout = b.EventTimeout
	}
	resolved_event_slow_timeout := event.EventSlowTimeout
	if resolved_event_slow_timeout == nil {
		resolved_event_slow_timeout = b.EventSlowTimeout
	}
	var slow_timer *time.Timer
	if resolved_event_slow_timeout != nil {
		slow_timer = time.AfterFunc(time.Duration(*resolved_event_slow_timeout*float64(time.Second)), func() {
			if event.EventStatus != "completed" {
				SlowWarningLogger(fmt.Sprintf("[bubus] Slow event processing: %s.on(%s) still running", b.Name, event.EventType))
			}
		})
	}
	err := runWithTimeout(ctx, resolved_event_timeout, func() error {
		if resolved_event_timeout == nil {
			return &EventTimeoutError{Message: fmt.Sprintf("%s.on(%s) timed out", b.Name, event.EventType), TimeoutSeconds: 0}
		}
		return &EventTimeoutError{Message: fmt.Sprintf("%s.on(%s) timed out after %.3fs", b.Name, event.EventType, *resolved_event_timeout), TimeoutSeconds: *resolved_event_timeout}
	}, func(ctx2 context.Context) error {
		return event.runHandlers(ctx2, b, handlers, pending_entries)
	})
	if slow_timer != nil {
		slow_timer.Stop()
	}
	if err != nil {
		for _, r := range pending_entries {
			if _, is_timeout := err.(*EventTimeoutError); is_timeout {
				if r.Status == EventResultCompleted {
					continue
				}
				if r.StartedAt != nil {
					r.replaceError((&EventHandlerAbortedError{Message: "Aborted running handler due to event timeout"}).Error())
				} else {
					r.replaceError((&EventHandlerCancelledError{Message: "Cancelled pending handler due to event timeout"}).Error())
				}
				continue
			}
			if r.Status == EventResultCompleted || r.Status == EventResultError {
				continue
			}
			r.markError(err)
		}
	}
	event.EventPendingBusCount--
	if event.EventPendingBusCount < 0 {
		event.EventPendingBusCount = 0
	}
	event.markCompleted()
	return nil
}

func (e *BaseEvent) runHandlers(ctx context.Context, bus *EventBus, handlers []*EventHandler, results []*EventResult) error {
	if len(handlers) == 0 {
		return nil
	}
	completion := e.EventHandlerCompletion
	if completion == "" {
		completion = bus.EventHandlerCompletion
	}
	concurrency := e.EventHandlerConcurrency
	if concurrency == "" {
		concurrency = bus.EventHandlerConcurrency
	}
	if completion == EventHandlerCompletionFirst && concurrency == EventHandlerConcurrencySerial {
		for i, h := range handlers {
			if err := runSingleHandler(ctx, bus, e, h, results[i]); err != nil {
				return err
			}
			if results[i].Status == EventResultCompleted && results[i].Result != nil {
				return nil
			}
		}
		return nil
	}
	if concurrency == EventHandlerConcurrencySerial {
		for i, h := range handlers {
			if err := runSingleHandler(ctx, bus, e, h, results[i]); err != nil {
				return err
			}
		}
		return nil
	}
	run_ctx := ctx
	var cancel context.CancelFunc
	if completion == EventHandlerCompletionFirst {
		run_ctx, cancel = context.WithCancel(ctx)
		defer cancel()
	}
	err_ch := make(chan error, len(handlers))
	wg := sync.WaitGroup{}
	for i, h := range handlers {
		wg.Add(1)
		go func(h *EventHandler, r *EventResult) {
			defer wg.Done()
			if err := runSingleHandler(run_ctx, bus, e, h, r); err != nil {
				err_ch <- err
			}
		}(h, results[i])
	}
	if completion == EventHandlerCompletionFirst {
		first_done := make(chan struct{})
		go func() { wg.Wait(); close(first_done) }()
		for {
			for _, r := range results {
				if r.Status == EventResultCompleted && r.Result != nil {
					if cancel != nil {
						cancel()
					}
					return nil
				}
			}
			select {
			case err := <-err_ch:
				// in first mode, continue waiting unless all done and no success
				_ = err
			case <-first_done:
				for _, r := range results {
					if r.Status == EventResultCompleted && r.Result != nil {
						return nil
					}
				}
				for _, r := range results {
					if r.Status == EventResultError {
						return errors.New(toErrorString(r.Error))
					}
				}
				return nil
			case <-time.After(time.Millisecond):
			}
		}
	}
	wg.Wait()
	select {
	case err := <-err_ch:
		return err
	default:
	}
	return nil
}

func runSingleHandler(ctx context.Context, bus *EventBus, event *BaseEvent, handler *EventHandler, result *EventResult) error {
	result.markStarted()
	ctx2 := ctx
	resolved_event_timeout := event.EventTimeout
	if resolved_event_timeout == nil {
		resolved_event_timeout = bus.EventTimeout
	}
	resolved_handler_timeout := handler.HandlerTimeout
	if resolved_handler_timeout == nil {
		resolved_handler_timeout = event.EventHandlerTimeout
	}
	if resolved_handler_timeout == nil {
		resolved_handler_timeout = bus.EventTimeout
	}
	if resolved_handler_timeout != nil && resolved_event_timeout != nil && *resolved_handler_timeout > *resolved_event_timeout {
		resolved_handler_timeout = resolved_event_timeout
	}
	resolved_handler_slow_timeout := handler.HandlerSlowTimeout
	if resolved_handler_slow_timeout == nil {
		resolved_handler_slow_timeout = event.EventHandlerSlowTimeout
	}
	if resolved_handler_slow_timeout == nil {
		resolved_handler_slow_timeout = event.EventSlowTimeout
	}
	if resolved_handler_slow_timeout == nil {
		resolved_handler_slow_timeout = bus.EventHandlerSlowTimeout
	}
	if resolved_handler_slow_timeout == nil {
		resolved_handler_slow_timeout = bus.EventSlowTimeout
	}
	var handler_slow_timer *time.Timer
	if resolved_handler_slow_timeout != nil && (resolved_handler_timeout == nil || *resolved_handler_timeout > *resolved_handler_slow_timeout) {
		handler_slow_timer = time.AfterFunc(time.Duration(*resolved_handler_slow_timeout*float64(time.Second)), func() {
			if result.Status == EventResultStarted {
				SlowWarningLogger(fmt.Sprintf("[bubus] Slow event handler: %s.on(%s, %s) still running", bus.Name, event.EventType, handler.HandlerName))
			}
		})
	}
	var return_value any
	err := runWithTimeout(ctx2, resolved_handler_timeout, func() error {
		if resolved_handler_timeout == nil {
			return &EventHandlerTimeoutError{Message: fmt.Sprintf("%s.on(%s, %s) timed out", bus.Name, event.EventType, handler.HandlerName), TimeoutSeconds: 0}
		}
		return &EventHandlerTimeoutError{Message: fmt.Sprintf("%s.on(%s, %s) timed out after %.3fs", bus.Name, event.EventType, handler.HandlerName, *resolved_handler_timeout), TimeoutSeconds: *resolved_handler_timeout}
	}, func(ctx3 context.Context) error {
		var err error
		if run_err := bus.locks.runWithHandlerDispatchContext(result, func() error {
			return_value, err = handler.Handle(ctx3, event)
			return err
		}); run_err != nil {
			return run_err
		}
		return err
	})
	if handler_slow_timer != nil {
		handler_slow_timer.Stop()
	}
	if err != nil {
		result.markError(err)
		return err
	}
	result.markCompleted(return_value)
	return nil
}

func (b *EventBus) processEventImmediately(ctx context.Context, event *BaseEvent, handler_result *EventResult) (*BaseEvent, error) {
	original_event := event
	if original_event.EventStatus == "completed" {
		return original_event, nil
	}
	b.mu.Lock()
	for i, queued := range b.pendingEventQueue {
		if queued == original_event {
			b.pendingEventQueue = append(b.pendingEventQueue[:i], b.pendingEventQueue[i+1:]...)
			break
		}
	}
	if b.inFlightEventIDs[original_event.EventID] {
		b.mu.Unlock()
		return original_event, nil
	}
	b.inFlightEventIDs[original_event.EventID] = true
	b.mu.Unlock()
	bypass_event_locks := b.locks.getActiveHandlerResult() != nil
	if err := b.processEvent(ctx, original_event, bypass_event_locks, nil); err != nil {
		return nil, err
	}
	return original_event, nil
}

func (b *EventBus) startRunloop() {
	b.mu.Lock()
	if b.runloopRunning {
		b.mu.Unlock()
		return
	}
	b.runloopRunning = true
	b.mu.Unlock()
	go b.runloop(context.Background())
}

func (b *EventBus) runloop(ctx context.Context) {
	for {
		if b.locks.isPaused() {
			_ = b.locks.waitUntilRunloopResumed(ctx)
		}
		b.mu.Lock()
		if len(b.pendingEventQueue) == 0 {
			b.runloopRunning = false
			b.mu.Unlock()
			return
		}
		next_event := b.pendingEventQueue[0]
		b.pendingEventQueue = b.pendingEventQueue[1:]
		if b.inFlightEventIDs[next_event.EventID] {
			b.mu.Unlock()
			continue
		}
		b.inFlightEventIDs[next_event.EventID] = true
		b.mu.Unlock()
		if err := b.processEvent(ctx, next_event, false, nil); err != nil && !errors.Is(err, context.Canceled) {
			// no-op log hook
		}
	}
}

func (b *EventBus) WaitUntilIdle(timeout *float64) bool { return b.locks.waitForIdle(timeout) }

func (b *EventBus) IsIdle() bool {
	for _, event := range b.EventHistory.Values() {
		for _, result := range event.EventResults {
			if result.EventBusID != b.ID {
				continue
			}
			if result.Status == EventResultPending || result.Status == EventResultStarted {
				return false
			}
		}
	}
	return true
}

func (b *EventBus) IsIdleAndQueueEmpty() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pendingEventQueue) == 0 && len(b.inFlightEventIDs) == 0 && b.IsIdle() && !b.runloopRunning
}

func (b *EventBus) ToJSON() ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	handlers := map[string]*EventHandler{}
	for id, handler := range b.handlers {
		handlers[id] = handler
	}
	handlers_by_key := map[string][]string{}
	for key, ids := range b.handlersByKey {
		handlers_by_key[key] = append([]string{}, ids...)
	}
	event_history := map[string]*BaseEvent{}
	for _, event := range b.EventHistory.Values() {
		event_history[event.EventID] = event
	}
	pending := make([]string, 0, len(b.pendingEventQueue))
	for _, event := range b.pendingEventQueue {
		pending = append(pending, event.EventID)
	}
	payload := EventBusJSON{
		ID: b.ID, Name: b.Name,
		MaxHistorySize:              b.EventHistory.MaxHistorySize,
		MaxHistoryDrop:              b.EventHistory.MaxHistoryDrop,
		EventConcurrency:            b.EventConcurrency,
		EventTimeout:                b.EventTimeout,
		EventSlowTimeout:            b.EventSlowTimeout,
		EventHandlerConcurrency:     b.EventHandlerConcurrency,
		EventHandlerCompletion:      b.EventHandlerCompletion,
		EventHandlerSlowTimeout:     b.EventHandlerSlowTimeout,
		EventHandlerDetectFilePaths: true,
		Handlers:                    handlers,
		HandlersByKey:               handlers_by_key,
		EventHistory:                event_history,
		PendingEventQueue:           pending,
	}
	return json.Marshal(payload)
}

func EventBusFromJSON(data []byte) (*EventBus, error) {
	var parsed EventBusJSON
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, err
	}
	bus := NewEventBus(parsed.Name, &EventBusOptions{
		ID:                          parsed.ID,
		MaxHistorySize:              parsed.MaxHistorySize,
		MaxHistoryDrop:              parsed.MaxHistoryDrop,
		EventConcurrency:            parsed.EventConcurrency,
		EventTimeout:                parsed.EventTimeout,
		EventSlowTimeout:            parsed.EventSlowTimeout,
		EventHandlerConcurrency:     parsed.EventHandlerConcurrency,
		EventHandlerCompletion:      parsed.EventHandlerCompletion,
		EventHandlerSlowTimeout:     parsed.EventHandlerSlowTimeout,
		EventHandlerDetectFilePaths: parsed.EventHandlerDetectFilePaths,
	})
	bus.handlers = parsed.Handlers
	bus.handlersByKey = parsed.HandlersByKey
	bus.EventHistory = NewEventHistory(parsed.MaxHistorySize, parsed.MaxHistoryDrop)
	for _, event := range parsed.EventHistory {
		event.Bus = bus
		for _, result := range event.EventResults {
			result.Event = event
			if handler := bus.handlers[result.HandlerID]; handler != nil {
				result.Handler = handler
			}
		}
		bus.EventHistory.AddEvent(event)
	}
	bus.pendingEventQueue = []*BaseEvent{}
	for _, event_id := range parsed.PendingEventQueue {
		if event := bus.EventHistory.GetEvent(event_id); event != nil {
			bus.pendingEventQueue = append(bus.pendingEventQueue, event)
		}
	}
	return bus, nil
}

func (b *EventBus) resolveFindWaiters(event *BaseEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	remaining := make([]*findWaiter, 0, len(b.findWaiters))
	for _, waiter := range b.findWaiters {
		if waiter.EventPattern != "*" && waiter.EventPattern != event.EventType {
			remaining = append(remaining, waiter)
			continue
		}
		if waiter.Matches != nil && !waiter.Matches(event) {
			remaining = append(remaining, waiter)
			continue
		}
		waiter.Resolve(event)
	}
	b.findWaiters = remaining
}

func (b *EventBus) eventIsChildOf(event *BaseEvent, ancestor *BaseEvent) bool {
	current := event.EventParentID
	visited := map[string]bool{}
	for current != nil {
		if *current == ancestor.EventID {
			return true
		}
		if visited[*current] {
			return false
		}
		visited[*current] = true
		parent := b.EventHistory.GetEvent(*current)
		if parent == nil {
			return false
		}
		current = parent.EventParentID
	}
	return false
}

func (b *EventBus) EventIsChildOf(event *BaseEvent, ancestor *BaseEvent) bool {
	return b.eventIsChildOf(event, ancestor)
}

func (b *EventBus) EventIsParentOf(parent *BaseEvent, child *BaseEvent) bool {
	return b.eventIsChildOf(child, parent)
}

func normalizePast(past any) (enabled bool, window *float64) {
	if past == nil {
		return true, nil
	}
	switch v := past.(type) {
	case bool:
		return v, nil
	case float64:
		if v < 0 {
			v = 0
		}
		return true, &v
	case int:
		f := float64(v)
		if f < 0 {
			f = 0
		}
		return true, &f
	default:
		return true, nil
	}
}

func normalizeFuture(future any) (enabled bool, timeout *float64) {
	if future == nil {
		return false, nil
	}
	switch v := future.(type) {
	case bool:
		return v, nil
	case float64:
		if v < 0 {
			v = 0
		}
		return true, &v
	case int:
		f := float64(v)
		if f < 0 {
			f = 0
		}
		return true, &f
	default:
		return false, nil
	}
}

func (b *EventBus) eventMatchesEquals(event *BaseEvent, equals map[string]any) bool {
	if len(equals) == 0 {
		return true
	}
	for key, value := range equals {
		switch key {
		case "event_status":
			if event.EventStatus != value {
				return false
			}
		case "event_type":
			if event.EventType != value {
				return false
			}
		default:
			payload_v, ok := event.Payload[key]
			if !ok || payload_v != value {
				return false
			}
		}
	}
	return true
}

func (b *EventBus) Find(event_pattern string, where func(event *BaseEvent) bool, options *FindOptions) (*BaseEvent, error) {
	if options == nil {
		options = &FindOptions{}
	}
	future_enabled, future_timeout := normalizeFuture(options.Future)
	historyMatch := b.EventHistory.Find(event_pattern, where, &EventHistoryFindOptions{Past: options.Past, ChildOf: options.ChildOf, Equals: options.Equals})
	if historyMatch != nil {
		return historyMatch, nil
	}
	if !future_enabled {
		return nil, nil
	}
	if event_pattern == "" {
		event_pattern = "*"
	}
	if where == nil {
		where = func(event *BaseEvent) bool { return true }
	}
	matches := func(event *BaseEvent) bool {
		if event_pattern != "*" && event.EventType != event_pattern {
			return false
		}
		if options.ChildOf != nil && !b.eventIsChildOf(event, options.ChildOf) {
			return false
		}
		if !b.eventMatchesEquals(event, options.Equals) {
			return false
		}
		return where(event)
	}
	resolved := make(chan *BaseEvent, 1)
	waiter := &findWaiter{EventPattern: event_pattern, Matches: matches, Resolve: func(event *BaseEvent) { resolved <- event }}
	b.mu.Lock()
	b.findWaiters = append(b.findWaiters, waiter)
	b.mu.Unlock()
	cleanup := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		for i := len(b.findWaiters) - 1; i >= 0; i-- {
			if b.findWaiters[i] == waiter {
				b.findWaiters = append(b.findWaiters[:i], b.findWaiters[i+1:]...)
				break
			}
		}
	}
	if future_timeout == nil {
		event := <-resolved
		cleanup()
		return event, nil
	}
	select {
	case event := <-resolved:
		cleanup()
		return event, nil
	case <-time.After(time.Duration(*future_timeout * float64(time.Second))):
		cleanup()
		return nil, nil
	}
}

func (b *EventBus) LogTree() string {
	b.mu.Lock()
	history := b.EventHistory.Values()
	b.mu.Unlock()
	lines := []string{}
	for _, event := range history {
		if event.EventParentID != nil {
			continue
		}
		lines = append(lines, b.logEventTree(event, "", true)...)
	}
	if len(lines) == 0 {
		return ""
	}
	return strings.Join(lines, "\n")
}

func (b *EventBus) logEventTree(event *BaseEvent, prefix string, isLast bool) []string {
	connector := "├──"
	nextPrefix := prefix + "│   "
	if isLast {
		connector = "└──"
		nextPrefix = prefix + "    "
	}
	dur := ""
	if event.EventStartedAt != nil && event.EventCompletedAt != nil {
		started_at, _ := time.Parse(time.RFC3339Nano, *event.EventStartedAt)
		completed_at, _ := time.Parse(time.RFC3339Nano, *event.EventCompletedAt)
		dur = fmt.Sprintf(" [%.3fs]", started_at.Sub(completed_at).Seconds())
	}
	line := fmt.Sprintf("%s%s %s#%s%s", prefix, connector, event.EventType, event.EventID[len(event.EventID)-4:], dur)
	out := []string{line}
	for _, r := range event.EventResults {
		sym := "✅"
		if r.Status == EventResultError {
			sym = "❌"
		}
		rline := fmt.Sprintf("%s%s %s %s.%s#%s", nextPrefix, connector, sym, b.Label(), r.HandlerName, r.HandlerID[len(r.HandlerID)-4:])
		if r.Result != nil {
			rline += fmt.Sprintf(" => %v", r.Result)
		}
		if r.Error != nil {
			rline += fmt.Sprintf(" err=%v", r.Error)
		}
		out = append(out, rline)
		for i, c := range r.EventChildren {
			out = append(out, b.logEventTree(c, nextPrefix, i == len(r.EventChildren)-1)...)
		}
	}
	return out
}

func (b *EventBus) Destroy() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers = map[string]*EventHandler{}
	b.handlersByKey = map[string][]string{}
	b.EventHistory = NewEventHistory(b.EventHistory.MaxHistorySize, b.EventHistory.MaxHistoryDrop)
	b.pendingEventQueue = []*BaseEvent{}
	b.inFlightEventIDs = map[string]bool{}
	b.findWaiters = []*findWaiter{}
	b.runloopRunning = false
}
