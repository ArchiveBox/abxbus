package abxbus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

var SlowWarningLogger = func(message string) {
	fmt.Println(message)
}

var eventBusRegistry = struct {
	sync.Mutex
	instances map[*EventBus]struct{}
}{instances: map[*EventBus]struct{}{}}

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
	EventHandlerDetectFilePaths *bool
	Middlewares                 []EventBusMiddleware
}

type FindOptions struct {
	Past    any
	Future  any
	ChildOf *BaseEvent
	Equals  map[string]any
}

type FilterOptions struct {
	Past    any
	Future  any
	ChildOf *BaseEvent
	Equals  map[string]any
	Limit   *int
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

type jsonObjectEntry struct {
	key   string
	value any
}

func marshalOrderedObject(entries []jsonObjectEntry) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, entry := range entries {
		if i > 0 {
			buf.WriteByte(',')
		}
		key, err := json.Marshal(entry.key)
		if err != nil {
			return nil, err
		}
		value, err := json.Marshal(entry.value)
		if err != nil {
			return nil, err
		}
		buf.Write(key)
		buf.WriteByte(':')
		buf.Write(value)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

type EventBus struct {
	ID   string
	Name string

	EventTimeout            *float64
	EventConcurrency        EventConcurrencyMode
	EventHandlerConcurrency EventHandlerConcurrencyMode
	EventHandlerCompletion  EventHandlerCompletionMode

	EventHandlerSlowTimeout     *float64
	EventSlowTimeout            *float64
	EventHandlerDetectFilePaths bool

	handlers          map[string]*EventHandler
	handlersByKey     map[string][]string
	EventHistory      *EventHistory
	pendingEventQueue []*BaseEvent
	inFlightEventIDs  map[string]bool
	runloopRunning    bool
	locks             *LockManager
	findWaiters       []*findWaiter
	middlewares       []EventBusMiddleware

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
		max_history_size = ptr(DefaultMaxHistorySize)
	} else if *max_history_size < 0 {
		max_history_size = nil
	}
	event_timeout := options.EventTimeout
	if event_timeout == nil {
		event_timeout = ptr(60.0)
	} else if *event_timeout < 0 {
		panic("event_timeout must be >= 0 or nil")
	}
	event_slow_timeout := options.EventSlowTimeout
	if event_slow_timeout == nil {
		event_slow_timeout = ptr(300.0)
	} else if *event_slow_timeout < 0 {
		panic("event_slow_timeout must be >= 0 or nil")
	}
	event_handler_slow_timeout := options.EventHandlerSlowTimeout
	if event_handler_slow_timeout == nil {
		event_handler_slow_timeout = ptr(30.0)
	} else if *event_handler_slow_timeout < 0 {
		panic("event_handler_slow_timeout must be >= 0 or nil")
	}
	detect_handler_file_paths := true
	if options.EventHandlerDetectFilePaths != nil {
		detect_handler_file_paths = *options.EventHandlerDetectFilePaths
	}
	bus := &EventBus{
		ID: id, Name: name,
		EventTimeout:                event_timeout,
		EventConcurrency:            options.EventConcurrency,
		EventHandlerConcurrency:     options.EventHandlerConcurrency,
		EventHandlerCompletion:      options.EventHandlerCompletion,
		EventHandlerSlowTimeout:     event_handler_slow_timeout,
		EventSlowTimeout:            event_slow_timeout,
		EventHandlerDetectFilePaths: detect_handler_file_paths,
		handlers:                    map[string]*EventHandler{},
		handlersByKey:               map[string][]string{},
		EventHistory:                NewEventHistory(max_history_size, options.MaxHistoryDrop),
		pendingEventQueue:           []*BaseEvent{},
		inFlightEventIDs:            map[string]bool{},
		findWaiters:                 []*findWaiter{},
		middlewares:                 append([]EventBusMiddleware{}, options.Middlewares...),
		global_lock:                 shared_global_event_lock,
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
	eventBusRegistry.Lock()
	eventBusRegistry.instances[bus] = struct{}{}
	eventBusRegistry.Unlock()
	return bus
}

func suffix(value string, n int) string {
	if len(value) <= n {
		return value
	}
	return value[len(value)-n:]
}

func (b *EventBus) Label() string { return fmt.Sprintf("%s#%s", b.Name, suffix(b.ID, 4)) }

func eventBusInstancesSnapshot() []*EventBus {
	eventBusRegistry.Lock()
	defer eventBusRegistry.Unlock()
	instances := make([]*EventBus, 0, len(eventBusRegistry.instances))
	for bus := range eventBusRegistry.instances {
		instances = append(instances, bus)
	}
	return instances
}

func (b *EventBus) notifyEventChange(event *BaseEvent, status string) {
	b.locks.notifyIdleListeners()
	for _, middleware := range append([]EventBusMiddleware{}, b.middlewares...) {
		middleware.OnEventChange(b, event, status)
	}
}

func (b *EventBus) notifyEventResultChange(event *BaseEvent, result *EventResult, status string) {
	b.locks.notifyIdleListeners()
	for _, middleware := range append([]EventBusMiddleware{}, b.middlewares...) {
		middleware.OnEventResultChange(b, event, result, status)
	}
}

func (b *EventBus) notifyBusHandlersChange(handler *EventHandler, registered bool) {
	for _, middleware := range append([]EventBusMiddleware{}, b.middlewares...) {
		middleware.OnBusHandlersChange(b, handler, registered)
	}
}

func (b *EventBus) On(event_pattern string, handler_name string, handler EventHandlerCallable, options *EventHandler) *EventHandler {
	if event_pattern == "" {
		event_pattern = "*"
	}
	h := NewEventHandler(b.Name, b.ID, event_pattern, handler_name, handler)
	explicitID := false
	if options != nil {
		if options.ID != "" {
			h.ID = options.ID
			explicitID = true
		}
		if options.HandlerName != "" {
			h.HandlerName = options.HandlerName
		}
		if options.HandlerRegisteredAt != "" {
			h.HandlerRegisteredAt = options.HandlerRegisteredAt
		}
		if options.HandlerTimeout != nil && *options.HandlerTimeout < 0 {
			panic("handler_timeout must be >= 0 or nil")
		}
		if options.HandlerSlowTimeout != nil && *options.HandlerSlowTimeout < 0 {
			panic("handler_slow_timeout must be >= 0 or nil")
		}
		h.HandlerTimeout = options.HandlerTimeout
		h.HandlerSlowTimeout = options.HandlerSlowTimeout
		h.HandlerFilePath = options.HandlerFilePath
	}
	if !explicitID {
		h.ID = ComputeHandlerID(b.ID, h.HandlerName, h.HandlerFilePath, h.HandlerRegisteredAt, event_pattern)
	}
	b.mu.Lock()
	b.handlers[h.ID] = h
	ids := b.handlersByKey[event_pattern]
	for _, id := range ids {
		if id == h.ID {
			b.mu.Unlock()
			return h
		}
	}
	b.handlersByKey[event_pattern] = append(ids, h.ID)
	b.mu.Unlock()
	b.notifyBusHandlersChange(h, true)
	return h
}

func (b *EventBus) Off(event_pattern string, handler any) {
	b.mu.Lock()
	ids := b.handlersByKey[event_pattern]
	removed := []*EventHandler{}
	if handler == nil {
		for _, id := range ids {
			if h := b.handlers[id]; h != nil {
				removed = append(removed, h)
			}
			delete(b.handlers, id)
		}
		delete(b.handlersByKey, event_pattern)
		b.mu.Unlock()
		for _, h := range removed {
			b.notifyBusHandlersChange(h, false)
		}
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
			if h != nil {
				removed = append(removed, h)
			}
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
	b.mu.Unlock()
	for _, h := range removed {
		b.notifyBusHandlersChange(h, false)
	}
}

func (b *EventBus) Emit(input any) *BaseEvent {
	return b.EmitWithContext(nil, input)
}

func (b *EventBus) EmitWithContext(ctx context.Context, input any) *BaseEvent {
	event, err := baseEventFromAny(input)
	if err != nil {
		panic(err)
	}
	original_event := event
	original_event.mu.Lock()
	if event.Bus == nil {
		original_event.Bus = b
	}
	if original_event.dispatchCtx == nil {
		original_event.dispatchCtx = ctx
	}
	if original_event.dispatchCtx == nil {
		original_event.dispatchCtx = b.locks.getActiveDispatchContext()
		if original_event.dispatchCtx == nil {
			original_event.dispatchCtx = context.Background()
		}
	}
	for _, label := range original_event.EventPath {
		if label == b.Label() {
			original_event.mu.Unlock()
			return original_event
		}
	}
	original_event.EventPath = append(original_event.EventPath, b.Label())
	original_event.EventPendingBusCount++
	original_event.mu.Unlock()
	if b.EventHistory.MaxHistorySize != nil && *b.EventHistory.MaxHistorySize > 0 && !b.EventHistory.MaxHistoryDrop && b.EventHistory.Size() >= *b.EventHistory.MaxHistorySize {
		panic(fmt.Sprintf("%s.emit(%s) rejected: history limit reached (%d/%d); set event_history.max_history_drop=true to drop old history instead.", b.Label(), original_event.EventType, b.EventHistory.Size(), *b.EventHistory.MaxHistorySize))
	}
	b.mu.Lock()
	b.EventHistory.AddEvent(original_event)
	b.resolveFindWaitersLocked(original_event)
	b.pendingEventQueue = append(b.pendingEventQueue, original_event)
	b.mu.Unlock()
	b.notifyEventChange(original_event, "pending")
	activeHandler := b.locks.getActiveHandlerResult()
	if activeHandler == nil {
		activeHandler = b.locks.getAnyActiveHandlerResult()
	}
	if activeHandler == nil {
		b.startRunloop()
	} else if b.locks.getLockForEvent(original_event) == nil {
		b.startParallelEventTaskFromQueue(original_event)
	} else {
		go func(result *EventResult) {
			_ = result.waitWithContext(context.Background())
			b.startRunloop()
		}(activeHandler)
	}
	return original_event
}

func (b *EventBus) Dispatch(event *BaseEvent) *BaseEvent { return b.Emit(event) }

func (b *EventBus) startParallelEventTaskFromQueue(event *BaseEvent) {
	b.mu.Lock()
	if b.inFlightEventIDs[event.EventID] {
		b.mu.Unlock()
		return
	}
	removed := false
	for i, queued := range b.pendingEventQueue {
		if queued.EventID == event.EventID {
			b.pendingEventQueue = append(b.pendingEventQueue[:i], b.pendingEventQueue[i+1:]...)
			removed = true
			break
		}
	}
	if !removed && event.status() == "completed" {
		b.mu.Unlock()
		return
	}
	b.inFlightEventIDs[event.EventID] = true
	b.mu.Unlock()

	firstHandlerStarted := make(chan struct{})
	go func() {
		if err := b.processEvent(context.Background(), event, false, nil, firstHandlerStarted); err != nil && !errors.Is(err, context.Canceled) {
			// no-op log hook
		}
	}()
	<-firstHandlerStarted
}

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
	if timeout_seconds == nil || *timeout_seconds <= 0 {
		return fn(ctx)
	}
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()
	timer := time.NewTimer(time.Duration(*timeout_seconds * float64(time.Second)))
	defer timer.Stop()
	errCh := make(chan error, 1)
	go func() {
		errCh <- fn(ctx2)
	}()
	select {
	case err := <-errCh:
		if err != nil && errors.Is(err, context.DeadlineExceeded) && on_timeout != nil {
			return on_timeout()
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		cancel()
		if on_timeout != nil {
			return on_timeout()
		}
		return context.DeadlineExceeded
	}
}

type handlerContext struct {
	base   context.Context
	values context.Context
}

type handlerCancelReasonContextKey struct{}
type handlerExecutionContextKey struct{}

func activeHandlerResultFromContext(ctx context.Context) *EventResult {
	if ctx == nil {
		return nil
	}
	if result, ok := ctx.Value(handlerExecutionContextKey{}).(*EventResult); ok {
		return result
	}
	return nil
}

func (c handlerContext) Deadline() (time.Time, bool) { return c.base.Deadline() }
func (c handlerContext) Done() <-chan struct{}       { return c.base.Done() }
func (c handlerContext) Err() error                  { return c.base.Err() }
func (c handlerContext) Value(key any) any {
	if c.values != nil {
		if value := c.values.Value(key); value != nil {
			return value
		}
	}
	return c.base.Value(key)
}

func mergeHandlerContext(base context.Context, values context.Context) context.Context {
	if values == nil || values == base {
		return base
	}
	return handlerContext{base: base, values: values}
}

func (b *EventBus) eventHasLocalActiveResults(event *BaseEvent) bool {
	for _, result := range event.eventResultsSnapshot() {
		if result == nil || result.EventBusID != b.ID {
			continue
		}
		status, _, _, _ := result.snapshot()
		if status == EventResultPending || status == EventResultStarted {
			return true
		}
	}
	return false
}

func (b *EventBus) eventHasLocalSettledResults(event *BaseEvent) bool {
	for _, result := range event.eventResultsSnapshot() {
		if result == nil || result.EventBusID != b.ID {
			continue
		}
		status, _, _, _ := result.snapshot()
		if status == EventResultCompleted || status == EventResultError {
			return true
		}
	}
	return false
}

func eventHasRunningResults(event *BaseEvent) bool {
	for _, result := range event.eventResultsSnapshot() {
		if result == nil {
			continue
		}
		status, _, _, _ := result.snapshot()
		if status == EventResultPending || status == EventResultStarted {
			return true
		}
	}
	return false
}

func completeEventAcrossBuses(event *BaseEvent) {
	if event.status() == "completed" {
		return
	}
	event.mu.Lock()
	if event.EventPendingBusCount < 0 {
		event.EventPendingBusCount = 0
	}
	event.mu.Unlock()
	event.markCompleted()
	for _, bus := range eventBusInstancesSnapshot() {
		if bus.EventHistory.GetEvent(event.EventID) != event {
			continue
		}
		bus.notifyEventChange(event, "completed")
		bus.EventHistory.TrimEventHistory(nil)
	}
}

func completeIdleEventAcrossBuses(event *BaseEvent) bool {
	if event.status() == "completed" {
		return true
	}
	event.mu.Lock()
	pendingBusCount := event.EventPendingBusCount
	event.mu.Unlock()
	if pendingBusCount > 0 {
		return false
	}
	if eventHasRunningResults(event) || eventQueuedOrInFlightAcrossBuses(event) {
		return false
	}
	completeEventAcrossBuses(event)
	return true
}

func startRunloopsForEvent(event *BaseEvent) {
	for _, bus := range eventBusInstancesSnapshot() {
		if bus.EventHistory.GetEvent(event.EventID) == event {
			bus.startRunloop()
		}
	}
}

func eventQueuedOrInFlightAcrossBuses(event *BaseEvent) bool {
	for _, bus := range eventBusInstancesSnapshot() {
		if bus.EventHistory.GetEvent(event.EventID) != event {
			continue
		}
		bus.mu.Lock()
		if bus.inFlightEventIDs[event.EventID] {
			bus.mu.Unlock()
			return true
		}
		for _, queued := range bus.pendingEventQueue {
			if queued.EventID == event.EventID {
				bus.mu.Unlock()
				return true
			}
		}
		bus.mu.Unlock()
	}
	return false
}

func settleSkippedActiveBuses(event *BaseEvent, skipped int) {
	if skipped <= 0 || eventHasRunningResults(event) || eventQueuedOrInFlightAcrossBuses(event) {
		return
	}
	event.mu.Lock()
	event.EventPendingBusCount -= skipped
	if event.EventPendingBusCount < 0 {
		event.EventPendingBusCount = 0
	}
	eventDone := event.EventPendingBusCount == 0
	event.mu.Unlock()
	if eventDone {
		completeEventAcrossBuses(event)
	}
}

func (b *EventBus) processEvent(ctx context.Context, event *BaseEvent, bypass_event_locks bool, pre_acquired_lock *AsyncLock, first_handler_started chan struct{}) error {
	signalFirstHandlerStarted := func() {}
	if first_handler_started != nil {
		var signal_once sync.Once
		signalFirstHandlerStarted = func() {
			signal_once.Do(func() { close(first_handler_started) })
		}
		defer signalFirstHandlerStarted()
	}
	defer func() {
		b.mu.Lock()
		delete(b.inFlightEventIDs, event.EventID)
		b.mu.Unlock()
		b.locks.notifyIdleListeners()
	}()
	if event.status() == "completed" {
		signalFirstHandlerStarted()
		return nil
	}
	if b.eventHasLocalActiveResults(event) {
		signalFirstHandlerStarted()
		_, err := event.waitWithContext(ctx)
		return err
	}
	if b.eventHasLocalSettledResults(event) {
		signalFirstHandlerStarted()
		return nil
	}
	event.mu.Lock()
	if event.dispatchCtx == nil && ctx != nil {
		event.dispatchCtx = ctx
	}
	previousBus := event.Bus
	event.Bus = b
	shouldRestoreBus := previousBus != nil && previousBus != b
	event.mu.Unlock()
	if shouldRestoreBus {
		defer func() { event.mu.Lock(); event.Bus = previousBus; event.mu.Unlock() }()
	}
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
		if event.status() == "completed" {
			signalFirstHandlerStarted()
			return nil
		}
		if b.eventHasLocalActiveResults(event) {
			signalFirstHandlerStarted()
			_, err := event.waitWithContext(ctx)
			return err
		}
		if b.eventHasLocalSettledResults(event) {
			signalFirstHandlerStarted()
			return nil
		}
	}
	event.markStarted()
	b.notifyEventChange(event, "started")
	handlers := b.getHandlersForEvent(event)
	if len(handlers) == 0 {
		signalFirstHandlerStarted()
	}
	pending_entries := make([]*EventResult, 0, len(handlers))
	for _, h := range handlers {
		event.mu.Lock()
		if event.EventResults == nil {
			event.EventResults = map[string]*EventResult{}
		}
		result := event.EventResults[h.ID]
		if result == nil {
			result = NewEventResult(event, h)
			event.EventResults[h.ID] = result
		}
		event.mu.Unlock()
		event.noteEventResultOrder(h.ID)
		pending_entries = append(pending_entries, result)
		if result.Status == EventResultPending {
			b.notifyEventResultChange(event, result, "pending")
		}
	}
	if event.EventTimeout != nil && *event.EventTimeout < 0 {
		panic("event_timeout must be >= 0 or nil")
	}
	if event.EventSlowTimeout != nil && *event.EventSlowTimeout < 0 {
		panic("event_slow_timeout must be >= 0 or nil")
	}
	if event.EventHandlerTimeout != nil && *event.EventHandlerTimeout < 0 {
		panic("event_handler_timeout must be >= 0 or nil")
	}
	if event.EventHandlerSlowTimeout != nil && *event.EventHandlerSlowTimeout < 0 {
		panic("event_handler_slow_timeout must be >= 0 or nil")
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
	if resolved_event_slow_timeout != nil && *resolved_event_slow_timeout > 0 {
		slow_timer = time.AfterFunc(time.Duration(*resolved_event_slow_timeout*float64(time.Second)), func() {
			if event.status() != "completed" {
				SlowWarningLogger(fmt.Sprintf("[abxbus] Slow event processing: %s.on(%s) still running", b.Name, event.EventType))
			}
		})
	}
	err := runWithTimeout(ctx, resolved_event_timeout, func() error {
		if resolved_event_timeout == nil {
			return &EventTimeoutError{Message: fmt.Sprintf("%s.on(%s) timed out", b.Name, event.EventType), TimeoutSeconds: 0}
		}
		return &EventTimeoutError{Message: fmt.Sprintf("%s.on(%s) timed out after %.3fs", b.Name, event.EventType, *resolved_event_timeout), TimeoutSeconds: *resolved_event_timeout}
	}, func(ctx2 context.Context) error {
		return event.runHandlers(ctx2, b, handlers, pending_entries, signalFirstHandlerStarted)
	})
	if slow_timer != nil {
		slow_timer.Stop()
	}
	if err != nil {
		for _, r := range pending_entries {
			status, _, _, startedAt := r.snapshot()
			if _, is_timeout := err.(*EventTimeoutError); is_timeout {
				if status == EventResultCompleted {
					continue
				}
				if startedAt != nil {
					if !r.replaceError((&EventHandlerAbortedError{Message: "Aborted running handler due to event timeout"}).Error()) {
						continue
					}
				} else {
					if !r.replaceError((&EventHandlerCancelledError{Message: "Cancelled pending handler due to event timeout"}).Error()) {
						continue
					}
				}
				b.notifyEventResultChange(event, r, "completed")
				continue
			}
			if status == EventResultCompleted || status == EventResultError {
				continue
			}
			if r.markError(err) {
				b.notifyEventResultChange(event, r, "completed")
			}
		}
	}
	event.mu.Lock()
	event.EventPendingBusCount--
	if event.EventPendingBusCount < 0 {
		event.EventPendingBusCount = 0
	}
	eventDone := event.EventPendingBusCount == 0
	event.mu.Unlock()
	if eventDone {
		event.markCompleted()
		b.notifyEventChange(event, "completed")
		b.EventHistory.TrimEventHistory(nil)
	}
	b.startRunloop()
	return nil
}

func (e *BaseEvent) runHandlers(ctx context.Context, bus *EventBus, handlers []*EventHandler, results []*EventResult, signalFirstHandlerStarted func()) error {
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
			if err := runSingleHandler(ctx, bus, e, h, results[i], signalFirstHandlerStarted); err != nil {
				return err
			}
			status, result, _, _ := results[i].snapshot()
			if status == EventResultCompleted && result != nil && !isBaseEventResult(result) {
				for j := i + 1; j < len(results); j++ {
					nextStatus, _, _, _ := results[j].snapshot()
					if nextStatus == EventResultPending {
						if results[j].replaceError((&EventHandlerCancelledError{Message: "Cancelled pending handler due to first-completion mode"}).Error()) {
							bus.notifyEventResultChange(e, results[j], "completed")
						}
					}
				}
				return nil
			}
		}
		return nil
	}
	if concurrency == EventHandlerConcurrencySerial {
		for i, h := range handlers {
			if err := runSingleHandler(ctx, bus, e, h, results[i], signalFirstHandlerStarted); err != nil {
				return err
			}
		}
		return nil
	}
	run_ctx := ctx
	var cancel context.CancelFunc
	if completion == EventHandlerCompletionFirst {
		run_ctx, cancel = context.WithCancel(ctx)
		run_ctx = context.WithValue(run_ctx, handlerCancelReasonContextKey{}, "first-completion")
		defer cancel()
	}
	err_ch := make(chan error, len(handlers))
	wg := sync.WaitGroup{}
	for i, h := range handlers {
		wg.Add(1)
		go func(h *EventHandler, r *EventResult) {
			defer wg.Done()
			if err := runSingleHandler(run_ctx, bus, e, h, r, signalFirstHandlerStarted); err != nil {
				err_ch <- err
			}
		}(h, results[i])
	}
	if completion == EventHandlerCompletionFirst {
		all_done := make(chan struct{})
		go func() { wg.Wait(); close(all_done) }()
		completionSignal := make(chan struct{}, len(results))
		for _, result := range results {
			go func(result *EventResult) {
				<-result.done_ch
				select {
				case completionSignal <- struct{}{}:
				default:
				}
			}(result)
		}
		hasSuccessfulResult := func() bool {
			for _, result := range results {
				status, value, _, _ := result.snapshot()
				if status == EventResultCompleted && value != nil && !isBaseEventResult(value) {
					return true
				}
			}
			return false
		}
		firstResultError := func() error {
			for _, result := range results {
				status, _, errValue, _ := result.snapshot()
				if status == EventResultError {
					return errors.New(toErrorString(errValue))
				}
			}
			return nil
		}
		for {
			if hasSuccessfulResult() {
				if cancel != nil {
					cancel()
				}
				return nil
			}
			select {
			case <-completionSignal:
			case <-all_done:
				if hasSuccessfulResult() {
					return nil
				}
				if err := firstResultError(); err != nil {
					return err
				}
				return nil
			case <-run_ctx.Done():
				return run_ctx.Err()
			case <-err_ch:
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

func runSingleHandler(ctx context.Context, bus *EventBus, event *BaseEvent, handler *EventHandler, result *EventResult, signalFirstHandlerStarted func()) error {
	status, _, _, _ := result.snapshot()
	if status != EventResultPending {
		if signalFirstHandlerStarted != nil {
			signalFirstHandlerStarted()
		}
		return nil
	}
	result.markStarted()
	defer result.releaseQueueJumpPauses()
	bus.notifyEventResultChange(event, result, "started")
	if signalFirstHandlerStarted != nil {
		signalFirstHandlerStarted()
	}
	ctx2 := ctx
	if event.dispatchCtx != nil {
		ctx2 = mergeHandlerContext(ctx, event.dispatchCtx)
	}
	explicit_handler_timeout := handler.HandlerTimeout
	if explicit_handler_timeout == nil {
		explicit_handler_timeout = event.EventHandlerTimeout
	}
	if explicit_handler_timeout != nil && *explicit_handler_timeout <= 0 {
		explicit_handler_timeout = nil
	}
	resolved_event_timeout := event.EventTimeout
	if resolved_event_timeout == nil {
		resolved_event_timeout = bus.EventTimeout
	}
	if resolved_event_timeout != nil && *resolved_event_timeout <= 0 {
		resolved_event_timeout = nil
	}
	resolved_handler_timeout := explicit_handler_timeout
	handler_timeout_from_event_timeout := false
	if resolved_handler_timeout == nil {
		resolved_handler_timeout = resolved_event_timeout
		handler_timeout_from_event_timeout = resolved_handler_timeout != nil
	} else if resolved_event_timeout != nil && *resolved_event_timeout < *resolved_handler_timeout {
		resolved_handler_timeout = resolved_event_timeout
		handler_timeout_from_event_timeout = true
	}
	result.HandlerTimeout = resolved_handler_timeout
	resolved_handler_slow_timeout := handler.HandlerSlowTimeout
	if resolved_handler_slow_timeout == nil {
		resolved_handler_slow_timeout = event.EventHandlerSlowTimeout
	}
	if resolved_handler_slow_timeout == nil {
		resolved_handler_slow_timeout = bus.EventHandlerSlowTimeout
	}
	var handler_slow_timer *time.Timer
	if resolved_handler_slow_timeout != nil && *resolved_handler_slow_timeout > 0 && (resolved_handler_timeout == nil || *resolved_handler_timeout > *resolved_handler_slow_timeout) {
		handler_slow_timer = time.AfterFunc(time.Duration(*resolved_handler_slow_timeout*float64(time.Second)), func() {
			status, _, _, _ := result.snapshot()
			if status == EventResultStarted {
				SlowWarningLogger(fmt.Sprintf("[abxbus] Slow event handler: %s.on(%s, %s) still running", bus.Name, event.EventType, handler.HandlerName))
			}
		})
	}
	var return_value any
	err := runWithTimeout(ctx2, resolved_handler_timeout, func() error {
		if handler_timeout_from_event_timeout {
			return &EventHandlerAbortedError{Message: "Aborted running handler due to event timeout"}
		}
		if resolved_handler_timeout == nil {
			return &EventHandlerTimeoutError{Message: fmt.Sprintf("%s.on(%s, %s) timed out", bus.Name, event.EventType, handler.HandlerName), TimeoutSeconds: 0}
		}
		return &EventHandlerTimeoutError{Message: fmt.Sprintf("%s.on(%s, %s) timed out after %.3fs", bus.Name, event.EventType, handler.HandlerName, *resolved_handler_timeout), TimeoutSeconds: *resolved_handler_timeout}
	}, func(ctx3 context.Context) error {
		var err error
		ctx3 = context.WithValue(ctx3, handlerExecutionContextKey{}, result)
		if run_err := bus.locks.runWithHandlerDispatchContext(result, ctx3, func() error {
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
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ctx.Err()
		}
		var eventTimeoutAbort *EventHandlerAbortedError
		if handler_timeout_from_event_timeout && errors.As(err, &eventTimeoutAbort) {
			if result.markError(&EventHandlerAbortedError{Message: "Aborted running handler due to event timeout"}) {
				bus.notifyEventResultChange(event, result, "completed")
			}
			timeoutSeconds := 0.0
			if resolved_handler_timeout != nil {
				timeoutSeconds = *resolved_handler_timeout
			}
			return &EventTimeoutError{Message: fmt.Sprintf("%s.on(%s) timed out after %.3fs", bus.Name, event.EventType, timeoutSeconds), TimeoutSeconds: timeoutSeconds}
		}
		if errors.Is(ctx.Err(), context.Canceled) && errors.Is(err, context.Canceled) {
			if ctx.Value(handlerCancelReasonContextKey{}) == "first-completion" {
				if result.markError(&EventHandlerAbortedError{Message: "Aborted running handler due to first-completion mode"}) {
					bus.notifyEventResultChange(event, result, "completed")
				}
				return nil
			}
			return ctx.Err()
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			return ctx.Err()
		}
		if result.markError(err) {
			bus.notifyEventResultChange(event, result, "completed")
		}
		return nil
	}
	if err := event.validateResultValue(return_value); err != nil {
		if result.markError(err) {
			bus.notifyEventResultChange(event, result, "completed")
		}
		return nil
	}
	if result.markCompleted(return_value) {
		bus.notifyEventResultChange(event, result, "completed")
	}
	return nil
}

func (b *EventBus) processEventImmediately(ctx context.Context, event *BaseEvent, handler_result *EventResult) (*BaseEvent, error) {
	original_event := event
	if original_event.status() == "completed" {
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
		if _, err := original_event.waitWithContext(ctx); err != nil {
			return nil, err
		}
		return original_event, nil
	}
	b.inFlightEventIDs[original_event.EventID] = true
	b.mu.Unlock()
	bypass_event_locks := b.locks.hasAnyActiveHandlerResult()
	if err := b.processEvent(ctx, original_event, bypass_event_locks, nil, nil); err != nil {
		return nil, err
	}
	if original_event.status() != "completed" {
		if _, err := original_event.waitWithContext(ctx); err != nil {
			return nil, err
		}
	}
	return original_event, nil
}

func (b *EventBus) processEventImmediatelyAcrossBuses(ctx context.Context, event *BaseEvent) (*BaseEvent, error) {
	originalEvent := event
	if originalEvent.status() == "completed" {
		return originalEvent, nil
	}

	for {
		instances := eventBusInstancesSnapshot()
		ordered := []*EventBus{}
		seen := map[*EventBus]bool{}
		skippedActiveBuses := 0
		originalEvent.mu.Lock()
		eventPath := append([]string{}, originalEvent.EventPath...)
		originalEvent.mu.Unlock()
		for _, label := range eventPath {
			for _, bus := range instances {
				if seen[bus] || bus.Label() != label || bus.EventHistory.GetEvent(originalEvent.EventID) != originalEvent {
					continue
				}
				if bus.eventHasLocalActiveResults(originalEvent) {
					skippedActiveBuses++
					seen[bus] = true
					continue
				}
				if bus.eventHasLocalSettledResults(originalEvent) {
					seen[bus] = true
					continue
				}
				ordered = append(ordered, bus)
				seen[bus] = true
			}
		}
		if !seen[b] && b.EventHistory.GetEvent(originalEvent.EventID) == originalEvent && !b.eventHasLocalActiveResults(originalEvent) && !b.eventHasLocalSettledResults(originalEvent) {
			ordered = append(ordered, b)
			seen[b] = true
		}
		if len(ordered) == 0 {
			settleSkippedActiveBuses(originalEvent, skippedActiveBuses)
			if _, err := originalEvent.waitWithContext(ctx); err != nil {
				return nil, err
			}
			return originalEvent, nil
		}

		initiatingLock := b.locks.getLockForEvent(originalEvent)
		activeHandlerResult := b.locks.getActiveHandlerResult()
		if activeHandlerResult == nil {
			activeHandlerResult = b.locks.getAnyActiveHandlerResult()
		}
		releases := []func(){}
		for _, bus := range ordered {
			if bus != b {
				releases = append(releases, bus.locks.requestRunloopPause())
			}
		}

		sawAlreadyInFlight := false
		for _, bus := range ordered {
			if bus.eventHasLocalSettledResults(originalEvent) {
				continue
			}
			busEventLock := bus.locks.getLockForEvent(originalEvent)
			bypassEventLocks := activeHandlerResult != nil && (bus == b || (initiatingLock != nil && busEventLock == initiatingLock))
			bus.mu.Lock()
			for i := len(bus.pendingEventQueue) - 1; i >= 0; i-- {
				if bus.pendingEventQueue[i].EventID == originalEvent.EventID {
					bus.pendingEventQueue = append(bus.pendingEventQueue[:i], bus.pendingEventQueue[i+1:]...)
				}
			}
			alreadyInFlight := bus.inFlightEventIDs[originalEvent.EventID]
			if !alreadyInFlight {
				bus.inFlightEventIDs[originalEvent.EventID] = true
			}
			bus.mu.Unlock()
			if alreadyInFlight {
				sawAlreadyInFlight = true
				continue
			}
			if bus.eventHasLocalSettledResults(originalEvent) {
				if !alreadyInFlight {
					bus.mu.Lock()
					delete(bus.inFlightEventIDs, originalEvent.EventID)
					bus.mu.Unlock()
				}
				continue
			}

			if err := bus.processEvent(ctx, originalEvent, bypassEventLocks, nil, nil); err != nil {
				for _, release := range releases {
					release()
				}
				return nil, err
			}
		}
		for _, release := range releases {
			release()
		}
		if originalEvent.status() == "completed" {
			return originalEvent, nil
		}
		if completeIdleEventAcrossBuses(originalEvent) {
			return originalEvent, nil
		}
		if sawAlreadyInFlight {
			if _, err := originalEvent.waitWithContext(ctx); err != nil {
				return nil, err
			}
			return originalEvent, nil
		}
	}
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
			b.locks.notifyIdleListeners()
			return
		}
		next_event := b.pendingEventQueue[0]
		b.pendingEventQueue = b.pendingEventQueue[1:]
		if b.locks.isPaused() {
			b.pendingEventQueue = append([]*BaseEvent{next_event}, b.pendingEventQueue...)
			b.mu.Unlock()
			continue
		}
		if b.inFlightEventIDs[next_event.EventID] {
			b.mu.Unlock()
			continue
		}
		b.inFlightEventIDs[next_event.EventID] = true
		b.mu.Unlock()
		process := func(event *BaseEvent, first_handler_started chan struct{}) {
			if err := b.processEvent(ctx, event, false, nil, first_handler_started); err != nil && !errors.Is(err, context.Canceled) {
				// no-op log hook
			}
		}
		nextEventConcurrency := next_event.EventConcurrency
		if nextEventConcurrency == "" {
			nextEventConcurrency = b.EventConcurrency
		}
		if nextEventConcurrency == EventConcurrencyParallel {
			first_handler_started := make(chan struct{})
			go process(next_event, first_handler_started)
			select {
			case <-first_handler_started:
			case <-ctx.Done():
				return
			}
			continue
		}
		process(next_event, nil)
	}
}

func (b *EventBus) WaitUntilIdle(timeout *float64) bool { return b.locks.waitForIdle(timeout) }

func (b *EventBus) IsIdle() bool {
	for _, event := range b.EventHistory.Values() {
		for _, result := range event.eventResultsSnapshot() {
			if result.EventBusID != b.ID {
				continue
			}
			status, _, _, _ := result.snapshot()
			if status == EventResultPending || status == EventResultStarted {
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

	patternEntries := make([]struct {
		key          string
		ids          []string
		registeredAt string
	}, 0, len(b.handlersByKey))
	for key, ids := range b.handlersByKey {
		firstRegisteredAt := ""
		for _, id := range ids {
			if handler := b.handlers[id]; handler != nil {
				if firstRegisteredAt == "" || handler.HandlerRegisteredAt < firstRegisteredAt {
					firstRegisteredAt = handler.HandlerRegisteredAt
				}
			}
		}
		patternEntries = append(patternEntries, struct {
			key          string
			ids          []string
			registeredAt string
		}{key: key, ids: append([]string{}, ids...), registeredAt: firstRegisteredAt})
	}
	sort.SliceStable(patternEntries, func(i, j int) bool {
		if patternEntries[i].registeredAt == patternEntries[j].registeredAt {
			return patternEntries[i].key < patternEntries[j].key
		}
		return patternEntries[i].registeredAt < patternEntries[j].registeredAt
	})
	handlersByKeyJSONEntries := make([]jsonObjectEntry, 0, len(patternEntries))
	for _, entry := range patternEntries {
		handlersByKeyJSONEntries = append(handlersByKeyJSONEntries, jsonObjectEntry{key: entry.key, value: entry.ids})
	}
	handlersByKeyJSON, err := marshalOrderedObject(handlersByKeyJSONEntries)
	if err != nil {
		return nil, err
	}

	seenHandlers := map[string]bool{}
	handlerJSONEntries := make([]jsonObjectEntry, 0, len(b.handlers))
	for _, patternEntry := range patternEntries {
		for _, id := range patternEntry.ids {
			handler := b.handlers[id]
			if handler == nil || seenHandlers[id] {
				continue
			}
			if handler.ID == "" {
				handler.ID = id
			}
			handlerJSONEntries = append(handlerJSONEntries, jsonObjectEntry{key: handler.ID, value: handler})
			seenHandlers[id] = true
		}
	}
	unindexedHandlers := make([]*EventHandler, 0)
	for id, handler := range b.handlers {
		if seenHandlers[id] {
			continue
		}
		if handler.ID == "" {
			handler.ID = id
		}
		unindexedHandlers = append(unindexedHandlers, handler)
	}
	sort.SliceStable(unindexedHandlers, func(i, j int) bool {
		if unindexedHandlers[i].HandlerRegisteredAt == unindexedHandlers[j].HandlerRegisteredAt {
			return unindexedHandlers[i].ID < unindexedHandlers[j].ID
		}
		return unindexedHandlers[i].HandlerRegisteredAt < unindexedHandlers[j].HandlerRegisteredAt
	})
	for _, handler := range unindexedHandlers {
		handlerJSONEntries = append(handlerJSONEntries, jsonObjectEntry{key: handler.ID, value: handler})
	}
	handlersJSON, err := marshalOrderedObject(handlerJSONEntries)
	if err != nil {
		return nil, err
	}

	eventHistoryJSONEntries := []jsonObjectEntry{}
	for _, event := range b.EventHistory.Values() {
		eventHistoryJSONEntries = append(eventHistoryJSONEntries, jsonObjectEntry{key: event.EventID, value: event})
	}
	eventHistoryJSON, err := marshalOrderedObject(eventHistoryJSONEntries)
	if err != nil {
		return nil, err
	}

	pending := make([]string, 0, len(b.pendingEventQueue))
	for _, event := range b.pendingEventQueue {
		pending = append(pending, event.EventID)
	}
	return marshalOrderedObject([]jsonObjectEntry{
		{key: "id", value: b.ID},
		{key: "name", value: b.Name},
		{key: "max_history_size", value: b.EventHistory.MaxHistorySize},
		{key: "max_history_drop", value: b.EventHistory.MaxHistoryDrop},
		{key: "event_concurrency", value: b.EventConcurrency},
		{key: "event_timeout", value: b.EventTimeout},
		{key: "event_slow_timeout", value: b.EventSlowTimeout},
		{key: "event_handler_concurrency", value: b.EventHandlerConcurrency},
		{key: "event_handler_completion", value: b.EventHandlerCompletion},
		{key: "event_handler_slow_timeout", value: b.EventHandlerSlowTimeout},
		{key: "event_handler_detect_file_paths", value: b.EventHandlerDetectFilePaths},
		{key: "handlers", value: json.RawMessage(handlersJSON)},
		{key: "handlers_by_key", value: json.RawMessage(handlersByKeyJSON)},
		{key: "event_history", value: json.RawMessage(eventHistoryJSON)},
		{key: "pending_event_queue", value: pending},
	})
}

func EventBusFromJSON(data []byte) (*EventBus, error) {
	var parsed EventBusJSON
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, err
	}
	var rawPayload struct {
		MaxHistorySize json.RawMessage `json:"max_history_size"`
		EventTimeout   json.RawMessage `json:"event_timeout"`
		EventHistory   json.RawMessage `json:"event_history"`
	}
	if err := json.Unmarshal(data, &rawPayload); err != nil {
		return nil, err
	}
	maxHistorySize := parsed.MaxHistorySize
	if rawPayload.MaxHistorySize == nil {
		maxHistorySize = ptr(DefaultMaxHistorySize)
	}
	eventTimeout := parsed.EventTimeout
	bus := NewEventBus(parsed.Name, &EventBusOptions{
		ID:                          parsed.ID,
		MaxHistorySize:              maxHistorySize,
		MaxHistoryDrop:              parsed.MaxHistoryDrop,
		EventConcurrency:            parsed.EventConcurrency,
		EventTimeout:                eventTimeout,
		EventSlowTimeout:            parsed.EventSlowTimeout,
		EventHandlerConcurrency:     parsed.EventHandlerConcurrency,
		EventHandlerCompletion:      parsed.EventHandlerCompletion,
		EventHandlerSlowTimeout:     parsed.EventHandlerSlowTimeout,
		EventHandlerDetectFilePaths: &parsed.EventHandlerDetectFilePaths,
	})
	if parsed.Handlers != nil {
		bus.handlers = parsed.Handlers
	}
	if parsed.HandlersByKey != nil {
		bus.handlersByKey = parsed.HandlersByKey
	}
	bus.EventHistory = NewEventHistory(maxHistorySize, parsed.MaxHistoryDrop)

	addHistoryEvent := func(eventID string, event *BaseEvent) {
		if event.EventID == "" {
			event.EventID = eventID
		}
		event.Bus = bus
		for _, result := range event.EventResults {
			result.Event = event
			if handler := bus.handlers[result.HandlerID]; handler != nil {
				result.Handler = handler
			}
		}
		bus.EventHistory.AddEvent(event)
	}

	if orderedHistory, ok, err := orderedJSONRawObjectEntries(rawPayload.EventHistory); err != nil {
		return nil, err
	} else if ok {
		for _, entry := range orderedHistory {
			var event BaseEvent
			if err := json.Unmarshal(entry.raw, &event); err != nil {
				return nil, err
			}
			addHistoryEvent(entry.key, &event)
		}
	} else {
		historyIDs := make([]string, 0, len(parsed.EventHistory))
		for eventID := range parsed.EventHistory {
			historyIDs = append(historyIDs, eventID)
		}
		sort.Strings(historyIDs)
		for _, eventID := range historyIDs {
			if event := parsed.EventHistory[eventID]; event != nil {
				addHistoryEvent(eventID, event)
			}
		}
	}

	bus.pendingEventQueue = []*BaseEvent{}
	for _, event_id := range parsed.PendingEventQueue {
		if event := bus.EventHistory.GetEvent(event_id); event != nil {
			bus.pendingEventQueue = append(bus.pendingEventQueue, event)
		}
	}
	return bus, nil
}

func (b *EventBus) resolveFindWaitersLocked(event *BaseEvent) {
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

func (b *EventBus) resolveFindWaiters(event *BaseEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.resolveFindWaitersLocked(event)
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

func eventMatchesEquals(event *BaseEvent, equals map[string]any) bool {
	if len(equals) == 0 {
		return true
	}
	data, err := event.ToJSON()
	if err != nil {
		return false
	}
	var record map[string]any
	if err := json.Unmarshal(data, &record); err != nil {
		return false
	}
	for key, value := range equals {
		actual, ok := record[key]
		if !ok || !reflect.DeepEqual(normalizeJSONValue(actual), normalizeJSONValue(value)) {
			return false
		}
	}
	return true
}

func (b *EventBus) eventMatchesEquals(event *BaseEvent, equals map[string]any) bool {
	return eventMatchesEquals(event, equals)
}

func (b *EventBus) Find(event_pattern string, where func(event *BaseEvent) bool, options *FindOptions) (*BaseEvent, error) {
	if options == nil {
		options = &FindOptions{}
	}
	future_enabled, future_timeout := normalizeFuture(options.Future)
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
	b.mu.Lock()
	historyMatch := b.EventHistory.Find(event_pattern, where, &EventHistoryFindOptions{Past: options.Past, ChildOf: options.ChildOf, Equals: options.Equals})
	if historyMatch != nil {
		b.mu.Unlock()
		return historyMatch, nil
	}
	if !future_enabled {
		b.mu.Unlock()
		return nil, nil
	}
	resolved := make(chan *BaseEvent, 1)
	waiter := &findWaiter{EventPattern: event_pattern, Matches: matches, Resolve: func(event *BaseEvent) {
		select {
		case resolved <- event:
		default:
		}
	}}
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
		if event == nil {
			return nil, nil
		}
		return event, nil
	}
	select {
	case event := <-resolved:
		cleanup()
		if event == nil {
			return nil, nil
		}
		return event, nil
	case <-time.After(time.Duration(*future_timeout * float64(time.Second))):
		cleanup()
		return nil, nil
	}
}

func (b *EventBus) Filter(event_pattern string, where func(event *BaseEvent) bool, options *FilterOptions) ([]*BaseEvent, error) {
	if options == nil {
		options = &FilterOptions{}
	}
	if options.Limit != nil && *options.Limit <= 0 {
		return []*BaseEvent{}, nil
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

	results := b.EventHistory.Filter(event_pattern, where, &EventHistoryFindOptions{
		Past:    options.Past,
		ChildOf: options.ChildOf,
		Equals:  options.Equals,
		Limit:   options.Limit,
	})
	if options.Limit != nil && len(results) >= *options.Limit {
		return results[:*options.Limit], nil
	}

	future_enabled, future_timeout := normalizeFuture(options.Future)
	if !future_enabled {
		return results, nil
	}

	resolved := make(chan *BaseEvent, 1)
	waiter := &findWaiter{EventPattern: event_pattern, Matches: matches, Resolve: func(event *BaseEvent) {
		select {
		case resolved <- event:
		default:
		}
	}}
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

	var event *BaseEvent
	if future_timeout == nil {
		event = <-resolved
	} else {
		select {
		case event = <-resolved:
		case <-time.After(time.Duration(*future_timeout * float64(time.Second))):
		}
	}
	cleanup()
	if event != nil {
		results = append(results, event)
	}
	if options.Limit != nil && len(results) > *options.Limit {
		return results[:*options.Limit], nil
	}
	return results, nil
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
		dur = fmt.Sprintf(" [%.3fs]", completed_at.Sub(started_at).Seconds())
	}
	line := fmt.Sprintf("%s%s %s#%s%s", prefix, connector, event.EventType, suffix(event.EventID, 4), dur)
	out := []string{line}
	for _, r := range event.sortedEventResults() {
		status, resultValue, errorValue, _ := r.snapshot()
		sym := "✅"
		if status == EventResultError {
			sym = "❌"
		}
		rline := fmt.Sprintf("%s%s %s %s.%s#%s", nextPrefix, connector, sym, b.Label(), r.HandlerName, suffix(r.HandlerID, 4))
		if resultValue != nil {
			rline += fmt.Sprintf(" => %v", resultValue)
		}
		if errorValue != nil {
			rline += fmt.Sprintf(" err=%v", errorValue)
		}
		out = append(out, rline)
		children := r.childEvents()
		for i, child := range children {
			out = append(out, b.logEventTree(child, nextPrefix, i == len(children)-1)...)
		}
	}
	return out
}

func (b *EventBus) Stop() {
	eventBusRegistry.Lock()
	delete(eventBusRegistry.instances, b)
	eventBusRegistry.Unlock()
	b.mu.Lock()
	waiters := append([]*findWaiter{}, b.findWaiters...)
	b.findWaiters = []*findWaiter{}
	b.handlers = map[string]*EventHandler{}
	b.handlersByKey = map[string][]string{}
	b.EventHistory.Clear()
	b.pendingEventQueue = []*BaseEvent{}
	b.inFlightEventIDs = map[string]bool{}
	b.runloopRunning = false
	b.mu.Unlock()
	for _, waiter := range waiters {
		waiter.Resolve(nil)
	}
}
