package abxbus

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"time"
)

type EventConcurrencyMode string

type EventHandlerConcurrencyMode string

type EventHandlerCompletionMode string

const (
	EventConcurrencyGlobalSerial EventConcurrencyMode = "global-serial"
	EventConcurrencyBusSerial    EventConcurrencyMode = "bus-serial"
	EventConcurrencyParallel     EventConcurrencyMode = "parallel"

	EventHandlerConcurrencySerial   EventHandlerConcurrencyMode = "serial"
	EventHandlerConcurrencyParallel EventHandlerConcurrencyMode = "parallel"

	EventHandlerCompletionAll   EventHandlerCompletionMode = "all"
	EventHandlerCompletionFirst EventHandlerCompletionMode = "first"
)

type AsyncLock struct {
	ch chan struct{}
}

func NewAsyncLock(size int) *AsyncLock {
	if size <= 0 {
		size = 1
	}
	return &AsyncLock{ch: make(chan struct{}, size)}
}

func (l *AsyncLock) Acquire(ctx context.Context) error {
	select {
	case l.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *AsyncLock) Release() {
	select {
	case <-l.ch:
	default:
	}
}

var shared_global_event_lock = NewAsyncLock(1)

type activeDispatchEntry struct {
	bus    *EventBus
	result *EventResult
	ctx    context.Context
}

type activeDispatchContextKey struct{}

func contextWithActiveDispatchEntry(ctx context.Context, bus *EventBus, result *EventResult) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, activeDispatchContextKey{}, activeDispatchEntry{
		bus:    bus,
		result: result,
		ctx:    ctx,
	})
}

func activeDispatchEntryFromContext(ctx context.Context) (activeDispatchEntry, bool) {
	if ctx == nil {
		return activeDispatchEntry{}, false
	}
	entry, ok := ctx.Value(activeDispatchContextKey{}).(activeDispatchEntry)
	entry.ctx = ctx
	return entry, ok && entry.result != nil
}

var activeDispatchRegistry = struct {
	sync.Mutex
	total       int
	byGoroutine map[uint64][]activeDispatchEntry
}{byGoroutine: map[uint64][]activeDispatchEntry{}}

type LockManager struct {
	bus *EventBus

	bus_event_lock *AsyncLock

	pause_mu      sync.Mutex
	pause_depth   int
	pause_waiters []chan struct{}

	active_mu               sync.Mutex
	active_handler_result   []*EventResult
	active_dispatch_context []context.Context
	active_dispatch_goid    []uint64

	idle_mu      sync.Mutex
	idle_waiters []chan struct{}
}

func NewLockManager(bus *EventBus) *LockManager {
	return &LockManager{bus: bus, bus_event_lock: NewAsyncLock(1)}
}

func (l *LockManager) getLockForEvent(event *BaseEvent) *AsyncLock {
	mode := event.EventConcurrency
	if mode == "" {
		mode = l.bus.EventConcurrency
	}
	switch mode {
	case EventConcurrencyGlobalSerial:
		return shared_global_event_lock
	case EventConcurrencyBusSerial:
		return l.bus_event_lock
	default:
		return nil
	}
}

func (l *LockManager) requestRunloopPause() func() {
	l.pause_mu.Lock()
	l.pause_depth++
	l.pause_mu.Unlock()
	released := false
	return func() {
		l.pause_mu.Lock()
		defer l.pause_mu.Unlock()
		if released {
			return
		}
		released = true
		if l.pause_depth > 0 {
			l.pause_depth--
		}
		if l.pause_depth == 0 {
			for _, w := range l.pause_waiters {
				close(w)
			}
			l.pause_waiters = nil
		}
	}
}

func (l *LockManager) isPaused() bool {
	l.pause_mu.Lock()
	defer l.pause_mu.Unlock()
	return l.pause_depth > 0
}

func (l *LockManager) waitUntilRunloopResumed(ctx context.Context) error {
	l.pause_mu.Lock()
	if l.pause_depth == 0 {
		l.pause_mu.Unlock()
		return nil
	}
	w := make(chan struct{})
	l.pause_waiters = append(l.pause_waiters, w)
	l.pause_mu.Unlock()
	select {
	case <-w:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *LockManager) runWithHandlerDispatchContext(result *EventResult, dispatchCtx context.Context, fn func() error) error {
	goid := currentGoroutineID()
	activeCtx := dispatchCtx
	if activeCtx == nil && result != nil && result.Event != nil && result.Event.dispatchCtx != nil {
		activeCtx = result.Event.dispatchCtx
	}
	activeCtx = contextWithActiveDispatchEntry(activeCtx, l.bus, result)
	var previousEventCtx context.Context
	if result != nil && result.Event != nil {
		result.Event.mu.Lock()
		previousEventCtx = result.Event.dispatchCtx
		result.Event.dispatchCtx = activeCtx
		result.Event.mu.Unlock()
	}
	l.active_mu.Lock()
	l.active_handler_result = append(l.active_handler_result, result)
	l.active_dispatch_goid = append(l.active_dispatch_goid, goid)
	l.active_dispatch_context = append(l.active_dispatch_context, activeCtx)
	l.active_mu.Unlock()
	if goid != 0 {
		activeDispatchRegistry.Lock()
		activeDispatchRegistry.total++
		activeDispatchRegistry.byGoroutine[goid] = append(activeDispatchRegistry.byGoroutine[goid], activeDispatchEntry{
			bus:    l.bus,
			result: result,
			ctx:    activeCtx,
		})
		activeDispatchRegistry.Unlock()
	}
	defer func() {
		if result != nil && result.Event != nil {
			result.Event.mu.Lock()
			if result.Event.dispatchCtx == activeCtx {
				result.Event.dispatchCtx = previousEventCtx
			}
			result.Event.mu.Unlock()
		}
		l.active_mu.Lock()
		for i := len(l.active_handler_result) - 1; i >= 0; i-- {
			if l.active_handler_result[i] == result {
				l.active_handler_result = append(l.active_handler_result[:i], l.active_handler_result[i+1:]...)
				l.active_dispatch_context = append(l.active_dispatch_context[:i], l.active_dispatch_context[i+1:]...)
				l.active_dispatch_goid = append(l.active_dispatch_goid[:i], l.active_dispatch_goid[i+1:]...)
				break
			}
		}
		l.active_mu.Unlock()
		if goid != 0 {
			activeDispatchRegistry.Lock()
			entries := activeDispatchRegistry.byGoroutine[goid]
			for i := len(entries) - 1; i >= 0; i-- {
				if entries[i].bus == l.bus && entries[i].result == result {
					entries = append(entries[:i], entries[i+1:]...)
					break
				}
			}
			if len(entries) == 0 {
				delete(activeDispatchRegistry.byGoroutine, goid)
			} else {
				activeDispatchRegistry.byGoroutine[goid] = entries
			}
			if activeDispatchRegistry.total > 0 {
				activeDispatchRegistry.total--
			}
			activeDispatchRegistry.Unlock()
		}
	}()
	return fn()
}

func hasActiveDispatchEntries() bool {
	activeDispatchRegistry.Lock()
	active := activeDispatchRegistry.total > 0
	activeDispatchRegistry.Unlock()
	return active
}

func activeDispatchEntryForGoroutine(goid uint64) (activeDispatchEntry, bool) {
	if goid == 0 {
		return activeDispatchEntry{}, false
	}
	activeDispatchRegistry.Lock()
	defer activeDispatchRegistry.Unlock()
	entries := activeDispatchRegistry.byGoroutine[goid]
	if len(entries) == 0 {
		return activeDispatchEntry{}, false
	}
	return entries[len(entries)-1], true
}

func (l *LockManager) getActiveHandlerResult() *EventResult {
	l.active_mu.Lock()
	defer l.active_mu.Unlock()
	if len(l.active_handler_result) == 0 {
		return nil
	}
	return l.active_handler_result[len(l.active_handler_result)-1]
}

func (l *LockManager) getAnyActiveHandlerResult() *EventResult {
	return l.getActiveHandlerResult()
}

func (l *LockManager) hasAnyActiveHandlerResult() bool {
	l.active_mu.Lock()
	defer l.active_mu.Unlock()
	return len(l.active_handler_result) > 0
}

func (l *LockManager) getActiveHandlerResultForGoroutine(goid uint64) *EventResult {
	if goid == 0 {
		return nil
	}
	l.active_mu.Lock()
	defer l.active_mu.Unlock()
	for i := len(l.active_handler_result) - 1; i >= 0; i-- {
		if i < len(l.active_dispatch_goid) && l.active_dispatch_goid[i] == goid {
			return l.active_handler_result[i]
		}
	}
	return nil
}

func (l *LockManager) waitForIdle(timeout *float64) bool {
	deadline := time.Time{}
	if timeout != nil {
		deadline = time.Now().Add(time.Duration(*timeout * float64(time.Second)))
	}
	for {
		if l.bus.IsIdleAndQueueEmpty() {
			return true
		}
		waiter := make(chan struct{})
		l.idle_mu.Lock()
		if l.bus.IsIdleAndQueueEmpty() {
			l.idle_mu.Unlock()
			return true
		}
		l.idle_waiters = append(l.idle_waiters, waiter)
		l.idle_mu.Unlock()
		if !deadline.IsZero() && time.Now().After(deadline) {
			l.removeIdleWaiter(waiter)
			return false
		}
		if deadline.IsZero() {
			<-waiter
			continue
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			l.removeIdleWaiter(waiter)
			return false
		}
		timer := time.NewTimer(remaining)
		select {
		case <-waiter:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
			l.removeIdleWaiter(waiter)
			return l.bus.IsIdleAndQueueEmpty()
		}
	}
}

func (l *LockManager) notifyIdleListeners() {
	l.idle_mu.Lock()
	waiters := l.idle_waiters
	l.idle_waiters = nil
	l.idle_mu.Unlock()
	for _, waiter := range waiters {
		close(waiter)
	}
}

func (l *LockManager) clear() {
	l.pause_mu.Lock()
	pauseWaiters := l.pause_waiters
	l.pause_depth = 0
	l.pause_waiters = nil
	l.pause_mu.Unlock()
	for _, waiter := range pauseWaiters {
		close(waiter)
	}

	l.active_mu.Lock()
	l.active_handler_result = nil
	l.active_dispatch_context = nil
	l.active_dispatch_goid = nil
	l.bus_event_lock = NewAsyncLock(1)
	l.active_mu.Unlock()

	l.notifyIdleListeners()
}

func (l *LockManager) removeIdleWaiter(waiter chan struct{}) {
	l.idle_mu.Lock()
	defer l.idle_mu.Unlock()
	for i, candidate := range l.idle_waiters {
		if candidate == waiter {
			l.idle_waiters = append(l.idle_waiters[:i], l.idle_waiters[i+1:]...)
			return
		}
	}
}

func (l *LockManager) getActiveDispatchContext() context.Context {
	l.active_mu.Lock()
	hasActive := len(l.active_dispatch_context) > 0
	l.active_mu.Unlock()
	if !hasActive {
		return nil
	}
	return l.getActiveDispatchContextForGoroutine(currentGoroutineID())
}

func (l *LockManager) hasActiveDispatchContext() bool {
	l.active_mu.Lock()
	defer l.active_mu.Unlock()
	return len(l.active_dispatch_context) > 0
}

func (l *LockManager) getActiveDispatchContextForGoroutine(goid uint64) context.Context {
	if goid == 0 {
		return nil
	}
	l.active_mu.Lock()
	defer l.active_mu.Unlock()
	for i := len(l.active_dispatch_context) - 1; i >= 0; i-- {
		if i < len(l.active_dispatch_goid) && l.active_dispatch_goid[i] == goid {
			return l.active_dispatch_context[i]
		}
	}
	return nil
}

func (l *LockManager) getActiveHandlerContext() context.Context {
	return l.getActiveDispatchContext()
}

func currentGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	const prefix = "goroutine "
	if n <= len(prefix) || string(buf[:len(prefix)]) != prefix {
		return 0
	}
	end := len(prefix)
	for end < n && buf[end] >= '0' && buf[end] <= '9' {
		end++
	}
	id, err := strconv.ParseUint(string(buf[len(prefix):end]), 10, 64)
	if err != nil {
		return 0
	}
	return id
}
