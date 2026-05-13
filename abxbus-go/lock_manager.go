package abxbus

import (
	"bytes"
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

type LockManager struct {
	bus *EventBus

	bus_event_lock *AsyncLock

	pause_mu      sync.Mutex
	pause_depth   int
	pause_waiters []chan struct{}

	active_mu               sync.Mutex
	active_handler_result   []*EventResult
	active_dispatch_context []context.Context
	active_handler_context  []context.Context
	active_handler_by_g     map[uint64][]*EventResult
	active_dispatch_by_g    map[uint64][]context.Context
	active_context_by_g     map[uint64][]context.Context

	idle_mu      sync.Mutex
	idle_waiters []chan struct{}
}

func NewLockManager(bus *EventBus) *LockManager {
	return &LockManager{
		bus:                  bus,
		bus_event_lock:       NewAsyncLock(1),
		active_handler_by_g:  map[uint64][]*EventResult{},
		active_dispatch_by_g: map[uint64][]context.Context{},
		active_context_by_g:  map[uint64][]context.Context{},
	}
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

func (l *LockManager) runWithHandlerDispatchContext(result *EventResult, handlerCtx context.Context, fn func() error) error {
	gid := currentGoroutineID()
	l.active_mu.Lock()
	l.active_handler_result = append(l.active_handler_result, result)
	if l.active_handler_by_g == nil {
		l.active_handler_by_g = map[uint64][]*EventResult{}
	}
	if l.active_dispatch_by_g == nil {
		l.active_dispatch_by_g = map[uint64][]context.Context{}
	}
	if l.active_context_by_g == nil {
		l.active_context_by_g = map[uint64][]context.Context{}
	}
	l.active_handler_by_g[gid] = append(l.active_handler_by_g[gid], result)
	if result != nil && result.Event != nil && result.Event.dispatchCtx != nil {
		l.active_dispatch_context = append(l.active_dispatch_context, result.Event.dispatchCtx)
		l.active_dispatch_by_g[gid] = append(l.active_dispatch_by_g[gid], result.Event.dispatchCtx)
	} else {
		l.active_dispatch_context = append(l.active_dispatch_context, nil)
		l.active_dispatch_by_g[gid] = append(l.active_dispatch_by_g[gid], nil)
	}
	l.active_handler_context = append(l.active_handler_context, handlerCtx)
	l.active_context_by_g[gid] = append(l.active_context_by_g[gid], handlerCtx)
	l.active_mu.Unlock()
	defer func() {
		l.active_mu.Lock()
		defer l.active_mu.Unlock()
		for i := len(l.active_handler_result) - 1; i >= 0; i-- {
			if l.active_handler_result[i] == result {
				l.active_handler_result = append(l.active_handler_result[:i], l.active_handler_result[i+1:]...)
				l.active_dispatch_context = append(l.active_dispatch_context[:i], l.active_dispatch_context[i+1:]...)
				l.active_handler_context = append(l.active_handler_context[:i], l.active_handler_context[i+1:]...)
				break
			}
		}
		if stack := l.active_handler_by_g[gid]; len(stack) > 0 {
			l.active_handler_by_g[gid] = stack[:len(stack)-1]
			if len(l.active_handler_by_g[gid]) == 0 {
				delete(l.active_handler_by_g, gid)
			}
		}
		if stack := l.active_dispatch_by_g[gid]; len(stack) > 0 {
			l.active_dispatch_by_g[gid] = stack[:len(stack)-1]
			if len(l.active_dispatch_by_g[gid]) == 0 {
				delete(l.active_dispatch_by_g, gid)
			}
		}
		if stack := l.active_context_by_g[gid]; len(stack) > 0 {
			l.active_context_by_g[gid] = stack[:len(stack)-1]
			if len(l.active_context_by_g[gid]) == 0 {
				delete(l.active_context_by_g, gid)
			}
		}
	}()
	return fn()
}

func (l *LockManager) getActiveHandlerResult() *EventResult {
	l.active_mu.Lock()
	defer l.active_mu.Unlock()
	gid := currentGoroutineID()
	if stack := l.active_handler_by_g[gid]; len(stack) > 0 {
		return stack[len(stack)-1]
	}
	return nil
}

func (l *LockManager) getAnyActiveHandlerResult() *EventResult {
	l.active_mu.Lock()
	defer l.active_mu.Unlock()
	if len(l.active_handler_result) == 0 {
		return nil
	}
	return l.active_handler_result[len(l.active_handler_result)-1]
}

func (l *LockManager) hasAnyActiveHandlerResult() bool {
	l.active_mu.Lock()
	defer l.active_mu.Unlock()
	return len(l.active_handler_result) > 0
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
	defer l.active_mu.Unlock()
	gid := currentGoroutineID()
	if stack := l.active_dispatch_by_g[gid]; len(stack) > 0 {
		return stack[len(stack)-1]
	}
	return nil
}

func (l *LockManager) getActiveHandlerContext() context.Context {
	l.active_mu.Lock()
	defer l.active_mu.Unlock()
	gid := currentGoroutineID()
	if stack := l.active_context_by_g[gid]; len(stack) > 0 {
		return stack[len(stack)-1]
	}
	return nil
}

func currentGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	fields := bytes.Fields(buf[:n])
	if len(fields) < 2 {
		return 0
	}
	id, _ := strconv.ParseUint(string(fields[1]), 10, 64)
	return id
}
