package bubus

import (
	"context"
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

func (l *LockManager) runWithHandlerDispatchContext(result *EventResult, fn func() error) error {
	l.active_mu.Lock()
	l.active_handler_result = append(l.active_handler_result, result)
	if result != nil && result.Event != nil && result.Event.dispatchCtx != nil {
		l.active_dispatch_context = append(l.active_dispatch_context, result.Event.dispatchCtx)
	} else {
		l.active_dispatch_context = append(l.active_dispatch_context, nil)
	}
	l.active_mu.Unlock()
	defer func() {
		l.active_mu.Lock()
		defer l.active_mu.Unlock()
		for i := len(l.active_handler_result) - 1; i >= 0; i-- {
			if l.active_handler_result[i] == result {
				l.active_handler_result = append(l.active_handler_result[:i], l.active_handler_result[i+1:]...)
				l.active_dispatch_context = append(l.active_dispatch_context[:i], l.active_dispatch_context[i+1:]...)
				break
			}
		}
	}()
	return fn()
}

func (l *LockManager) getActiveHandlerResult() *EventResult {
	l.active_mu.Lock()
	defer l.active_mu.Unlock()
	if len(l.active_handler_result) == 0 {
		return nil
	}
	return l.active_handler_result[len(l.active_handler_result)-1]
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
		if !deadline.IsZero() && time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
}

func (l *LockManager) getActiveDispatchContext() context.Context {
	l.active_mu.Lock()
	defer l.active_mu.Unlock()
	if len(l.active_dispatch_context) == 0 {
		return nil
	}
	return l.active_dispatch_context[len(l.active_dispatch_context)-1]
}
