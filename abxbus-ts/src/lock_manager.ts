import type { BaseEvent } from './base_event.js'
import type { EventResult } from './event_result.js'

// ─── Deferred / withResolvers ────────────────────────────────────────────────

export type Deferred<T> = {
  promise: Promise<T>
  resolve: (value: T | PromiseLike<T>) => void
  reject: (reason?: unknown) => void
}

export const withResolvers = <T>(): Deferred<T> => {
  if (typeof Promise.withResolvers === 'function') {
    return Promise.withResolvers<T>()
  }
  let resolve!: (value: T | PromiseLike<T>) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((resolve_fn, reject_fn) => {
    resolve = resolve_fn
    reject = reject_fn
  })
  return { promise, resolve, reject }
}

// ─── Concurrency modes ──────────────────────────────────────────────────────

export const EVENT_CONCURRENCY_MODES = ['global-serial', 'bus-serial', 'parallel'] as const
export type EventConcurrencyMode = (typeof EVENT_CONCURRENCY_MODES)[number]

export const EVENT_HANDLER_CONCURRENCY_MODES = ['serial', 'parallel'] as const
export type EventHandlerConcurrencyMode = (typeof EVENT_HANDLER_CONCURRENCY_MODES)[number]

export const EVENT_HANDLER_COMPLETION_MODES = ['all', 'first'] as const
export type EventHandlerCompletionMode = (typeof EVENT_HANDLER_COMPLETION_MODES)[number]

// ─── AsyncLock ───────────────────────────────────────────────────────────────

export class AsyncLock {
  size: number
  in_use: number
  waiters: Array<() => void>

  constructor(size: number) {
    this.size = size
    this.in_use = 0
    this.waiters = []
  }

  async acquire(): Promise<void> {
    if (this.size === Infinity) {
      return
    }
    if (this.in_use < this.size) {
      this.in_use += 1
      return
    }
    await new Promise<void>((resolve) => {
      this.waiters.push(resolve)
    })
  }

  release(): void {
    if (this.size === Infinity) {
      return
    }
    const next = this.waiters.shift()
    if (next) {
      // Handoff: keep permit accounted for and transfer directly to next waiter.
      next()
      return
    }
    this.in_use = Math.max(0, this.in_use - 1)
  }
}

export const runWithLock = async <T>(lock: AsyncLock | null, fn: () => Promise<T>): Promise<T> => {
  if (!lock) {
    return await fn()
  }
  await lock.acquire()
  try {
    return await fn()
  } finally {
    lock.release()
  }
}

// ─── HandlerLock ─────────────────────────────────────────────────────────────

export type HandlerExecutionState = 'held' | 'yielded' | 'closed'

// Tracks a single handler execution's ownership of a handler lock.
// Reacquire is race-safe: if the handler exits while waiting to reclaim,
// the reclaimed lock is immediately released to avoid leaks.
export class HandlerLock {
  private lock: AsyncLock | null
  private state: HandlerExecutionState

  constructor(lock: AsyncLock | null) {
    this.lock = lock
    this.state = 'held'
  }

  // used by EventBus._processEventImmediately to yield the parent handler's lock to the child event so it can be processed immediately
  yieldHandlerLockForChildRun(): boolean {
    if (!this.lock || this.state !== 'held') {
      return false
    }
    this.state = 'yielded'
    this.lock.release()
    return true
  }

  // used by EventBus._processEventImmediately to reacquire the handler lock after the child event has been processed
  async reclaimHandlerLockIfRunning(): Promise<boolean> {
    if (!this.lock || this.state !== 'yielded') {
      return false
    }
    await this.lock.acquire()
    if (this.state !== 'yielded') {
      // Handler exited while this reacquire was pending.
      this.lock.release()
      return false
    }
    this.state = 'held'
    return true
  }

  // used by EventResult.runHandler to exit the handler lock after the handler has finished executing
  exitHandlerRun(): void {
    if (this.state === 'closed') {
      return
    }
    const should_release = !!this.lock && this.state === 'held'
    this.state = 'closed'
    if (should_release) {
      this.lock!.release()
    }
  }

  // used by EventBus._processEventImmediately to yield the handler lock and reacquire it after the child event has been processed
  async runQueueJump<T>(fn: () => Promise<T>): Promise<T> {
    const yielded = this.yieldHandlerLockForChildRun()
    try {
      return await fn()
    } finally {
      if (yielded) {
        await this.reclaimHandlerLockIfRunning()
      }
    }
  }
}

// ─── LockManager ─────────────────────────────────────────────────────────────

// Interface that must be implemented by the EventBus class to be used by the LockManager
export type EventBusInterfaceForLockManager = {
  isIdleAndQueueEmpty: () => boolean
  event_concurrency: EventConcurrencyMode
  _lock_for_event_global_serial: AsyncLock
}

export type LockManagerOptions = {
  auto_schedule_idle_checks?: boolean
}

// The LockManager is responsible for managing the concurrency of events and handlers
export class LockManager {
  private bus: EventBusInterfaceForLockManager // Live bus reference; used to read defaults and idle state.
  private auto_schedule_idle_checks: boolean

  readonly bus_event_lock: AsyncLock // Per-bus event lock; created with LockManager and never swapped.
  private pause_depth: number // Re-entrant pause counter; increments on _requestRunloopPause, decrements on release.
  private pause_waiters: Array<() => void> // Resolvers for _waitUntilRunloopResumed; drained when pause_depth hits 0.
  private active_handler_results: EventResult[] // Stack of active handler results for "inside handler" detection.

  private idle_waiters: Array<(became_idle: boolean) => void> // Resolvers waiting for stable idle; cleared when idle confirmed.
  private idle_check_pending: boolean // Debounce flag to avoid scheduling redundant idle checks.
  private idle_check_streak: number // Counts consecutive idle checks; used to require two ticks of idle.

  constructor(bus: EventBusInterfaceForLockManager, options: LockManagerOptions = {}) {
    this.bus = bus
    this.auto_schedule_idle_checks = options.auto_schedule_idle_checks ?? true
    this.bus_event_lock = new AsyncLock(1) // used for the bus-serial concurrency mode

    this.pause_depth = 0
    this.pause_waiters = []
    this.active_handler_results = []

    this.idle_waiters = []
    this.idle_check_pending = false
    this.idle_check_streak = 0
  }

  // Low-level runloop pause: increments a re-entrant counter and returns a release
  // function. Used for broad, bus-scoped pauses during queue-jump across buses.
  _requestRunloopPause(): () => void {
    this.pause_depth += 1
    let released = false
    return () => {
      if (released) {
        return
      }
      released = true
      this.pause_depth = Math.max(0, this.pause_depth - 1)
      if (this.pause_depth !== 0) {
        return
      }
      const waiters = this.pause_waiters
      this.pause_waiters = []
      for (const resolve of waiters) {
        resolve()
      }
    }
  }

  _waitUntilRunloopResumed(): Promise<void> {
    if (this.pause_depth === 0) {
      return Promise.resolve()
    }
    return new Promise((resolve) => {
      this.pause_waiters.push(resolve)
    })
  }

  _isPaused(): boolean {
    return this.pause_depth > 0
  }

  async _runWithHandlerDispatchContext<T>(result: EventResult, fn: () => Promise<T>): Promise<T> {
    this.active_handler_results.push(result)
    try {
      return await fn()
    } finally {
      const idx = this.active_handler_results.indexOf(result)
      if (idx >= 0) {
        this.active_handler_results.splice(idx, 1)
      }
    }
  }

  _getActiveHandlerResult(): EventResult | undefined {
    return this.active_handler_results[this.active_handler_results.length - 1]
  }

  _getActiveHandlerResults(): EventResult[] {
    return [...this.active_handler_results]
  }

  // Per-bus check: true only if this specific bus has a handler on its stack.
  // For cross-bus queue-jumping, EventBus._processEventImmediately uses getParentEventResultAcrossAllBuses()
  // to walk up the parent event tree, and the bus proxy passes handler_result
  // to _processEventImmediately so it can yield/reacquire the correct lock.
  _isAnyHandlerActive(): boolean {
    return this.active_handler_results.length > 0
  }

  waitForIdle(timeout_seconds: number | null = null): Promise<boolean> {
    return new Promise((resolve) => {
      let done = false
      let timeout_id: ReturnType<typeof setTimeout> | null = null

      const finish = (became_idle: boolean): void => {
        if (done) {
          return
        }
        done = true
        if (timeout_id !== null) {
          clearTimeout(timeout_id)
          timeout_id = null
        }
        resolve(became_idle)
      }

      this.idle_waiters.push(finish)
      this.scheduleIdleCheck()

      if (timeout_seconds === null || timeout_seconds === undefined) {
        return
      }

      const timeout_ms = Math.max(0, Number(timeout_seconds)) * 1000
      if (!Number.isFinite(timeout_ms)) {
        return
      }

      timeout_id = setTimeout(() => {
        const index = this.idle_waiters.indexOf(finish)
        if (index >= 0) {
          this.idle_waiters.splice(index, 1)
        }
        finish(false)
      }, timeout_ms)
    })
  }

  // Called by EventBus.markEventCompleted and EventBus.markHandlerCompleted to notify
  // waitUntilIdle() callers that the bus may now be idle.
  _notifyIdleListeners(): void {
    // Fast-path: most completions have no waitUntilIdle() callers waiting,
    // so skip expensive idle snapshot scans in that common case.
    if (this.idle_waiters.length === 0) {
      this.idle_check_streak = 0
      return
    }

    if (!this.bus.isIdleAndQueueEmpty()) {
      this.idle_check_streak = 0
      if (this.idle_waiters.length > 0) {
        this.scheduleIdleCheck()
      }
      return
    }

    this.idle_check_streak += 1
    if (this.idle_check_streak < 2) {
      if (this.idle_waiters.length > 0) {
        this.scheduleIdleCheck()
      }
      return
    }

    this.idle_check_streak = 0
    const waiters = this.idle_waiters
    this.idle_waiters = []
    for (const resolve of waiters) {
      resolve(true)
    }
  }

  // get the bus-level lock that prevents/allows multiple events to be processed concurrently on the same bus
  getLockForEvent(event: BaseEvent): AsyncLock | null {
    const resolved = event.event_concurrency ?? this.bus.event_concurrency
    if (resolved === 'parallel') {
      return null
    }
    if (resolved === 'global-serial') {
      return this.bus._lock_for_event_global_serial
    }
    return this.bus_event_lock
  }

  async _runWithEventLock<T>(
    event: BaseEvent,
    fn: () => Promise<T>,
    options: { bypass_event_locks?: boolean; pre_acquired_lock?: AsyncLock | null } = {}
  ): Promise<T> {
    const pre_acquired = options.pre_acquired_lock ?? null
    if (options.bypass_event_locks || pre_acquired) {
      return await fn()
    }
    return await runWithLock(this.getLockForEvent(event), fn)
  }

  async _runWithHandlerLock<T>(
    event: BaseEvent,
    default_handler_concurrency: EventHandlerConcurrencyMode | undefined,
    fn: (lock: HandlerLock | null) => Promise<T>
  ): Promise<T> {
    const lock = event._getHandlerLock(default_handler_concurrency)
    if (lock) {
      await lock.acquire()
    }
    const handler_lock = lock ? new HandlerLock(lock) : null
    try {
      return await fn(handler_lock)
    } finally {
      handler_lock?.exitHandlerRun()
    }
  }

  // Schedules a debounced idle check to run after a short delay. Used to gate
  // waitUntilIdle() calls during handler execution and after event completion.
  private scheduleIdleCheck(): void {
    if (!this.auto_schedule_idle_checks) {
      return
    }
    if (this.idle_check_pending) {
      return
    }
    this.idle_check_pending = true
    setTimeout(() => {
      this.idle_check_pending = false
      this._notifyIdleListeners()
    }, 0)
  }

  // Reset all state to initial values
  clear(): void {
    this.pause_depth = 0
    this.pause_waiters = []
    this.active_handler_results = []
    this.idle_waiters = []
    this.idle_check_pending = false
    this.idle_check_streak = 0
  }
}
