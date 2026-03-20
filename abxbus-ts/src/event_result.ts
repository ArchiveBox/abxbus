import { v7 as uuidv7 } from 'uuid'

import { z } from 'zod'

import { BaseEvent } from './base_event.js'
import type { EventBus } from './event_bus.js'
import { EventHandler, EventHandlerCancelledError, EventHandlerResultSchemaError, EventHandlerTimeoutError } from './event_handler.js'
import { withResolvers, type HandlerLock } from './lock_manager.js'
import type { Deferred } from './lock_manager.js'
import type { EventHandlerCallable, EventResultType } from './types.js'
import { isZodSchema } from './types.js'
import { _runWithAsyncContext } from './async_context.js'
import { RetryTimeoutError } from './retry.js'
import { _runWithAbortMonitor, _runWithSlowMonitor, _runWithTimeout } from './timing.js'
import { monotonicDatetime } from './helpers.js'

// More precise than event.event_status, includes separate 'error' state for handlers that throw errors during execution
export type EventResultStatus = 'pending' | 'started' | 'completed' | 'error'

export const EventResultJSONSchema = z
  .object({
    id: z.string(),
    status: z.enum(['pending', 'started', 'completed', 'error']),
    event_id: z.string(),
    handler_id: z.string(),
    handler_name: z.string(),
    handler_file_path: z.string().nullable().optional(),
    handler_timeout: z.number().nullable().optional(),
    handler_slow_timeout: z.number().nullable().optional(),
    handler_registered_at: z.string().datetime().optional(),
    handler_event_pattern: z.union([z.string(), z.literal('*')]).optional(),
    eventbus_name: z.string(),
    eventbus_id: z.string().uuid(),
    started_at: z.string().datetime().nullable().optional(),
    completed_at: z.string().datetime().nullable().optional(),
    result: z.unknown().optional(),
    error: z.unknown().optional(),
    event_children: z.array(z.string()),
  })
  .strict()

export type EventResultJSON = z.infer<typeof EventResultJSONSchema>

// Object that tracks the pending or completed execution of a single event handler
export class EventResult<TEvent extends BaseEvent = BaseEvent> {
  id: string // unique uuidv7 identifier for the event result
  status: EventResultStatus // 'pending', 'started', 'completed', or 'error'
  event: TEvent // the Event that the handler is processing
  handler: EventHandler // the EventHandler object that going to process the event
  started_at: string | null
  completed_at: string | null
  result?: EventResultType<TEvent> // parsed return value from the event handler
  error?: unknown // error object thrown by the event handler, or null if the handler completed successfully
  event_children: BaseEvent[] // list of emitted child events

  // Abort signal: created when handler starts, rejected by _signalAbort() to
  // interrupt runHandler's await via Promise.race.
  _abort: Deferred<never> | null
  // Handler lock: tracks ownership of the handler concurrency lock
  // during handler execution. Set by runHandler(), used by
  // _processEventImmediately for yield-and-reacquire during queue-jumps.
  _lock: HandlerLock | null
  // Runloop pause releases keyed by bus for queue-jump; released when handler exits.
  _queue_jump_pause_releases: Map<EventBus, () => void> | null

  constructor(params: { event: TEvent; handler: EventHandler }) {
    this.id = uuidv7()
    this.status = 'pending'
    this.event = params.event
    this.handler = params.handler
    this.started_at = null
    this.completed_at = null
    this.result = undefined
    this.error = undefined
    this.event_children = []
    this._abort = null
    this._lock = null
    this._queue_jump_pause_releases = null
  }

  toString(): string {
    return `${this.result ?? 'null'} (${this.status})`
  }

  get event_id(): string {
    return this.event.event_id
  }

  get bus(): EventBus {
    return this.event.bus!
  }

  get handler_id(): string {
    return this.handler.id
  }

  get handler_name(): string {
    return this.handler.handler_name
  }

  get handler_file_path(): string | null {
    return this.handler.handler_file_path
  }

  get eventbus_name(): string {
    return this.handler.eventbus_name
  }

  get eventbus_id(): string {
    return this.handler.eventbus_id
  }

  get eventbus_label(): string {
    return `${this.handler.eventbus_name}#${this.handler.eventbus_id.slice(-4)}`
  }

  private getHookBus(): EventBus | undefined {
    const root_bus = this.event.bus
    if (!root_bus) {
      return undefined
    }
    return root_bus.all_instances.findBusById(this.eventbus_id) ?? root_bus
  }

  private async _notifyStatusHook(status: 'started' | 'completed'): Promise<void> {
    const hook_bus = this.getHookBus()
    if (!hook_bus) {
      return
    }
    const event_for_hook = hook_bus._getEventProxyScopedToThisBus(this.event._event_original ?? this.event, this)
    await hook_bus.onEventResultChange(event_for_hook, this, status)
  }

  // shortcut for the result value so users can do event_result.value instead of event_result.result
  get value(): EventResultType<TEvent> | undefined {
    return this.result
  }

  // Per-result schema reference derives from the parent event schema.
  // It is intentionally not serialized with each EventResult to avoid duplication.
  get result_type(): TEvent['event_result_type'] {
    const original_event = this.event._event_original ?? this.event
    return original_event.event_result_type as TEvent['event_result_type']
  }

  // Link a child event emitted by this handler run to the parent event/result.
  _linkEmittedChildEvent(child_event: BaseEvent): void {
    const original_child = child_event._event_original ?? child_event
    const parent_event = this.event._event_original ?? this.event
    if (original_child.event_id === parent_event.event_id) {
      return
    }
    if (!original_child.event_parent_id) {
      original_child.event_parent_id = parent_event.event_id
    }
    if (!original_child.event_emitted_by_handler_id) {
      original_child.event_emitted_by_handler_id = this.handler_id
    }
    if (!this.event_children.some((child) => child.event_id === original_child.event_id)) {
      this.event_children.push(original_child)
    }
  }

  // Get the raw return value from the handler, even if it threw an error / failed validation
  get raw_value(): EventResultType<TEvent> | undefined {
    if (this.error && (this.error as any).raw_value !== undefined) {
      return (this.error as any).raw_value
    }
    return this.result
  }

  // Resolve handler timeout in seconds using precedence: handler -> event -> bus defaults.
  get handler_timeout(): number | null {
    const original = this.event._event_original ?? this.event
    const resolved_event_timeout = original.event_timeout ?? this.bus.event_timeout

    let resolved_handler_timeout: number | null
    if (this.handler.handler_timeout !== undefined) {
      resolved_handler_timeout = this.handler.handler_timeout
    } else if (original.event_handler_timeout !== undefined) {
      resolved_handler_timeout = original.event_handler_timeout
    } else {
      resolved_handler_timeout = this.bus.event_timeout
    }

    if (resolved_handler_timeout === null && resolved_event_timeout === null) {
      return null
    }
    if (resolved_handler_timeout === null) {
      return resolved_event_timeout
    }
    if (resolved_event_timeout === null) {
      return resolved_handler_timeout
    }
    return Math.min(resolved_handler_timeout, resolved_event_timeout)
  }

  // Resolve slow handler warning threshold in seconds using precedence: handler -> event -> bus defaults.
  get handler_slow_timeout(): number | null {
    const original = this.event._event_original ?? this.event

    if (this.handler.handler_slow_timeout !== undefined) {
      return this.handler.handler_slow_timeout
    }
    if (original.event_handler_slow_timeout !== undefined) {
      return original.event_handler_slow_timeout
    }
    const event_slow_timeout = (original as { event_slow_timeout?: number | null }).event_slow_timeout
    if (event_slow_timeout !== undefined) {
      return event_slow_timeout
    }
    if (this.bus?.event_handler_slow_timeout !== undefined) {
      return this.bus.event_handler_slow_timeout
    }
    return this.bus?.event_slow_timeout ?? null
  }

  // Create a slow-handler warning timer that logs if the handler runs too long.
  _createSlowHandlerWarningTimer(effective_timeout: number | null): ReturnType<typeof setTimeout> | null {
    const handler_warn_timeout = this.handler_slow_timeout
    const warn_ms = handler_warn_timeout === null ? null : handler_warn_timeout * 1000
    const should_warn = warn_ms !== null && (effective_timeout === null || effective_timeout * 1000 > warn_ms)
    if (!should_warn || warn_ms === null) {
      return null
    }
    const event = this.event._event_original ?? this.event
    const bus_name = this.handler.eventbus_name
    const started_at_ms = performance.now()
    return setTimeout(() => {
      if (this.status !== 'started') {
        return
      }
      const elapsed_ms = performance.now() - started_at_ms
      const elapsed_seconds = (elapsed_ms / 1000).toFixed(1)
      console.warn(
        `[abxbus] Slow event handler: ${bus_name}.on(${event.toString()}, ${this.handler.toString()}) still running after ${elapsed_seconds}s`
      )
    }, warn_ms)
  }

  _ensureQueueJumpPause(bus: EventBus): void {
    if (!this._queue_jump_pause_releases) {
      this._queue_jump_pause_releases = new Map()
    }
    if (this._queue_jump_pause_releases.has(bus)) {
      return
    }
    this._queue_jump_pause_releases.set(bus, bus.locks._requestRunloopPause())
  }

  _releaseQueueJumpPauses(): void {
    if (!this._queue_jump_pause_releases) {
      return
    }
    for (const release of this._queue_jump_pause_releases.values()) {
      release()
    }
    this._queue_jump_pause_releases.clear()
  }

  update(params: { status?: EventResultStatus; result?: EventResultType<TEvent> | BaseEvent | undefined; error?: unknown }): this {
    const has_status = 'status' in params
    const has_result = 'result' in params
    const has_error = 'error' in params

    if (has_result) {
      const raw_result = params.result
      this.status = 'completed'
      if (
        this.event.event_result_type &&
        raw_result !== undefined &&
        !(raw_result instanceof BaseEvent) &&
        isZodSchema(this.event.event_result_type)
      ) {
        const parsed = this.event.event_result_type.safeParse(raw_result)
        if (parsed.success) {
          this.result = parsed.data as EventResultType<TEvent>
        } else {
          const error = new EventHandlerResultSchemaError(
            `Event handler return value ${JSON.stringify(raw_result).slice(0, 20)}... did not match event_result_type: ${parsed.error.message}`,
            { event_result: this, cause: parsed.error, raw_value: raw_result }
          )
          this.error = error
          this.result = undefined
          this.status = 'error'
        }
      } else {
        this.result = raw_result as EventResultType<TEvent> | undefined
      }
    }

    if (has_error) {
      this.error = params.error
      this.status = 'error'
    }

    if (has_status && params.status !== undefined) {
      this.status = params.status
    }

    if (this.status !== 'pending' && this.started_at === null) {
      this.started_at = monotonicDatetime()
    }
    if ((this.status === 'completed' || this.status === 'error') && this.completed_at === null) {
      this.completed_at = monotonicDatetime()
    }

    return this
  }

  private _createHandlerTimeoutError(event: BaseEvent): EventHandlerTimeoutError {
    return new EventHandlerTimeoutError(
      `${this.bus.toString()}.on(${event.toString()}, ${this.handler.toString()}) timed out after ${this.handler_timeout}s`,
      {
        event_result: this,
        timeout_seconds: this.handler_timeout,
      }
    )
  }

  private _handleHandlerError(event: BaseEvent, error: unknown): void {
    const normalized_error =
      error instanceof RetryTimeoutError
        ? new EventHandlerTimeoutError(error.message, { event_result: this, timeout_seconds: error.timeout_seconds, cause: error })
        : error
    if (normalized_error instanceof EventHandlerTimeoutError) {
      this._markError(normalized_error, false)
      event._cancelPendingChildProcessing(normalized_error)
    } else {
      this._markError(normalized_error, false)
    }
  }

  private _onHandlerExit(slow_handler_warning_timer: ReturnType<typeof setTimeout> | null): void {
    this._abort = null
    this._lock = null
    this._releaseQueueJumpPauses()
    if (slow_handler_warning_timer) {
      clearTimeout(slow_handler_warning_timer)
    }
  }

  // Run one handler invocation with timeout/slow-monitor/error handling.
  // Handler lock acquisition is owned by BaseEvent._runHandlers(...).
  async runHandler(handler_lock: HandlerLock | null): Promise<void> {
    if (this.status === 'error' && this.error instanceof EventHandlerCancelledError) {
      return
    }

    const event = this.event._event_original ?? this.event
    const handler_event = this.bus._getEventProxyScopedToThisBus(event, this)
    if (this._lock) {
      this._lock.exitHandlerRun()
    }

    let slow_handler_warning_timer: ReturnType<typeof setTimeout> | null = null
    // if the result is already in an error or completed state, exit early
    if (this.status === 'error' || this.status === 'completed') {
      return
    }

    this._lock = handler_lock
    await this.bus.locks._runWithHandlerDispatchContext(this, async () => {
      await _runWithAsyncContext(event._getDispatchContext() ?? null, async () => {
        try {
          const should_notify_started = this.status === 'pending'
          const abort_signal = this._markStarted(false)
          if (should_notify_started) {
            await this._notifyStatusHook('started')
          }
          slow_handler_warning_timer = this._createSlowHandlerWarningTimer(this.handler_timeout)
          const handler_result = await _runWithTimeout(
            this.handler_timeout,
            () => this._createHandlerTimeoutError(event),
            () =>
              _runWithSlowMonitor(slow_handler_warning_timer, () =>
                _runWithAbortMonitor(() => this.handler._handler_async(handler_event), abort_signal)
              )
          )
          this._markCompleted(handler_result as EventResultType<TEvent> | BaseEvent | undefined, false)
        } catch (error) {
          this._handleHandlerError(event, error)
        } finally {
          if (this.status === 'completed' || this.status === 'error') {
            await this._notifyStatusHook('completed')
          }
          this._onHandlerExit(slow_handler_warning_timer)
        }
      })
    })
  }

  // Reject the abort promise, causing runHandler's Promise.race to
  // throw immediately — even if the handler has no timeout.
  _signalAbort(error: Error): void {
    if (this._abort) {
      this._abort.reject(error)
      this._abort = null
    }
  }

  // Mark started and return the abort promise for Promise.race.
  _markStarted(notify_hook: boolean = true): Promise<never> {
    if (!this._abort) {
      this._abort = withResolvers<never>()
    }
    if (this.status === 'pending') {
      this.update({ status: 'started' })
      if (notify_hook) {
        void this._notifyStatusHook('started')
      }
    }
    return this._abort.promise
  }

  _markCompleted(result: EventResultType<TEvent> | BaseEvent | undefined, notify_hook: boolean = true): void {
    if (this.status === 'completed' || this.status === 'error') return
    this.update({ result })
    if (notify_hook) {
      void this._notifyStatusHook('completed')
    }
  }

  _markError(error: unknown, notify_hook: boolean = true): void {
    if (this.status === 'completed' || this.status === 'error') return
    this.update({ error })
    if (notify_hook) {
      void this._notifyStatusHook('completed')
    }
  }

  toJSON(): EventResultJSON {
    return {
      id: this.id,
      status: this.status,
      event_id: this.event.event_id,
      handler_id: this.handler_id,
      handler_name: this.handler_name,
      handler_file_path: this.handler_file_path,
      handler_timeout: this.handler.handler_timeout,
      handler_slow_timeout: this.handler.handler_slow_timeout,
      handler_registered_at: this.handler.handler_registered_at,
      handler_event_pattern: this.handler.event_pattern,
      eventbus_name: this.eventbus_name,
      eventbus_id: this.eventbus_id,
      started_at: this.started_at,
      completed_at: this.completed_at,
      result: this.result,
      error: this.error,
      event_children: this.event_children.map((child) => child.event_id),
    }
  }

  static fromJSON<TEvent extends BaseEvent>(event: TEvent, data: unknown): EventResult<TEvent> {
    const record = EventResultJSONSchema.parse(data)
    const handler_record = {
      id: record.handler_id,
      eventbus_name: record.eventbus_name,
      eventbus_id: record.eventbus_id,
      event_pattern: record.handler_event_pattern ?? event.event_type,
      handler_name: record.handler_name,
      handler_file_path: record.handler_file_path ?? null,
      handler_timeout: record.handler_timeout,
      handler_slow_timeout: record.handler_slow_timeout,
      handler_registered_at: record.handler_registered_at ?? event.event_created_at,
    } as const
    const handler_stub = EventHandler.fromJSON(handler_record, (() => undefined) as EventHandlerCallable)

    const result = new EventResult<TEvent>({ event, handler: handler_stub })
    result.id = record.id
    result.status = record.status
    result.started_at = record.started_at === null || record.started_at === undefined ? null : monotonicDatetime(record.started_at)
    result.completed_at = record.completed_at === null || record.completed_at === undefined ? null : monotonicDatetime(record.completed_at)
    if ('result' in record) {
      result.result = record.result as EventResultType<TEvent>
    }
    if ('error' in record) {
      result.error = record.error
    }
    result.event_children = []
    return result
  }
}
