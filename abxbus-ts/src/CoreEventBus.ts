import { spawn, type ChildProcess } from 'node:child_process'
import { createHash, randomUUID } from 'node:crypto'

import { RustCoreClient, coreNamespace, type CoreMessage } from './CoreClient.js'
import { BaseEvent, BaseEventSchema } from './BaseEvent.js'
import type { BaseEventJSON } from './BaseEvent.js'
import {
  EventHandler,
  EventHandlerAbortedError,
  EventHandlerCancelledError,
  EventHandlerResultSchemaError,
  EventHandlerTimeoutError,
} from './EventHandler.js'
import type { EventHandlerJSON } from './EventHandler.js'
import { EventResult } from './EventResult.js'
import { captureAsyncContext, createAsyncLocalStorage, getActiveAbortContext } from './async_context.js'
import {
  AsyncLock,
  HandlerLock,
  runWithLock,
  type EventConcurrencyMode,
  type EventHandlerCompletionMode,
  type EventHandlerConcurrencyMode,
} from './LockManager.js'
import type { EventBusMiddleware, EventBusMiddlewareCtor, EventBusMiddlewareInput } from './EventBusMiddleware.js'
import { logTree } from './logging.js'
import { RetryTimeoutError } from './retry.js'
import { monotonicDatetime } from './helpers.js'
import {
  cancelWarningTimer as cancelCoalescedTimer,
  scheduleWarningTimer as scheduleCoalescedTimer,
  type WarningTimerHandle,
} from './timing.js'
import {
  normalizeEventPattern,
  toJsonSchema,
  type EventHandlerCallable,
  type EventPattern,
  type FilterOptions,
  type FindOptions,
} from './types.js'

export type CoreHandler<T extends BaseEvent = BaseEvent> = EventHandlerCallable<T>
type CoalescedTimerHandle = WarningTimerHandle

const CORE_EVENT_FIELD_NAMES = new Set(Object.keys(BaseEventSchema.shape))
const core_handler_context = createAsyncLocalStorage()
type CoreOutcomeBatch = { outcomes: CoreMessage[]; route_id: string | null }
const core_outcome_batch_context = createAsyncLocalStorage()
const CORE_ROUTE_SLICE_LIMIT = 65_536
const FAST_EVENT_DEADLINE_ABORT_THRESHOLD_SECONDS = 0.000001

function invocationMessagesFromCoreMessage(message: CoreMessage): CoreMessage[] {
  if (message.type === 'invoke_handler') {
    return [message]
  }
  if (message.type === 'invoke_handlers_compact') {
    const raw_invocations = Array.isArray(message.invocations) ? message.invocations : []
    const route_id = typeof message.route_id === 'string' ? message.route_id : ''
    const event_id = typeof message.event_id === 'string' ? message.event_id : ''
    const bus_id = typeof message.bus_id === 'string' ? message.bus_id : ''
    const event_deadline_at = typeof message.event_deadline_at === 'string' ? message.event_deadline_at : undefined
    const route_paused = message.route_paused === true
    const invocations: CoreMessage[] = []
    for (const entry of raw_invocations) {
      if (!Array.isArray(entry) || entry.length < 5) {
        continue
      }
      const [invocation_id, result_id, handler_id, fence, deadline_at] = entry
      if (typeof invocation_id !== 'string' || typeof result_id !== 'string' || typeof handler_id !== 'string') {
        continue
      }
      const invocation: CoreMessage = {
        type: 'invoke_handler',
        invocation_id,
        result_id,
        route_id,
        event_id,
        bus_id,
        handler_id,
        fence,
        route_paused,
      }
      if (typeof deadline_at === 'string') invocation.deadline_at = deadline_at
      if (event_deadline_at) invocation.event_deadline_at = event_deadline_at
      invocations.push(invocation)
    }
    return invocations
  }
  return []
}

function invocationMessagesFromCoreMessages(messages: CoreMessage[]): CoreMessage[] {
  const invocations: CoreMessage[] = []
  for (const message of messages) {
    invocations.push(...invocationMessagesFromCoreMessage(message))
  }
  return invocations
}

function messageIsInvocationBatch(message: CoreMessage): boolean {
  return message.type === 'invoke_handler' || message.type === 'invoke_handlers_compact'
}

export type RustCoreEventBusOptions = {
  id?: string
  core?: RustCoreClient
  event_concurrency?: EventConcurrencyMode
  event_handler_concurrency?: EventHandlerConcurrencyMode
  event_handler_completion?: EventHandlerCompletionMode
  event_handler_detect_file_paths?: boolean
  event_timeout?: number | null
  event_slow_timeout?: number | null
  event_handler_timeout?: number | null
  event_handler_slow_timeout?: number | null
  max_history_size?: number | null
  max_history_drop?: boolean
  middlewares?: EventBusMiddlewareInput[]
  background_worker?: boolean
}

export type RustCoreHandlerOptions = {
  id?: string
  handler_name?: string
  handler_registered_at?: string
  handler_file_path?: string | null
  handler_timeout?: number | null
  handler_slow_timeout?: number | null
  handler_concurrency?: EventHandlerConcurrencyMode | null
  handler_completion?: EventHandlerCompletionMode | null
  [key: string]: unknown
}

export type RustCoreFilterOptions = {
  limit?: number | null
  [field: string]: unknown
}

export type RustCoreEventBusJSON = {
  id: string
  name: string
  max_history_size: number | null
  max_history_drop: boolean
  event_concurrency: EventConcurrencyMode
  event_timeout: number | null
  event_slow_timeout: number | null
  event_handler_concurrency: EventHandlerConcurrencyMode
  event_handler_completion: EventHandlerCompletionMode
  event_handler_timeout: number | null
  event_handler_slow_timeout: number | null
  event_handler_detect_file_paths: boolean
  handlers: Record<string, EventHandlerJSON>
  handlers_by_key: Record<string, string[]>
  event_history: Record<string, BaseEventJSON>
  pending_event_queue: string[]
}

export class CoreEventBusRegistry {
  private _bus_refs = new Set<WeakRef<RustCoreEventBus>>()
  private _bus_refs_by_id = new Map<string, WeakRef<RustCoreEventBus>>()

  add(bus: RustCoreEventBus): void {
    const ref = new WeakRef(bus)
    this._bus_refs.add(ref)
    this._bus_refs_by_id.set(bus.bus_id, ref)
    this._bus_refs_by_id.set(bus.id, ref)
  }

  discard(bus: RustCoreEventBus): void {
    for (const ref of this._bus_refs) {
      const current = ref.deref()
      if (!current || current === bus) {
        this._bus_refs.delete(ref)
      }
    }
    for (const [bus_id, ref] of this._bus_refs_by_id.entries()) {
      const current = ref.deref()
      if (!current || current === bus) {
        this._bus_refs_by_id.delete(bus_id)
      }
    }
  }

  has(bus: RustCoreEventBus): boolean {
    for (const ref of this._bus_refs) {
      const current = ref.deref()
      if (!current) {
        this._bus_refs.delete(ref)
        continue
      }
      if (current === bus) {
        return true
      }
    }
    return false
  }

  get size(): number {
    let count = 0
    for (const ref of this._bus_refs) {
      if (ref.deref()) {
        count += 1
      } else {
        this._bus_refs.delete(ref)
      }
    }
    return count
  }

  *[Symbol.iterator](): IterableIterator<RustCoreEventBus> {
    for (const ref of this._bus_refs) {
      const bus = ref.deref()
      if (bus) {
        yield bus
      } else {
        this._bus_refs.delete(ref)
      }
    }
  }

  findBusById(bus_id: string): RustCoreEventBus | undefined {
    const indexed_ref = this._bus_refs_by_id.get(bus_id)
    if (indexed_ref) {
      const indexed_bus = indexed_ref.deref()
      if (indexed_bus && (indexed_bus.id === bus_id || indexed_bus.bus_id === bus_id)) {
        return indexed_bus
      }
      this._bus_refs_by_id.delete(bus_id)
    }
    for (const bus of this) {
      if (bus.id === bus_id || bus.bus_id === bus_id) {
        this._bus_refs_by_id.set(bus_id, new WeakRef(bus))
        return bus
      }
    }
    return undefined
  }

  findEventById(event_id: string): BaseEvent | null {
    for (const bus of this) {
      const event = bus.findEventById(event_id)
      if (event) {
        return event
      }
    }
    return null
  }
}

export const rustCoreEventBusRegistry = new CoreEventBusRegistry()

const normalizeMiddlewares = (middlewares?: EventBusMiddlewareInput[]): EventBusMiddleware[] => {
  const normalized: EventBusMiddleware[] = []
  for (const middleware of middlewares ?? []) {
    if (!middleware) continue
    if (typeof middleware === 'function') {
      normalized.push(new (middleware as EventBusMiddlewareCtor)())
    } else {
      normalized.push(middleware as EventBusMiddleware)
    }
  }
  return normalized
}

const normalizeMaxHistorySize = (value: number | null | undefined): number | null => {
  if (value === undefined) {
    return 100
  }
  if (value === null) {
    return null
  }
  if (value <= 0) return 0
  return value
}

export class CoreEventHistory implements Iterable<[string, BaseEvent]> {
  max_history_size: number | null
  max_history_drop: boolean
  private readonly bus: RustCoreEventBus

  constructor(bus: RustCoreEventBus, options: { max_history_size?: number | null; max_history_drop?: boolean } = {}) {
    this.bus = bus
    this.max_history_size = normalizeMaxHistorySize(options.max_history_size)
    this.max_history_drop = options.max_history_drop ?? false
  }

  get size(): number {
    const event_ids = this.bus.historyEventIds('*')
    this.bus.collectEvictedCompletedEventRefs(new Set(event_ids))
    return event_ids.length
  }

  [Symbol.iterator](): Iterator<[string, BaseEvent]> {
    return this.entries()
  }

  *entries(): IterableIterator<[string, BaseEvent]> {
    for (const event of this.values()) {
      yield [event.event_id, event]
    }
  }

  *keys(): IterableIterator<string> {
    for (const event of this.values()) {
      yield event.event_id
    }
  }

  *values(): IterableIterator<BaseEvent> {
    for (const event_id of [...this.bus.historyEventIds('*')].reverse()) {
      const record = this.bus.core.getEvent(event_id)
      if (record && this.bus.coreRecordBelongsToThisBus(record)) {
        yield this.bus.eventFromCoreRecord('*', record)
      }
    }
  }

  clear(): void {}

  get(event_id: string): BaseEvent | undefined {
    return this.getEvent(event_id)
  }

  set(event_id: string, event: BaseEvent): this {
    if (event.event_id !== event_id) {
      event.event_id = event_id
    }
    event.event_bus = this.bus as never
    this.bus.importEventsToCore([event], this.bus.pending_event_queue)
    this.bus.rememberLiveEvent(event_id, event)
    return this
  }

  has(event_id: string): boolean {
    const event_ids = this.bus.historyEventIds('*')
    const visible_event_ids = new Set(event_ids)
    this.bus.collectEvictedCompletedEventRefs(visible_event_ids)
    return visible_event_ids.has(event_id)
  }

  delete(event_id: string): boolean {
    void event_id
    return false
  }

  addEvent(event: BaseEvent): void {
    this.set(event.event_id, event)
  }

  getEvent(event_id: string): BaseEvent | undefined {
    if (!this.has(event_id)) {
      return undefined
    }
    const record = this.bus.core.getEvent(event_id)
    return record ? this.bus.eventFromCoreRecord('*', record) : undefined
  }

  removeEvent(event_id: string): boolean {
    return this.delete(event_id)
  }

  hasEvent(event_id: string): boolean {
    return this.has(event_id)
  }

  find(event_pattern: '*', options?: FindOptions<BaseEvent>): Promise<BaseEvent | null>
  find(event_pattern: '*', where: (event: BaseEvent) => boolean, options?: FindOptions<BaseEvent>): Promise<BaseEvent | null>
  find<T extends BaseEvent>(event_pattern: EventPattern<T>, options?: FindOptions<T>): Promise<T | null>
  find<T extends BaseEvent>(event_pattern: EventPattern<T>, where: (event: T) => boolean, options?: FindOptions<T>): Promise<T | null>
  find<T extends BaseEvent>(
    event_pattern: EventPattern<T> | '*',
    where_or_options: ((event: T) => boolean) | FindOptions<T> = {},
    maybe_options: FindOptions<T> = {}
  ): Promise<T | null> {
    return this.bus.find(event_pattern, where_or_options as never, maybe_options)
  }

  filter(event_pattern: '*', options?: FilterOptions<BaseEvent>): Promise<BaseEvent[]>
  filter(event_pattern: '*', where: (event: BaseEvent) => boolean, options?: FilterOptions<BaseEvent>): Promise<BaseEvent[]>
  filter<T extends BaseEvent>(event_pattern: EventPattern<T>, options?: FilterOptions<T>): Promise<T[]>
  filter<T extends BaseEvent>(event_pattern: EventPattern<T>, where: (event: T) => boolean, options?: FilterOptions<T>): Promise<T[]>
  filter<T extends BaseEvent>(
    event_pattern: EventPattern<T> | '*',
    where_or_options: ((event: T) => boolean) | FilterOptions<T> = {},
    maybe_options: FilterOptions<T> = {}
  ): Promise<T[]> {
    return this.bus.filter(event_pattern, where_or_options as never, maybe_options)
  }
}

export class CoreLockFacade {
  private readonly bus: RustCoreEventBus
  private paused_count = 0
  private pause_waiters: Array<() => void> = []
  private active_handler_results: EventResult[] = []
  readonly bus_event_lock = new AsyncLock(1)

  constructor(bus: RustCoreEventBus) {
    this.bus = bus
  }

  _getActiveHandlerResultForCurrentAsyncContext(): EventResult | undefined {
    const result = this.bus.activeHandlerResult() ?? undefined
    return result?.status === 'started' ? result : undefined
  }

  _getActiveHandlerResults(): EventResult[] {
    return [...this.active_handler_results]
  }

  _requestRunloopPause(): () => void {
    this.paused_count += 1
    let released = false
    return () => {
      if (released) {
        return
      }
      released = true
      this.paused_count = Math.max(0, this.paused_count - 1)
      if (this.paused_count === 0) {
        const waiters = this.pause_waiters.splice(0)
        for (const resolve of waiters) resolve()
        this.bus._onRunloopResumed()
      }
    }
  }

  _waitUntilRunloopResumed(): Promise<void> {
    if (this.paused_count === 0) {
      return Promise.resolve()
    }
    return new Promise((resolve) => this.pause_waiters.push(resolve))
  }

  _isPaused(): boolean {
    return this.paused_count > 0
  }

  _notifyIdleListeners(): void {}

  _isAnyHandlerActive(): boolean {
    return !this.bus.isIdle()
  }

  getLockForEvent(event: BaseEvent): AsyncLock | null {
    const mode = event.event_concurrency ?? this.bus.event_concurrency
    if (mode === 'parallel') {
      return null
    }
    if (mode === 'global-serial') {
      return this.bus._lock_for_event_global_serial
    }
    return this.bus_event_lock
  }

  waitForIdle(timeout: number | null = null): Promise<boolean> {
    return this.bus.waitUntilIdle(timeout)
  }

  async _runWithHandlerLock<T>(
    event: BaseEvent,
    default_concurrency: EventHandlerConcurrencyMode,
    fn: (handler_lock: HandlerLock | null) => Promise<T> | T
  ): Promise<T> {
    const lock = event._getHandlerLock(default_concurrency)
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

  async _runWithHandlerDispatchContext<T>(result: EventResult, fn: () => Promise<T> | T): Promise<T> {
    this.active_handler_results.push(result)
    try {
      return await this.bus.runWithActiveHandlerResult(result, fn)
    } finally {
      const index = this.active_handler_results.indexOf(result)
      if (index >= 0) {
        this.active_handler_results.splice(index, 1)
      }
    }
  }

  async _runWithEventLock<T>(
    event: BaseEvent,
    fn: () => Promise<T> | T,
    options: { bypass_event_locks?: boolean; pre_acquired_lock?: AsyncLock | null } = {}
  ): Promise<T> {
    if (options.bypass_event_locks || options.pre_acquired_lock) {
      return await fn()
    }
    return await runWithLock(this.getLockForEvent(event), async () => await fn())
  }

  clear(): void {
    this.paused_count = 0
    this.pause_waiters.splice(0).forEach((resolve) => resolve())
    this.active_handler_results = []
  }
}

export class RustCoreEventBus {
  readonly core: RustCoreClient
  readonly name: string
  readonly id: string
  readonly bus_id: string
  readonly label: string
  readonly event_concurrency: EventConcurrencyMode
  readonly event_handler_concurrency: EventHandlerConcurrencyMode
  readonly event_handler_completion: EventHandlerCompletionMode
  readonly event_handler_detect_file_paths: boolean
  readonly event_timeout: number | null
  readonly event_slow_timeout: number | null
  readonly event_handler_timeout: number | null
  readonly event_handler_slow_timeout: number | null

  readonly handlers: Map<string, EventHandler>
  readonly handlers_by_key: Map<string, string[]>
  readonly event_history: CoreEventHistory
  readonly locks: CoreLockFacade
  all_instances: CoreEventBusRegistry
  _lock_for_event_global_serial: AsyncLock
  in_flight_event_ids: Set<string>
  runloop_running: boolean

  private registered: boolean
  private events: Map<string, BaseEvent>
  private processing: Set<string>
  private processing_tasks: Set<Promise<unknown>>
  private middlewares: EventBusMiddleware[]
  private invocation_by_result_id: Map<string, string>
  private abort_by_invocation_id: Map<string, (error: Error, reason?: unknown) => void>
  private event_types_by_pattern: Map<string, EventPattern | '*'>
  private event_timeout_causes: Map<string, EventHandlerTimeoutError>
  private event_completion_waiters: Map<string, Array<(event: BaseEvent) => void>>
  private event_emission_waiters: Set<(event: BaseEvent | null) => void>
  private invocation_worker: ChildProcess | null
  private background_worker_enabled: boolean
  private foreground_drain_task: Promise<void> | null
  private foreground_drain_requested: boolean
  private serial_route_pause_releases: Map<string, () => void>
  private completed_event_refs: Map<string, WeakRef<BaseEvent>>
  private closed: boolean
  private readonly owns_shared_core: boolean
  private registered_max_history_size: number | null | undefined
  private registered_max_history_drop: boolean | undefined

  constructor(name = 'EventBus', options: RustCoreEventBusOptions = {}) {
    this.owns_shared_core = options.core === undefined
    this.core = options.core ?? RustCoreClient.acquireNamed(name)
    this.name = name
    this.bus_id = options.id ?? defaultCoreBusId(name, rustCoreEventBusRegistry)
    this.id = this.bus_id
    this.label = `${name}#${this.bus_id.slice(-4)}`
    this.event_concurrency = options.event_concurrency ?? 'bus-serial'
    this.event_handler_concurrency = options.event_handler_concurrency ?? 'serial'
    this.event_handler_completion = options.event_handler_completion ?? 'all'
    this.event_handler_detect_file_paths = options.event_handler_detect_file_paths ?? true
    this.event_timeout = options.event_timeout === undefined ? 60 : options.event_timeout
    this.event_slow_timeout = options.event_slow_timeout === undefined ? 300 : options.event_slow_timeout
    this.event_handler_timeout = options.event_handler_timeout ?? null
    this.event_handler_slow_timeout = options.event_handler_slow_timeout === undefined ? 30 : options.event_handler_slow_timeout
    this.handlers = new Map()
    this.handlers_by_key = new Map()
    this.in_flight_event_ids = new Set()
    this.runloop_running = false
    this.event_history = new CoreEventHistory(this, {
      max_history_size: options.max_history_size,
      max_history_drop: options.max_history_drop ?? false,
    })
    this._lock_for_event_global_serial = new AsyncLock(1)
    this.locks = new CoreLockFacade(this)
    this.all_instances = rustCoreEventBusRegistry
    this.registered = false
    this.events = new Map()
    this.processing = new Set()
    this.processing_tasks = new Set()
    this.middlewares = normalizeMiddlewares(options.middlewares)
    this.invocation_by_result_id = new Map()
    this.abort_by_invocation_id = new Map()
    this.event_types_by_pattern = new Map()
    this.event_timeout_causes = new Map()
    this.event_completion_waiters = new Map()
    this.event_emission_waiters = new Set()
    this.invocation_worker = null
    this.background_worker_enabled = options.background_worker ?? true
    this.foreground_drain_task = null
    this.foreground_drain_requested = false
    this.serial_route_pause_releases = new Map()
    this.completed_event_refs = new Map()
    this.closed = false
    this.registered_max_history_size = undefined
    this.registered_max_history_drop = undefined
    this.all_instances.add(this)
    this.dispatch = this.dispatch.bind(this)
    this.emit = this.emit.bind(this)
    this.start()
  }

  get pending_event_queue(): BaseEvent[] {
    return this.pendingQueueEventsFromCore()
  }

  get find_waiters(): Set<(event: BaseEvent) => void> {
    return this.event_emission_waiters as Set<(event: BaseEvent) => void>
  }

  set pending_event_queue(events: BaseEvent[]) {
    const pending_events = events.map((event) => event._event_original ?? event)
    for (const event of pending_events) {
      event.event_bus = this as never
    }
    if (pending_events.length > 0) {
      this.importEventsToCore(pending_events, pending_events)
    }
  }

  toString(): string {
    return this.label
  }

  defaultsRecord(): Record<string, unknown> {
    return {
      event_concurrency: this.event_concurrency,
      event_handler_concurrency: this.event_handler_concurrency,
      event_handler_completion: this.event_handler_completion,
      event_timeout: this.event_timeout,
      event_slow_timeout: this.event_slow_timeout,
      event_handler_timeout: this.event_handler_timeout,
      event_handler_slow_timeout: this.event_handler_slow_timeout,
    }
  }

  private busRecord(): Record<string, unknown> {
    return {
      bus_id: this.bus_id,
      name: this.name,
      label: this.label,
      host_id: this.core.session_id,
      defaults: this.defaultsRecord(),
      max_history_size: normalizeMaxHistorySize(this.event_history.max_history_size),
      max_history_drop: this.event_history.max_history_drop,
    }
  }

  start(): void {
    if (this.registered) {
      return
    }
    this.core.registerBus(this.busRecord())
    this.registered_max_history_size = this.event_history.max_history_size
    this.registered_max_history_drop = this.event_history.max_history_drop
    this.registered = true
  }

  private syncBusHistoryPolicy(): void {
    if (
      this.registered_max_history_size === this.event_history.max_history_size &&
      this.registered_max_history_drop === this.event_history.max_history_drop
    ) {
      return
    }
    this.core.registerBus(this.busRecord())
    this.registered_max_history_size = this.event_history.max_history_size
    this.registered_max_history_drop = this.event_history.max_history_drop
  }

  on<T extends BaseEvent>(event_type: EventPattern<T>, handler: CoreHandler<T>, options?: RustCoreHandlerOptions): EventHandler
  on(event_type: '*', handler: CoreHandler, options?: RustCoreHandlerOptions): EventHandler
  on(event_type: EventPattern | '*', handler: CoreHandler, options: RustCoreHandlerOptions = {}): EventHandler {
    if (this.closed) {
      throw new Error('EventBus is destroyed')
    }
    const event_pattern = normalizeEventPattern(event_type)
    this.event_types_by_pattern.set(event_pattern, event_type)
    const handler_entry = EventHandler.fromCallable({
      handler,
      event_pattern,
      eventbus_name: this.name,
      eventbus_id: this.bus_id,
      detect_handler_file_path: this.event_handler_detect_file_paths,
      handler_timeout: options.handler_timeout,
      handler_slow_timeout: options.handler_slow_timeout,
    })
    if (options.handler_name) {
      handler_entry.handler_name = options.handler_name
    }
    if (typeof options.id === 'string') {
      handler_entry.id = options.id
    } else if (options.handler_name) {
      handler_entry.id = EventHandler.computeHandlerId({
        eventbus_id: handler_entry.eventbus_id,
        handler_name: handler_entry.handler_name,
        handler_file_path: handler_entry.handler_file_path,
        handler_registered_at: handler_entry.handler_registered_at,
        event_pattern: handler_entry.event_pattern,
      })
    }
    if (typeof options.handler_registered_at === 'string') {
      handler_entry.handler_registered_at = options.handler_registered_at
    }
    if (typeof options.handler_file_path === 'string' || options.handler_file_path === null) {
      handler_entry.handler_file_path = options.handler_file_path
    }
    const handler_id = handler_entry.id
    this.handlers.set(handler_id, handler_entry)
    const ids = this.handlers_by_key.get(event_pattern)
    if (ids) ids.push(handler_id)
    else this.handlers_by_key.set(event_pattern, [handler_id])
    this.core.registerHandler({
      handler_id,
      bus_id: this.bus_id,
      host_id: this.core.session_id,
      event_pattern,
      handler_name: handler_entry.handler_name,
      handler_file_path: handler_entry.handler_file_path,
      handler_registered_at: handler_entry.handler_registered_at,
      handler_timeout: handler_entry.handler_timeout ?? null,
      handler_slow_timeout: handler_entry.handler_slow_timeout ?? null,
      handler_concurrency: options.handler_concurrency ?? null,
      handler_completion: options.handler_completion ?? null,
    })
    void this._onBusHandlersChange(handler_entry, true)
    if (this.background_worker_enabled) {
      this.startWorker()
    }
    return handler_entry
  }

  off<T extends BaseEvent>(event_type: EventPattern<T> | '*', handler?: CoreHandler<T> | string | EventHandler): void {
    const event_pattern = normalizeEventPattern(event_type)
    const match_by_id = typeof handler === 'string' || handler instanceof EventHandler
    const handler_id_arg = handler instanceof EventHandler ? handler.id : handler
    for (const entry of Array.from(this.handlers.values())) {
      if (entry.event_pattern !== event_pattern) {
        continue
      }
      if (handler_id_arg !== undefined && (match_by_id ? entry.id !== handler_id_arg : entry.handler !== handler_id_arg)) {
        continue
      }
      this.handlers.delete(entry.id)
      const ids = this.handlers_by_key.get(entry.event_pattern)
      if (ids) {
        const index = ids.indexOf(entry.id)
        if (index >= 0) ids.splice(index, 1)
        if (ids.length === 0) this.handlers_by_key.delete(entry.event_pattern)
      }
      this.core.unregisterHandler(entry.id)
      void this._onBusHandlersChange(entry, false)
    }
  }

  emit<T extends BaseEvent>(event: T): T
  emit(event: Record<string, unknown>): Promise<CoreMessage>
  emit<T extends BaseEvent>(event: T | Record<string, unknown>): T | Promise<CoreMessage> {
    if (this.closed) {
      throw new Error('EventBus is destroyed')
    }
    const abort_context = getActiveAbortContext()
    if (abort_context?.isAborted()) {
      throw abort_context.error() ?? new Error('Handler aborted')
    }
    if (!(event instanceof BaseEvent)) {
      const event_obj = BaseEvent.fromJSON(event)
      const emitted = this.emit(event_obj)
      return this._processEventImmediately(emitted).then((completed) => ({
        type: 'event_completed',
        event_id: completed.event_id,
      }))
    }
    const event_obj = event
    event_obj.event_bus = this as never
    const active_result = this.locks._getActiveHandlerResultForCurrentAsyncContext()
    const active_parent = active_result?.event._event_original ?? active_result?.event
    const explicit_active_child_emit =
      active_result !== undefined &&
      active_parent !== undefined &&
      event_obj.event_id !== active_parent.event_id &&
      event_obj.event_parent_id === active_parent.event_id
    const forwarded_active_child_emit =
      active_result !== undefined &&
      active_parent !== undefined &&
      active_result.eventbus_id === this.id &&
      event_obj.event_id !== active_parent.event_id &&
      event_obj.event_parent_id === null &&
      Array.isArray(active_parent.event_path) &&
      active_parent.event_path.length > 1
    const active_child_emit = explicit_active_child_emit || forwarded_active_child_emit
    if (active_result && active_child_emit && !event_obj.event_emitted_by_handler_id) {
      event_obj.event_emitted_by_handler_id = active_result.handler_id
      event_obj.event_emitted_by_result_id = active_result.id
      active_result._linkEmittedChildEvent(event_obj)
    }
    const event_concurrency = event_obj.event_concurrency ?? this.event_concurrency
    if (active_result && active_child_emit && event_concurrency !== 'parallel') {
      active_result._ensureQueueJumpPause(this as never)
    }
    if (!Array.isArray(event_obj.event_path)) {
      event_obj.event_path = []
    }
    if (event_obj._core_known && event_obj.event_path.includes(this.label)) {
      return event_obj
    }
    if (event_obj._getDispatchContext() === undefined) {
      event_obj._setDispatchContext(captureAsyncContext())
    }
    const forwarding_existing_event = event_obj._core_known
    event_obj.event_path.push(this.label)
    event_obj.event_status = 'pending'
    this.events.set(event_obj.event_id, event_obj)
    this.ensurePendingLocalResults(event_obj)
    this.in_flight_event_ids.add(event_obj.event_id)
    this._notifyEventChange(event_obj, 'pending')
    this.syncBusHistoryPolicy()
    let core_messages: CoreMessage[]
    try {
      const cross_bus_explicit_child_emit =
        active_result !== undefined && explicit_active_child_emit && active_result.eventbus_id !== this.id
      const defer_start =
        active_result !== undefined
          ? cross_bus_explicit_child_emit || (active_result.eventbus_id === this.id && explicit_active_child_emit)
          : true
      if (forwarding_existing_event) {
        core_messages = this.core.forwardEvent(event_obj.event_id, this.bus_id, defer_start, true, {
          ...this.coreForwardControlOptions(event_obj),
        })
      } else {
        core_messages = this.core.emitEvent(this.coreEventRecordForEmit(event_obj), this.bus_id, defer_start, true)
      }
    } catch (error) {
      this.events.delete(event_obj.event_id)
      this.in_flight_event_ids.delete(event_obj.event_id)
      throw error
    }
    event_obj._core_known = true
    this.notifyEventEmissionWaiters(event_obj)
    const core_started_work = core_messages.some(messageIsInvocationBatch)
    if (!active_result && !this.background_worker_enabled) {
      if (core_started_work) {
        const task = this.handleWorkerMessages(core_messages).catch(() => {
          // Explicit waits surface errors; foreground eager processing is reconciled by later waits/snapshots.
        })
        this.processing_tasks.add(task)
        task.finally(() => this.processing_tasks.delete(task))
      } else {
        for (const message of core_messages) {
          if (!messageIsInvocationBatch(message)) {
            this.applyCoreMessage(message)
          }
        }
        this.scheduleForegroundDrain()
      }
    } else if (!active_result && core_started_work) {
      const task = this.handleWorkerMessages(core_messages).catch(() => {
        // Explicit waits surface errors; the eager background pass must not
        // leak unhandled rejections during close or queue-jump handoff.
      })
      this.processing_tasks.add(task)
      task.finally(() => this.processing_tasks.delete(task))
    } else if (core_started_work) {
      const task = this.runWithoutCoreOutcomeBatch(() => this.handleWorkerMessages(core_messages)).catch(() => {
        // Core-issued handler invocations are reconciled by later waits/snapshots.
      })
      this.processing_tasks.add(task)
      task.finally(() => this.processing_tasks.delete(task))
    } else {
      for (const message of core_messages) {
        this.applyCoreMessage(message)
      }
    }
    return event_obj
  }

  private scheduleForegroundDrain(): void {
    if (this.closed || this.background_worker_enabled || this.locks._isPaused()) {
      return
    }
    if (this.foreground_drain_task) {
      this.foreground_drain_requested = true
      return
    }
    this.foreground_drain_requested = false
    const task = new Promise<void>((resolve) => {
      setTimeout(resolve, 0)
    })
      .then(() => this.drainForegroundCore())
      .catch(() => {
        // Explicit waits reconcile against core state.
      })
      .finally(() => {
        this.foreground_drain_task = null
        this.processing_tasks.delete(task)
        if (this.foreground_drain_requested) {
          this.scheduleForegroundDrain()
        }
      })
    this.foreground_drain_task = task
    this.processing_tasks.add(task)
  }

  _onRunloopResumed(): void {
    this.scheduleForegroundDrain()
  }

  private async drainForegroundCore(): Promise<void> {
    for (let i = 0; i < 1000; i += 1) {
      const progressed = await this.processAvailableCoreMessages()
      if (!progressed) {
        return
      }
    }
  }

  private async processAvailableCoreMessages(): Promise<boolean> {
    if (this.closed) {
      return false
    }
    const messages = this.core.processNextRoute(this.bus_id, CORE_ROUTE_SLICE_LIMIT, true)
    if (messages.length === 0) {
      return false
    }
    for (const message of messages) {
      if (!messageIsInvocationBatch(message)) this.applyCoreMessageToLocalBuses(message)
    }
    const invocation_messages = invocationMessagesFromCoreMessages(messages)
    if (invocation_messages.length > 0) {
      const task = this.runAndApplyInvocationMessages(invocation_messages).catch(() => {
        // Explicit waits reconcile against core state.
      })
      this.processing_tasks.add(task)
      task.finally(() => this.processing_tasks.delete(task))
    }
    return true
  }

  private coreEventRecordForEmit(event: BaseEvent): Record<string, unknown> {
    const record: Record<string, unknown> = {
      event_id: event.event_id,
      event_type: event.event_type,
      event_version: event.event_version,
      event_created_at: event.event_created_at,
      event_path: event.event_path,
    }
    if (event.event_result_type !== undefined) {
      record.event_result_type = event.event_result_type ? toJsonSchema(event.event_result_type) : event.event_result_type
    }
    if (event.event_timeout !== null) record.event_timeout = event.event_timeout
    if (event.event_slow_timeout !== undefined && event.event_slow_timeout !== null) record.event_slow_timeout = event.event_slow_timeout
    if (event.event_concurrency !== undefined && event.event_concurrency !== null) record.event_concurrency = event.event_concurrency
    if (event.event_handler_timeout !== undefined && event.event_handler_timeout !== null) {
      record.event_handler_timeout = event.event_handler_timeout
    }
    if (event.event_handler_slow_timeout !== undefined && event.event_handler_slow_timeout !== null) {
      record.event_handler_slow_timeout = event.event_handler_slow_timeout
    }
    if (event.event_handler_concurrency !== undefined && event.event_handler_concurrency !== null) {
      record.event_handler_concurrency = event.event_handler_concurrency
    }
    if (event.event_handler_completion !== undefined && event.event_handler_completion !== null) {
      record.event_handler_completion = event.event_handler_completion
    }
    if (event.event_blocks_parent_completion) record.event_blocks_parent_completion = true
    if (event.event_parent_id !== null) record.event_parent_id = event.event_parent_id
    if (event.event_emitted_by_result_id !== null) record.event_emitted_by_result_id = event.event_emitted_by_result_id
    if (event.event_emitted_by_handler_id !== null) record.event_emitted_by_handler_id = event.event_emitted_by_handler_id
    for (const [key, value] of Object.entries(event as unknown as Record<string, unknown>)) {
      if (key.startsWith('_') || key === 'bus' || key === 'event_bus' || key === 'event_results') continue
      if (CORE_EVENT_FIELD_NAMES.has(key)) continue
      if (value === undefined || typeof value === 'function') continue
      record[key] = value
    }
    return record
  }

  private coreForwardControlOptions(event: BaseEvent): Record<string, unknown> {
    const options: Record<string, unknown> = {}
    if (event.event_timeout !== undefined && event.event_timeout !== null) options.event_timeout = event.event_timeout
    if (event.event_slow_timeout !== undefined && event.event_slow_timeout !== null) options.event_slow_timeout = event.event_slow_timeout
    if (event.event_concurrency !== undefined && event.event_concurrency !== null) options.event_concurrency = event.event_concurrency
    if (event.event_handler_timeout !== undefined && event.event_handler_timeout !== null) {
      options.event_handler_timeout = event.event_handler_timeout
    }
    if (event.event_handler_slow_timeout !== undefined && event.event_handler_slow_timeout !== null) {
      options.event_handler_slow_timeout = event.event_handler_slow_timeout
    }
    if (event.event_handler_concurrency !== undefined && event.event_handler_concurrency !== null) {
      options.event_handler_concurrency = event.event_handler_concurrency
    }
    if (event.event_handler_completion !== undefined && event.event_handler_completion !== null) {
      options.event_handler_completion = event.event_handler_completion
    }
    if (event.event_blocks_parent_completion) options.event_blocks_parent_completion = true
    return options
  }

  dispatch<T extends BaseEvent>(event: T): T {
    return this.emit(event)
  }

  onEventChange(_event: BaseEvent, _status: 'pending' | 'started' | 'completed'): Promise<void> {
    return this._runMiddlewareHook('onEventChange', [this, _event, _status])
  }

  onEventResultChange(_event: BaseEvent, _result: EventResult, _status: 'pending' | 'started' | 'completed'): Promise<void> {
    return this._runMiddlewareHook('onEventResultChange', [this, _event, _result, _status])
  }

  hasMiddlewareHooks(): boolean {
    return this.hasEventChangeHooks() || this.hasEventResultHooks()
  }

  hasEventChangeHooks(): boolean {
    return this.middlewares.length > 0 || this.onEventChange !== RustCoreEventBus.prototype.onEventChange
  }

  hasEventResultHooks(): boolean {
    return this.middlewares.length > 0 || this.onEventResultChange !== RustCoreEventBus.prototype.onEventResultChange
  }

  _notifyEventChange(event: BaseEvent, status: 'pending' | 'started' | 'completed'): void {
    if (this.hasEventChangeHooks()) {
      void this.onEventChange(event, status)
    }
  }

  _notifyEventResultChange(event: BaseEvent, result: EventResult, status: 'pending' | 'started' | 'completed'): void {
    if (this.hasEventResultHooks()) {
      void this.onEventResultChange(event, result, status)
    }
  }

  findEventById(event_id: string): BaseEvent | null {
    const local = this.events.get(event_id)
    if (local) return local
    const record = this.core.getEvent(event_id)
    return record && this.coreRecordBelongsToThisBus(record) ? this.eventFromCoreRecord('*', record) : null
  }

  findLocalEventById(event_id: string): BaseEvent | undefined {
    return this.events.get(event_id)
  }

  rememberEvent(event_id: string, event: BaseEvent): void {
    if (event.event_id !== event_id) {
      event.event_id = event_id
    }
    this.importEventsToCore([event], this.pending_event_queue)
  }

  rememberLiveEvent(event_id: string, event: BaseEvent): void {
    this.events.set(event_id, event)
  }

  forgetEvent(event_id: string): void {
    this.events.delete(event_id)
  }

  localEvents(): IterableIterator<BaseEvent> {
    return this.events.values()
  }

  importEventsToCore(events: BaseEvent[], pending_events: BaseEvent[] = []): void {
    this.syncBusHistoryPolicy()
    for (const event of events) {
      const original = event._event_original ?? event
      original._core_known = true
    }
    this.core.importBusSnapshot({
      bus: this.busRecord(),
      handlers: Array.from(this.handlers.values()).map((handler) => ({
        handler_id: handler.id,
        bus_id: this.bus_id,
        host_id: this.core.session_id,
        event_pattern: handler.event_pattern,
        handler_name: handler.handler_name,
        handler_file_path: handler.handler_file_path,
        handler_registered_at: handler.handler_registered_at,
        handler_timeout: handler.handler_timeout ?? null,
        handler_slow_timeout: handler.handler_slow_timeout ?? null,
        handler_concurrency: null,
        handler_completion: null,
      })),
      events: events.map((event) => (event._event_original ?? event).toJSON()),
      pending_event_ids: pending_events.map((event) => (event._event_original ?? event).event_id),
    })
  }

  historyRecords(event_pattern = '*', limit: number | null = null): Record<string, unknown>[] {
    return this.core.listEvents(event_pattern, limit, this.bus_id).filter((record) => this.coreRecordBelongsToThisBus(record))
  }

  historyEventIds(event_pattern = '*', limit: number | null = null, statuses: string[] | null = null): string[] {
    return this.core.listEventIds(event_pattern, limit, this.bus_id, statuses)
  }

  coreRecordBelongsToThisBus(record: Record<string, unknown>): boolean {
    const event_id = record.event_id
    if (typeof event_id === 'string' && this.events.has(event_id)) {
      return true
    }
    const event_path = record.event_path
    if (Array.isArray(event_path) && event_path.some((label) => label === this.label)) {
      return true
    }
    const event_results = record.event_results
    if (event_results && typeof event_results === 'object' && !Array.isArray(event_results)) {
      for (const result of Object.values(event_results as Record<string, unknown>)) {
        if (!result || typeof result !== 'object') continue
        const result_record = result as Record<string, unknown>
        if (result_record.eventbus_id === this.bus_id || result_record.eventbus_name === this.name) {
          return true
        }
      }
    }
    return false
  }

  activeHandlerResult(): EventResult | null {
    return (core_handler_context?.getStore() as EventResult | undefined) ?? null
  }

  _getEventProxyScopedToThisBus<T extends BaseEvent>(event: T, _handler_result?: EventResult): T {
    return event
  }

  _hasProcessedEvent(event: BaseEvent): boolean {
    return event.event_path.includes(this.label) && event.event_status === 'completed'
  }

  _getHandlersForEvent(event: BaseEvent): EventHandler[] {
    const event_type = event.event_type
    return Array.from(this.handlers.values()).filter((handler) => handler.event_pattern === '*' || handler.event_pattern === event_type)
  }

  private ensurePendingLocalResults(event: BaseEvent): void {
    const original = event._event_original ?? event
    for (const handler of this._getHandlersForEvent(original)) {
      const existing = original.event_results.get(handler.id)
      if (existing) {
        existing.handler = handler
        continue
      }
      const result = new EventResult({ event: original, handler })
      original.event_results.set(handler.id, result)
      this._notifyEventResultChange(original, result, 'pending')
    }
  }

  async _processEventImmediately<T extends BaseEvent>(event: T, _handler_result?: EventResult): Promise<T> {
    const event_id = (event._event_original ?? event).event_id
    const active_result = _handler_result ?? this.locks._getActiveHandlerResultForCurrentAsyncContext()
    const active_bus = active_result ? active_result.bus : undefined
    const invocation_id = active_result
      ? (this.invocation_by_result_id.get(active_result.id) ?? active_bus?.invocation_by_result_id.get(active_result.id))
      : undefined
    if (active_result && invocation_id) {
      const block_parent_completion = this.eventIsOwnedByActiveHandler(event, active_result)
      const produced = this.core.queueJumpEvent(event_id, invocation_id, block_parent_completion)
      return (await this.runWithoutCoreOutcomeBatch(() =>
        this.applyAndRunMessagesUntilEventCompleted(event_id, produced, active_bus?.bus_id ?? this.bus_id, true)
      )) as T
    }
    const parent_invocation_id = this.findParentInvocationForEvent(event)
    if (parent_invocation_id) {
      const produced = this.core.queueJumpEvent(event_id, parent_invocation_id)
      const parent_bus = this.findBusForInvocation(parent_invocation_id) ?? this
      return (await this.runWithoutCoreOutcomeBatch(() =>
        this.applyAndRunMessagesUntilEventCompleted(event_id, produced, parent_bus.bus_id, true)
      )) as T
    }
    if (this.background_worker_enabled && this._getHandlersForEvent(event).length > 0) {
      return await this.waitForLocalCompletionPush(event)
    }
    const active_invocation_for_queue_jump = this.findActiveInvocationForQueueJump(event)
    if (active_invocation_for_queue_jump) {
      const produced = this.core.queueJumpEvent(event_id, active_invocation_for_queue_jump, false)
      return (await this.runWithoutCoreOutcomeBatch(() =>
        this.applyAndRunMessagesUntilEventCompleted(event_id, produced, this.bus_id, true)
      )) as T
    }
    if (!this.background_worker_enabled && this.processing_tasks.size > 0) {
      if (
        this.foreground_drain_task !== null &&
        this.processing_tasks.size === 1 &&
        this.processing_tasks.has(this.foreground_drain_task)
      ) {
        return (await this.processUntilEventCompleted(event_id)) as T
      }
      return await this.waitForLocalCompletionOrProcessingDrain(event)
    }
    return (await this.processUntilEventCompleted(event_id)) as T
  }

  private findActiveInvocationForQueueJump(event: BaseEvent): string | undefined {
    const original = event._event_original ?? event
    if (original.event_status === 'completed') {
      return undefined
    }
    for (const invocation_id of this.invocation_by_result_id.values()) {
      return invocation_id
    }
    for (const bus of this.localBusesForCoreSession()) {
      if (bus === this) {
        continue
      }
      if (!original.event_path.includes(bus.label)) {
        continue
      }
      for (const invocation_id of bus.invocation_by_result_id.values()) {
        return invocation_id
      }
    }
    return undefined
  }

  private waitForLocalCompletionPush<T extends BaseEvent>(event: T): Promise<T> {
    const original = event._event_original ?? event
    if (original.event_status === 'completed') {
      return Promise.resolve(this.completedSnapshotOrEvent(original) as T)
    }
    const current = this.events.get(original.event_id)
    const current_original = current?._event_original ?? current
    if (current_original?.event_status === 'completed') {
      return Promise.resolve(this.completedSnapshotOrEvent(current_original) as T)
    }
    return new Promise<T>((resolve) => {
      const waiters = this.event_completion_waiters.get(original.event_id) ?? []
      waiters.push((completed) => resolve((completed._event_original ?? completed) as T))
      this.event_completion_waiters.set(original.event_id, waiters)
    })
  }

  private async waitForLocalCompletionOrProcessingDrain<T extends BaseEvent>(event: T): Promise<T> {
    const original = event._event_original ?? event
    const current = this.events.get(original.event_id) ?? original
    if (current.event_status === 'completed') {
      return this.completedSnapshotOrEvent(current._event_original ?? current) as T
    }
    const current_tasks = Array.from(this.processing_tasks)
    if (current_tasks.length === 0) {
      return (await this.processUntilEventCompleted(original.event_id)) as T
    }
    let waiter: ((event: BaseEvent) => void) | null = null
    const completed = new Promise<T>((resolve) => {
      waiter = (completed_event) => resolve((completed_event._event_original ?? completed_event) as T)
      const waiters = this.event_completion_waiters.get(original.event_id) ?? []
      waiters.push(waiter)
      this.event_completion_waiters.set(original.event_id, waiters)
    })
    try {
      const drained = Promise.allSettled(current_tasks).then(() => ({ type: 'drained' as const }))
      const first = await Promise.race([completed.then((completed_event) => ({ type: 'completed' as const, completed_event })), drained])
      if (first.type === 'completed') {
        return this.completedSnapshotOrEvent(first.completed_event) as T
      }
      const refreshed = this.events.get(original.event_id) ?? original
      if (refreshed.event_status === 'completed') {
        return this.completedSnapshotOrEvent(refreshed._event_original ?? refreshed) as T
      }
      return (await this.processUntilEventCompleted(original.event_id)) as T
    } finally {
      if (waiter) {
        const waiters = this.event_completion_waiters.get(original.event_id) ?? []
        this.event_completion_waiters.set(
          original.event_id,
          waiters.filter((candidate) => candidate !== waiter)
        )
      }
    }
  }

  async _waitForEventCompletedInQueueOrder<T extends BaseEvent>(event: T): Promise<T> {
    const original = event._event_original ?? event
    const active_result = this.locks._getActiveHandlerResultForCurrentAsyncContext()
    const active_invocation_id = active_result ? this.invocation_by_result_id.get(active_result.id) : undefined
    const parent_invocation_id =
      active_result && active_invocation_id && this.eventIsOwnedByActiveHandler(original, active_result)
        ? active_invocation_id
        : this.findParentInvocationForEvent(original)
    if (parent_invocation_id) {
      for (const message of this.core.awaitEvent(original.event_id, parent_invocation_id)) {
        this.applyCoreMessage(message)
      }
      return (await this.runWithoutCoreOutcomeBatch(() => {
        if (!this.background_worker_enabled) {
          return this.processUntilEventCompleted(original.event_id)
        }
        return this.waitForCoreCompletedEvent(original)
      })) as T
    }
    if (!this.background_worker_enabled) {
      if (this.processing_tasks.size > 0) {
        return (await this.waitForLocalCompletionOrProcessingDrain(original)) as T
      }
      return (await this.processUntilEventCompleted(original.event_id)) as T
    }
    const wait_messages = await this.waitForEventCompleted(original.event_id)
    for (const message of wait_messages) {
      this.applyCoreMessage(message)
    }
    const snapshot_message = wait_messages.find((message) => message.type === 'event_snapshot')
    const snapshot = snapshot_message?.event as Record<string, unknown> | undefined
    if (snapshot?.event_status === 'completed') {
      this.applyCoreSnapshotToEvent(original, snapshot)
    }
    return original as T
  }

  private eventIsOwnedByActiveHandler(event: BaseEvent, active_result: EventResult): boolean {
    const original = event._event_original ?? event
    const active_parent = active_result.event._event_original ?? active_result.event
    if (original.event_emitted_by_result_id === active_result.id) {
      return true
    }
    if (
      original.event_parent_id === active_parent.event_id &&
      original.event_emitted_by_handler_id === active_result.handler_id &&
      active_result.event_children.some((child) => (child._event_original ?? child).event_id === original.event_id)
    ) {
      return true
    }
    return false
  }

  private findParentInvocationForEvent(event: BaseEvent): string | undefined {
    const original = event._event_original ?? event
    const result_id = original.event_emitted_by_result_id
    if (!result_id) {
      return undefined
    }
    for (const bus of this.all_instances) {
      const invocation_id = bus.invocation_by_result_id.get(result_id)
      if (invocation_id) {
        return invocation_id
      }
    }
    return undefined
  }

  private findBusForInvocation(invocation_id: string): RustCoreEventBus | undefined {
    for (const bus of this.localBusesForCoreSession()) {
      for (const candidate_invocation_id of bus.invocation_by_result_id.values()) {
        if (candidate_invocation_id === invocation_id) {
          return bus
        }
      }
    }
    return undefined
  }

  async processUntilEventCompleted(event_id: string, initial_messages: CoreMessage[] | null = null): Promise<BaseEvent> {
    if (this.processing.has(event_id)) {
      const event = this.events.get(event_id)
      if (!event) {
        const snapshot = this.core.getEvent(event_id)
        if (snapshot?.event_status === 'completed') {
          return this.completedSnapshotOrEvent(this.eventFromCoreRecord('*', snapshot))
        }
        throw new Error(`missing event: ${event_id}`)
      }
      return await this.waitForCoreCompletedEvent(event)
    }
    this.processing.add(event_id)
    try {
      for (let i = 0; i < 10000; i += 1) {
        const messages = initial_messages ?? this.core.processNextRoute(this.bus_id, CORE_ROUTE_SLICE_LIMIT, true)
        initial_messages = null
        const has_actionable_messages = messages.some((message) => message.type !== 'heartbeat_ack')
        for (const message of messages) {
          if (!messageIsInvocationBatch(message)) this.applyCoreMessageToLocalBuses(message)
        }
        const invocation_messages = invocationMessagesFromCoreMessages(messages)
        if (invocation_messages.length > 0) {
          await this.runAndApplyInvocationMessages(invocation_messages)
          const event = this.events.get(event_id)
          if (event?.event_status === 'completed') {
            return this.completedSnapshotOrEvent(event)
          }
        }
        const event = this.events.get(event_id)
        if (event?.event_status === 'completed') {
          return this.completedSnapshotOrEvent(event)
        }
        if (!has_actionable_messages) {
          if (!this.background_worker_enabled) {
            const snapshot = this.core.getEvent(event_id)
            if (snapshot?.event_status === 'completed') {
              if (!event) {
                return this.completedSnapshotOrEvent(this.eventFromCoreRecord('*', snapshot))
              }
              this.applyCoreSnapshotToEvent(event, snapshot)
              return this.completedSnapshotOrEvent(event)
            }
            const active_tasks = Array.from(this.processing_tasks).filter((task) => task !== this.foreground_drain_task)
            if (active_tasks.length > 0) {
              await Promise.race([
                ...active_tasks.map((task) => task.catch(() => undefined)),
                new Promise((resolve) => setTimeout(resolve, 1)),
              ])
              continue
            }
            await new Promise((resolve) => setTimeout(resolve, 1))
            continue
          }
          const wait_messages = await this.waitForEventCompleted(event_id)
          for (const message of wait_messages) {
            this.applyCoreMessage(message)
          }
          const snapshot_message = wait_messages.find((message) => message.type === 'event_snapshot')
          const snapshot = snapshot_message?.event as Record<string, unknown> | undefined
          if (snapshot?.event_status === 'completed') {
            if (!event) {
              return this.completedSnapshotOrEvent(this.eventFromCoreRecord('*', snapshot))
            }
            this.applyCoreSnapshotToEvent(event, snapshot)
            return this.completedSnapshotOrEvent(event)
          }
          throw new Error(`event wait completed without a completed snapshot: ${event_id}`)
        }
      }
      throw new Error(`event did not complete within iteration limit: ${event_id}`)
    } finally {
      this.processing.delete(event_id)
      const completed_event = this.events.get(event_id)
      if (completed_event?.event_status === 'completed') {
        this.releaseCompletedLocalEvent(completed_event)
      }
    }
  }

  private async waitForCoreCompletedEvent<T extends BaseEvent>(event: T): Promise<T> {
    const original = event._event_original ?? event
    const wait_messages = await this.waitForEventCompleted(original.event_id)
    for (const message of wait_messages) {
      this.applyCoreMessage(message)
    }
    const snapshot_message = wait_messages.find((message) => message.type === 'event_snapshot')
    const snapshot = snapshot_message?.event as Record<string, unknown> | undefined
    if (snapshot?.event_status === 'completed') {
      this.applyCoreSnapshotToEvent(original, snapshot)
    }
    return original as T
  }

  runUntilEventCompleted(event_id: string): Promise<BaseEvent> {
    return this.processUntilEventCompleted(event_id)
  }

  _syncEventRuntimeOptions(event: BaseEvent): void {
    const original = event._event_original ?? event
    const messages = this.core.updateEventOptions(original.event_id, {
      event_handler_completion: original.event_handler_completion ?? null,
      event_blocks_parent_completion: original.event_blocks_parent_completion,
    })
    for (const message of messages) {
      this.applyCoreMessage(message)
    }
  }

  async waitUntilIdle(timeout: number | null = null): Promise<boolean> {
    const deadline = timeout === null ? null : Date.now() + Math.max(0, timeout) * 1000
    for (;;) {
      if (this.processing_tasks.size === 0 && this.core.waitBusIdle(this.bus_id, 0)) {
        this.refreshKnownEventsFromCore()
        this.core.releaseTransportSoon()
        return true
      }
      let progressed = false
      const messages = this.core.processNextRoute(this.bus_id, CORE_ROUTE_SLICE_LIMIT, true)
      if (messages.length > 0) {
        progressed = true
      }
      for (const message of messages) {
        if (!messageIsInvocationBatch(message)) this.applyCoreMessage(message)
      }
      const invocation_messages = invocationMessagesFromCoreMessages(messages)
      if (invocation_messages.length > 0) {
        const invocation_task = this.runAndApplyInvocationMessages(invocation_messages)
        const tracked_task = invocation_task.catch(() => undefined)
        this.processing_tasks.add(tracked_task)
        tracked_task.finally(() => this.processing_tasks.delete(tracked_task))
        if (deadline === null) {
          await invocation_task
        } else {
          const remaining_ms = Math.max(0, deadline - Date.now())
          const completed = await Promise.race([
            tracked_task.then(() => true),
            new Promise<boolean>((resolve) => setTimeout(() => resolve(false), remaining_ms)),
          ])
          if (!completed) {
            return false
          }
        }
        progressed = true
      }
      if (this.core.waitBusIdle(this.bus_id, 0) && this.processing_tasks.size === 0) {
        this.refreshKnownEventsFromCore()
        this.core.releaseTransportSoon()
        return true
      }
      if (deadline !== null && Date.now() >= deadline) {
        return false
      }
      const active_tasks = Array.from(this.processing_tasks).filter((task) => task !== this.foreground_drain_task)
      if (active_tasks.length > 0) {
        const remaining_ms = deadline === null ? null : Math.max(0, deadline - Date.now())
        const wait_ms = remaining_ms === null ? (progressed ? 0 : 1) : Math.min(remaining_ms, progressed ? 0 : 1)
        await Promise.race([
          ...active_tasks.map((task) => task.catch(() => undefined)),
          new Promise((resolve) => setTimeout(resolve, wait_ms)),
        ])
        continue
      }
      await new Promise((resolve) => setTimeout(resolve, progressed ? 0 : 1))
    }
  }

  isIdle(): boolean {
    const records = this.core.listEvents('*')
    return records.every((record) => {
      const event = record as { event_results?: Record<string, { eventbus_id?: string; status?: string }> }
      return Object.values(event.event_results ?? {}).every(
        (result) => result.eventbus_id !== this.bus_id || (result.status !== 'pending' && result.status !== 'started')
      )
    })
  }

  isIdleAndQueueEmpty(): boolean {
    return this.isIdle()
  }

  removeEventFromPendingQueue(_event: BaseEvent): number {
    return 0
  }

  isEventInFlightOrQueued(event_id: string): boolean {
    const record = this.core.getEvent(event_id)
    return record?.event_status === 'pending' || record?.event_status === 'started'
  }

  removeEventFromHistory(event_id: string): boolean {
    void event_id
    return false
  }

  async destroy(options: number | { timeout?: number | null; clear?: boolean } = {}): Promise<void> {
    const timeout = typeof options === 'number' ? options : options.timeout === undefined ? 0 : options.timeout
    const clear = typeof options === 'number' ? true : (options.clear ?? true)
    this.closed = true
    if (timeout !== null && timeout !== undefined && timeout > 0) {
      await this.waitUntilIdle(timeout)
    }
    try {
      this.stopWorker()
    } catch {
      // The core may already have been stopped explicitly.
    }
    this.runloop_running = false
    this.event_completion_waiters.clear()
    for (const resolve of this.event_emission_waiters) {
      resolve(null as never)
    }
    this.event_emission_waiters.clear()
    for (const release of this.serial_route_pause_releases.values()) {
      release()
    }
    this.serial_route_pause_releases.clear()
    this.all_instances.discard(this)
    if (!clear) {
      return
    }
    try {
      this.core.unregisterBus(this.bus_id)
    } catch {
      // The core may already have been stopped explicitly.
    }
    try {
      if (this.owns_shared_core) {
        RustCoreClient.releaseNamed(this.core)
      } else {
        this.core.disconnectHost()
        this.core.disconnect()
      }
    } catch {
      // The core transport may already be closed.
    }
    this.handlers.clear()
    this.handlers_by_key.clear()
    this.events.clear()
    this.processing.clear()
    this.processing_tasks.clear()
    this.in_flight_event_ids.clear()
    this.completed_event_refs.clear()
    this.locks.clear()
  }

  eventIsChildOf(child_event: BaseEvent, parent_event: BaseEvent): boolean {
    if (child_event.event_id === parent_event.event_id) {
      return false
    }
    const target_child_id = child_event.event_id
    const visited_children = new Set<string>()
    const children_stack = [...parent_event.event_children]
    while (children_stack.length > 0) {
      const child = children_stack.pop()
      if (!child) {
        continue
      }
      const child_id = (child._event_original ?? child).event_id
      if (child_id === target_child_id) {
        return true
      }
      if (visited_children.has(child_id)) {
        continue
      }
      visited_children.add(child_id)
      children_stack.push(...child.event_children)
    }
    let current_parent_id = child_event.event_parent_id
    while (current_parent_id) {
      if (current_parent_id === parent_event.event_id) {
        return true
      }
      const parent = this.findEventById(current_parent_id)
      if (!parent) {
        return false
      }
      current_parent_id = parent.event_parent_id
    }
    return false
  }

  eventIsParentOf(parent_event: BaseEvent, child_event: BaseEvent): boolean {
    return this.eventIsChildOf(child_event, parent_event)
  }

  logTree(): string {
    return logTree(this)
  }

  private async applyAndRunMessagesUntilEventCompleted(
    event_id: string,
    messages: CoreMessage[],
    preferred_bus_id: string | null = null,
    wait_for_local_push = false
  ): Promise<BaseEvent> {
    const local_event_before_run = this.events.get(event_id)
    for (const message of messages) {
      if (!messageIsInvocationBatch(message)) this.applyCoreMessageToLocalBuses(message)
    }
    const invocation_messages = invocationMessagesFromCoreMessages(messages)
    if (invocation_messages.length > 0) {
      await this.runInvocationMessagesInCoreRouteOrder(invocation_messages, preferred_bus_id)
      if (wait_for_local_push) {
        const event = this.events.get(event_id) ?? local_event_before_run
        if (event) {
          return await this.waitForLocalCompletionPush(event)
        }
      }
      return this.processUntilEventCompleted(event_id)
    }
    if (wait_for_local_push) {
      const event = this.events.get(event_id) ?? local_event_before_run
      if (event) {
        if ((event._event_original ?? event).event_status === 'completed') {
          return this.completedSnapshotOrEvent(event._event_original ?? event)
        }
        if (this.background_worker_enabled) {
          return await this.waitForLocalCompletionPush(event)
        }
        return this.processUntilEventCompleted(event_id)
      }
    }
    return this.processUntilEventCompleted(event_id)
  }

  private async runInvocationMessagesInCoreRouteOrder(
    invocations: CoreMessage[],
    preferred_bus_id: string | null = null
  ): Promise<CoreMessage[]> {
    const produced_messages: CoreMessage[] = []
    const route_groups: CoreMessage[][] = []
    const route_group_by_id = new Map<string, CoreMessage[]>()
    for (let index = 0; index < invocations.length; index += 1) {
      const message = invocations[index]
      const route_id = typeof message.route_id === 'string' ? message.route_id : `__invocation_${index}`
      let group = route_group_by_id.get(route_id)
      if (!group) {
        group = []
        route_group_by_id.set(route_id, group)
        route_groups.push(group)
      }
      group.push(message)
    }

    if (preferred_bus_id) {
      route_groups.sort((left, right) => {
        const left_matches = left.some((message) => message.bus_id === preferred_bus_id)
        const right_matches = right.some((message) => message.bus_id === preferred_bus_id)
        return Number(right_matches) - Number(left_matches)
      })
    }

    for (const group of route_groups) {
      const produced_groups = await Promise.all(
        group.map((message) => this.busForInvocation(message).runInvocation(message, { signalWorker: true }))
      )
      for (const produced of produced_groups) {
        for (const entry of produced) {
          produced_messages.push(entry)
          this.applyCoreMessageToLocalBuses(entry)
        }
      }
    }
    return produced_messages
  }

  private completedSnapshotOrEvent(event: BaseEvent): BaseEvent {
    this.core.releaseTransportSoon()
    this.cancelFirstModeLosersForCompletedEvent(event)
    this.releaseCompletedLocalEvent(event)
    return event
  }

  private releaseCompletedLocalEvent(event: BaseEvent): void {
    const original = event._event_original ?? event
    if (original.event_status !== 'completed') {
      return
    }
    if (this.processing.has(original.event_id)) {
      return
    }
    if (Array.from(original.event_results.values()).some((result) => result.result_is_event_reference)) {
      const snapshot = this.core.getEvent(original.event_id)
      if (snapshot) {
        this.applyCoreSnapshotToEvent(original, snapshot)
      }
    }
    this.trackLocalEventUntilCoreEvictsIt(original)
    this.events.delete(original.event_id)
    this.in_flight_event_ids.delete(original.event_id)
  }

  private trackLocalEventUntilCoreEvictsIt(event: BaseEvent): void {
    if (!this.event_history.max_history_drop || this.event_history.max_history_size === null) {
      return
    }
    if (this.completed_event_refs.get(event.event_id)?.deref()) {
      return
    }
    this.completed_event_refs.set(event.event_id, new WeakRef(event))
    const max_history_size = Math.max(1, this.event_history.max_history_size)
    const cleanup_threshold = Math.max(max_history_size * 2, max_history_size + 64)
    if (max_history_size <= 1 || this.completed_event_refs.size > cleanup_threshold) {
      this.collectEvictedCompletedEventRefs()
    }
  }

  collectEvictedCompletedEventRefs(visible_event_ids: Set<string> | null = null): void {
    if (this.completed_event_refs.size === 0) {
      return
    }
    visible_event_ids ??= new Set(this.historyEventIds('*'))
    for (const [event_id, event_ref] of this.completed_event_refs.entries()) {
      const event = event_ref.deref()
      if (!event) {
        this.completed_event_refs.delete(event_id)
        continue
      }
      if (!visible_event_ids.has(event_id)) {
        if (event.event_status !== 'completed') {
          const snapshot = this.core.getEvent(event_id)
          if (snapshot?.event_status !== 'completed') {
            continue
          }
        }
        event._gc()
        this.completed_event_refs.delete(event_id)
      }
    }
  }

  private pendingQueueEventsFromCore(): BaseEvent[] {
    const pending: BaseEvent[] = []
    for (const event_id of this.core.listPendingEventIds(this.bus_id)) {
      const record = this.core.getEvent(event_id)
      if (!record || !this.coreRecordBelongsToThisBus(record)) {
        continue
      }
      const event = this.events.get(event_id)
      if (event) {
        this.applyCoreSnapshotToEvent(event._event_original ?? event, record)
        pending.push(event)
      } else {
        const restored = this.eventFromCoreRecord('*', record)
        this.applyCoreSnapshotToEvent(restored, record)
        pending.push(restored)
      }
    }
    return pending
  }

  private refreshPendingQueueFromCore(): void {
    void this.pendingQueueEventsFromCore()
  }

  private refreshKnownEventsFromCore(): void {
    for (const event of this.events.values()) {
      const original = event._event_original ?? event
      if (original.event_status === 'completed') {
        continue
      }
      const snapshot = this.core.getEvent(original.event_id)
      if (snapshot) {
        this.applyCoreSnapshotToEvent(original, snapshot)
      }
    }
    this.refreshPendingQueueFromCore()
  }

  private applyCoreSnapshotToEvent(event: BaseEvent, snapshot: Record<string, unknown>): void {
    const snapshot_event = BaseEvent.fromJSON(snapshot)
    event._core_known = true
    const event_results = snapshot.event_results
    if (event_results && typeof event_results === 'object' && !Array.isArray(event_results)) {
      for (const result of this.orderedResultRecords(event_results as Record<string, unknown>)) {
        this.applyResultSnapshot(event, result)
      }
    }
    this.cancelFirstModeLosersForCompletedEvent(event)
    event.event_started_at = snapshot_event.event_started_at
    event.event_completed_at = snapshot_event.event_completed_at
    event.event_parent_id = snapshot_event.event_parent_id
    event.event_emitted_by_handler_id = snapshot_event.event_emitted_by_handler_id
    event.event_emitted_by_result_id = snapshot_event.event_emitted_by_result_id
    event.event_blocks_parent_completion = snapshot_event.event_blocks_parent_completion
    event.event_path = snapshot_event.event_path
    if (snapshot_event.event_status === 'completed') {
      event._markCompleted(true, false)
    } else if (snapshot_event.event_status === 'started') {
      event._markStarted(snapshot_event.event_started_at, false)
    }
  }

  async find<T extends BaseEvent>(
    event_type: EventPattern<T> | '*',
    where_or_options: ((event: T) => boolean) | FindOptions<T> = {},
    maybe_options: FindOptions<T> = {}
  ): Promise<T | null> {
    const options = typeof where_or_options === 'function' ? maybe_options : where_or_options
    const limit_field_value = Object.prototype.hasOwnProperty.call(options, 'limit') ? options.limit : undefined
    const { limit: _ignored_limit, ...rest } = options
    void _ignored_limit
    const has_where_filter = typeof where_or_options === 'function'
    const where = has_where_filter ? where_or_options : () => true
    const effective_where =
      limit_field_value === undefined
        ? where
        : (event: T) => (event as unknown as Record<string, unknown>).limit === limit_field_value && where(event)
    const results =
      !has_where_filter && limit_field_value === undefined
        ? await this.filter(event_type, { ...rest, limit: 1 })
        : await this.filter(event_type, effective_where, { ...rest, limit: 1 })
    return results[0] ?? null
  }

  async filter<T extends BaseEvent>(
    event_type: EventPattern<T> | '*',
    where_or_options: ((event: T) => boolean) | FilterOptions<T> = {},
    maybe_options: FilterOptions<T> = {}
  ): Promise<T[]> {
    if (this.closed) {
      throw new Error('EventBus is destroyed')
    }
    const has_where_filter = typeof where_or_options === 'function'
    const where = has_where_filter ? where_or_options : () => true
    const options = typeof where_or_options === 'function' ? maybe_options : where_or_options
    const limit = typeof options.limit === 'number' ? options.limit : null
    if (limit !== null && limit <= 0) {
      return []
    }
    const future = options.future ?? false
    const past = options.past ?? true
    if (past === false && future === false) {
      return []
    }
    const event_pattern = normalizeEventPattern(event_type)
    const child_of = options.child_of ?? null
    const cutoff_at = past === true || past === false ? null : new Date(Date.now() - Math.max(0, Number(past)) * 1000).toISOString()
    const field_filters = Object.entries(options).filter(
      ([key, value]) => key !== 'limit' && key !== 'past' && key !== 'future' && key !== 'child_of' && value !== undefined
    )
    const can_limit_core_history =
      limit !== null && cutoff_at === null && child_of === null && field_filters.length === 0 && !has_where_filter
    const records = past === false ? [] : this.historyRecords(event_pattern, can_limit_core_history ? limit : null)
    const matches: T[] = []
    for (const record of records) {
      const event = this.eventFromCoreRecord(event_type, record) as T
      if (cutoff_at !== null && this.timestampBefore(event.event_created_at, cutoff_at)) {
        continue
      }
      if (child_of && !this.eventIsChildOf(event, child_of)) {
        continue
      }
      if (!field_filters.every(([field_name, expected]) => (event as unknown as Record<string, unknown>)[field_name] === expected)) {
        continue
      }
      if (!where(event)) {
        continue
      }
      matches.push(event)
      if (limit !== null && matches.length >= limit) {
        break
      }
    }
    if (future !== false && (limit === null || matches.length < limit)) {
      const future_match = await this.waitForFutureMatch(event_type, where, options)
      if (future_match) {
        matches.push(future_match as T)
      }
    }
    return matches
  }

  disconnect(): void {
    if (this.closed) {
      return
    }
    this.closed = true
    this.stopWorker()
    this.all_instances.discard(this)
    try {
      this.core.unregisterBus(this.bus_id)
    } catch {
      // Best-effort cleanup; the core may already have dropped this bus.
    }
    try {
      if (!this.owns_shared_core) {
        this.core.disconnectHost()
      }
    } catch {
      // The core may already have been stopped explicitly.
    }
    if (this.owns_shared_core) {
      RustCoreClient.releaseNamed(this.core)
    } else {
      this.core.disconnect()
    }
  }

  close(): void {
    this.disconnect()
  }

  stop(): void {
    if (this.closed) {
      return
    }
    this.closed = true
    try {
      try {
        this.core.disconnectHost()
      } catch {
        // The core may already be stopping.
      }
      this.stopWorker()
      try {
        this.core.unregisterBus(this.bus_id)
      } catch {
        // Best-effort cleanup before stopping the core.
      }
      if (this.owns_shared_core) {
        RustCoreClient.releaseNamed(this.core, { stopCore: true })
      } else {
        this.core.stop()
      }
    } finally {
      this.invocation_worker = null
    }
  }

  scheduleMicrotask(fn: () => void): void {
    if (typeof queueMicrotask === 'function') {
      queueMicrotask(fn)
      return
    }
    void Promise.resolve().then(fn)
  }

  toJSON(): RustCoreEventBusJSON {
    const handlers: Record<string, EventHandlerJSON> = {}
    for (const [handler_id, handler] of this.handlers.entries()) {
      handlers[handler_id] = handler.toJSON()
    }
    const handlers_by_key: Record<string, string[]> = {}
    for (const [key, ids] of this.handlers_by_key.entries()) {
      handlers_by_key[key] = [...ids]
    }
    const event_history: Record<string, BaseEventJSON> = {}
    for (const [event_id, event] of this.event_history.entries()) {
      event_history[event_id] = event.toJSON()
    }
    const payload: RustCoreEventBusJSON = {
      id: this.id,
      name: this.name,
      max_history_size: this.event_history.max_history_size,
      max_history_drop: this.event_history.max_history_drop,
      event_concurrency: this.event_concurrency,
      event_timeout: this.event_timeout,
      event_slow_timeout: this.event_slow_timeout,
      event_handler_concurrency: this.event_handler_concurrency,
      event_handler_completion: this.event_handler_completion,
      event_handler_timeout: this.event_handler_timeout,
      event_handler_slow_timeout: this.event_handler_slow_timeout,
      event_handler_detect_file_paths: this.event_handler_detect_file_paths,
      handlers,
      handlers_by_key,
      event_history,
      pending_event_queue: this.pending_event_queue.map((event) => (event._event_original ?? event).event_id),
    }
    return payload
  }

  eventFromCoreRecord<T extends BaseEvent>(event_type: EventPattern<T> | '*', record: Record<string, unknown>): BaseEvent {
    const event_id = record.event_id
    if (typeof event_id === 'string') {
      const local = this.events.get(event_id)
      if (local) {
        this.applyCoreSnapshotToEvent(local._event_original ?? local, record)
        return local
      }
    }
    if (event_type !== '*' && typeof event_type !== 'string') {
      const fromJSON = (event_type as unknown as { fromJSON?: (data: unknown) => BaseEvent }).fromJSON
      if (typeof fromJSON === 'function') {
        const event = fromJSON(record)
        event.event_bus = this as never
        this.applyCoreSnapshotToEvent(event, record)
        this.notifyEventEmissionWaiters(event)
        return event
      }
    }
    const event = BaseEvent.fromJSON(record)
    event.event_bus = this as never
    this.applyCoreSnapshotToEvent(event, record)
    this.notifyEventEmissionWaiters(event)
    return event
  }

  private isStaleInvocationOutcomeError(error: unknown): boolean {
    if (!(error instanceof Error)) {
      return false
    }
    const message = error.message.toLowerCase()
    return message.includes('stale invocation outcome') || message.includes('missing result')
  }

  private eventSnapshotMessageForInvocation(invocation: CoreMessage): CoreMessage[] {
    const event_id = typeof invocation.event_id === 'string' ? invocation.event_id : null
    if (!event_id) return []
    const local = this.events.get(event_id)
    return local ? [{ type: 'event_snapshot', event: (local._event_original ?? local).toJSON() }] : []
  }

  private completeHandlerOrSnapshot(
    invocation: CoreMessage,
    value: unknown,
    options: {
      result_is_event_reference?: boolean
      process_route_after?: boolean
      process_available_after?: boolean
      compact_response?: boolean
    }
  ): CoreMessage[] {
    try {
      return this.core.completeHandler(invocation, value, options)
    } catch (error) {
      if (this.isStaleInvocationOutcomeError(error)) {
        return this.eventSnapshotMessageForInvocation(invocation)
      }
      throw error
    }
  }

  private completeHandlerNoPatchesOrSnapshot(
    invocation: CoreMessage,
    value: unknown,
    options: {
      result_is_event_reference?: boolean
      process_route_after?: boolean
      process_available_after?: boolean
      compact_response?: boolean
    }
  ): CoreMessage[] {
    try {
      return this.core.completeHandlerNoPatches(invocation, value, options)
    } catch (error) {
      if (this.isStaleInvocationOutcomeError(error)) {
        return this.eventSnapshotMessageForInvocation(invocation)
      }
      throw error
    }
  }

  private canUsePatchlessCompletedOutcome(
    invocation: CoreMessage,
    event: BaseEvent,
    result: EventResult,
    returned_event: BaseEvent | null
  ): boolean {
    if (returned_event !== null || result.result_is_event_reference) {
      return false
    }
    if (typeof invocation.bus_id !== 'string' || invocation.bus_id !== this.bus_id) {
      return false
    }
    if ((event._event_original ?? event).event_handler_completion === 'first') {
      return false
    }
    const snapshot = invocation.event_snapshot
    const snapshot_record =
      snapshot && typeof snapshot === 'object' && !Array.isArray(snapshot) ? (snapshot as Record<string, unknown>) : null
    if (snapshot_record?.event_handler_completion === 'first') {
      return false
    }
    return result.status === 'completed'
  }

  private errorHandlerOrSnapshot(
    invocation: CoreMessage,
    error: unknown,
    options: { process_route_after?: boolean; process_available_after?: boolean; compact_response?: boolean }
  ): CoreMessage[] {
    try {
      return this.core.errorHandler(invocation, error, options)
    } catch (core_error) {
      if (this.isStaleInvocationOutcomeError(core_error)) {
        return this.eventSnapshotMessageForInvocation(invocation)
      }
      throw core_error
    }
  }

  private activeCoreOutcomeBatch(): CoreOutcomeBatch | null {
    return (core_outcome_batch_context?.getStore() as CoreOutcomeBatch | undefined) ?? null
  }

  private runWithCoreOutcomeBatch<T>(batch: CoreOutcomeBatch, fn: () => T): T {
    if (!core_outcome_batch_context) {
      return fn()
    }
    return core_outcome_batch_context.run(batch, fn)
  }

  private runWithoutCoreOutcomeBatch<T>(fn: () => T): T {
    if (!core_outcome_batch_context) {
      return fn()
    }
    return core_outcome_batch_context.run(undefined, fn)
  }

  private localOutcomePatchMessages(
    invocation: CoreMessage,
    handler_entry: EventHandler,
    event: BaseEvent,
    result: EventResult,
    outcome: CoreMessage
  ): CoreMessage[] {
    const outcome_status = typeof outcome.status === 'string' ? outcome.status : 'completed'
    const result_status = outcome_status === 'errored' ? 'error' : 'completed'
    const patch_type = outcome_status === 'errored' ? 'result_cancelled' : 'result_completed'
    const result_id = typeof invocation.result_id === 'string' ? invocation.result_id : result.id
    const result_record: Record<string, unknown> = {
      result_id,
      event_id: event.event_id,
      handler_id: handler_entry.id,
      handler_name: handler_entry.handler_name,
      handler_file_path: handler_entry.handler_file_path,
      handler_timeout: handler_entry.handler_timeout ?? null,
      handler_slow_timeout: handler_entry.handler_slow_timeout ?? null,
      timeout: result.timeout,
      handler_registered_at: handler_entry.handler_registered_at,
      handler_event_pattern: handler_entry.event_pattern,
      eventbus_name: handler_entry.eventbus_name,
      eventbus_id: handler_entry.eventbus_id,
      started_at: result.started_at,
      completed_at: result.completed_at,
      status: result_status,
      result: result_status === 'completed' ? outcome.value : null,
      error: result_status === 'error' ? outcome.error : null,
      result_is_event_reference: outcome.result_is_event_reference === true,
      result_is_undefined: outcome.result_is_undefined === true,
      event_children: result.event_children.map((child) => (child._event_original ?? child).event_id),
    }
    const completed_at = result.completed_at ?? event.event_completed_at ?? monotonicDatetime()
    const event_started_at = event.event_started_at ?? completed_at
    return [
      { type: 'patch', patch: { type: patch_type, result: result_record } },
      { type: 'patch', patch: { type: 'event_completed_compact', event_id: event.event_id, completed_at, event_started_at } },
    ]
  }

  private commitInvocationOutcomeMessages(
    invocation: CoreMessage,
    handler_entry: EventHandler,
    event: BaseEvent,
    result: EventResult,
    outcome: CoreMessage,
    outcome_record: CoreMessage,
    active_batch: CoreOutcomeBatch | null,
    owned_batch: CoreOutcomeBatch | null,
    direct_commit: () => CoreMessage[]
  ): CoreMessage[] {
    const invocation_route_id = typeof invocation.route_id === 'string' ? invocation.route_id : null
    if (active_batch && active_batch.route_id !== null && active_batch.route_id === invocation_route_id) {
      active_batch.outcomes.push(outcome_record)
      return this.localOutcomePatchMessages(invocation, handler_entry, event, result, outcome)
    }
    const pending_outcomes = owned_batch?.outcomes ?? []
    if (pending_outcomes.length > 0) {
      return this.core.completeHandlerOutcomes([...pending_outcomes, outcome_record], { compact_response: true })
    }
    return direct_commit()
  }

  private invocationCanUseBatchedOutcome(invocation: CoreMessage): boolean {
    const handler_id = typeof invocation.handler_id === 'string' ? invocation.handler_id : null
    const handler_entry = handler_id ? this.handlers.get(handler_id) : undefined
    if (handler_entry?.event_pattern === '*') {
      return false
    }
    const snapshot = invocation.event_snapshot
    const snapshot_record =
      snapshot && typeof snapshot === 'object' && !Array.isArray(snapshot) ? (snapshot as Record<string, unknown>) : null
    const event_id = typeof invocation.event_id === 'string' ? invocation.event_id : null
    const local_event = event_id ? this.events.get(event_id) : undefined
    const local_original = local_event?._event_original ?? local_event
    if (local_original?.event_handler_completion === 'first') {
      return false
    }
    if (typeof (local_event?._event_original ?? local_event)?.event_timeout === 'number') {
      return false
    }
    if (typeof snapshot_record?.event_timeout === 'number') {
      return false
    }
    return (snapshot_record?.event_handler_completion ?? this.event_handler_completion) !== 'first'
  }

  private invocationUsesFirstCompletion(invocation: CoreMessage, event: BaseEvent): boolean {
    const snapshot = invocation.event_snapshot
    const snapshot_record =
      snapshot && typeof snapshot === 'object' && !Array.isArray(snapshot) ? (snapshot as Record<string, unknown>) : null
    return (
      ((event._event_original ?? event).event_handler_completion ??
        snapshot_record?.event_handler_completion ??
        this.event_handler_completion) === 'first'
    )
  }

  private eventForInvocation(invocation: CoreMessage, handler_entry: EventHandler): BaseEvent {
    const event_id = typeof invocation.event_id === 'string' ? invocation.event_id : null
    if (!event_id) {
      throw new Error(`missing event: ${String(invocation.event_id)}`)
    }
    const local = this.events.get(event_id)
    if (local) {
      return local
    }
    const snapshot = invocation.event_snapshot
    const record =
      snapshot && typeof snapshot === 'object' && !Array.isArray(snapshot)
        ? (snapshot as Record<string, unknown>)
        : this.core.getEvent(event_id)
    if (!record) {
      throw new Error(`missing event: ${event_id}`)
    }
    const event = this.eventFromCoreRecord(
      this.event_types_by_pattern.get(handler_entry.event_pattern) ?? handler_entry.event_pattern,
      record
    )
    this.events.set(event.event_id, event)
    return event
  }

  private async runInvocationOutcome(invocation: CoreMessage, options: { signalWorker?: boolean } = {}): Promise<CoreMessage | null> {
    const handler_entry = this.handlers.get(invocation.handler_id as string)
    if (!handler_entry) {
      if (this.closed) return null
      throw new Error(`missing handler: ${String(invocation.handler_id)}`)
    }
    const event = this.eventForInvocation(invocation, handler_entry)
    const event_was_pending = event.event_status === 'pending'
    const snapshot = invocation.event_snapshot
    const snapshot_record =
      snapshot && typeof snapshot === 'object' && !Array.isArray(snapshot) ? (snapshot as Record<string, unknown>) : null
    const event_started_at = typeof snapshot_record?.event_started_at === 'string' ? snapshot_record.event_started_at : null
    event._markStarted(event_started_at, false)
    if (event_was_pending && event.event_status === 'started') {
      this._notifyEventChange(event, 'started')
    }
    let result = event.event_results.get(handler_entry.id)
    const created_result = !result
    if (!result) {
      result = new EventResult({
        event,
        handler: handler_entry,
        id: typeof invocation.result_id === 'string' ? invocation.result_id : undefined,
      })
      event.event_results.set(handler_entry.id, result)
    } else {
      result.handler = handler_entry
      if (typeof invocation.result_id === 'string') {
        result.id = invocation.result_id
      }
    }
    if (!created_result) {
      result.timeout = result.resolveEffectiveTimeout()
    }
    if (created_result) {
      this._notifyEventResultChange(event, result, 'pending')
    }
    if (typeof invocation.invocation_id === 'string') {
      this.invocation_by_result_id.set(result.id, invocation.invocation_id)
    }
    const invocation_id = typeof invocation.invocation_id === 'string' ? invocation.invocation_id : null
    const invocation_result_id = invocation_id ? result.id : null
    let foreground_signal_timer: ReturnType<typeof setTimeout> | null = null
    let foreground_event_timeout_timer: CoalescedTimerHandle | null = null
    let foreground_event_slow_timer: CoalescedTimerHandle | null = null
    if (invocation_id) {
      this.abort_by_invocation_id.set(invocation_id, (error: Error, reason?: unknown) => {
        error = this.invocationAbortError(error, reason, result)
        if (result.status === 'pending') {
          if (error instanceof EventHandlerAbortedError) {
            result._signalAbort(error)
          } else {
            result._markError(error, false)
          }
          return
        }
        result._signalAbort(error)
      })
    }
    try {
      foreground_event_timeout_timer =
        !this.background_worker_enabled || options.signalWorker ? this.startForegroundEventTimeoutTimer(invocation, event, result) : null
      foreground_event_slow_timer =
        !this.background_worker_enabled || options.signalWorker ? this.startForegroundEventSlowWarningTimer(invocation, event) : null
      await this.locks._runWithHandlerLock(event, this.event_handler_concurrency, async (handler_lock) => {
        foreground_signal_timer =
          invocation_id && (!this.background_worker_enabled || options.signalWorker)
            ? this.startForegroundCoreSignalTimer(invocation, event)
            : null
        await result.runHandler(handler_lock)
      })
    } catch (error) {
      result._markError(error, false)
    } finally {
      if (foreground_signal_timer) {
        clearTimeout(foreground_signal_timer)
      }
      cancelCoalescedTimer(foreground_event_timeout_timer)
      cancelCoalescedTimer(foreground_event_slow_timer)
      if (invocation_id) {
        this.releaseSerialRoutePausesForInvocation(invocation_id)
      }
      if (invocation_id) {
        this.abort_by_invocation_id.delete(invocation_id)
      }
      if (invocation_result_id) {
        this.invocation_by_result_id.delete(invocation_result_id)
      }
    }
    const returned_event = result.result instanceof BaseEvent ? (result.result._event_original ?? result.result) : null
    const outcome =
      result.status === 'error' && result.error !== undefined
        ? this.core.erroredHandlerOutcome(result.error)
        : this.core.completedHandlerOutcome(returned_event ? returned_event.event_id : result.result, returned_event !== null)
    return this.core.handlerOutcomeRecord(invocation, outcome, {
      process_available_after: this.processAvailableAfterHandlerOutcome(invocation),
    })
  }

  private async runInvocation(invocation: CoreMessage, options: { signalWorker?: boolean } = {}): Promise<CoreMessage[]> {
    const handler_entry = this.handlers.get(invocation.handler_id as string)
    if (!handler_entry) throw new Error(`missing handler: ${String(invocation.handler_id)}`)
    const event = this.eventForInvocation(invocation, handler_entry)
    const event_was_pending = event.event_status === 'pending'
    const snapshot = invocation.event_snapshot
    const snapshot_record =
      snapshot && typeof snapshot === 'object' && !Array.isArray(snapshot) ? (snapshot as Record<string, unknown>) : null
    const event_started_at = typeof snapshot_record?.event_started_at === 'string' ? snapshot_record.event_started_at : null
    event._markStarted(event_started_at, false)
    if (event_was_pending && event.event_status === 'started') {
      this._notifyEventChange(event, 'started')
    }
    let result = event.event_results.get(handler_entry.id)
    const created_result = !result
    if (!result) {
      result = new EventResult({
        event,
        handler: handler_entry,
        id: typeof invocation.result_id === 'string' ? invocation.result_id : undefined,
      })
      event.event_results.set(handler_entry.id, result)
    } else {
      result.handler = handler_entry
      if (typeof invocation.result_id === 'string') {
        result.id = invocation.result_id
      }
    }
    if (!created_result) {
      result.timeout = result.resolveEffectiveTimeout()
    }
    if (created_result) {
      this._notifyEventResultChange(event, result, 'pending')
    }
    if (typeof invocation.invocation_id === 'string') {
      this.invocation_by_result_id.set(result.id, invocation.invocation_id)
    }
    let messages: CoreMessage[]
    const invocation_id = typeof invocation.invocation_id === 'string' ? invocation.invocation_id : null
    const invocation_result_id = invocation_id ? result.id : null
    let foreground_signal_timer: ReturnType<typeof setTimeout> | null = null
    let foreground_event_timeout_timer: CoalescedTimerHandle | null = null
    let foreground_event_slow_timer: CoalescedTimerHandle | null = null
    if (invocation_id) {
      this.abort_by_invocation_id.set(invocation_id, (error: Error, reason?: unknown) => {
        error = this.invocationAbortError(error, reason, result)
        if (result.status === 'pending') {
          if (error instanceof EventHandlerAbortedError) {
            result._signalAbort(error)
          } else {
            result._markError(error, false)
          }
          return
        }
        result._signalAbort(error)
      })
    }
    const active_batch = this.activeCoreOutcomeBatch()
    const owned_batch: CoreOutcomeBatch | null =
      active_batch || this.invocationUsesFirstCompletion(invocation, event)
        ? null
        : { outcomes: [], route_id: typeof invocation.route_id === 'string' ? invocation.route_id : null }
    try {
      foreground_event_timeout_timer =
        !this.background_worker_enabled || options.signalWorker ? this.startForegroundEventTimeoutTimer(invocation, event, result) : null
      foreground_event_slow_timer =
        !this.background_worker_enabled || options.signalWorker ? this.startForegroundEventSlowWarningTimer(invocation, event) : null
      const run_handler = async (): Promise<void> => {
        await this.locks._runWithHandlerLock(event, this.event_handler_concurrency, async (handler_lock) => {
          if (this.applyExpiredEventTimeout(invocation, event, result)) {
            return
          }
          foreground_signal_timer =
            invocation_id && (!this.background_worker_enabled || options.signalWorker)
              ? this.startForegroundCoreSignalTimer(invocation, event)
              : null
          await result.runHandler(handler_lock)
        })
      }
      if (owned_batch) {
        await this.runWithCoreOutcomeBatch(owned_batch, run_handler)
      } else {
        await run_handler()
      }
      if (result.status === 'error' && result.error !== undefined) {
        const outcome = this.core.erroredHandlerOutcome(result.error)
        const outcome_record = this.core.handlerOutcomeRecord(invocation, outcome, {
          process_available_after: this.processAvailableAfterHandlerOutcome(invocation),
        })
        messages = this.commitInvocationOutcomeMessages(
          invocation,
          handler_entry,
          event,
          result,
          outcome,
          outcome_record,
          active_batch,
          owned_batch,
          () =>
            this.errorHandlerOrSnapshot(invocation, result.error, {
              process_route_after: true,
              process_available_after: this.processAvailableAfterHandlerOutcome(invocation),
              compact_response: true,
            })
        )
      } else {
        const returned_event = result.result instanceof BaseEvent ? (result.result._event_original ?? result.result) : null
        const outcome = this.core.completedHandlerOutcome(returned_event ? returned_event.event_id : result.result, returned_event !== null)
        const outcome_record = this.core.handlerOutcomeRecord(invocation, outcome, {
          process_available_after: this.processAvailableAfterHandlerOutcome(invocation),
        })
        const process_available_after = this.processAvailableAfterHandlerOutcome(invocation)
        const can_use_patchless =
          active_batch === null &&
          (owned_batch?.outcomes.length ?? 0) === 0 &&
          this.canUsePatchlessCompletedOutcome(invocation, event, result, returned_event)
        if (can_use_patchless) {
          messages = this.completeHandlerNoPatchesOrSnapshot(invocation, result.result, {
            process_route_after: true,
            process_available_after,
            compact_response: true,
          })
        } else {
          messages = this.commitInvocationOutcomeMessages(
            invocation,
            handler_entry,
            event,
            result,
            outcome,
            outcome_record,
            active_batch,
            owned_batch,
            () =>
              this.completeHandlerOrSnapshot(invocation, returned_event ? returned_event.event_id : result.result, {
                result_is_event_reference: returned_event !== null,
                process_route_after: true,
                process_available_after,
                compact_response: true,
              })
          )
        }
      }
    } catch (error) {
      result._markError(error, false)
      const outcome = this.core.erroredHandlerOutcome(error)
      const outcome_record = this.core.handlerOutcomeRecord(invocation, outcome, {
        process_available_after: this.processAvailableAfterHandlerOutcome(invocation),
      })
      messages = this.commitInvocationOutcomeMessages(
        invocation,
        handler_entry,
        event,
        result,
        outcome,
        outcome_record,
        active_batch,
        owned_batch,
        () =>
          this.errorHandlerOrSnapshot(invocation, error, {
            process_route_after: true,
            process_available_after: this.processAvailableAfterHandlerOutcome(invocation),
            compact_response: true,
          })
      )
    } finally {
      if (foreground_signal_timer) {
        clearTimeout(foreground_signal_timer)
      }
      cancelCoalescedTimer(foreground_event_timeout_timer)
      cancelCoalescedTimer(foreground_event_slow_timer)
      if (invocation_id) {
        this.releaseSerialRoutePausesForInvocation(invocation_id)
      }
      if (invocation_id) {
        this.abort_by_invocation_id.delete(invocation_id)
      }
      if (invocation_result_id) {
        this.invocation_by_result_id.delete(invocation_result_id)
      }
    }
    for (const message of messages) {
      if (messageIsInvocationBatch(message)) {
        continue
      }
      this.applyCoreMessageToLocalBuses(message)
    }
    const preferred_bus_id = typeof invocation.bus_id === 'string' ? invocation.bus_id : null
    const invocation_messages = invocationMessagesFromCoreMessages(messages).sort((left, right) => {
      if (!preferred_bus_id) return 0
      return Number(right.bus_id === preferred_bus_id) - Number(left.bus_id === preferred_bus_id)
    })
    await this.runInvocationMessagesInCoreRouteOrder(invocation_messages, preferred_bus_id)
    const routes_with_progress = new Set<string>()
    const compact_event_completed = messages.some((message) => {
      if (message.type !== 'patch') return false
      const patch = message.patch as { type?: string }
      return patch.type === 'event_completed_compact'
    })
    const invocation_event_completed = messages.some((message) => {
      if (message.type !== 'patch') return false
      const patch = message.patch as { type?: string; event_id?: string }
      return (patch.type === 'event_completed' || patch.type === 'event_completed_compact') && patch.event_id === invocation.event_id
    })
    for (const message of messages) {
      const invocations = invocationMessagesFromCoreMessage(message)
      if (invocations.length > 0) {
        for (const invocation_message of invocations) {
          if (typeof invocation_message.route_id === 'string') {
            routes_with_progress.add(invocation_message.route_id)
          }
        }
        continue
      }
      if (message.type !== 'patch') continue
      const patch = message.patch as { type?: string; route_id?: string }
      if ((patch.type === 'route_completed' || patch.type === 'event_completed') && typeof patch.route_id === 'string') {
        routes_with_progress.add(patch.route_id)
      }
    }
    for (const message of messages) {
      if (message.type !== 'patch') continue
      const patch = message.patch as { type?: string; result?: { route_id?: string } }
      const route_id = patch.result?.route_id
      if (compact_event_completed && route_id) {
        routes_with_progress.add(route_id)
      }
      if (patch.type === 'result_completed' && route_id && !routes_with_progress.has(route_id) && !invocation_event_completed) {
        let followups: CoreMessage[]
        try {
          followups = this.core.processRoute(route_id, CORE_ROUTE_SLICE_LIMIT, true)
        } catch (error) {
          if (error instanceof Error && error.message.toLowerCase().includes('missing route')) {
            const event_id = typeof invocation.event_id === 'string' ? invocation.event_id : null
            followups = event_id ? this.eventSnapshotMessageForInvocation({ event_id }) : []
          } else {
            throw error
          }
        }
        for (const followup of followups) {
          if (!messageIsInvocationBatch(followup)) {
            this.applyCoreMessageToLocalBuses(followup)
          }
        }
        const followup_invocations = invocationMessagesFromCoreMessages(followups).sort((left, right) => {
          if (!preferred_bus_id) return 0
          return Number(right.bus_id === preferred_bus_id) - Number(left.bus_id === preferred_bus_id)
        })
        await this.runInvocationMessagesInCoreRouteOrder(followup_invocations, preferred_bus_id)
      }
    }
    return []
  }

  private startForegroundCoreSignalTimer(invocation: CoreMessage, event: BaseEvent): ReturnType<typeof setTimeout> | null {
    const deadlines: number[] = []
    const deadline_at = this.timestampMs(invocation.deadline_at)
    if (Number.isFinite(deadline_at)) {
      deadlines.push(deadline_at)
    }
    const snapshot = invocation.event_snapshot
    const snapshot_record =
      snapshot && typeof snapshot === 'object' && !Array.isArray(snapshot) ? (snapshot as Record<string, unknown>) : null
    const started_at = typeof snapshot_record?.event_started_at === 'string' ? snapshot_record.event_started_at : event.event_started_at
    const started_at_ms = this.timestampMs(started_at)
    const snapshot_timeout = this.resolveTimeoutOverride(snapshot_record?.event_timeout)
    const local_timeout = this.resolveTimeoutOverride((event._event_original ?? event).event_timeout)
    const event_timeout = snapshot_timeout !== undefined ? snapshot_timeout : local_timeout !== undefined ? local_timeout : null
    if (Number.isFinite(started_at_ms) && event_timeout !== null) {
      deadlines.push(started_at_ms + (event_timeout + this.eventTimeoutIpcGraceSeconds(event_timeout)) * 1000)
    }
    const now = Date.now()
    const deadline = deadlines.sort((left, right) => left - right)[0]
    if (deadline === undefined) {
      return null
    }
    const delay_ms = Math.max(0, deadline - now)
    const timer = setTimeout(() => {
      try {
        const messages = this.core.processNextRoute(this.bus_id, 0)
        for (const message of messages) {
          this.applyCoreMessageToLocalBuses(message)
        }
      } catch {
        // The normal handler outcome path will reconcile with the core snapshot.
      }
    }, delay_ms)
    timer.unref?.()
    return timer
  }

  private applyExpiredEventTimeout(invocation: CoreMessage, event: BaseEvent, result: EventResult): boolean {
    const timeout = this.eventTimeoutForInvocation(invocation, event)
    if (timeout === null) {
      return false
    }
    if (timeout <= FAST_EVENT_DEADLINE_ABORT_THRESHOLD_SECONDS) {
      this.abortResultForEventTimeout(event, result, timeout)
      return true
    }
    const started_at = this.startedAtForInvocation(invocation, event)
    const started_at_ms = this.timestampMs(started_at)
    if (!Number.isFinite(started_at_ms) || Date.now() < started_at_ms + timeout * 1000) {
      return false
    }
    this.abortResultForEventTimeout(event, result, timeout)
    return true
  }

  private startForegroundEventTimeoutTimer(invocation: CoreMessage, event: BaseEvent, result: EventResult): CoalescedTimerHandle | null {
    const event_timeout = this.eventTimeoutForInvocation(invocation, event)
    if (event_timeout === null) {
      return null
    }
    const started_at = this.startedAtForInvocation(invocation, event)
    const started_at_ms = this.timestampMs(started_at)
    if (!Number.isFinite(started_at_ms)) {
      return null
    }
    const delay_ms = Math.max(0, started_at_ms + event_timeout * 1000 - Date.now())
    if (delay_ms === 0) {
      this.abortResultForEventTimeout(event, result, event_timeout)
      return null
    }
    return scheduleCoalescedTimer(delay_ms, () => {
      if (result.status === 'completed') {
        return
      }
      this.abortResultForEventTimeout(event, result, event_timeout)
    })
  }

  private startedAtForInvocation(invocation: CoreMessage, event: BaseEvent): string | null {
    const snapshot = invocation.event_snapshot
    const snapshot_record =
      snapshot && typeof snapshot === 'object' && !Array.isArray(snapshot) ? (snapshot as Record<string, unknown>) : null
    return typeof snapshot_record?.event_started_at === 'string' ? snapshot_record.event_started_at : event.event_started_at
  }

  private eventTimeoutForInvocation(invocation: CoreMessage, event: BaseEvent): number | null {
    const snapshot = invocation.event_snapshot
    const snapshot_record =
      snapshot && typeof snapshot === 'object' && !Array.isArray(snapshot) ? (snapshot as Record<string, unknown>) : null
    const snapshot_timeout = this.resolveTimeoutOverride(snapshot_record?.event_timeout)
    if (snapshot_timeout !== undefined) return snapshot_timeout
    const local_timeout = this.resolveTimeoutOverride((event._event_original ?? event).event_timeout)
    if (local_timeout !== undefined) return local_timeout
    const bus_timeout = this.resolveTimeoutOverride(this.event_timeout)
    return bus_timeout ?? null
  }

  private startForegroundEventSlowWarningTimer(invocation: CoreMessage, event: BaseEvent): CoalescedTimerHandle | null {
    const event_slow_timeout = this.eventSlowTimeoutForInvocation(invocation, event)
    if (event_slow_timeout === null) {
      return null
    }
    const started_at = this.startedAtForInvocation(invocation, event)
    const started_at_ms = this.timestampMs(started_at)
    if (!Number.isFinite(started_at_ms)) {
      return null
    }
    const delay_ms = Math.max(0, started_at_ms + event_slow_timeout * 1000 - Date.now())
    return scheduleCoalescedTimer(delay_ms, () => {
      if ((event._event_original ?? event).event_status !== 'completed') {
        this.warnSlowEvent((event._event_original ?? event).event_id)
      }
    })
  }

  private eventSlowTimeoutForInvocation(invocation: CoreMessage, event: BaseEvent): number | null {
    const snapshot = invocation.event_snapshot
    const snapshot_record =
      snapshot && typeof snapshot === 'object' && !Array.isArray(snapshot) ? (snapshot as Record<string, unknown>) : null
    const snapshot_timeout = this.resolveTimeoutOverride(snapshot_record?.event_slow_timeout)
    if (snapshot_timeout !== undefined) return snapshot_timeout
    const local_timeout = this.resolveTimeoutOverride((event._event_original ?? event).event_slow_timeout)
    if (local_timeout !== undefined) return local_timeout
    const bus_timeout = this.resolveTimeoutOverride(this.event_slow_timeout)
    return bus_timeout ?? null
  }

  private abortResultForEventTimeout(event: BaseEvent, result: EventResult, event_timeout: number): void {
    if (result.status === 'completed' || result.status === 'error') {
      return
    }
    const original = event._event_original ?? event
    const cause = this.eventTimeoutCause(original.event_id, result, { timeout: event_timeout })
    original._cancelPendingChildProcessing(cause)
    for (const event_result of original.event_results.values()) {
      if (event_result.status === 'pending') {
        const cancelled_error = new EventHandlerCancelledError(`Cancelled pending handler: event timeout after ${event_timeout}s`, {
          event_result,
          timeout_seconds: event_timeout,
          cause,
        })
        event_result._markError(cancelled_error, false)
      } else if (event_result.status === 'started') {
        event_result._lock?.exitHandlerRun()
        const aborted_error = new EventHandlerAbortedError(`Aborted running handler: event timeout after ${event_timeout}s`, {
          event_result,
          timeout_seconds: event_timeout,
          cause,
        })
        event_result._markError(aborted_error, false)
        event_result._signalAbort(aborted_error)
      }
    }
  }

  private invocationAbortError(error: Error, reason: unknown, result: EventResult): Error {
    if (reason === 'first-result-won') {
      const cause = new Error("event_handler_completion='first' resolved: another handler returned a result first")
      if (result.status === 'pending') {
        return new EventHandlerCancelledError("Cancelled: event_handler_completion='first' resolved", {
          event_result: result,
          cause,
        })
      }
      return new EventHandlerAbortedError("Aborted: event_handler_completion='first' resolved", {
        event_result: result,
        cause,
      })
    }
    return error
  }

  private eventTimeoutIpcGraceSeconds(timeout: number): number {
    if (timeout >= 0.1) {
      return 0.03
    }
    return Math.min(timeout * 0.1, 0.005)
  }

  private processAvailableAfterHandlerOutcome(invocation: CoreMessage): boolean {
    const route_id = typeof invocation.route_id === 'string' ? invocation.route_id : null
    const invocation_id = typeof invocation.invocation_id === 'string' ? invocation.invocation_id : null
    if (route_id && invocation_id && this.serial_route_pause_releases.has(`${route_id}:${invocation_id}`)) {
      return false
    }
    return true
  }

  private releaseSerialRoutePausesForInvocation(invocation_id: string): void {
    for (const [key, release] of Array.from(this.serial_route_pause_releases.entries())) {
      if (!key.endsWith(`:${invocation_id}`)) {
        continue
      }
      this.serial_route_pause_releases.delete(key)
      release()
    }
  }

  private applyResultSnapshot(event: BaseEvent, raw_result: unknown): void {
    if (!raw_result || typeof raw_result !== 'object' || Array.isArray(raw_result)) {
      return
    }
    const result_record = raw_result as Record<string, unknown>
    const result = EventResult.fromJSON(event, result_record)
    result.event = event
    this.normalizeResultError(result)
    const existing = event.event_results.get(result.handler_id)
    if (existing) {
      const invocation_id = this.invocation_by_result_id.get(existing.id) ?? this.invocation_by_result_id.get(result.id)
      if (result.status === 'error' && result.error instanceof Error && invocation_id) {
        this.abort_by_invocation_id.get(invocation_id)?.(result.error)
      }
      existing.id = result.id
      existing.status = result.status
      existing.started_at = result.started_at
      existing.completed_at = result.completed_at
      existing.result = result.result
      existing.result_is_event_reference = result.result_is_event_reference
      existing.timeout = result.timeout
      existing.error = result.error
      this.retargetErrorResult(existing.error, existing)
      this.attachResultChildren(existing, this.childIdsFromResultRecord(result_record))
      event.event_results.set(existing.handler_id, existing)
      return
    }
    this.attachResultChildren(result, this.childIdsFromResultRecord(result_record))
    event.event_results.set(result.handler_id, result)
  }

  private retargetErrorResult(error: unknown, result: EventResult): void {
    if (!error || typeof error !== 'object') {
      return
    }
    const error_record = error as { event_result?: EventResult; cause?: unknown }
    if ('event_result' in error_record) {
      error_record.event_result = result
    }
    if (error_record.cause instanceof Error && error_record.cause !== error) {
      this.retargetErrorResult(error_record.cause, result)
    }
  }

  private orderedResultRecords(event_results: Record<string, unknown>): Record<string, unknown>[] {
    return Object.values(event_results)
      .filter((result): result is Record<string, unknown> => Boolean(result) && typeof result === 'object' && !Array.isArray(result))
      .sort((left, right) => {
        const left_seq = typeof left.handler_seq === 'number' ? left.handler_seq : Number.MAX_SAFE_INTEGER
        const right_seq = typeof right.handler_seq === 'number' ? right.handler_seq : Number.MAX_SAFE_INTEGER
        if (left_seq !== right_seq) return left_seq - right_seq
        const left_registered_at = typeof left.handler_registered_at === 'string' ? left.handler_registered_at : ''
        const right_registered_at = typeof right.handler_registered_at === 'string' ? right.handler_registered_at : ''
        if (left_registered_at !== right_registered_at) return left_registered_at < right_registered_at ? -1 : 1
        const left_id = typeof left.handler_id === 'string' ? left.handler_id : ''
        const right_id = typeof right.handler_id === 'string' ? right.handler_id : ''
        return left_id < right_id ? -1 : left_id > right_id ? 1 : 0
      })
  }

  private childIdsFromResultRecord(record: Record<string, unknown>): string[] {
    const children = record.event_children
    if (!Array.isArray(children)) {
      return []
    }
    return children.filter((child_id): child_id is string => typeof child_id === 'string')
  }

  private attachResultChildren(result: EventResult, child_ids: string[]): void {
    if (child_ids.length === 0) {
      return
    }
    const existing = new Set(result.event_children.map((child) => (child._event_original ?? child).event_id))
    const parent_event = result.event._event_original ?? result.event
    for (const child_id of child_ids) {
      if (child_id === parent_event.event_id || existing.has(child_id)) {
        continue
      }
      let child = this.events.get(child_id)
      if (!child) {
        const child_snapshot = this.core.getEvent(child_id)
        if (!child_snapshot) {
          continue
        }
        const child_type = typeof child_snapshot.event_type === 'string' ? child_snapshot.event_type : '*'
        child = this.eventFromCoreRecord(this.event_types_by_pattern.get(child_type) ?? '*', child_snapshot)
        this.applyCoreSnapshotToEvent(child, child_snapshot)
      }
      result.event_children.push(child)
      existing.add(child_id)
    }
  }

  private normalizeResultError(result: EventResult): void {
    const error = result.error
    const restored_error = this.restoreCoreError(result, error)
    if (restored_error) {
      result.error = restored_error
    }
  }

  private restoreCoreError(result: EventResult, error: unknown, result_record?: Record<string, unknown>): unknown {
    if (!error) {
      return error
    }
    if (error instanceof Error) {
      return error
    }

    const error_record = error && typeof error === 'object' && !Array.isArray(error) ? (error as Record<string, unknown>) : {}
    const kind = typeof error_record.kind === 'string' ? error_record.kind : null
    const message =
      typeof error_record.message === 'string'
        ? this.publicCoreErrorMessage(error_record.message, kind)
        : this.publicCoreErrorMessage(String(error), kind)
    const details =
      error_record.details && typeof error_record.details === 'object' ? (error_record.details as Record<string, unknown>) : {}
    const timeout_seconds = this.timeoutSecondsForCoreError(result, kind, result_record, details)

    if (kind === 'schema_error') {
      return new EventHandlerResultSchemaError(message, {
        event_result: result,
        timeout_seconds,
        cause: new Error(message),
        raw_value: details.raw_value,
      })
    }
    if (kind === 'handler_timeout') {
      if (details.name === 'RetryTimeoutError' || details.cause_name === 'RetryTimeoutError') {
        const retry_timeout = new RetryTimeoutError(typeof details.cause_message === 'string' ? details.cause_message : message, {
          timeout_seconds:
            this.numberFromUnknown(details.cause_timeout_seconds) ??
            this.numberFromUnknown(details.timeout_seconds) ??
            timeout_seconds ??
            0,
          attempt: this.numberFromUnknown(details.cause_attempt) ?? this.numberFromUnknown(details.attempt) ?? 1,
        })
        return new EventHandlerTimeoutError(message, {
          event_result: result,
          timeout_seconds: timeout_seconds ?? retry_timeout.timeout_seconds,
          cause: retry_timeout,
        })
      }
      return new EventHandlerTimeoutError(message, {
        event_result: result,
        timeout_seconds,
      })
    }
    if (kind === 'handler_aborted') {
      return new EventHandlerAbortedError(message, {
        event_result: result,
        timeout_seconds: this.eventTimeoutSecondsForResult(result, result_record),
        cause: this.causeForCancellation(result, message, result_record),
      })
    }
    if (kind === 'handler_cancelled') {
      return new EventHandlerCancelledError(message, {
        event_result: result,
        timeout_seconds: this.eventTimeoutSecondsForResult(result, result_record),
        cause: this.causeForCancellation(result, message, result_record),
      })
    }
    if (kind === 'host_error' && details.name === 'RetryTimeoutError') {
      const retry_timeout = new RetryTimeoutError(message, {
        timeout_seconds: this.numberFromUnknown(details.timeout_seconds) ?? timeout_seconds ?? 0,
        attempt: this.numberFromUnknown(details.attempt) ?? 1,
      })
      return new EventHandlerTimeoutError(message, {
        event_result: result,
        timeout_seconds: retry_timeout.timeout_seconds,
        cause: retry_timeout,
      })
    }

    const lowered = message.toLowerCase()
    if (lowered.includes('cancelled')) {
      return new EventHandlerCancelledError(message, {
        event_result: result,
        timeout_seconds: this.eventTimeoutSecondsForResult(result, result_record),
        cause: this.causeForCancellation(result, message, result_record),
      })
    }
    if (lowered.includes('aborted')) {
      return new EventHandlerAbortedError(message, {
        event_result: result,
        timeout_seconds: this.eventTimeoutSecondsForResult(result, result_record),
        cause: this.causeForCancellation(result, message, result_record),
      })
    }
    if (lowered.includes('timed out') || lowered.includes('timeout')) {
      return new EventHandlerTimeoutError(message, {
        event_result: result,
        timeout_seconds,
      })
    }

    const restored = this.restoreGenericError(message, typeof details.name === 'string' ? details.name : null)
    return restored
  }

  private restoreGenericError(message: string, name: string | null): Error {
    switch (name) {
      case 'AggregateError':
        return new AggregateError([], message)
      case 'EvalError':
        return new EvalError(message)
      case 'RangeError':
        return new RangeError(message)
      case 'ReferenceError':
        return new ReferenceError(message)
      case 'SyntaxError':
        return new SyntaxError(message)
      case 'TypeError':
        return new TypeError(message)
      case 'URIError':
        return new URIError(message)
      default: {
        const restored = new Error(message)
        if (name) {
          restored.name = name
        }
        return restored
      }
    }
  }

  private publicCoreErrorMessage(message: string, kind: string | null): string {
    if ((kind === 'handler_aborted' || kind === 'handler_cancelled') && message.includes('event timed out')) {
      return message.replace('event timed out', 'event timeout')
    }
    if ((kind === 'handler_aborted' || kind === 'handler_cancelled') && message === 'first result resolved') {
      return kind === 'handler_aborted'
        ? "Aborted: event_handler_completion='first' resolved"
        : "Cancelled: event_handler_completion='first' resolved"
    }
    return message
  }

  private causeForCancellation(result: EventResult, message: string, result_record?: Record<string, unknown>): Error {
    const lowered = message.toLowerCase()
    if (!lowered.includes('timeout') && !lowered.includes('timed out')) {
      return new Error(message)
    }
    const parent_event_id = this.parentTimeoutEventIdFromMessage(message)
    if (parent_event_id) {
      return this.eventTimeoutCause(parent_event_id, result, result_record)
    }
    return this.eventTimeoutCause((result.event._event_original ?? result.event).event_id, result, result_record)
  }

  private parentTimeoutEventIdFromMessage(message: string): string | null {
    const match = message.match(/parent event ([0-9a-fA-F-]{8,}) timed out/)
    return match?.[1] ?? null
  }

  private eventTimeoutCause(event_id: string, result: EventResult, result_record?: Record<string, unknown>): EventHandlerTimeoutError {
    const existing = this.event_timeout_causes.get(event_id)
    if (existing) {
      return existing
    }
    const event = this.events.get(event_id)
    const timeout_seconds =
      this.numberFromUnknown((event?._event_original ?? event)?.event_timeout) ??
      this.numberFromUnknown((event?._event_original ?? event)?.event_bus?.event_timeout) ??
      this.numberFromUnknown(result_record?.timeout) ??
      this.numberFromUnknown(result.event.event_bus?.event_timeout) ??
      this.numberFromUnknown(result.event.event_timeout) ??
      null
    const event_type = (event?._event_original ?? event)?.event_type ?? result.event.event_type
    const message = `event timeout after ${timeout_seconds ?? 'unknown'}s for ${event_type}#${event_id.slice(-4)}`
    const timeout_error = new EventHandlerTimeoutError(message, {
      event_result: result,
      timeout_seconds,
      cause: new Error(message),
    })
    this.event_timeout_causes.set(event_id, timeout_error)
    return timeout_error
  }

  private timeoutSecondsForCoreError(
    result: EventResult,
    kind: string | null,
    result_record?: Record<string, unknown>,
    details: Record<string, unknown> = {}
  ): number | null {
    return (
      this.numberFromUnknown(details.timeout_seconds) ??
      this.numberFromUnknown(result_record?.timeout) ??
      (kind === 'handler_timeout' ? result.handler_timeout : this.eventTimeoutSecondsForResult(result, result_record))
    )
  }

  private eventTimeoutSecondsForResult(result: EventResult, result_record?: Record<string, unknown>): number | null {
    const original = result.event._event_original ?? result.event
    const local_timeout = this.resolveTimeoutOverride(original.event_timeout)
    if (local_timeout !== undefined) return local_timeout
    const original_bus_timeout = this.resolveTimeoutOverride(original.event_bus?.event_timeout)
    if (original_bus_timeout !== undefined) return original_bus_timeout
    const record_timeout = this.numberFromUnknown(result_record?.timeout)
    if (record_timeout !== null) return record_timeout
    const result_bus_timeout = this.resolveTimeoutOverride(result.event.event_bus?.event_timeout)
    if (result_bus_timeout !== undefined) return result_bus_timeout
    return result.handler_timeout
  }

  private resolveTimeoutOverride(value: unknown): number | null | undefined {
    if (typeof value !== 'number' || !Number.isFinite(value)) {
      return undefined
    }
    return value > 0 ? value : null
  }

  private positiveTimeoutFromUnknown(value: unknown): number | null {
    const number = this.numberFromUnknown(value)
    return number !== null && number > 0 ? number : null
  }

  private numberFromUnknown(value: unknown): number | null {
    return typeof value === 'number' && Number.isFinite(value) ? value : null
  }

  private timestampMs(value: unknown): number {
    if (typeof value !== 'string') {
      return Number.NaN
    }
    const parsed = Date.parse(value)
    if (Number.isFinite(parsed)) {
      return parsed
    }
    return Date.parse(value.replace(/\.(\d{3})\d+(Z|[+-]\d{2}:\d{2})$/, '.$1$2'))
  }

  private timestampBefore(left: unknown, right: unknown): boolean {
    if (typeof left !== 'string' || typeof right !== 'string') {
      return false
    }
    const left_ms = this.timestampMs(left)
    const right_ms = this.timestampMs(right)
    if (Number.isFinite(left_ms) && Number.isFinite(right_ms)) {
      return left_ms < right_ms
    }
    return left < right
  }

  private warnSlowEvent(event_id: string | undefined): void {
    if (!event_id) {
      return
    }
    let event = this.events.get(event_id)
    if (!event) {
      const snapshot = this.core.getEvent(event_id)
      if (snapshot && this.coreRecordBelongsToThisBus(snapshot)) {
        event = this.eventFromCoreRecord('*', snapshot)
        this.applyCoreSnapshotToEvent(event, snapshot)
      }
    }
    if (!event) {
      return
    }
    const running_handler_count = [...event.event_results.values()].filter((result) => result.status === 'started').length
    const started_at = event.event_started_at ?? event.event_created_at
    const elapsed_ms = Math.max(0, Date.now() - this.timestampMs(started_at))
    const elapsed_seconds = (elapsed_ms / 1000).toFixed(2)
    console.warn(
      `[abxbus] Slow event processing: ${this.name}.on(${event.event_type}#${event.event_id.slice(-4)}, ${running_handler_count} handlers) still running after ${elapsed_seconds}s`
    )
  }

  private warnSlowResult(result_id: string | undefined): void {
    if (!result_id) {
      return
    }
    for (const event of this.events.values()) {
      for (const result of event.event_results.values()) {
        if (result.id !== result_id) {
          continue
        }
        const started_at = result.started_at ?? event.event_started_at ?? event.event_created_at
        const elapsed_ms = Math.max(0, Date.now() - this.timestampMs(started_at))
        const elapsed_seconds = (elapsed_ms / 1000).toFixed(1)
        console.warn(
          `[abxbus] Slow event handler: ${result.handler.eventbus_name}.on(${event.toString()}, ${result.handler.toString()}) still running after ${elapsed_seconds}s`
        )
        return
      }
    }
  }

  runWithActiveHandlerResult<T>(result: EventResult, fn: () => T): T {
    if (!core_handler_context) {
      return fn()
    }
    return core_handler_context.run(result, fn)
  }

  private startWorker(): void {
    if (this.invocation_worker) {
      return
    }
    const worker = spawnWaitProcess({
      coreClientUrl: new URL('./CoreClient.ts', import.meta.url).href,
      busName: this.name,
      busId: this.bus_id,
      sessionId: this.core.session_id,
      lastPatchSeq: this.core.getPatchSeq(),
      mode: 'invocations',
    })
    worker.on('message', (messages: CoreMessage[]) => {
      void this.handleWorkerMessages(this.core.filterUnseenPatchMessages(messages))
    })
    worker.on('exit', () => {
      if (this.invocation_worker === worker) {
        this.invocation_worker = null
      }
    })
    worker.unref()
    this.invocation_worker = worker
  }

  private stopWorker(): void {
    const worker = this.invocation_worker
    if (!worker) {
      return
    }
    this.invocation_worker = null
    let exited = false
    worker.once('exit', () => {
      exited = true
    })
    try {
      worker.send({ type: 'close' })
    } catch {
      worker.kill()
      return
    }
    if (!exited) {
      worker.kill()
    }
  }

  private async handleWorkerMessages(messages: CoreMessage[]): Promise<void> {
    if (this.closed) {
      return
    }
    for (const message of messages) {
      if (!messageIsInvocationBatch(message)) this.applyCoreMessageToLocalBuses(message)
    }
    await this.runAndApplyInvocationMessages(invocationMessagesFromCoreMessages(messages))
  }

  private async runAndApplyInvocationMessages(invocations: CoreMessage[]): Promise<void> {
    let pending = invocations
    const seen_invocations = new Set<string>()
    while (pending.length > 0) {
      const current = pending.filter((message) => {
        const invocation_id = typeof message.invocation_id === 'string' ? message.invocation_id : null
        if (!invocation_id) {
          return true
        }
        if (seen_invocations.has(invocation_id)) {
          return false
        }
        seen_invocations.add(invocation_id)
        return true
      })
      pending = []
      if (current.length === 0) {
        continue
      }
      const produced = await this.runInvocationMessages(current)
      pending = []
      for (const message of produced) {
        const nested_invocations = invocationMessagesFromCoreMessage(message)
        if (nested_invocations.length > 0) {
          pending.push(...nested_invocations)
          continue
        }
        this.applyCoreMessageToLocalBuses(message)
      }
    }
  }

  private async runInvocationMessages(invocations: CoreMessage[]): Promise<CoreMessage[]> {
    const produced_messages: CoreMessage[] = []
    while (invocations.length > 0) {
      const invocation_buses = invocations.map((message) => this.busForInvocation(message))
      const can_batch =
        invocations.length > 1 && invocations.every((message, index) => invocation_buses[index].invocationCanUseBatchedOutcome(message))
      if (can_batch) {
        const current_invocations = invocations
        const current_invocation_buses = invocation_buses
        const bus_ids = new Set(
          current_invocations
            .map((message) => (typeof message.bus_id === 'string' ? message.bus_id : null))
            .filter((bus_id): bus_id is string => bus_id !== null)
        )
        const outcomes = (
          await Promise.all(current_invocations.map((message, index) => current_invocation_buses[index].runInvocationOutcome(message)))
        ).filter((outcome): outcome is CoreMessage => outcome !== null)
        invocations = []
        if (outcomes.length > 0) {
          produced_messages.push(...this.core.completeHandlerOutcomes(outcomes, { compact_response: true }))
          for (const bus_id of bus_ids) {
            produced_messages.push(...this.core.processNextRoute(bus_id, CORE_ROUTE_SLICE_LIMIT, true))
          }
        }
      } else {
        const pending_invocations = invocations.map((message, index) => ({ work: invocation_buses[index].runInvocation(message) }))
        invocations = []
        while (pending_invocations.length > 0) {
          const settled = await Promise.race(pending_invocations.map(({ work }, index) => work.then((produced) => ({ index, produced }))))
          pending_invocations.splice(settled.index, 1)
          for (const message of settled.produced) {
            produced_messages.push(message)
          }
        }
      }
    }
    return produced_messages
  }

  private resolveEventCompletionWaiters(event: BaseEvent): void {
    const original = event._event_original ?? event
    const waiters = this.event_completion_waiters.get(original.event_id)
    if (waiters && waiters.length > 0) {
      this.event_completion_waiters.delete(original.event_id)
      for (const resolve of waiters) {
        resolve(original)
      }
    }
    this.releaseCompletedLocalEvent(original)
  }

  private notifyEventEmissionWaiters(event: BaseEvent): void {
    const original = event._event_original ?? event
    for (const notify of Array.from(this.event_emission_waiters)) {
      notify(original)
    }
  }

  private waitForLocalEventEmission<T extends BaseEvent>(
    matches: (event: BaseEvent) => event is T
  ): {
    promise: Promise<T | null>
    cancel: () => void
  } {
    let waiter: ((event: BaseEvent | null) => void) | null = null
    const promise = new Promise<T | null>((resolve) => {
      waiter = (event: BaseEvent | null) => {
        if (event === null) {
          if (waiter) {
            this.event_emission_waiters.delete(waiter)
            waiter = null
          }
          resolve(null)
          return
        }
        if (!matches(event)) {
          return
        }
        if (waiter) {
          this.event_emission_waiters.delete(waiter)
          waiter = null
        }
        resolve(event)
      }
      this.event_emission_waiters.add(waiter)
    })
    return {
      promise,
      cancel: () => {
        if (waiter) {
          this.event_emission_waiters.delete(waiter)
          waiter = null
        }
      },
    }
  }

  private waitForEventCompleted(event_id: string): Promise<CoreMessage[]> {
    return new Promise((resolve, reject) => {
      const worker = spawnWaitProcess({
        coreClientUrl: new URL('./CoreClient.ts', import.meta.url).href,
        busName: this.name,
        sessionId: this.core.session_id,
        lastPatchSeq: this.core.getPatchSeq(),
        mode: 'event',
        eventId: event_id,
      })
      worker.once('message', (messages: CoreMessage[]) => {
        closeWaitWorker(worker)
        resolve(this.core.filterUnseenPatchMessages(messages))
      })
      worker.once('error', (error) => {
        closeWaitWorker(worker)
        reject(error)
      })
      worker.once('exit', (code) => {
        if (code !== 0) {
          reject(new Error(`core wait worker exited with code ${code}`))
        }
      })
    })
  }

  private localBusesForCoreSession(): RustCoreEventBus[] {
    return Array.from(this.all_instances).filter((bus) => !bus.closed && bus.core.session_id === this.core.session_id)
  }

  private busForInvocation(message: CoreMessage): RustCoreEventBus {
    const handler_id = message.handler_id
    const bus_id = message.bus_id
    const event_id = message.event_id
    if (typeof bus_id === 'string') {
      const indexed_bus = this.all_instances.findBusById(bus_id)
      if (indexed_bus && !indexed_bus.closed && indexed_bus.core.session_id === this.core.session_id) {
        return indexed_bus
      }
      if (bus_id === this.bus_id) {
        return this
      }
    }
    const buses = this.localBusesForCoreSession()
    if (typeof handler_id === 'string' && typeof event_id === 'string') {
      for (const bus of buses) {
        if (bus.handlers.has(handler_id) && bus.events.has(event_id)) {
          return bus
        }
      }
    }
    for (const bus of buses) {
      if (typeof handler_id === 'string' && bus.handlers.has(handler_id)) {
        return bus
      }
      if (typeof bus_id === 'string' && bus.bus_id === bus_id) {
        return bus
      }
    }
    return this
  }

  private applyCoreMessageToLocalBuses(message: CoreMessage): void {
    if (this.core.filterUnseenPatchMessages([message]).length === 0) {
      return
    }
    try {
      const targets = this.targetBusesForCoreMessage(message)
      for (const bus of targets) {
        bus.applyCoreMessageFromSharedCursor(message)
      }
    } finally {
      this.core.ackPatchMessages(message)
    }
  }

  private targetBusesForCoreMessage(message: CoreMessage): RustCoreEventBus[] {
    if (message.type !== 'patch') {
      const bus_id = typeof message.bus_id === 'string' ? message.bus_id : null
      if (bus_id) {
        const bus = this.all_instances.findBusById(bus_id)
        if (bus && !bus.closed && bus.core.session_id === this.core.session_id) {
          return [bus]
        }
      }
      const buses = this.localBusesForCoreSession()
      return buses.length > 0 ? buses : [this]
    }
    const patch = message.patch as CoreMessage | undefined
    if (!patch) {
      return [this]
    }
    const target_bus_ids = new Set<string>()
    const addBusId = (value: unknown): void => {
      if (typeof value === 'string') target_bus_ids.add(value)
    }
    addBusId(patch.bus_id)
    const route = patch.route
    if (route && typeof route === 'object' && !Array.isArray(route)) {
      addBusId((route as Record<string, unknown>).bus_id)
    }
    const result = patch.result
    if (result && typeof result === 'object' && !Array.isArray(result)) {
      addBusId((result as Record<string, unknown>).bus_id)
      addBusId((result as Record<string, unknown>).eventbus_id)
    }
    if (target_bus_ids.size > 0) {
      const indexed_targets: RustCoreEventBus[] = []
      for (const bus_id of target_bus_ids) {
        const bus = this.all_instances.findBusById(bus_id)
        if (bus && !bus.closed && bus.core.session_id === this.core.session_id) {
          indexed_targets.push(bus)
        }
      }
      if (indexed_targets.length > 0) {
        return indexed_targets
      }
      const buses = this.localBusesForCoreSession()
      const targets = buses.filter((bus) => target_bus_ids.has(bus.bus_id))
      return targets.length > 0 ? targets : [this]
    }
    const event_id = typeof patch.event_id === 'string' ? patch.event_id : null
    if (event_id) {
      const buses = this.localBusesForCoreSession()
      const targets = buses.filter((bus) => bus.events.has(event_id))
      return targets.length > 0 ? targets : [this]
    }
    const buses = this.localBusesForCoreSession()
    return buses.length > 0 ? buses : [this]
  }

  private applyCoreMessage(message: CoreMessage): void {
    if (this.core.filterUnseenPatchMessages([message]).length === 0) {
      return
    }
    if (message.type === 'patch' && !this.patchBelongsToThisBus(message.patch as CoreMessage | undefined)) {
      this.core.ackPatchMessages(message)
      return
    }
    try {
      this.applyCoreMessageUnchecked(message)
    } finally {
      this.core.ackPatchMessages(message)
    }
  }

  private applyCoreMessageFromSharedCursor(message: CoreMessage): void {
    if (message.type === 'patch' && !this.patchBelongsToThisBus(message.patch as CoreMessage | undefined)) {
      return
    }
    this.applyCoreMessageUnchecked(message)
  }

  private patchBelongsToThisBus(patch: CoreMessage | undefined): boolean {
    if (!patch) return false
    const event_id = typeof patch.event_id === 'string' ? patch.event_id : null
    if (event_id && this.events.has(event_id)) return true
    if (patch.type === 'event_emitted') {
      const route = patch.route
      if (route && typeof route === 'object' && !Array.isArray(route) && (route as Record<string, unknown>).bus_id === this.bus_id) {
        return true
      }
      const event = patch.event
      if (event && typeof event === 'object' && !Array.isArray(event)) {
        return this.coreRecordBelongsToThisBus(event as Record<string, unknown>)
      }
    }
    if (patch.type === 'serial_route_resumed') {
      const route_id = patch.route_id
      const invocation_id = patch.invocation_id
      if (
        typeof route_id === 'string' &&
        typeof invocation_id === 'string' &&
        this.serial_route_pause_releases.has(`${route_id}:${invocation_id}`)
      ) {
        return true
      }
    }
    const result = patch.result
    if (result && typeof result === 'object' && !Array.isArray(result)) {
      const record = result as Record<string, unknown>
      if (record.bus_id === this.bus_id || record.eventbus_id === this.bus_id) return true
      if (typeof record.event_id === 'string' && this.events.has(record.event_id)) return true
      if (typeof record.result_id === 'string' && this.localResultIdExists(record.result_id)) return true
    }
    const result_id = typeof patch.result_id === 'string' ? patch.result_id : null
    if (result_id && this.localResultIdExists(result_id)) return true
    const invocation_id = typeof patch.invocation_id === 'string' ? patch.invocation_id : null
    if (invocation_id && [...this.invocation_by_result_id.values()].includes(invocation_id)) return true
    return false
  }

  private localResultIdExists(result_id: string): boolean {
    if (this.invocation_by_result_id.has(result_id)) return true
    for (const event of this.events.values()) {
      if ([...event.event_results.values()].some((result) => result.id === result_id)) {
        return true
      }
    }
    return false
  }

  private applyCoreMessageUnchecked(message: CoreMessage): void {
    if (message.type === 'cancel_invocation') {
      const invocation_id = message.invocation_id
      if (typeof invocation_id === 'string') {
        const abort = this.abort_by_invocation_id.get(invocation_id)
        const reason = message.reason
        abort?.(new Error(`handler invocation cancelled: ${String(reason ?? 'cancelled')}`), reason)
      }
      return
    }
    if (message.type === 'event_snapshot') {
      const snapshot = message.event
      if (!snapshot || typeof snapshot !== 'object' || Array.isArray(snapshot)) {
        return
      }
      const record = snapshot as Record<string, unknown>
      const event_id = record.event_id
      if (typeof event_id !== 'string' || !this.coreRecordBelongsToThisBus(record)) {
        return
      }
      const event = this.events.get(event_id)
      if (!event) {
        return
      }
      this.applyCoreSnapshotToEvent(event._event_original ?? event, record)
      return
    }
    if (message.type === 'event_completed') {
      const event = this.events.get(message.event_id as string)
      if (event) {
        if (event.event_status === 'pending') {
          event._markStarted(null, false)
          this._notifyEventChange(event, 'started')
        }
        event._markCompleted(true, false)
        this.resolveEventCompletionWaiters(event)
      }
      return
    }
    if (message.type !== 'patch') return
    const patch = message.patch as CoreMessage | undefined
    if (!patch) return
    if (patch.type === 'event_emitted') {
      const event_record = patch.event
      if (event_record && typeof event_record === 'object' && !Array.isArray(event_record)) {
        const event = this.eventFromCoreRecord('*', event_record as Record<string, unknown>)
        const local = this.events.get(event.event_id)
        if (local) {
          this.applyCoreSnapshotToEvent(local._event_original ?? local, event_record as Record<string, unknown>)
        }
      }
      return
    }
    if (patch.type === 'event_started') {
      const event = this.events.get(patch.event_id as string)
      const was_pending = event?.event_status === 'pending'
      event?._markStarted(typeof patch.started_at === 'string' ? patch.started_at : null, false)
      if (event && was_pending && event.event_status === 'started') {
        this._notifyEventChange(event, 'started')
      }
      return
    }
    if (patch.type === 'event_updated') {
      const event = this.events.get(patch.event_id as string)
      if (event) {
        if (patch.event_handler_completion === 'all' || patch.event_handler_completion === 'first') {
          event.event_handler_completion = patch.event_handler_completion
        }
        if (typeof patch.event_blocks_parent_completion === 'boolean') {
          event.event_blocks_parent_completion = patch.event_blocks_parent_completion
        }
      }
      return
    }
    if (patch.type === 'serial_route_paused') {
      const route_id = patch.route_id
      const invocation_id = patch.invocation_id
      if (typeof route_id === 'string' && typeof invocation_id === 'string') {
        if (![...this.invocation_by_result_id.values()].includes(invocation_id)) {
          return
        }
        const key = `${route_id}:${invocation_id}`
        if (!this.serial_route_pause_releases.has(key)) {
          this.serial_route_pause_releases.set(key, this.locks._requestRunloopPause())
        }
      }
      return
    }
    if (patch.type === 'serial_route_resumed') {
      const route_id = patch.route_id
      const invocation_id = patch.invocation_id
      if (typeof route_id === 'string' && typeof invocation_id === 'string') {
        const key = `${route_id}:${invocation_id}`
        const release = this.serial_route_pause_releases.get(key)
        if (release) {
          this.serial_route_pause_releases.delete(key)
          release()
        }
      }
      return
    }
    if (patch.type === 'result_pending') {
      this.applyResultRecord(patch.result as Record<string, unknown> | undefined, 'pending')
      return
    }
    if (patch.type === 'result_started') {
      const result_id = patch.result_id
      const invocation_id = patch.invocation_id
      const started_at = patch.started_at
      if (typeof result_id === 'string' && typeof invocation_id === 'string') {
        this.invocation_by_result_id.set(result_id, invocation_id)
      }
      for (const event of this.events.values()) {
        for (const result of event.event_results.values()) {
          if (result.id === result_id) {
            const previous_status = result.status
            result.update({ status: 'started' })
            if (typeof started_at === 'string') {
              result.started_at = started_at
            }
            if (previous_status !== 'started') {
              this._notifyEventResultChange(event, result, 'started')
            }
            return
          }
        }
      }
      return
    }
    if (patch.type === 'result_completed' || patch.type === 'result_cancelled' || patch.type === 'result_timed_out') {
      this.applyResultRecord(patch.result as Record<string, unknown> | undefined)
      return
    }
    if (patch.type === 'event_slow_warning') {
      this.warnSlowEvent(patch.event_id as string | undefined)
      return
    }
    if (patch.type === 'result_slow_warning') {
      this.warnSlowResult(patch.result_id as string | undefined)
      return
    }
    if (patch.type === 'event_completed') {
      const event_id = patch.event_id
      if (typeof event_id !== 'string') return
      for (const bus of this.localBusesForCoreSession()) {
        const event = bus.events.get(event_id)
        if (!event) continue
        const event_results = patch.event_results
        if (event_results && typeof event_results === 'object' && !Array.isArray(event_results)) {
          for (const result_record of bus.orderedResultRecords(event_results as Record<string, unknown>)) {
            bus.applyResultSnapshot(event, result_record)
          }
        }
        if (Array.isArray(patch.event_path)) {
          event.event_path = patch.event_path.filter((entry): entry is string => typeof entry === 'string')
        }
        if (typeof patch.event_parent_id === 'string' || patch.event_parent_id === null) {
          event.event_parent_id = patch.event_parent_id
        }
        if (typeof patch.event_emitted_by_handler_id === 'string' || patch.event_emitted_by_handler_id === null) {
          event.event_emitted_by_handler_id = patch.event_emitted_by_handler_id
        }
        if (typeof patch.event_emitted_by_result_id === 'string' || patch.event_emitted_by_result_id === null) {
          event.event_emitted_by_result_id = patch.event_emitted_by_result_id
        }
        if (typeof patch.event_blocks_parent_completion === 'boolean') {
          event.event_blocks_parent_completion = patch.event_blocks_parent_completion
        }
        if (typeof patch.event_started_at === 'string') {
          event.event_started_at = patch.event_started_at
        }
        bus.cancelFirstModeLosersForCompletedEvent(event)
        if (event.event_status === 'pending') {
          event._markStarted(typeof patch.event_started_at === 'string' ? patch.event_started_at : null, false)
          bus._notifyEventChange(event, 'started')
        }
        event._markCompleted(true, false, typeof patch.completed_at === 'string' ? patch.completed_at : null)
        bus.in_flight_event_ids.delete(event.event_id)
        bus.resolveEventCompletionWaiters(event)
      }
      return
    }
    if (patch.type === 'event_completed_compact') {
      const event_id = patch.event_id
      if (typeof event_id !== 'string') return
      for (const bus of this.localBusesForCoreSession()) {
        const event = bus.events.get(event_id)
        if (!event) continue
        const started_at = typeof patch.event_started_at === 'string' ? patch.event_started_at : null
        if (event.event_status === 'pending') {
          event._markStarted(started_at, false)
          bus._notifyEventChange(event, 'started')
        } else if (started_at !== null) {
          event.event_started_at = monotonicDatetime(started_at)
        }
        bus.cancelFirstModeLosersForCompletedEvent(event)
        event._markCompleted(true, false, typeof patch.completed_at === 'string' ? patch.completed_at : null)
        bus.in_flight_event_ids.delete(event.event_id)
        bus.resolveEventCompletionWaiters(event)
      }
      return
    }
  }

  private applyResultRecord(record: Record<string, unknown> | undefined, status_override?: string): void {
    if (!record) return
    const event_id = record.event_id
    const handler_id = record.handler_id
    if (typeof event_id !== 'string' || typeof handler_id !== 'string') return
    const event = this.events.get(event_id)
    const handler =
      this.handlers.get(handler_id) ??
      this.localBusesForCoreSession()
        .find((bus) => bus.handlers.has(handler_id))
        ?.handlers.get(handler_id)
    if (!event || !handler) return

    let result = event.event_results.get(handler_id)
    const created = !result
    if (!result) {
      result = new EventResult({
        event,
        handler,
        id: typeof record.result_id === 'string' ? record.result_id : undefined,
      })
      event.event_results.set(handler_id, result)
    } else if (typeof record.result_id === 'string') {
      result.id = record.result_id
    }
    if (typeof record.started_at === 'string') {
      result.started_at = record.started_at
    }
    if (typeof record.completed_at === 'string') {
      result.completed_at = record.completed_at
    }
    const status = status_override ?? record.status
    if (status === 'pending' || status === 'started') {
      if (result.status === 'completed' || result.status === 'error') {
        return
      }
      const previous_status = result.status
      result.update({ status })
      if (created || previous_status !== status) {
        this._notifyEventResultChange(event, result, status)
      }
    } else if (status === 'completed') {
      const was_terminal = result.status === 'completed' || result.status === 'error'
      result.result_is_event_reference = record.result_is_event_reference === true
      result.update({ result: record.result_is_undefined === true ? undefined : (record.result as never) })
      if (typeof record.completed_at === 'string') {
        result.completed_at = record.completed_at
      }
      this.attachResultChildren(result, this.childIdsFromResultRecord(record))
      this.invocation_by_result_id.delete(result.id)
      if (!was_terminal) {
        this._notifyEventResultChange(event, result, 'completed')
      }
    } else if (status === 'error' || status === 'cancelled') {
      const was_terminal = result.status === 'completed' || result.status === 'error'
      const restored_error = this.restoreCoreError(result, record.error, record)
      if (restored_error instanceof Error) {
        const invocation_id = this.invocation_by_result_id.get(result.id)
        if (invocation_id) {
          this.abort_by_invocation_id.get(invocation_id)?.(restored_error)
        }
      }
      if (!was_terminal) {
        result.update({ error: restored_error ?? new Error('handler failed') })
      }
      if (typeof record.completed_at === 'string') {
        result.completed_at = record.completed_at
      }
      this.attachResultChildren(result, this.childIdsFromResultRecord(record))
      this.invocation_by_result_id.delete(result.id)
      if (!was_terminal) {
        this._notifyEventResultChange(event, result, 'completed')
      }
    }
  }

  private cancelFirstModeLosersForCompletedEvent(event: BaseEvent): void {
    const original = event._event_original ?? event
    const completion = original.event_handler_completion ?? this.event_handler_completion
    if (completion !== 'first') {
      return
    }
    const winner_bus_ids = new Set<string>()
    for (const result of original.event_results.values()) {
      if (winner_bus_ids.has(result.eventbus_id)) {
        continue
      }
      if (
        result.status === 'completed' &&
        result.result !== undefined &&
        result.result !== null &&
        !(result.result instanceof Error) &&
        !(result.result instanceof BaseEvent) &&
        result.error === undefined
      ) {
        winner_bus_ids.add(result.eventbus_id)
        original._markRemainingFirstModeResultCancelled(result)
      }
    }
  }

  private async _runMiddlewareHook(hook: keyof EventBusMiddleware, args: unknown[]): Promise<void> {
    if (this.middlewares.length === 0) {
      return
    }
    for (const middleware of this.middlewares) {
      const callback = middleware[hook]
      if (!callback) {
        continue
      }
      await (callback as (...hook_args: unknown[]) => void | Promise<void>).apply(middleware, args)
    }
  }

  private async _onBusHandlersChange(handler: EventHandler, registered: boolean): Promise<void> {
    await this._runMiddlewareHook('onBusHandlersChange', [this, handler, registered])
  }

  private async waitForFutureMatch<T extends BaseEvent>(
    event_type: EventPattern<T> | '*',
    where: (event: T) => boolean,
    options: FilterOptions<T>
  ): Promise<T | null> {
    const event_pattern = normalizeEventPattern(event_type)
    const future = options.future
    const timeout_ms = future === true || future === undefined ? null : Math.max(0, Number(future)) * 1000
    const deadline = timeout_ms === null ? null : Date.now() + timeout_ms
    const field_filters = Object.entries(options).filter(
      ([key, value]) => key !== 'limit' && key !== 'past' && key !== 'future' && key !== 'child_of' && value !== undefined
    )
    const wait_started_at = null
    const seen_event_ids = new Set(this.historyEventIds(event_pattern))
    let after_event_id: string | null = null
    let after_created_at: string | null = null
    const matches = (event: BaseEvent): event is T => {
      if (event_pattern !== '*' && event.event_type !== event_pattern) {
        return false
      }
      if (options.child_of && !this.eventIsChildOf(event, options.child_of)) {
        return false
      }
      if (!field_filters.every(([field_name, expected]) => (event as unknown as Record<string, unknown>)[field_name] === expected)) {
        return false
      }
      return where(event as T)
    }
    const local_wait = this.waitForLocalEventEmission<T>(matches)
    try {
      for (;;) {
        const remaining_ms = deadline === null ? null : Math.max(0, deadline - Date.now())
        if (remaining_ms !== null && remaining_ms <= 0) {
          return this.findCoreRecordCreatedAfter(event_type, matches, wait_started_at, seen_event_ids)
        }
        if (!this.background_worker_enabled) {
          await this.runWithoutCoreOutcomeBatch(() => this.processAvailableCoreMessages())
          const core_match = this.findCoreRecordCreatedAfter(event_type, matches, wait_started_at, seen_event_ids)
          if (core_match) {
            return core_match
          }
          const poll_ms = remaining_ms === null ? 10 : Math.min(10, remaining_ms)
          const outcome = await Promise.race([
            local_wait.promise.then((event) => ({ type: 'local' as const, event })),
            new Promise<{ type: 'poll' }>((resolve) => setTimeout(() => resolve({ type: 'poll' }), poll_ms)),
          ])
          if (outcome.type === 'local') {
            return outcome.event
          }
          continue
        }
        const core_wait = this.waitForEventEmittedCancellable(
          event_pattern,
          Array.from(seen_event_ids),
          after_event_id,
          after_created_at,
          remaining_ms
        )
        const outcome = await Promise.race([
          local_wait.promise.then((event) => ({ type: 'local' as const, event })),
          core_wait.promise.then((records) => ({ type: 'core' as const, records })),
        ])
        core_wait.cancel()
        if (outcome.type === 'local') {
          return outcome.event
        }
        const records = outcome.records
        if (records.length === 0) {
          return this.findCoreRecordCreatedAfter(event_type, matches, wait_started_at, seen_event_ids)
        }
        for (const record of records) {
          const event_id = record.event_id
          const already_seen = typeof event_id === 'string' && seen_event_ids.has(event_id)
          if (typeof event_id === 'string') {
            seen_event_ids.add(event_id)
            after_event_id = event_id
          }
          if (typeof record.event_created_at === 'string') {
            after_created_at = record.event_created_at
          }
          if (already_seen) {
            continue
          }
          const event = this.eventFromCoreRecord(event_type, record)
          if (matches(event)) {
            return event
          }
        }
      }
    } finally {
      local_wait.cancel()
    }
  }

  private findCoreRecordCreatedAfter<T extends BaseEvent>(
    event_type: EventPattern<T> | '*',
    matches: (event: BaseEvent) => event is T,
    created_after: string | null,
    seen_event_ids: Set<string> | null = null
  ): T | null {
    const event_pattern = normalizeEventPattern(event_type)
    for (const record of this.historyRecords(event_pattern)) {
      const event_id = record.event_id
      if (typeof event_id === 'string' && seen_event_ids?.has(event_id)) {
        continue
      }
      if (created_after !== null && this.timestampBefore(record.event_created_at, created_after)) {
        continue
      }
      const event = this.eventFromCoreRecord(event_type, record)
      if (matches(event)) {
        return event
      }
    }
    return null
  }

  private waitForEventEmittedCancellable(
    event_pattern: string,
    seen_event_ids: string[],
    after_event_id: string | null,
    after_created_at: string | null,
    timeout_ms: number | null
  ): { promise: Promise<Record<string, unknown>[]>; cancel: () => void } {
    let worker: ChildProcess | null = null
    let timeout_id: ReturnType<typeof setTimeout> | null = null
    let settled = false
    const cleanup = (): void => {
      if (timeout_id) {
        clearTimeout(timeout_id)
        timeout_id = null
      }
      if (worker) {
        closeWaitWorker(worker)
        worker = null
      }
    }
    const promise = new Promise<Record<string, unknown>[]>((resolve, reject) => {
      worker = spawnWaitProcess({
        coreClientUrl: new URL('./CoreClient.ts', import.meta.url).href,
        busName: this.name,
        busId: this.bus_id,
        sessionId: this.core.session_id,
        lastPatchSeq: this.core.getPatchSeq(),
        mode: 'emitted',
        eventPattern: event_pattern,
        seenEventIds: seen_event_ids,
        afterEventId: after_event_id,
        afterCreatedAt: after_created_at,
      })
      const settle = (value: Record<string, unknown>[]): void => {
        if (settled) return
        settled = true
        cleanup()
        resolve(value)
      }
      worker.once('message', (messages: CoreMessage[]) => {
        const list_message = messages.find((message) => message.type === 'event_list')
        const events = Array.isArray(list_message?.events) ? list_message.events : []
        settle(
          events.filter((event): event is Record<string, unknown> => typeof event === 'object' && event !== null && !Array.isArray(event))
        )
      })
      worker.once('error', (error) => {
        if (settled) return
        settled = true
        cleanup()
        reject(error)
      })
      worker.once('exit', (code) => {
        if (!settled && code !== 0) {
          settled = true
          cleanup()
          reject(new Error(`core wait worker exited with code ${code}`))
        }
      })
      if (timeout_ms !== null) {
        timeout_id = setTimeout(() => settle([]), timeout_ms)
        timeout_id.unref?.()
      }
    })
    return {
      promise,
      cancel: () => {
        if (settled) return
        settled = true
        cleanup()
      },
    }
  }
}

const WAIT_WORKER_SOURCE = `
const workerData = JSON.parse(process.env.ABXBUS_CORE_WORKER_DATA)
const sendMessage = (message) => {
  if (!process.connected) return false
  try {
    process.send(message)
    return true
  } catch (error) {
    if (error && typeof error === 'object' && error.code === 'EPIPE') return false
    throw error
  }
}

;(async () => {
  const { RustCoreClient } = await import(workerData.coreClientUrl)
  const client = new RustCoreClient({ bus_name: workerData.busName, session_id: workerData.sessionId })
  if (typeof workerData.lastPatchSeq === 'number') {
    client.setPatchSeq(workerData.lastPatchSeq)
  }
  let closing = false
  process.on('message', (message) => {
    if (message && typeof message === 'object' && message.type === 'close') {
      closing = true
    }
  })

  const sendMessages = (messages) => {
    if (sendMessage(messages)) {
      client.ackPatchMessages(messages)
    }
  }

  try {
    if (workerData.mode === 'event') {
      sendMessages(client.waitEventCompleted(workerData.eventId))
    } else if (workerData.mode === 'emitted') {
      sendMessages(client.waitEventEmitted(
        workerData.busId,
        workerData.eventPattern,
        workerData.seenEventIds ?? [],
        workerData.afterEventId ?? null,
        workerData.afterCreatedAt ?? null
      ))
    } else if (workerData.mode === 'idle') {
      sendMessage(client.waitBusIdle(workerData.busId, workerData.timeout))
    } else if (workerData.mode === 'signals') {
      const messages = client.waitInvocations(workerData.busId, 0)
      sendMessages(messages)
    } else {
      while (!closing) {
        const messages = client.waitInvocations(workerData.busId, 16)
        if (!sendMessage(messages)) {
          break
        }
        client.ackPatchMessages(messages)
        if (messages.some((message) => message.type === 'disconnected' || message.type === 'session_closed' || message.type === 'core_stopped')) {
          break
        }
      }
    }
  } finally {
    client.closeTransportOnly({ closeSession: true })
    if (workerData.mode === 'event' || workerData.mode === 'emitted' || workerData.mode === 'idle' || workerData.mode === 'signals') {
      process.exit(0)
    }
  }
})().catch((error) => {
  throw error
})
`

const spawnWaitProcess = (workerData: Record<string, unknown>): ChildProcess => {
  return spawn(process.execPath, [...workerExecArgv(), '-e', WAIT_WORKER_SOURCE], {
    stdio: ['ignore', 'ignore', 'inherit', 'ipc'],
    env: {
      ...process.env,
      ABXBUS_CORE_NAMESPACE: coreNamespace(),
      ABXBUS_CORE_WORKER_DATA: JSON.stringify(workerData),
    },
  })
}

const closeWaitWorker = (worker: ChildProcess): void => {
  try {
    worker.disconnect()
  } catch {
    // The worker may have already exited and closed its IPC channel.
  }
  setTimeout(() => {
    if (worker.exitCode === null && !worker.killed) {
      worker.kill()
    }
  }, 10).unref?.()
}

const workerExecArgv = (): string[] => {
  const args: string[] = []
  for (let i = 0; i < process.execArgv.length; i += 1) {
    const arg = process.execArgv[i]
    if (arg === '--import' || arg === '--loader') {
      const value = process.execArgv[i + 1]
      if (value) {
        args.push(arg, value)
        i += 1
      }
    } else if (arg.startsWith('--import=') || arg.startsWith('--loader=')) {
      args.push(arg)
    }
  }
  return args
}

export const stableCoreBusId = (bus_name: string): string => {
  const namespace_dns = Buffer.from('6ba7b8109dad11d180b400c04fd430c8', 'hex')
  const bytes = Buffer.from(createHash('sha1').update(namespace_dns).update(`abxbus-core-bus:${bus_name}`).digest().subarray(0, 16))
  bytes[6] = (bytes[6] & 0x0f) | 0x50
  bytes[8] = (bytes[8] & 0x3f) | 0x80
  const hex = bytes.toString('hex').slice(0, 32)
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`
}

export const uniqueCoreBusId = (): string => randomUUID()

export const defaultCoreBusId = (bus_name: string, registry: CoreEventBusRegistry): string => {
  for (const bus of registry) {
    if (bus.name === bus_name) {
      return uniqueCoreBusId()
    }
  }
  return stableCoreBusId(bus_name)
}
