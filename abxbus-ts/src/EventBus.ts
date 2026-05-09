import {
  CoreEventBusRegistry,
  RustCoreEventBus,
  defaultCoreBusId,
  type RustCoreEventBusJSON,
  type RustCoreEventBusOptions,
} from './CoreEventBus.js'
import { BaseEvent, type BaseEventJSON } from './BaseEvent.js'
import { EventHandler, type EventHandlerJSON } from './EventHandler.js'
import { AsyncLock } from './LockManager.js'
import type { EventHandlerCallable } from './types.js'

export type EventBusOptions = RustCoreEventBusOptions

export type EventBusJSON = RustCoreEventBusJSON

export class GlobalEventBusRegistry extends CoreEventBusRegistry {}

export class EventBus extends RustCoreEventBus {
  static get all_instances(): GlobalEventBusRegistry {
    return registryForEventBusClass(this)
  }

  constructor(name = 'EventBus', options: EventBusOptions = {}) {
    const registry = (new.target as typeof EventBus).all_instances
    super(name, {
      ...options,
      id: options.id ?? defaultCoreBusId(name, registry),
      background_worker: options.background_worker ?? false,
    })
    this._lock_for_event_global_serial = globalLockForEventBusClass(this.constructor as typeof EventBus)
    this.all_instances.discard(this)
    this.all_instances = registry
    this.all_instances.add(this)
  }

  private static _stubHandlerFn(): EventHandlerCallable {
    return (() => undefined) as EventHandlerCallable
  }

  static fromJSON(data: unknown): EventBus {
    if (!data || typeof data !== 'object') {
      throw new Error('EventBus.fromJSON(data) requires an object')
    }
    const record = data as Record<string, unknown>
    const name = typeof record.name === 'string' ? record.name : 'EventBus'
    const options: EventBusOptions = {}

    if (typeof record.id === 'string') options.id = record.id
    if (typeof record.max_history_size === 'number' || record.max_history_size === null) options.max_history_size = record.max_history_size
    if (typeof record.max_history_drop === 'boolean') options.max_history_drop = record.max_history_drop
    if (
      record.event_concurrency === 'global-serial' ||
      record.event_concurrency === 'bus-serial' ||
      record.event_concurrency === 'parallel'
    ) {
      options.event_concurrency = record.event_concurrency
    }
    if (typeof record.event_timeout === 'number') options.event_timeout = record.event_timeout
    if (typeof record.event_slow_timeout === 'number' || record.event_slow_timeout === null) {
      options.event_slow_timeout = record.event_slow_timeout
    }
    if (record.event_handler_concurrency === 'serial' || record.event_handler_concurrency === 'parallel') {
      options.event_handler_concurrency = record.event_handler_concurrency
    }
    if (record.event_handler_completion === 'all' || record.event_handler_completion === 'first') {
      options.event_handler_completion = record.event_handler_completion
    }
    if (typeof record.event_handler_timeout === 'number' || record.event_handler_timeout === null) {
      options.event_handler_timeout = record.event_handler_timeout
    }
    if (typeof record.event_handler_slow_timeout === 'number' || record.event_handler_slow_timeout === null) {
      options.event_handler_slow_timeout = record.event_handler_slow_timeout
    }
    if (typeof record.event_handler_detect_file_paths === 'boolean') {
      options.event_handler_detect_file_paths = record.event_handler_detect_file_paths
    }

    const bus = new EventBus(name, options)

    if (record.handlers === undefined) {
      // Missing handler maps are accepted as an empty registry.
    } else if (record.handlers && typeof record.handlers === 'object' && !Array.isArray(record.handlers)) {
      for (const [handler_id, payload] of Object.entries(record.handlers as Record<string, unknown>)) {
        if (!payload || typeof payload !== 'object') continue
        const parsed = EventHandler.fromJSON(
          {
            ...(payload as EventHandlerJSON),
            id: typeof (payload as { id?: unknown }).id === 'string' ? (payload as { id: string }).id : handler_id,
          },
          EventBus._stubHandlerFn()
        )
        bus.handlers.set(parsed.id, parsed)
      }
    } else {
      throw new Error('EventBus.fromJSON(data) requires handlers as an id-keyed object')
    }

    if (record.handlers_by_key === undefined) {
      bus.handlers_by_key.clear()
    } else if (record.handlers_by_key && typeof record.handlers_by_key === 'object' && !Array.isArray(record.handlers_by_key)) {
      bus.handlers_by_key.clear()
      for (const [raw_key, raw_ids] of Object.entries(record.handlers_by_key as Record<string, unknown>)) {
        if (!Array.isArray(raw_ids)) continue
        bus.handlers_by_key.set(
          raw_key,
          raw_ids.filter((id): id is string => typeof id === 'string')
        )
      }
    } else {
      throw new Error('EventBus.fromJSON(data) requires handlers_by_key as an object')
    }

    if (!record.event_history || typeof record.event_history !== 'object' || Array.isArray(record.event_history)) {
      throw new Error('EventBus.fromJSON(data) requires event_history as an id-keyed object')
    }
    for (const payload of Object.values(record.event_history as Record<string, BaseEventJSON>)) {
      const event_results = payload.event_results
      if (!event_results || typeof event_results !== 'object' || Array.isArray(event_results)) continue
      for (const [result_key, result_payload] of Object.entries(event_results as Record<string, unknown>)) {
        if (!result_payload || typeof result_payload !== 'object' || Array.isArray(result_payload)) {
          continue
        }
        const result_record = result_payload as Record<string, unknown>
        const handler_id = typeof result_record.handler_id === 'string' ? result_record.handler_id : result_key
        if (bus.handlers.has(handler_id)) {
          continue
        }
        const event_pattern =
          typeof result_record.handler_event_pattern === 'string'
            ? result_record.handler_event_pattern
            : typeof payload.event_type === 'string'
              ? payload.event_type
              : '*'
        const handler = EventHandler.fromJSON(
          {
            id: handler_id,
            eventbus_name: typeof result_record.eventbus_name === 'string' ? result_record.eventbus_name : bus.name,
            eventbus_id: typeof result_record.eventbus_id === 'string' ? result_record.eventbus_id : bus.bus_id,
            event_pattern,
            handler_name: typeof result_record.handler_name === 'string' ? result_record.handler_name : handler_id,
            handler_file_path: typeof result_record.handler_file_path === 'string' ? result_record.handler_file_path : null,
            handler_timeout: typeof result_record.handler_timeout === 'number' ? result_record.handler_timeout : null,
            handler_slow_timeout: typeof result_record.handler_slow_timeout === 'number' ? result_record.handler_slow_timeout : null,
            handler_registered_at:
              typeof result_record.handler_registered_at === 'string'
                ? result_record.handler_registered_at
                : typeof payload.event_created_at === 'string'
                  ? payload.event_created_at
                  : undefined,
          },
          EventBus._stubHandlerFn()
        )
        bus.handlers.set(handler.id, handler)
        const ids = bus.handlers_by_key.get(handler.event_pattern) ?? []
        if (!ids.includes(handler.id)) {
          ids.push(handler.id)
        }
        bus.handlers_by_key.set(handler.event_pattern, ids)
      }
    }

    if (record.pending_event_queue === undefined) {
      record.pending_event_queue = []
    }
    if (!Array.isArray(record.pending_event_queue)) {
      throw new Error('EventBus.fromJSON(data) requires pending_event_queue as an array of event ids')
    }
    const pending_event_ids = record.pending_event_queue.filter((event_id): event_id is string => typeof event_id === 'string')

    bus.core.importBusSnapshot({
      bus: {
        bus_id: bus.bus_id,
        name: bus.name,
        label: bus.label,
        host_id: bus.core.session_id,
        defaults: bus.defaultsRecord(),
        max_history_size: bus.event_history.max_history_size,
        max_history_drop: bus.event_history.max_history_drop,
      },
      handlers: Array.from(bus.handlers.values()).map((handler) => ({
        handler_id: handler.id,
        bus_id: bus.bus_id,
        host_id: bus.core.session_id,
        event_pattern: handler.event_pattern,
        handler_name: handler.handler_name,
        handler_file_path: handler.handler_file_path,
        handler_registered_at: handler.handler_registered_at,
        handler_timeout: handler.handler_timeout ?? null,
        handler_slow_timeout: handler.handler_slow_timeout ?? null,
        handler_concurrency: null,
        handler_completion: null,
      })),
      events: Object.values(record.event_history as Record<string, Record<string, unknown>>),
      pending_event_ids,
    })
    for (const [event_id, payload] of Object.entries(record.event_history as Record<string, BaseEventJSON>)) {
      const event = BaseEvent.fromJSON({
        ...payload,
        event_id: typeof payload.event_id === 'string' ? payload.event_id : event_id,
      })
      event.event_bus = bus as never
      const results_by_handler = new Map()
      for (const result of event.event_results.values()) {
        const handler = bus.handlers.get(result.handler_id)
        if (handler) {
          result.handler = handler
        }
        results_by_handler.set(result.handler_id, result)
      }
      event.event_results = results_by_handler
      bus.rememberLiveEvent(event.event_id, event)
    }
    bus.pending_event_queue = pending_event_ids
      .map((event_id) => bus.findEventById(event_id))
      .filter((event): event is BaseEvent => event !== null)

    return bus
  }
}

const event_bus_global_locks = new WeakMap<typeof EventBus, AsyncLock>()
const event_bus_registries = new WeakMap<typeof EventBus, GlobalEventBusRegistry>()
const process_registry_refs = new Set<WeakRef<GlobalEventBusRegistry>>()
let process_cleanup_registered = false

const globalLockForEventBusClass = (bus_class: typeof EventBus): AsyncLock => {
  let lock = event_bus_global_locks.get(bus_class)
  if (!lock) {
    lock = new AsyncLock(1)
    event_bus_global_locks.set(bus_class, lock)
  }
  return lock
}

const registryForEventBusClass = (bus_class: typeof EventBus): GlobalEventBusRegistry => {
  let registry = event_bus_registries.get(bus_class)
  if (!registry) {
    registry = new GlobalEventBusRegistry()
    event_bus_registries.set(bus_class, registry)
    process_registry_refs.add(new WeakRef(registry))
    registerProcessCleanup()
  }
  return registry
}

const registerProcessCleanup = (): void => {
  if (process_cleanup_registered) {
    return
  }
  const maybe_process = (globalThis as { process?: { once?: (event: string, listener: () => void) => void } }).process
  if (typeof maybe_process?.once !== 'function') {
    return
  }
  process_cleanup_registered = true
  maybe_process.once('exit', () => {
    for (const ref of process_registry_refs) {
      const registry = ref.deref()
      if (!registry) {
        process_registry_refs.delete(ref)
        continue
      }
      for (const bus of registry) {
        bus.disconnect()
      }
    }
  })
}
