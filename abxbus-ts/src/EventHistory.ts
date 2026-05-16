import { BaseEvent } from './BaseEvent.js'
import type { EventPattern, FindWindow } from './types.js'
import { normalizeEventPattern } from './types.js'
import { monotonicDatetime } from './helpers.js'

const normalizeMaxHistorySize = (value: number | null | undefined): number | null => {
  if (value === undefined) {
    return 100
  }
  if (value === null || value <= 0) {
    return null
  }
  return value
}

export type EventHistoryFindOptions = {
  past?: FindWindow
  future?: FindWindow
  child_of?: BaseEvent | null
  event_is_child_of?: (event: BaseEvent, ancestor: BaseEvent) => boolean
  wait_for_future_match?: (
    event_pattern: string | '*',
    matches: (event: BaseEvent) => boolean,
    future: FindWindow
  ) => Promise<BaseEvent | null>
} & Record<string, unknown>

export type EventHistoryFilterOptions = EventHistoryFindOptions & { limit?: number | null }

export type EventHistoryTrimOptions<TEvent extends BaseEvent = BaseEvent> = {
  is_event_complete?: (event: TEvent) => boolean
  on_remove?: (event: TEvent) => void
  owner_label?: string
  max_history_size?: number | null
  max_history_drop?: boolean
}

export class EventHistory<TEvent extends BaseEvent = BaseEvent> implements Iterable<[string, TEvent]> {
  max_history_size: number | null
  max_history_drop: boolean

  private _events: Map<string, TEvent>
  private _warned_about_dropping_uncompleted_events: boolean

  constructor(options: { max_history_size?: number | null; max_history_drop?: boolean } = {}) {
    this.max_history_size = normalizeMaxHistorySize(options.max_history_size)
    this.max_history_drop = options.max_history_drop ?? false
    this._events = new Map()
    this._warned_about_dropping_uncompleted_events = false
  }

  get size(): number {
    return this._events.size
  }

  [Symbol.iterator](): Iterator<[string, TEvent]> {
    return this._events[Symbol.iterator]()
  }

  entries(): IterableIterator<[string, TEvent]> {
    return this._events.entries()
  }

  keys(): IterableIterator<string> {
    return this._events.keys()
  }

  values(): IterableIterator<TEvent> {
    return this._events.values()
  }

  clear(): void {
    this._events.clear()
  }

  get(event_id: string): TEvent | undefined {
    return this._events.get(event_id)
  }

  set(event_id: string, event: TEvent): this {
    this._events.set(event_id, event)
    return this
  }

  has(event_id: string): boolean {
    return this._events.has(event_id)
  }

  delete(event_id: string): boolean {
    return this._events.delete(event_id)
  }

  addEvent(event: TEvent): void {
    this._events.set(event.event_id, event)
  }

  getEvent(event_id: string): TEvent | undefined {
    return this._events.get(event_id)
  }

  removeEvent(event_id: string): boolean {
    return this._events.delete(event_id)
  }

  hasEvent(event_id: string): boolean {
    return this._events.has(event_id)
  }

  static normalizeEventPattern(event_pattern: EventPattern | '*'): string | '*' {
    return normalizeEventPattern(event_pattern)
  }

  find(event_pattern: '*', where?: (event: TEvent) => boolean, options?: EventHistoryFindOptions): Promise<TEvent | null>
  find<TMatch extends TEvent>(
    event_pattern: EventPattern<TMatch>,
    where?: (event: TMatch) => boolean,
    options?: EventHistoryFindOptions
  ): Promise<TMatch | null>
  async find(
    event_pattern: EventPattern<TEvent> | '*',
    where: (event: TEvent) => boolean = () => true,
    options: EventHistoryFindOptions = {}
  ): Promise<TEvent | null> {
    // `limit` field-equality filter would collide with filter()'s cap arg; route it through `where`.
    let effective_where = where
    let effective_options: EventHistoryFindOptions = options
    if (Object.prototype.hasOwnProperty.call(options, 'limit')) {
      const { limit: limit_field_value, ...rest } = options as EventHistoryFindOptions & { limit: unknown }
      const inner_where = where
      effective_where = (event: TEvent) => (event as unknown as Record<string, unknown>).limit === limit_field_value && inner_where(event)
      effective_options = rest as EventHistoryFindOptions
    }
    const results = await this.filter(event_pattern as EventPattern<TEvent> | '*', effective_where, {
      ...effective_options,
      limit: 1,
    })
    return results.length > 0 ? results[0] : null
  }

  filter(event_pattern: '*', where?: (event: TEvent) => boolean, options?: EventHistoryFilterOptions): Promise<TEvent[]>
  filter<TMatch extends TEvent>(
    event_pattern: EventPattern<TMatch>,
    where?: (event: TMatch) => boolean,
    options?: EventHistoryFilterOptions
  ): Promise<TMatch[]>
  async filter(
    event_pattern: EventPattern<TEvent> | '*',
    where: (event: TEvent) => boolean = () => true,
    options: EventHistoryFilterOptions = {}
  ): Promise<TEvent[]> {
    const past = options.past ?? true
    const future = options.future ?? false
    const child_of = options.child_of ?? null
    const eventIsChildOf = options.event_is_child_of ?? ((event: BaseEvent, ancestor: BaseEvent) => this.eventIsChildOf(event, ancestor))
    const waitForFutureMatch = options.wait_for_future_match
    const limit = options.limit ?? null
    if (past === false && future === false) {
      return []
    }

    if (limit !== null && limit <= 0) {
      return []
    }

    const event_key = EventHistory.normalizeEventPattern(event_pattern)
    const cutoff_at = past === true ? null : monotonicDatetime(new Date(Date.now() - Math.max(0, Number(past)) * 1000).toISOString())

    const event_field_filters = Object.entries(options).filter(
      ([key, value]) =>
        key !== 'past' &&
        key !== 'future' &&
        key !== 'child_of' &&
        key !== 'event_is_child_of' &&
        key !== 'wait_for_future_match' &&
        key !== 'limit' &&
        value !== undefined
    )

    const matches = (event: BaseEvent): boolean =>
      (event_key === '*' || event.event_type === event_key) &&
      (!child_of || eventIsChildOf(event, child_of)) &&
      event_field_filters.every(([field_name, expected]) => (event as unknown as Record<string, unknown>)[field_name] === expected) &&
      where(event as TEvent)

    const results: TEvent[] = []
    if (past !== false) {
      const history_values = Array.from(this._events.values())
      for (let i = history_values.length - 1; i >= 0; i -= 1) {
        const event = history_values[i]
        if (cutoff_at !== null && event.event_created_at < cutoff_at) {
          continue
        }
        if (matches(event)) {
          results.push(event)
          if (limit !== null && results.length >= limit) {
            return results
          }
        }
      }
    }

    if (future === false || !waitForFutureMatch) {
      return results
    }

    const future_match = (await waitForFutureMatch(event_key, matches, future)) as TEvent | null
    if (future_match !== null) {
      results.push(future_match)
    }
    return results
  }

  trimEventHistory(options: EventHistoryTrimOptions<TEvent> = {}): number {
    const max_history_size = normalizeMaxHistorySize(options.max_history_size ?? this.max_history_size)
    const max_history_drop = options.max_history_drop ?? this.max_history_drop
    if (max_history_size === null) {
      return 0
    }

    const is_event_complete = options.is_event_complete ?? ((event: TEvent) => event.event_status === 'completed')
    const on_remove = options.on_remove

    if (!max_history_drop || this.size <= max_history_size) {
      return 0
    }

    let remaining_overage = this.size - max_history_size
    let removed_count = 0
    const remove_event = (event_id: string, event: TEvent): void => {
      this._events.delete(event_id)
      on_remove?.(event)
      removed_count += 1
    }

    for (const [event_id, event] of Array.from(this._events.entries())) {
      if (remaining_overage <= 0) {
        break
      }
      if (!is_event_complete(event)) {
        continue
      }
      remove_event(event_id, event)
      remaining_overage -= 1
    }

    let dropped_uncompleted = 0
    for (const [event_id, event] of Array.from(this._events.entries())) {
      if (remaining_overage <= 0) {
        break
      }
      if (!is_event_complete(event)) {
        dropped_uncompleted += 1
      }
      remove_event(event_id, event)
      remaining_overage -= 1
    }

    if (dropped_uncompleted > 0 && !this._warned_about_dropping_uncompleted_events) {
      this._warned_about_dropping_uncompleted_events = true
      const owner_label = options.owner_label ?? 'EventBus'
      console.error(
        `[abxbus] ⚠️ Bus ${owner_label} has exceeded max_history_size=${max_history_size} and is dropping oldest history entries (even uncompleted events). Increase max_history_size or set max_history_drop=false to reject.`
      )
    }

    return removed_count
  }

  private eventIsChildOf(event: BaseEvent, ancestor: BaseEvent): boolean {
    let current_parent_id = event.event_parent_id
    const visited = new Set<string>()

    while (current_parent_id && !visited.has(current_parent_id)) {
      if (current_parent_id === ancestor.event_id) {
        return true
      }
      visited.add(current_parent_id)
      const parent = this._events.get(current_parent_id)
      if (!parent) {
        return false
      }
      current_parent_id = parent.event_parent_id
    }

    return false
  }
}
