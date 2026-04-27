# `abxbus`: 📢 Production-ready multi-language event bus

<img width="200" alt="image" src="https://github.com/user-attachments/assets/b3525c24-51ba-496c-b327-ccdfe46a7362" align="right" />

[![DeepWiki: Python](https://img.shields.io/badge/DeepWiki-abxbus%2FPython-yellow.svg?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACwAAAAyCAYAAAAnWDnqAAAAAXNSR0IArs4c6QAAA05JREFUaEPtmUtyEzEQhtWTQyQLHNak2AB7ZnyXZMEjXMGeK/AIi+QuHrMnbChYY7MIh8g01fJoopFb0uhhEqqcbWTp06/uv1saEDv4O3n3dV60RfP947Mm9/SQc0ICFQgzfc4CYZoTPAswgSJCCUJUnAAoRHOAUOcATwbmVLWdGoH//PB8mnKqScAhsD0kYP3j/Yt5LPQe2KvcXmGvRHcDnpxfL2zOYJ1mFwrryWTz0advv1Ut4CJgf5uhDuDj5eUcAUoahrdY/56ebRWeraTjMt/00Sh3UDtjgHtQNHwcRGOC98BJEAEymycmYcWwOprTgcB6VZ5JK5TAJ+fXGLBm3FDAmn6oPPjR4rKCAoJCal2eAiQp2x0vxTPB3ALO2CRkwmDy5WohzBDwSEFKRwPbknEggCPB/imwrycgxX2NzoMCHhPkDwqYMr9tRcP5qNrMZHkVnOjRMWwLCcr8ohBVb1OMjxLwGCvjTikrsBOiA6fNyCrm8V1rP93iVPpwaE+gO0SsWmPiXB+jikdf6SizrT5qKasx5j8ABbHpFTx+vFXp9EnYQmLx02h1QTTrl6eDqxLnGjporxl3NL3agEvXdT0WmEost648sQOYAeJS9Q7bfUVoMGnjo4AZdUMQku50McDcMWcBPvr0SzbTAFDfvJqwLzgxwATnCgnp4wDl6Aa+Ax283gghmj+vj7feE2KBBRMW3FzOpLOADl0Isb5587h/U4gGvkt5v60Z1VLG8BhYjbzRwyQZemwAd6cCR5/XFWLYZRIMpX39AR0tjaGGiGzLVyhse5C9RKC6ai42ppWPKiBagOvaYk8lO7DajerabOZP46Lby5wKjw1HCRx7p9sVMOWGzb/vA1hwiWc6jm3MvQDTogQkiqIhJV0nBQBTU+3okKCFDy9WwferkHjtxib7t3xIUQtHxnIwtx4mpg26/HfwVNVDb4oI9RHmx5WGelRVlrtiw43zboCLaxv46AZeB3IlTkwouebTr1y2NjSpHz68WNFjHvupy3q8TFn3Hos2IAk4Ju5dCo8B3wP7VPr/FGaKiG+T+v+TQqIrOqMTL1VdWV1DdmcbO8KXBz6esmYWYKPwDL5b5FA1a0hwapHiom0r/cKaoqr+27/XcrS5UwSMbQAAAABJRU5ErkJggg==)](https://deepwiki.com/ArchiveBox/abxbus) [![PyPI - Version](https://img.shields.io/pypi/v/abxbus)](https://pypi.org/project/abxbus/) [![GitHub License](https://img.shields.io/github/license/ArchiveBox/abxbus)](https://github.com/ArchiveBox/abxbus) ![GitHub last commit](https://img.shields.io/github/last-commit/ArchiveBox/abxbus)

[![DeepWiki: TS](https://img.shields.io/badge/DeepWiki-abxbus%2FTypescript-blue.svg?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACwAAAAyCAYAAAAnWDnqAAAAAXNSR0IArs4c6QAAA05JREFUaEPtmUtyEzEQhtWTQyQLHNak2AB7ZnyXZMEjXMGeK/AIi+QuHrMnbChYY7MIh8g01fJoopFb0uhhEqqcbWTp06/uv1saEDv4O3n3dV60RfP947Mm9/SQc0ICFQgzfc4CYZoTPAswgSJCCUJUnAAoRHOAUOcATwbmVLWdGoH//PB8mnKqScAhsD0kYP3j/Yt5LPQe2KvcXmGvRHcDnpxfL2zOYJ1mFwrryWTz0advv1Ut4CJgf5uhDuDj5eUcAUoahrdY/56ebRWeraTjMt/00Sh3UDtjgHtQNHwcRGOC98BJEAEymycmYcWwOprTgcB6VZ5JK5TAJ+fXGLBm3FDAmn6oPPjR4rKCAoJCal2eAiQp2x0vxTPB3ALO2CRkwmDy5WohzBDwSEFKRwPbknEggCPB/imwrycgxX2NzoMCHhPkDwqYMr9tRcP5qNrMZHkVnOjRMWwLCcr8ohBVb1OMjxLwGCvjTikrsBOiA6fNyCrm8V1rP93iVPpwaE+gO0SsWmPiXB+jikdf6SizrT5qKasx5j8ABbHpFTx+vFXp9EnYQmLx02h1QTTrl6eDqxLnGjporxl3NL3agEvXdT0WmEost648sQOYAeJS9Q7bfUVoMGnjo4AZdUMQku50McDcMWcBPvr0SzbTAFDfvJqwLzgxwATnCgnp4wDl6Aa+Ax283gghmj+vj7feE2KBBRMW3FzOpLOADl0Isb5587h/U4gGvkt5v60Z1VLG8BhYjbzRwyQZemwAd6cCR5/XFWLYZRIMpX39AR0tjaGGiGzLVyhse5C9RKC6ai42ppWPKiBagOvaYk8lO7DajerabOZP46Lby5wKjw1HCRx7p9sVMOWGzb/vA1hwiWc6jm3MvQDTogQkiqIhJV0nBQBTU+3okKCFDy9WwferkHjtxib7t3xIUQtHxnIwtx4mpg26/HfwVNVDb4oI9RHmx5WGelRVlrtiw43zboCLaxv46AZeB3IlTkwouebTr1y2NjSpHz68WNFjHvupy3q8TFn3Hos2IAk4Ju5dCo8B3wP7VPr/FGaKiG+T+v+TQqIrOqMTL1VdWV1DdmcbO8KXBz6esmYWYKPwDL5b5FA1a0hwapHiom0r/cKaoqr+27/XcrS5UwSMbQAAAABJRU5ErkJggg==)](https://deepwiki.com/ArchiveBox/abxbus/3-typescript-implementation) [![NPM Version](https://img.shields.io/npm/v/abxbus)](https://www.npmjs.com/package/abxbus)

AbxBus is an in-memory event bus library for async Python and TS (node/bun/deno/browser).

It's designed for quickly building resilient, predictable, complex event-driven apps.

It "just works" with an intuitive, but powerful event JSON format + emit API that's consistent across both languages and scales consistently from one event to millions (~0.2ms/event):

```python
bus.on(SomeEvent, some_function)
bus.emit(SomeEvent({some_data: 132}))
```

It's async native, has proper automatic nested event tracking, and powerful concurrency control options. The API is inspired by `EventEmitter` or [`emittery`](https://github.com/sindresorhus/emittery) in JS, but it takes it a step further:

- nice Zod / Pydantic schemas for events that can be exchanged between both languages
- automatic UUIDv7s and monotonic nanosecond timestamps for ordering events globally
- built in locking options to force strict global FIFO processing or fully parallel processing

---

♾️ It's inspired by the simplicity of async and events in `JS` but with baked-in features that allow to eliminate most of the tedious repetitive complexity in event-driven codebases:

- correct timeout enforcement across multiple levels of events, including cancellation of awaited/blocking child work when a parent times out
- ability to strongly type hint and enforce the return type of event handlers at compile-time
- ability to queue events on the bus, or inline await them for immediate execution like a normal function call
- handles ~5,000 events/sec/core in both languages, with ~2kb/event RAM consumed per event during active processing

## 🔗 Links

- Issue Tracker: https://github.com/ArchiveBox/abxbus/issues
- Documentation: https://abxbus.archivebox.io
- DeepWiki: https://deepwiki.com/ArchiveBox/abxbus
- PyPI: https://pypi.org/project/abxbus/
- NPM: https://www.npmjs.com/package/abxbus

<br/>

## 🔢 Quickstart

```bash
npm install abxbus
```

```ts
import { BaseEvent, EventBus } from 'abxbus'
import { z } from 'zod'

const CreateUserEvent = BaseEvent.extend('CreateUserEvent', {
  email: z.string(),
  event_result_type: z.object({ user_id: z.string() }),
})

const bus = new EventBus('MyAuthEventBus')

bus.on(CreateUserEvent, async (event) => {
  const user = await yourCreateUserLogic(event.email)
  return { user_id: user.id }
})

const event = bus.emit(CreateUserEvent({ email: 'someuser@example.com' }))
await event.done()
console.log(event.event_result) // { user_id: 'some-user-uuid' }
```

<br/>

---

<br/>

## ✨ Features

The features offered in TS are broadly similar to the ones offered in the python library.

- Typed events with Zod schemas (cross-compatible with Pydantic events from python library)
- FIFO event queueing with configurable concurrency
- Nested event support with automatic parent/child tracking
- Cross-bus forwarding with loop prevention
- Handler result tracking + validation + timeout enforcement
- History retention controls (`max_history_size`) for memory bounds
- Optional `@retry` decorator for easy management of per-handler retries, timeouts, and semaphore-limited execution

See the [Python README](../README.md) for more details.

<br/>

---

<br/>

## 📚 API Documentation

<details>
<summary><strong><code>EventBus</code></strong></summary>

The main bus class that registers handlers, schedules events, and tracks results.

Constructor:

```ts
new EventBus(name?: string, options?: {
  id?: string
  max_history_size?: number | null
  event_concurrency?: 'global-serial' | 'bus-serial' | 'parallel' | null
  event_timeout?: number | null
  event_slow_timeout?: number | null
  event_handler_concurrency?: 'serial' | 'parallel' | null
  event_handler_completion?: 'all' | 'first'
  event_handler_slow_timeout?: number | null
  event_handler_detect_file_paths?: boolean
})
```

#### Constructor options

| Option                            | Type                                                    | Default        | Purpose                                                                                                                                                              |
| --------------------------------- | ------------------------------------------------------- | -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`                              | `string`                                                | `uuidv7()`     | Override bus UUID (mostly for serialization/tests).                                                                                                                  |
| `max_history_size`                | `number \| null`                                        | `100`          | Max events kept in `event_history`; `null` = unbounded; `0` = keep only in-flight events and drop completed events immediately.                                      |
| `max_history_drop`                | `boolean`                                               | `false`        | If `true`, when history is full drop oldest history entries (including uncompleted if needed). If `false`, reject new emits when history reaches `max_history_size`. |
| `event_concurrency`               | `'global-serial' \| 'bus-serial' \| 'parallel' \| null` | `'bus-serial'` | Event-level scheduling policy.                                                                                                                                       |
| `event_handler_concurrency`       | `'serial' \| 'parallel' \| null`                        | `'serial'`     | Per-event handler scheduling policy.                                                                                                                                 |
| `event_handler_completion`        | `'all' \| 'first'`                                      | `'all'`        | Event completion mode if event does not override it.                                                                                                                 |
| `event_timeout`                   | `number \| null`                                        | `60`           | Default per-handler timeout budget in seconds (unless overridden).                                                                                                   |
| `event_handler_slow_timeout`      | `number \| null`                                        | `30`           | Slow handler warning threshold (seconds).                                                                                                                            |
| `event_slow_timeout`              | `number \| null`                                        | `300`          | Slow event warning threshold (seconds).                                                                                                                              |
| `event_handler_detect_file_paths` | `boolean`                                               | `true`         | Capture source file:line for handlers (slower, better logs).                                                                                                         |

#### Runtime state properties

- `id: string`
- `name: string`
- `label: string` (`${name}#${id.slice(-4)}`)
- `handlers: Map<string, EventHandler>`
- `handlers_by_key: Map<string, string[]>`
- `event_history: Map<string, BaseEvent>`
- `pending_event_queue: BaseEvent[]`
- `in_flight_event_ids: Set<string>`
- `locks: LockManager`

#### `on()`

```ts
on<T extends BaseEvent>(
  event_pattern: string | '*' | EventClass<T>,
  handler: EventHandlerCallable<T>,
  options?: Partial<EventHandler>
): EventHandler
```

Use during startup/composition to register handlers.

Advanced `options` fields, these can be used to override defaults per-handler if needed:

- `handler_timeout?: number | null` hard delay before handler execution is aborted with a `HandlerTimeoutError`
- `handler_slow_timeout?: number | null` delay before emitting a slow handler warning log line
- `handler_name?: string` optional name to use instead of `anonymous` if handler is an unnamed arrow function
- `handler_file_path?: string | null` optional path/to/source/file.js:lineno where the handler is defined, used for logging only
- `id?: string` unique UUID for the handler (normally a hash of bus_id + event_pattern + handler_name + handler_registered_at)

Notes:

- Prefer class/factory keys (`bus.on(MyEvent, handler)`) for typed payload/result inference.
- String and `'*'` matching are supported (`bus.on('MyEvent', ...)`, `bus.on('*', ...)`).
- Returns an `EventHandler` object you can later pass to `off()` to de-register the handler if needed.

#### `off()`

```ts
off<T extends BaseEvent>(
  event_pattern: EventPattern<T> | '*',
  handler?: EventHandlerCallable<T> | string | EventHandler
): void
```

Use when tearing down subscriptions (tests, plugin unload, hot-reload).

- Omit `handler` to remove all handlers for `event_pattern`.
- Pass handler function reference to remove one by function identity.
- Pass handler id (`string`) or `EventHandler` object to remove by id.
- use `bus.off('*')` to remove _all_ registered handlers from the bus

#### `emit()`

```ts
emit<T extends BaseEvent>(event: T): T
```

Behavior notes:

- Per-event config fields stay on the event as provided; when unset (`null`/`undefined`), each bus resolves its own defaults at processing time.
- If same event ends up forwarded through multiple buses, it is loop-protected using `event_path`.
- Emit is synchronous and returns immediately with the same event object (`event.event_status` is initially `'pending'`).

Normal lifecycle:

1. Create event instance (`const event = MyEvent({...})`).
2. Emit (`const queued = bus.emit(event)`).
3. Await with `await queued.done()` (immediate/queue-jump semantics) or `await queued.eventCompleted()` (bus queue order).
4. Inspect `queued.event_results`, `queued.event_result`, `queued.event_errors`, etc. if you need to access handler return values

#### `find()`

```ts
find<T extends BaseEvent>(event_pattern: EventPattern<T> | '*', options?: FindOptions): Promise<T | null>
find<T extends BaseEvent>(
  event_pattern: EventPattern<T> | '*',
  where: (event: T) => boolean,
  options?: FindOptions
): Promise<T | null>
```

Where:

```ts
type FindOptions = {
  past?: boolean | number // true to look through all past events, or number in seconds to filter time range
  future?: boolean | number // true to wait for event to appear indefinitely, or number in seconds to wait for event to appear
  child_of?: BaseEvent | null // filter to only match events that are a child_of: some_parent_event
} & {
  // event_status: 'pending' | 'started' | 'completed'
  // event_id: 'some-exact-event-uuid-here',
  // event_started_at: string | null (exact iso datetime string or null)
  // ... any event field can be passed to filter events using simple equality checks
  [key: string]: unknown
}
```

`bus.find()` returns the first matching event (in emit timestamp order).
To find multiple matching events, iterate through `bus.event_history.filter((event) => ...some condition...)` manually.

`where` behavior:
Any filter predicate function in the form of `(event) => true | false`, returning true to consider the event a match.

```ts
const matching_event = bus.find(SomeEvent, (event) => event.some_field == 123)
// or to match all event types:
const matching_event = bus.find('*', (event) => event.some_field == 123)
```

`past` behavior:

- `true`: search all history.
- `false`: skip searching past event history.
- `number`: search events emitted within last `N` seconds.

`future` behavior:

- `true`: wait forever for future match.
- `false`: do not wait.
- `number`: wait up to `N` seconds.

Lifecycle use:

- Use for idempotency / de-dupe before emit (`past: ...`).
- Use for synchronization/waiting (`future: ...`).
- Combine both to "check recent then wait".
- Add `child_of` to constrain by parent/ancestor event chain.
- Add any event field (e.g. `event_status`, `event_id`, `event_timeout`, `user_id`) to filter by strict equality.
- Use wildcard matching with predicates when you want to search all event types: `bus.find('*', (event) => ...)`.

Debouncing expensive events with `find()`:

```ts
const some_expensive_event = (await bus.find(ExpensiveEvent, { past: 15, future: 5 })) ?? bus.emit(ExpensiveEvent({}))
await some_expensive_event.done()
```

Important semantics:

- Past lookup matches any emitted events, not just completed events.
- Past/future matches resolve as soon as event is emitted. If you need the completed event, await `event.done()` or pass `{event_status: 'completed'}` to filter only for completed events.
- If both `past` and `future` are omitted, defaults are `past: true, future: false`.
- If both `past` and `future` are `false`, it returns `null` immediately.
- Detailed behavior matrix is covered in `abxbus-ts/tests/eventbus_find.test.ts`.

#### `waitUntilIdle(timeout?)`

`await bus.waitUntilIdle()` is the normal "drain bus work" call to wait until bus is done processing everything queued.
Pass an optional timeout in seconds (`await bus.waitUntilIdle(5)`) for a bounded wait.

```ts
bus.emit(OneEvent(...))
bus.emit(TwoEvent(...))
bus.emit(ThreeEvent(...))
await bus.waitUntilIdle()   // this resolves once all three events have finished processing
await bus.waitUntilIdle(5)  // wait up to 5 seconds, then continue even if work is still in-flight
```

#### Emit styles from handlers

Most handler code should use `await event.emit(ChildEvent({})).done()`. That creates a linked child and marks it as blocking parent completion.

| Style | `event_parent_id` | `event_blocks_parent_completion` | Blocks current handler? | Effect |
| --- | --- | --- | --- | --- |
| `await event.emit(ChildEvent({})).done()` | Parent event id | `true` | Yes | Linked child work; parent completion waits too. |
| `event.emit(ChildEvent({}))` without awaiting | Parent event id | `false` | No | Linked background child; visible in ancestry but parent completion does not wait. |
| `await bus.emit(TopLevelEvent({})).done()` | `null` | `false` | Yes | Detached top-level event; the handler waits naturally because it is awaited. |
| `bus.emit(TopLevelEvent({}))` without awaiting | `null` | `false` | No | True detached background event with no retained parent relationship. |

#### Parent/child/event lookup helpers

```ts
eventIsChildOf(child_event: BaseEvent, paret_event: BaseEvent): boolean
eventIsParentOf(parent_event: BaseEvent, child_event: BaseEvent): boolean
findEventById(event_id: string): BaseEvent | null
```

#### `toString()` / `toJSON()` / `fromJSON()`

```ts
toString(): string
toJSON(): EventBusJSON
EventBus.fromJSON(data: unknown): EventBus
```

- `toString()` returns `BusName#abcd` style labels used in logs/errors.
- `toJSON()` exports full bus state snapshot (config, handlers, indexes, event_history, pending queue, in-flight ids, find-waiter snapshots).
- `fromJSON()` restores a new bus instance from that payload (handler functions are restored as no-op stubs).

#### `logTree()`

```ts
logTree(): string
```

- `logTree()` returns a full event log hierarchy tree diagram for debugging.

#### `destroy()`

```ts
destroy(): void
```

- `destroy()` clears handlers/history/locks and removes this bus from global weak registry.
- `destroy()`/GC behavior is exercised in `abxbus-ts/tests/eventbus.test.ts` and `abxbus-ts/tests/eventbus_performance.test.ts`.

</details>

<details>
<summary><strong><code>BaseEvent</code></strong></summary>

Base class + factory builder for typed event models.

Define your own strongly typed events with `BaseEvent.extend('EventName', {...zod fields...})`:

```ts
const MyEvent = BaseEvent.extend('MyEvent', {
  some_key: z.string(),
  some_other_key: z.number(),
  // ...
  // any other payload fields you want to include can go here

  // fields that start with event_* are reserved for metadata used by the library
  event_result_type: z.string().optional(),
  event_timeout: 60,
  // ...
})

const pending_event = MyEvent({ some_key: 'abc', some_other_key: 234 })
const queued_event = bus.emit(pending_event)
const completed_event = await queued_event.done()
```

API behavior and lifecycle examples:

- `abxbus-ts/examples/simple.ts`
- `abxbus-ts/examples/immediate_event_processing.ts`
- `abxbus-ts/examples/forwarding_between_busses.ts`
- `abxbus-ts/tests/eventbus.test.ts`
- `abxbus-ts/tests/eventbus_find.test.ts`
- `abxbus-ts/tests/event_handler_first.test.ts`
- `abxbus-ts/tests/base_event_event_bus_proxy.test.ts`
- `abxbus-ts/tests/eventbus_timeout.test.ts`
- `abxbus-ts/tests/event_result.test.ts`

#### Event configuration fields

Special configuration fields you can set on each event to control processing:

- `event_result_type?: z.ZodTypeAny | String | Number | Boolean | Array | Object`
- `event_version?: string` (default: `'0.0.1'`; useful for your own schema/data migrations)
- `event_timeout?: number | null`
- `event_handler_timeout?: number | null`
- `event_handler_slow_timeout?: number | null`
- `event_concurrency?: 'global-serial' | 'bus-serial' | 'parallel' | null`
- `event_handler_concurrency?: 'serial' | 'parallel' | null`
- `event_handler_completion?: 'all' | 'first'`

#### Runtime state fields

- `event_id`, `event_type`, `event_version`
- `event_path: string[]` (bus labels like `BusName#ab12`)
- `event_parent_id: string | null`
- `event_emitted_by_handler_id: string | null`
- `event_blocks_parent_completion: boolean`
- `event_status: 'pending' | 'started' | 'completed'`
- `event_results: Map<string, EventResult>`
- `event_pending_bus_count: number`
- `event_created_at: string`
- `event_started_at: string | null`
- `event_completed_at: string | null`

#### Read-only attributes

- `event_parent` -> `BaseEvent | undefined`
- `event_children` -> `BaseEvent[]`
- `event_descendants` -> `BaseEvent[]`
- `event_errors` -> `Error[]`
- `event_result` -> `EventResultType<this> | undefined`

#### `done()`

```ts
done(options?: { raise_if_any?: boolean }): Promise<this>
```

- If called from inside a running handler, it queue-jumps child processing immediately.
- If called outside handler context, it waits for normal completion (or processes immediately if already next).
- Re-raises the first handler exception encountered after processing completes.
- Pass `{ raise_if_any: false }` to only wait for completion without re-raising handler exceptions.
- Rejects if event is not attached to a bus (`event has no bus attached`).
- Queue-jump behavior is demonstrated in `abxbus-ts/examples/immediate_event_processing.ts` and `abxbus-ts/tests/base_event_event_bus_proxy.test.ts`.

#### `eventCompleted()`

```ts
eventCompleted(): Promise<this>
```

- Waits for completion in normal runloop order.
- Use inside handlers when you explicitly do not want queue-jump behavior.

#### `first()`

```ts
first(): Promise<EventResultType<this> | undefined>
```

- Forces `event_handler_completion = 'first'` for this run.
- Returns temporally first non-`undefined` successful handler result.
- Cancels pending/running losing handlers on the same bus.
- Re-raises the first non-cancellation handler exception encountered after processing completes.
- Returns `undefined` only when no handler produces a successful non-`undefined` value and no handler raises.
- Cancellation and winner-selection behavior is covered in `abxbus-ts/tests/event_handler_first.test.ts`.

#### `eventResultsList(include?, options?)`

```ts
eventResultsList(
  include?: (result: EventResultType<this> | undefined, event_result: EventResult<this>) => boolean,
  options?: {
    timeout?: number | null
    include?: (result: EventResultType<this> | undefined, event_result: EventResult<this>) => boolean
    raise_if_any?: boolean
    raise_if_none?: boolean
  }
): Promise<Array<EventResultType<this> | undefined>>
```

- Returns handler result values in `event_results` order.
- Default filter includes completed non-`null`/non-`undefined` non-error, non-forwarded (`BaseEvent`) values.
- `raise_if_any` defaults to `true` and throws when any handler result has an error.
- `raise_if_none` defaults to `true` and throws when no results match `include`.
- `timeout` is in seconds and bounds how long to wait for completion.
- Examples:
  - `await event.eventResultsList({ raise_if_any: false, raise_if_none: false })`
  - `await event.eventResultsList((result) => typeof result === 'object', { raise_if_any: false })`

#### `eventResultUpdate(handler, options?)`

```ts
eventResultUpdate(
  handler: EventHandler | EventHandlerCallable<this>,
  options?: {
    eventbus?: EventBus
    status?: 'pending' | 'started' | 'completed' | 'error'
    result?: EventResultType<this> | BaseEvent | undefined
    error?: unknown
  }
): EventResult<this>
```

- Creates (if missing) or updates one `event_results` entry for the given handler id.
- Useful for deterministic seeding/rehydration paths before resuming normal dispatch.
- Example:
  - `const seeded = event.eventResultUpdate(handler_entry, { eventbus: bus, status: 'pending' })`
  - `seeded.update({ status: 'completed', result: 'seeded' })`

#### `reset()`

```ts
reset(): this
```

- Returns a fresh event copy with runtime state reset to pending so it can be emitted again safely.
- Original event object is unchanged.
- Generates a new UUIDv7 `event_id` for the returned copy.
- Clears runtime completion state (`event_results`, status/timestamps, captured async context, done signal, local bus binding).

#### `toString()` / `toJSON()` / `fromJSON()`

```ts
toString(): string
toJSON(): BaseEventData
BaseEvent.fromJSON(data: unknown): BaseEvent
EventFactory.fromJSON?.(data: unknown): TypedEvent
```

- JSON format is cross-language compatible with Python implementation.
- `event_result_type` is serialized as JSON Schema when possible and rehydrated on `fromJSON`.
- In TypeScript-only usage, `event_result_type` can be any Zod schema shape or base type like `number | string | boolean | etc.`. For cross-language roundtrips, object-like schemas (including Python `TypedDict`/`dataclass`-style shapes) are reconstructed on Python as Pydantic models, JSON object keys are always strings, and some fine-grained string-shape constraints may be normalized between Zod and Pydantic.
- Round-trip coverage is in `abxbus-ts/tests/event_result_typed_results.test.ts` and `abxbus-ts/tests/eventbus.test.ts`.

</details>

<details>
<summary><strong><code>EventResult</code></strong></summary>

Each handler execution creates one `EventResult` stored in `event.event_results`.

#### Main fields

- `id: string` (uuidv7 string)
- `status: 'pending' | 'started' | 'completed' | 'error'`
- `event: BaseEvent`
- `handler: EventHandler`
- `result: EventResultType<this> | undefined`
- `error: unknown | undefined`
- `started_at: string | null` (ISO datetime string)
- `completed_at: string | null` (ISO datetime string)
- `event_children: BaseEvent[]`

#### Read-only getters

- `event_id` -> `string` uuiv7 of the event the result is for
- `bus` -> `EventBus` instance it's associated with
- `handler_id` -> `string` uuidv5 of the `EventHandler`
- `handler_name` -> `string | 'anonymous'` function name of the handler method
- `handler_file_path` -> `string | null` path/to/file.js:lineno where the handler method is defined
- `eventbus_name` -> `string` name, same as `this.bus.name`
- `eventbus_id` -> `string` uuidv7, same as `this.bus.id`
- `eventbus_label` -> `string` label, same as `this.bus.label`
- `value` -> `EventResultType<this> | undefined` alias of `this.result`
- `raw_value` -> `any` raw result value before schema validation, available when handler return value validation fails
- `handler_timeout` -> `number` seconds before handler execution is aborted (precedence: handler config -> event config -> bus level defaults)
- `handler_slow_timeout` -> `number` seconds before logging a slow execution warning (same prececence as `handler_timeout`)

#### `toString()` / `toJSON()` / `fromJSON()`

```ts
toString(): string
toJSON(): EventResultJSON
EventResult.fromJSON(event, data): EventResult
```

</details>

<details>
<summary><strong><code>EventHandler</code></strong></summary>

Represents one registered handler entry on a bus. You usually get these from `bus.on(...)`, then pass them to `bus.off(...)` to remove.

#### Main fields

- `id` unique handler UUIDv5 (deterministic hash from bus/event/handler metadata unless overridden)
- `handler` function reference that executes for matching events
- `handler_name` function name (or `'anonymous'`)
- `handler_file_path` detected source path (`~/path/file.ts:line`) or `null`
- `handler_timeout` optional timeout override in seconds (`null` disables timeout limit)
- `handler_slow_timeout` optional slow-warning threshold in seconds (`null` disables slow warning)
- `handler_registered_at` ISO timestamp
- `event_pattern` subscribed key (`'SomeEvent'` or `'*'`)
- `eventbus_name` bus name where this handler was registered
- `eventbus_id` bus UUID where this handler was registered

#### `toString()` / `toJSON()` / `fromJSON()`

```ts
toString(): string
toJSON(): EventHandlerJSON
EventHandler.fromJSON(data: unknown, handler?: EventHandlerCallable): EventHandler
```

- `toString()` returns `handlerName() (path:line)` when path/name are available, otherwise `function#abcd()`.
- `toJSON()` emits only serializable handler metadata (never function bodies).
- `fromJSON()` reconstructs the handler entry and accepts an optional real function to re-bind execution behavior.

<br/>

---

<br/>

</details>

## 🏃 Runtimes

`abxbus-ts` supports all major JS runtimes.

- Node.js (default development and test runtime)
- Browsers (ESM)
- Bun
- Deno

### Browser support notes

- The package output is ESM (`./dist/esm`) which is supported by all browsers [released after 2018](https://caniuse.com/?search=ESM)
- `AsyncLocalStorage` is preserved at emit and used during handling when available (Node/Bun), otel/tracing context will work normally in those environments

### Performance comparison (local run, per-event)

Measured locally on an `Apple M4 Pro` with:

- `pnpm run perf:node` (`node v22.21.1`)
- `pnpm run perf:bun` (`bun v1.3.9`)
- `pnpm run perf:deno` (`deno v2.6.8`)
- `pnpm run perf:browser` (`chrome v145.0.7632.6`)

| Runtime            | 1 bus x 50k events x 1 handler | 500 buses x 100 events x 1 handler | 1 bus x 1 event x 50k parallel handlers | 1 bus x 50k events x 50k one-off handlers | Worst case (N buses x N events x N handlers) |
| ------------------ | ------------------------------ | ---------------------------------- | --------------------------------------- | ----------------------------------------- | -------------------------------------------- |
| Node               | `0.015ms/event`, `0.6kb/event` | `0.058ms/event`, `0.1kb/event`     | `0.021ms/handler`, `3.8kb/handler`      | `0.028ms/event`, `0.6kb/event`            | `0.442ms/event`, `0.9kb/event`               |
| Bun                | `0.011ms/event`, `2.5kb/event` | `0.054ms/event`, `1.0kb/event`     | `0.006ms/handler`, `4.5kb/handler`      | `0.019ms/event`, `2.8kb/event`            | `0.441ms/event`, `3.1kb/event`               |
| Deno               | `0.018ms/event`, `1.2kb/event` | `0.063ms/event`, `0.4kb/event`     | `0.024ms/handler`, `3.1kb/handler`      | `0.064ms/event`, `2.6kb/event`            | `0.461ms/event`, `7.9kb/event`               |
| Browser (Chromium) | `0.030ms/event`                | `0.197ms/event`                    | `0.022ms/handler`                       | `0.022ms/event`                           | `1.566ms/event`                              |

Notes:

- `kb/event` is peak RSS delta per event during active processing (most representative of OS-visible RAM in Activity Monitor / Task Manager, with `EventBus.max_history_size=1`)
- In `1 bus x 1 event x 50k parallel handlers` stats are shown per-handler for clarity, `0.02ms/handler * 50k handlers ~= 1000ms` for the entire event
- Browser runtime does not expose memory usage directly, in practice memory performance in-browser is comparable to Node (they both use V8)

<br/>

---

<br/>

## 👾 Development

```bash
git clone https://github.com/ArchiveBox/abxbus abxbus && cd abxbus

cd ./abxbus-ts
pnpm install

prek install           # install pre-commit hooks
prek run --all-files   # run pre-commit hooks on all files manually

pnpm lint
pnpm test
```
