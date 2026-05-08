# TODO: Rust Core + Thin Language Bindings

This plan describes the target architecture where all correctness-sensitive
EventBus behavior lives in one Rust core, and Python/TypeScript/Go/Elixir
packages are thin native wrappers around that core.

The goal is to avoid reimplementing queues, locks, completion semantics,
timeouts, cancellation, EventStore behavior, and cross-language transport in
each language.

## Goals

- One canonical Rust implementation of core bus behavior.
- Thin language wrappers that provide native ergonomics and handler callbacks.
- No alternate Python/TypeScript/Go/Elixir local runtimes.
- Deterministic behavior matching current Python and TypeScript behavior.
- Local embedded mode with very low overhead.
- Shared cross-process mode over Unix domain sockets.
- Network-capable transport later without changing language wrapper semantics.
- A conformance suite that proves all bindings observe the same core behavior.

## Non-Goals

- Do not keep Python/TypeScript lock managers as independent sources of truth.
- Do not make every language reimplement the scheduler.
- Do not make NATS/Kafka/Redis/Postgres the protocol itself.
- Do not rely on middleware observer hooks to implement distributed execution.
- Do not require Rust to directly call arbitrary language handlers through raw
  callbacks in a way that owns language async runtime semantics.

## Architecture Summary

There is one core:

```text
abxbus-core (Rust)
  - EventStore
  - scheduler
  - lock manager
  - timeout/cancellation manager
  - event/result state machine
  - handler registry metadata
  - transport/session protocol
  - state patch stream
```

Each language package provides only:

```text
abxbus-py / abxbus-ts / abxbus-go / abxbus-elixir
  - native EventBus API
  - native event class/schema ergonomics
  - native handler callable registry
  - native handler invocation when Rust core asks for it
  - native exception/result serialization
  - mirrored read-only-ish event/result object views
```

The central rule:

```text
Hosts execute user code.
Rust core commits state.
```

Language wrappers may produce handler outcomes, but the Rust core decides
whether the outcome is accepted, stale, timed out, cancelled, or ignored.

## Deployment Modes

### Embedded Mode

One process embeds `abxbus-core`:

```text
Python process
  Python wrapper
    -> Rust core via PyO3
```

```text
Node process
  TypeScript wrapper
    -> Rust core via N-API
```

Good for normal single-process use and for keeping local overhead low.

### Daemon Mode

Multiple processes connect to one Rust daemon:

```text
python client -> /tmp/sharedbus.sock -> abxbusd
node client   -> /tmp/sharedbus.sock -> abxbusd
go client     -> /tmp/sharedbus.sock -> abxbusd
```

Good for cross-language and cross-process use.

### Network Mode

The same core protocol runs over TCP, QUIC, or a broker-backed transport:

```text
client core session -> network transport -> leader core session
```

NATS/Kafka/Redis/Postgres can become EventStore or transport backends, but the
bus protocol remains owned by the Rust core.

## Language Wrapper API

The public API should stay native:

```python
bus = EventBus("shared", schoolbus="/tmp/sharedbus.sock")

@bus.on(TestEvent)
async def on_test(event):
    print(event.test)

event = bus.emit(TestEvent(test="hi"))
await event
assert event.event_completed_at is not None
```

```ts
const bus = new EventBus("shared", { schoolbus: "/tmp/sharedbus.sock" })

bus.on(TestEvent, async (event) => {
  console.log(event.test)
})

const event = bus.emit(TestEvent({ test: "hi" }))
await event.done()
```

Internally:

```text
EventBus.emit()
  -> core.emit_event(...)
  -> returns native Event view backed by core event_id

Event.done() / await event
  -> core.wait_event_completed(event_id)

EventResult fields
  -> updated from core patch stream
```

## Core Data Model

The Rust core owns canonical records.

```rust
struct BusRecord {
    bus_id: BusId,
    name: String,
    label: String,
    defaults: BusDefaults,
    host_id: HostId,
}

struct BusDefaults {
    event_concurrency: EventConcurrency,
    event_handler_concurrency: HandlerConcurrency,
    event_handler_completion: HandlerCompletion,
    event_timeout: Option<Duration>,
    event_slow_timeout: Option<Duration>,
    event_handler_timeout: Option<Duration>,
    event_handler_slow_timeout: Option<Duration>,
}

struct HandlerRecord {
    handler_id: HandlerId,
    bus_id: BusId,
    host_id: HostId,
    event_pattern: EventPattern,
    handler_name: String,
    handler_file_path: Option<String>,
    handler_registered_at: Timestamp,
    handler_timeout: Option<Duration>,
    handler_slow_timeout: Option<Duration>,

    // Future extension. Current Python/TS mostly resolve handler concurrency
    // from event override or bus default.
    handler_concurrency: Option<HandlerConcurrency>,
    handler_completion: Option<HandlerCompletion>,
}

struct EventRecord {
    event_id: EventId,
    event_type: String,
    event_version: String,
    payload: Value,

    event_timeout: Option<Duration>,
    event_slow_timeout: Option<Duration>,
    event_concurrency: Option<EventConcurrency>,
    event_handler_timeout: Option<Duration>,
    event_handler_slow_timeout: Option<Duration>,
    event_handler_concurrency: Option<HandlerConcurrency>,
    event_handler_completion: Option<HandlerCompletion>,

    event_parent_id: Option<EventId>,
    event_emitted_by_result_id: Option<ResultId>,
    event_emitted_by_handler_id: Option<HandlerId>,
    event_blocks_parent_completion: bool,
    event_path: Vec<BusId>,

    event_created_at: Timestamp,
    event_started_at: Option<Timestamp>,
    event_completed_at: Option<Timestamp>,
    event_status: EventStatus,
}

struct EventRouteRecord {
    route_id: RouteId,
    event_id: EventId,
    bus_id: BusId,
    route_seq: u64,
    status: RouteStatus,
    handler_cursor: usize,
    serial_handler_paused_by: Option<InvocationId>,
    event_lock_lease: Option<LockLease>,
}

struct EventResultRecord {
    result_id: ResultId,
    event_id: EventId,
    route_id: RouteId,
    bus_id: BusId,
    handler_id: HandlerId,
    handler_seq: usize,

    status: ResultStatus,
    result: Option<Value>,
    error: Option<CoreError>,
    started_at: Option<Timestamp>,
    completed_at: Option<Timestamp>,

    timeout: Option<Duration>,
    event_children: Vec<EventId>,

    // Future extension: per-result overrides, useful for retry/semaphore or
    // generated/synthetic handler results.
    handler_concurrency: Option<HandlerConcurrency>,
    handler_completion: Option<HandlerCompletion>,

    invocation: Option<InvocationState>,
}
```

## Default Resolution Rules

The current Python and TypeScript behavior must remain the gold standard.

Defaults are resolved when a bus route processes an event, not copied onto the
event at emit time.

```rust
fn resolve_event_concurrency(event: &EventRecord, route: &EventRouteRecord, bus: &BusRecord) -> EventConcurrency {
    event.event_concurrency.unwrap_or(bus.defaults.event_concurrency)
}

fn resolve_handler_concurrency(
    event: &EventRecord,
    result: &EventResultRecord,
    handler: &HandlerRecord,
    bus: &BusRecord,
) -> HandlerConcurrency {
    result.handler_concurrency
        .or(handler.handler_concurrency)
        .or(event.event_handler_concurrency)
        .unwrap_or(bus.defaults.event_handler_concurrency)
}

fn resolve_handler_completion(
    event: &EventRecord,
    result: &EventResultRecord,
    handler: &HandlerRecord,
    bus: &BusRecord,
) -> HandlerCompletion {
    result.handler_completion
        .or(handler.handler_completion)
        .or(event.event_handler_completion)
        .unwrap_or(bus.defaults.event_handler_completion)
}

fn resolve_handler_timeout(
    event: &EventRecord,
    handler: &HandlerRecord,
    bus: &BusRecord,
) -> Option<Duration> {
    min_non_null([
        handler.handler_timeout,
        event.event_handler_timeout,
        event.event_timeout,
        bus.defaults.event_handler_timeout,
        bus.defaults.event_timeout,
    ])
}
```

Important compatibility rules:

- `None` / `null` on the event means "defer to processing bus default".
- Explicit event fields beat bus defaults.
- Forwarded events use the processing bus defaults unless the event explicitly
  overrides the option.
- Event fields should remain unset in public event objects when users left them
  unset; resolved values are runtime decisions, not user data mutation.

## Lock Resources

The Rust lock manager uses named resources, leases, and fencing tokens.

```rust
enum LockResource {
    EventGlobal,
    EventBus(BusId),
    HandlerEvent(EventId),
    Custom(String),
}

struct LockLease {
    resource: LockResource,
    owner: InvocationId,
    fence: u64,
    lease_expires_at: Timestamp,
    suspended: bool,
}
```

Resource resolution:

```rust
fn event_lock_resource(mode: EventConcurrency, bus_id: BusId) -> Option<LockResource> {
    match mode {
        EventConcurrency::GlobalSerial => Some(LockResource::EventGlobal),
        EventConcurrency::BusSerial => Some(LockResource::EventBus(bus_id)),
        EventConcurrency::Parallel => None,
    }
}

fn handler_lock_resource(mode: HandlerConcurrency, event_id: EventId) -> Option<LockResource> {
    match mode {
        HandlerConcurrency::Serial => Some(LockResource::HandlerEvent(event_id)),
        HandlerConcurrency::Parallel => None,
    }
}
```

The core lock manager must guarantee:

- FIFO waiter order per lock resource.
- No new acquire slips ahead of an existing waiter.
- Reentrant/borrowed event lock behavior for queue-jump on the initiating bus.
- Fenced release and completion writes.
- Lease expiry takeover in distributed mode.
- Stale writers cannot complete a result after losing a lease.

## Scheduler + Lock Manager Split

Locks prevent illegal overlap, but locks alone do not preserve all semantics.

The scheduler decides eligibility:

```rust
fn result_is_eligible(
    event: &EventRecord,
    route: &EventRouteRecord,
    result: &EventResultRecord,
    handler: &HandlerRecord,
    bus: &BusRecord,
) -> bool {
    if result.status != ResultStatus::Pending {
        return false;
    }

    if event_has_first_winner(event.event_id, route.route_id) {
        return false;
    }

    let concurrency = resolve_handler_concurrency(event, result, handler, bus);

    match concurrency {
        HandlerConcurrency::Parallel => true,
        HandlerConcurrency::Serial => {
            result.handler_seq == route.handler_cursor
                && route.serial_handler_paused_by.is_none()
        }
    }
}
```

This distinction is critical for queue-jump:

- The parent handler may temporarily release its handler lock while awaiting a
  child.
- Releasing that lock must not allow the parent's next handler to start.
- Therefore serial handler order is enforced by `handler_cursor` and
  `serial_handler_paused_by`, not only by the lock.

## Event Processing Pseudocode

```rust
async fn process_event_route(core: &mut Core, route_id: RouteId, qj: Option<QueueJumpContext>) {
    let route = core.store.route(route_id);
    let bus = core.store.bus(route.bus_id);
    let event = core.store.event(route.event_id);

    let event_mode = resolve_event_concurrency(&event, &route, &bus);
    let event_resource = event_lock_resource(event_mode, route.bus_id);
    let event_owner = InvocationId::event_route(route_id);

    let event_lease = core.locks.acquire(event_resource, event_owner, qj.as_ref()).await;
    core.store.set_route_event_lease(route_id, event_lease);

    core.mark_event_started_if_needed(event.event_id);
    core.mark_route_started(route_id);
    core.create_missing_result_rows(route_id);

    loop {
        let runnable = core.scheduler.eligible_results(route_id);
        if runnable.is_empty() {
            break;
        }

        let completion = core.scheduler.resolved_completion_for_route(route_id);
        let concurrency = core.scheduler.resolved_handler_concurrency_for_route(route_id);

        match concurrency {
            HandlerConcurrency::Serial => {
                let result_id = runnable[0];
                core.run_one_handler_result(result_id).await;

                if completion == HandlerCompletion::First && core.event_has_first_winner(route_id) {
                    core.cancel_remaining_results(route_id, CancelReason::FirstResultWon).await;
                    break;
                }
            }
            HandlerConcurrency::Parallel => {
                core.start_all_handler_results(runnable).await;
                core.wait_for_parallel_group(route_id, completion).await;

                if completion == HandlerCompletion::First && core.event_has_first_winner(route_id) {
                    core.cancel_remaining_results(route_id, CancelReason::FirstResultWon).await;
                    break;
                }
            }
        }
    }

    core.locks.release_route_event_lease(route_id);
    core.complete_route_if_done(route_id);
    core.complete_event_if_done(route.event_id);
}
```

## Handler Invocation Protocol

Rust core asks the host to run a handler. The host never commits final state.

```rust
struct InvokeHandler {
    invocation_id: InvocationId,
    result_id: ResultId,
    event_id: EventId,
    bus_id: BusId,
    handler_id: HandlerId,
    fence: u64,
    deadline_at: Option<Timestamp>,
    cancel_token: CancelToken,
    event_snapshot: EventEnvelope,
    result_snapshot: EventResultEnvelope,
}

enum HandlerOutcome {
    Completed { value: Value },
    Errored { error: HostErrorEnvelope },
    Cancelled { reason: CancelReason },
}
```

Core acceptance rule:

```rust
fn accept_handler_outcome(core: &mut Core, msg: HandlerOutcomeMessage) -> Result<(), StaleWrite> {
    let result = core.store.result(msg.result_id);
    let invocation = result.invocation.as_ref().ok_or(StaleWrite)?;

    if invocation.invocation_id != msg.invocation_id {
        return Err(StaleWrite);
    }
    if invocation.fence != msg.fence {
        return Err(StaleWrite);
    }
    if result.status != ResultStatus::Started {
        return Err(StaleWrite);
    }
    if core.clock.now() > invocation.deadline_at.unwrap_or(Timestamp::MAX) {
        return Err(StaleWrite);
    }

    core.commit_handler_outcome(msg);
    Ok(())
}
```

## Timeout, Slow Warning, and Cancellation Ownership

Rust core owns:

- event timeout timers
- handler timeout timers
- slow event warnings
- slow handler warnings
- cancellation graph
- canonical error taxonomy
- stale completion rejection

Host wrappers own only best-effort interruption of native code.

```text
Core timer fires first:
  result.status = error
  result.error = EventHandlerTimeoutError(...)
  result.completed_at = core timestamp
  send CancelInvocation(invocation_id)
  release locks / advance cursors
  reject later host completion

Host returns first:
  core validates invocation_id + fence + deadline
  core commits completion/error
  core cancels losers if first-mode winner exists
```

Cancellation reasons:

```rust
enum CancelReason {
    HandlerTimeout,
    EventTimeout,
    ParentTimeout,
    FirstResultWon,
    BusShutdown,
    HostDisconnected,
    LeaseExpired,
    ExplicitCancel,
}
```

Host mappings:

- Python: cancel asyncio task when possible; sync handlers are cooperative unless
  isolated.
- TypeScript: AbortSignal/Promise race; CPU-bound handlers require worker/process
  isolation.
- Go: `context.Context`.
- Elixir: task shutdown/kill where safe.

Two execution modes:

```text
soft-cancel:
  core commits cancellation
  host tries to interrupt
  late host outcome ignored

hard-kill:
  host handlers run in isolated worker process
  core terminates worker on timeout/cancel
```

## Queue-Jump Semantics

Current Python/TypeScript behavior:

- `event.done()` / awaiting an event from inside a handler can queue-jump an
  awaited child.
- The awaited child blocks parent completion.
- Unawaited children retain lineage but do not block parent completion.
- Queue-jump should preempt queued siblings on the same initiating bus.
- Queue-jump should not double-run in-flight events.
- Queue-jump across buses processes only buses in the event path.
- On the initiating bus, the child may borrow/bypass the parent-held event lock.
- On forward buses, event locks are respected unless they resolve to the exact
  same global lock.
- Handler locks are not bypassed. The parent handler lock is temporarily yielded,
  the child runs normally, then the parent handler lock is reacquired.

Core pseudocode:

```rust
async fn await_event_from_handler(
    core: &mut Core,
    parent_invocation_id: InvocationId,
    child_event_id: EventId,
) {
    core.mark_child_blocks_parent_completion(parent_invocation_id, child_event_id);

    let qj = core.build_queue_jump_context(parent_invocation_id, child_event_id);

    let suspended = core.locks.suspend_parent_handler_locks(parent_invocation_id);
    core.store.pause_parent_serial_route(parent_invocation_id);

    for route_id in core.routes_for_event_path_order(child_event_id) {
        core.process_event_route(route_id, Some(qj.clone())).await;
    }

    core.wait_event_completed(child_event_id).await;

    core.store.unpause_parent_serial_route(parent_invocation_id);
    core.locks.reclaim_parent_handler_locks(suspended).await;
}
```

Queue-jump context:

```rust
struct QueueJumpContext {
    parent_invocation_id: InvocationId,
    initiating_bus_id: BusId,
    borrowed_event_locks: HashMap<LockResource, LockLease>,
    depth: usize,
}
```

Recursion guard:

```rust
if qj.depth > bus.max_handler_recursion_depth {
    core.cancel_result(parent_invocation_id, CoreError::InfiniteLoopDetected);
}
```

## First-Mode Semantics

Gold-standard behavior:

- First-mode winner is a completed result with no error and a non-null,
  non-BaseEvent result.
- Falsy values like `0`, `false`, and empty string are valid winners.
- `None` / `undefined` is not a winner.
- A returned event object is not a first-mode value winner.
- Serial first-mode runs handlers in order until a winner appears.
- Parallel first-mode starts eligible handlers concurrently and cancels losers
  once a winner commits.
- Pending or started non-winners are marked cancelled/error.

Core predicate:

```rust
fn is_first_mode_winner(result: &EventResultRecord) -> bool {
    result.status == ResultStatus::Completed
        && result.error.is_none()
        && result.result.is_some()
        && !result.result_is_event_reference
}
```

Serial first-mode:

```rust
while let Some(result_id) = next_serial_result(route_id) {
    run_one_handler_result(result_id).await;
    if is_first_mode_winner(result(result_id)) {
        cancel_remaining_results(route_id, CancelReason::FirstResultWon).await;
        break;
    }
}
```

Parallel first-mode:

```rust
start_all_eligible_results(route_id).await;

loop {
    let changed = wait_next_result_change(route_id).await;
    if is_first_mode_winner(result(changed.result_id)) {
        cancel_remaining_results(route_id, CancelReason::FirstResultWon).await;
        break;
    }
    if all_results_terminal(route_id) {
        break;
    }
}
```

## Event Completion Semantics

An event completes only when:

- all route records for the event are terminal;
- all handler results required by each route are terminal;
- no first-mode pending/started results remain uncancelled after a winner;
- all awaited child events with `event_blocks_parent_completion = true` are
  completed;
- no active lease can still commit a result for that event.

```rust
fn complete_event_if_done(core: &mut Core, event_id: EventId) {
    if !all_routes_done(event_id) {
        return;
    }
    if !all_blocking_children_done(event_id) {
        return;
    }

    core.store.mark_event_completed(event_id, core.clock.now());
    core.patch_bus.broadcast(EventPatch::Completed(event_id));
    core.try_complete_parent_chain(event_id);
}
```

## Host Binding Boundary

Each language binding implements a small host interface:

```rust
trait HostRuntime {
    async fn invoke_handler(&self, msg: InvokeHandler) -> HandlerOutcome;
    async fn cancel_invocation(&self, invocation_id: InvocationId, reason: CancelReason);
    async fn apply_patch(&self, patch: CorePatch);
}
```

Wrapper responsibilities:

- Register handler callable locally by `handler_id`.
- Convert native event classes to/from `EventEnvelope`.
- Invoke native handler when core sends `InvokeHandler`.
- Catch native exceptions and serialize them.
- Cooperatively cancel native invocation when core sends cancellation.
- Apply patches to native mirrored event/result views.

Wrappers must not:

- schedule handlers themselves;
- mark results completed themselves;
- decide event completion;
- decide timeout/cancellation winners;
- implement lock or queue semantics.

## FFI Packaging

Suggested bindings:

- Python: PyO3 + maturin.
- TypeScript/Node: N-API via `napi-rs`.
- Go: cgo for embedded core, or pure protocol client for daemon mode.
- Elixir: Rustler NIF for embedded mode, or port/daemon client for safer
  process isolation.

Shared generated schema:

- Use Rust structs as schema source.
- Generate JSON Schema / TypeScript types / Python Pydantic models / Go structs
  / Elixir structs where practical.
- Keep wire envelopes stable and versioned.

## Transport Protocol

Protocol is transport-agnostic. Initial local transport:

- embedded direct calls;
- Unix domain socket for daemon mode.

Later transports:

- TCP for LAN;
- QUIC for multiplexed network mode;
- NATS/Kafka as optional transport or EventStore backend;
- Redis/Postgres/SQLite as EventStore backends.

Message families:

```text
session:
  hello
  register_bus
  register_handler
  unregister_handler
  heartbeat

events:
  emit_event
  forward_event
  await_event
  wait_event_completed

invocation:
  invoke_handler
  handler_started
  handler_completed
  handler_errored
  cancel_invocation

patches:
  event_patch
  result_patch
  warning_patch
  bus_idle_patch
```

Every state-changing message includes:

- core protocol version;
- session id;
- event id / result id as applicable;
- invocation id as applicable;
- fence token where applicable.

## EventStore Backends

Core should start with an in-memory EventStore, then grow backends.

Required EventStore operations:

```rust
trait EventStore {
    fn insert_event(&mut self, event: EventRecord) -> Result<()>;
    fn insert_route(&mut self, route: EventRouteRecord) -> Result<()>;
    fn insert_result(&mut self, result: EventResultRecord) -> Result<()>;

    fn claim_lock(&mut self, resource: LockResource, owner: InvocationId) -> Result<LockLease>;
    fn renew_lock(&mut self, lease: &LockLease) -> Result<()>;
    fn release_lock(&mut self, lease: &LockLease) -> Result<()>;

    fn complete_result_fenced(&mut self, outcome: HandlerOutcomeCommit) -> Result<()>;
    fn complete_route_if_done(&mut self, route_id: RouteId) -> Result<()>;
    fn complete_event_if_done(&mut self, event_id: EventId) -> Result<()>;

    fn append_patch(&mut self, patch: CorePatch) -> Result<()>;
}
```

Backend order:

1. In-memory core store.
2. SQLite local durable store.
3. Redis low-latency shared store.
4. Postgres durable network store.
5. NATS/Kafka broker-native mapping if still useful.

## Conformance Tests

The Rust core must pass behavior copied from current Python and TypeScript tests.

Required categories:

- event concurrency:
  - global-serial serializes across buses;
  - bus-serial serializes per bus but overlaps across buses;
  - parallel overlaps on same bus;
  - event override beats bus default.

- handler concurrency:
  - serial handlers preserve order per event;
  - parallel handlers overlap;
  - event override beats processing bus default;
  - forwarded event uses processing bus default.

- completion modes:
  - all waits for all handlers;
  - first serial stops at first winner;
  - first parallel cancels losers;
  - `0`, `false`, and `""` are winners;
  - `null` / `undefined` is not a winner;
  - returned event reference is not a winner.

- queue-jump:
  - awaited child preempts queued sibling on initiating bus;
  - already in-flight event is not double-run;
  - nested awaited children complete inside-out;
  - multiple awaits on same event work;
  - forward bus bus-serial lock is respected;
  - fully parallel forward bus starts immediately;
  - parallel events + serial handlers overlap across events;
  - handler locks are yielded/reclaimed, not bypassed.

- timeout/cancellation:
  - handler timeout marks result error;
  - event timeout is a hard cap;
  - first-mode losers are cancelled;
  - late host completions after timeout are rejected;
  - parent timeout cancels awaited children but not unawaited children.

- lifecycle:
  - event fields remain unset when defaulted;
  - route-level resolved defaults do not mutate user fields;
  - middleware/state patches fire in deterministic order;
  - wait-until-idle waits for queues, in-flight invocations, and patch delivery.

## Migration Plan

### Phase 0: Spec and Fixtures

- Freeze current Python and TypeScript behavioral tests as conformance fixtures.
- Define stable event/result/handler JSON envelopes.
- Define error taxonomy and state transition table.
- Define patch stream format.

### Phase 1: Rust Core In-Memory Prototype

- Implement Rust records and in-memory EventStore.
- Implement deterministic scheduler and lock manager.
- Implement handler invocation through a mock host.
- Port core concurrency, completion, timeout, and queue-jump tests to Rust.

### Phase 2: Python Binding

- Replace Python runtime internals with PyO3 calls.
- Keep Python API and event class ergonomics.
- Implement Python host callback registry.
- Mirror core patches onto Python event/result views.
- Delete or deprecate Python lock manager/runtime code after parity.

### Phase 3: TypeScript Binding

- Replace TypeScript runtime internals with N-API core calls.
- Keep TypeScript event class ergonomics and type inference.
- Implement JS host callback registry and AbortSignal cancellation bridge.
- Mirror patches onto TS event/result views.

### Phase 4: Daemon Mode

- Implement `abxbusd`.
- Implement UDS transport.
- Allow embedded core to connect to daemon for shared bus mode.
- Ensure first starter can spawn or bind daemon, but leader election can remain
  out of scope initially.

### Phase 5: Go and Elixir

- Add thin wrappers against the same core protocol.
- Prefer daemon/protocol clients first if embedded FFI packaging is costly.
- Add embedded mode once API shape stabilizes.

### Phase 6: Durable / Network Backends

- Add SQLite durable local store.
- Add Redis/Postgres shared stores.
- Add TCP/QUIC transport.
- Evaluate NATS/Kafka only after the core state machine is stable.

## Open Questions

- Should handler callable identity include language/runtime id in addition to
  bus id and file path?
- How should class-specific event schemas be versioned across languages?
- Should daemon mode always use process-isolated handlers for hard cancellation,
  or only opt in?
- How much of retry/semaphore should move into core as custom lock resources?
- Should `find(future=...)` remain host-local or become a core waiter primitive?
- What is the exact patch ordering contract for middleware hooks?
- How should host crashes map to error type and retry/reclaim behavior?

## Design Principle

If behavior affects correctness, Rust core owns it.

If behavior affects language ergonomics, the language wrapper owns it.

If behavior affects both, the Rust core owns the canonical state and the wrapper
renders that state in the host language.
