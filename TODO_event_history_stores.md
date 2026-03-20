# TODO: Distributed EventHistory Stores (PostgreSQL + Redis + SQLite + NATS + Kafka)

This plan defines a swappable `EventHistory` backend that owns both:
- event history state
- queue/claim/lock state

Goal: allow multiple nodes/processes to collaborate on the same bus workload without conflicting claims.

This document is implementation planning only.

## Scope

- Add a backend interface that supports in-memory, PostgreSQL, Redis, SQLite, NATS, and Kafka.
- Move queue claim/lease/lock state into `EventHistory`.
- Use one unified runtime model across all backends (no optional alternate mode).
- Preserve current runtime execution shape:
  - EventBus level wrappers (`withEventLock` / `with_event_lock`)
  - Event level processing wrappers
  - EventResult level wrappers (`withHandlerLock` / `with_handler_lock`, timeout, slow monitor, error handling)
- Keep naming aligned across languages:
  - TS methods in camelCase
  - Python methods in snake_case
  - same terminology and method intent in both runtimes

## Non-goals

- No middleware/backend plugin porting in this phase.
- No docs/examples migration in this phase beyond internal TODO/spec docs.
- No compatibility shims for old lock/history internals.

## Hard Requirements

1. Multi-node safety:
- Two workers must never execute the same event claim concurrently.
- Two workers must never execute the same handler claim concurrently.

2. Crash safety:
- Stale claims must expire and become reclaimable.
- Stale workers must be unable to write final state after losing lease.

3. Determinism:
- Claim semantics must be deterministic under contention.
- Tests must enforce no flaky behavior.

4. Scale:
- Support very high churn (millions of bus instances over days).
- Avoid per-bus schema/key explosions where possible.

## Architecture Summary

Canonical state moves into `EventHistory` backend:
- event records
- handler result records
- runnable queue ordering
- claim leases
- lock leases (event-level and handler-level resource locks)

Canonical entity names across backends:
- `event_buses`
- `event_handlers`
- `events`
- `event_results`
- `locks`

`event_handlers` payload should use the same schema as `EventHandler` JSON in each runtime (`to_json_dict` / `toJSON`), with distributed-runtime lease fields stored alongside it.

`LockManager` remains as runtime orchestration API surface, but delegates claim/lease operations to `EventHistory`.

`EventBus` retains ownership of bus-local concerns:
- find waiters/futures
- middleware hooks
- process orchestration and nested wrappers

For NATS/Kafka backends, this same model is represented as native broker messages/streams.
There is no separate runtime mode; broker-native mapping is part of the core design.

## Unified Store Interface (Conceptual)

### Existing history operations

Python:
- `add_event(event)`
- `get_event(event_id)`
- `remove_event(event_id)`
- `has_event(event_id)`
- `find(...)`
- `trim_event_history(...)`

TypeScript:
- `addEvent(event)`
- `getEvent(event_id)`
- `removeEvent(event_id)`
- `hasEvent(event_id)`
- `find(...)`
- `trimEventHistory(...)`

### New distributed claim/lock operations

Python:
- `claim_next_event(...)`
- `renew_event_claim(...)`
- `release_event_claim(...)`
- `claim_next_handler_result(...)`
- `renew_handler_claim(...)`
- `release_handler_claim(...)`
- `complete_handler_result(...)`
- `complete_event_if_done(...)`
- `claim_lock(resource_key, ...)`
- `renew_lock(resource_key, ...)`
- `release_lock(resource_key, ...)`

TypeScript:
- `claimNextEvent(...)`
- `renewEventClaim(...)`
- `releaseEventClaim(...)`
- `claimNextHandlerResult(...)`
- `renewHandlerClaim(...)`
- `releaseHandlerClaim(...)`
- `completeHandlerResult(...)`
- `completeEventIfDone(...)`
- `claimLock(resourceKey, ...)`
- `renewLock(resourceKey, ...)`
- `releaseLock(resourceKey, ...)`

### Claim model

All claims/locks must use:
- owner id (`worker_id`)
- opaque claim token (`uuid`)
- lease expiry timestamp
- fencing token (monotonic integer) for stale-writer protection on lock resources

## Resource Keys

Standard lock resources:
- `event:global`
- `event:bus:{bus_id}`
- `handler:event:{event_id}`

Lock policy:
- event concurrency mode resolves which event resource key is used.
- handler serial mode uses `handler:event:{event_id}`.
- handler parallel mode skips handler lock resource.

## Lifecycle Semantics

1. Event enqueue:
- Insert event into backend history.
- Mark runnable queue position.

2. Event claim:
- Atomically claim next eligible event.
- Acquire/validate event-level lock resource claim.
- Return event claim token + lease metadata.

3. Handler claim:
- Atomically claim next eligible handler result for that event.
- Acquire/validate handler lock claim if serial.
- Return handler claim token + lease metadata.

4. Execution:
- Existing nested closure/context-manager runtime remains.
- Heartbeat renews event/handler claims while executing.

5. Completion:
- Complete handler result with token/fence checks.
- Release handler claim and lock claim.
- Complete event when all handlers done.
- Release event claim and lock claim.

## Broker Mapping (Required for NATS and Kafka Backends)

EventBus dispatch must publish normal broker events that non-abxbus consumers can consume directly.

Public event lane (domain events):
- topic/subject pattern: `events.{event_type}` (or single stream + `event_type` header)
- payload: normal event JSON
- headers: `event_id`, `bus_id`, `event_type`, `event_parent_id`, tracing/correlation ids

AbxBus control lane (runtime coordination):
- `abxbus.event_buses`
- `abxbus.event_handlers`
- `abxbus.events`
- `abxbus.event_results`
- `abxbus.locks`

Rules:
- Public lane messages are first-class outputs of `bus.emit(...)`.
- AbxBus workers consume both public + control lanes as needed.
- External consumers can consume only public lane and still see standard event traffic.
- Handler callable code stays local to each machine; only handler metadata/identity is distributed.

## Backend-Specific Plan

## PostgreSQL Backend

Model:
- Shared tables keyed by `bus_id`, not per-bus tables.
- Use short transactions and lease-based claims.

Core primitives:
- `UPDATE ... WHERE ... RETURNING` claim transitions.
- `INSERT ... ON CONFLICT ... WHERE expired RETURNING` for lock claims.
- Optional `FOR UPDATE SKIP LOCKED` for candidate selection.

Tables:
- `event_buses`
- `event_handlers`
- `events`
- `event_results`
- `locks`

Indexes:
- events: `(bus_id, status, queue_seq)`, `(claim_expires_at)`
- handler results: `(event_id, status)`, `(claim_expires_at)`
- locks: `(resource_key)`, `(expires_at)`

Pros:
- Strong transactional semantics.
- Good for durable multi-node coordination.

Risks:
- Hot index contention under very high parallelism.
- Requires careful batching/trim policy.

## Redis Backend

Model:
- Lease claims and lock ownership via Lua scripts.
- Prefer bounded key cardinality per bus.

Data layout (recommended), equivalent to SQL table names:
- `event_buses` namespace:
  - `eh:event_buses:data` (hash `bus_id -> EventBus JSON`)
- `event_handlers` namespace:
  - `eh:event_handlers:data` (hash `handler_id -> EventHandler JSON + lease metadata`)
  - `eh:event_handlers:by_bus:{bus_id}` (set/zset of handler ids)
- `events` namespace:
  - `eh:events:data` (hash `event_id -> Event JSON + claim metadata`)
  - `eh:events:queue:{bus_id}` (zset queue order)
- `event_results` namespace:
  - `eh:event_results:data` (hash `result_id -> EventResult JSON + claim metadata`)
  - `eh:event_results:by_event:{event_id}` (set/zset of result ids)
- `locks` namespace:
  - `eh:locks:data` (hash `resource_key -> owner/token/fence/expires`)
  - `eh:locks:fence:{resource_key}` (counter key)

Core primitives:
- `EVAL` Lua for atomic claim/release/renew.
- `SET NX PX` style lease acquisition wrapped in scripts.
- compare token before release/complete writes.

Pros:
- High throughput, low-latency claims.
- Good for bursty worker pools.

Risks:
- Key explosion if naive key-per-event forever.
- Needs strict TTL/cleanup discipline.

## SQLite Backend

Model:
- Lease + CAS updates only (no `SELECT FOR UPDATE` assumptions).
- Shared DB with WAL mode and short `BEGIN IMMEDIATE` transactions.

Core primitives:
- CTE candidate selection + guarded `UPDATE ... RETURNING` claim.
- lock claim via UPSERT guarded by expiry timestamp.

PRAGMAs:
- `journal_mode=WAL`
- `busy_timeout` configured
- `synchronous=NORMAL` (or stricter if needed)

Pros:
- Simple deployment.
- Works for local/single-host multi-process collaboration.

Risks:
- Write contention with many workers.
- Not ideal for geographically distributed nodes.

## NATS Backend

Model:
- JetStream streams/consumers are authoritative transport for both public and control lanes.
- Handler registrations and claims are broker-native messages; local callable resolution stays in-process.

Subjects/streams:
- Public: `events.*`
- Control:
  - `abxbus.event_buses.*`
  - `abxbus.event_handlers.*`
  - `abxbus.events.*`
  - `abxbus.event_results.*`
  - `abxbus.locks.*`

Core primitives:
- durable consumers for work distribution
- idempotent claim/complete events keyed by `(event_id, handler_id, attempt)` or equivalent token
- lock claim/renew/release via lock subjects with fencing semantics

Pros:
- Native event-first integration for existing NATS consumers.
- Low latency fanout and delivery.

Risks:
- Exactly-once semantics not guaranteed; idempotency required.
- Requires strict ordering and dedupe strategy in control lane consumers.

## Kafka Backend

Model:
- Topics are authoritative transport for both public and control lanes.
- Compacted control topics hold latest state snapshots while event topics preserve append log.

Topics:
- Public: `events.<event_type>` (or shared `events` topic keyed by `event_type`)
- Control:
  - `abxbus.event_buses`
  - `abxbus.event_handlers`
  - `abxbus.events`
  - `abxbus.event_results`
  - `abxbus.locks`

Core primitives:
- consumer groups for distributed processing
- compacted topics for registration/lock latest-state materialization
- idempotent producer + transactional writes where available

Pros:
- Native event-first integration for existing Kafka consumers.
- Strong ecosystem tooling for replay and audit.

Risks:
- Partitioning strategy can create hot keys.
- Requires materialized control-state consumers for efficient claim decisions.

## Scale Strategy (Millions of Buses Over Days)

1. Shared storage namespaces only:
- No per-bus table creation.
- Bus id as indexed column/key prefix.

2. Lifecycle cleanup:
- hard trim policies required (`max_history_size`/`max_history_drop`)
- backend-level TTL/archive for stopped buses

3. Active set optimization:
- maintain small active-bus index for schedulers
- avoid scanning cold buses

4. Batch operations:
- batched trim/delete and bounded cleanup loops
- avoid N+1 claim queries

5. Cardinality controls:
- Redis: avoid permanent key-per-handler history where possible
- SQL: partition/compact on age if needed

## Migration Plan

## Phase 0: Contracts and invariants

- Define shared claim/lock semantics and error taxonomy.
- Freeze wrapper nesting order and ownership boundaries.
- Add conformance tests for expected store behavior.

## Phase 1: In-memory backend parity

- Extend existing in-memory `EventHistory` to implement full claim interface.
- Rewire `LockManager` to delegate through store API.
- Keep behavior equivalent to current runtime.

## Phase 2: Bus/event integration

- Replace direct queue/in-flight mutation with store methods.
- Keep `find` waiter ownership on bus.
- Ensure event-level locks only acquired at bus layer, handler-level only at event/result layer.

## Phase 3: PostgreSQL backend

- Implement schema + migrations + claim SQL.
- Add lease renewal + fencing checks.
- Add deterministic contention tests across 2+ workers.

## Phase 4: Redis backend

- Implement Lua scripts for claim/release/renew/complete.
- Add stale-lease takeover tests.
- Add key-cardinality/load tests.

## Phase 5: SQLite backend

- Implement CAS claim SQL and lock UPSERT.
- Tune WAL and busy timeout defaults.
- Add local multi-process contention tests.

## Phase 6: NATS backend

- Implement subject/topic mapping for public + control lanes.
- Implement deterministic claim/lock consumer logic.
- Validate interoperability with non-abxbus NATS consumers on public lane.

## Phase 7: Kafka backend

- Implement topic mapping for public + control lanes.
- Implement compaction + idempotent producer strategy for control topics.
- Validate interoperability with non-abxbus Kafka consumers on public lane.

## Phase 8: Performance + reliability hardening

- Stress tests at high event volume and worker count.
- Verify no flaky timing-dependent races.
- Ensure total suite time remains under project threshold.

## Conformance Test Matrix (All Backends)

1. Single-claim exclusivity:
- two workers racing for same event -> exactly one claim succeeds.

2. Lease expiry takeover:
- worker A claims and stops renewing -> worker B claims after expiry.

3. Stale writer rejection:
- worker A loses claim; completion write from A must fail.

4. Handler serial lock correctness:
- only one handler runs at a time per event when serial.

5. Handler parallel correctness:
- handler-level lock omitted in parallel mode.

6. Event concurrency modes:
- `global-serial`, `bus-serial`, `parallel` all enforce intended lock resource semantics.

7. Recovery:
- restart worker mid-event, ensure task is recoverable and completed once.

8. Trim correctness:
- retention rules preserve expected history semantics and ordering.

9. Broker interoperability:
- events emitted by abxbus are consumable by normal non-abxbus Kafka/NATS consumers.

## Open Questions

1. Cross-node `find(future=...)`:
- keep bus-local waiters only, or add distributed waiter mechanism?

2. Lock lease defaults:
- choose default lease durations and heartbeat cadence per backend.

3. Fairness policy:
- strict FIFO vs throughput-optimized batching under high contention.

4. Durability policy:
- should transient pending/started snapshots be fully durable in all backends, or eventual for Redis mode?

## Deliverables

- Unified `EventHistory` claim/lock interface in Python + TS.
- In-memory + PostgreSQL + Redis + SQLite + NATS + Kafka implementations.
- Shared conformance test suite run against all backends.
- Deterministic stress/perf tests for multi-node collaborative processing.
