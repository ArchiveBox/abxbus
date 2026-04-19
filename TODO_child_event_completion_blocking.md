# TODO: Child Event Completion Blocking

codex resume 'abxbus implement event.emit(...) to explicitly block parent event completion'

## Goal

Make child-event completion blocking explicit and simple in both Python `abxbus` and `abxbus-ts`.

Desired API:

- `event.emit(child)`
  - emits a child event owned by the current parent event
  - always blocks parent event completion, even if the caller never awaits it
- `bus.emit(child)` / `event.bus.emit(child)`
  - emits a normal event with ancestry preserved
  - does not block parent event completion by itself
- `await child`
  - does **not** rewrite parentage
  - does **not** upgrade the child into an owned blocking child
  - naturally blocks the current handler because the handler is still running until the await finishes

This matches the real `bus.find()` / cross-parent usage pattern:

- waiting on some event should block the current handler
- only explicit owned child work should block parent completion after the handler returns

## Core Design

Add a real event field:

- `event.event_blocks_parent_completion: bool = False`

Meaning:

- `False`
  - this event may still be a descendant for ancestry/logging/history purposes
  - but it does not keep its parent event open after the emitting handler finishes
- `True`
  - this event is an owned blocking child
  - the parent event cannot complete until this child completes

The field is public runtime/event metadata and should travel with the event across serialization and bus bridges.

## Emission Semantics

### `event.emit(child)`

Implementation contract:

- set `child.event_blocks_parent_completion = True`
- dispatch using the current event's bus
- preserve normal ancestry linkage:
  - `event_parent_id`
  - `event_emitted_by_handler_id`
  - inclusion in `event_children`

Use this for true phase-owned child work.

### `bus.emit(child)` / `event.bus.emit(child)`

Implementation contract:

- leave `child.event_blocks_parent_completion = False`
- preserve normal ancestry linkage:
  - `event_parent_id`
  - `event_emitted_by_handler_id`
  - inclusion in `event_children`

Use this for detached or later work that should still appear in ancestry/history.

### `await child`

Implementation contract:

- do not mutate:
  - `event_parent_id`
  - `event_emitted_by_handler_id`
  - `event_blocks_parent_completion`
- rely on normal handler lifecycle:
  - while a handler is awaiting, its `EventResult` is still `started`
  - therefore the current parent event cannot complete yet

This is the correct behavior for events discovered via `bus.find()` or shared across parents.

## Lifecycle Rules

Parent completion should depend on two independent things:

1. all handler results are finished
2. all child events with `event_blocks_parent_completion=True` are complete

That means:

- an awaited detached event blocks the current handler naturally
- once the handler returns, only explicit blocking children keep the parent open

## Python Touch Points

Files:

- [abxbus/base_event.py](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/base_event.py)
- [abxbus/event_bus.py](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/event_bus.py)

Field definition / event schema:

- add or finalize `event_blocks_parent_completion: bool = False` on `BaseEvent`
- ensure it is included in normal model validation / dump / reset behavior

Emit transition points:

- [`EventBus.emit(...)` in `event_bus.py`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/event_bus.py#L1082)
  - keep responsibility limited to queueing and ancestry linkage
  - do not implicitly force child completion blocking
- add `BaseEvent.emit(...)` in [`base_event.py`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/base_event.py)
  - set the field to `True`
  - delegate to `self.bus.emit(...)`

Completion / wait paths:

- [`BaseEvent.__await__`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/base_event.py#L927)
- [`BaseEvent.event_completed()`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/base_event.py#L948)
- [`BaseEvent.event_result()`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/base_event.py#L1286)
- [`BaseEvent.event_results_list()`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/base_event.py#L1343)

These wait paths should stay unchanged semantically: they wait for handler / event completion, but should not mutate parent-child ownership.

Parent-completion checks:

- [`BaseEvent._mark_completed()`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/base_event.py#L1470)
- [`BaseEvent._are_all_children_complete()`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/base_event.py#L1570)

Rule:

- only child events with `event_blocks_parent_completion=True` should keep the parent open

Parent-cancellation paths:

- [`BaseEvent._cancel_pending_child_processing()`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/base_event.py#L1602)
- any bus finalization / timeout path that recursively cancels child work

Rule:

- only owned blocking children should be canceled as part of parent cancellation
- detached descendants should keep their original ancestry but not be force-canceled just because the parent timed out or completed

Reset / serialization paths:

- [`BaseEvent.event_reset()`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/base_event.py#L1563)
- bridge payload dumps in:
  - [abxbus/bridge_jsonl.py](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/bridge_jsonl.py)
  - [abxbus/bridge_sqlite.py](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/bridge_sqlite.py)
  - [abxbus/bridge_postgres.py](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/bridge_postgres.py)
  - [abxbus/bridge_redis.py](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/bridge_redis.py)
  - [abxbus/bridge_nats.py](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/bridge_nats.py)
  - [abxbus/bridges.py](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/bridges.py)

Caveat:

- keeping the field serialized is desirable so cross-bus child ownership semantics stay consistent

## TypeScript Touch Points

Files:

- [abxbus-ts/src/base_event.ts](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/base_event.ts)
- [abxbus-ts/src/event_bus.ts](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/event_bus.ts)
- [abxbus-ts/src/event_result.ts](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/event_result.ts)

Field definition / schema:

- add `event_blocks_parent_completion` to:
  - `BaseEventSchema`
  - `BaseEventFields`
  - `BaseEvent` class properties
  - `toJSON()` / `fromJSON()`

Emit transition points:

- [`EventBus.emit(...)` in `event_bus.ts`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/event_bus.ts#L720)
  - keep ancestry linkage
  - do not force child completion blocking
- add real `BaseEvent.emit(...)` in [`base_event.ts`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/base_event.ts)
  - set the field to `true`
  - delegate to `this.bus.emit(...)`

Child-link tracking:

- [`EventResult._linkEmittedChildEvent(...)` in `event_result.ts`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/event_result.ts#L147)

Rule:

- keep this method ancestry-focused
- do not overload it with blocking semantics beyond preserving the child's own field value

Completion / wait paths:

- [`done()`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/base_event.ts#L828)
- [`eventCompleted()`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/base_event.ts#L972)
- [`first()`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/base_event.ts#L860)

These should remain pure wait paths. They should not rewrite parentage or child-blocking ownership.

Parent-completion checks:

- [`_markCompleted()`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/base_event.ts#L1016)
- [`_areAllChildrenComplete()`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/base_event.ts#L1101)

Rule:

- only descendants with `event_blocks_parent_completion === true` should keep the parent open

Reset / serialization paths:

- [`eventReset()`](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/base_event.ts#L995)
- bridge payloads in:
  - [abxbus-ts/src/bridge_jsonl.ts](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/bridge_jsonl.ts)
  - [abxbus-ts/src/bridge_sqlite.ts](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/bridge_sqlite.ts)
  - [abxbus-ts/src/bridge_postgres.ts](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/bridge_postgres.ts)
  - [abxbus-ts/src/bridge_redis.ts](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/bridge_redis.ts)
  - [abxbus-ts/src/bridge_nats.ts](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/bridge_nats.ts)
  - [abxbus-ts/src/bridges.ts](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/bridges.ts)

## Important Caveats

### Awaiting another event is not child ownership

This is the main rule to preserve.

Examples:

- waiting on an event returned by `bus.find()`
- waiting on an event emitted by some other parent
- waiting on a shared global event reference

In all of those cases:

- the current handler should remain blocked naturally
- the awaited event should keep its original `event_parent_id`
- the awaited event should keep its original `event_blocks_parent_completion` value

### Ancestry and completion are intentionally different

An event may be:

- a descendant for history/logging/tree purposes
- but not completion-blocking for its original parent

That is expected and should be preserved.

### Parent timeout / cancellation must respect ownership

If a parent is canceled or times out:

- owned blocking children should cascade-cancel
- detached descendants should not be canceled only because they share ancestry

This is a real semantic boundary and should be reflected in both runtimes.

## Downstream Consequences

### `abx-dl`

Files likely affected:

- [abx_dl/events.py](/Users/squash/Local/Code/archiveboxes/new/abx-dl/abx_dl/events.py)
- [abx_dl/orchestrator.py](/Users/squash/Local/Code/archiveboxes/new/abx-dl/abx_dl/orchestrator.py)
- [abx_dl/services/crawl_service.py](/Users/squash/Local/Code/archiveboxes/new/abx-dl/abx_dl/services/crawl_service.py)
- [abx_dl/services/snapshot_service.py](/Users/squash/Local/Code/archiveboxes/new/abx-dl/abx_dl/services/snapshot_service.py)
- [abx_dl/services/process_service.py](/Users/squash/Local/Code/archiveboxes/new/abx-dl/abx_dl/services/process_service.py)

Required transition:

- phase-owned hook work should use `event.emit(...)`
- detached later work should keep using `bus.emit(...)`

### `archivebox`

Anything relying on phase completion semantics will inherit the corrected ownership model once `abx-dl` switches to explicit `event.emit(...)` for phase-owned child work.

## Tests To Add

Python:

- `event.emit(child)` keeps parent incomplete after handler return until child finishes
- `bus.emit(child)` does not keep parent incomplete after handler return
- awaiting a detached child inside a handler blocks only because the handler is still running
- awaiting a child found via `bus.find()` does not rewrite parentage
- parent timeout cancels owned blocking children but not detached descendants
- bridge round-trip preserves `event_blocks_parent_completion`

TypeScript:

- same coverage as Python using `done()` / `eventCompleted()` wait APIs

## Recommended Order

1. implement the field and `event.emit(...)` in Python
2. update Python completion/cancellation semantics to honor the field
3. mirror the same design in `abxbus-ts`
4. update `abx-dl` to use `event.emit(...)` for phase-owned child work
5. run downstream `archivebox` integration tests against the new semantics
