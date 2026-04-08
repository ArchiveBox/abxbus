# TODO: Child Event Completion Blocking Semantics

## Goal

Make child-event completion blocking explicit in both Python `abxbus` and `abxbus-ts`.

Desired API:

- `event.emit(child)`
  - emit a child event
  - always blocks parent event completion
  - blocks even if the caller does not await it
- `bus.emit(child)` / `event.bus.emit(child)`
  - emit a later/non-blocking event
  - does **not** block parent completion by default
  - only becomes completion-blocking if the emitted event is explicitly awaited

This is needed because `abx-dl` has both kinds of child work:

- phase-owned hook processes that must complete before a phase can finish
- detached/later follow-up work that should not keep the parent phase open

## Current Problem

Today, both Python and TypeScript conflate:

- ancestry/linkage: "this event was emitted by this parent/handler"
- completion-blocking: "this child must finish before the parent can complete"

As a result, `bus.emit(child)` from inside a handler currently behaves like a completion-blocking child event even if the caller never awaits it.

That is what caused the recent `archivebox` / `abx-dl` lifecycle bug:

- a background `ProcessEvent` emitted during crawl/snapshot setup kept the phase event incomplete
- the phase never advanced the way the caller expected

## Verified Current Behavior

### Python

Relevant files:

- [abxbus/base_event.py](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/base_event.py)
- [abxbus/event_bus.py](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/event_bus.py)

Current behavior:

- `EventBus.emit(...)` auto-links emitted child events into the current handler's `event_children`
- `_are_all_children_complete()` checks all linked child events
- parent completion therefore waits for all linked children, regardless of whether the child was awaited

### TypeScript

Relevant files:

- [abxbus-ts/src/base_event.ts](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/base_event.ts)
- [abxbus-ts/src/event_bus.ts](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/event_bus.ts)
- [abxbus-ts/src/event_result.ts](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/event_result.ts)

Current behavior:

- `event.bus.emit(child)` is intercepted by the bus-scoped proxy in `event_bus.ts`
- that proxy calls `handler_result._linkEmittedChildEvent(child)`
- `_linkEmittedChildEvent(...)` pushes the child into `result.event_children`
- `BaseEvent._areAllChildrenComplete()` uses `event_descendants`
- parent completion therefore waits for all emitted child events, even if the caller never waits on them

Important note:

- `abxbus-ts` does **not** currently have a real `BaseEvent.emit(child)` API
- the current child-linking behavior comes from proxied `event.bus.emit(...)`

## Proposed Design

Do **not** add a new public event field/config option for this.

Instead, split runtime bookkeeping into two internal buckets:

- emitted children
  - all emitted descendants
  - used for ancestry, logging, tree views, `child_of`, debugging
- blocking children
  - only the subset that should delay parent completion
  - used for:
    - parent completion checks
    - parent timeout cancellation
    - first-mode loser cancellation

## Planned Semantics

### 1. `event.emit(child)`

- new method in both Python and TypeScript
- same high-level signature/behavior as `bus.emit(child)`
- internally:
  - dispatch through the bus
  - register the child as emitted
  - register the child as completion-blocking for the current handler

### 2. `bus.emit(child)` / `event.bus.emit(child)`

- dispatch and ancestry-link only
- do **not** automatically register the child as completion-blocking

### 3. Awaiting a bus-emitted event upgrades it to blocking

Python:

- `await bus.emit(child)`
- branch in `BaseEvent.__await__`
- if currently inside a handler and this child was emitted by that handler, upgrade it into the blocking-child set before waiting

TypeScript:

- `await bus.emit(child).done()`
- branch in `BaseEvent.done()`
- if currently inside the emitting handler, upgrade the child into the blocking-child set before waiting

This keeps the queueing path unified and moves the semantic split into child registration / wait paths.

## Why This Design

This gives the API contract we want:

- `event.emit(child)` = explicit blocking child work
- `bus.emit(child)` = detached/non-blocking by default
- awaiting a bus-emitted child = opt in to blocking semantics

It also maps cleanly onto `abx-dl`:

- hook `ProcessEvent`s owned by a phase should use `event.emit(...)`
- detached follow-up work should use `bus.emit(...)`

## Implementation Notes

### Python `abxbus`

Likely touch points:

- [abxbus/base_event.py](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/base_event.py)
- [abxbus/event_bus.py](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/event_bus.py)
- possibly [abxbus/event_result.py](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/event_result.py) if logic is split there later

Expected changes:

- add `BaseEvent.emit(...)`
- stop auto-registering every handler-emitted `bus.emit(...)` child as completion-blocking
- keep ancestry tracking for all emitted children
- introduce internal runtime-only blocking-child bookkeeping
- make `_are_all_children_complete()` check only blocking children
- make parent timeout / cancellation code only cancel blocking children
- make `BaseEvent.__await__` upgrade a matching emitted child into the blocking set

### TypeScript `abxbus-ts`

Likely touch points:

- [abxbus-ts/src/base_event.ts](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/base_event.ts)
- [abxbus-ts/src/event_bus.ts](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/event_bus.ts)
- [abxbus-ts/src/event_result.ts](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus-ts/src/event_result.ts)

Expected changes:

- add a real `BaseEvent.emit(...)`
- remove "all child emits are blocking" behavior from the generic proxied `event.bus.emit(...)` path
- preserve ancestry linking in `event.bus.emit(...)`
- split `event_children` vs internal blocking-child tracking
- make `_areAllChildrenComplete()` use blocking descendants only
- make `done()` upgrade a matching bus-emitted child into the blocking set before waiting

## Caveats

### Await-detection cannot happen at `emit()` time

We cannot know at `bus.emit(...)` call time whether the caller will await the returned event later.

So the upgrade to "blocking child" must happen in:

- Python: `__await__`
- TypeScript: `done()` / equivalent wait path

### Ancestry and completion must stay separate

Detached children still need:

- `event_parent_id`
- `event_emitted_by_handler_id`
- event-tree/logging visibility
- `child_of=...` style history queries

But they must not delay parent completion unless explicitly blocking.

### Timeout cancellation behavior will change

Today, parent timeout cancellation cascades through all linked children.

After the refactor, timeout cancellation should only cascade through blocking children.

That is desirable, but it is a real semantic change and must be documented/tested.

## Current Temporary Local Fixes

There are already local changes made during debugging that should be revisited once the explicit API is implemented:

### In `abx-dl`

- [abx_dl/services/process_service.py](/Users/squash/Local/Code/archiveboxes/new/abx-dl/abx_dl/services/process_service.py)
- [abx_dl/services/crawl_service.py](/Users/squash/Local/Code/archiveboxes/new/abx-dl/abx_dl/services/crawl_service.py)
- [abx_dl/services/snapshot_service.py](/Users/squash/Local/Code/archiveboxes/new/abx-dl/abx_dl/services/snapshot_service.py)
- [abx_dl/orchestrator.py](/Users/squash/Local/Code/archiveboxes/new/abx-dl/abx_dl/orchestrator.py)
- [abx_dl/events.py](/Users/squash/Local/Code/archiveboxes/new/abx-dl/abx_dl/events.py)

These changes fixed real timeouts/hangs, but they are partly compensating for the current `abxbus` child-completion semantics.

Once `event.emit(...)` exists, `abx-dl` should be updated to use explicit blocking emits for phase-owned hook work instead of relying on bus-global child-link semantics.

### In Python `abxbus`

- [abxbus/base_event.py](/Users/squash/Local/Code/archiveboxes/new/abxbus/abxbus/base_event.py)

A temporary local field-based approach was introduced during debugging. That should likely be replaced by the explicit `event.emit(...)` / awaited-`bus.emit(...)` design so we do not keep a public/configurable field for this.

## Consequences For Other Repos

### `abx-dl`

Most important downstream repo.

Places to audit:

- crawl setup hook emission
- snapshot hook emission
- cleanup kill events
- any fire-and-forget or background follow-up work

Desired end state:

- phase-owned hook processes use `event.emit(...)`
- detached work uses `bus.emit(...)`

### `archivebox`

Indirectly affected through `abx-dl`.

The recent passing full test suite in `archivebox` depended on fixing the child-completion semantics problem, especially around:

- title extraction
- chrome crawl/snapshot setup/cleanup lifecycle
- phase progression after background hooks

After the explicit API change, rerun the full `archivebox` suite and re-check browser-heavy tests first.

### `abxbus-ts` consumers

Any TS caller that currently assumes:

- `event.bus.emit(child)` automatically creates a blocking child

will change behavior after this refactor.

Those callers will need to switch to:

- `event.emit(child)` for explicit blocking child behavior

## Tests To Add / Update

### Python `abxbus`

Add tests proving:

- `event.emit(child)` blocks parent completion even if not awaited
- `bus.emit(child)` does not block parent completion when not awaited
- `await bus.emit(child)` upgrades the child to blocking
- parent timeout cancels blocking children but not detached children

### TypeScript `abxbus-ts`

Add equivalent tests proving:

- `event.emit(child)` blocks parent completion
- `event.bus.emit(child)` / `bus.emit(child)` is non-blocking by default
- `await bus.emit(child).done()` upgrades to blocking
- timeout/first-mode cancellation only hits blocking children

### `abx-dl`

Update/add integration coverage proving:

- phase-owned background hooks keep the phase open only when emitted explicitly as blocking children
- detached bus-emitted follow-up work does not stall phase progression

### `archivebox`

Rerun at minimum:

- title/chrome-related tests
- full suite

## Recommended Order Of Work

1. Implement the explicit semantics in Python `abxbus`.
2. Update Python `abx-dl` to use `event.emit(...)` for phase-owned hook work.
3. Remove the temporary field-based workaround from Python `abxbus` / `abx-dl`.
4. Re-run `abx-dl` and `archivebox` suites.
5. Mirror the same semantics in `abxbus-ts`.
6. Update TS consumers/tests if any rely on old auto-blocking child behavior.

## Summary

The key design rule should be:

- **child ancestry is cheap and automatic**
- **child completion blocking is explicit**

API:

- `event.emit(child)` => explicit blocking child
- `bus.emit(child)` => detached by default
- awaiting a bus-emitted child => upgrades it to blocking

That preserves the orchestration guarantees `abx-dl` needs without forcing every emitted child event to block parent completion forever.
