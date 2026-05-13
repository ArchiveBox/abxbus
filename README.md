# `abxbus`: 📢 Production-ready multi-language event bus

<img width="200" alt="image" src="https://github.com/user-attachments/assets/b3525c24-51ba-496c-b327-ccdfe46a7362" align="right" />

[![DeepWiki: Python](https://img.shields.io/badge/DeepWiki-abxbus%2FPython-yellow.svg?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACwAAAAyCAYAAAAnWDnqAAAAAXNSR0IArs4c6QAAA05JREFUaEPtmUtyEzEQhtWTQyQLHNak2AB7ZnyXZMEjXMGeK/AIi+QuHrMnbChYY7MIh8g01fJoopFb0uhhEqqcbWTp06/uv1saEDv4O3n3dV60RfP947Mm9/SQc0ICFQgzfc4CYZoTPAswgSJCCUJUnAAoRHOAUOcATwbmVLWdGoH//PB8mnKqScAhsD0kYP3j/Yt5LPQe2KvcXmGvRHcDnpxfL2zOYJ1mFwrryWTz0advv1Ut4CJgf5uhDuDj5eUcAUoahrdY/56ebRWeraTjMt/00Sh3UDtjgHtQNHwcRGOC98BJEAEymycmYcWwOprTgcB6VZ5JK5TAJ+fXGLBm3FDAmn6oPPjR4rKCAoJCal2eAiQp2x0vxTPB3ALO2CRkwmDy5WohzBDwSEFKRwPbknEggCPB/imwrycgxX2NzoMCHhPkDwqYMr9tRcP5qNrMZHkVnOjRMWwLCcr8ohBVb1OMjxLwGCvjTikrsBOiA6fNyCrm8V1rP93iVPpwaE+gO0SsWmPiXB+jikdf6SizrT5qKasx5j8ABbHpFTx+vFXp9EnYQmLx02h1QTTrl6eDqxLnGjporxl3NL3agEvXdT0WmEost648sQOYAeJS9Q7bfUVoMGnjo4AZdUMQku50McDcMWcBPvr0SzbTAFDfvJqwLzgxwATnCgnp4wDl6Aa+Ax283gghmj+vj7feE2KBBRMW3FzOpLOADl0Isb5587h/U4gGvkt5v60Z1VLG8BhYjbzRwyQZemwAd6cCR5/XFWLYZRIMpX39AR0tjaGGiGzLVyhse5C9RKC6ai42ppWPKiBagOvaYk8lO7DajerabOZP46Lby5wKjw1HCRx7p9sVMOWGzb/vA1hwiWc6jm3MvQDTogQkiqIhJV0nBQBTU+3okKCFDy9WwferkHjtxib7t3xIUQtHxnIwtx4mpg26/HfwVNVDb4oI9RHmx5WGelRVlrtiw43zboCLaxv46AZeB3IlTkwouebTr1y2NjSpHz68WNFjHvupy3q8TFn3Hos2IAk4Ju5dCo8B3wP7VPr/FGaKiG+T+v+TQqIrOqMTL1VdWV1DdmcbO8KXBz6esmYWYKPwDL5b5FA1a0hwapHiom0r/cKaoqr+27/XcrS5UwSMbQAAAABJRU5ErkJggg==)](https://deepwiki.com/ArchiveBox/abxbus) [![PyPI - Version](https://img.shields.io/pypi/v/abxbus)](https://pypi.org/project/abxbus/) [![PyPi Downloads/week](https://static.pepy.tech/badge/bubus/week)](https://pepy.tech/projects/abxbus) ![GitHub last commit](https://img.shields.io/github/last-commit/ArchiveBox/abxbus)

[![DeepWiki: TS](https://img.shields.io/badge/DeepWiki-abxbus%2FTypescript-blue.svg?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACwAAAAyCAYAAAAnWDnqAAAAAXNSR0IArs4c6QAAA05JREFUaEPtmUtyEzEQhtWTQyQLHNak2AB7ZnyXZMEjXMGeK/AIi+QuHrMnbChYY7MIh8g01fJoopFb0uhhEqqcbWTp06/uv1saEDv4O3n3dV60RfP947Mm9/SQc0ICFQgzfc4CYZoTPAswgSJCCUJUnAAoRHOAUOcATwbmVLWdGoH//PB8mnKqScAhsD0kYP3j/Yt5LPQe2KvcXmGvRHcDnpxfL2zOYJ1mFwrryWTz0advv1Ut4CJgf5uhDuDj5eUcAUoahrdY/56ebRWeraTjMt/00Sh3UDtjgHtQNHwcRGOC98BJEAEymycmYcWwOprTgcB6VZ5JK5TAJ+fXGLBm3FDAmn6oPPjR4rKCAoJCal2eAiQp2x0vxTPB3ALO2CRkwmDy5WohzBDwSEFKRwPbknEggCPB/imwrycgxX2NzoMCHhPkDwqYMr9tRcP5qNrMZHkVnOjRMWwLCcr8ohBVb1OMjxLwGCvjTikrsBOiA6fNyCrm8V1rP93iVPpwaE+gO0SsWmPiXB+jikdf6SizrT5qKasx5j8ABbHpFTx+vFXp9EnYQmLx02h1QTTrl6eDqxLnGjporxl3NL3agEvXdT0WmEost648sQOYAeJS9Q7bfUVoMGnjo4AZdUMQku50McDcMWcBPvr0SzbTAFDfvJqwLzgxwATnCgnp4wDl6Aa+Ax283gghmj+vj7feE2KBBRMW3FzOpLOADl0Isb5587h/U4gGvkt5v60Z1VLG8BhYjbzRwyQZemwAd6cCR5/XFWLYZRIMpX39AR0tjaGGiGzLVyhse5C9RKC6ai42ppWPKiBagOvaYk8lO7DajerabOZP46Lby5wKjw1HCRx7p9sVMOWGzb/vA1hwiWc6jm3MvQDTogQkiqIhJV0nBQBTU+3okKCFDy9WwferkHjtxib7t3xIUQtHxnIwtx4mpg26/HfwVNVDb4oI9RHmx5WGelRVlrtiw43zboCLaxv46AZeB3IlTkwouebTr1y2NjSpHz68WNFjHvupy3q8TFn3Hos2IAk4Ju5dCo8B3wP7VPr/FGaKiG+T+v+TQqIrOqMTL1VdWV1DdmcbO8KXBz6esmYWYKPwDL5b5FA1a0hwapHiom0r/cKaoqr+27/XcrS5UwSMbQAAAABJRU5ErkJggg==)](https://deepwiki.com/ArchiveBox/abxbus/3-typescript-implementation) [![NPM Version](https://img.shields.io/npm/v/abxbus)](https://www.npmjs.com/package/abxbus) [![PyPi Downloads/month](https://static.pepy.tech/badge/bubus/month)](https://pepy.tech/projects/abxbus) [![GitHub License](https://img.shields.io/github/license/ArchiveBox/abxbus)](https://github.com/ArchiveBox/abxbus)

AbxBus is an in-memory event bus library for async Python, TypeScript (node/browser), Rust, and Go.

It's designed for quickly building resilient, predictable, complex event-driven apps.

It "just works" with an intuitive, but powerful event JSON format + emit API that's consistent across runtimes and scales consistently from one event up to millions (~0.2ms/event):

```python
class SomeEvent(BaseEvent):
    some_data: int

def handle_some_event(event: SomeEvent):
    print('hi!')

bus.on(SomeEvent, some_function)
await bus.emit(SomeEvent(some_data=132)).now()
# "hi!""
```

It's async native, has proper automatic nested event tracking, and powerful concurrency control options. The API is inspired by `EventEmitter` or [`emittery`](https://github.com/sindresorhus/emittery) in JS, but it takes it a step further:

- nice Pydantic / Zod schemas for events that can be exchanged between runtimes
- automatic UUIDv7s and monotonic nanosecond timestamps for ordering events globally
- built in locking options to force strict global FIFO processing or fully parallel processing

---

♾️ It's inspired by the simplicity of async and events in `JS` but with baked-in features that allow to eliminate most of the tedious repetitive complexity in event-driven codebases:

- correct timeout enforcement across multiple levels of events, including cancellation of awaited/blocking child work when a parent times out
- ability to strongly type hint and enforce the return type of event handlers at compile-time
- ability to queue events on the bus, or inline await them for immediate execution like a normal function call
- handles thousands of events/sec/core; see the runtime matrix below for current measured numbers

<br/>

## 🔢 Quickstart

Install abxbus and get started with a simple event-driven application:

```bash
pip install abxbus      # see ./abxbus-ts/README.md for JS instructions
```

```python
import asyncio
from abxbus import EventBus, BaseEvent
from your_auth_events import AuthRequestEvent, AuthResponseEvent

class UserLoginEvent(BaseEvent[str]):
    username: str
    is_admin: bool

async def handle_login(event: UserLoginEvent) -> str:
    auth_request = await event.emit(AuthRequestEvent(...)).now()  # nested events supported
    auth_response = await event.event_bus.find(AuthResponseEvent, child_of=auth_request, future=30)
    return f"User {event.username} logged in admin={event.is_admin} with API response: {await auth_response.event_result()}"

bus = EventBus()
bus.on(UserLoginEvent, handle_login)
bus.on(AuthRequestEvent, AuthAPI.post)

event = bus.emit(UserLoginEvent(username="alice", is_admin=True))
print(await event.event_result())
# User alice logged in admin=True with API response: {...}
```

<br/>

---

<br/>

## ✨ Features

<br/>

<details>
<summary><strong>🔎 Event Pattern Matching</strong></summary>

[Subscribe to events](https://abxbus.archivebox.io/features/event-pattern-matching) using multiple patterns:

```python
# By event model class (recommended for best type hinting)
bus.on(UserActionEvent, handler)

# By event type string
bus.on('UserActionEvent', handler)

# Wildcard - handle all events
bus.on('*', universal_handler)
```

<br/>

</details>

<details>
<summary><strong>🔀 Async and Sync Handler Support</strong></summary>

Register both [synchronous and asynchronous handlers](https://abxbus.archivebox.io/features/async-sync-handlers) for maximum flexibility:

```python
# Async handler
async def async_handler(event: SomeEvent) -> str:
    await asyncio.sleep(0.1)  # Simulate async work
    return "async result"

# Sync handler
def sync_handler(event: SomeEvent) -> str:
    return "sync result"

bus.on(SomeEvent, async_handler)
bus.on(SomeEvent, sync_handler)
```

Handlers can also be defined under classes for easier organization:

```python
class SomeService:
    some_value = 'this works'

    async def handlers_can_be_methods(self, event: SomeEvent) -> str:
        return self.some_value

    @classmethod
    async def handler_can_be_classmethods(cls, event: SomeEvent) -> str:
        return cls.some_value

    @staticmethod
    async def handlers_can_be_staticmethods(event: SomeEvent) -> str:
        return 'this works too'

# All usage patterns behave the same:
bus.on(SomeEvent, SomeService().handlers_can_be_methods)
bus.on(SomeEvent, SomeService.handler_can_be_classmethods)
bus.on(SomeEvent, SomeService.handlers_can_be_staticmethods)
```

<br/>

</details>

<details>
<summary><strong>🔠 Type-Safe Events with Pydantic</strong></summary>

Define events as Pydantic models with [full type checking and validation](https://abxbus.archivebox.io/features/typed-events):

```python
from typing import Any
from abxbus import BaseEvent

class OrderCreatedEvent(BaseEvent):
    order_id: str
    customer_id: str
    total_amount: float
    items: list[dict[str, Any]]

# Events are automatically validated
event = OrderCreatedEvent(
    order_id="ORD-123",
    customer_id="CUST-456",
    total_amount=99.99,
    items=[{"sku": "ITEM-1", "quantity": 2}]
)
```

> [!TIP]
> You can also enforce the types of [event handler return values](https://abxbus.archivebox.io/features/return-value-handling#typed-return-values).

<br/>

</details>

<details>
<summary><strong>⏩ Forward `Events` Between `EventBus`s</strong></summary>

You can define separate `EventBus` instances in different "microservices" to separate different areas of concern.
`EventBus`s can be set up to [forward events between each other](https://abxbus.archivebox.io/features/forwarding-between-buses) (with automatic loop prevention):

```python
# Create a hierarchy of buses
main_bus = EventBus(name='MainBus')
auth_bus = EventBus(name='AuthBus')
data_bus = EventBus(name='DataBus')

# Share all or specific events between buses
main_bus.on('*', auth_bus.emit)  # if main bus gets LoginEvent, will forward to AuthBus
auth_bus.on('*', data_bus.emit)  # auth bus will forward everything to DataBus
data_bus.on('*', main_bus.emit)  # don't worry! event will only be processed once by each, no infinite loop occurs

# Events flow through the hierarchy with tracking
event = main_bus.emit(LoginEvent())
await event.now()
print(event.event_path)  # ['MainBus#ab12', 'AuthBus#cd34', 'DataBus#ef56']  # list of bus labels that already processed the event
```

<br/>

</details>

<details>
<summary><strong>🔱 Event Handler Return Value Support</strong></summary>

[Collect results](https://abxbus.archivebox.io/features/return-value-handling) from multiple handlers:

```python
async def load_user_config(event: GetConfigEvent) -> dict[str, Any]:
    return {"debug": True, "port": 8080}

async def load_system_config(event: GetConfigEvent) -> dict[str, Any]:
    return {"debug": False, "timeout": 30}

bus.on(GetConfigEvent, load_user_config)
bus.on(GetConfigEvent, load_system_config)

# Get all handler result values
event = await bus.emit(GetConfigEvent()).now()
results = await event.event_results_list()

# Inspect per-handler metadata when needed
for handler_id, event_result in event.event_results.items():
    print(handler_id, event_result.handler_name, event_result.result)
```

<br/>

</details>

<details>
<summary><strong>🚦 FIFO / Parallel Event Processing</strong></summary>

By default, events and their handlers are processed in [strict serial FIFO order](https://abxbus.archivebox.io/concurrency/events-bus-serial), maintaining consistency:

```python
# Events are processed in the order they were emitted
for i in range(10):
    bus.emit(ProcessTaskEvent(task_id=i))

# Even with async handlers, order is preserved
await bus.wait_until_idle(timeout=30.0)
```

If a handler emits and awaits any child events during execution, those events will [jump the FIFO queue](https://abxbus.archivebox.io/concurrency/immediate-execution) and be processed immediately:

```python
def child_handler(event: SomeOtherEvent) -> str:
    return 'xzy123'

async def main_handler(event: MainEvent) -> str:
    # emit a linked child event
    child_event = event.emit(SomeOtherEvent())

    # now() marks it as parent-completion-blocking and can queue-jump it
    completed_child_event = await child_event.now()
    return f'result from awaiting child event: {await completed_child_event.event_result()}'  # 'xyz123'

bus.on(SomeOtherEvent, child_handler)
bus.on(MainEvent, main_handler)

main_event = await bus.emit(MainEvent()).now()
print(await main_event.event_result())
# result from awaiting child event: xyz123
```

You can also set [`event_concurrency='parallel'`](https://abxbus.archivebox.io/concurrency/events-parallel) and [`event_handler_concurrency='parallel'`](https://abxbus.archivebox.io/concurrency/handlers-parallel) options per-bus, per-event, or per-handler enable parallel processing when needed.

<br/>

</details>

<details>
<summary><strong>🪆 Emit Nested Child Events From Handlers</strong></summary>

[Automatically track event relationships](https://abxbus.archivebox.io/features/parent-child-tracking) and causality tree:

```python
async def parent_handler(event: BaseEvent):
    # Most handler code should use this: linked child work that blocks parent completion.
    blocking_child = await event.emit(ChildEvent()).now()
    assert blocking_child.event_parent_id == event.event_id
    assert blocking_child.event_blocks_parent_completion is True

    # Linked background work keeps ancestry but does not hold the parent open.
    linked_background_child = event.emit(ChildEvent())
    assert linked_background_child.event_parent_id == event.event_id
    assert linked_background_child.event_blocks_parent_completion is False

    # Awaiting bus.emit(...) blocks this handler naturally, but creates a top-level event.
    detached_blocking_event = await event.event_bus.emit(ChildEvent()).now()
    assert detached_blocking_event.event_parent_id is None
    assert detached_blocking_event.event_blocks_parent_completion is False

    # Un-awaited bus.emit(...) is a true detached background event.
    detached_background_event = event.event_bus.emit(ChildEvent())
    assert detached_background_event.event_parent_id is None
    assert detached_background_event.event_blocks_parent_completion is False

async def run_main():
    bus.on(ChildEvent, child_handler)
    bus.on(ParentEvent, parent_handler)

    parent_event = bus.emit(ParentEvent())
    print(parent_event.event_children)           # show all the child events emitted during handling of an event
    await parent_event.now()
    print(bus.log_tree())
    await bus.destroy()

if __name__ == '__main__':
    asyncio.run(run_main())
```

<img width="100%" alt="show the whole tree of events at any time using the logging helpers" src="https://github.com/user-attachments/assets/f94684a6-7694-4066-b948-46925f47b56c" /><br/>
<img width="100%" alt="intelligent timeout handling to differentiate handler that timed out from handler that was interrupted" src="https://github.com/user-attachments/assets/8da341fd-6c26-4c68-8fec-aef1ca55c189" />

<br/><br/>

</details>

<details>
<summary><strong>🔎 Find Events in History or Wait for Future Events</strong></summary>

[`find()`](https://abxbus.archivebox.io/features/find-events) is the single lookup API: search history, wait for future events, or combine both to check for an existing recent event before emitting a new one.

```python
# Default: non-blocking history lookup (past=True, future=False)
existing = await bus.find(ResponseEvent)

# Wait only for future matches
future = await bus.find(ResponseEvent, past=False, future=5)

# Combine event predicate + event metadata filters
match = await bus.find(
    ResponseEvent,
    where=lambda e: e.request_id == my_id,
    event_status='completed',
    future=5,
)

# Wildcard: match any event type, filtered by metadata/predicate
any_completed = await bus.find(
    '*',
    where=lambda e: e.event_type.endswith('ResultEvent'),
    event_status='completed',
    future=5,
)
```

#### Finding Child Events

When you emit an event that triggers child events, use `child_of` to find specific descendants:

```python
# Emit a parent event that triggers child events
nav_event = await bus.emit(NavigateToUrlEvent(url="https://example.com")).now()

# Find a child event (already fired while NavigateToUrlEvent was being handled)
new_tab = await bus.find(TabCreatedEvent, child_of=nav_event, past=5)
if new_tab:
    print(f"New tab created: {new_tab.tab_id}")
```

This solves race conditions where child events fire before you start waiting for them.

#### Returning Multiple Matches with `filter()`

`filter()` takes the same arguments as `find()` but returns the list of all matching events
(newest to oldest), plus an optional `limit` argument to cap the result count.

```python
recent = await bus.filter(ResponseEvent, past=10, future=False, limit=5)
```

See the `EventBus.find(...)` API section below for full parameter details.

> [!IMPORTANT]
> `find()` resolves when the event is first _emitted_ to the `EventBus`, not when it completes.
> Use `await event.now()` for immediate-await semantics (queue-jumps when called inside a handler), or `await event.wait()` to always wait in normal queue order.
> Python also supports `await event` as a Python-only shortcut for `await event.now()`.
> If no match is found (or future timeout elapses), `find()` returns `None`.

<br/>

</details>

<details>
<summary><strong>🔁 Event Debouncing</strong></summary>

Avoid re-running expensive work by reusing recent events. The `find()` method makes [debouncing](https://abxbus.archivebox.io/features/find-events#7-debounce-expensive-work) simple:

```python
# Simple debouncing: reuse event from last 10 seconds, or emit new
event = await bus.find(ScreenshotEvent, past=10, future=False) or bus.emit(ScreenshotEvent())
event = await event.now()

# Advanced: check history, wait briefly for new event to appear, fallback to emit new event
event = (
    await bus.find(SyncEvent, past=True, future=False)   # Check all history (instant)
    or await bus.find(SyncEvent, past=False, future=5)   # Wait up to 5s for in-flight
    or bus.emit(SyncEvent())                         # Fallback: emit new
)
await event.now()                                              # get completed event
```

<br/>

</details>

<details>
<summary><strong>🎯 Event Handler Return Values</strong></summary>

There are two ways to get [return values](https://abxbus.archivebox.io/features/return-value-handling) from event handlers:

**1. Have handlers return their values directly, which puts them in `event.event_results`:**

```python
class DoSomeMathEvent(BaseEvent[int]):  # BaseEvent[int] = handlers are validated as returning int
    a: int
    b: int

    # int passed above gets saved to:
    # event_result_type = int

def do_some_math(event: DoSomeMathEvent) -> int:
    return event.a + event.b

event_bus.on(DoSomeMathEvent, do_some_math)
event = await event_bus.emit(DoSomeMathEvent(a=100, b=120)).now(first_result=True)
print(await event.event_result())
# 220
```

You can use these helpers to interact with the results returned by handlers:

- `BaseEvent.event_result()`
- `BaseEvent.event_results_list()`
- Inspect raw per-handler entries via `BaseEvent.event_results`

**2. Have the handler do the work, then emit another event containing the result value, which other code can find:**

```python
def do_some_math(event: DoSomeMathEvent[int]) -> int:
    result = event.a + event.b
    event.emit(MathCompleteEvent(final_sum=result))

event_bus.on(DoSomeMathEvent, do_some_math)
await event_bus.emit(DoSomeMathEvent(a=100, b=120)).now()
result_event = await event_bus.find(MathCompleteEvent, past=False, future=30)
print(result_event.final_sum)
# 220
```

These events can also be emitted automatically for you if you enable the [`AutoReturnEventMiddleware`](https://abxbus.archivebox.io/integrations/middleware-auto-return).

#### Annotating Event Handler Return Value Types

AbxBus supports optional [strict typing for Event handler return values](https://abxbus.archivebox.io/features/return-value-handling#typed-return-values) using a generic parameter passed to `BaseEvent[ReturnTypeHere]`.
For example if you use `BaseEvent[str]`, abxbus would enforce that all handler functions must return `str | None` at compile-time via IDE/`mypy`/`pyright`/`ty` type hints, and at runtime when each handler finishes.

```python
class ScreenshotEvent(BaseEvent[bytes]):  # BaseEvent[bytes] will enforce that handlers can only return bytes
    width: int
    height: int

async def on_ScreenshotEvent(event: ScreenshotEvent) -> bytes:
    return b'someimagebytes...'  # ✅ IDE type-hints & runtime both enforce return type matches expected: bytes
    return 123                   # ❌ will show mypy/pyright issue + raise TypeError if the wrong type is returned

event_bus.on(ScreenshotEvent, on_ScreenshotEvent)

# Handler return values are automatically validated against the bytes type
event = await event_bus.emit(ScreenshotEvent(...)).now(first_result=True)
returned_bytes = await event.event_result()
assert isinstance(returned_bytes, bytes)
```

**Important:** The validation uses Pydantic's `TypeAdapter`, which validates but does not coerce types. Handlers must return the exact type specified or `None`:

```python
class StringEvent(BaseEvent[str]):
    pass

# ✅ This works - returns the expected str type
def good_handler(event: StringEvent) -> str:
    return "hello"

# ❌ This fails validation - returns int instead of str
def bad_handler(event: StringEvent) -> str:
    return 42  # ValidationError: expected str, got int
```

This also works with complex types and Pydantic models:

```python
class EmailMessage(BaseModel):
    subject: str
    content_len: int
    email_from: str

class FetchInboxEvent(BaseEvent[list[EmailMessage]]):
    account_id: UUID
    auth_key: str

async def fetch_from_gmail(event: FetchInboxEvent) -> list[EmailMessage]:
    return [EmailMessage(subject=msg.subj, ...) for msg in GmailAPI.get_msgs(event.account_id, ...)]

event_bus.on(FetchInboxEvent, fetch_from_gmail)

# Return values are automatically validated as list[EmailMessage]
event = await event_bus.emit(FetchInboxEvent(account_id='124', ...)).now(first_result=True)
email_list = await event.event_result()
```

For pure Python usage, `event_result_type` can be any Python/Pydantic type you want. For cross-language JSON roundtrips, object-like shapes (e.g. `TypedDict`, `dataclass`, model-like dict schemas) rehydrate on Python as Pydantic models, map keys are constrained to JSON object string keys, and fine-grained string constraints/custom field validator logic is not preserved.

<br/>

</details>

<details>
<summary><strong>🧵 ContextVar Propagation</strong></summary>

ContextVars set before `emit()` are [automatically propagated to event handlers](https://abxbus.archivebox.io/features/context-propagation). This is essential for request-scoped context like request IDs, user sessions, or tracing spans:

```python
from contextvars import ContextVar

# Define your context variables
request_id: ContextVar[str] = ContextVar('request_id', default='<unset>')
user_id: ContextVar[str] = ContextVar('user_id', default='<unset>')

async def handler(event: MyEvent) -> str:
    # Handler sees the context values that were set before emit()
    print(f"Request: {request_id.get()}, User: {user_id.get()}")
    return "done"

bus.on(MyEvent, handler)

# Set context before emit (e.g., in FastAPI middleware)
request_id.set('req-12345')
user_id.set('user-abc')

# Handler will see request_id='req-12345' and user_id='user-abc'
await bus.emit(MyEvent()).now()
```

**Context propagates through nested handlers:**

```python
async def parent_handler(event: ParentEvent) -> str:
    # Context is captured at emit time
    print(f"Parent sees: {request_id.get()}")  # 'req-12345'

    # Child events inherit the same context
    await event.emit(ChildEvent()).now()
    return "parent_done"

async def child_handler(event: ChildEvent) -> str:
    # Child also sees the original emit context
    print(f"Child sees: {request_id.get()}")  # 'req-12345'
    return "child_done"
```

**Context isolation between emits:**

Each emit captures its own context snapshot. Concurrent emits with different context values are properly isolated:

```python
request_id.set('req-A')
event_a = bus.emit(MyEvent())  # Handler A sees 'req-A'

request_id.set('req-B')
event_b = bus.emit(MyEvent())  # Handler B sees 'req-B'

await event_a.now()  # Still sees 'req-A'
await event_b.now()  # Still sees 'req-B'
```

> [!NOTE]
> Context is captured at `emit()` time, not when the handler executes. This ensures handlers see the context from the call site, even if the event is processed later from a queue.

<br/>

</details>

<details>
<summary><strong>🧹 Memory Management</strong></summary>

EventBus includes [automatic memory management](https://abxbus.archivebox.io/api/eventbus#shared-configuration-semantics) to prevent unbounded growth in long-running applications:

```python
# Create a bus with memory limits (default: 100 events)
bus = EventBus(max_history_size=100)  # Keep max 100 events in history

# Or disable memory limits for unlimited history
bus = EventBus(max_history_size=None)

# Or keep only in-flight events in history (drop each event as soon as it completes)
bus = EventBus(max_history_size=0)

# Or reject new emits when history is full (instead of dropping old history)
bus = EventBus(max_history_size=100, max_history_drop=False)
```

**Automatic Cleanup:**

- When `max_history_size` is set and `max_history_drop=True`, EventBus removes old events when the limit is exceeded
- If `max_history_size=0`, history keeps only pending/started events and drops each event immediately after completion
- If `max_history_drop=True`, the bus may drop oldest history entries even if they are uncompleted events
- Completed events are removed first (oldest first), then started events, then pending events
- This ensures active events are preserved while cleaning up old completed events

**Manual Memory Management:**

```python
# For request-scoped buses (e.g. web servers), clear all memory after each request
try:
    event_service = EventService()  # Creates internal EventBus
    await event_service.process_request()
finally:
    # Clear all event history and remove from global tracking
    await event_service.eventbus.destroy(clear=True)
```

**Memory Monitoring:**

- EventBus automatically monitors total memory usage across all instances
- Warnings are logged when total memory exceeds 50MB
- Use `bus.destroy(clear=True)` to completely free memory for unused buses
- To avoid memory leaks from big events, the default limits are intentionally kept low. events are normally processed as they come in, and there is rarely a need to keep every event in memory longer after its complete. long-term storage should be accomplished using other mechanisms, like the WAL

<br/>

</details>

<details>
<summary><strong>⛓️ Parallel Handler Execution</strong></summary>

> [!CAUTION]
> **Not Recommended.** Only for advanced users willing to implement their own concurrency control.

Enable [parallel processing](https://abxbus.archivebox.io/concurrency/handlers-parallel) of handlers for better performance.  
The harsh tradeoff is less deterministic ordering as handler execution order will not be guaranteed when run in parallel.
(It's very hard to write non-flaky/reliable applications when handler execution order is not guaranteed.)

```python
# Create bus with parallel handler execution
bus = EventBus(event_handler_concurrency='parallel')

# Multiple handlers run concurrently for each event
bus.on('DataEvent', slow_handler_1)  # Takes 1 second
bus.on('DataEvent', slow_handler_2)  # Takes 1 second

start = time.time()
await bus.emit(DataEvent()).now()
# Total time: ~1 second (not 2)
```

<br/>

</details>

<details>
<summary><strong>🧩 Middlewares</strong></summary>

[Middlewares](https://abxbus.archivebox.io/integrations/middlewares) can observe or mutate the `EventResult` at each step, emit additional events, or trigger other side effects (metrics, retries, auth checks, etc.).

```python
from abxbus import EventBus
from abxbus.middlewares import LoggerEventBusMiddleware, WALEventBusMiddleware, SQLiteHistoryMirrorMiddleware, OtelTracingMiddleware

bus = EventBus(
    name='MyBus',
    middlewares=[
        SQLiteHistoryMirrorMiddleware('./events.sqlite3'),
        WALEventBusMiddleware('./events.jsonl'),
        LoggerEventBusMiddleware('./events.log'),
        OtelTracingMiddleware(),
        # ...
    ],
)

await bus.emit(SecondEventAbc(some_key="banana")).now()
# will persist all events to sqlite + events.jsonl + events.log
```

Built-in middlewares you can import from `abxbus.middlewares`:

- `AutoErrorEventMiddleware`: on handler error, fire-and-forget emits `OriginalEventTypeErrorEvent` with `{error, error_type}` (skips `*ErrorEvent`/`*ResultEvent` sources). Useful when downstream/remote consumers only see events and need explicit failure notifications.
- `AutoReturnEventMiddleware`: on non-`None` handler return, fire-and-forget emits `OriginalEventTypeResultEvent` with `{data}` (skips `*ErrorEvent`/`*ResultEvent` sources). Useful for bridges/remote systems since handler return values do not cross bridge boundaries, but events do.
- `AutoHandlerChangeEventMiddleware`: emits `BusHandlerRegisteredEvent({handler})` / `BusHandlerUnregisteredEvent({handler})` when handlers are added/removed via `.on()` / `.off()`.
- `OtelTracingMiddleware`: emits OpenTelemetry spans for events and handlers with parent-child linking; can be exported to Sentry via Sentry's OpenTelemetry integration.
- `WALEventBusMiddleware`: persists completed events to JSONL for replay/debugging.
- `LoggerEventBusMiddleware`: writes event/handler transitions to stdout and optionally to file.
- `SQLiteHistoryMirrorMiddleware`: mirrors event and handler snapshots into append-only SQLite `events_log` and `event_results_log` tables for auditing/debugging.

#### Defining a custom middleware

Handler middlewares subclass `EventBusMiddleware` and override whichever lifecycle hooks they need (`on_event_change`, `on_event_result_change`, `on_bus_handlers_change`):

```python
from abxbus.middlewares import EventBusMiddleware

class AnalyticsMiddleware(EventBusMiddleware):
    async def on_event_result_change(self, eventbus, event, event_result, status):
        if status == 'started':
            await analytics_bus.emit(HandlerStartedAnalyticsEvent(event_id=event_result.event_id)).now()
        elif status == 'completed':
            await analytics_bus.emit(
                HandlerCompletedAnalyticsEvent(
                    event_id=event_result.event_id,
                    error=repr(event_result.error) if event_result.error else None,
                )
            ).now()

    async def on_bus_handlers_change(self, eventbus, handler, registered):
        await analytics_bus.emit(
            HandlerRegistryChangedEvent(handler_id=handler.id, registered=registered, bus=eventbus.name)
        ).now()
```

<br/>

---

---

<br/>

</details>

## 📚 API Documentation

<details>
<summary><strong><code>EventBus</code></strong></summary>

The main event bus class that manages event processing and handler execution.

```python
EventBus(
    name: str | None = None,
    event_concurrency: Literal['global-serial', 'bus-serial', 'parallel'] = 'bus-serial',
    event_handler_concurrency: Literal['serial', 'parallel'] = 'serial',
    event_handler_completion: Literal['all', 'first'] = 'all',
    event_timeout: float | None = 60.0,
    event_slow_timeout: float | None = 300.0,
    event_handler_slow_timeout: float | None = 30.0,
    event_handler_detect_file_paths: bool = True,
    max_history_size: int | None = 100,
    max_history_drop: bool = False,
    middlewares: Sequence[EventBusMiddleware | type[EventBusMiddleware]] | None = None,
)
```

**Parameters:**

- `name`: Optional unique name for the bus (auto-generated if not provided)
- `event_concurrency`: Default event scheduling mode: `'global-serial'`, `'bus-serial'` (default), or `'parallel'` (resolved at processing time when `event.event_concurrency` is unset)
- `event_handler_concurrency`: Default handler execution mode for events on this bus: `'serial'` (default) or `'parallel'` (resolved at processing time when `event.event_handler_concurrency` is unset)
- `event_handler_completion`: Handler completion mode for each event: `'all'` (default, wait for all handlers) or `'first'` (complete once first successful non-`None` result is available), resolved at processing time when `event.event_handler_completion` is unset
- `event_timeout`: Default per-event timeout in seconds resolved at processing time when `event.event_timeout` is `None`
- `event_slow_timeout`: Default slow-event warning threshold in seconds resolved at processing time when `event.event_slow_timeout` is `None`
- `event_handler_slow_timeout`: Default slow-handler warning threshold in seconds resolved at processing time when `event.event_handler_slow_timeout` is `None`
- `event_handler_detect_file_paths`: Whether to auto-detect handler source file paths at registration time (slightly slower when enabled)
- `max_history_size`: Maximum number of events to keep in history (default: 100, `None` = unlimited, `0` = keep only in-flight events and drop completed events immediately)
- `max_history_drop`: If `True`, drop oldest history entries when full (even uncompleted events). If `False` (default), reject new emits once history reaches `max_history_size` (except when `max_history_size=0`, which never rejects on history size)
- `middlewares`: Optional list of `EventBusMiddleware` subclasses or instances that hook into handler execution for analytics, logging, retries, etc. (see [Middlewares](#middlewares) for more info)

Timeout precedence matches TS:

- Effective handler timeout = `min(resolved_handler_timeout, event_timeout)` where `resolved_handler_timeout` resolves in order: `handler.handler_timeout` -> `event.event_handler_timeout` -> `bus.event_timeout`.
- Slow handler warning threshold resolves in order: `handler.handler_slow_timeout` -> `event.event_handler_slow_timeout` -> `bus.event_handler_slow_timeout`.
- Bus defaults are applied at execution time by the bus currently processing the event. Unset event fields stay unset on the event object so forwarded events can inherit the target bus defaults.

#### `EventBus` Properties

- `name`: The bus identifier
- `id`: Unique UUID7 for this bus instance
- `event_history`: Dict of all events the bus has seen by event_id (limited by `max_history_size`)
- `events_pending`: List of events waiting to be processed
- `events_started`: List of events currently being processed
- `events_completed`: List of completed events
- `all_instances`: Class-level WeakSet tracking all active EventBus instances (for memory monitoring)

#### `EventBus` Methods

##### `on(event_type: str | Type[BaseEvent], handler: Callable)`

Subscribe a handler to events matching a specific event type or `'*'` for all events.

```python
bus.on('UserEvent', handler_func)  # By event type string
bus.on(UserEvent, handler_func)    # By event class
bus.on('*', handler_func)          # Wildcard - all events
```

##### `emit(event: BaseEvent) -> BaseEvent`

Enqueue an event for processing and return the pending `Event` immediately (synchronous).

```python
event = bus.emit(MyEvent(data="test"))
result = await event.now()  # immediate path (queue-jumps when called inside a handler)
result_in_queue_order = await event.wait()  # always waits in normal queue order
```

**Note:** Queueing is unbounded. History pressure is controlled by `max_history_size` + `max_history_drop`:

- `max_history_drop=True`: absorb new events and trim old history entries (even uncompleted events).
- `max_history_drop=False`: raise `RuntimeError` when history is full.
- `max_history_size=0`: keep pending/in-flight events only; completed events are immediately removed from history.

##### `find(event_type: str | Literal['*'] | Type[BaseEvent], *, where: Callable[[BaseEvent], bool]=None, child_of: BaseEvent | None=None, past: bool | float | timedelta=True, future: bool | float=False, **event_fields) -> BaseEvent | None`

Find an event matching criteria in history and/or future. This is the recommended unified method for event lookup.

**Parameters:**

- `event_type`: The event type string, `'*'` wildcard, or model class to find
- `where`: Predicate function for filtering (default: matches all)
- `child_of`: Only match events that are descendants of this parent event
- `past`: Controls history search behavior (default: `True`)
  - `True`: search all history
  - `False`: skip history search
  - `float`/`timedelta`: search events from last N seconds only
- `future`: Controls future wait behavior (default: `False`)
  - `True`: wait forever for matching event
  - `False`: don't wait for future events
  - `float`: wait up to N seconds for matching event
- `**event_fields`: Optional equality filters for any event fields (for example `event_status='completed'`, `user_id='u-1'`)

```python
# Default call is non-blocking history lookup (past=True, future=False)
event = await bus.find(ResponseEvent)

# Find child of a specific parent event
child = await bus.find(ChildEvent, child_of=parent_event, future=5)

# Wait only for future events (ignore history)
event = await bus.find(ResponseEvent, past=False, future=5)

# Search recent history + optionally wait
event = await bus.find(ResponseEvent, past=5, future=5)

# Filter by event metadata
completed = await bus.find(ResponseEvent, event_status='completed')

# Wildcard match across all event types
any_completed = await bus.find('*', event_status='completed', past=True, future=False)
```

##### `filter(event_type, *, limit: int | None=None, ...) -> list[BaseEvent]`

Same as [`find()`](#find-event-type-str--literal--type-base-event--where-callable-base-event-bool-none-child-of-base-event--none-none-past-bool--float--timedelta-true-future-bool--float-false-event-fields---base-event--none)
but returns the list of all matching events (newest to oldest) instead of just the first match.
Accepts an additional `limit` argument to cap the result count.

```python
recent = await bus.filter(ResponseEvent, past=10, future=False, limit=5)
```

##### `event_is_child_of(event: BaseEvent, ancestor: BaseEvent) -> bool`

Check if event is a descendant of ancestor (child, grandchild, etc.).

```python
if bus.event_is_child_of(child_event, parent_event):
    print("child_event is a descendant of parent_event")
```

##### `event_is_parent_of(event: BaseEvent, descendant: BaseEvent) -> bool`

Check if event is an ancestor of descendant (parent, grandparent, etc.).

```python
if bus.event_is_parent_of(parent_event, child_event):
    print("parent_event is an ancestor of child_event")
```

##### `wait_until_idle(timeout: float | None=None)`

Wait until all events are processed and the bus is idle.

```python
await bus.wait_until_idle()             # wait indefinitely until EventBus has finished processing all events

await bus.wait_until_idle(timeout=5.0)  # wait up to 5 seconds
```

##### `destroy(timeout: float | None=None, clear: bool=True)`

Destroy the event bus, optionally waiting for pending events first.

```python
await bus.destroy(timeout=1.0)       # wait up to 1sec, then destroy and clear by default
await bus.destroy()                  # destroy immediately and clear handlers/history/runtime state
await bus.destroy(clear=False)       # stop runtime work but keep handlers/history so the bus can resume
```

---

</details>

<details>
<summary><strong><code>BaseEvent</code></strong></summary>

Base class for all events. Subclass `BaseEvent` to define your own events.

Make sure none of your own event data fields start with `event_` or `model_` to avoid clashing with `BaseEvent` or `pydantic` builtin attrs.

#### `BaseEvent` Fields

```python
T_EventResultType = TypeVar('T_EventResultType', bound=Any, default=None)

class BaseEvent(BaseModel, Generic[T_EventResultType]):
    # special config fields
    event_id: str                # Unique UUID7 identifier, auto-generated if not provided
    event_type: str              # Defaults to class name e.g. 'BaseEvent'
    event_result_type: Any | None  # Pydantic model/python type to validate handler return values, defaults to T_EventResultType
    event_version: str           # Defaults to '0.0.1' (override per class/instance for event payload versioning)
    event_timeout: float | None = None # Event timeout in seconds (bus default resolved at processing time if None)
    event_handler_timeout: float | None = None # Optional per-event handler timeout cap in seconds
    event_slow_timeout: float | None = None # Optional per-event slow-event warning threshold
    event_handler_slow_timeout: float | None = None # Optional per-event slow-handler warning threshold
    event_concurrency: Literal['global-serial', 'bus-serial', 'parallel'] | None = None  # optional per-event scheduling override (None -> bus default at processing time)
    event_handler_concurrency: Literal['serial', 'parallel'] | None = None  # optional per-event handler scheduling override (None -> bus default at processing time)
    event_handler_completion: Literal['all', 'first'] | None = None  # optional per-event completion override (None -> bus default at processing time)

    # runtime state fields
    event_status: Literal['pending', 'started', 'completed']  # event processing status (auto-set)
    event_created_at: str        # Canonical ISO timestamp with 9 fractional digits (auto-set)
    event_started_at: str | None # Set when first handler starts
    event_completed_at: str | None # Set when event processing completes
    event_parent_id: str | None  # Parent event ID that led to this event during handling (auto-set)
    event_path: list[str]        # List of bus labels traversed, e.g. BusName#ab12 (auto-set)
    event_results: dict[str, EventResult]   # Handler results {<handler uuid>: EventResult} (auto-set)
    event_children: list[BaseEvent] # getter property to list any child events emitted during handling
    event_bus: EventBus          # getter property to get the bus the event was emitted on

    # payload fields
    # ... subclass BaseEvent to add your own event payload fields here ...
    # some_key: str
    # some_other_key: dict[str, int]
    # ...
    # (they should not start with event_* to avoid conflict with special built-in fields)
```

#### `BaseEvent` Methods

##### `now(first_result: bool=False, timeout: float | None=None) -> Self`

Immediate path for the `Event` object.

- Outside a handler: processes the event immediately when it has not started yet, otherwise waits for completion.
- Inside a handler: queue-jumps this child event so it can run immediately, then returns the event.
- `first_result=True` waits only until the first valid result is available; remaining handlers continue running.
- `timeout` limits this wait call only. Use `event_timeout=0` / `event_handler_timeout=0` to disable execution timeouts.
- Python-only shortcut: `await event` is equivalent to `await event.now()`.

```python
event = bus.emit(MyEvent())
completed_event = await event.now()
first_result_event = await event.now(first_result=True, timeout=0.25)

raw_result_values = [event_result.result for event_result in completed_event.event_results.values()]
# equivalent to: completed_event.event_results_list()  (see below)
```

##### `wait(first_result: bool=False, timeout: float | None=None) -> Self`

- Never queue-jumps.
- Waits until the event is completed by normal runloop queue order.
- `first_result=True` waits only until the first valid result is available; remaining handlers continue running.
- `timeout` limits this wait call only.

```python
event = bus.emit(MyEvent())
completed_event = await event.wait()
first_result_event = await event.wait(first_result=True)
```

##### `reset() -> Self`

Return a fresh event copy with runtime processing state reset back to pending.

- Intended for re-emitting an already-seen event as a fresh event (for example after crossing a bridge boundary).
- The original event object is not mutated, it returns a new copy with some fields reset.
- A new UUIDv7 `event_id` is generated for the returned copy (to allow it to process as a separate event it needs a new unique uuid)
- Runtime completion state is cleared (`event_results`, completion signal/flags, processed timestamp, emit context).

##### `event_result_update(handler, eventbus: EventBus | None=None, **kwargs) -> EventResult`

Create or update a single `EventResult` entry for a handler.

- If no entry exists yet for the handler id, a pending result row is created.
- Useful for deterministic seeding/rehydration before normal processing resumes.
- Supports `status`, `result`, `error`, and `timeout` updates through `**kwargs`.

```python
seeded = event.event_result_update(handler=handler_entry, eventbus=bus, status='pending')
seeded.update(status='completed', result='seeded')
```

##### `event_result(include: EventResultFilter=None, raise_if_any: bool=True, raise_if_none: bool=False) -> Any`

Utility method helper to execute all the handlers and return the first handler's raw result value.

**Parameters:**

- `include`: Filter function `(result, event_result) -> bool` to include only specific results (default: only non-None, non-exception results)
- `raise_if_any`: If `True`, raise exception if any handler raises any `Exception` (`default: True`)
- `raise_if_none`: If `True`, raise exception if results are empty / all results are `None` or `Exception` (`default: False`)
- If every handler errors, only `raise_if_any=False` plus `raise_if_none=False` suppresses the error and returns `None`; every other option combination raises.

```python
# by default it returns the first successful non-None result value
result = await event.event_result()

# Get result from first handler that returns a string
valid_result = await event.event_result(include=lambda result, _: isinstance(result, str) and len(result) > 100)

# Get result but don't raise exceptions or error for 0 results, just return None
result_or_none = await event.event_result(raise_if_any=False, raise_if_none=False)
```

##### `event_results_list(include: EventResultFilter=None, raise_if_any: bool=True, raise_if_none: bool=False) -> list[Any]`

Utility method helper to get all raw result values in a list.

**Parameters:**

- `include`: Filter function `(result, event_result) -> bool` to include only specific results (default: only non-None, non-exception results)
- `raise_if_any`: If `True`, raise exception if any handler raises any `Exception` (`default: True`)
- `raise_if_none`: If `True`, raise exception if results are empty / all results are `None` or `Exception` (`default: False`)
- If every handler errors, only `raise_if_any=False` plus `raise_if_none=False` suppresses the error and returns `[]`; every other option combination raises.

```python
# by default it returns all successful non-None result values
results = await event.event_results_list()
# [result1, result2]

# Only include results that are strings longer than 10 characters
filtered_results = await event.event_results_list(include=lambda result, _: isinstance(result, str) and len(result) > 10)

# Get all results without raising on errors
all_results = await event.event_results_list(raise_if_any=False, raise_if_none=False)
```

`event_results_list()` is the canonical collection helper for multiple handler return values.

##### `event_bus` (property)

Shortcut to get the `EventBus` that is currently processing this event. Can be used to avoid having to pass an `EventBus` instance to your handlers.

```python
bus = EventBus()

async def some_handler(event: MyEvent):
    # Most handler code should do this: linked child work that blocks parent completion.
    child_event = await event.emit(ChildEvent()).now()

    # Un-awaited event.emit(...) keeps parentage without holding the parent open.
    background_child = event.emit(ChildEvent())

    # Use bus.emit(...) for detached root/background work.
    detached_event = bus.emit(ChildEvent())
```

---

</details>

<details>
<summary><strong><code>EventResult</code></strong></summary>

The placeholder object that represents the pending result from a single handler executing an event.  
`Event.event_results` contains a `dict[PythonIdStr, EventResult]` in the shape of `{handler_id: EventResult()}`.

You generally won't interact with this class directly—the bus instantiates and updates it for you—but its API is documented here for advanced integrations and custom emit loops.

#### `EventResult` Fields

```python
class EventResult(BaseModel):
    id: str                    # Unique identifier
    handler_id: str           # Handler function ID
    handler_name: str         # Handler function name
    eventbus_id: str          # Bus that executed this handler
    eventbus_name: str        # Bus name

    status: str               # 'pending', 'started', 'completed', 'error'
    result: Any               # Handler return value
    error: BaseException | None  # Captured exception if the handler failed

    started_at: str | None      # Canonical ISO timestamp when handler started
    completed_at: str | None    # Canonical ISO timestamp when handler completed
    timeout: float | None            # Handler timeout in seconds
    event_children: list[BaseEvent] # child events emitted during handler execution
```

#### `EventResult` Methods

##### `await result`

Await the `EventResult` object directly to get the raw result value.

```python
handler_result = event.event_results['handler_id']
value = await handler_result  # Returns result or raises an exception if handler hits an error
```

- `run_handler(event, handler, *, eventbus, timeout, enter_handler_context, exit_handler_context, format_exception_for_log)`  
  Low-level helper that runs the handler, updates timing/status fields, captures errors, and notifies its completion signal. `EventBus._run_handler()` (private/internal) delegates to this; you generally should not call either directly unless you are extending internals.

</details>

<details>
<summary><strong><code>EventHandler</code></strong></summary>

Serializable metadata wrapper around a registered handler callable.

You usually get an `EventHandler` back from `bus.on(...)`, can pass it to `bus.off(...)`, and may see it in middleware hooks like `on_bus_handlers_change(...)`.

#### `EventHandler` Fields

```python
class EventHandler(BaseModel):
    id: str                          # Stable handler identifier
    handler_name: str                # Callable name
    handler_file_path: str | None    # Source file path (if known)
    handler_timeout: float | None    # Optional per-handler timeout override
    handler_slow_timeout: float | None  # Optional "slow handler" threshold
    handler_registered_at: str       # Registration timestamp (ISO string, 9 fractional digits)
    event_pattern: str               # Registered event pattern (type name or '*')
    eventbus_name: str               # Owning EventBus name
    eventbus_id: str                 # Owning EventBus ID
```

The raw callable is stored on `handler`, but is excluded from JSON serialization (`model_dump(mode='json', exclude={'handler'})`).

#### `EventHandler` Properties and Methods

- `label` (property): Short display label like `my_handler#abcd`.
- `model_dump(mode='json', exclude={'handler'}) -> dict[str, Any]`: JSON-compatible metadata dict (callable excluded).
- `from_json_dict(data, handler=None) -> EventHandler`: Rebuilds metadata; optional callable reattachment.
- `from_callable(...) -> EventHandler`: Build a new handler entry from a callable plus bus/pattern metadata.

---

</details>

## 🏃 Performance

```bash
uv run tests/performance_runtime.py --no-json
pnpm --dir abxbus-ts run perf:node
cargo test --manifest-path abxbus-rust/Cargo.toml --release --test test_eventbus_performance -- --nocapture
(cd abxbus-go && go test ./tests -run TestPerformance -count=1 -timeout=180s -v)
```

| Runtime | 1 bus x 50k events x 1 handler   | 500 buses x 100 events x 1 handler | 1 bus x 1 event x 50k parallel handlers | 1 bus x 50k events x 50k one-off handlers | Worst case (N buses x N events x N handlers) |
| ------- | -------------------------------- | ---------------------------------- | --------------------------------------- | ----------------------------------------- | -------------------------------------------- |
| Python  | `0.366ms/event`, `0.188kb/event` | `0.408ms/event`, `0.153kb/event`   | `0.093ms/handler`, `10.197kb/handler`   | `0.633ms/event`, `0.145kb/event`          | `0.504ms/event`, `4.171kb/event`             |
| Rust    | `0.067ms/event`                  | `0.070ms/event`                    | `0.062ms/handler`                       | `0.077ms/event`                           | `0.227ms/event`                              |
| Go      | `0.016ms/event`                  | `0.011ms/event`                    | `0.085ms/handler`                       | `0.011ms/event`                           | `0.041ms/event`                              |
| TypeScript (Node) | `0.065ms/event`, `4.145kb/event` | `0.078ms/event`, `1.562kb/event`   | `0.065ms/handler`, `11.631kb/handler`   | `0.123ms/event`, `2.182kb/event`          | `0.344ms/event`, `12.619kb/event`            |

<br/>

---

---

<br/>

## 👾 Development

Set up the python development environment using `uv`:

```bash
git clone https://github.com/ArchiveBox/abxbus && cd abxbus

# Create virtual environment with Python 3.12
uv venv --python 3.12

# Activate virtual environment (varies by OS)
source .venv/bin/activate  # On Unix/macOS
# or
.venv\Scripts\activate  # On Windows

# Install dependencies
uv sync --dev --all-extras
```

Recommended once per clone:

```bash
prek install           # install pre-commit hooks
prek run --all-files   # run pre-commit hooks on all files manually
```

```bash
# Run linter & type checker
uv run ruff check --fix
uv run ruff format
uv run pyright

# Run all tests
uv run pytest -vxs --full-trace tests/

# Run specific test file
uv run pytest tests/test_eventbus.py

# Run Python perf suite
uv run tests/performance_runtime.py

# Run the entire lint+test+examples+perf suite for both python and ts
./test.sh
```

> For AbxBus-TS development see the `abxbus-ts/README.md` `# Development` section.
> For AbxBus-Rust development see `abxbus-rust/README.md`.
> For AbxBus-Go development run `go test ./...` from `abxbus-go/`; cross-runtime Go parity is covered by `tests/test_cross_runtime_roundtrip.py` and `abxbus-ts/tests/cross_runtime_roundtrip.test.ts`.

## 🔗 Inspiration

- https://www.cosmicpython.com/book/chapter_08_events_and_message_bus.html#message_bus_diagram ⭐️
- https://developer.mozilla.org/en-US/docs/Web/API/EventTarget ⭐️
- https://github.com/sindresorhus/emittery ⭐️, https://github.com/EventEmitter2/EventEmitter2, https://github.com/vitaly-t/sub-events
- https://github.com/pytest-dev/pluggy ⭐️
- https://github.com/teamhide/fastapi-event ⭐️
- https://github.com/ethereum/lahja ⭐️
- https://github.com/enricostara/eventure ⭐️
- https://github.com/akhundMurad/diator ⭐️
- https://github.com/n89nanda/pyeventbus
- https://github.com/iunary/aioemit
- https://github.com/dboslee/evently
- https://github.com/faust-streaming/faust
- https://github.com/ArcletProject/Letoderea
- https://github.com/seanpar203/event-bus
- https://github.com/n89nanda/pyeventbus
- https://github.com/nicolaszein/py-async-bus
- https://github.com/AngusWG/simple-event-bus
- https://www.joeltok.com/posts/2021-03-building-an-event-bus-in-python/
- See more here: https://abxbus.archivebox.io/further-reading/similar-projects

> [!TIP]
> **Don't like working with event-driven interfaces?**
> Check out our [`abxbus.events_suck`](https://abxbus.archivebox.io/further-reading/events-suck) wrapper utils that can help wrap events workflows in a simpler imperative API...

---

> [🍃 Main Documentation](https://abxbus.archivebox.io) | [🧠 DeepWiki | Get AI Help](https://deepwiki.com/ArchiveBox/abxbus) | [🐍 PyPI Package](https://pypi.org/project/abxbus) | [📦 NPM Package](https://npmjs.com/package/abxbus) | [</> Github](https://github.com/ArchiveBox/abxbus)
>
> <img width="400" alt="image" src="https://github.com/user-attachments/assets/cedb5a2e-0643-4240-9a3d-5f27cb8b5741" /><img width="400" alt="image" src="https://github.com/user-attachments/assets/3ee0ee8c-8322-449f-979b-5c99ba6bd960" />

## 🏛️ License

This project is licensed under the MIT License.

This repo is a fork that adds many new features and performance enhancements over the [original project named `bubus`](https://github.com/browser-use/bubus), which was built to power the [Browser-Use Agent](https://github.com/browser-use/browser-use/tree/main/browser_use/browser/watchdogs) (but has since gone stale).

Timeline:

- 2025-06 `v1.0.1`: Original library released https://github.com/browser-use/bubus
- 2025-10 `v1.5.1`: Browser-Use v0.6.0 released, first version powered by `bubus`
- 2025-11 `v1.7.1`: `bubus` forked to `pirate/bbus` temporarily; `ContextVar` support, `Middlewares`, and `bus.find()` added
- 2026-01 `v2.3.2`: `bubus-ts` Typescript implementation released, cross-compatible with Python version (now `abxbus-ts`)
- 2026-03 `v2.4.1`: Fork renamed from `pirate/bbus -> ArchiveBox/abxbus`; added dual `CJS`/`ESM` support, bugfixes and perf improvmeents
- 2026-03 `v2.4.9`: Added `update()`, `uninstall()`, and support for `uv`, `gem`, `cargo`, `go get`, `docker`, and `nix`. Used in new [`abx-dl`](https://github.com/ArchiveBox/abx-dl) project and [ArchiveBox](https://github.com/ArchiveBox/ArchiveBox).
