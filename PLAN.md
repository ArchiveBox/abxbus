# Plan: Rust-Owned Event Loop via pyo3-async-runtimes

## Goal
Rust owns the **entire dispatch loop**. Python is a thin shell that provides handler callables and Pydantic models.

## Architecture

```
Python                          Rust (tokio)
──────                          ────────────
bus.on(Event, handler)  ──→    store PyObject callable + metadata
bus.emit(event)         ──→    enqueue to tokio mpsc, return awaitable
await event             ──→    resolves when Rust marks event complete

                               ┌─ dispatch loop (tokio task) ─────────┐
                               │ dequeue event                        │
                               │ resolve event lock (tokio Semaphore) │
                               │ get_handlers_for_event()             │
                               │ resolve handler lock mode             │
                               │ for each handler:                     │
                               │   resolve timeout                     │
                               │   tokio::time::timeout(               │
                               │     call Python handler via into_future│
                               │   )                                   │
                               │ check completion                      │
                               │ signal Python awaitable               │
                               └──────────────────────────────────────┘
```

## What moves to Rust

| Component | Current (Python) | Target (Rust) |
|-----------|-----------------|---------------|
| Queue | `asyncio.Queue` | `tokio::sync::mpsc` channel |
| Dispatch loop | `_run_loop_weak()` | tokio task polling mpsc |
| Event locks | `asyncio.Semaphore` | `tokio::sync::Semaphore` |
| Handler locks | `asyncio.Semaphore` per event | `tokio::sync::Semaphore` per event |
| Handler invocation | `event_result.run_handler()` | `into_future(py_callable.call1(...))` |
| Timeout enforcement | `asyncio.timeout()` | `tokio::time::timeout()` |
| Completion signal | `asyncio.Event` | `tokio::sync::oneshot` → Python awaitable via `future_into_py` |
| Serial/parallel dispatch | `asyncio.create_task` vs direct | `tokio::spawn` vs `.await` |
| Parent/child tracking | ContextVar + emit() | Already in Rust, plus emit() sets parent |
| Handler registration | `self.handlers` dict | Already in Rust, now also stores `PyObject` |

## What stays in Python

- `BaseEvent[T]` / `EventResult[T]` Pydantic models (type system, serialization)
- Handler callables themselves (they're Python functions)
- ContextVar propagation for `current_event` / `current_handler` (Python-specific)
- Middleware hooks (`on_event_change`, `on_event_result_change`) — Python callables
- The `EventBus` class shell (thin wrapper calling into Rust)

## Dependencies

```toml
[dependencies]
pyo3 = { version = "0.25", features = ["extension-module"] }
pyo3-async-runtimes = { version = "0.25", features = ["tokio-runtime"] }
tokio = { version = "1", features = ["full"] }
```

## Implementation Steps

### Step 1: Add pyo3-async-runtimes dependency
- Add to Cargo.toml
- Verify it builds with our pyo3 version

### Step 2: Store handler callables in Rust
- `register_handler()` takes a `PyObject` (the Python callable)
- `HandlerMeta` stores `handler: PyObject`
- `unregister_handler()` drops the PyObject

### Step 3: Rust dispatch loop
- `RustEventBusCore` owns a `tokio::sync::mpsc::UnboundedSender<String>` (event_id queue)
- Background tokio task receives event_ids, runs the dispatch logic
- `emit()` becomes an async Rust method that:
  1. Stores event metadata
  2. Sends event_id to channel
  3. Returns a `future_into_py` awaitable that resolves on completion

### Step 4: Handler firing from Rust
- For each handler, Rust:
  1. Acquires GIL
  2. Calls `handler_callable.call1(py, (event_obj,))`
  3. If result is a coroutine, uses `into_future()` to await it
  4. Wraps in `tokio::time::timeout()` for timeout enforcement
  5. Records result status

### Step 5: Lock management in Rust
- Event-level locks: `HashMap<String, Arc<tokio::sync::Semaphore>>` keyed by lock mode
- Handler-level locks: per-event `Semaphore` for serial mode
- Global serial: single `Semaphore(1)` on the core
- Bus serial: single `Semaphore(1)` on the core

### Step 6: Completion signaling
- Each event gets a `tokio::sync::watch::Sender<bool>`
- When all results are terminal, Rust sends `true`
- The Python awaitable (from `future_into_py`) awaits the watch receiver

### Step 7: Wire up Python EventBus
- `emit()` → `self._rust_core.emit(event)` (returns awaitable)
- `on()` → `self._rust_core.register_handler(id, pattern, callable)`
- `_run_loop_weak()` → removed (Rust owns the loop)
- `_process_event()` → removed
- `_run_handler()` → removed (Rust calls handlers directly)
- `step()` → removed
- Keep: `find()`, `wait_for_event()` (thin wrappers over Rust)

## Key Gotchas (from research)

1. **GIL across .await**: Must release GIL before awaiting. Pattern: `Python::with_gil(|py| { setup })`, then `.await` outside GIL.
2. **TaskLocals**: Need to capture `TaskLocals` (Python event loop ref) before spawning tokio tasks that call Python.
3. **Python owns main thread**: Python's asyncio loop runs on main thread. Tokio runs in background threads.
4. **ContextVars**: Use `TaskLocals::copy_context()` to preserve Python context vars across the bridge.
5. **Middleware**: Still Python callables. Rust calls them via GIL at status change points.
