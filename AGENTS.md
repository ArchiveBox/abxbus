# abxbus Agent Guide

`abxbus` is the multi-runtime event bus used for typed events, handler execution, event history, timeout handling, and cross-language event contracts. Keep this repo on `main`.

## Shared Standards

- Use `uv` and `uv run` for Python commands. Do not use system `python`, direct `.venv/bin/python`, or `pip` commands.
- Prefer existing repo patterns, helper APIs, fixtures, scripts, and command surfaces.
- Keep edits focused and minimal. Do not add wrappers, shims, aliases, or extra abstraction layers unless the current code path requires them.
- Do not weaken assertions, skip tests, xfail tests, or accept flaky behavior.
- No mocks, monkeypatches, fakes, simulated buses, fake handlers, or direct shortcuts around user-facing flows.
- Tests and verification should use real events, real buses, real handlers, real async execution, real subprocesses when relevant, real files, and existing fixtures.
- Assertions must verify real correctness: event ordering, event history, handler results, timeouts, cancellation, side effects, and emitted records.
- Start behavior fixes with a red failing test when a test is requested or practical.
- Trace root causes from observed behavior. Do not paper over failures with retries, wider timeouts, broad fallbacks, or looser assertions.
- Read `README.md` for the full event API, runtime matrix, bridge, and language-specific surface.

## Development Setup

```console
uv sync
uv run pytest --collect-only -q
```

## User-Facing Setup

```bash
project_dir="$(mktemp -d)"
trap 'rm -rf "$project_dir"' EXIT
uv init --bare "$project_dir"
uv add --project "$project_dir" abxbus
```

Python usage:

```python
import asyncio
from abxbus import EventBus, BaseEvent

class UserEvent(BaseEvent[str]):
    username: str

async def handle_user(event: UserEvent) -> str:
    return event.username

async def main():
    bus = EventBus()
    bus.on(UserEvent, handle_user)
    event = await bus.emit(UserEvent(username="alice")).now()
    assert await event.event_result() == "alice"
    await bus.destroy()

asyncio.run(main())
```

## Basic Usage

```console
uv run pytest tests -q
uv run pytest tests/test_event_bus.py -q
uv run prek run --all-files
```

Keep event ordering and replay behavior deterministic. Event lifecycle behavior belongs in the core bus implementation and its existing language-specific counterparts.
