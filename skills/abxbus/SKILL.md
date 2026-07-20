---
name: abxbus
description: Use this when working on typed events, event bus execution, event history, ordering, timeouts, cancellation, and multi-runtime event contracts.
---

# abxbus

## Purpose

`abxbus` is the multi-runtime event bus used for typed events, handler execution, event history, timeout handling, and language contracts.

## Shared Rules

- Keep this repo on branch `main`.
- Use `uv` and `uv run` for Python commands.
- Do not use system `python`, direct `.venv/bin/python`, or `pip` commands.
- Use real events, real buses, real handlers, real async execution, real subprocesses when relevant, and real files.
- Do not mock, monkeypatch, fake, simulate, skip, xfail, or weaken tests.
- Verify event ordering, event history, handler results, timeouts, cancellation, emitted records, and side effects.
- Read `README.md` for the full event API, runtime matrix, bridge, and language-specific surface.

## Development Setup

```bash
uv sync --dev --all-extras --no-extra tachyon
pnpm --dir abxbus-ts install --frozen-lockfile
uv run pytest --collect-only -q
```

## User-Facing Setup

```bash
project_dir="$(mktemp -d)"
trap 'rm -rf "$project_dir"' EXIT
uv init --bare "$project_dir"
uv add --project "$project_dir" abxbus
```

## Basic Usage

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

## Verification

```bash
uv run pytest -n auto --dist loadfile \
    --ignore=tests/test_cross_runtime_roundtrip.py \
    --ignore=tests/test_eventbus_performance.py \
    tests -q
uv run pytest tests/test_eventbus.py -q
env -u GIT_CONFIG_COUNT -u GIT_CONFIG_KEY_0 -u GIT_CONFIG_VALUE_0 uv run prek run --all-files
```

Dedicated CI jobs run `tests/test_cross_runtime_roundtrip.py` with all required
native tools and bridge services, and `tests/test_eventbus_performance.py` with
isolated performance thresholds.

Keep event ordering and replay behavior deterministic.
