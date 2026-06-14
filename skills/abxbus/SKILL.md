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
uv sync
uv run pytest --collect-only -q
```

## User-Facing Setup

```bash
uv add abxbus
```

## Basic Usage

```python
from abxbus import EventBus, BaseEvent

class UserEvent(BaseEvent[str]):
    username: str

async def handle_user(event: UserEvent) -> str:
    return event.username

bus = EventBus()
bus.on(UserEvent, handle_user)
result = await bus.emit(UserEvent(username="alice")).result()
```

## Verification

```bash
uv run pytest tests -q
uv run pytest tests/test_event_bus.py -q
uv run prek run --all-files
```

Keep event ordering and replay behavior deterministic.
