"""JSONL bridge for forwarding events between runtimes.

This bridge is intentionally simple:
- emit/dispatch appends one raw event JSON object per line
- listener polls the file and emits any unseen lines
"""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from uuid_extensions import uuid7str

from abxbus.base_event import BaseEvent
from abxbus.event_bus import EventPatternType, in_handler_context


class JSONLEventBridge:
    def __init__(self, path: str, *, poll_interval: float = 0.25, name: str | None = None):
        self.path = Path(path)
        self.poll_interval = poll_interval
        self.name = name or f'JSONLEventBridge_{uuid7str()[-8:]}'

        self._running = False
        self._start_task: asyncio.Task[None] | None = None
        self._start_lock = asyncio.Lock()
        self._listener_task: asyncio.Task[None] | None = None
        self._byte_offset = 0
        self._pending_line = ''
        self._handlers: list[tuple[EventPatternType, Callable[[BaseEvent[Any]], Any]]] = []

    def on(self, event_pattern: EventPatternType, handler: Callable[[BaseEvent[Any]], Any]) -> None:
        self._ensure_started()
        self._handlers.append((event_pattern, handler))

    async def emit(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        self._ensure_started()

        payload = event.model_dump(mode='json')
        self.path.parent.mkdir(parents=True, exist_ok=True)

        await asyncio.to_thread(self._append_line, json.dumps(payload, separators=(',', ':')))

        if in_handler_context():
            return None
        return event

    async def dispatch(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        return await self.emit(event)

    async def start(self) -> None:
        current_task = asyncio.current_task()
        if self._start_task is not None and self._start_task is not current_task and not self._start_task.done():
            await self._start_task
            return

        if self._running:
            return

        try:
            async with self._start_lock:
                if self._running:
                    return
                self.path.parent.mkdir(parents=True, exist_ok=True)
                self.path.touch(exist_ok=True)
                self._byte_offset = self.path.stat().st_size
                self._pending_line = ''
                self._running = True
                if self._listener_task is None or self._listener_task.done():
                    self._listener_task = asyncio.create_task(self._listen_loop())
        finally:
            if self._start_task is current_task:
                self._start_task = None

    async def close(self, *, clear: bool = True) -> None:
        if self._start_task is not None:
            self._start_task.cancel()
            await asyncio.gather(self._start_task, return_exceptions=True)
            self._start_task = None
        self._running = False
        if self._listener_task is not None:
            self._listener_task.cancel()
            await asyncio.gather(self._listener_task, return_exceptions=True)
            self._listener_task = None

    def _ensure_started(self) -> None:
        if self._running:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        if self._start_task is None or self._start_task.done():
            self._start_task = asyncio.create_task(self.start())

    async def _listen_loop(self) -> None:
        while self._running:
            try:
                await self._poll_new_lines()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            await asyncio.sleep(self.poll_interval)

    async def _poll_new_lines(self) -> None:
        previous_offset = self._byte_offset
        appended_text, new_offset = await asyncio.to_thread(self._read_appended_text, previous_offset)
        self._byte_offset = new_offset

        if new_offset < previous_offset:
            self._pending_line = ''

        if not appended_text:
            return

        combined_text = self._pending_line + appended_text
        new_lines = combined_text.split('\n')
        self._pending_line = new_lines.pop() if new_lines else ''

        for line in new_lines:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            await self._dispatch_inbound_payload(payload)

    async def _dispatch_inbound_payload(self, payload: Any) -> None:
        event = BaseEvent[Any].model_validate(payload).event_reset()
        for event_pattern, handler in list(self._handlers):
            if not self._matches(event_pattern, event):
                continue
            result = handler(event)
            if inspect.isawaitable(result):
                await result

    @staticmethod
    def _matches(event_pattern: EventPatternType, event: BaseEvent[Any]) -> bool:
        if event_pattern == '*':
            return True
        if isinstance(event_pattern, str):
            return event_pattern == event.event_type
        return event.event_type == event_pattern.__name__

    def _read_appended_text(self, offset: int) -> tuple[str, int]:
        try:
            with self.path.open('r', encoding='utf-8') as fp:
                fp.seek(0, 2)
                file_size = fp.tell()

                start_offset = 0 if file_size < offset else offset
                if file_size == start_offset:
                    return '', file_size

                fp.seek(start_offset)
                return fp.read(), fp.tell()
        except FileNotFoundError:
            return '', 0

    def _append_line(self, payload: str) -> None:
        with self.path.open('a', encoding='utf-8') as fp:
            fp.write(payload + '\n')
