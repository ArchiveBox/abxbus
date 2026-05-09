"""NATS bridge for forwarding events between runtimes.

Optional dependency: nats-py
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
from collections.abc import Callable
from typing import Any

from uuid_extensions import uuid7str

from abxbus.base_event import BaseEvent
from abxbus.event_bus import EventPatternType, in_handler_context
from abxbus.helpers import QueueShutDown


class NATSEventBridge:
    def __init__(self, server: str, subject: str, *, name: str | None = None):
        self.server = server
        self.subject = subject
        self.name = name or f'NATSEventBridge_{uuid7str()[-8:]}'

        self._running = False
        self._start_task: asyncio.Task[None] | None = None
        self._start_lock = asyncio.Lock()
        self._nc: Any | None = None
        self._handlers: list[tuple[EventPatternType, Callable[[BaseEvent[Any]], Any]]] = []

    def on(self, event_pattern: EventPatternType, handler: Callable[[BaseEvent[Any]], Any]) -> None:
        self._ensure_started()
        self._handlers.append((event_pattern, handler))

    async def emit(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        self._ensure_started()
        if self._nc is None:
            await self.start()

        payload = event.model_dump(mode='json')
        assert self._nc is not None
        await self._nc.publish(self.subject, json.dumps(payload, separators=(',', ':')).encode('utf-8'))

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

                nats_module = self._load_nats()
                nc = await nats_module.connect(self.server)

                async def _on_msg(msg: Any) -> None:
                    try:
                        payload = json.loads(msg.data.decode('utf-8'))
                    except Exception:
                        return
                    try:
                        await self._dispatch_inbound_payload(payload)
                    except QueueShutDown:
                        return

                try:
                    await nc.subscribe(self.subject, cb=_on_msg)
                except Exception:
                    try:
                        await nc.close()
                    except Exception:
                        pass
                    raise

                self._nc = nc
                self._running = True
        finally:
            if self._start_task is current_task:
                self._start_task = None

    async def close(self, *, clear: bool = True) -> None:
        if self._start_task is not None:
            self._start_task.cancel()
            await asyncio.gather(self._start_task, return_exceptions=True)
            self._start_task = None
        self._running = False
        if self._nc is not None:
            try:
                await self._nc.drain()
            except Exception:
                pass
            try:
                await self._nc.close()
            except Exception:
                pass
            self._nc = None

    def _ensure_started(self) -> None:
        if self._running:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        if self._start_task is None or self._start_task.done():
            self._start_task = asyncio.create_task(self.start())

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

    @staticmethod
    def _load_nats() -> Any:
        try:
            return importlib.import_module('nats')
        except ModuleNotFoundError as exc:
            raise RuntimeError('NATSEventBridge requires optional dependency: pip install nats-py') from exc
