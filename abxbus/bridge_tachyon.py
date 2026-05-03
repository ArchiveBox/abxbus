"""Tachyon SPSC IPC bridge for forwarding events between runtimes.

Optional dependency: tachyon-ipc

Tachyon is a same-machine shared-memory ring buffer with single-producer/
single-consumer semantics. Each bridge instance plays exactly one role per
session (sender XOR listener) — the role is committed on the first call to
``emit()`` (sender) or ``on()`` (listener).

Usage:
    bridge = TachyonEventBridge('/tmp/abxbus.sock')

    # listener side (creates the SHM arena, must exist before sender connects)
    bridge.on('SomeEvent', handler)

    # sender side (separate process or instance)
    sender = TachyonEventBridge('/tmp/abxbus.sock')
    await sender.emit(event)
"""

from __future__ import annotations

import asyncio
import importlib
import json
import os
import threading
import time
from collections.abc import Callable
from typing import Any

from uuid_extensions import uuid7str

from abxbus.base_event import BaseEvent
from abxbus.event_bus import EventBus, EventPatternType, in_handler_context

_DEFAULT_TACHYON_CAPACITY = 1 << 20
_TACHYON_SOCKET_WAIT_TIMEOUT = 5.0


class TachyonEventBridge:
    def __init__(self, path: str, *, capacity: int = _DEFAULT_TACHYON_CAPACITY, name: str | None = None):
        if not path:
            raise ValueError('TachyonEventBridge path must not be empty')
        if capacity <= 0 or (capacity & (capacity - 1)) != 0:
            raise ValueError(f'TachyonEventBridge capacity must be a positive power of two, got: {capacity}')
        self.path = path
        self.capacity = int(capacity)
        self._inbound_bus = EventBus(name=name or f'TachyonEventBridge_{uuid7str()[-8:]}', max_history_size=0)

        self._send_bus: Any | None = None
        self._send_lock = asyncio.Lock()
        self._listener_bus: Any | None = None
        self._listener_thread: threading.Thread | None = None
        self._listener_loop: asyncio.AbstractEventLoop | None = None
        self._listener_init_error: BaseException | None = None
        self._running = False

    def on(self, event_pattern: EventPatternType, handler: Callable[[BaseEvent[Any]], Any]) -> None:
        self._ensure_listener_started()
        self._inbound_bus.on(event_pattern, handler)

    async def emit(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        await self._ensure_sender_connected()
        payload = event.model_dump(mode='json')
        encoded = json.dumps(payload, separators=(',', ':')).encode('utf-8')
        assert self._send_bus is not None
        await asyncio.to_thread(self._send_bus.send, encoded, 1)
        if in_handler_context():
            return None
        return event

    async def dispatch(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        return await self.emit(event)

    async def start(self) -> None:
        # Role is committed lazily on first on() / emit(); start() is a no-op.
        return

    async def close(self, *, clear: bool = True) -> None:
        # The listener thread is daemon and blocked on an uninterruptible recv;
        # we orphan it (it dies at process exit) rather than risk a deadlocked join.
        self._running = False
        if self._send_bus is not None:
            try:
                self._send_bus.__exit__(None, None, None)
            except Exception:
                pass
            self._send_bus = None
        self._listener_bus = None
        self._listener_thread = None
        if os.path.exists(self.path):
            try:
                os.unlink(self.path)
            except OSError:
                pass
        await self._inbound_bus.stop(clear=clear)

    def _ensure_listener_started(self) -> None:
        if self._listener_thread is not None:
            return
        try:
            self._listener_loop = asyncio.get_running_loop()
        except RuntimeError:
            self._listener_loop = None

        tachyon_module = self._load_tachyon()
        if os.path.exists(self.path):
            try:
                os.unlink(self.path)
            except OSError:
                pass

        bus_ready = threading.Event()

        def _run() -> None:
            try:
                bus = tachyon_module.Bus.listen(self.path, self.capacity)
            except BaseException as exc:
                self._listener_init_error = exc
                bus_ready.set()
                return
            self._listener_bus = bus
            self._running = True
            bus_ready.set()
            try:
                for msg in bus:
                    if not self._running:
                        break
                    try:
                        payload = json.loads(bytes(msg.data).decode('utf-8'))
                    except Exception:
                        continue
                    self._dispatch_inbound_payload_threadsafe(payload)
            except Exception:
                pass

        thread = threading.Thread(target=_run, daemon=True, name='TachyonEventBridge-listener')
        thread.start()
        self._listener_thread = thread

        deadline = time.monotonic() + _TACHYON_SOCKET_WAIT_TIMEOUT
        while time.monotonic() < deadline:
            if self._listener_init_error is not None:
                raise RuntimeError(f'TachyonEventBridge failed to listen on {self.path}') from self._listener_init_error
            if os.path.exists(self.path):
                return
            time.sleep(0.005)

    async def _ensure_sender_connected(self) -> None:
        if self._send_bus is not None:
            return
        async with self._send_lock:
            if self._send_bus is not None:
                return
            tachyon_module = self._load_tachyon()
            self._send_bus = await asyncio.to_thread(self._connect_with_retries, tachyon_module)

    def _connect_with_retries(self, tachyon_module: Any) -> Any:
        last_exc: BaseException | None = None
        deadline = time.monotonic() + _TACHYON_SOCKET_WAIT_TIMEOUT
        while time.monotonic() < deadline:
            try:
                return tachyon_module.Bus.connect(self.path)
            except Exception as exc:
                last_exc = exc
                time.sleep(0.01)
        raise RuntimeError(f'TachyonEventBridge failed to connect to {self.path}') from last_exc

    def _dispatch_inbound_payload_threadsafe(self, payload: Any) -> None:
        try:
            event = BaseEvent[Any].model_validate(payload).event_reset()
        except Exception:
            return
        loop = self._listener_loop
        if loop is None or loop.is_closed():
            return
        loop.call_soon_threadsafe(self._inbound_bus.emit, event)

    @staticmethod
    def _load_tachyon() -> Any:
        try:
            return importlib.import_module('tachyon')
        except ModuleNotFoundError as exc:
            raise RuntimeError('TachyonEventBridge requires optional dependency: pip install tachyon-ipc') from exc
