"""Redis pub/sub bridge for forwarding events between runtimes.

Optional dependency: redis>=5 (uses redis.asyncio)

Usage:
    # channel from URL path
    bridge = RedisEventBridge('redis://user:pass@localhost:6379/1/my_channel')

    # explicit channel override
    bridge = RedisEventBridge('redis://user:pass@localhost:6379/1', channel='my_channel')

Connection URL format:
    redis://user:pass@host:6379/1/channel_name

The first path segment is the Redis logical DB index.
An optional second path segment is used as the pub/sub channel.
If channel is omitted in both URL and constructor, defaults to "abxbus_events".
"""

from __future__ import annotations

import asyncio
import importlib
import json
from collections.abc import Callable
from typing import Any, cast
from urllib.parse import urlsplit, urlunsplit

from uuid_extensions import uuid7str

from abxbus.base_event import BaseEvent
from abxbus.event_bus import EventBus, EventPatternType, in_handler_context

_DEFAULT_REDIS_CHANNEL = 'abxbus_events'
_DB_INIT_KEY = '__abxbus:bridge_init__'


def _parse_redis_url(redis_url: str, channel: str | None) -> tuple[str, str]:
    parsed = urlsplit(redis_url)
    scheme = parsed.scheme.lower()
    if scheme not in ('redis', 'rediss'):
        raise ValueError(f'RedisEventBridge URL must use redis:// or rediss://, got: {redis_url}')

    path_segments = [segment for segment in parsed.path.split('/') if segment]
    if len(path_segments) > 2:
        raise ValueError(f'RedisEventBridge URL path must be /<db> or /<db>/<channel>, got: {parsed.path or "/"}')

    db_index = '0'
    channel_from_url: str | None = None
    if path_segments:
        db_index = path_segments[0]
        if not db_index.isdigit():
            raise ValueError(f'RedisEventBridge URL db path segment must be numeric, got: {db_index!r} in {redis_url}')
        if len(path_segments) == 2:
            channel_from_url = path_segments[1]

    resolved_channel = channel or channel_from_url or _DEFAULT_REDIS_CHANNEL
    if not resolved_channel:
        raise ValueError('RedisEventBridge channel must not be empty')

    normalized_path = f'/{db_index}'
    normalized_url = urlunsplit((parsed.scheme, parsed.netloc, normalized_path, parsed.query, parsed.fragment))
    return normalized_url, resolved_channel


class RedisEventBridge:
    def __init__(self, redis_url: str, channel: str | None = None, *, name: str | None = None):
        self.url, self.channel = _parse_redis_url(redis_url, channel)
        self._inbound_bus = EventBus(name=name or f'RedisEventBridge_{uuid7str()[-8:]}', max_history_size=0)

        self._running = False
        self._start_task: asyncio.Task[None] | None = None
        self._start_lock = asyncio.Lock()
        self._listener_task: asyncio.Task[None] | None = None
        self._redis_pub: Any | None = None
        self._redis_sub: Any | None = None
        self._pubsub: Any | None = None

    def on(self, event_pattern: EventPatternType, handler: Callable[[BaseEvent[Any]], Any]) -> None:
        self._ensure_started()
        self._inbound_bus.on(event_pattern, handler)

    async def emit(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        self._ensure_started()
        if self._redis_pub is None:
            await self.start()

        payload = event.model_dump(mode='json')
        assert self._redis_pub is not None
        await self._redis_pub.publish(self.channel, json.dumps(payload, separators=(',', ':')))

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

                redis_asyncio = self._load_redis_asyncio()
                redis_pub = redis_asyncio.from_url(self.url, decode_responses=True)
                redis_sub = redis_asyncio.from_url(self.url, decode_responses=True)
                pubsub = redis_sub.pubsub()

                try:
                    # Redis logical DBs are created lazily; writing a short-lived key initializes/validates the selected DB.
                    await redis_pub.set(_DB_INIT_KEY, '1', ex=60, nx=True)
                    await pubsub.subscribe(self.channel)
                except Exception:
                    await self._close_pubsub(pubsub)
                    await self._close_redis_client(redis_sub)
                    await self._close_redis_client(redis_pub)
                    raise

                self._redis_pub = redis_pub
                self._redis_sub = redis_sub
                self._pubsub = pubsub
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

        if self._pubsub is not None:
            await self._close_pubsub(self._pubsub)
            self._pubsub = None
        if self._redis_sub is not None:
            await self._close_redis_client(self._redis_sub)
            self._redis_sub = None
        if self._redis_pub is not None:
            await self._close_redis_client(self._redis_pub)
            self._redis_pub = None

        await self._inbound_bus.stop(clear=clear)

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
        assert self._pubsub is not None
        try:
            async for message in self._pubsub.listen():
                if not self._running:
                    break
                if not isinstance(message, dict):
                    continue
                message_dict = cast(dict[str, Any], message)
                if message_dict.get('type') != 'message':
                    continue

                raw_data = message_dict.get('data')
                if isinstance(raw_data, bytes):
                    data = raw_data.decode('utf-8')
                elif isinstance(raw_data, str):
                    data = raw_data
                else:
                    continue

                try:
                    payload = json.loads(data)
                except Exception:
                    continue
                await self._dispatch_inbound_payload(payload)
        except asyncio.CancelledError:
            raise
        except Exception:
            if self._running:
                await asyncio.sleep(0.05)

    async def _dispatch_inbound_payload(self, payload: Any) -> None:
        event = BaseEvent[Any].model_validate(payload).event_reset()
        self._inbound_bus.emit(event)

    async def _close_pubsub(self, pubsub: Any) -> None:
        try:
            await pubsub.unsubscribe(self.channel)
        except Exception:
            pass
        try:
            await pubsub.close()
        except Exception:
            pass

    @staticmethod
    async def _close_redis_client(client: Any) -> None:
        try:
            await client.close()
        except Exception:
            pass

    @staticmethod
    def _load_redis_asyncio() -> Any:
        try:
            return importlib.import_module('redis.asyncio')
        except ModuleNotFoundError as exc:
            raise RuntimeError('RedisEventBridge requires optional dependency: pip install redis') from exc
