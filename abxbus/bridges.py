"""IPC bridges for forwarding EventBus instances over HTTP or unix sockets."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from anyio import Path as AnyPath
from uuid_extensions import uuid7str

from abxbus.base_event import BaseEvent
from abxbus.bridge_jsonl import JSONLEventBridge
from abxbus.bridge_sqlite import SQLiteEventBridge
from abxbus.event_bus import EventPatternType, in_handler_context

logger = logging.getLogger('abxbus.bridges')
UNIX_SOCKET_MAX_PATH_CHARS = 90
__all__ = [
    'EventBridge',
    'HTTPEventBridge',
    'SocketEventBridge',
    'JSONLEventBridge',
    'SQLiteEventBridge',
    'NATSEventBridge',
    'RedisEventBridge',
    'PostgresEventBridge',
    'TachyonEventBridge',
]

if TYPE_CHECKING:
    from .bridge_nats import NATSEventBridge
    from .bridge_postgres import PostgresEventBridge
    from .bridge_redis import RedisEventBridge
    from .bridge_tachyon import TachyonEventBridge

_LAZY_BRIDGE_MODULES: dict[str, str] = {
    'NATSEventBridge': '.bridge_nats',
    'RedisEventBridge': '.bridge_redis',
    'PostgresEventBridge': '.bridge_postgres',
    'TachyonEventBridge': '.bridge_tachyon',
}


class _Endpoint:
    def __init__(
        self,
        raw: str,
        scheme: Literal['unix', 'http', 'https'],
        *,
        host: str | None = None,
        port: int | None = None,
        path: str | None = None,
    ):
        self.raw = raw
        self.scheme = scheme
        self.host = host
        self.port = port
        self.path = path


def _parse_endpoint(raw_endpoint: str) -> _Endpoint:
    parsed = urlparse(raw_endpoint)
    scheme = parsed.scheme.lower()

    if scheme == 'unix':
        socket_path = parsed.path or parsed.netloc
        if not socket_path:
            raise ValueError(f'Invalid unix endpoint (missing socket path): {raw_endpoint}')
        socket_path_len = len(socket_path.encode('utf-8'))
        if socket_path_len > UNIX_SOCKET_MAX_PATH_CHARS:
            raise ValueError(
                f'Unix socket path is too long ({socket_path_len} chars), max is {UNIX_SOCKET_MAX_PATH_CHARS}: {socket_path}'
            )
        return _Endpoint(raw_endpoint, 'unix', path=socket_path)

    if scheme == 'http':
        if not parsed.hostname:
            raise ValueError(f'Invalid HTTP endpoint (missing hostname): {raw_endpoint}')
        request_path = parsed.path or '/'
        if parsed.query:
            request_path = f'{request_path}?{parsed.query}'
        port = parsed.port if parsed.port is not None else 80
        return _Endpoint(raw_endpoint, 'http', host=parsed.hostname, port=port, path=request_path)

    if scheme == 'https':
        if not parsed.hostname:
            raise ValueError(f'Invalid HTTP endpoint (missing hostname): {raw_endpoint}')
        request_path = parsed.path or '/'
        if parsed.query:
            request_path = f'{request_path}?{parsed.query}'
        port = parsed.port if parsed.port is not None else 443
        return _Endpoint(raw_endpoint, 'https', host=parsed.hostname, port=port, path=request_path)

    raise ValueError(f'Unsupported endpoint scheme: {raw_endpoint}')


class EventBridge:
    """Shared bridge implementation exposing EventBus-like on/emit/dispatch."""

    def __init__(
        self,
        send_to: str | None = None,
        listen_on: str | None = None,
        *,
        name: str | None = None,
    ):
        self.send_to = _parse_endpoint(send_to) if send_to else None
        self.listen_on = _parse_endpoint(listen_on) if listen_on else None
        self.name = name or f'EventBridge_{uuid7str()[-8:]}'

        self._server: asyncio.AbstractServer | None = None
        self._start_lock = asyncio.Lock()
        self._listen_socket_path: Path | None = None
        self._autostart_task: asyncio.Task[None] | None = None
        self._handlers: list[tuple[EventPatternType, Callable[[BaseEvent[Any]], Any]]] = []

    def on(self, event_pattern: EventPatternType, handler: Callable[[BaseEvent[Any]], Any]) -> None:
        self._ensure_listener_started()
        self._handlers.append((event_pattern, handler))

    async def emit(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        if self.send_to is None:
            raise RuntimeError(f'{self.__class__.__name__}.emit() requires send_to=...')

        payload = event.model_dump(mode='json')

        if self.send_to.scheme == 'unix':
            await self._send_unix(self.send_to, payload)
        else:
            await self._send_http(self.send_to, payload)

        if in_handler_context():
            return None
        return event

    async def dispatch(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        return await self.emit(event)

    async def start(self) -> None:
        if self.listen_on is None or self._server is not None:
            return

        async with self._start_lock:
            if self._server is not None:
                return

            endpoint = self.listen_on
            assert endpoint is not None
            if endpoint.scheme == 'unix':
                socket_path = Path(endpoint.path or '')
                if not socket_path.is_absolute():
                    raise ValueError(f'unix listen_on path must be absolute, got: {endpoint.raw}')
                socket_path.parent.mkdir(parents=True, exist_ok=True)
                async_socket_path = AnyPath(socket_path)
                if await async_socket_path.exists():
                    await async_socket_path.unlink()
                self._listen_socket_path = socket_path
                self._server = await asyncio.start_unix_server(self._handle_unix_client, path=str(socket_path))
                return

            if endpoint.scheme != 'http':
                raise ValueError(f'listen_on only supports unix:// or http:// endpoints, got: {endpoint.raw}')
            assert endpoint.host is not None
            assert endpoint.port is not None
            self._server = await asyncio.start_server(self._handle_http_client, host=endpoint.host, port=endpoint.port)

    async def close(self, *, clear: bool = True) -> None:
        if self._autostart_task is not None:
            await asyncio.gather(self._autostart_task, return_exceptions=True)
            self._autostart_task = None

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        if self._listen_socket_path and self._listen_socket_path.exists():
            self._listen_socket_path.unlink()
            self._listen_socket_path = None

    def _ensure_listener_started(self) -> None:
        if self.listen_on is None or self._server is not None:
            return
        if self._autostart_task is not None and not self._autostart_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._autostart_task = loop.create_task(self.start())

    async def _handle_unix_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                if not line.strip():
                    continue
                await self._handle_incoming_bytes(line)
        finally:
            writer.close()
            await writer.wait_closed()

    async def _handle_http_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            raw_headers = await reader.readuntil(b'\r\n\r\n')
        except asyncio.IncompleteReadError:
            await self._write_http_response(writer, status=400, body='incomplete request')
            return
        except asyncio.LimitOverrunError:
            await self._write_http_response(writer, status=400, body='headers too large')
            return

        header_lines = raw_headers.decode('utf-8', errors='replace').split('\r\n')
        if not header_lines or not header_lines[0]:
            await self._write_http_response(writer, status=400, body='missing request line')
            return

        parts = header_lines[0].split(' ', 2)
        if len(parts) != 3:
            await self._write_http_response(writer, status=400, body='invalid request line')
            return
        method, request_target, _version = parts

        headers: dict[str, str] = {}
        for line in header_lines[1:]:
            if not line:
                continue
            name, separator, value = line.partition(':')
            if not separator:
                continue
            headers[name.strip().lower()] = value.strip()

        if method.upper() != 'POST':
            await self._write_http_response(writer, status=405, body='method not allowed')
            return

        expected_path = (self.listen_on.path if self.listen_on else None) or '/'
        if request_target != expected_path:
            await self._write_http_response(writer, status=404, body='not found')
            return

        content_length = headers.get('content-length', '0')
        try:
            body_size = int(content_length)
        except ValueError:
            await self._write_http_response(writer, status=400, body='invalid content-length')
            return

        if body_size < 0:
            await self._write_http_response(writer, status=400, body='invalid content-length')
            return

        try:
            body = await reader.readexactly(body_size)
        except asyncio.IncompleteReadError:
            await self._write_http_response(writer, status=400, body='incomplete body')
            return

        try:
            await self._handle_incoming_bytes(body)
        except Exception as exc:  # pragma: no cover
            logger.exception('Failed to process inbound IPC event: %s', exc)
            await self._write_http_response(writer, status=500, body='failed to process event')
            return

        await self._write_http_response(writer, status=202, body='accepted')

    async def _handle_incoming_bytes(self, payload: bytes) -> None:
        message = json.loads(payload.decode('utf-8'))
        event = BaseEvent[Any].model_validate(message).event_reset()
        await self._dispatch_event(event)

    async def _dispatch_event(self, event: BaseEvent[Any]) -> None:
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

    async def _send_unix(self, endpoint: _Endpoint, payload: dict[str, Any]) -> None:
        socket_path = endpoint.path or ''
        if not socket_path:
            raise ValueError(f'Invalid unix endpoint: {endpoint.raw}')

        _reader, writer = await asyncio.open_unix_connection(path=socket_path)
        writer.write(json.dumps(payload, separators=(',', ':')).encode('utf-8'))
        writer.write(b'\n')
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async def _send_http(self, endpoint: _Endpoint, payload: dict[str, Any]) -> None:
        payload_bytes = json.dumps(payload, separators=(',', ':')).encode('utf-8')
        request = Request(
            endpoint.raw,
            data=payload_bytes,
            headers={
                'content-type': 'application/json',
                'content-length': str(len(payload_bytes)),
            },
            method='POST',
        )

        def _post() -> int:
            with urlopen(request, timeout=10) as response:
                return int(response.status)

        status_code = await asyncio.to_thread(_post)
        if status_code < 200 or status_code >= 300:
            raise RuntimeError(f'IPC HTTP send failed with status {status_code}: {endpoint.raw}')

    @staticmethod
    async def _write_http_response(writer: asyncio.StreamWriter, *, status: int, body: str) -> None:
        reasons = {
            202: 'Accepted',
            400: 'Bad Request',
            404: 'Not Found',
            405: 'Method Not Allowed',
            500: 'Internal Server Error',
        }
        reason = reasons.get(status, 'OK')
        body_bytes = body.encode('utf-8')
        headers = [
            f'HTTP/1.1 {status} {reason}',
            f'content-length: {len(body_bytes)}',
            'content-type: text/plain; charset=utf-8',
            'connection: close',
            '',
            '',
        ]
        writer.write('\r\n'.join(headers).encode('utf-8'))
        writer.write(body_bytes)
        await writer.drain()
        writer.close()
        await writer.wait_closed()


class HTTPEventBridge(EventBridge):
    """Bridge events over HTTP(S) endpoints."""

    def __init__(self, send_to: str | None = None, listen_on: str | None = None, *, name: str | None = None):
        if send_to and _parse_endpoint(send_to).scheme == 'unix':
            raise ValueError('HTTPEventBridge send_to must be http:// or https://')
        if listen_on and _parse_endpoint(listen_on).scheme != 'http':
            raise ValueError('HTTPEventBridge listen_on must be http://')
        super().__init__(send_to=send_to, listen_on=listen_on, name=name or f'HTTPEventBridge_{uuid7str()[-8:]}')


class SocketEventBridge(EventBridge):
    """Bridge events over a unix domain socket path."""

    def __init__(self, path: str | None = None, *, name: str | None = None):
        if path is None:
            send_to = None
            listen_on = None
        else:
            normalized = path[7:] if path.startswith('unix://') else path
            if not normalized:
                raise ValueError('SocketEventBridge path must not be empty')
            send_to = f'unix://{normalized}'
            listen_on = f'unix://{normalized}'

        super().__init__(send_to=send_to, listen_on=listen_on, name=name or f'SocketEventBridge_{uuid7str()[-8:]}')


def __getattr__(name: str) -> Any:
    module_name = _LAZY_BRIDGE_MODULES.get(name)
    if module_name is None:
        raise AttributeError(name)
    module = importlib.import_module(module_name, __package__)
    value = getattr(module, name)
    globals()[name] = value
    return value
