"""Process-isolated roundtrip tests for bridge transports."""

from __future__ import annotations

import asyncio
import json
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from shutil import rmtree
from typing import Any

import pytest
from uuid_extensions import uuid7str

from abxbus import BaseEvent, HTTPEventBridge, SocketEventBridge
from abxbus.bridge_jsonl import JSONLEventBridge
from abxbus.bridge_nats import NATSEventBridge
from abxbus.bridge_postgres import PostgresEventBridge
from abxbus.bridge_redis import RedisEventBridge
from abxbus.bridge_sqlite import SQLiteEventBridge
from abxbus.bridge_tachyon import TachyonEventBridge


class IPCPingEvent(BaseEvent):
    label: str


_TEST_RUN_ID = f'{int(time.time() * 1000)}-{uuid7str()[-8:]}'


def _make_temp_dir(prefix: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=f'{prefix}-{_TEST_RUN_ID}-'))


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('127.0.0.1', 0))
        return int(sock.getsockname()[1])


def _canonical(payload: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in payload.items():
        if key.endswith('_at') and isinstance(value, str):
            try:
                normalized[key] = datetime.fromisoformat(value).timestamp()
                continue
            except ValueError:
                pass
        normalized[key] = value
    return normalized


def _normalize_roundtrip_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _canonical(payload)
    normalized.pop('event_id', None)
    normalized.pop('event_path', None)
    # The listener snapshots the event from inside its local handler, where the
    # receiving bus has already attached handler bookkeeping that was not part
    # of the bridge payload.
    normalized.pop('event_results', None)
    # Dispatch now materializes event_concurrency defaults on the receiving bus.
    if normalized.get('event_concurrency') is None:
        normalized['event_concurrency'] = 'bus-serial'
    # Dispatch also materializes handler-level defaults on the receiving bus.
    if normalized.get('event_handler_concurrency') is None:
        normalized['event_handler_concurrency'] = 'serial'
    if normalized.get('event_handler_completion') is None:
        normalized['event_handler_completion'] = 'all'
    # event_status/event_started_at are now serialized, but the receiving bus
    # can advance them while handling the event. Normalize in-flight statuses.
    if normalized.get('event_status') in ('pending', 'started'):
        normalized['event_status'] = 'pending'
        normalized['event_started_at'] = None
        normalized['event_completed_at'] = None
    return normalized


@asynccontextmanager
async def _running_process(command: list[str], *, cwd: Path | None = None) -> AsyncIterator[subprocess.Popen[str]]:
    process = subprocess.Popen(
        command,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        yield process
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


async def _wait_for_port(port: int, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            _, writer = await asyncio.open_connection('127.0.0.1', port)
            writer.close()
            await writer.wait_closed()
            return
        except OSError:
            await asyncio.sleep(0.05)
    raise TimeoutError(f'port did not open in time: {port}')


async def _wait_for_path(path: Path, *, process: subprocess.Popen[str], timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise AssertionError(f'worker exited early ({process.returncode})\nstdout:\n{stdout}\nstderr:\n{stderr}')
        await asyncio.sleep(0.05)
    raise TimeoutError(f'path did not appear in time: {path}')


def _make_sender_bridge(kind: str, config: dict[str, Any], *, low_latency: bool = False) -> Any:
    if kind == 'http':
        return HTTPEventBridge(send_to=str(config['endpoint']))
    if kind == 'socket':
        return SocketEventBridge(path=str(config['path']))
    if kind == 'jsonl':
        return JSONLEventBridge(str(config['path']), poll_interval=0.001 if low_latency else 0.05)
    if kind == 'sqlite':
        return SQLiteEventBridge(
            str(config['path']),
            str(config['table']),
            poll_interval=0.001 if low_latency else 0.05,
        )
    if kind == 'redis':
        return RedisEventBridge(str(config['url']))
    if kind == 'nats':
        return NATSEventBridge(str(config['server']), str(config['subject']))
    if kind == 'postgres':
        return PostgresEventBridge(str(config['url']))
    if kind == 'tachyon':
        return TachyonEventBridge(str(config['path']))
    raise ValueError(f'Unsupported bridge kind: {kind}')


def _make_listener_bridge(kind: str, config: dict[str, Any], *, low_latency: bool = False) -> Any:
    if kind == 'http':
        return HTTPEventBridge(listen_on=str(config['endpoint']))
    if kind == 'socket':
        return SocketEventBridge(path=str(config['path']))
    if kind == 'jsonl':
        return JSONLEventBridge(str(config['path']), poll_interval=0.001 if low_latency else 0.05)
    if kind == 'sqlite':
        return SQLiteEventBridge(
            str(config['path']),
            str(config['table']),
            poll_interval=0.001 if low_latency else 0.05,
        )
    if kind == 'redis':
        return RedisEventBridge(str(config['url']))
    if kind == 'nats':
        return NATSEventBridge(str(config['server']), str(config['subject']))
    if kind == 'postgres':
        return PostgresEventBridge(str(config['url']))
    if kind == 'tachyon':
        return TachyonEventBridge(str(config['path']))
    raise ValueError(f'Unsupported bridge kind: {kind}')


async def _measure_warm_latency_ms(kind: str, config: dict[str, Any]) -> float:
    attempts = 3
    last_error: BaseException | None = None

    for _attempt in range(attempts):
        sender = _make_sender_bridge(kind, config, low_latency=True)
        receiver = _make_listener_bridge(kind, config, low_latency=True)

        run_suffix = uuid7str()[-8:]
        warmup_prefix = f'warmup_{run_suffix}_'
        measured_prefix = f'measured_{run_suffix}_'
        warmup_count_target = 5
        measured_count_target = 1000

        warmup_seen_count = 0
        measured_seen_count = 0
        warmup_seen = asyncio.Event()
        measured_seen = asyncio.Event()

        async def _on_event(event: BaseEvent[Any]) -> None:
            nonlocal warmup_seen_count, measured_seen_count
            label = getattr(event, 'label', '')
            if not isinstance(label, str):
                return
            if label.startswith(warmup_prefix):
                warmup_seen_count += 1
                if warmup_seen_count >= warmup_count_target:
                    warmup_seen.set()
                return
            if label.startswith(measured_prefix):
                measured_seen_count += 1
                if measured_seen_count >= measured_count_target:
                    measured_seen.set()

        try:
            await sender.start()
            await receiver.start()
            receiver.on('IPCPingEvent', _on_event)
            await asyncio.sleep(0.1)

            for index in range(warmup_count_target):
                await sender.emit(
                    IPCPingEvent(
                        label=f'{warmup_prefix}{index}',
                    )
                )
            await asyncio.wait_for(warmup_seen.wait(), timeout=60.0)

            start_ns = time.perf_counter_ns()
            for index in range(measured_count_target):
                await sender.emit(
                    IPCPingEvent(
                        label=f'{measured_prefix}{index}',
                    )
                )
            await asyncio.wait_for(measured_seen.wait(), timeout=600.0)
            elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            return elapsed_ms / measured_count_target
        except TimeoutError as exc:
            last_error = exc
        finally:
            await sender.close()
            await receiver.close()

        await asyncio.sleep(0.2)

    raise RuntimeError(f'bridge latency measurement timed out after {attempts} attempts: {kind}') from last_error


async def _assert_roundtrip(kind: str, config: dict[str, Any]) -> None:
    temp_path = _make_temp_dir(f'abxbus-bridge-{kind}')
    try:
        worker_config_path = temp_path / 'worker_config.json'
        worker_ready_path = temp_path / 'worker_ready'
        received_event_path = temp_path / 'received_event.json'
        worker_config = {
            **config,
            'kind': kind,
            'ready_path': str(worker_ready_path),
            'output_path': str(received_event_path),
        }
        worker_config_path.write_text(json.dumps(worker_config), encoding='utf-8')

        sender = _make_sender_bridge(kind, config)

        worker = subprocess.Popen(
            [sys.executable, str(Path(__file__).with_name('bridge_listener_worker.py')), str(worker_config_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            await _wait_for_path(worker_ready_path, process=worker)
            if kind == 'postgres':
                await sender.start()
            outbound = IPCPingEvent(
                label=f'{kind}_ok',
                event_result_type={
                    '$schema': 'https://json-schema.org/draft/2020-12/schema',
                    'type': 'object',
                    'properties': {
                        'ok': {'type': 'boolean'},
                        'score': {'type': 'number'},
                        'tags': {'type': 'array', 'items': {'type': 'string'}},
                    },
                    'required': ['ok', 'score', 'tags'],
                    'additionalProperties': False,
                },
            )
            await sender.emit(outbound)
            await _wait_for_path(received_event_path, process=worker)
            received_payload = json.loads(received_event_path.read_text(encoding='utf-8'))
            assert 'event_status' in received_payload
            assert 'event_started_at' in received_payload
            assert _normalize_roundtrip_payload(received_payload) == _normalize_roundtrip_payload(
                outbound.model_dump(mode='json')
            )
        finally:
            await sender.close()
            if worker.poll() is None:
                worker.terminate()
                try:
                    worker.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    worker.kill()
                    worker.wait(timeout=5)
    finally:
        rmtree(temp_path, ignore_errors=True)


@pytest.mark.asyncio
async def test_http_event_bridge_roundtrip_between_processes() -> None:
    endpoint = f'http://127.0.0.1:{_free_tcp_port()}/events'
    await _assert_roundtrip('http', {'endpoint': endpoint})
    latency_ms = await _measure_warm_latency_ms('http', {'endpoint': endpoint})
    print(f'LATENCY python http {latency_ms:.3f}ms')


@pytest.mark.asyncio
async def test_socket_event_bridge_roundtrip_between_processes() -> None:
    socket_path = Path('/tmp') / f'bb-{_TEST_RUN_ID}-{uuid7str()[-8:]}.sock'
    await _assert_roundtrip('socket', {'path': str(socket_path)})
    latency_ms = await _measure_warm_latency_ms('socket', {'path': str(socket_path)})
    print(f'LATENCY python socket {latency_ms:.3f}ms')


def test_socket_event_bridge_rejects_long_socket_paths() -> None:
    long_path = '/tmp/' + ('a' * 100) + '.sock'
    with pytest.raises(ValueError, match='too long'):
        SocketEventBridge(path=long_path)


@pytest.mark.asyncio
async def test_jsonl_event_bridge_roundtrip_between_processes() -> None:
    temp_dir = _make_temp_dir('abxbus-jsonl')
    try:
        jsonl_path = temp_dir / 'events.jsonl'
        await _assert_roundtrip('jsonl', {'path': str(jsonl_path)})
        latency_ms = await _measure_warm_latency_ms('jsonl', {'path': str(jsonl_path)})
        print(f'LATENCY python jsonl {latency_ms:.3f}ms')
    finally:
        rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_sqlite_event_bridge_roundtrip_between_processes() -> None:
    temp_dir = _make_temp_dir('abxbus-sqlite')
    try:
        sqlite_path = temp_dir / 'events.sqlite3'
        await _assert_roundtrip('sqlite', {'path': str(sqlite_path), 'table': 'abxbus_events'})

        with sqlite3.connect(sqlite_path) as conn:
            columns = {str(row[1]) for row in conn.execute('PRAGMA table_info("abxbus_events")').fetchall()}
            assert 'event_payload' in columns
            assert 'label' not in columns
            assert all(column == 'event_payload' or column.startswith('event_') for column in columns)

            row = conn.execute(
                'SELECT event_payload FROM "abxbus_events" ORDER BY COALESCE("event_created_at", \'\') DESC LIMIT 1'
            ).fetchone()
            assert row is not None
            payload = json.loads(str(row[0]))
            assert payload.get('label') == 'sqlite_ok'

        measure_sqlite_path = temp_dir / 'events.measure.sqlite3'
        latency_ms = await _measure_warm_latency_ms('sqlite', {'path': str(measure_sqlite_path), 'table': 'abxbus_events'})
        print(f'LATENCY python sqlite {latency_ms:.3f}ms')
    finally:
        rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_redis_event_bridge_roundtrip_between_processes() -> None:
    temp_dir = _make_temp_dir('abxbus-redis')
    try:
        port = _free_tcp_port()
        command = [
            'redis-server',
            '--save',
            '',
            '--appendonly',
            'no',
            '--bind',
            '127.0.0.1',
            '--port',
            str(port),
            '--dir',
            str(temp_dir),
        ]
        async with _running_process(command) as redis_process:
            await _wait_for_port(port)
            await _assert_roundtrip('redis', {'url': f'redis://127.0.0.1:{port}/1/abxbus_events'})
            latency_ms = await _measure_warm_latency_ms('redis', {'url': f'redis://127.0.0.1:{port}/1/abxbus_events'})
            print(f'LATENCY python redis {latency_ms:.3f}ms')
            assert redis_process.poll() is None
    finally:
        rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_nats_event_bridge_roundtrip_between_processes() -> None:
    port = _free_tcp_port()
    command = ['nats-server', '-a', '127.0.0.1', '-p', str(port)]
    async with _running_process(command) as nats_process:
        await _wait_for_port(port)
        await _assert_roundtrip('nats', {'server': f'nats://127.0.0.1:{port}', 'subject': 'abxbus_events'})
        latency_ms = await _measure_warm_latency_ms('nats', {'server': f'nats://127.0.0.1:{port}', 'subject': 'abxbus_events'})
        print(f'LATENCY python nats {latency_ms:.3f}ms')
        assert nats_process.poll() is None


@pytest.mark.asyncio
async def test_tachyon_event_bridge_roundtrip_between_processes() -> None:
    socket_path = Path('/tmp') / f'bb-tachyon-{_TEST_RUN_ID}-{uuid7str()[-8:]}.sock'
    try:
        await _assert_roundtrip('tachyon', {'path': str(socket_path)})
        latency_ms = await _measure_warm_latency_ms('tachyon', {'path': str(socket_path)})
        print(f'LATENCY python tachyon {latency_ms:.3f}ms')
    finally:
        if socket_path.exists():
            try:
                socket_path.unlink()
            except OSError:
                pass


@pytest.mark.asyncio
async def test_postgres_event_bridge_roundtrip_between_processes() -> None:
    temp_dir = _make_temp_dir('abxbus-postgres')
    try:
        data_dir = temp_dir / 'pgdata'
        initdb = subprocess.run(
            ['initdb', '-D', str(data_dir), '-A', 'trust', '-U', 'postgres'],
            capture_output=True,
            text=True,
            check=False,
        )
        assert initdb.returncode == 0, f'initdb failed\nstdout:\n{initdb.stdout}\nstderr:\n{initdb.stderr}'

        port = _free_tcp_port()
        command = ['postgres', '-D', str(data_dir), '-h', '127.0.0.1', '-p', str(port), '-k', '/tmp']
        async with _running_process(command) as postgres_process:
            await _wait_for_port(port)
            await _assert_roundtrip('postgres', {'url': f'postgresql://postgres@127.0.0.1:{port}/postgres/abxbus_events'})

            asyncpg = __import__('asyncpg')
            conn = await asyncpg.connect(f'postgresql://postgres@127.0.0.1:{port}/postgres')
            try:
                rows = await conn.fetch(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = $1
                    """,
                    'abxbus_events',
                )
                columns = {str(row['column_name']) for row in rows}
                assert 'event_payload' in columns
                assert 'label' not in columns
                assert all(column == 'event_payload' or column.startswith('event_') for column in columns)

                row = await conn.fetchrow(
                    'SELECT event_payload FROM "abxbus_events" ORDER BY COALESCE("event_created_at", \'\') DESC LIMIT 1'
                )
                assert row is not None
                payload = json.loads(str(row['event_payload']))
                assert payload.get('label') == 'postgres_ok'
            finally:
                await conn.close()

            latency_ms = await _measure_warm_latency_ms(
                'postgres', {'url': f'postgresql://postgres@127.0.0.1:{port}/postgres/abxbus_events'}
            )
            print(f'LATENCY python postgres {latency_ms:.3f}ms')
            assert postgres_process.poll() is None
    finally:
        rmtree(temp_dir, ignore_errors=True)
