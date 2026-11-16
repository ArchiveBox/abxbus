"""Redis bridge forwarding must preserve routing and never echo inbound events."""

import asyncio
import shutil
import socket
import subprocess
from pathlib import Path
from typing import Any

import pytest

from abxbus import BaseEvent, EventBus
from abxbus.bridge_redis import RedisEventBridge


class FrontendEvent(BaseEvent[Any]):
    message: str


@pytest.mark.asyncio
async def test_redis_bridge_bidirectional_routing(tmp_path: Path) -> None:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    process = subprocess.Popen(
        [
            shutil.which('redis-server') or 'redis-server',
            '--port',
            str(port),
            '--bind',
            '127.0.0.1',
            '--save',
            '',
            '--appendonly',
            'no',
            '--dir',
            str(tmp_path),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    assert process.stdout is not None
    while 'Ready to accept connections' not in await asyncio.to_thread(process.stdout.readline):
        assert process.poll() is None
    bridges = [RedisEventBridge(f'redis://127.0.0.1:{port}/0/routing', name='SameBridgeName') for _ in range(2)]
    buses = [EventBus(), EventBus()]
    seen: list[list[BaseEvent[Any]]] = [[], []]
    received = asyncio.Event()

    async def handle_a(event: BaseEvent[Any]) -> None:
        seen[0].append(event)

    async def handle_b(event: BaseEvent[Any]) -> None:
        seen[1].append(event)
        received.set()

    try:
        for bridge, bus, handler in zip(bridges, buses, (handle_a, handle_b), strict=True):
            await bridge.start()
            bridge.on('*', bus.emit)
            bus.on('*', bridge.emit)
            bus.on('*', handler)
        for sender in range(2):
            received.clear()
            event = FrontendEvent(message='Hello from frontend')
            await asyncio.wait_for(buses[sender].emit(event).wait(), 2)
            await asyncio.wait_for(received.wait(), 2)
            # A quiet observation window catches self-delivery and cascading pub/sub echoes.
            await asyncio.sleep(0.1)
            assert [len(events) for events in seen] == [sender + 1, sender + 1]
            forwarded = seen[1 - sender][sender]
            assert forwarded.event_id != event.event_id
            assert getattr(forwarded, 'message') == 'Hello from frontend'
            assert all(not entry.startswith('SameBridgeName') for entry in event.event_path)
            assert len([entry for entry in forwarded.event_path if entry.startswith('SameBridgeName')]) == 2
        for bus in buses:
            await asyncio.wait_for(bus.wait_until_idle(), 1)
        assert all(result.error is None for events in seen for item in events for result in item.event_results.values())
    finally:
        for bridge in bridges:
            await bridge.close()
        for bus in buses:
            await bus.destroy()
        process.terminate()
        process.wait(timeout=5)
