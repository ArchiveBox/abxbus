# pyright: basic
"""Active awaits must never execute a foreign event loop's queued handlers."""

import asyncio
import threading

import pytest

from abxbus import BaseEvent, EventBus


class IsolationEvent(BaseEvent[None]):
    pass


async def test_now_does_not_process_other_loops_buses():
    loop_b = asyncio.new_event_loop()
    thread = threading.Thread(target=loop_b.run_forever, daemon=True)
    thread.start()
    release = asyncio.Event()
    started = threading.Event()
    ran_on = []

    async def setup():
        bus = EventBus(name='ForeignLoop')

        async def blocker(event: IsolationEvent):
            started.set()
            await release.wait()

        async def probe(event: IsolationEvent):
            ran_on.append(asyncio.get_running_loop())

        bus.on('Blocker', blocker)
        bus.on('Probe', probe)
        bus.emit(IsolationEvent(event_type='Blocker'))
        event = bus.emit(IsolationEvent(event_type='Probe'))
        return bus, event

    bus = None
    try:
        bus, event = await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(setup(), loop_b))
        assert await asyncio.to_thread(started.wait, 2)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(event.now(), 0.05)
        assert not ran_on, 'now() ran a foreign-loop queued handler'
        loop_b.call_soon_threadsafe(release.set)

        async def finish():
            await bus.wait_until_idle(timeout=2)

        await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(finish(), loop_b))
        assert ran_on == [loop_b]
    finally:
        loop_b.call_soon_threadsafe(release.set)
        if bus is not None:
            await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(bus.destroy(), loop_b))
        loop_b.call_soon_threadsafe(loop_b.stop)
        await asyncio.to_thread(thread.join, 2)
        loop_b.close()


async def test_now_does_not_drain_unrelated_bus():
    bus = EventBus(name='AwaitingBus')
    other = EventBus(name='UnrelatedBus')
    started, release = asyncio.Event(), asyncio.Event()
    seen = []
    own_seen = []

    async def own(event: IsolationEvent):
        own_seen.append('own')

    async def blocker(event: IsolationEvent):
        started.set()
        await release.wait()

    async def probe(event: IsolationEvent):
        seen.append('probe')

    other.on('Blocker', blocker)
    other.on('Probe', probe)
    bus.on(IsolationEvent, own)
    try:
        other.emit(IsolationEvent(event_type='Blocker'))
        queued = other.emit(IsolationEvent(event_type='Probe'))
        await asyncio.wait_for(started.wait(), 2)
        await asyncio.wait_for(bus.emit(IsolationEvent()).now(), 2)
        assert own_seen == ['own']
        assert not seen
        release.set()
        await queued.wait(timeout=2)
        assert seen == ['probe']
    finally:
        release.set()
        await bus.destroy()
        await other.destroy()
