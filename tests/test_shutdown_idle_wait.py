# pyright: basic
"""Terminal shutdown must not restart the bus or strand an existing idle wait."""

import asyncio

import pytest

from abxbus import BaseEvent, EventBus


class ShutdownEvent(BaseEvent[None]):
    pass


@pytest.mark.parametrize('clear', [False, True])
async def test_destroy_is_terminal_for_emit_and_new_idle_wait(clear: bool):
    bus = EventBus()
    await bus.emit(ShutdownEvent())
    await bus.destroy(clear=clear)

    with pytest.raises(RuntimeError, match='destroyed'):
        bus.emit(ShutdownEvent())
    with pytest.raises(RuntimeError, match='destroyed'):
        await bus.wait_until_idle()

    assert not bus._is_running
    assert bus._runloop_task is None


@pytest.mark.parametrize('clear', [False, True])
async def test_destroy_releases_existing_idle_wait(clear: bool):
    bus = EventBus()
    started, release = asyncio.Event(), asyncio.Event()

    async def handle(event: ShutdownEvent) -> None:
        started.set()
        await release.wait()

    bus.on(ShutdownEvent, handle)
    bus.emit(ShutdownEvent())
    bus.emit(ShutdownEvent())
    await asyncio.wait_for(started.wait(), timeout=1)
    waiter = asyncio.create_task(bus.wait_until_idle())
    await asyncio.sleep(0)
    assert not waiter.done()

    try:
        await bus.destroy(clear=clear)
        assert await asyncio.wait_for(waiter, timeout=1) is None
        assert not bus._is_running
        assert bus._runloop_task is None
    finally:
        release.set()
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        await bus.destroy()


async def test_cancel_manual_step_releases_queue_getter():
    bus = EventBus()
    await bus.emit(ShutdownEvent())
    await bus.wait_until_idle()
    queue = bus.pending_event_queue
    assert queue is not None
    async with asyncio.timeout(1):
        while len(queue._getters) != 1:  # noqa: ASYNC110 - synchronize with the real queue waiter
            await asyncio.sleep(0)

    step = asyncio.create_task(bus.step(wait_for_timeout=10))
    try:
        async with asyncio.timeout(1):
            while len(queue._getters) != 2:  # noqa: ASYNC110 - synchronize with the real queue waiter
                await asyncio.sleep(0)
        step.cancel()
        with pytest.raises(asyncio.CancelledError):
            await step
        assert len(queue._getters) == 1
        await bus.emit(ShutdownEvent())
        await bus.wait_until_idle(timeout=1)
        assert queue.empty()
    finally:
        step.cancel()
        await asyncio.gather(step, return_exceptions=True)
        await bus.destroy()


async def test_idle_wait_after_new_emit_waits_for_handler():
    bus = EventBus()
    await bus.wait_until_idle()
    started, release = asyncio.Event(), asyncio.Event()

    async def handle(event: ShutdownEvent) -> None:
        started.set()
        await release.wait()

    bus.on(ShutdownEvent, handle)
    event = bus.emit(ShutdownEvent())
    waiter = asyncio.create_task(bus.wait_until_idle())
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        assert not waiter.done()
        release.set()
        await asyncio.wait_for(waiter, timeout=1)
        assert event.event_status == 'completed'
        assert bus.pending_event_queue is not None
        assert bus.pending_event_queue.empty()
    finally:
        release.set()
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        await bus.destroy()


async def test_cancel_manual_step_does_not_lose_newly_queued_event():
    class NumberedEvent(BaseEvent[None]):
        number: int

    bus = EventBus()
    started, release = asyncio.Event(), asyncio.Event()
    handled: list[int] = []

    async def handle(event: NumberedEvent) -> None:
        if event.number == 0:
            started.set()
            await release.wait()
        handled.append(event.number)

    bus.on(NumberedEvent, handle)
    bus.emit(NumberedEvent(number=0))
    await asyncio.wait_for(started.wait(), timeout=1)
    queue = bus.pending_event_queue
    assert queue is not None
    step = asyncio.create_task(bus.step(wait_for_timeout=10))
    try:
        async with asyncio.timeout(1):
            while not queue._getters:  # noqa: ASYNC110 - wait for the real queue consumer
                await asyncio.sleep(0)
        event = bus.emit(NumberedEvent(number=1))
        step.cancel()
        with pytest.raises(asyncio.CancelledError):
            await step
        release.set()
        await bus.wait_until_idle(timeout=1)
        assert handled == [0, 1]
        assert event.event_status == 'completed'
        assert queue.empty()
    finally:
        release.set()
        step.cancel()
        await asyncio.gather(step, return_exceptions=True)
        await bus.destroy()
