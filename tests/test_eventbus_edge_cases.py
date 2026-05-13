import asyncio
import time

import pytest

from abxbus import BaseEvent, EventBus, EventStatus


class ResetCoverageEvent(BaseEvent[None]):
    label: str


class IdleTimeoutCoverageEvent(BaseEvent[None]):
    label: str = 'slow'


class DestroyCoverageEvent(BaseEvent[None]):
    label: str = 'destroy'


@pytest.mark.asyncio
async def test_event_reset_creates_fresh_pending_event_for_cross_bus_dispatch():
    bus_a = EventBus(name='ResetCoverageBusA')
    bus_b = EventBus(name='ResetCoverageBusB')
    seen_a: list[str] = []
    seen_b: list[str] = []

    bus_a.on(ResetCoverageEvent, lambda event: seen_a.append(event.label))
    bus_b.on(ResetCoverageEvent, lambda event: seen_b.append(event.label))

    completed = await bus_a.emit(ResetCoverageEvent(label='hello'))
    assert completed.event_status == EventStatus.COMPLETED
    assert len(completed.event_results) == 1

    fresh = completed.event_reset()
    assert fresh.event_id != completed.event_id
    assert fresh.event_status == EventStatus.PENDING
    assert fresh.event_completed_at is None
    assert fresh.event_results == {}

    forwarded = await bus_b.emit(fresh)
    assert forwarded.event_status == EventStatus.COMPLETED
    assert seen_a == ['hello']
    assert seen_b == ['hello']
    assert any(path.startswith('ResetCoverageBusA#') for path in forwarded.event_path)
    assert any(path.startswith('ResetCoverageBusB#') for path in forwarded.event_path)

    await bus_a.destroy(timeout=0, clear=True)
    await bus_b.destroy(timeout=0, clear=True)


@pytest.mark.asyncio
async def test_wait_until_idle_timeout_path_recovers_after_inflight_handler_finishes():
    bus = EventBus(name='IdleTimeoutCoverageBus')
    handler_started = asyncio.Event()
    release_handler = asyncio.Event()

    async def slow_handler(event: IdleTimeoutCoverageEvent) -> None:
        handler_started.set()
        await release_handler.wait()

    bus.on(IdleTimeoutCoverageEvent, slow_handler)
    pending = bus.emit(IdleTimeoutCoverageEvent())
    await handler_started.wait()

    start = time.perf_counter()
    await bus.wait_until_idle(timeout=0.01)
    elapsed = time.perf_counter() - start
    assert elapsed < 0.5
    assert pending.event_status != EventStatus.COMPLETED

    release_handler.set()
    await pending
    await bus.wait_until_idle(timeout=1.0)
    assert pending.event_status == EventStatus.COMPLETED

    await bus.destroy(timeout=0, clear=True)


@pytest.mark.asyncio
async def test_destroy_timeout_zero_clears_running_bus_and_releases_name():
    bus_name = 'DestroyCoverageBus'
    bus = EventBus(name=bus_name)

    async def slow_handler(event: DestroyCoverageEvent) -> None:
        await asyncio.sleep(0.2)

    bus.on(DestroyCoverageEvent, slow_handler)
    _pending = bus.emit(DestroyCoverageEvent())
    await asyncio.sleep(0)

    start = time.perf_counter()
    await bus.destroy(timeout=0, clear=True)
    elapsed = time.perf_counter() - start

    assert elapsed < 0.5
    assert bus.name.startswith('_destroyed_')
    assert all(instance is not bus for instance in list(EventBus.all_instances))

    replacement = EventBus(name=bus_name)
    replacement.on(DestroyCoverageEvent, lambda event: None)
    await replacement.emit(DestroyCoverageEvent())
    await replacement.destroy(timeout=0, clear=True)
