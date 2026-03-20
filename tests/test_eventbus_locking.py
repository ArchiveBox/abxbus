import asyncio

import pytest

from abxbus import BaseEvent, EventBus, EventConcurrencyMode
from abxbus.retry import retry


class GlobalSerialEvent(BaseEvent[str]):
    order: int = 0
    source: str = 'a'


class PerBusSerialEvent(BaseEvent[str]):
    order: int = 0
    source: str = 'a'


class ParallelEvent(BaseEvent[str]):
    order: int = 0


class ParallelHandlerEvent(BaseEvent[str]):
    pass


class OverrideParallelEvent(BaseEvent[str]):
    order: int = 0
    event_concurrency: EventConcurrencyMode | None = EventConcurrencyMode.PARALLEL


class OverrideSerialEvent(BaseEvent[str]):
    order: int = 0
    event_concurrency: EventConcurrencyMode | None = EventConcurrencyMode.BUS_SERIAL


class ParentEvent(BaseEvent[str]):
    pass


class ChildEvent(BaseEvent[str]):
    pass


class SiblingEvent(BaseEvent[str]):
    pass


class HandlerLockEvent(BaseEvent[str]):
    order: int = 0
    source: str = 'a'


@pytest.mark.asyncio
async def test_event_concurrency_global_serial_allows_only_one_inflight_across_buses() -> None:
    bus_a = EventBus(name='GlobalSerialA', event_concurrency='global-serial')
    bus_b = EventBus(name='GlobalSerialB', event_concurrency='global-serial')
    in_flight = 0
    max_in_flight = 0
    starts: list[str] = []

    async def handler(event: GlobalSerialEvent) -> None:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        starts.append(f'{event.source}:{event.order}')
        await asyncio.sleep(0.01)
        in_flight -= 1

    bus_a.on(GlobalSerialEvent, handler)
    bus_b.on(GlobalSerialEvent, handler)

    try:
        for i in range(3):
            bus_a.emit(GlobalSerialEvent(order=i, source='a'))
            bus_b.emit(GlobalSerialEvent(order=i, source='b'))

        await asyncio.gather(bus_a.wait_until_idle(), bus_b.wait_until_idle())

        starts_a = [int(value.split(':')[1]) for value in starts if value.startswith('a:')]
        starts_b = [int(value.split(':')[1]) for value in starts if value.startswith('b:')]
        assert max_in_flight == 1
        assert starts_a == [0, 1, 2]
        assert starts_b == [0, 1, 2]
    finally:
        await bus_a.stop(clear=True, timeout=0)
        await bus_b.stop(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_event_concurrency_bus_serial_serializes_per_bus_but_overlaps_across_buses() -> None:
    bus_a = EventBus(name='BusSerialA', event_concurrency='bus-serial')
    bus_b = EventBus(name='BusSerialB', event_concurrency='bus-serial')
    b_started = asyncio.Event()

    in_flight_global = 0
    max_in_flight_global = 0
    in_flight_a = 0
    in_flight_b = 0
    max_in_flight_a = 0
    max_in_flight_b = 0

    async def on_a(_event: PerBusSerialEvent) -> None:
        nonlocal in_flight_global, max_in_flight_global, in_flight_a, max_in_flight_a
        in_flight_global += 1
        in_flight_a += 1
        max_in_flight_global = max(max_in_flight_global, in_flight_global)
        max_in_flight_a = max(max_in_flight_a, in_flight_a)
        await b_started.wait()
        await asyncio.sleep(0.01)
        in_flight_global -= 1
        in_flight_a -= 1

    async def on_b(_event: PerBusSerialEvent) -> None:
        nonlocal in_flight_global, max_in_flight_global, in_flight_b, max_in_flight_b
        in_flight_global += 1
        in_flight_b += 1
        max_in_flight_global = max(max_in_flight_global, in_flight_global)
        max_in_flight_b = max(max_in_flight_b, in_flight_b)
        b_started.set()
        await asyncio.sleep(0.01)
        in_flight_global -= 1
        in_flight_b -= 1

    bus_a.on(PerBusSerialEvent, on_a)
    bus_b.on(PerBusSerialEvent, on_b)

    try:
        bus_a.emit(PerBusSerialEvent(order=0, source='a'))
        bus_b.emit(PerBusSerialEvent(order=0, source='b'))
        await asyncio.gather(bus_a.wait_until_idle(), bus_b.wait_until_idle())

        assert max_in_flight_a == 1
        assert max_in_flight_b == 1
        assert max_in_flight_global >= 2
    finally:
        await bus_a.stop(clear=True, timeout=0)
        await bus_b.stop(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_event_concurrency_parallel_allows_same_bus_events_to_overlap() -> None:
    bus = EventBus(name='ParallelEventBus', event_concurrency='parallel', event_handler_concurrency='parallel')
    release = asyncio.Event()
    overlap_seen = asyncio.Event()

    in_flight = 0
    max_in_flight = 0

    async def handler(_event: ParallelEvent) -> None:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        if in_flight >= 2:
            overlap_seen.set()
        await release.wait()
        await asyncio.sleep(0.005)
        in_flight -= 1

    bus.on(ParallelEvent, handler)

    try:
        first = bus.emit(ParallelEvent(order=0))
        second = bus.emit(ParallelEvent(order=1))
        await asyncio.wait_for(overlap_seen.wait(), timeout=1.0)
        release.set()
        await asyncio.gather(first, second)
        await bus.wait_until_idle()

        assert max_in_flight >= 2
    finally:
        await bus.stop(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_event_handler_concurrency_parallel_runs_handlers_for_same_event_concurrently() -> None:
    bus = EventBus(name='ParallelHandlerBus', event_concurrency='bus-serial', event_handler_concurrency='parallel')
    release = asyncio.Event()
    overlap_seen = asyncio.Event()
    in_flight = 0
    max_in_flight = 0

    async def handler_a(_event: ParallelHandlerEvent) -> None:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        if in_flight >= 2:
            overlap_seen.set()
        await release.wait()
        in_flight -= 1

    async def handler_b(_event: ParallelHandlerEvent) -> None:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        if in_flight >= 2:
            overlap_seen.set()
        await release.wait()
        in_flight -= 1

    bus.on(ParallelHandlerEvent, handler_a)
    bus.on(ParallelHandlerEvent, handler_b)

    try:
        event = bus.emit(ParallelHandlerEvent())
        await asyncio.wait_for(overlap_seen.wait(), timeout=1.0)
        release.set()
        await event
        await bus.wait_until_idle()

        assert max_in_flight >= 2
    finally:
        await bus.stop(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_event_concurrency_override_parallel_beats_bus_serial_default() -> None:
    bus = EventBus(name='OverrideParallelBus', event_concurrency='bus-serial', event_handler_concurrency='parallel')
    release = asyncio.Event()
    overlap_seen = asyncio.Event()
    in_flight = 0
    max_in_flight = 0

    async def handler(_event: OverrideParallelEvent) -> None:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        if in_flight >= 2:
            overlap_seen.set()
        await release.wait()
        in_flight -= 1

    bus.on(OverrideParallelEvent, handler)

    try:
        first = bus.emit(OverrideParallelEvent(order=0, event_concurrency=EventConcurrencyMode.PARALLEL))
        second = bus.emit(OverrideParallelEvent(order=1, event_concurrency=EventConcurrencyMode.PARALLEL))
        await asyncio.wait_for(overlap_seen.wait(), timeout=1.0)
        release.set()
        await asyncio.gather(first, second)
        await bus.wait_until_idle()

        assert max_in_flight >= 2
    finally:
        await bus.stop(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_event_concurrency_override_bus_serial_beats_bus_parallel_default() -> None:
    bus = EventBus(name='OverrideBusSerialBus', event_concurrency='parallel', event_handler_concurrency='parallel')
    release = asyncio.Event()
    in_flight = 0
    max_in_flight = 0

    async def handler(_event: OverrideSerialEvent) -> None:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await release.wait()
        in_flight -= 1

    bus.on(OverrideSerialEvent, handler)

    try:
        first = bus.emit(OverrideSerialEvent(order=0, event_concurrency=EventConcurrencyMode.BUS_SERIAL))
        second = bus.emit(OverrideSerialEvent(order=1, event_concurrency=EventConcurrencyMode.BUS_SERIAL))
        await asyncio.sleep(0.02)
        assert max_in_flight == 1

        release.set()
        await asyncio.gather(first, second)
        await bus.wait_until_idle()
    finally:
        await bus.stop(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_queue_jump_awaited_child_preempts_queued_sibling_on_same_bus() -> None:
    bus = EventBus(name='QueueJumpBus', event_concurrency='bus-serial', event_handler_concurrency='serial')
    order: list[str] = []

    async def on_parent(event: ParentEvent) -> None:
        order.append('parent_start')
        child = event.event_bus.emit(ChildEvent())
        await child
        order.append('parent_end')

    async def on_child(_event: ChildEvent) -> None:
        order.append('child_start')
        await asyncio.sleep(0.005)
        order.append('child_end')

    async def on_sibling(_event: SiblingEvent) -> None:
        order.append('sibling')

    bus.on(ParentEvent, on_parent)
    bus.on(ChildEvent, on_child)
    bus.on(SiblingEvent, on_sibling)

    try:
        parent = bus.emit(ParentEvent())
        sibling = bus.emit(SiblingEvent())
        await asyncio.gather(parent, sibling)
        await bus.wait_until_idle()

        assert order == ['parent_start', 'child_start', 'child_end', 'parent_end', 'sibling']
    finally:
        await bus.stop(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_retry_global_handler_lock_serializes_handlers_across_buses() -> None:
    bus_a = EventBus(name='GlobalHandlerA', event_concurrency='parallel', event_handler_concurrency='serial')
    bus_b = EventBus(name='GlobalHandlerB', event_concurrency='parallel', event_handler_concurrency='serial')

    in_flight = 0
    max_in_flight = 0

    @retry(semaphore_scope='global', semaphore_name='eventbus_locking_global_handler', semaphore_limit=1)
    async def locked_handler(_event: HandlerLockEvent) -> None:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.005)
        in_flight -= 1

    bus_a.on(HandlerLockEvent, locked_handler)
    bus_b.on(HandlerLockEvent, locked_handler)

    try:
        for i in range(4):
            bus_a.emit(HandlerLockEvent(order=i, source='a'))
            bus_b.emit(HandlerLockEvent(order=i, source='b'))

        await asyncio.gather(bus_a.wait_until_idle(), bus_b.wait_until_idle())
        assert max_in_flight == 1
    finally:
        await bus_a.stop(clear=True, timeout=0)
        await bus_b.stop(clear=True, timeout=0)
