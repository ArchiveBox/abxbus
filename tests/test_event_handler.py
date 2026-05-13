import asyncio

from abxbus import BaseEvent, EventBus, EventHandlerConcurrencyMode


class ConcurrencyEvent(BaseEvent[str]):
    pass


async def test_event_handler_concurrency_bus_default_remains_unset_on_dispatch() -> None:
    bus = EventBus(name='ConcurrencyDefaultBus', event_handler_concurrency=EventHandlerConcurrencyMode.PARALLEL)

    async def one_handler(_event: ConcurrencyEvent) -> str:
        return 'ok'

    bus.on(ConcurrencyEvent, one_handler)

    try:
        event = bus.emit(ConcurrencyEvent())
        assert event.event_handler_concurrency is None
        await event
    finally:
        await bus.destroy()


async def test_event_handler_concurrency_per_event_override_controls_execution_mode() -> None:
    bus = EventBus(name='ConcurrencyPerEventBus', event_handler_concurrency=EventHandlerConcurrencyMode.PARALLEL)
    inflight_by_event_id: dict[str, int] = {}
    max_inflight_by_event_id: dict[str, int] = {}
    counter_lock = asyncio.Lock()

    async def track_concurrency(event: ConcurrencyEvent) -> None:
        event_id = event.event_id
        async with counter_lock:
            current_inflight = inflight_by_event_id.get(event_id, 0) + 1
            inflight_by_event_id[event_id] = current_inflight
            max_inflight_by_event_id[event_id] = max(max_inflight_by_event_id.get(event_id, 0), current_inflight)
        await asyncio.sleep(0.02)
        async with counter_lock:
            inflight_by_event_id[event_id] = max(inflight_by_event_id.get(event_id, 1) - 1, 0)

    async def handler_a(event: ConcurrencyEvent) -> str:
        await track_concurrency(event)
        return 'a'

    async def handler_b(event: ConcurrencyEvent) -> str:
        await track_concurrency(event)
        return 'b'

    bus.on(ConcurrencyEvent, handler_a)
    bus.on(ConcurrencyEvent, handler_b)

    try:
        serial_event = bus.emit(ConcurrencyEvent(event_handler_concurrency=EventHandlerConcurrencyMode.SERIAL))
        parallel_event = bus.emit(ConcurrencyEvent(event_handler_concurrency=EventHandlerConcurrencyMode.PARALLEL))
        assert serial_event.event_handler_concurrency == EventHandlerConcurrencyMode.SERIAL
        assert parallel_event.event_handler_concurrency == EventHandlerConcurrencyMode.PARALLEL

        await serial_event
        await parallel_event

        assert max_inflight_by_event_id.get(serial_event.event_id) == 1
        assert max_inflight_by_event_id.get(parallel_event.event_id, 0) >= 2
    finally:
        await bus.destroy()
