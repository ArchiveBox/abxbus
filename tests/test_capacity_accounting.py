"""Pending events occupy one history slot, not separate queue and processing slots."""

import asyncio

import pytest

from abxbus import BaseEvent, EventBus


class CapacityEvent(BaseEvent[None]):
    pass


@pytest.mark.parametrize('limit', [50, 100, 150])
async def test_capacity_does_not_double_count_queued_events(limit: int):
    bus = EventBus(max_history_size=limit)
    release = asyncio.Event()

    async def blocked_handler(event: CapacityEvent) -> None:
        await release.wait()

    bus.on(CapacityEvent, blocked_handler)
    try:
        events = [bus.emit(CapacityEvent()) for _ in range(limit)]
        assert len(bus.event_history) == limit
        rejected = CapacityEvent()
        with pytest.raises(RuntimeError, match=rf'history limit reached \({limit}/{limit}\)'):
            bus.emit(rejected)
        assert rejected.event_id not in bus.event_history
        assert len(bus.event_history) == limit
        release.set()
        await bus.wait_until_idle(timeout=5)
        assert all(event.event_status == 'completed' for event in events)
    finally:
        release.set()
        await bus.destroy()
