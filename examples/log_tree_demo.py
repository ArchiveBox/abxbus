#!/usr/bin/env -S uv run python
"""Run: uv run python examples/log_tree_demo.py"""

import asyncio
from typing import Any

from abxbus import BaseEvent, EventBus


class RootEvent(BaseEvent[str]):
    url: str


class ChildEvent(BaseEvent[str]):
    tab_id: str


class GrandchildEvent(BaseEvent[str]):
    status: str


async def delay_ms(ms: int) -> None:
    await asyncio.sleep(ms / 1000.0)


async def main() -> None:
    bus_a = EventBus('BusA')
    bus_b = EventBus('BusB')

    try:

        async def forward_to_bus_b(event: BaseEvent[Any]) -> str:
            await delay_ms(20)
            bus_b.emit(event)
            return 'forwarded_to_bus_b'

        bus_a.on('*', forward_to_bus_b)

        async def root_fast_handler(event: RootEvent) -> str:
            await delay_ms(10)
            child = event.emit(ChildEvent(tab_id='tab-123', event_timeout=0.1))
            await child.now()
            return 'root_fast_handler_ok'

        async def root_slow_handler(event: RootEvent) -> str:
            event.emit(ChildEvent(tab_id='tab-timeout', event_timeout=0.1))
            await delay_ms(400)
            return 'root_slow_handler_timeout'

        bus_a.on(RootEvent, root_fast_handler)
        bus_a.on(RootEvent, root_slow_handler)

        async def child_slow_handler(_event: ChildEvent) -> str:
            await delay_ms(150)
            return 'child_slow_handler_done'

        async def child_fast_handler(event: ChildEvent) -> str:
            await delay_ms(10)
            grandchild = event.emit(GrandchildEvent(status='ok', event_timeout=0.05))
            await grandchild.now()
            return 'child_handler_ok'

        async def grandchild_fast_handler(_event: GrandchildEvent) -> str:
            await delay_ms(5)
            return 'grandchild_fast_handler_ok'

        async def grandchild_slow_handler(_event: GrandchildEvent) -> str:
            await delay_ms(60)
            return 'grandchild_slow_handler_timeout'

        bus_b.on(ChildEvent, child_slow_handler)
        bus_b.on(ChildEvent, child_fast_handler)
        bus_b.on(GrandchildEvent, grandchild_fast_handler)
        bus_b.on(GrandchildEvent, grandchild_slow_handler)

        root_event = bus_a.emit(RootEvent(url='https://example.com', event_timeout=0.25))
        await root_event.now()

        print('\n=== BusA log_tree ===')
        print(bus_a.log_tree())

        print('\n=== BusB log_tree ===')
        print(bus_b.log_tree())
    finally:
        await bus_a.stop(clear=True, timeout=0)
        await bus_b.stop(clear=True, timeout=0)


if __name__ == '__main__':
    asyncio.run(main())
