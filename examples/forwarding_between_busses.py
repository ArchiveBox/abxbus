#!/usr/bin/env -S uv run python
"""Run: uv run python examples/forwarding_between_busses.py"""

import asyncio

from abxbus import BaseEvent, EventBus


class ForwardedEvent(BaseEvent[None]):
    message: str


async def main() -> None:
    bus_a = EventBus('BusA')
    bus_b = EventBus('BusB')
    bus_c = EventBus('BusC')

    try:
        handle_counts = {'BusA': 0, 'BusB': 0, 'BusC': 0}
        seen_event_ids = {'BusA': set[str](), 'BusB': set[str](), 'BusC': set[str]()}

        def on_a(event: ForwardedEvent) -> None:
            handle_counts['BusA'] += 1
            seen_event_ids['BusA'].add(event.event_id)
            print(f'[BusA] handled {event.event_id} (count={handle_counts["BusA"]})')

        def on_b(event: ForwardedEvent) -> None:
            handle_counts['BusB'] += 1
            seen_event_ids['BusB'].add(event.event_id)
            print(f'[BusB] handled {event.event_id} (count={handle_counts["BusB"]})')

        def on_c(event: ForwardedEvent) -> None:
            handle_counts['BusC'] += 1
            seen_event_ids['BusC'].add(event.event_id)
            print(f'[BusC] handled {event.event_id} (count={handle_counts["BusC"]})')

        bus_a.on(ForwardedEvent, on_a)
        bus_b.on(ForwardedEvent, on_b)
        bus_c.on(ForwardedEvent, on_c)

        # Ring forwarding:
        # A -> B -> C -> A
        bus_a.on('*', bus_b.emit)
        bus_b.on('*', bus_c.emit)
        bus_c.on('*', bus_a.emit)

        print('Dispatching ForwardedEvent on BusA with cyclic forwarding A -> B -> C -> A')

        event = bus_a.emit(ForwardedEvent(message='hello across 3 buses'))
        await event.now()
        await asyncio.gather(bus_a.wait_until_idle(), bus_b.wait_until_idle(), bus_c.wait_until_idle())

        path = event.event_path
        total_handles = handle_counts['BusA'] + handle_counts['BusB'] + handle_counts['BusC']

        print('\nFinal propagation summary:')
        print(f'- event_id: {event.event_id}')
        print(f'- event_path: {" -> ".join(path)}')
        print(f'- handle counts: {handle_counts}')
        print(
            '- unique ids seen per bus: '
            f'A={len(seen_event_ids["BusA"])}, '
            f'B={len(seen_event_ids["BusB"])}, '
            f'C={len(seen_event_ids["BusC"])}'
        )
        print(f'- total handles: {total_handles}')

        handled_once_per_bus = handle_counts['BusA'] == 1 and handle_counts['BusB'] == 1 and handle_counts['BusC'] == 1
        visited_three_buses = len(path) == 3

        if handled_once_per_bus and visited_three_buses:
            print('\nLoop prevention confirmed: each bus handled the event at most once.')
        else:
            print('\nUnexpected forwarding result. Check handlers/forwarding setup.')

        print('\n=== BusA log_tree() ===')
        print(bus_a.log_tree())
        print('\n=== BusB log_tree() ===')
        print(bus_b.log_tree())
        print('\n=== BusC log_tree() ===')
        print(bus_c.log_tree())
    finally:
        await bus_a.destroy(clear=True)
        await bus_b.destroy(clear=True)
        await bus_c.destroy(clear=True)


if __name__ == '__main__':
    asyncio.run(main())
