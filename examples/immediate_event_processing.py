#!/usr/bin/env -S uv run python
"""Run: uv run python examples/immediate_event_processing.py"""

import asyncio
from typing import Literal

from abxbus import BaseEvent, EventBus, EventConcurrencyMode


class ParentEvent(BaseEvent[None]):
    mode: Literal['immediate', 'queued']


class ChildEvent(BaseEvent[None]):
    scenario: Literal['immediate', 'queued']


class SiblingEvent(BaseEvent[None]):
    scenario: Literal['immediate', 'queued']


async def delay_ms(ms: int) -> None:
    await asyncio.sleep(ms / 1000.0)


async def main() -> None:
    bus_a = EventBus(
        name='QueueJumpDemoA',
        event_concurrency='bus-serial',
        event_handler_concurrency='serial',
    )
    bus_b = EventBus(
        name='QueueJumpDemoB',
        event_concurrency='bus-serial',
        event_handler_concurrency='serial',
    )

    try:
        step = 0

        def log(message: str) -> None:
            nonlocal step
            step += 1
            print(f'{step:02d}. {message}')

        # Forward sibling/child events from bus_a -> bus_b.
        def forward_child(event: ChildEvent) -> None:
            log(f'[forward] {event.event_type}({event.scenario}) bus_a -> bus_b')
            bus_b.emit(event)

        def forward_sibling(event: SiblingEvent) -> None:
            log(f'[forward] {event.event_type}({event.scenario}) bus_a -> bus_b')
            bus_b.emit(event)

        bus_a.on(ChildEvent, forward_child)
        bus_a.on(SiblingEvent, forward_sibling)

        # Local handlers on bus_a.
        async def on_child_a(event: ChildEvent) -> None:
            log(f'[bus_a] child start ({event.scenario})')
            await delay_ms(8)
            log(f'[bus_a] child end ({event.scenario})')

        async def on_sibling_a(event: SiblingEvent) -> None:
            log(f'[bus_a] sibling start ({event.scenario})')
            await delay_ms(14)
            log(f'[bus_a] sibling end ({event.scenario})')

        bus_a.on(ChildEvent, on_child_a)
        bus_a.on(SiblingEvent, on_sibling_a)

        # Forwarded handlers on bus_b.
        async def on_child_b(event: ChildEvent) -> None:
            log(f'[bus_b] child start ({event.scenario})')
            await delay_ms(4)
            log(f'[bus_b] child end ({event.scenario})')

        async def on_sibling_b(event: SiblingEvent) -> None:
            log(f'[bus_b] sibling start ({event.scenario})')
            await delay_ms(6)
            log(f'[bus_b] sibling end ({event.scenario})')

        bus_b.on(ChildEvent, on_child_b)
        bus_b.on(SiblingEvent, on_sibling_b)

        # Parent handler queues sibling first, then child, then compares await behavior.
        async def on_parent(event: ParentEvent) -> None:
            log(f'[parent:{event.mode}] start')

            event.event_bus.emit(SiblingEvent(scenario=event.mode))
            log(f'[parent:{event.mode}] sibling queued')

            child = event.event_bus.emit(ChildEvent(scenario=event.mode))
            log(f'[parent:{event.mode}] child queued')

            if event.mode == 'immediate':
                # Immediate: queue-jump by awaiting child directly inside handler context.
                log(f'[parent:{event.mode}] await child')
                await child
                log(f'[parent:{event.mode}] child await resolved')
            else:
                # Queued: wait on completion signal without queue-jump processing.
                log(f'[parent:{event.mode}] await child.event_completed()')
                await child.event_completed()
                log(f'[parent:{event.mode}] child.event_completed() resolved')

            log(f'[parent:{event.mode}] end')

        bus_a.on(ParentEvent, on_parent)

        async def run_scenario(mode: Literal['immediate', 'queued']) -> None:
            log(f'----- scenario={mode} -----')

            parent = bus_a.emit(
                ParentEvent(
                    mode=mode,
                    event_concurrency=EventConcurrencyMode.PARALLEL,
                )
            )

            await parent
            await asyncio.gather(bus_a.wait_until_idle(), bus_b.wait_until_idle())
            log(f'----- done scenario={mode} -----')

        await run_scenario('immediate')
        await run_scenario('queued')

        print('\nExpected behavior:')
        print('- immediate: child runs before sibling (queue-jump) and parent resumes right after child.')
        print('- queued: sibling runs first, child waits in normal queue order, parent resumes later.')
        print('\n=== bus_a.log_tree() ===')
        print(bus_a.log_tree())
        print('\n=== bus_b.log_tree() ===')
        print(bus_b.log_tree())
    finally:
        await bus_a.stop(clear=True, timeout=0)
        await bus_b.stop(clear=True, timeout=0)


if __name__ == '__main__':
    asyncio.run(main())
