#!/usr/bin/env -S uv run python
"""Run: uv run python examples/concurrency_options.py"""

import asyncio
import time
from typing import Literal

from abxbus import BaseEvent, EventBus, EventConcurrencyMode, EventHandlerConcurrencyMode


class WorkEvent(BaseEvent[None]):
    lane: str
    order: int
    ms: int


class HandlerEvent(BaseEvent[None]):
    label: str


class OverrideEvent(BaseEvent[None]):
    label: str
    order: int
    ms: int


class TimeoutEvent(BaseEvent[str]):
    ms: int


async def sleep_ms(ms: int) -> None:
    await asyncio.sleep(ms / 1000.0)


def make_logger(section: str):
    started_at = time.perf_counter()

    def log(message: str) -> None:
        elapsed_ms = (time.perf_counter() - started_at) * 1000
        print(f'[{section}] +{elapsed_ms:.1f}ms {message}')

    return log


async def event_concurrency_demo() -> None:
    global_log = make_logger('event:global-serial')
    global_a = EventBus('GlobalSerialA', event_concurrency='global-serial', event_handler_concurrency='serial')
    global_b = EventBus('GlobalSerialB', event_concurrency='global-serial', event_handler_concurrency='serial')

    try:
        global_in_flight = 0
        global_max = 0

        async def global_handler(event: WorkEvent) -> None:
            nonlocal global_in_flight, global_max
            global_in_flight += 1
            global_max = max(global_max, global_in_flight)
            global_log(f'{event.lane}{event.order} start (global in-flight={global_in_flight})')
            await sleep_ms(event.ms)
            global_log(f'{event.lane}{event.order} end')
            global_in_flight -= 1

        global_a.on(WorkEvent, global_handler)
        global_b.on(WorkEvent, global_handler)

        global_a.emit(WorkEvent(lane='A', order=0, ms=45))
        global_b.emit(WorkEvent(lane='B', order=0, ms=45))
        global_a.emit(WorkEvent(lane='A', order=1, ms=45))
        global_b.emit(WorkEvent(lane='B', order=1, ms=45))
        await asyncio.gather(global_a.wait_until_idle(), global_b.wait_until_idle())

        global_log(f'max in-flight across both buses: {global_max} (expect 1 in global-serial)')
        print('\n=== global_a.log_tree() ===')
        print(global_a.log_tree())
        print('\n=== global_b.log_tree() ===')
        print(global_b.log_tree())
    finally:
        await global_a.destroy(clear=True)
        await global_b.destroy(clear=True)

    bus_log = make_logger('event:bus-serial')
    bus_a = EventBus('BusSerialA', event_concurrency='bus-serial', event_handler_concurrency='serial')
    bus_b = EventBus('BusSerialB', event_concurrency='bus-serial', event_handler_concurrency='serial')

    try:
        per_bus_in_flight: dict[str, int] = {'A': 0, 'B': 0}
        per_bus_max: dict[str, int] = {'A': 0, 'B': 0}
        mixed_global_in_flight = 0
        mixed_global_max = 0

        async def bus_handler(event: WorkEvent) -> None:
            nonlocal mixed_global_in_flight, mixed_global_max
            lane = event.lane
            mixed_global_in_flight += 1
            mixed_global_max = max(mixed_global_max, mixed_global_in_flight)
            per_bus_in_flight[lane] += 1
            per_bus_max[lane] = max(per_bus_max[lane], per_bus_in_flight[lane])
            bus_log(f'{lane}{event.order} start (global={mixed_global_in_flight}, lane={per_bus_in_flight[lane]})')
            await sleep_ms(event.ms)
            bus_log(f'{lane}{event.order} end')
            per_bus_in_flight[lane] -= 1
            mixed_global_in_flight -= 1

        bus_a.on(WorkEvent, bus_handler)
        bus_b.on(WorkEvent, bus_handler)

        bus_a.emit(WorkEvent(lane='A', order=0, ms=45))
        bus_b.emit(WorkEvent(lane='B', order=0, ms=45))
        bus_a.emit(WorkEvent(lane='A', order=1, ms=45))
        bus_b.emit(WorkEvent(lane='B', order=1, ms=45))
        await asyncio.gather(bus_a.wait_until_idle(), bus_b.wait_until_idle())

        bus_log(
            f'max in-flight global={mixed_global_max}, per-bus A={per_bus_max["A"]}, '
            f'B={per_bus_max["B"]} (expect global >= 2, per-bus = 1)'
        )
        print('\n=== bus_a.log_tree() ===')
        print(bus_a.log_tree())
        print('\n=== bus_b.log_tree() ===')
        print(bus_b.log_tree())
    finally:
        await bus_a.destroy(clear=True)
        await bus_b.destroy(clear=True)


async def handler_concurrency_demo() -> None:
    async def run_case(mode: Literal['serial', 'parallel']) -> None:
        log = make_logger(f'handler:{mode}')
        bus = EventBus(f'HandlerMode_{mode}', event_concurrency='parallel', event_handler_concurrency=mode)

        try:
            in_flight = 0
            max_in_flight = 0

            def make_handler(name: str, ms: int):
                async def handler(event: HandlerEvent) -> None:
                    nonlocal in_flight, max_in_flight
                    in_flight += 1
                    max_in_flight = max(max_in_flight, in_flight)
                    log(f'{event.label}:{name} start (handlers in-flight={in_flight})')
                    await sleep_ms(ms)
                    log(f'{event.label}:{name} end')
                    in_flight -= 1

                return handler

            bus.on(HandlerEvent, make_handler('slow', 60))
            bus.on(HandlerEvent, make_handler('fast', 20))

            event = bus.emit(HandlerEvent(label=mode))
            await event.now()
            await bus.wait_until_idle()
            log(f'max handler overlap: {max_in_flight} (expect 1 for serial, >= 2 for parallel)')
            print(f'\n=== {bus.name}.log_tree() ===')
            print(bus.log_tree())
        finally:
            await bus.destroy(clear=True)

    await run_case('serial')
    await run_case('parallel')


async def event_override_demo() -> None:
    log = make_logger('override:precedence')
    bus = EventBus('OverrideBus', event_concurrency='bus-serial', event_handler_concurrency='serial')

    try:
        active_events: set[str] = set()
        per_event_handlers: dict[str, int] = {}
        active_handlers = 0
        max_handlers = 0
        max_events = 0

        def reset_metrics() -> None:
            nonlocal active_events, per_event_handlers, active_handlers, max_handlers, max_events
            active_events = set()
            per_event_handlers = {}
            active_handlers = 0
            max_handlers = 0
            max_events = 0

        def track_start(event: OverrideEvent, handler_name: str, label: str) -> None:
            nonlocal active_handlers, max_handlers, max_events
            active_handlers += 1
            max_handlers = max(max_handlers, active_handlers)
            per_event_handlers[event.event_id] = per_event_handlers.get(event.event_id, 0) + 1
            active_events.add(event.event_id)
            max_events = max(max_events, len(active_events))
            log(f'{label}:{event.order}:{handler_name} start (events={len(active_events)}, handlers={active_handlers})')

        def track_end(event: OverrideEvent, handler_name: str, label: str) -> None:
            nonlocal active_handlers
            active_handlers -= 1
            count = per_event_handlers.get(event.event_id, 1) - 1
            if count <= 0:
                per_event_handlers.pop(event.event_id, None)
                active_events.discard(event.event_id)
            else:
                per_event_handlers[event.event_id] = count
            log(f'{label}:{event.order}:{handler_name} end')

        async def run_pair(label: str, use_override: bool) -> None:
            reset_metrics()

            async def handler_a(event: OverrideEvent) -> None:
                track_start(event, 'A', label)
                await sleep_ms(event.ms)
                track_end(event, 'A', label)

            async def handler_b(event: OverrideEvent) -> None:
                track_start(event, 'B', label)
                await sleep_ms(event.ms)
                track_end(event, 'B', label)

            bus.off(OverrideEvent)
            bus.on(OverrideEvent, handler_a)
            bus.on(OverrideEvent, handler_b)

            if use_override:
                bus.emit(
                    OverrideEvent(
                        label=label,
                        order=0,
                        ms=45,
                        event_concurrency=EventConcurrencyMode.PARALLEL,
                        event_handler_concurrency=EventHandlerConcurrencyMode.PARALLEL,
                    )
                )
                bus.emit(
                    OverrideEvent(
                        label=label,
                        order=1,
                        ms=45,
                        event_concurrency=EventConcurrencyMode.PARALLEL,
                        event_handler_concurrency=EventHandlerConcurrencyMode.PARALLEL,
                    )
                )
            else:
                bus.emit(OverrideEvent(label=label, order=0, ms=45))
                bus.emit(OverrideEvent(label=label, order=1, ms=45))
            await bus.wait_until_idle()
            log(f'{label} summary -> max events={max_events}, max handlers={max_handlers}')

        await run_pair('bus-defaults', use_override=False)
        await run_pair('event-overrides', use_override=True)

        print('\n=== OverrideBus.log_tree() ===')
        print(bus.log_tree())
    finally:
        await bus.destroy(clear=True)


async def handler_timeout_demo() -> None:
    log = make_logger('timeout:handler-option')
    bus = EventBus(
        'TimeoutBus',
        event_concurrency='parallel',
        event_handler_concurrency='parallel',
        event_timeout=0.2,
    )

    try:

        async def slow_handler(event: TimeoutEvent) -> str:
            log('slow handler start')
            await sleep_ms(event.ms)
            log('slow handler finished body (but may already be timed out)')
            return 'slow'

        slow_entry = bus.on(TimeoutEvent, slow_handler)
        slow_entry.handler_timeout = 0.03

        async def fast_handler(_event: TimeoutEvent) -> str:
            log('fast handler start')
            await sleep_ms(10)
            log('fast handler end')
            return 'fast'

        fast_entry = bus.on(TimeoutEvent, fast_handler)
        fast_entry.handler_timeout = 0.1

        event = bus.emit(TimeoutEvent(ms=60, event_handler_timeout=0.5))
        await event.now()

        slow_result = event.event_results.get(slow_entry.id)
        handler_timed_out = slow_result is not None and isinstance(slow_result.error, TimeoutError)
        log(
            f'slow handler status={slow_result.status if slow_result else "missing"}, timeout_error={"yes" if handler_timed_out else "no"}'
        )

        await bus.wait_until_idle()
        print('\n=== TimeoutBus.log_tree() ===')
        print(bus.log_tree())
    finally:
        await bus.destroy(clear=True)


async def main() -> None:
    await event_concurrency_demo()
    await handler_concurrency_demo()
    await event_override_demo()
    await handler_timeout_demo()


if __name__ == '__main__':
    asyncio.run(main())
