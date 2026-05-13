import asyncio

import pytest

from abxbus import BaseEvent, EventBus, EventHandlerConcurrencyMode


class RelayEvent(BaseEvent[str]):
    """Minimal event used for forwarding completion race regression coverage."""


class SelfParentForwardEvent(BaseEvent[str]):
    """Event used to guard against self-parent cycles during forwarding."""


class ForwardedDefaultsTriggerEvent(BaseEvent[None]):
    """Event that emits forwarded children to validate per-bus default resolution."""


class ForwardedDefaultsChildEvent(BaseEvent[str]):
    """Forwarded child event used to validate local-default vs explicit-override behavior."""

    mode: str


class ForwardedFirstDefaultsEvent(BaseEvent[str]):
    """Forwarded event used to validate first-mode behavior against processing-bus defaults."""


def _dump_bus_state(buses: list[EventBus]) -> str:
    lines: list[str] = []
    for bus in buses:
        queue_size = bus.pending_event_queue.qsize() if bus.pending_event_queue else 0
        lines.append(
            f'{bus.label} queue={queue_size} active={len(bus.in_flight_event_ids)} '
            f'processing={len(bus.processing_event_ids)} history={len(bus.event_history)}'
        )
    for bus in buses:
        lines.append(f'--- {bus.label}.log_tree() ---')
        lines.append(bus.log_tree())
    return '\n'.join(lines)


@pytest.mark.asyncio
async def test_forwarded_event_does_not_leave_stale_active_ids():
    """
    Regression test for the original forwarding completion race:
    an event could be marked completed while another bus still retained its
    event_id in in_flight_event_ids, causing wait_until_idle() to hang.
    """
    peer1 = EventBus(name='RacePeer1')
    peer2 = EventBus(name='RacePeer2')
    peer3 = EventBus(name='RacePeer3')
    buses = [peer1, peer2, peer3]

    async def local_handler(_event: BaseEvent[str]) -> str:
        return 'ok'

    peer1.on('*', local_handler)
    peer2.on('*', local_handler)
    peer3.on('*', local_handler)

    # Circular forwarding: peer1 -> peer2 -> peer3 -> peer1
    peer1.on('*', peer2.emit)
    peer2.on('*', peer3.emit)
    peer3.on('*', peer1.emit)

    async def wait_all_idle(timeout: float = 5.0) -> None:
        for bus in buses:
            await asyncio.wait_for(bus.wait_until_idle(), timeout=timeout)

    try:
        # Warm-up propagation (this setup made the original bug deterministic on
        # the immediately-following dispatch from peer2).
        peer1.emit(RelayEvent())
        await asyncio.sleep(0.2)
        await wait_all_idle()

        second = peer2.emit(RelayEvent())
        await asyncio.sleep(0.2)
        try:
            await wait_all_idle()
        except TimeoutError:
            pytest.fail(f'Forwarding completion race left bus(es) non-idle.\n{_dump_bus_state(buses)}')

        assert second.event_status == 'completed'
        for bus in buses:
            assert second.event_id not in bus.in_flight_event_ids
            assert second.event_id not in bus.processing_event_ids

    finally:
        await peer1.destroy(clear=True)
        await peer2.destroy(clear=True)
        await peer3.destroy(clear=True)


@pytest.mark.asyncio
async def test_forwarding_same_event_does_not_set_self_parent_id():
    origin = EventBus(name='SelfParentOrigin')
    target = EventBus(name='SelfParentTarget')

    async def on_origin(_event: SelfParentForwardEvent) -> str:
        return 'origin-ok'

    async def on_target(_event: SelfParentForwardEvent) -> str:
        return 'target-ok'

    origin.on(SelfParentForwardEvent, on_origin)
    target.on(SelfParentForwardEvent, on_target)
    origin.on('*', target.emit)

    try:
        event = origin.emit(SelfParentForwardEvent())
        await event
        await asyncio.gather(origin.wait_until_idle(), target.wait_until_idle())

        assert event.event_parent_id is None
        assert event.event_path == [origin.label, target.label]
    finally:
        await origin.destroy(clear=True)
        await target.destroy(clear=True)


@pytest.mark.asyncio
async def test_forwarded_event_uses_processing_bus_defaults_unless_overridden():
    bus_a = EventBus(name='ForwardDefaultsA', event_handler_concurrency='serial')
    bus_b = EventBus(name='ForwardDefaultsB', event_handler_concurrency='parallel')
    log: list[str] = []

    async def handler_1(event: ForwardedDefaultsChildEvent) -> str:
        log.append(f'{event.mode}:b1_start')
        await asyncio.sleep(0.015)
        log.append(f'{event.mode}:b1_end')
        return 'b1'

    async def handler_2(event: ForwardedDefaultsChildEvent) -> str:
        log.append(f'{event.mode}:b2_start')
        await asyncio.sleep(0.005)
        log.append(f'{event.mode}:b2_end')
        return 'b2'

    async def trigger(event: ForwardedDefaultsTriggerEvent) -> None:
        assert event.event_bus is not None
        inherited = event.event_bus.emit(ForwardedDefaultsChildEvent(mode='inherited', event_timeout=0))
        bus_b.emit(inherited)
        await inherited

        overridden = event.event_bus.emit(
            ForwardedDefaultsChildEvent(
                mode='override',
                event_timeout=0,
                event_handler_concurrency=EventHandlerConcurrencyMode.SERIAL,
            )
        )
        bus_b.emit(overridden)
        await overridden

    bus_b.on(ForwardedDefaultsChildEvent, handler_1)
    bus_b.on(ForwardedDefaultsChildEvent, handler_2)
    bus_a.on(ForwardedDefaultsTriggerEvent, trigger)

    try:
        top = bus_a.emit(ForwardedDefaultsTriggerEvent())
        await top
        await asyncio.gather(bus_a.wait_until_idle(), bus_b.wait_until_idle())

        inherited_b1_end = log.index('inherited:b1_end')
        inherited_b2_start = log.index('inherited:b2_start')
        assert inherited_b2_start < inherited_b1_end, (
            f'inherited defaults should use bus_b parallel handler concurrency; got log: {log}'
        )

        override_b1_end = log.index('override:b1_end')
        override_b2_start = log.index('override:b2_start')
        assert override_b1_end < override_b2_start, (
            f'explicit event override should force serial handler concurrency; got log: {log}'
        )
    finally:
        await bus_a.destroy(clear=True)
        await bus_b.destroy(clear=True)


@pytest.mark.asyncio
async def test_forwarded_first_mode_uses_processing_bus_handler_concurrency_defaults():
    bus_a = EventBus(
        name='ForwardedFirstDefaultsA',
        event_handler_concurrency='serial',
        event_handler_completion='all',
    )
    bus_b = EventBus(
        name='ForwardedFirstDefaultsB',
        event_handler_concurrency='parallel',
        event_handler_completion='first',
    )
    log: list[str] = []

    async def slow_handler(_event: ForwardedFirstDefaultsEvent) -> str:
        log.append('slow_start')
        await asyncio.sleep(0.02)
        log.append('slow_end')
        return 'slow'

    async def fast_handler(_event: ForwardedFirstDefaultsEvent) -> str:
        log.append('fast_start')
        await asyncio.sleep(0.001)
        log.append('fast_end')
        return 'fast'

    bus_a.on('*', bus_b.emit)
    bus_b.on(ForwardedFirstDefaultsEvent, slow_handler)
    bus_b.on(ForwardedFirstDefaultsEvent, fast_handler)

    try:
        event = await bus_a.emit(ForwardedFirstDefaultsEvent(event_timeout=0)).now(first_result=True)
        result = await event.event_result(raise_if_any=False)
        await asyncio.gather(bus_a.wait_until_idle(), bus_b.wait_until_idle())

        assert result == 'fast', f'first-mode on processing bus should pick fast handler; got result={result!r} log={log}'
        assert 'slow_start' in log, f'slow handler should start under parallel first-mode; log={log}'
        assert 'fast_start' in log, f'fast handler should start under parallel first-mode; log={log}'
    finally:
        await bus_a.destroy(clear=True)
        await bus_b.destroy(clear=True)
