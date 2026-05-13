import asyncio

import pytest

from abxbus import BaseEvent, EventBus, EventHandlerConcurrencyMode


class PingEvent(BaseEvent[None]):
    value: int


class OrderEvent(BaseEvent[None]):
    order: int


class SelfParentForwardEvent(BaseEvent[str]):
    """Event used to guard against self-parent cycles during forwarding."""


class ForwardedDefaultsTriggerEvent(BaseEvent[None]):
    """Event that emits forwarded children to validate per-bus default resolution."""


class ForwardedDefaultsChildEvent(BaseEvent[str]):
    """Forwarded child event used to validate local-default vs explicit-override behavior."""

    mode: str


class ForwardedFirstDefaultsEvent(BaseEvent[str]):
    """Forwarded event used to validate first-mode behavior against processing-bus defaults."""


class ProxyDispatchRootEvent(BaseEvent[str]):
    """Parent event for proxy-dispatch child-linking coverage."""


class ProxyDispatchChildEvent(BaseEvent[str]):
    """Child event for proxy-dispatch child-linking coverage."""


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


async def _wait_all_idle(buses: list[EventBus], timeout: float = 5.0) -> None:
    for bus in buses:
        await asyncio.wait_for(bus.wait_until_idle(), timeout=timeout)


def _index(log: list[str], value: str) -> int:
    try:
        return log.index(value)
    except ValueError as err:
        raise AssertionError(f'missing required log entry {value!r}; log={log}') from err


@pytest.mark.asyncio
async def test_events_forward_between_buses_without_duplication():
    bus_a = EventBus(name='BusA')
    bus_b = EventBus(name='BusB')
    bus_c = EventBus(name='BusC')
    seen_a: list[str] = []
    seen_b: list[str] = []
    seen_c: list[str] = []

    bus_a.on(PingEvent, lambda event: seen_a.append(event.event_id))
    bus_b.on(PingEvent, lambda event: seen_b.append(event.event_id))
    bus_c.on(PingEvent, lambda event: seen_c.append(event.event_id))
    bus_a.on('*', bus_b.emit)
    bus_b.on('*', bus_c.emit)

    try:
        event = bus_a.emit(PingEvent(value=1))
        await event.now()
        await _wait_all_idle([bus_a, bus_b, bus_c])

        assert seen_a == [event.event_id]
        assert seen_b == [event.event_id]
        assert seen_c == [event.event_id]
        assert event.event_path == [bus_a.label, bus_b.label, bus_c.label]
        assert event.event_pending_bus_count == 0
    finally:
        await bus_a.destroy(clear=True)
        await bus_b.destroy(clear=True)
        await bus_c.destroy(clear=True)


@pytest.mark.asyncio
async def test_tree_level_hierarchy_bubbling():
    parent_bus = EventBus(name='ParentBus')
    child_bus = EventBus(name='ChildBus')
    subchild_bus = EventBus(name='SubchildBus')
    seen_parent: list[str] = []
    seen_child: list[str] = []
    seen_subchild: list[str] = []

    parent_bus.on(PingEvent, lambda event: seen_parent.append(event.event_id))
    child_bus.on(PingEvent, lambda event: seen_child.append(event.event_id))
    subchild_bus.on(PingEvent, lambda event: seen_subchild.append(event.event_id))
    child_bus.on('*', parent_bus.emit)
    subchild_bus.on('*', child_bus.emit)

    try:
        bottom = subchild_bus.emit(PingEvent(value=1))
        await bottom.now()
        await _wait_all_idle([subchild_bus, child_bus, parent_bus])

        assert seen_subchild == [bottom.event_id]
        assert seen_child == [bottom.event_id]
        assert seen_parent == [bottom.event_id]
        assert bottom.event_path == [subchild_bus.label, child_bus.label, parent_bus.label]

        seen_parent.clear()
        seen_child.clear()
        seen_subchild.clear()

        middle = child_bus.emit(PingEvent(value=2))
        await middle.now()
        await _wait_all_idle([child_bus, parent_bus])

        assert seen_subchild == []
        assert seen_child == [middle.event_id]
        assert seen_parent == [middle.event_id]
        assert middle.event_path == [child_bus.label, parent_bus.label]
    finally:
        await parent_bus.destroy(clear=True)
        await child_bus.destroy(clear=True)
        await subchild_bus.destroy(clear=True)


@pytest.mark.asyncio
async def test_forwarding_disambiguates_buses_that_share_the_same_name():
    bus_a = EventBus(name='SharedName')
    bus_b = EventBus(name='SharedName')
    seen_a: list[str] = []
    seen_b: list[str] = []

    bus_a.on(PingEvent, lambda event: seen_a.append(event.event_id))
    bus_b.on(PingEvent, lambda event: seen_b.append(event.event_id))
    bus_a.on('*', bus_b.emit)

    try:
        event = bus_a.emit(PingEvent(value=99))
        await event.now()
        await _wait_all_idle([bus_a, bus_b])

        assert seen_a == [event.event_id]
        assert seen_b == [event.event_id]
        assert bus_a.label != bus_b.label
        assert event.event_path == [bus_a.label, bus_b.label]
    finally:
        await bus_a.destroy(clear=True)
        await bus_b.destroy(clear=True)


@pytest.mark.asyncio
async def test_await_event_now_waits_for_handlers_on_forwarded_buses():
    bus_a = EventBus(name='ForwardWaitA')
    bus_b = EventBus(name='ForwardWaitB')
    bus_c = EventBus(name='ForwardWaitC')
    completion_log: list[str] = []

    async def handler_a(_event: PingEvent) -> None:
        await asyncio.sleep(0.01)
        completion_log.append('A')

    async def handler_b(_event: PingEvent) -> None:
        await asyncio.sleep(0.03)
        completion_log.append('B')

    async def handler_c(_event: PingEvent) -> None:
        await asyncio.sleep(0.05)
        completion_log.append('C')

    bus_a.on(PingEvent, handler_a)
    bus_b.on(PingEvent, handler_b)
    bus_c.on(PingEvent, handler_c)
    bus_a.on('*', bus_b.emit)
    bus_b.on('*', bus_c.emit)

    try:
        event = bus_a.emit(PingEvent(value=2))
        await event.now()
        await _wait_all_idle([bus_a, bus_b, bus_c])

        assert sorted(completion_log) == ['A', 'B', 'C']
        assert event.event_pending_bus_count == 0
        assert event.event_path == [bus_a.label, bus_b.label, bus_c.label]
    finally:
        await bus_a.destroy(clear=True)
        await bus_b.destroy(clear=True)
        await bus_c.destroy(clear=True)


@pytest.mark.asyncio
async def test_circular_forwarding_from_first_peer_does_not_loop():
    peer1 = EventBus(name='Peer1')
    peer2 = EventBus(name='Peer2')
    peer3 = EventBus(name='Peer3')
    seen1: list[str] = []
    seen2: list[str] = []
    seen3: list[str] = []
    buses = [peer1, peer2, peer3]

    peer1.on(PingEvent, lambda event: seen1.append(event.event_id))
    peer2.on(PingEvent, lambda event: seen2.append(event.event_id))
    peer3.on(PingEvent, lambda event: seen3.append(event.event_id))
    peer1.on('*', peer2.emit)
    peer2.on('*', peer3.emit)
    peer3.on('*', peer1.emit)

    try:
        event = peer1.emit(PingEvent(value=42))
        await event.now()
        await _wait_all_idle(buses)

        assert seen1 == [event.event_id]
        assert seen2 == [event.event_id]
        assert seen3 == [event.event_id]
        assert event.event_path == [peer1.label, peer2.label, peer3.label]
    finally:
        await peer1.destroy(clear=True)
        await peer2.destroy(clear=True)
        await peer3.destroy(clear=True)


@pytest.mark.asyncio
async def test_circular_forwarding_from_middle_peer_does_not_loop():
    peer1 = EventBus(name='RacePeer1')
    peer2 = EventBus(name='RacePeer2')
    peer3 = EventBus(name='RacePeer3')
    seen1: list[str] = []
    seen2: list[str] = []
    seen3: list[str] = []
    buses = [peer1, peer2, peer3]

    peer1.on(PingEvent, lambda event: seen1.append(event.event_id))
    peer2.on(PingEvent, lambda event: seen2.append(event.event_id))
    peer3.on(PingEvent, lambda event: seen3.append(event.event_id))
    peer1.on('*', peer2.emit)
    peer2.on('*', peer3.emit)
    peer3.on('*', peer1.emit)

    try:
        # Warm-up propagation keeps the original stale-active-id race deterministic.
        warmup = peer1.emit(PingEvent(value=42))
        await warmup.now()
        await _wait_all_idle(buses)
        seen1.clear()
        seen2.clear()
        seen3.clear()

        event = peer2.emit(PingEvent(value=99))
        await event.now()
        try:
            await _wait_all_idle(buses)
        except TimeoutError:
            pytest.fail(f'Forwarding completion race left bus(es) non-idle.\n{_dump_bus_state(buses)}')

        assert seen1 == [event.event_id]
        assert seen2 == [event.event_id]
        assert seen3 == [event.event_id]
        assert event.event_path == [peer2.label, peer3.label, peer1.label]
        assert event.event_status == 'completed'
        for bus in buses:
            assert event.event_id not in bus.in_flight_event_ids
            assert event.event_id not in bus.processing_event_ids
    finally:
        await peer1.destroy(clear=True)
        await peer2.destroy(clear=True)
        await peer3.destroy(clear=True)


@pytest.mark.asyncio
async def test_await_event_now_waits_when_forwarding_handler_is_async_delayed():
    bus_a = EventBus(name='BusADelayedForward')
    bus_b = EventBus(name='BusBDelayedForward')
    bus_a_done = False
    bus_b_done = False

    async def handler_a(_event: PingEvent) -> None:
        nonlocal bus_a_done
        await asyncio.sleep(0.02)
        bus_a_done = True

    async def handler_b(_event: PingEvent) -> None:
        nonlocal bus_b_done
        await asyncio.sleep(0.01)
        bus_b_done = True

    async def delayed_forward(event: PingEvent) -> None:
        await asyncio.sleep(0.03)
        bus_b.emit(event)

    bus_a.on(PingEvent, handler_a)
    bus_b.on(PingEvent, handler_b)
    bus_a.on('*', delayed_forward)

    try:
        event = bus_a.emit(PingEvent(value=3))
        await event.now()

        assert bus_a_done is True
        assert bus_b_done is True
        assert event.event_pending_bus_count == 0
        assert event.event_path == [bus_a.label, bus_b.label]
    finally:
        await bus_a.destroy(clear=True)
        await bus_b.destroy(clear=True)


@pytest.mark.asyncio
async def test_forwarding_same_event_does_not_set_self_parent_id():
    origin = EventBus(name='SelfParentOrigin')
    target = EventBus(name='SelfParentTarget')

    origin.on(SelfParentForwardEvent, lambda _event: 'origin-ok')
    target.on(SelfParentForwardEvent, lambda _event: 'target-ok')
    origin.on('*', target.emit)

    try:
        event = origin.emit(SelfParentForwardEvent())
        await event.now()
        await _wait_all_idle([origin, target])

        assert event.event_parent_id is None
        assert event.event_path == [origin.label, target.label]
    finally:
        await origin.destroy(clear=True)
        await target.destroy(clear=True)


@pytest.mark.asyncio
async def test_forwarded_event_uses_processing_bus_defaults():
    bus_a = EventBus(name='ForwardDefaultsA', event_handler_concurrency='serial', event_timeout=1.5)
    bus_b = EventBus(name='ForwardDefaultsB', event_handler_concurrency='parallel', event_timeout=2.5)
    log: list[str] = []
    inherited_ref: ForwardedDefaultsChildEvent | None = None

    async def handler_1(event: ForwardedDefaultsChildEvent) -> str:
        assert event.event_timeout is None
        assert event.event_handler_concurrency is None
        assert event.event_handler_completion is None
        log.append(f'{event.mode}:b1_start')
        await asyncio.sleep(0.015)
        log.append(f'{event.mode}:b1_end')
        return 'b1'

    async def handler_2(event: ForwardedDefaultsChildEvent) -> str:
        assert event.event_timeout is None
        assert event.event_handler_concurrency is None
        assert event.event_handler_completion is None
        log.append(f'{event.mode}:b2_start')
        await asyncio.sleep(0.005)
        log.append(f'{event.mode}:b2_end')
        return 'b2'

    async def trigger(event: ForwardedDefaultsTriggerEvent) -> None:
        nonlocal inherited_ref
        inherited = event.emit(ForwardedDefaultsChildEvent(mode='inherited'))
        inherited_ref = inherited
        bus_b.emit(inherited)
        await inherited.now()

    bus_b.on(ForwardedDefaultsChildEvent, handler_1)
    bus_b.on(ForwardedDefaultsChildEvent, handler_2)
    bus_a.on(ForwardedDefaultsTriggerEvent, trigger)

    try:
        top = bus_a.emit(ForwardedDefaultsTriggerEvent())
        await top.now()
        await _wait_all_idle([bus_a, bus_b])

        inherited_b1_end = _index(log, 'inherited:b1_end')
        inherited_b2_start = _index(log, 'inherited:b2_start')
        assert inherited_b2_start < inherited_b1_end, f'inherited mode should use bus_b parallel concurrency; log={log}'
        assert inherited_ref is not None
        assert inherited_ref.event_timeout is None
        assert inherited_ref.event_handler_concurrency is None
        assert inherited_ref.event_handler_completion is None
        bus_b_results = [result for result in inherited_ref.event_results.values() if result.handler.eventbus_id == bus_b.id]
        assert bus_b_results
        assert all(result.timeout == bus_b.event_timeout for result in bus_b_results)
    finally:
        await bus_a.destroy(clear=True)
        await bus_b.destroy(clear=True)


@pytest.mark.asyncio
async def test_forwarded_event_preserves_explicit_handler_concurrency_override():
    bus_a = EventBus(name='ForwardOverrideA', event_handler_concurrency='parallel')
    bus_b = EventBus(name='ForwardOverrideB', event_handler_concurrency='parallel')
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
        override = event.emit(
            ForwardedDefaultsChildEvent(
                mode='override',
                event_timeout=0,
                event_handler_concurrency=EventHandlerConcurrencyMode.SERIAL,
            )
        )
        bus_b.emit(override)
        await override.now()

    bus_b.on(ForwardedDefaultsChildEvent, handler_1)
    bus_b.on(ForwardedDefaultsChildEvent, handler_2)
    bus_a.on(ForwardedDefaultsTriggerEvent, trigger)

    try:
        top = bus_a.emit(ForwardedDefaultsTriggerEvent())
        await top.now()
        await _wait_all_idle([bus_a, bus_b])

        override_b1_end = _index(log, 'override:b1_end')
        override_b2_start = _index(log, 'override:b2_start')
        assert override_b1_end < override_b2_start, f'explicit override should force serial concurrency; log={log}'
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
        await _wait_all_idle([bus_a, bus_b])

        assert result == 'fast', f'first-mode on processing bus should pick fast handler; result={result!r} log={log}'
        assert 'slow_start' in log, f'slow handler should start under parallel first-mode; log={log}'
        assert 'fast_start' in log, f'fast handler should start under parallel first-mode; log={log}'
    finally:
        await bus_a.destroy(clear=True)
        await bus_b.destroy(clear=True)


@pytest.mark.asyncio
async def test_proxy_dispatch_auto_links_child_events_like_emit():
    bus = EventBus(name='ProxyDispatchAutoLinkBus')

    async def root_handler(event: ProxyDispatchRootEvent) -> str:
        event.emit(ProxyDispatchChildEvent())
        return 'root'

    bus.on(ProxyDispatchRootEvent, root_handler)
    bus.on(ProxyDispatchChildEvent, lambda _event: 'child')

    try:
        root = bus.emit(ProxyDispatchRootEvent())
        await root.now()
        await bus.wait_until_idle()

        assert len(root.event_children) == 1
        child = root.event_children[0]
        assert child.event_parent_id == root.event_id
        assert root.event_children[0].event_id == child.event_id
    finally:
        await bus.destroy(clear=True)


@pytest.mark.asyncio
async def test_proxy_dispatch_of_same_event_does_not_self_parent_or_self_link_child():
    bus = EventBus(name='ProxyDispatchSameEventBus')

    async def root_handler(event: ProxyDispatchRootEvent) -> str:
        event.emit(event)
        return 'root'

    bus.on(ProxyDispatchRootEvent, root_handler)

    try:
        root = bus.emit(ProxyDispatchRootEvent())
        await root.now()
        await bus.wait_until_idle()

        assert root.event_parent_id is None
        assert len(root.event_children) == 0
    finally:
        await bus.destroy(clear=True)


@pytest.mark.asyncio
async def test_events_are_processed_in_fifo_order():
    bus = EventBus(name='FifoBus')
    processed_orders: list[int] = []
    handler_start_times: list[float] = []

    async def order_handler(event: OrderEvent) -> None:
        handler_start_times.append(asyncio.get_running_loop().time())
        if event.order % 2 == 0:
            await asyncio.sleep(0.03)
        else:
            await asyncio.sleep(0.005)
        processed_orders.append(event.order)

    bus.on(OrderEvent, order_handler)

    try:
        for order in range(10):
            bus.emit(OrderEvent(order=order))

        await bus.wait_until_idle()

        assert processed_orders == list(range(10))
        assert all(handler_start_times[i] >= handler_start_times[i - 1] for i in range(1, len(handler_start_times)))
    finally:
        await bus.destroy(clear=True)
