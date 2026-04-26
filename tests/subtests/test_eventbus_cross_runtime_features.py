# pyright: basic
"""Cross-feature parity guarantees shared with TypeScript runtime tests."""

from __future__ import annotations

import asyncio
from contextvars import ContextVar

import pytest

from abxbus import BaseEvent, EventBus
from abxbus.event_handler import EventHandlerAbortedError, EventHandlerCancelledError, EventHandlerTimeoutError

request_id_var: ContextVar[str] = ContextVar('request_id', default='<unset>')


class QueueJumpRootEvent(BaseEvent[str]):
    pass


class QueueJumpChildEvent(BaseEvent[str]):
    pass


class QueueJumpSiblingEvent(BaseEvent[str]):
    pass


class ConcurrencyIntersectionEvent(BaseEvent[str]):
    token: int


class TimeoutEnforcementEvent(BaseEvent[str]):
    event_timeout: float | None = 0.02


class TimeoutFollowupEvent(BaseEvent[str]):
    pass


class ZeroHistoryEvent(BaseEvent[str]):
    value: str


class ContextParentEvent(BaseEvent[str]):
    pass


class ContextChildEvent(BaseEvent[str]):
    pass


class PendingVisibilityEvent(BaseEvent[str]):
    tag: str


class BackpressureEvent(BaseEvent[str]):
    value: str


async def test_queue_jump_preserves_parent_child_lineage_and_find_visibility() -> None:
    bus = EventBus(
        name='ParityQueueJumpBus',
        event_concurrency='bus-serial',
        event_handler_concurrency='serial',
    )
    execution_order: list[str] = []
    child_event_id: str | None = None

    async def on_root(event: QueueJumpRootEvent) -> str:
        execution_order.append('root:start')
        child = event.emit(QueueJumpChildEvent())
        await child
        execution_order.append('root:end')
        return 'root-ok'

    async def on_child(event: QueueJumpChildEvent) -> str:
        nonlocal child_event_id
        child_event_id = event.event_id
        execution_order.append('child')
        await asyncio.sleep(0.005)
        return 'child-ok'

    async def on_sibling(_event: QueueJumpSiblingEvent) -> str:
        execution_order.append('sibling')
        return 'sibling-ok'

    bus.on(QueueJumpRootEvent, on_root)
    bus.on(QueueJumpChildEvent, on_child)
    bus.on(QueueJumpSiblingEvent, on_sibling)

    try:
        root = bus.emit(QueueJumpRootEvent())
        sibling = bus.emit(QueueJumpSiblingEvent())
        await root
        await sibling
        await bus.wait_until_idle()

        assert execution_order == ['root:start', 'child', 'root:end', 'sibling']

        found_child = await bus.find(QueueJumpChildEvent, child_of=root, past=True, future=False)
        assert found_child is not None
        assert child_event_id is not None
        assert found_child.event_id == child_event_id
        assert found_child.event_parent_id == root.event_id
        root_result = next(result for result in root.event_results.values() if result.handler_name.endswith('on_root'))
        assert any(child.event_id == found_child.event_id for child in root_result.event_children)
    finally:
        await bus.stop()


async def test_concurrency_intersection_parallel_events_with_serial_handlers() -> None:
    bus = EventBus(
        name='ParityConcurrencyIntersectionBus',
        event_concurrency='parallel',
        event_handler_concurrency='serial',
        max_history_size=None,
    )

    current_by_event: dict[str, int] = {}
    max_by_event: dict[str, int] = {}
    global_current = 0
    global_max = 0
    counter_lock = asyncio.Lock()

    async def tracked_handler(event: ConcurrencyIntersectionEvent) -> str:
        nonlocal global_current, global_max
        async with counter_lock:
            current = current_by_event.get(event.event_id, 0) + 1
            current_by_event[event.event_id] = current
            max_by_event[event.event_id] = max(max_by_event.get(event.event_id, 0), current)

            global_current += 1
            global_max = max(global_max, global_current)

        await asyncio.sleep(0.01)

        async with counter_lock:
            current_by_event[event.event_id] = max(0, current_by_event.get(event.event_id, 1) - 1)
            global_current -= 1

        return f'ok-{event.token}'

    async def tracked_handler_a(event: ConcurrencyIntersectionEvent) -> str:
        return await tracked_handler(event)

    async def tracked_handler_b(event: ConcurrencyIntersectionEvent) -> str:
        return await tracked_handler(event)

    bus.on(ConcurrencyIntersectionEvent, tracked_handler_a)
    bus.on(ConcurrencyIntersectionEvent, tracked_handler_b)

    try:
        events = [bus.emit(ConcurrencyIntersectionEvent(token=index)) for index in range(8)]
        await asyncio.gather(*events)
        await bus.wait_until_idle()

        for event in events:
            assert max_by_event.get(event.event_id) == 1
            assert all(result.status == 'completed' for result in event.event_results.values())

        assert global_max >= 2
    finally:
        await bus.stop()


async def test_timeout_enforcement_does_not_break_followup_processing_or_queue_state() -> None:
    bus = EventBus(name='ParityTimeoutEnforcementBus', event_handler_concurrency='parallel')

    async def slow_handler_a(_event: TimeoutEnforcementEvent) -> str:
        await asyncio.sleep(0.2)
        return 'slow-a'

    async def slow_handler_b(_event: TimeoutEnforcementEvent) -> str:
        await asyncio.sleep(0.2)
        return 'slow-b'

    async def followup_handler(_event: TimeoutFollowupEvent) -> str:
        return 'followup-ok'

    bus.on(TimeoutEnforcementEvent, slow_handler_a)
    bus.on(TimeoutEnforcementEvent, slow_handler_b)
    bus.on(TimeoutFollowupEvent, followup_handler)

    try:
        timed_out = await bus.emit(TimeoutEnforcementEvent())
        assert timed_out.event_status == 'completed'
        assert timed_out.event_results
        assert all(result.status == 'error' for result in timed_out.event_results.values())
        assert all(
            isinstance(result.error, (EventHandlerAbortedError, EventHandlerTimeoutError, EventHandlerCancelledError))
            for result in timed_out.event_results.values()
        )

        followup = await bus.emit(TimeoutFollowupEvent())
        followup_result = await followup.event_result(raise_if_any=False, raise_if_none=False)
        assert followup_result == 'followup-ok'
        assert all(result.status == 'completed' for result in followup.event_results.values())

        await bus.wait_until_idle()
        assert bus.pending_event_queue is not None
        assert bus.pending_event_queue.qsize() == 0
        assert not bus.in_flight_event_ids
    finally:
        await bus.stop()


async def test_zero_history_backpressure_with_find_future_still_resolves_new_events() -> None:
    bus = EventBus(name='ParityZeroHistoryBus', max_history_size=0, max_history_drop=False)

    async def handler(event: ZeroHistoryEvent) -> str:
        return f'ok:{event.value}'

    bus.on(ZeroHistoryEvent, handler)

    try:
        first = await bus.emit(ZeroHistoryEvent(value='first'))
        assert first.event_id not in bus.event_history

        past = await bus.find(ZeroHistoryEvent, past=True, future=False)
        assert past is None

        captured_future_id: str | None = None

        async def dispatch_later() -> None:
            nonlocal captured_future_id
            await asyncio.sleep(0.02)
            future_event = bus.emit(ZeroHistoryEvent(value='future'))
            captured_future_id = future_event.event_id

        future_task = asyncio.create_task(dispatch_later())
        future_match = await bus.find(ZeroHistoryEvent, where=lambda event: event.value == 'future', past=False, future=1.0)
        await future_task

        assert future_match is not None
        assert future_match.value == 'future'
        assert captured_future_id is not None
        assert future_match.event_id == captured_future_id

        await bus.wait_until_idle()
        assert len(bus.event_history) == 0
    finally:
        await bus.stop()


async def test_context_propagates_through_forwarding_and_child_dispatch_with_lineage_intact() -> None:
    bus_a = EventBus(name='ParityContextForwardA')
    bus_b = EventBus(name='ParityContextForwardB')

    captured_parent_request_id: str | None = None
    captured_child_request_id: str | None = None
    parent_event_id: str | None = None
    child_parent_id: str | None = None

    async def on_parent(event: ContextParentEvent) -> str:
        nonlocal captured_parent_request_id, parent_event_id
        captured_parent_request_id = request_id_var.get()
        parent_event_id = event.event_id

        child = event.emit(ContextChildEvent())
        await child
        return 'parent-ok'

    async def on_child(event: ContextChildEvent) -> str:
        nonlocal captured_child_request_id, child_parent_id
        captured_child_request_id = request_id_var.get()
        child_parent_id = event.event_parent_id
        return 'child-ok'

    bus_a.on('*', bus_b.emit)
    bus_b.on(ContextParentEvent, on_parent)
    bus_b.on(ContextChildEvent, on_child)

    request_id = 'fc81f432-98cd-7a06-824c-dafed74761bb'
    token = request_id_var.set(request_id)
    try:
        parent = await bus_a.emit(ContextParentEvent())
        await bus_b.wait_until_idle()

        assert captured_parent_request_id == request_id
        assert captured_child_request_id == request_id
        assert parent_event_id is not None
        assert child_parent_id == parent_event_id
        assert parent.event_path[0].startswith('ParityContextForwardA#')
        assert any(path.startswith('ParityContextForwardB#') for path in parent.event_path)

        found_child = await bus_b.find(ContextChildEvent, child_of=parent, past=True, future=False)
        assert found_child is not None
        assert found_child.event_parent_id == parent.event_id
    finally:
        request_id_var.reset(token)
        await bus_a.stop()
        await bus_b.stop()


async def test_pending_queue_find_visibility_transitions_to_completed_after_release() -> None:
    bus = EventBus(
        name='ParityPendingFindBus',
        event_concurrency='bus-serial',
        event_handler_concurrency='serial',
        max_history_size=None,
    )
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(event: PendingVisibilityEvent) -> str:
        if event.tag == 'blocking':
            started.set()
            await release.wait()
        return f'ok:{event.tag}'

    bus.on(PendingVisibilityEvent, handler)

    try:
        blocking = bus.emit(PendingVisibilityEvent(tag='blocking'))
        await started.wait()

        queued = bus.emit(PendingVisibilityEvent(tag='queued'))
        await asyncio.sleep(0.01)

        pending = await bus.find(
            PendingVisibilityEvent,
            where=lambda event: event.tag == 'queued',
            past=True,
            future=False,
            event_status='pending',
        )
        assert pending is not None
        assert pending.event_id == queued.event_id

        release.set()
        await blocking
        await queued
        await bus.wait_until_idle()

        completed = await bus.find(
            PendingVisibilityEvent,
            where=lambda event: event.tag == 'queued',
            past=True,
            future=False,
            event_status='completed',
        )
        assert completed is not None
        assert completed.event_id == queued.event_id
        assert bus.pending_event_queue is not None
        assert bus.pending_event_queue.qsize() == 0
        assert not bus.in_flight_event_ids
    finally:
        await bus.stop()


async def test_history_backpressure_rejects_overflow_and_preserves_findable_history() -> None:
    bus = EventBus(name='ParityBackpressureBus', max_history_size=1, max_history_drop=False)

    async def handler(event: BackpressureEvent) -> str:
        return f'ok:{event.value}'

    bus.on(BackpressureEvent, handler)

    try:
        first = await bus.emit(BackpressureEvent(value='first'))
        assert len(bus.event_history) == 1
        assert first.event_id in bus.event_history

        found_first = await bus.find(BackpressureEvent, where=lambda event: event.value == 'first', past=True, future=False)
        assert found_first is not None
        assert found_first.event_id == first.event_id

        with pytest.raises(RuntimeError):
            _ = bus.emit(BackpressureEvent(value='second'))

        assert len(bus.event_history) == 1
        assert first.event_id in bus.event_history
        assert bus.pending_event_queue is not None
        assert bus.pending_event_queue.qsize() == 0
        assert not bus.in_flight_event_ids
    finally:
        await bus.stop()
