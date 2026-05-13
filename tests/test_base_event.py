import asyncio
import gc
import logging
import time

import pytest
from pydantic import ValidationError

from abxbus import BaseEvent, EventBus, EventConcurrencyMode, EventStatus


@pytest.fixture(autouse=True)
async def cleanup_eventbus_instances():
    """Ensure EventBus instances are cleaned up between tests"""
    yield
    # Force garbage collection to clean up any lingering EventBus instances
    gc.collect()
    # Give event loops time to clean up
    await asyncio.sleep(0.01)


class MainEvent(BaseEvent[None]):
    message: str = 'test'


class ChildEvent(BaseEvent[None]):
    data: str = 'child'


class GrandchildEvent(BaseEvent[None]):
    info: str = 'grandchild'


async def test_event_bus_aliases_bus_property():
    bus = EventBus(name='AliasBus')
    seen_bus = None
    seen_event_bus = None

    async def handler(event: MainEvent):
        nonlocal seen_bus, seen_event_bus
        seen_bus = event.bus
        seen_event_bus = event.event_bus

    bus.on(MainEvent, handler)
    await bus.emit(MainEvent())
    assert seen_bus is bus
    assert seen_event_bus is bus
    assert seen_bus is seen_event_bus
    await bus.destroy()


async def test_await_event_queue_jumps_inside_handler():
    class ParentEvent(BaseEvent[None]):
        pass

    class ChildEvent(BaseEvent[None]):
        pass

    class SiblingEvent(BaseEvent[None]):
        pass

    bus = EventBus(
        name='QueueJumpAwaitEventBus',
        event_concurrency='bus-serial',
        event_handler_concurrency='serial',
    )
    order: list[str] = []

    async def on_parent(event: ParentEvent) -> None:
        order.append('parent_start')
        # Explicitly detached: this sibling should stay queued until the awaited child finishes.
        event.bus.emit(SiblingEvent())
        child = event.emit(ChildEvent())
        await child
        order.append('parent_end')

    async def on_child(_: ChildEvent) -> None:
        order.append('child')

    async def on_sibling(_: SiblingEvent) -> None:
        order.append('sibling')

    bus.on(ParentEvent, on_parent)
    bus.on(ChildEvent, on_child)
    bus.on(SiblingEvent, on_sibling)

    await bus.emit(ParentEvent())
    await bus.wait_until_idle()
    assert order == ['parent_start', 'child', 'parent_end', 'sibling']
    await bus.destroy()


async def test_base_event_now_inside_handler_no_args():
    class ParentEvent(BaseEvent[None]):
        pass

    class ChildEvent(BaseEvent[None]):
        pass

    class SiblingEvent(BaseEvent[None]):
        pass

    bus = EventBus(
        name='QueueJumpNowBus',
        event_concurrency='bus-serial',
        event_handler_concurrency='serial',
    )
    order: list[str] = []

    async def on_parent(event: ParentEvent) -> None:
        order.append('parent_start')
        # Explicitly detached: this sibling should stay queued until the awaited child finishes.
        event.bus.emit(SiblingEvent())
        child = event.emit(ChildEvent())
        await child.now()
        order.append('parent_end')

    async def on_child(_: ChildEvent) -> None:
        order.append('child')

    async def on_sibling(_: SiblingEvent) -> None:
        order.append('sibling')

    bus.on(ParentEvent, on_parent)
    bus.on(ChildEvent, on_child)
    bus.on(SiblingEvent, on_sibling)

    await bus.emit(ParentEvent()).now()
    await bus.wait_until_idle()
    assert order == ['parent_start', 'child', 'parent_end', 'sibling']
    await bus.destroy()


async def test_base_event_now_inside_handler_with_args():
    class ParentEvent(BaseEvent[None]):
        pass

    class ChildEvent(BaseEvent[None]):
        pass

    class SiblingEvent(BaseEvent[None]):
        pass

    bus = EventBus(
        name='QueueJumpNowArgsBus',
        event_concurrency='bus-serial',
        event_handler_concurrency='serial',
    )
    order: list[str] = []
    child_ref: ChildEvent | None = None

    async def on_parent(event: ParentEvent) -> None:
        nonlocal child_ref
        order.append('parent_start')
        event.bus.emit(SiblingEvent())
        child_ref = event.emit(ChildEvent())
        await child_ref.now(timeout=1.0)
        order.append('parent_end')

    async def on_child(_: ChildEvent) -> None:
        order.append('child')
        raise ValueError('child failure')

    async def on_sibling(_: SiblingEvent) -> None:
        order.append('sibling')

    bus.on(ParentEvent, on_parent)
    bus.on(ChildEvent, on_child)
    bus.on(SiblingEvent, on_sibling)

    await bus.emit(ParentEvent()).now()
    await bus.wait_until_idle()
    assert order == ['parent_start', 'child', 'parent_end', 'sibling']
    assert child_ref is not None
    assert child_ref.event_status == 'completed'
    assert any(result.status == 'error' for result in child_ref.event_results.values())
    await bus.destroy()


async def test_wait_outside_handler_preserves_normal_queue_order():
    class BlockerEvent(BaseEvent[None]):
        pass

    class TargetEvent(BaseEvent[None]):
        pass

    bus = EventBus(
        name='WaitOutsideHandlerQueueOrderBus',
        event_concurrency='bus-serial',
        event_handler_concurrency='serial',
    )
    order: list[str] = []
    blocker_started = asyncio.Event()
    release_blocker = asyncio.Event()

    async def on_blocker(_: BlockerEvent) -> None:
        order.append('blocker_start')
        blocker_started.set()
        await release_blocker.wait()
        order.append('blocker_end')

    async def on_target(_: TargetEvent) -> None:
        order.append('target')

    bus.on(BlockerEvent, on_blocker)
    bus.on(TargetEvent, on_target)

    try:
        bus.emit(BlockerEvent())
        await asyncio.wait_for(blocker_started.wait(), timeout=1.0)
        target = bus.emit(TargetEvent())
        target_done = asyncio.create_task(target.wait())
        await asyncio.sleep(0.05)
        assert order == ['blocker_start']
        release_blocker.set()
        assert await asyncio.wait_for(target_done, timeout=1.0) is target
        await bus.wait_until_idle()
        assert order == ['blocker_start', 'blocker_end', 'target']
    finally:
        release_blocker.set()
        await bus.destroy()


async def test_wait_outside_handler_allows_normal_parallel_processing():
    class BlockerEvent(BaseEvent[None]):
        pass

    class TargetEvent(BaseEvent[None]):
        pass

    bus = EventBus(
        name='WaitOutsideHandlerParallelQueueOrderBus',
        event_concurrency='bus-serial',
        event_handler_concurrency='serial',
    )
    order: list[str] = []
    blocker_started = asyncio.Event()
    release_blocker = asyncio.Event()

    async def on_blocker(_: BlockerEvent) -> None:
        order.append('blocker_start')
        blocker_started.set()
        await release_blocker.wait()
        order.append('blocker_end')

    async def on_target(_: TargetEvent) -> None:
        order.append('target')

    bus.on(BlockerEvent, on_blocker)
    bus.on(TargetEvent, on_target)

    try:
        bus.emit(BlockerEvent())
        await asyncio.wait_for(blocker_started.wait(), timeout=1.0)
        target = bus.emit(TargetEvent(event_concurrency=EventConcurrencyMode.PARALLEL))
        target_done = asyncio.create_task(target.wait())
        await asyncio.sleep(0.05)
        assert order == ['blocker_start', 'target']
        release_blocker.set()
        assert await asyncio.wait_for(target_done, timeout=1.0) is target
        await bus.wait_until_idle()
        assert order == ['blocker_start', 'target', 'blocker_end']
    finally:
        release_blocker.set()
        await bus.destroy()


async def test_wait_returns_event_without_forcing_queued_execution():
    class BlockerEvent(BaseEvent[None]):
        pass

    class TargetEvent(BaseEvent[str]):
        pass

    bus = EventBus(name='WaitPassiveQueueOrderBus', event_concurrency='bus-serial', event_handler_concurrency='serial')
    order: list[str] = []
    blocker_started = asyncio.Event()
    release_blocker = asyncio.Event()

    async def on_blocker(_: BlockerEvent) -> None:
        order.append('blocker_start')
        blocker_started.set()
        await release_blocker.wait()
        order.append('blocker_end')

    async def on_target(_: TargetEvent) -> str:
        order.append('target')
        return 'target'

    bus.on(BlockerEvent, on_blocker)
    bus.on(TargetEvent, on_target)

    try:
        bus.emit(BlockerEvent())
        await asyncio.wait_for(blocker_started.wait(), timeout=1.0)
        target = bus.emit(TargetEvent())
        wait_task = asyncio.create_task(target.wait(timeout=1.0))
        await asyncio.sleep(0.05)
        assert order == ['blocker_start']
        release_blocker.set()
        assert await asyncio.wait_for(wait_task, timeout=1.0) is target
        assert order == ['blocker_start', 'blocker_end', 'target']
    finally:
        release_blocker.set()
        await bus.destroy()


async def test_now_returns_event_and_queue_jumps_queued_execution():
    class BlockerEvent(BaseEvent[None]):
        pass

    class TargetEvent(BaseEvent[str]):
        pass

    bus = EventBus(name='NowActiveQueueJumpBus', event_concurrency='bus-serial', event_handler_concurrency='serial')
    order: list[str] = []
    blocker_started = asyncio.Event()
    release_blocker = asyncio.Event()

    async def on_blocker(_: BlockerEvent) -> None:
        order.append('blocker_start')
        blocker_started.set()
        await release_blocker.wait()
        order.append('blocker_end')

    async def on_target(_: TargetEvent) -> str:
        order.append('target')
        return 'target'

    bus.on(BlockerEvent, on_blocker)
    bus.on(TargetEvent, on_target)

    try:
        bus.emit(BlockerEvent())
        await asyncio.wait_for(blocker_started.wait(), timeout=1.0)
        target = bus.emit(TargetEvent())
        now_task = asyncio.create_task(target.now(timeout=1.0))
        await asyncio.sleep(0.05)
        assert order == ['blocker_start', 'target']
        assert await asyncio.wait_for(now_task, timeout=1.0) is target
        release_blocker.set()
        await bus.wait_until_idle(timeout=1.0)
        assert order == ['blocker_start', 'target', 'blocker_end']
    finally:
        release_blocker.set()
        await bus.destroy()


async def test_await_event_is_python_shortcut_for_now():
    class BlockerEvent(BaseEvent[None]):
        pass

    class TargetEvent(BaseEvent[str]):
        pass

    bus = EventBus(name='AwaitEventNowShortcutBus', event_concurrency='bus-serial', event_handler_concurrency='serial')
    order: list[str] = []
    blocker_started = asyncio.Event()
    release_blocker = asyncio.Event()

    async def on_blocker(_: BlockerEvent) -> None:
        order.append('blocker_start')
        blocker_started.set()
        await release_blocker.wait()
        order.append('blocker_end')

    async def on_target(_: TargetEvent) -> str:
        order.append('target')
        return 'target'

    bus.on(BlockerEvent, on_blocker)
    bus.on(TargetEvent, on_target)

    try:
        bus.emit(BlockerEvent())
        await asyncio.wait_for(blocker_started.wait(), timeout=1.0)
        target = bus.emit(TargetEvent())
        await target
        assert order == ['blocker_start', 'target']
        assert await target.event_result() == 'target'
        release_blocker.set()
        await bus.wait_until_idle(timeout=1.0)
        assert order == ['blocker_start', 'target', 'blocker_end']
    finally:
        release_blocker.set()
        await bus.destroy()


async def test_wait_first_result_returns_before_event_completion():
    class FirstResultEvent(BaseEvent[str]):
        pass

    bus = EventBus(
        name='WaitFirstResultBus',
        event_concurrency='parallel',
        event_handler_concurrency='parallel',
        event_timeout=0,
    )
    slow_finished = asyncio.Event()

    async def medium_handler(_: FirstResultEvent) -> str:
        await asyncio.sleep(0.03)
        return 'medium'

    async def fast_handler(_: FirstResultEvent) -> str:
        await asyncio.sleep(0.01)
        return 'fast'

    async def slow_handler(_: FirstResultEvent) -> str:
        await asyncio.sleep(0.25)
        slow_finished.set()
        return 'slow'

    bus.on(FirstResultEvent, medium_handler)
    bus.on(FirstResultEvent, fast_handler)
    bus.on(FirstResultEvent, slow_handler)

    try:
        event = bus.emit(FirstResultEvent(event_concurrency=EventConcurrencyMode.PARALLEL))
        assert await event.wait(timeout=1.0, first_result=True) is event
        assert await event.event_result(raise_if_any=False) == 'fast'
        await asyncio.sleep(0.05)
        assert await event.event_results_list(raise_if_any=False) == ['medium', 'fast']
        assert not slow_finished.is_set()
        assert event.event_status != EventStatus.COMPLETED
        await asyncio.wait_for(slow_finished.wait(), timeout=1.0)
        await bus.wait_until_idle(timeout=1.0)
        assert event.event_status == EventStatus.COMPLETED
    finally:
        await bus.destroy()


async def test_now_first_result_returns_before_event_completion():
    class FirstResultEvent(BaseEvent[str]):
        pass

    bus = EventBus(name='NowFirstResultBus', event_handler_concurrency='parallel', event_timeout=0)
    slow_finished = asyncio.Event()

    async def medium_handler(_: FirstResultEvent) -> str:
        await asyncio.sleep(0.03)
        return 'medium'

    async def fast_handler(_: FirstResultEvent) -> str:
        await asyncio.sleep(0.01)
        return 'fast'

    async def slow_handler(_: FirstResultEvent) -> str:
        await asyncio.sleep(0.25)
        slow_finished.set()
        return 'slow'

    bus.on(FirstResultEvent, medium_handler)
    bus.on(FirstResultEvent, fast_handler)
    bus.on(FirstResultEvent, slow_handler)

    try:
        event = bus.emit(FirstResultEvent(event_concurrency=EventConcurrencyMode.PARALLEL))
        assert await event.now(timeout=1.0, first_result=True) is event
        assert await event.event_result(raise_if_any=False) == 'fast'
        await asyncio.sleep(0.05)
        assert await event.event_results_list(raise_if_any=False) == ['medium', 'fast']
        assert not slow_finished.is_set()
        assert event.event_status != EventStatus.COMPLETED
        await asyncio.wait_for(slow_finished.wait(), timeout=1.0)
        await bus.wait_until_idle(timeout=1.0)
    finally:
        await bus.destroy()


async def test_now_timeout_limits_caller_wait_and_background_processing_continues():
    class TimeoutEvent(BaseEvent[str]):
        pass

    bus = EventBus(name='NowTimeoutCallerWaitBus', event_timeout=0)
    handler_started = asyncio.Event()
    release_handler = asyncio.Event()

    async def handler(_: TimeoutEvent) -> str:
        handler_started.set()
        await release_handler.wait()
        return 'done'

    bus.on(TimeoutEvent, handler)

    try:
        event = bus.emit(TimeoutEvent())
        with pytest.raises(asyncio.TimeoutError):
            await event.now(timeout=0.01)

        assert event.event_status != EventStatus.COMPLETED
        await asyncio.wait_for(handler_started.wait(), timeout=1.0)

        release_handler.set()
        assert await asyncio.wait_for(event.wait(timeout=1.0), timeout=1.0) is event
        assert event.event_status == EventStatus.COMPLETED
        assert await event.event_result() == 'done'
    finally:
        release_handler.set()
        await bus.destroy()


async def test_event_result_starts_never_started_event_and_returns_first_result():
    class BlockerEvent(BaseEvent[None]):
        pass

    class TargetEvent(BaseEvent[str]):
        pass

    bus = EventBus(name='EventResultShortcutQueueJumpBus', event_concurrency='bus-serial', event_handler_concurrency='serial')
    order: list[str] = []
    blocker_started = asyncio.Event()
    release_blocker = asyncio.Event()

    async def on_blocker(_: BlockerEvent) -> None:
        order.append('blocker_start')
        blocker_started.set()
        await release_blocker.wait()
        order.append('blocker_end')

    async def on_target(_: TargetEvent) -> str:
        order.append('target')
        return 'target'

    bus.on(BlockerEvent, on_blocker)
    bus.on(TargetEvent, on_target)

    try:
        bus.emit(BlockerEvent())
        await asyncio.wait_for(blocker_started.wait(), timeout=1.0)
        target = bus.emit(TargetEvent())
        result_task = asyncio.create_task(target.event_result())
        await asyncio.sleep(0.05)
        assert order == ['blocker_start', 'target']
        assert await asyncio.wait_for(result_task, timeout=1.0) == 'target'
        release_blocker.set()
        await bus.wait_until_idle(timeout=1.0)
    finally:
        release_blocker.set()
        await bus.destroy()


async def test_event_results_list_starts_never_started_event_and_returns_all_results():
    class BlockerEvent(BaseEvent[None]):
        pass

    class TargetEvent(BaseEvent[str]):
        pass

    bus = EventBus(name='EventResultsShortcutQueueJumpBus', event_concurrency='bus-serial', event_handler_concurrency='serial')
    order: list[str] = []
    blocker_started = asyncio.Event()
    release_blocker = asyncio.Event()

    async def on_blocker(_: BlockerEvent) -> None:
        order.append('blocker_start')
        blocker_started.set()
        await release_blocker.wait()
        order.append('blocker_end')

    async def first_handler(_: TargetEvent) -> str:
        order.append('first')
        return 'first'

    async def second_handler(_: TargetEvent) -> str:
        order.append('second')
        return 'second'

    bus.on(BlockerEvent, on_blocker)
    bus.on(TargetEvent, first_handler)
    bus.on(TargetEvent, second_handler)

    try:
        bus.emit(BlockerEvent())
        await asyncio.wait_for(blocker_started.wait(), timeout=1.0)
        target = bus.emit(TargetEvent())
        results_task = asyncio.create_task(target.event_results_list())
        await asyncio.sleep(0.05)
        assert order == ['blocker_start', 'first', 'second']
        assert await asyncio.wait_for(results_task, timeout=1.0) == ['first', 'second']
        assert isinstance(target.event_results, dict)
        assert len(target.event_results) == 2
        assert [result.result for result in target.event_results.values()] == ['first', 'second']
        release_blocker.set()
        await bus.wait_until_idle(timeout=1.0)
    finally:
        release_blocker.set()
        await bus.destroy()


async def test_event_result_helpers_do_not_wait_for_started_event():
    class StartedEvent(BaseEvent[str]):
        pass

    bus = EventBus(
        name='EventResultHelpersStartedBus',
        event_concurrency='parallel',
        event_handler_concurrency='parallel',
        event_timeout=0,
    )
    handler_started = asyncio.Event()
    release_handler = asyncio.Event()

    async def slow_handler(_: StartedEvent) -> str:
        handler_started.set()
        await release_handler.wait()
        return 'late'

    bus.on(StartedEvent, slow_handler)

    try:
        event = bus.emit(StartedEvent())
        await asyncio.wait_for(handler_started.wait(), timeout=1.0)

        assert event.event_status == EventStatus.STARTED
        assert await asyncio.wait_for(event.event_result(raise_if_none=False), timeout=0.05) is None
        assert await asyncio.wait_for(event.event_results_list(raise_if_none=False), timeout=0.05) == []
        assert event.event_status == EventStatus.STARTED

        release_handler.set()
        await bus.wait_until_idle(timeout=1.0)
    finally:
        release_handler.set()
        await bus.destroy()


async def test_now_on_already_executing_event_waits_without_duplicate_execution():
    class ExecutingEvent(BaseEvent[str]):
        pass

    bus = EventBus(name='NowAlreadyExecutingBus', event_handler_concurrency='serial', event_timeout=0)
    started = asyncio.Event()
    release = asyncio.Event()
    run_count = 0

    async def handler(_: ExecutingEvent) -> str:
        nonlocal run_count
        run_count += 1
        started.set()
        await release.wait()
        return 'done'

    bus.on(ExecutingEvent, handler)

    try:
        event = bus.emit(ExecutingEvent())
        await asyncio.wait_for(started.wait(), timeout=1.0)
        now_task = asyncio.create_task(event.now(timeout=1.0))
        await asyncio.sleep(0.05)
        assert run_count == 1
        release.set()
        assert await asyncio.wait_for(now_task, timeout=1.0) is event
        assert await event.event_result() == 'done'
        assert run_count == 1
    finally:
        release.set()
        await bus.destroy()


async def test_now_with_rapid_handler_churn_does_not_duplicate_execution():
    class ChurnEvent(BaseEvent[str]):
        pass

    total_events = 200
    bus = EventBus(name='NowRapidHandlerChurnBus', event_timeout=0, max_history_size=512, max_history_drop=True)
    run_count = 0

    try:
        for _ in range(total_events):

            async def handler(_: ChurnEvent) -> str:
                nonlocal run_count
                run_count += 1
                await asyncio.sleep(0)
                return 'done'

            registered_handler = bus.on(ChurnEvent, handler)
            event = bus.emit(ChurnEvent())
            assert await event.now(timeout=1.0) is event
            await asyncio.sleep(0)
            await bus.wait_until_idle(timeout=1.0)
            bus.off(ChurnEvent, registered_handler)

        assert run_count == total_events
    finally:
        await bus.destroy()


async def test_event_result_options_apply_to_current_results():
    class ResultOptionsEvent(BaseEvent[str]):
        pass

    bus = EventBus(name='EventResultOptionsCurrentResultsBus', event_handler_concurrency='parallel', event_timeout=0)
    release_slow = asyncio.Event()

    async def fail_handler(_: ResultOptionsEvent) -> str:
        raise RuntimeError('option boom')

    async def keep_handler(_: ResultOptionsEvent) -> str:
        await asyncio.sleep(0.01)
        return 'keep'

    async def slow_handler(_: ResultOptionsEvent) -> str:
        await release_slow.wait()
        return 'late'

    bus.on(ResultOptionsEvent, fail_handler)
    bus.on(ResultOptionsEvent, keep_handler)
    bus.on(ResultOptionsEvent, slow_handler)

    try:
        event = await bus.emit(ResultOptionsEvent()).now(timeout=1.0, first_result=True)
        assert await event.event_result(raise_if_any=False) == 'keep'
        with pytest.raises(RuntimeError, match='option boom'):
            await event.event_result(raise_if_any=True)
        assert (
            await event.event_results_list(
                include=lambda result, _event_result: result == 'missing',
                raise_if_any=False,
                raise_if_none=False,
            )
            == []
        )
    finally:
        release_slow.set()
        await bus.destroy()


async def test_event_result_raises_processing_error_group_after_completion():
    class ErrorEvent(BaseEvent[None]):
        pass

    bus = EventBus(
        name='BaseEventNowRaisesFirstErrorBus',
        event_handler_concurrency='parallel',
        event_timeout=0,
    )

    async def first_handler(_: ErrorEvent) -> None:
        await asyncio.sleep(0.001)
        raise ValueError('first failure')

    async def second_handler(_: ErrorEvent) -> None:
        await asyncio.sleep(0.01)
        raise RuntimeError('second failure')

    bus.on(ErrorEvent, first_handler)
    bus.on(ErrorEvent, second_handler)

    event = bus.emit(ErrorEvent())
    await event.now()
    with pytest.raises(ExceptionGroup, match='had 2 handler error'):
        await event.event_result()

    assert event.event_status == 'completed'
    assert len(event.event_results) == 2
    assert all(result.status == 'error' for result in event.event_results.values())
    assert await event.event_result(raise_if_any=False, raise_if_none=False) is None
    await bus.destroy()


async def test_base_event_now_outside_handler_no_args():
    class ErrorEvent(BaseEvent[None]):
        pass

    bus = EventBus(name='BaseEventNowArgsOutsideBus')

    async def handler(_: ErrorEvent) -> None:
        raise ValueError('outside suppressed failure')

    bus.on(ErrorEvent, handler)

    event = await bus.emit(ErrorEvent()).now()

    assert event.event_status == 'completed'
    assert any(result.status == 'error' for result in event.event_results.values())
    await bus.destroy()


async def test_base_event_now_outside_handler_with_args():
    class ErrorEvent(BaseEvent[None]):
        pass

    bus = EventBus(name='BaseEventNowArgsOutsideBus')

    async def handler(_: ErrorEvent) -> None:
        raise ValueError('outside suppressed failure')

    bus.on(ErrorEvent, handler)

    event = await bus.emit(ErrorEvent()).now(timeout=1.0)

    assert event.event_status == 'completed'
    assert any(result.status == 'error' for result in event.event_results.values())
    assert await event.event_result(raise_if_any=False, raise_if_none=False) is None
    await bus.destroy()


async def test_parallel_event_concurrency_plus_immediate_execution_races_child_events_inside_handlers():
    class ParentEvent(BaseEvent[None]):
        pass

    class SomeChildEvent1(BaseEvent[str]):
        pass

    class SomeChildEvent2(BaseEvent[str]):
        pass

    class SomeChildEvent3(BaseEvent[str]):
        pass

    bus = EventBus(
        name='ParallelImmediateRaceBus',
        event_concurrency='parallel',
        event_handler_concurrency='serial',
    )
    order: list[str] = []
    release = asyncio.Event()
    all_started = asyncio.Event()
    in_flight = 0
    max_in_flight = 0

    async def track_child(label: str) -> str:
        nonlocal in_flight, max_in_flight
        order.append(f'{label}_start')
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        if in_flight == 3:
            all_started.set()
        await release.wait()
        order.append(f'{label}_end')
        in_flight -= 1
        return label

    async def on_parent(event: ParentEvent) -> None:
        order.append('parent_start')
        settled = await asyncio.gather(
            event.emit(SomeChildEvent1()),
            event.emit(SomeChildEvent2()),
            event.emit(SomeChildEvent3()),
            return_exceptions=True,
        )
        order.append('parent_end')
        assert len(settled) == 3
        assert all(isinstance(item, BaseEvent) for item in settled)

    async def on_child_1(_: SomeChildEvent1) -> str:
        return await track_child('child1')

    async def on_child_2(_: SomeChildEvent2) -> str:
        return await track_child('child2')

    async def on_child_3(_: SomeChildEvent3) -> str:
        return await track_child('child3')

    bus.on(ParentEvent, on_parent)
    bus.on(SomeChildEvent1, on_child_1)
    bus.on(SomeChildEvent2, on_child_2)
    bus.on(SomeChildEvent3, on_child_3)

    parent = bus.emit(ParentEvent())
    await asyncio.wait_for(all_started.wait(), timeout=1.0)
    assert max_in_flight >= 3
    assert 'parent_end' not in order

    release.set()
    await parent
    await bus.wait_until_idle()

    parent_end_index = order.index('parent_end')
    for label in ('child1', 'child2', 'child3'):
        assert order.index(f'{label}_start') < parent_end_index
        assert order.index(f'{label}_end') < parent_end_index

    await bus.destroy()


async def test_awaited_parallel_queue_jump_child_does_not_pause_later_parallel_child_events():
    class ParentEvent(BaseEvent[None]):
        pass

    class ChildEvent(BaseEvent[str]):
        name: str

    class ObservedEvent(BaseEvent[None]):
        name: str

    bus = EventBus(
        name='ParallelQueueJumpDoesNotPauseBus',
        event_concurrency='bus-serial',
        event_handler_concurrency='serial',
    )
    log: list[str] = []

    async def on_parent(event: ParentEvent) -> None:
        log.append('parent_start')
        child = await event.emit(ChildEvent(name='awaited', event_concurrency=EventConcurrencyMode.PARALLEL)).now(
            first_result=True
        )
        assert await child.event_result(raise_if_any=False) == 'awaited'
        log.append('parent_after_awaited')

        event.emit(ChildEvent(name='bg', event_concurrency=EventConcurrencyMode.PARALLEL))
        log.append('parent_after_bg_emit')
        found = await bus.find(
            ObservedEvent,
            lambda candidate: candidate.name == 'bg',
            past=True,
            future=0.2,
        )
        log.append(f'parent_found_{found is not None}')
        assert found is not None, f'background parallel child should run while parent handler is waiting: {log}'

    async def on_child(event: ChildEvent) -> str:
        log.append(f'child_start_{event.name}')
        if event.name == 'bg':
            event.emit(ObservedEvent(name='bg'))
        log.append(f'child_end_{event.name}')
        return event.name

    async def on_observed(_: ObservedEvent) -> None:
        log.append('observed_seen')

    bus.on(ParentEvent, on_parent)
    bus.on(ChildEvent, on_child)
    bus.on(ObservedEvent, on_observed)

    await bus.emit(ParentEvent(event_timeout=0))
    await bus.wait_until_idle()

    assert log.index('child_start_bg') < log.index('parent_found_True'), log
    await bus.destroy()


async def test_wait_waits_in_queue_order_inside_handler():
    class ParentEvent(BaseEvent[None]):
        pass

    class ChildEvent(BaseEvent[None]):
        pass

    class SiblingEvent(BaseEvent[None]):
        pass

    bus = EventBus(
        name='QueueOrderEventCompletedBus',
        event_concurrency='parallel',
        event_handler_concurrency='parallel',
    )
    order: list[str] = []

    async def on_parent(event: ParentEvent) -> None:
        order.append('parent_start')
        # Explicitly detached: this sibling should keep normal queue order.
        event.bus.emit(SiblingEvent())
        child = event.emit(ChildEvent())
        await child.wait()
        order.append('parent_end')

    async def on_child(_: ChildEvent) -> None:
        order.append('child_start')
        await asyncio.sleep(0.001)
        order.append('child_end')

    async def on_sibling(_: SiblingEvent) -> None:
        order.append('sibling_start')
        await asyncio.sleep(0.001)
        order.append('sibling_end')

    bus.on(ParentEvent, on_parent)
    bus.on(ChildEvent, on_child)
    bus.on(SiblingEvent, on_sibling)

    await bus.emit(ParentEvent())
    await bus.wait_until_idle()
    assert order.index('sibling_start') < order.index('child_start')
    assert order.index('child_end') < order.index('parent_end')
    await bus.destroy()


async def test_wait_is_passive_inside_handlers_and_times_out_for_serial_events():
    class ParentEvent(BaseEvent[None]):
        pass

    class SerialEmittedEvent(BaseEvent[None]):
        pass

    class SerialFoundEvent(BaseEvent[None]):
        pass

    bus = EventBus(name='PassiveSerialEventCompletedBus', event_concurrency='bus-serial')
    order: list[str] = []

    async def on_parent(event: ParentEvent) -> None:
        order.append('parent_start')
        emitted = event.emit(SerialEmittedEvent())
        found_source = event.emit(SerialFoundEvent())
        found = await bus.find(SerialFoundEvent, past=True, future=False)
        assert found is found_source
        assert found is not None

        with pytest.raises(TimeoutError):
            await emitted.wait(timeout=0.02)
        order.append('emitted_timeout')
        with pytest.raises(TimeoutError):
            await found.wait(timeout=0.02)
        order.append('found_timeout')
        assert 'emitted_start' not in order
        assert 'found_start' not in order
        assert not emitted.event_blocks_parent_completion
        assert not found.event_blocks_parent_completion
        order.append('parent_end')

    async def on_emitted(_: SerialEmittedEvent) -> None:
        order.append('emitted_start')

    async def on_found(_: SerialFoundEvent) -> None:
        order.append('found_start')

    bus.on(ParentEvent, on_parent)
    bus.on(SerialEmittedEvent, on_emitted)
    bus.on(SerialFoundEvent, on_found)

    await bus.emit(ParentEvent()).now()
    await bus.wait_until_idle()
    assert order == ['parent_start', 'emitted_timeout', 'found_timeout', 'parent_end', 'emitted_start', 'found_start']
    await bus.destroy()


async def test_wait_serial_wait_inside_handler_times_out_and_warns_about_slow_handler(
    caplog: pytest.LogCaptureFixture,
):
    class ParentEvent(BaseEvent[None]):
        pass

    class SerialChildEvent(BaseEvent[None]):
        pass

    caplog.set_level(logging.WARNING, logger='abxbus')
    bus = EventBus(
        name='EventCompletedSerialDeadlockWarningBus',
        event_concurrency='bus-serial',
        event_slow_timeout=0,
        event_handler_slow_timeout=0.01,
    )
    order: list[str] = []

    async def on_parent(event: ParentEvent) -> None:
        order.append('parent_start')
        child = event.emit(SerialChildEvent())
        found = await bus.find(SerialChildEvent, past=True, future=False)
        assert found is child
        assert found is not None
        with pytest.raises(TimeoutError):
            await found.wait(timeout=0.05)
        order.append('child_timeout')
        assert 'child_start' not in order
        assert not found.event_blocks_parent_completion
        order.append('parent_end')

    async def on_child(_: SerialChildEvent) -> None:
        order.append('child_start')

    bus.on(ParentEvent, on_parent)
    bus.on(SerialChildEvent, on_child)

    await bus.emit(ParentEvent()).now()
    await bus.wait_until_idle()
    assert order == ['parent_start', 'child_timeout', 'parent_end', 'child_start']
    assert any('Slow event handler:' in record.message for record in caplog.records)
    await bus.destroy()


async def test_deferred_emit_after_handler_completion_is_accepted():
    class ParentEvent(BaseEvent[None]):
        pass

    class DeferredChildEvent(BaseEvent[None]):
        pass

    bus = EventBus(name='DeferredEmitAfterCompletionBus', event_concurrency='bus-serial')
    order: list[str] = []
    emitted = asyncio.Event()

    async def on_parent(event: ParentEvent) -> None:
        order.append('parent_start')

        async def emit_after_completion() -> None:
            await asyncio.sleep(0.02)
            order.append('deferred_emit')
            event.emit(DeferredChildEvent())
            emitted.set()

        asyncio.create_task(emit_after_completion())
        order.append('parent_end')

    async def on_child(_: DeferredChildEvent) -> None:
        order.append('child_start')

    bus.on(ParentEvent, on_parent)
    bus.on(DeferredChildEvent, on_child)

    await bus.emit(ParentEvent()).now()
    await asyncio.wait_for(emitted.wait(), timeout=1)
    await bus.wait_until_idle(timeout=1)
    assert order == ['parent_start', 'parent_end', 'deferred_emit', 'child_start']
    await bus.destroy()


async def test_wait_waits_for_normal_parallel_processing_inside_handlers():
    class ParentEvent(BaseEvent[None]):
        pass

    class ParallelEmittedEvent(BaseEvent[None]):
        pass

    class ParallelFoundEvent(BaseEvent[None]):
        pass

    bus = EventBus(name='PassiveParallelEventCompletedBus', event_concurrency='bus-serial')
    order: list[str] = []

    async def on_parent(event: ParentEvent) -> None:
        order.append('parent_start')
        emitted = event.emit(ParallelEmittedEvent(event_concurrency=EventConcurrencyMode.PARALLEL))
        found_source = event.emit(ParallelFoundEvent(event_concurrency=EventConcurrencyMode.PARALLEL))
        found = await bus.find(ParallelFoundEvent, past=True, future=False)
        assert found is found_source
        assert found is not None

        await emitted.wait(timeout=1)
        order.append('emitted_completed')
        await found.wait(timeout=1)
        order.append('found_completed')
        assert not emitted.event_blocks_parent_completion
        assert not found.event_blocks_parent_completion
        order.append('parent_end')

    async def on_emitted(_: ParallelEmittedEvent) -> None:
        order.append('emitted_start')
        await asyncio.sleep(0.001)
        order.append('emitted_end')

    async def on_found(_: ParallelFoundEvent) -> None:
        order.append('found_start')
        await asyncio.sleep(0.001)
        order.append('found_end')

    bus.on(ParentEvent, on_parent)
    bus.on(ParallelEmittedEvent, on_emitted)
    bus.on(ParallelFoundEvent, on_found)

    await bus.emit(ParentEvent()).now()
    await bus.wait_until_idle()
    assert order.index('emitted_end') < order.index('emitted_completed')
    assert order.index('found_end') < order.index('found_completed')
    assert order[-1] == 'parent_end'
    await bus.destroy()


async def test_wait_waits_for_future_parallel_event_found_after_handler_starts():
    class SomeOtherEvent(BaseEvent[None]):
        pass

    class ParallelEvent(BaseEvent[None]):
        pass

    bus = EventBus(name='FutureParallelEventCompletedBus', event_concurrency='bus-serial')
    other_started = asyncio.Event()
    release_find = asyncio.Event()
    parallel_started = asyncio.Event()
    continued = asyncio.Event()
    waited_for: list[float] = []

    async def on_other(_: SomeOtherEvent) -> None:
        other_started.set()
        await release_find.wait()
        found = await bus.find(ParallelEvent, past=True, future=False)
        assert found is not None
        started_at = time.perf_counter()
        await found.wait(timeout=1)
        waited_for.append(time.perf_counter() - started_at)
        continued.set()

    async def on_parallel(_: ParallelEvent) -> None:
        parallel_started.set()
        await asyncio.sleep(0.25)

    bus.on(SomeOtherEvent, on_other)
    bus.on(ParallelEvent, on_parallel)

    other = bus.emit(SomeOtherEvent())
    await other_started.wait()
    bus.emit(ParallelEvent(event_concurrency=EventConcurrencyMode.PARALLEL))
    await parallel_started.wait()
    release_find.set()
    await asyncio.wait_for(continued.wait(), timeout=1)
    await other.now()
    await bus.wait_until_idle()
    assert waited_for and waited_for[0] >= 0.15
    await bus.destroy()


async def test_wait_returns_event_accepts_timeout_and_rejects_unattached_pending_event():
    with pytest.raises(RuntimeError, match='no bus attached'):
        await BaseEvent().wait(timeout=0.01)

    completed = BaseEvent(event_status=EventStatus.COMPLETED)
    assert await completed.wait(timeout=0.01) is completed

    class SlowEvent(BaseEvent[None]):
        pass

    bus = EventBus(name='EventCompletedTimeoutBus', event_concurrency='bus-serial')
    release_handler = asyncio.Event()

    async def on_slow(_: SlowEvent) -> None:
        await release_handler.wait()

    bus.on(SlowEvent, on_slow)
    try:
        event = bus.emit(SlowEvent())
        with pytest.raises(TimeoutError):
            await event.wait(timeout=0.01)
        release_handler.set()
        assert await event.wait(timeout=1.0) is event
    finally:
        release_handler.set()
        await bus.destroy()


async def test_reserved_runtime_fields_are_rejected():
    with pytest.raises(ValidationError, match='Field "bus" is reserved'):
        MainEvent.model_validate({'bus': 'payload_bus_field'})
    with pytest.raises(ValidationError, match='Field "now" is reserved'):
        MainEvent.model_validate({'now': 'payload_now_field'})
    with pytest.raises(ValidationError, match='Field "wait" is reserved'):
        MainEvent.model_validate({'wait': 'payload_wait_field'})
    with pytest.raises(ValidationError, match='Field "toString" is reserved'):
        MainEvent.model_validate({'toString': 'payload_to_string_field'})
    with pytest.raises(ValidationError, match='Field "toJSON" is reserved'):
        MainEvent.model_validate({'toJSON': 'payload_to_json_field'})
    with pytest.raises(ValidationError, match='Field "fromJSON" is reserved'):
        MainEvent.model_validate({'fromJSON': 'payload_from_json_field'})


async def test_unknown_event_prefixed_field_rejected_in_payload():
    with pytest.raises(ValidationError, match='starts with "event_" but is not a recognized BaseEvent field'):
        MainEvent.model_validate({'event_some_field_we_dont_recognize': 123})


async def test_model_prefixed_field_rejected_in_payload():
    with pytest.raises(ValidationError, match='starts with "model_" and is reserved for Pydantic model internals'):
        MainEvent.model_validate({'model_something_random': 123})


async def test_builtin_event_prefixed_override_is_allowed():
    class AllowedTimeoutOverrideEvent(BaseEvent[None]):
        event_timeout: float | None = 234234
        event_slow_timeout: float | None = 12

    event = AllowedTimeoutOverrideEvent()
    assert event.event_timeout == 234234
    assert event.event_slow_timeout == 12


async def test_event_at_fields_are_recognized():
    event = MainEvent.model_validate(
        {
            'event_created_at': '2025-01-02T03:04:05.678901234Z',
            'event_started_at': '2025-01-02T03:04:06.100000000Z',
            'event_completed_at': '2025-01-02T03:04:07.200000000Z',
            'event_slow_timeout': 1.5,
            'event_emitted_by_handler_id': '018f8e40-1234-7000-8000-000000000301',
            'event_pending_bus_count': 2,
        }
    )
    assert event.event_created_at == '2025-01-02T03:04:05.678901234Z'
    assert event.event_started_at == '2025-01-02T03:04:06.100000000Z'
    assert event.event_completed_at == '2025-01-02T03:04:07.200000000Z'
    assert event.event_slow_timeout == 1.5
    assert event.event_emitted_by_handler_id == '018f8e40-1234-7000-8000-000000000301'
    assert event.event_pending_bus_count == 2


async def test_event_result_update_creates_and_updates_typed_handler_results():
    class EventResultUpdateEvent(BaseEvent[str]):
        pass

    bus = EventBus(name='BaseEventEventResultUpdateBus')
    event = EventResultUpdateEvent()
    handler_entry = bus.on(EventResultUpdateEvent, lambda _: 'ok')

    pending = event.event_result_update(handler_entry, eventbus=bus, status='pending')
    assert event.event_results[handler_entry.id] is pending
    assert pending.status == 'pending'

    completed = event.event_result_update(handler_entry, eventbus=bus, status='completed', result='seeded')
    assert completed is pending
    assert completed.status == 'completed'
    assert completed.result == 'seeded'

    await bus.destroy()


async def test_event_result_update_status_only_preserves_existing_error_and_result():
    class EventResultUpdateStatusOnlyEvent(BaseEvent[str]):
        pass

    bus = EventBus(name='BaseEventEventResultUpdateStatusOnlyBus')

    def handler(_: EventResultUpdateStatusOnlyEvent) -> str:
        return 'ok'

    handler_entry = bus.on(EventResultUpdateStatusOnlyEvent, handler)
    event = EventResultUpdateStatusOnlyEvent()

    errored = event.event_result_update(handler_entry, eventbus=bus, error=RuntimeError('seeded error'))
    assert errored.status == 'error'
    assert isinstance(errored.error, RuntimeError)

    status_only = event.event_result_update(handler_entry, eventbus=bus, status='pending')
    assert status_only.status == 'pending'
    assert isinstance(status_only.error, RuntimeError)
    assert status_only.result is None

    await bus.destroy()


async def test_python_serialized_at_fields_are_strings():
    bus = EventBus(name='TsIntBus')

    class TsIntEvent(BaseEvent[str]):
        value: str = 'ok'

    bus.on(TsIntEvent, lambda _event: 'done')
    event = await bus.emit(TsIntEvent(value='hello'))

    payload = event.model_dump(mode='json')
    assert isinstance(payload['event_created_at'], str)
    assert isinstance(payload['event_started_at'], str)
    assert isinstance(payload['event_completed_at'], str)
    assert payload['event_created_at'].endswith('Z')
    assert payload['event_started_at'].endswith('Z')
    assert payload['event_completed_at'].endswith('Z')

    first_result = next(iter(event.event_results.values()))
    result_payload = first_result.model_dump(mode='json')
    assert isinstance(result_payload['handler_registered_at'], str)
    assert result_payload['handler_registered_at'].endswith('Z')

    await bus.destroy()


async def test_builtin_model_prefixed_override_is_allowed():
    class AllowedModelConfigOverrideEvent(BaseEvent[None]):
        model_config = BaseEvent.model_config | {'title': 'AllowedModelConfigOverrideEvent'}

    event = AllowedModelConfigOverrideEvent()
    assert event.event_type == 'AllowedModelConfigOverrideEvent'


async def test_event_bus_property_single_bus():
    """Test bus property with a single EventBus instance"""
    bus = EventBus(name='TestBus')

    # Track if handler was called
    handler_called = False
    dispatched_child = None

    async def handler(event: MainEvent):
        nonlocal handler_called, dispatched_child
        handler_called = True

        # Should be able to access event_bus inside handler
        assert event.bus == bus
        assert event.bus.name == 'TestBus'

        # Should be able to dispatch child events from the current event.
        dispatched_child = await event.emit(ChildEvent())

    bus.on(MainEvent, handler)

    # Dispatch event and wait for completion
    await bus.emit(MainEvent())

    assert handler_called
    assert dispatched_child is not None
    assert isinstance(dispatched_child, ChildEvent)

    await bus.destroy()


async def test_event_bus_property_multiple_buses():
    """Test bus property with multiple EventBus instances"""
    bus1 = EventBus(name='Bus1')
    bus2 = EventBus(name='Bus2')

    handler1_called = False
    handler2_called = False

    async def handler1(event: MainEvent):
        nonlocal handler1_called
        handler1_called = True
        # Inside bus1 handler, event_bus should return bus1
        assert event.bus == bus1
        assert event.bus.name == 'Bus1'

    async def handler2(event: MainEvent):
        nonlocal handler2_called
        handler2_called = True
        # Inside bus2 handler, event_bus should return bus2
        assert event.bus == bus2
        assert event.bus.name == 'Bus2'

    bus1.on(MainEvent, handler1)
    bus2.on(MainEvent, handler2)

    # Dispatch to bus1
    await bus1.emit(MainEvent(message='bus1'))
    assert handler1_called

    # Dispatch to bus2
    await bus2.emit(MainEvent(message='bus2'))
    assert handler2_called

    await bus1.destroy()
    await bus2.destroy()


async def test_event_bus_property_with_forwarding():
    """Test bus property with event forwarding between buses"""
    bus1 = EventBus(name='Bus1')
    bus2 = EventBus(name='Bus2')

    # Forward all events from bus1 to bus2
    bus1.on('*', bus2.emit)

    handler_bus = None
    handler_complete = asyncio.Event()

    async def handler(event: MainEvent):
        nonlocal handler_bus
        # When forwarded, the event_bus should be the bus currently processing
        handler_bus = event.bus
        handler_complete.set()

    bus2.on(MainEvent, handler)

    # Dispatch to bus1, which forwards to bus2
    event = bus1.emit(MainEvent())

    # Wait for handler to complete
    await handler_complete.wait()

    # The handler in bus2 should see bus2 as the event_bus
    assert handler_bus is not None
    assert handler_bus.name == 'Bus2'
    # Verify it's the same bus instance (they should be the same object)
    assert handler_bus is bus2

    # Also wait for the event to fully complete
    await event

    await bus1.destroy()
    await bus2.destroy()


async def test_event_bus_property_outside_handler():
    """Test that bus property raises error when accessed outside handler"""
    bus = EventBus(name='TestBus')

    event = MainEvent()

    # Should raise error when accessed outside handler context
    with pytest.raises(AttributeError, match='bus property can only be accessed from within an event handler'):
        _ = event.bus

    # Even after dispatching, accessing outside handler should fail
    dispatched_event = await bus.emit(event)

    with pytest.raises(AttributeError, match='bus property can only be accessed from within an event handler'):
        _ = dispatched_event.bus

    await bus.destroy()


async def test_event_bus_property_nested_handlers():
    """Test bus property in nested handler scenarios"""
    bus = EventBus(name='MainBus')

    inner_bus_name = None

    async def outer_handler(event: MainEvent):
        # Dispatch a child event from within handler
        child = ChildEvent()

        async def inner_handler(child_event: ChildEvent):
            nonlocal inner_bus_name
            # Both parent and child should see the same bus
            assert child_event.bus == event.bus
            inner_bus_name = child_event.bus.name

        bus.on(ChildEvent, inner_handler)
        await event.emit(child)

    bus.on(MainEvent, outer_handler)

    await bus.emit(MainEvent())

    assert inner_bus_name == 'MainBus'

    await bus.destroy()


async def test_event_bus_property_no_active_bus():
    """Test bus property when EventBus has been garbage collected"""
    # This is a tricky edge case - create and destroy a bus

    event = None

    async def create_and_dispatch():
        nonlocal event
        bus = EventBus(name='TempBus')

        async def handler(e: MainEvent):
            # Save the event for later
            nonlocal event
            event = e

        bus.on(MainEvent, handler)
        await bus.emit(MainEvent())
        await bus.destroy()
        # Bus goes out of scope here and may be garbage collected

    await create_and_dispatch()

    # Force garbage collection
    import gc

    gc.collect()

    # Event exists but bus might be gone
    assert event is not None

    # Create a new handler context to test
    new_bus = EventBus(name='NewBus')

    error_raised = False

    async def new_handler(e: MainEvent):
        nonlocal error_raised
        assert event is not None
        try:
            # The old event doesn't belong to this bus
            _ = event.bus
        except RuntimeError:
            error_raised = True

    new_bus.on(MainEvent, new_handler)
    await new_bus.emit(MainEvent())

    # Should have raised an error since the original bus is gone
    assert error_raised

    await new_bus.destroy()


async def test_event_bus_property_child_dispatch():
    """Test bus property when dispatching child events from handlers"""
    bus = EventBus(name='MainBus')

    # Track execution order and bus references
    execution_order: list[str] = []
    child_event_ref = None
    grandchild_event_ref = None

    async def parent_handler(event: MainEvent):
        execution_order.append('parent_start')

        # Verify we can access event_bus
        assert event.bus == bus
        assert event.bus.name == 'MainBus'

        # Dispatch an owned child event using event.emit.
        nonlocal child_event_ref
        child_event_ref = event.emit(ChildEvent(data='from_parent'))

        # Python-only `await event` uses the same immediate path as event.now().
        await child_event_ref

        execution_order.append('parent_end')

    async def child_handler(event: ChildEvent):
        execution_order.append('child_start')

        # Child should see the same bus
        assert event.bus == bus
        assert event.bus.name == 'MainBus'
        assert event.data == 'from_parent'

        # Dispatch a grandchild event
        nonlocal grandchild_event_ref
        grandchild_event_ref = event.emit(GrandchildEvent(info='from_child'))

        # Wait for grandchild to complete
        await grandchild_event_ref

        execution_order.append('child_end')

    async def grandchild_handler(event: GrandchildEvent):
        execution_order.append('grandchild_start')

        # Grandchild should also see the same bus
        assert event.bus == bus
        assert event.bus.name == 'MainBus'
        assert event.info == 'from_child'

        execution_order.append('grandchild_end')

    # Register handlers
    bus.on(MainEvent, parent_handler)
    bus.on(ChildEvent, child_handler)
    bus.on(GrandchildEvent, grandchild_handler)

    # Dispatch the parent event
    parent_event = await bus.emit(MainEvent(message='start'))

    # Verify execution order - child events should complete before parent
    assert execution_order == ['parent_start', 'child_start', 'grandchild_start', 'grandchild_end', 'child_end', 'parent_end']

    # Verify all events completed
    assert parent_event.event_status == 'completed'
    assert child_event_ref is not None
    assert child_event_ref.event_status == 'completed'
    assert grandchild_event_ref is not None
    assert grandchild_event_ref.event_status == 'completed'

    # Verify parent-child relationships
    assert child_event_ref.event_parent_id == parent_event.event_id
    assert grandchild_event_ref.event_parent_id == child_event_ref.event_id

    await bus.destroy()


async def test_event_bus_property_multi_bus_child_dispatch():
    """Test bus property when child events are dispatched across multiple buses"""
    bus1 = EventBus(name='Bus1')
    bus2 = EventBus(name='Bus2')

    # Forward all events from bus1 to bus2
    bus1.on('*', bus2.emit)

    child_dispatch_bus = None
    child_handler_bus = None
    handlers_complete = asyncio.Event()

    async def parent_handler(event: MainEvent):
        # This handler runs in bus2 (due to forwarding)
        assert event.bus == bus2

        # Dispatch child using event.emit (should dispatch to bus2).
        nonlocal child_dispatch_bus
        child_dispatch_bus = event.bus
        await event.emit(ChildEvent(data='from_bus2_handler'))

    async def child_handler(event: ChildEvent):
        # Child handler should see bus2 as well
        nonlocal child_handler_bus
        child_handler_bus = event.bus
        assert event.data == 'from_bus2_handler'
        handlers_complete.set()

    # Only register handlers on bus2
    bus2.on(MainEvent, parent_handler)
    bus2.on(ChildEvent, child_handler)

    # Dispatch to bus1, which forwards to bus2
    parent_event = bus1.emit(MainEvent(message='start'))

    # Wait for handlers to complete
    await asyncio.wait_for(handlers_complete.wait(), timeout=5.0)

    # Also await the parent event
    await parent_event

    # Verify child was dispatched to bus2
    assert child_dispatch_bus is not None
    assert child_handler_bus is not None
    assert id(child_dispatch_bus) == id(bus2)
    assert id(child_handler_bus) == id(bus2)

    await bus1.destroy()
    await bus2.destroy()


# Folded from test_base_event_runtime_state.py to keep test layout class-based.
"""Test that the AttributeError bug related to 'event_completed_at' is fixed"""

from contextlib import suppress

from abxbus import BaseEvent
from abxbus.helpers import monotonic_datetime


class SampleEvent(BaseEvent[str]):
    data: str = 'test'


def _noop_handler(_event: SampleEvent) -> None:
    return


def test_event_started_at_with_deserialized_event():
    """Test that event_started_at works even with events created through deserialization"""
    # Create an event and convert to dict (simulating serialization)
    event = SampleEvent(data='original')
    event_dict = event.model_dump()

    # Create a new event from the dict (simulating deserialization)
    deserialized_event = SampleEvent.model_validate(event_dict)

    # This should not raise AttributeError
    assert deserialized_event.event_started_at is None
    assert deserialized_event.event_completed_at is None


def test_event_started_at_with_json_deserialization():
    """Test that event_started_at works with JSON deserialization"""
    # Create an event and convert to JSON
    event = SampleEvent(data='json_test')
    json_str = event.model_dump_json()

    # Create a new event from JSON
    deserialized_event = SampleEvent.model_validate_json(json_str)

    # This should not raise AttributeError
    assert deserialized_event.event_started_at is None
    assert deserialized_event.event_completed_at is None


async def test_event_started_at_after_processing():
    """Test that event_started_at works correctly after event processing"""
    bus = EventBus(name='TestBus')

    # Handler that does nothing
    async def test_handler(event: SampleEvent) -> str:
        await asyncio.sleep(0.01)
        return 'done'

    bus.on('SampleEvent', test_handler)

    # Dispatch event
    event = await bus.emit(SampleEvent(data='processing_test'))

    # Check timestamps - should not raise AttributeError
    assert event.event_started_at is not None
    assert event.event_completed_at is not None
    assert isinstance(event.event_started_at, str)
    assert isinstance(event.event_completed_at, str)

    await bus.destroy()


async def test_event_without_handlers():
    """Test that events without handlers still work with timestamp properties"""
    event = SampleEvent(data='no_handlers')
    bus = EventBus(name='TestBusNoHandlers')

    # Should not raise AttributeError when accessing these properties
    assert event.event_started_at is None  # No handlers started
    assert event.event_completed_at is None  # Not complete yet

    processed_event = await bus.emit(event)
    await bus.destroy()

    # After marking complete, it should be set
    # When no handlers but event is completed, event_started_at returns event_completed_at
    assert processed_event.event_started_at is not None  # Uses event_completed_at
    assert processed_event.event_completed_at is not None  # Now it's complete
    assert processed_event.event_status == 'completed'
    assert processed_event.event_started_at == processed_event.event_completed_at


async def test_event_with_manually_set_completed_at():
    """Test events where event_completed_at is manually set (like in test_eventbus_log_tree.py)"""
    event = SampleEvent(data='manual')
    bus = EventBus(name='TestBusManualCompletedAt')

    # Initialize the completion signal
    _ = event.event_completed_signal

    # Manually set the completed timestamp (as done in tests)
    if hasattr(event, 'event_completed_at'):
        event.event_completed_at = monotonic_datetime()

    # Stateful runtime fields are no longer derived from event_results/event_completed_at on read.
    # Manually assigning event_completed_at alone does not mutate status/started_at.
    assert event.event_started_at is None
    assert event.event_status == 'pending'
    assert event.event_completed_at is not None

    # Reconcile state through public lifecycle processing.
    processed_event = await bus.emit(event)
    assert processed_event.event_status == 'completed'
    assert processed_event.event_started_at is not None
    assert processed_event.event_completed_at is not None

    # Also exercise the "existing completed handler results" completion path.
    seeded_event = SampleEvent(data='manual-seeded-result')
    seeded_result = seeded_event.event_result_update(handler=_noop_handler, status='started')
    assert seeded_event.event_status == 'started'
    assert seeded_event.event_completed_at is None
    seeded_result.update(status='completed', result='done')
    assert seeded_event.event_completed_at is None

    reconciled_seeded_event = await bus.emit(seeded_event)
    await bus.destroy()
    assert reconciled_seeded_event.event_status == 'completed'
    assert reconciled_seeded_event.event_started_at is not None
    assert reconciled_seeded_event.event_completed_at is not None


def test_event_copy_preserves_private_attrs():
    """Test that copying events preserves private attributes"""
    event = SampleEvent(data='copy_test')

    # Access properties to ensure private attrs are initialized
    _ = event.event_started_at
    _ = event.event_completed_at

    # Create a copy using model_copy
    copied_event = event.model_copy()

    # Should not raise AttributeError
    assert copied_event.event_started_at is None
    assert copied_event.event_completed_at is None


def test_event_started_at_is_serialized_and_stateful():
    """event_started_at should be included in JSON dumps and remain stable once set."""
    event = SampleEvent(data='serialize-started-at')

    pending_payload = event.model_dump(mode='json')
    assert 'event_started_at' in pending_payload
    assert pending_payload['event_started_at'] is None

    event.event_result_update(handler=_noop_handler, status='started')
    first_started_at = event.model_dump(mode='json')['event_started_at']
    assert isinstance(first_started_at, str)

    forced_started_at = '2020-01-01T00:00:00.000000000Z'
    result = next(iter(event.event_results.values()))
    result.started_at = forced_started_at

    second_started_at = event.model_dump(mode='json')['event_started_at']
    assert isinstance(second_started_at, str)
    assert second_started_at == first_started_at
    assert second_started_at != forced_started_at


async def test_event_status_is_serialized_and_stateful():
    """event_status should be included in JSON dumps and track lifecycle transitions via runtime updates."""
    event = SampleEvent(data='serialize-status')

    pending_payload = event.model_dump(mode='json')
    assert pending_payload['event_status'] == 'pending'

    bus = EventBus(name='TestBusSerializeStatus')
    handler_entered = asyncio.Event()
    release_handler = asyncio.Event()

    async def slow_handler(_event: SampleEvent) -> str:
        handler_entered.set()
        await release_handler.wait()
        return 'ok'

    bus.on('SampleEvent', slow_handler)

    processing_task = asyncio.create_task(bus.emit(event).wait())
    try:
        await asyncio.wait_for(handler_entered.wait(), timeout=1.0)
        started_payload = event.model_dump(mode='json')
        assert started_payload['event_status'] == 'started'

        release_handler.set()
        completed_event = await asyncio.wait_for(processing_task, timeout=1.0)
        completed_payload = completed_event.model_dump(mode='json')
        assert completed_payload['event_status'] == 'completed'
    finally:
        release_handler.set()
        if not processing_task.done():
            processing_task.cancel()
            with suppress(asyncio.CancelledError):
                await processing_task
        await bus.destroy()


# Folded from test_events_suck.py to keep test layout class-based.
import inspect
from typing import Any

from abxbus import BaseEvent, events_suck


class CreateUserEvent(BaseEvent[str]):
    id: str | None = None
    name: str
    age: int
    nickname: str | None = None


class UpdateUserEvent(BaseEvent[bool]):
    id: str
    name: str | None = None
    age: int | None = None
    source: str | None = None


class SomeLegacyImperativeClass:
    def __init__(self):
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def create(self, id: str | None, name: str, age: int) -> str:
        self.calls.append(('create', {'id': id, 'name': name, 'age': age}))
        return f'{name}-{age}'

    def update(self, id: str, name: str | None = None, age: int | None = None, **extra: Any) -> bool:
        self.calls.append(('update', {'id': id, 'name': name, 'age': age, **extra}))
        return bool(id)


def ping_user(user_id: str) -> str:
    return f'pong:{user_id}'


async def test_events_suck_wrap_emits_and_returns_first_result():
    bus = EventBus('EventsSuckBus')
    seen_payloads: list[dict[str, Any]] = []

    async def on_create(event: CreateUserEvent) -> str:
        seen_payloads.append(
            {
                'id': event.id,
                'name': event.name,
                'age': event.age,
                'nickname': event.nickname,
            }
        )
        return 'user-123'

    async def on_update(event: UpdateUserEvent) -> bool:
        seen_payloads.append(
            {
                'id': event.id,
                'name': event.name,
                'age': event.age,
                'source': event.source,
            }
        )
        return event.age == 46

    bus.on(CreateUserEvent, on_create)
    bus.on(UpdateUserEvent, on_update)

    MySDKClient = events_suck.wrap(
        'MySDKClient',
        {
            'create': CreateUserEvent,
            'update': UpdateUserEvent,
        },
    )
    client = MySDKClient(bus=bus)

    created_id = await client.create(name='bob', age=45, nickname='bobby')
    updated = await client.update(id=created_id, age=46, source='sync')

    assert created_id == 'user-123'
    assert updated is True
    assert seen_payloads == [
        {'id': None, 'name': 'bob', 'age': 45, 'nickname': 'bobby'},
        {'id': created_id, 'name': None, 'age': 46, 'source': 'sync'},
    ]

    await bus.destroy(clear=True)


def test_events_suck_wrap_builds_typed_method_signature():
    TestClient = events_suck.wrap('TestClient', {'create': CreateUserEvent})
    signature = inspect.signature(TestClient.create)
    params = signature.parameters

    assert list(params) == ['self', 'id', 'name', 'age', 'nickname', 'extra']
    assert params['id'].annotation == str | None
    assert params['id'].default is None
    assert params['name'].annotation is str
    assert params['name'].default is inspect.Parameter.empty
    assert params['age'].annotation is int
    assert params['nickname'].annotation == str | None
    assert params['nickname'].default is None
    assert params['extra'].kind == inspect.Parameter.VAR_KEYWORD
    assert signature.return_annotation is str


async def test_events_suck_make_events_and_make_handler_runtime_binding():
    events = events_suck.make_events(
        {
            'FooBarAPICreateEvent': SomeLegacyImperativeClass.create,
            'FooBarAPIUpdateEvent': SomeLegacyImperativeClass.update,
            'FooBarAPIPingEvent': ping_user,
        }
    )
    FooBarAPICreateEvent = events.FooBarAPICreateEvent
    FooBarAPIUpdateEvent = events.FooBarAPIUpdateEvent
    FooBarAPIPingEvent = events.FooBarAPIPingEvent

    assert FooBarAPICreateEvent.model_fields['id'].annotation == str | None
    assert FooBarAPICreateEvent.model_fields['name'].annotation is str
    assert FooBarAPICreateEvent.model_fields['age'].annotation is int
    assert FooBarAPICreateEvent.model_fields['event_result_type'].default is str

    bus = EventBus('LegacyBus')
    impl = SomeLegacyImperativeClass()
    bus.on(FooBarAPICreateEvent, events_suck.make_handler(impl.create))
    bus.on(FooBarAPIUpdateEvent, events_suck.make_handler(impl.update))
    bus.on(FooBarAPIPingEvent, events_suck.make_handler(ping_user))

    create_result = await bus.emit(FooBarAPICreateEvent(name='bob', age=45)).event_result()
    update_result = await bus.emit(
        FooBarAPIUpdateEvent(id='4ddee2b7-782f-7bbf-84ff-6aad2693982e', age=46, source='sync')
    ).event_result()
    ping_result = await bus.emit(FooBarAPIPingEvent(user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced')).event_result()

    assert create_result == 'bob-45'
    assert update_result is True
    assert ping_result == 'pong:e692b6cb-ae63-773b-8557-3218f7ce5ced'
    assert impl.calls == [
        ('create', {'id': None, 'name': 'bob', 'age': 45}),
        ('update', {'id': '4ddee2b7-782f-7bbf-84ff-6aad2693982e', 'name': None, 'age': 46, 'source': 'sync'}),
    ]

    await bus.destroy(clear=True)
