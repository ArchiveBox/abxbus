"""Test for per-handler timeout enforcement matching the exact scenario from the issue"""

import asyncio
import logging

import pytest

from abxbus import (
    BaseEvent,
    EventBus,
    EventHandlerAbortedError,
    EventHandlerCancelledError,
    EventHandlerTimeoutError,
)
from abxbus.retry import retry


# Event definitions
class TopmostEvent(BaseEvent[str]):
    """Event for navigating to a URL"""

    url: str = 'https://example.com'

    event_timeout: float | None = 5.0


class ChildEvent(BaseEvent[str]):
    """Event for tab creation"""

    tab_id: str = 'tab-123'

    event_timeout: float | None = 2


class GrandchildEvent(BaseEvent[str]):
    """Event for navigation completion"""

    success: bool = True

    event_timeout: float | None = 1


# Watchdog classes
class HandlerClass1:
    async def on_TopmostEvent(self, event: TopmostEvent) -> str:
        """Completes quickly - 1 second"""
        await asyncio.sleep(0.1)
        return 'HandlerClass1.on_TopmostEvent completed after 0.1s'

    async def on_GrandchildEvent(self, event: GrandchildEvent) -> str:
        """Starts but gets interrupted after 1 second by parent timeout"""
        await asyncio.sleep(5)  # Would take 5 seconds but will be interrupted
        return 'HandlerClass1.on_GrandchildEvent completed after 5s'


class HandlerClass2:
    async def on_GrandchildEvent(self, event: GrandchildEvent) -> str:
        """Completes instantly"""
        # No sleep - completes immediately
        return 'HandlerClass2.on_GrandchildEvent completed immediately'


class HandlerClass3:
    async def on_GrandchildEvent(self, event: GrandchildEvent) -> str:
        """Never gets to run - pending when timeout occurs"""
        await asyncio.sleep(0.2)
        return 'HandlerClass3.on_GrandchildEvent completed after 0.2s'


class HandlerClass4:
    async def on_GrandchildEvent(self, event: GrandchildEvent) -> str:
        """Never gets to run - pending when timeout occurs"""
        await asyncio.sleep(0.1)
        return 'HandlerClass4.on_GrandchildEvent completed after 0.1s'


class MainClass0:
    def __init__(self, bus: EventBus):
        self.bus = bus

    async def on_TopmostEvent(self, event: TopmostEvent) -> str:
        """Takes 11 seconds total - dispatches ChildEvent"""
        # Do some work
        await asyncio.sleep(1)

        # Dispatch and wait for ChildEvent
        child_event = self.bus.emit(ChildEvent())
        try:
            await child_event  # This will timeout after 10s
        except Exception as e:
            print(f'DEBUG: Parent caught child error: {type(e).__name__}: {e}')

            import threading

            all_tasks = asyncio.all_tasks()
            print(f'\nOutstanding asyncio tasks ({len(all_tasks)}):')
            for task in all_tasks:
                print(f'  - {task.get_name()}: {task._state} - {task.get_coro()}')

            # List all threads
            all_threads = threading.enumerate()
            print(f'\nActive threads ({len(all_threads)}):')
            for thread in all_threads:
                print(f'  - {thread.name}: {thread.is_alive()}')

            raise

        # Would continue but won't get here due to timeout
        return 'MainClass0.on_TopmostEvent completed after all child events'

    async def on_ChildEvent(self, event: ChildEvent) -> str:
        """Takes 10 seconds - will timeout, dispatches GrandchildEvent"""
        # Dispatch GrandchildEvent immediately
        grandchild_event = self.bus.emit(GrandchildEvent())

        # Wait for GrandchildEvent to complete
        # This will take 9s (MainClass0) + 0s (AboutBlank) + partial HandlerClass1 time
        # Since handlers run serially and we have a 10s timeout, we'll timeout while
        # HandlerClass1 is still running (after about 1s of its 5s execution)
        await grandchild_event  # .event_result(raise_if_any=False, raise_if_none=True, timeout=15)

        # Would continue but we timeout first
        return 'MainClass0.on_ChildEvent completed after GrandchildEvent() finished processing'

    async def on_GrandchildEvent(self, event: GrandchildEvent) -> str:
        """Completes in 5 seconds"""
        # print('GRANDCHILD EVENT HANDLING STARTED')
        await asyncio.sleep(2)
        return 'MainClass0.on_GrandchildEvent completed after 2s'


@pytest.mark.asyncio
async def test_nested_timeout_scenario_from_issue():
    """Test the exact timeout scenario described in the issue

    This tests:
    1. TopmostEvent with 30s timeout dispatches ChildEvent
    2. ChildEvent with 10s timeout times out after 10s
    3. GrandchildEvent is dispatched from ChildEvent handler
    4. Some handlers complete, some are interrupted, some never run
    5. The timeout tree logging shows the complete hierarchy
    """
    # Create single event bus
    bus = EventBus(name='MainClass0EventBus')

    # Create instances
    handlerclass1 = HandlerClass1()
    handlerclass2 = HandlerClass2()
    handlerclass3 = HandlerClass3()
    handlerclass4 = HandlerClass4()
    mainclass0 = MainClass0(bus)

    # Register handlers for TopmostEvent
    bus.on('TopmostEvent', handlerclass1.on_TopmostEvent)
    bus.on('TopmostEvent', mainclass0.on_TopmostEvent)

    # Register handlers for ChildEvent
    bus.on('ChildEvent', mainclass0.on_ChildEvent)

    # Register handlers for GrandchildEvent (order matters for the test)
    bus.on('GrandchildEvent', mainclass0.on_GrandchildEvent)
    bus.on('GrandchildEvent', handlerclass2.on_GrandchildEvent)
    bus.on('GrandchildEvent', handlerclass1.on_GrandchildEvent)
    bus.on('GrandchildEvent', handlerclass3.on_GrandchildEvent)
    bus.on('GrandchildEvent', handlerclass4.on_GrandchildEvent)

    # Dispatch the root event
    navigate_event = bus.emit(TopmostEvent())

    # Wait for it to complete (will fail due to timeout)
    # with pytest.raises((RuntimeError, TimeoutError)) as exc_info:
    try:
        await (
            navigate_event
        )  # .event_result(raise_if_any=True, raise_if_none=True, timeout=20)  # The event should complete with an error
    except Exception as e:
        print(f'Exception caught: {type(e).__name__}: {e}')
        raise

    # import ipdb; ipdb.set_trace()

    # print('-----------------------------------------------------')
    # print(f"Exception caught: {type(exc_info.value).__name__}: {exc_info.value}")
    # # assert 'ChildEvent' in str(exc_info.value) or 'ChildEvent' in str(exc_info.value)

    await bus.destroy(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_handler_timeout_marks_error_and_other_handlers_still_complete():
    """Focused timeout behavior: one handler times out, another still completes."""
    bus = EventBus(name='TimeoutFocusedBus')

    class TimeoutFocusedEvent(BaseEvent[str]):
        event_timeout: float | None = 0.2
        event_handler_timeout: float | None = 0.01

    execution_order: list[str] = []

    async def slow_handler(event: TimeoutFocusedEvent) -> str:
        execution_order.append('slow_start')
        await asyncio.sleep(0.05)
        execution_order.append('slow_end')
        return 'slow'

    async def fast_handler(event: TimeoutFocusedEvent) -> str:
        execution_order.append('fast_start')
        return 'fast'

    bus.on(TimeoutFocusedEvent, slow_handler)
    bus.on(TimeoutFocusedEvent, fast_handler)

    try:
        event = await bus.emit(TimeoutFocusedEvent())
        await bus.wait_until_idle()

        slow_result = next((r for r in event.event_results.values() if r.handler_name.endswith('slow_handler')), None)
        fast_result = next((r for r in event.event_results.values() if r.handler_name.endswith('fast_handler')), None)

        assert slow_result is not None
        assert slow_result.status == 'error'
        assert isinstance(slow_result.error, TimeoutError)

        assert fast_result is not None
        assert fast_result.status == 'completed'
        assert fast_result.result == 'fast'
        assert 'fast_start' in execution_order
    finally:
        await bus.destroy(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_handler_timeout_ignores_late_handler_result_and_late_emits() -> None:
    bus = EventBus(name='TimeoutIgnoresLateHandlerBus')

    class TimeoutIgnoresLateHandlerEvent(BaseEvent[str]):
        event_timeout: float | None = 0.2
        event_handler_timeout: float | None = 0.01

    class LateAfterTimeoutEvent(BaseEvent[str]):
        pass

    late_attempt = asyncio.Event()
    late_handler_ran = False

    async def slow_handler(event: TimeoutIgnoresLateHandlerEvent) -> str:
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            late_attempt.set()
            event.emit(LateAfterTimeoutEvent())
            return 'late success'
        return 'unexpected success'

    async def late_handler(_event: LateAfterTimeoutEvent) -> str:
        nonlocal late_handler_ran
        late_handler_ran = True
        return 'late child'

    bus.on(TimeoutIgnoresLateHandlerEvent, slow_handler)
    bus.on(LateAfterTimeoutEvent, late_handler)

    try:
        event = await bus.emit(TimeoutIgnoresLateHandlerEvent())
        await asyncio.wait_for(late_attempt.wait(), timeout=1)
        await asyncio.sleep(0.03)
        await bus.wait_until_idle(timeout=1)

        slow_result = next(result for result in event.event_results.values() if result.handler_name.endswith('slow_handler'))
        assert slow_result.status == 'error'
        assert isinstance(slow_result.error, EventHandlerTimeoutError)
        assert slow_result.result is None
        assert await bus.find(LateAfterTimeoutEvent, past=True, future=False) is None
        assert late_handler_ran is False
    finally:
        await bus.destroy(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_event_timeout_ignores_late_handler_result_and_late_emits() -> None:
    bus = EventBus(name='TimeoutIgnoresLateEventBus')

    class TimeoutIgnoresLateEvent(BaseEvent[str]):
        event_timeout: float | None = 0.01

    class LateAfterTimeoutEvent(BaseEvent[str]):
        pass

    late_attempt = asyncio.Event()
    late_handler_ran = False

    async def slow_handler(event: TimeoutIgnoresLateEvent) -> str:
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            late_attempt.set()
            event.emit(LateAfterTimeoutEvent())
            return 'late success'
        return 'unexpected success'

    async def late_handler(_event: LateAfterTimeoutEvent) -> str:
        nonlocal late_handler_ran
        late_handler_ran = True
        return 'late child'

    bus.on(TimeoutIgnoresLateEvent, slow_handler)
    bus.on(LateAfterTimeoutEvent, late_handler)

    try:
        event = bus.emit(TimeoutIgnoresLateEvent())
        await event.wait(timeout=1)
        await asyncio.wait_for(late_attempt.wait(), timeout=1)
        await asyncio.sleep(0.03)
        await bus.wait_until_idle(timeout=1)

        slow_result = next(result for result in event.event_results.values() if result.handler_name.endswith('slow_handler'))
        assert slow_result.status == 'error'
        assert isinstance(slow_result.error, EventHandlerAbortedError)
        assert slow_result.result is None
        assert await bus.find(LateAfterTimeoutEvent, past=True, future=False) is None
        assert late_handler_ran is False
    finally:
        await bus.destroy(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_event_timeout_is_hard_cap_across_serial_handlers():
    bus = EventBus(name='EventHardCapBus')

    class HardCapEvent(BaseEvent[str]):
        event_timeout: float | None = 0.05

    async def first_handler(_event: HardCapEvent) -> str:
        await asyncio.sleep(0.03)
        return 'first'

    async def second_handler(_event: HardCapEvent) -> str:
        await asyncio.sleep(0.03)
        return 'second'

    async def pending_handler(_event: HardCapEvent) -> str:
        return 'pending'

    bus.on(HardCapEvent, first_handler)
    bus.on(HardCapEvent, second_handler)
    bus.on(HardCapEvent, pending_handler)

    try:
        event = await bus.emit(HardCapEvent())
        await bus.wait_until_idle()

        first_result = next(result for result in event.event_results.values() if result.handler_name.endswith('first_handler'))
        second_result = next(result for result in event.event_results.values() if result.handler_name.endswith('second_handler'))
        pending_result = next(
            result for result in event.event_results.values() if result.handler_name.endswith('pending_handler')
        )

        assert first_result.status == 'completed'
        assert first_result.result == 'first'
        assert second_result.status == 'error'
        assert isinstance(second_result.error, EventHandlerAbortedError)
        assert pending_result.status == 'error'
        assert isinstance(pending_result.error, EventHandlerCancelledError)
    finally:
        await bus.destroy(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_event_timeout_is_hard_cap_in_parallel_mode() -> None:
    bus = EventBus(name='EventHardCapParallelBus', event_handler_concurrency='parallel')

    class HardCapParallelEvent(BaseEvent[str]):
        event_timeout: float | None = 0.03

    async def slow_a(_event: HardCapParallelEvent) -> str:
        await asyncio.sleep(0.1)
        return 'a'

    async def slow_b(_event: HardCapParallelEvent) -> str:
        await asyncio.sleep(0.1)
        return 'b'

    bus.on(HardCapParallelEvent, slow_a)
    bus.on(HardCapParallelEvent, slow_b)

    try:
        event = await bus.emit(HardCapParallelEvent())
        await bus.wait_until_idle()

        assert len(event.event_results) == 2
        assert all(result.status == 'error' for result in event.event_results.values())
        assert all(
            isinstance(result.error, (EventHandlerAbortedError, EventHandlerCancelledError, EventHandlerTimeoutError))
            for result in event.event_results.values()
        )
        assert any(
            isinstance(result.error, (EventHandlerAbortedError, EventHandlerTimeoutError))
            for result in event.event_results.values()
        )
    finally:
        await bus.destroy(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_event_level_timeout_marks_started_parallel_handlers_as_aborted_or_timed_out() -> None:
    bus = EventBus(name='EventHardCapParallelAbortedOnlyBus', event_handler_concurrency='parallel')

    class HardCapParallelAbortOnlyEvent(BaseEvent[str]):
        event_timeout: float | None = 0.03

    started_a = asyncio.Event()
    started_b = asyncio.Event()
    both_started = asyncio.Event()

    async def slow_a(_event: HardCapParallelAbortOnlyEvent) -> str:
        started_a.set()
        if started_b.is_set():
            both_started.set()
        await both_started.wait()
        await asyncio.sleep(0.2)
        return 'a'

    async def slow_b(_event: HardCapParallelAbortOnlyEvent) -> str:
        started_b.set()
        if started_a.is_set():
            both_started.set()
        await both_started.wait()
        await asyncio.sleep(0.2)
        return 'b'

    bus.on(HardCapParallelAbortOnlyEvent, slow_a)
    bus.on(HardCapParallelAbortOnlyEvent, slow_b)

    try:
        event = bus.emit(HardCapParallelAbortOnlyEvent())
        await asyncio.wait_for(asyncio.gather(started_a.wait(), started_b.wait()), timeout=0.2)
        both_started.set()
        await event
        await bus.wait_until_idle()

        assert len(event.event_results) == 2
        assert all(result.status == 'error' for result in event.event_results.values())
        assert all(
            isinstance(result.error, (EventHandlerAbortedError, EventHandlerTimeoutError))
            for result in event.event_results.values()
        )
        assert not any(isinstance(result.error, EventHandlerCancelledError) for result in event.event_results.values())
    finally:
        await bus.destroy(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_event_timeout_does_not_relabel_preexisting_handler_timeout() -> None:
    bus = EventBus(name='EventTimeoutPreservesHandlerTimeoutBus', event_handler_concurrency='parallel')

    class MixedTimeoutEvent(BaseEvent[str]):
        event_timeout: float | None = 0.05

    @retry(max_attempts=1, timeout=0.01)
    async def handler_with_own_timeout(_event: MixedTimeoutEvent) -> str:
        await asyncio.sleep(0.05)
        return 'own-timeout'

    async def long_running_handler(_event: MixedTimeoutEvent) -> str:
        await asyncio.sleep(0.2)
        return 'long-running'

    bus.on(MixedTimeoutEvent, handler_with_own_timeout)
    bus.on(MixedTimeoutEvent, long_running_handler)

    try:
        event = bus.emit(MixedTimeoutEvent())
        await event
        await bus.wait_until_idle()

        results = list(event.event_results.values())
        assert len(results) == 2
        assert any(isinstance(result.error, EventHandlerTimeoutError) for result in results)
        assert any(isinstance(result.error, EventHandlerAbortedError) for result in results)
    finally:
        await bus.destroy(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_multi_bus_timeout_is_recorded_on_target_bus():
    """Closest Python equivalent: same event dispatched to two buses, timeout on target bus is captured."""
    bus_a = EventBus(name='MultiTimeoutA')
    bus_b = EventBus(name='MultiTimeoutB')

    class MultiBusTimeoutEvent(BaseEvent[str]):
        event_timeout: float | None = 0.01

    async def slow_target_handler(event: MultiBusTimeoutEvent) -> str:
        await asyncio.sleep(0.05)
        return 'slow'

    bus_b.on(MultiBusTimeoutEvent, slow_target_handler)

    try:
        event = MultiBusTimeoutEvent()
        bus_a.emit(event)
        bus_b.emit(event)
        await bus_b.wait_until_idle()

        bus_b_result = next((r for r in event.event_results.values() if r.eventbus_name == bus_b.name), None)
        assert bus_b_result is not None
        assert bus_b_result.status == 'error'
        assert isinstance(bus_b_result.error, EventHandlerAbortedError)
        assert event.event_path == [bus_a.label, bus_b.label]
    finally:
        await bus_a.destroy(clear=True, timeout=0)
        await bus_b.destroy(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_followup_event_runs_after_parent_timeout_in_queue_jump_path():
    """
    Regression guard: timeout in a handler that awaited a child event should not
    stall subsequent events on the same bus.
    """
    bus = EventBus(name='TimeoutQueueJumpFollowupBus')

    class ParentEvent(BaseEvent[str]):
        event_timeout: float | None = 0.02

    class ChildEvent(BaseEvent[str]):
        event_timeout: float | None = 0.2

    class TailEvent(BaseEvent[str]):
        event_timeout: float | None = 0.2

    tail_runs = 0

    async def child_handler(event: ChildEvent) -> str:
        await asyncio.sleep(0.001)
        return 'child_done'

    async def parent_handler(event: ParentEvent) -> str:
        child = bus.emit(ChildEvent())
        await child
        await asyncio.sleep(0.05)  # Exceeds parent timeout
        return 'parent_done'

    async def tail_handler(event: TailEvent) -> str:
        nonlocal tail_runs
        tail_runs += 1
        return 'tail_done'

    bus.on(ParentEvent, parent_handler)
    bus.on(ChildEvent, child_handler)
    bus.on(TailEvent, tail_handler)

    try:
        parent = await bus.emit(ParentEvent())
        await bus.wait_until_idle()

        parent_result = next(iter(parent.event_results.values()))
        assert parent_result.status == 'error'
        assert isinstance(parent_result.error, EventHandlerAbortedError)

        tail = bus.emit(TailEvent())
        completed_tail = await asyncio.wait_for(tail, timeout=1.0)
        assert completed_tail.event_status == 'completed'
        assert tail_runs == 1
    finally:
        await bus.destroy(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_forwarded_timeout_path_does_not_stall_followup_events():
    """
    Regression guard: if a forwarded awaited child times out, subsequent events
    should still run on both source and target buses.
    """
    bus_a = EventBus(name='TimeoutForwardA')
    bus_b = EventBus(name='TimeoutForwardB')

    class ParentEvent(BaseEvent[str]):
        # This test validates child-timeout recovery, not parent-timeout behavior.
        # Keep parent timeout well above observed queue/lock jitter in full-suite runs.
        event_timeout: float | None = 1.0

    class ChildEvent(BaseEvent[str]):
        event_timeout: float | None = 0.01

    class TailEvent(BaseEvent[str]):
        event_timeout: float | None = 0.2

    bus_a_tail_runs = 0
    bus_b_tail_runs = 0
    child_ref: ChildEvent | None = None

    async def parent_handler(event: ParentEvent) -> str:
        nonlocal child_ref
        child = bus_a.emit(ChildEvent())
        child_ref = child
        await child
        return 'parent_done'

    async def slow_child_handler(event: ChildEvent) -> str:
        await asyncio.sleep(0.05)  # Guaranteed timeout on child.
        return 'child_done'

    async def tail_handler_a(event: TailEvent) -> str:
        nonlocal bus_a_tail_runs
        bus_a_tail_runs += 1
        return 'tail_a'

    async def tail_handler_b(event: TailEvent) -> str:
        nonlocal bus_b_tail_runs
        bus_b_tail_runs += 1
        return 'tail_b'

    bus_a.on(ParentEvent, parent_handler)
    bus_a.on(TailEvent, tail_handler_a)
    bus_a.on('*', bus_b.emit)
    bus_b.on(ChildEvent, slow_child_handler)
    bus_b.on(TailEvent, tail_handler_b)

    try:
        parent = await bus_a.emit(ParentEvent())
        await bus_a.wait_until_idle()
        await bus_b.wait_until_idle()

        parent_result = next(result for result in parent.event_results.values() if result.handler_name.endswith('parent_handler'))
        assert parent_result.status == 'completed'

        assert child_ref is not None
        assert any(
            isinstance(result.error, (EventHandlerTimeoutError, EventHandlerAbortedError))
            for result in child_ref.event_results.values()
        ), child_ref.event_results

        # Lock/queue state should remain healthy after timeout.
        tail = bus_a.emit(TailEvent())
        completed_tail = await asyncio.wait_for(tail, timeout=1.0)
        await bus_a.wait_until_idle()
        await bus_b.wait_until_idle()

        assert completed_tail.event_status == 'completed'
        assert bus_a_tail_runs == 1
        assert bus_b_tail_runs == 1
    finally:
        await bus_a.destroy(clear=True, timeout=0)
        await bus_b.destroy(clear=True, timeout=0)


# Consolidated from tests/test_event_timeout_defaults.py


class TimeoutDefaultsEvent(BaseEvent[str]):
    pass


@pytest.mark.asyncio
async def test_processing_time_timeout_defaults_resolve_at_execution_time() -> None:
    bus = EventBus(
        name='TimeoutDefaultsResolveBus',
        event_timeout=12.0,
        event_slow_timeout=34.0,
        event_handler_slow_timeout=56.0,
    )

    async def handler(_event: TimeoutDefaultsEvent) -> str:
        return 'ok'

    bus.on(TimeoutDefaultsEvent, handler)

    try:
        event = bus.emit(TimeoutDefaultsEvent())
        assert event.event_timeout is None
        assert event.event_handler_timeout is None
        assert event.event_handler_slow_timeout is None
        assert event.event_slow_timeout is None
        assert event.event_concurrency is None
        assert event.event_handler_concurrency is None
        assert event.event_handler_completion is None

        completed = await event
        assert completed.event_timeout is None
        assert completed.event_handler_slow_timeout is None
        assert completed.event_slow_timeout is None
        assert completed.event_concurrency is None
        assert completed.event_handler_concurrency is None
        assert completed.event_handler_completion is None
        handler_result = next(iter(completed.event_results.values()))
        assert handler_result.timeout is not None and abs(handler_result.timeout - 12.0) < 1e-9
    finally:
        await bus.destroy()


@pytest.mark.asyncio
async def test_event_timeout_none_uses_bus_default_timeout_at_execution() -> None:
    bus = EventBus(name='TimeoutNoneUsesBusDefault', event_timeout=0.01)

    async def handler(_event: TimeoutDefaultsEvent) -> str:
        await asyncio.sleep(0.02)
        return 'slow'

    bus.on(TimeoutDefaultsEvent, handler)

    try:
        event = await bus.emit(TimeoutDefaultsEvent(event_timeout=None)).now()
        handler_result = next(iter(event.event_results.values()))
        assert event.event_timeout is None
        assert handler_result.status == 'error'
        assert isinstance(handler_result.error, EventHandlerAbortedError)
    finally:
        await bus.destroy()


@pytest.mark.asyncio
async def test_handler_timeout_resolution_matches_ts_precedence() -> None:
    bus = EventBus(name='TimeoutPrecedenceBus', event_timeout=0.2)

    async def default_handler(_event: TimeoutDefaultsEvent) -> str:
        await asyncio.sleep(0.001)
        return 'default'

    async def overridden_handler(_event: TimeoutDefaultsEvent) -> str:
        await asyncio.sleep(0.001)
        return 'override'

    bus.on(TimeoutDefaultsEvent, default_handler)
    overridden_entry = bus.on(TimeoutDefaultsEvent, overridden_handler)
    overridden_entry.handler_timeout = 0.12

    try:
        event = await bus.emit(TimeoutDefaultsEvent(event_timeout=0.2, event_handler_timeout=0.05))

        default_result = next(
            result for result in event.event_results.values() if result.handler_name.endswith('default_handler')
        )
        overridden_result = next(
            result for result in event.event_results.values() if result.handler_name.endswith('overridden_handler')
        )

        assert default_result.timeout is not None and abs(default_result.timeout - 0.05) < 1e-9
        assert overridden_result.timeout is not None and abs(overridden_result.timeout - 0.12) < 1e-9

        tighter_event_timeout = await bus.emit(TimeoutDefaultsEvent(event_timeout=0.08, event_handler_timeout=0.2))
        tighter_default = next(
            result for result in tighter_event_timeout.event_results.values() if result.handler_name.endswith('default_handler')
        )
        tighter_overridden = next(
            result
            for result in tighter_event_timeout.event_results.values()
            if result.handler_name.endswith('overridden_handler')
        )

        assert tighter_default.timeout is not None and abs(tighter_default.timeout - 0.08) < 1e-9
        assert tighter_overridden.timeout is not None and abs(tighter_overridden.timeout - 0.08) < 1e-9
    finally:
        await bus.destroy()


@pytest.mark.asyncio
async def test_event_handler_detect_file_paths_toggle() -> None:
    bus = EventBus(name='NoDetectPathsBus', event_handler_detect_file_paths=False)

    async def handler(_event: TimeoutDefaultsEvent) -> str:
        return 'ok'

    try:
        entry = bus.on(TimeoutDefaultsEvent, handler)
        assert entry.handler_file_path is None
    finally:
        await bus.destroy()


@pytest.mark.asyncio
async def test_handler_slow_warning_uses_event_handler_slow_timeout(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger='abxbus')
    bus = EventBus(
        name='SlowHandlerWarnBus',
        event_timeout=0.5,
        event_slow_timeout=0.5,
        event_handler_slow_timeout=0.5,
    )

    async def slow_handler(_event: TimeoutDefaultsEvent) -> str:
        await asyncio.sleep(0.03)
        logger = logging.getLogger('abxbus')
        logger.warning('slow warning child handler finishing')
        return 'ok'

    bus.on(TimeoutDefaultsEvent, slow_handler)

    try:
        await bus.emit(TimeoutDefaultsEvent(event_slow_timeout=0, event_handler_slow_timeout=0.01))
        messages = [record.message for record in caplog.records]
        slow_warning_index = next(index for index, message in enumerate(messages) if 'Slow event handler:' in message)
        finish_marker_index = next(
            index for index, message in enumerate(messages) if 'slow warning child handler finishing' in message
        )
        assert slow_warning_index < finish_marker_index
        assert not any('Slow event processing:' in message for message in messages)
    finally:
        await bus.destroy()


@pytest.mark.asyncio
async def test_event_slow_warning_uses_event_slow_timeout(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger='abxbus')
    bus = EventBus(
        name='SlowEventWarnBus',
        event_timeout=0.5,
        event_slow_timeout=0.5,
        event_handler_slow_timeout=0.5,
    )

    async def slow_event_handler(_event: TimeoutDefaultsEvent) -> str:
        await asyncio.sleep(0.03)
        logger = logging.getLogger('abxbus')
        logger.warning('slow warning child handler finishing')
        return 'ok'

    bus.on(TimeoutDefaultsEvent, slow_event_handler)

    try:
        await bus.emit(TimeoutDefaultsEvent(event_slow_timeout=0.01, event_handler_slow_timeout=0))
        messages = [record.message for record in caplog.records]
        slow_warning_index = next(index for index, message in enumerate(messages) if 'Slow event processing:' in message)
        finish_marker_index = next(
            index for index, message in enumerate(messages) if 'slow warning child handler finishing' in message
        )
        assert slow_warning_index < finish_marker_index
        assert not any('Slow event handler:' in message for message in messages)
    finally:
        await bus.destroy()


@pytest.mark.asyncio
async def test_slow_handler_and_slow_event_warnings_can_both_fire(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger='abxbus')
    bus = EventBus(
        name='SlowComboWarnBus',
        event_timeout=0.5,
        event_slow_timeout=0.5,
        event_handler_slow_timeout=0.5,
    )

    async def slow_handler(_event: TimeoutDefaultsEvent) -> str:
        await asyncio.sleep(0.03)
        return 'ok'

    bus.on(TimeoutDefaultsEvent, slow_handler)

    try:
        await bus.emit(TimeoutDefaultsEvent(event_slow_timeout=0.01, event_handler_slow_timeout=0.01))
        messages = [record.message for record in caplog.records]
        assert any('Slow event handler:' in message for message in messages)
        assert any('Slow event processing:' in message for message in messages)
    finally:
        await bus.destroy()


@pytest.mark.asyncio
async def test_zero_slow_warning_thresholds_disable_event_and_handler_slow_warnings(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger='abxbus')
    bus = EventBus(
        name='NoSlowWarnBus',
        event_timeout=0.5,
        event_slow_timeout=0,
        event_handler_slow_timeout=0,
    )

    async def slow_handler(_event: TimeoutDefaultsEvent) -> str:
        await asyncio.sleep(0.03)
        logger = logging.getLogger('abxbus')
        logger.warning('slow warning child handler finishing')
        return 'ok'

    bus.on(TimeoutDefaultsEvent, slow_handler)

    try:
        await bus.emit(TimeoutDefaultsEvent())
        messages = [record.message for record in caplog.records]
        assert not any('Slow event handler:' in message for message in messages)
        assert not any('Slow event processing:' in message for message in messages)
        assert any('slow warning child handler finishing' in message for message in messages)
    finally:
        await bus.destroy()
