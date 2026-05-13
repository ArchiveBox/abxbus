# pyright: basic
"""
Comprehensive tests for the EventBus implementation.

Tests cover:
- Basic event enqueueing and processing
- Sync and async contexts
- Handler registration and execution
- FIFO ordering
- Parallel handler execution
- Error handling
- Write-ahead logging
- Serialization
- Batch operations
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from pydantic import Field

from abxbus import BaseEvent, EventBus
from abxbus.helpers import monotonic_datetime


class CreateAgentTaskEvent(BaseEvent):
    """Test event model for creating an agent task"""

    user_id: str
    agent_session_id: str
    llm_model: str
    task: str


# Test event models
class UserActionEvent(BaseEvent):
    """Test event model for user actions"""

    action: str
    user_id: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class SystemEventModel(BaseEvent):
    """Test event model for system events"""

    name: str
    severity: str = 'info'
    details: dict[str, Any] = Field(default_factory=dict)


class RecursiveEvent(BaseEvent):
    """Test event model for handler recursion guard behavior."""

    level: int = 0
    max_level: int = 0


@pytest.fixture
async def eventbus():
    """Create an event bus for testing"""
    bus = EventBus(max_history_size=10000)  # Increase history limit for tests
    yield bus
    await bus.destroy()


@pytest.fixture
async def parallel_eventbus():
    """Create an event bus with parallel handler execution"""
    bus = EventBus(event_handler_concurrency='parallel')
    yield bus
    await bus.destroy()


class TestEventBusBasics:
    """Test basic EventBus functionality"""

    async def test_eventbus_initialization(self):
        """Test that EventBus initializes correctly"""
        bus = EventBus()

        assert bus._is_running is False
        assert bus._runloop_task is None
        assert len(bus.event_history) == 0
        assert len(bus.handlers_by_key.get('*', [])) == 0  # No default logger anymore
        assert bus.event_history.max_history_drop is False

    def test_eventbus_accepts_custom_id(self):
        """EventBus constructor accepts id=... to set bus UUID."""
        custom_id = '018f8e40-1234-7000-8000-000000001234'
        bus = EventBus(id=custom_id)

        assert bus.id == custom_id
        assert bus.label.endswith('#1234')

    def test_eventbus_accepts_custom_handler_recursion_depth(self):
        """EventBus exposes a configurable per-handler recursion ceiling."""
        bus = EventBus(max_handler_recursion_depth=5)

        assert bus.max_handler_recursion_depth == 5

    @pytest.mark.anyio
    async def test_custom_handler_recursion_depth_allows_deeper_nested_handlers(self):
        """A higher configured recursion ceiling should allow deeper nested queue-jumps."""
        bus = EventBus(name='CustomRecursionDepthBus', max_handler_recursion_depth=5)
        seen_levels: list[int] = []

        async def handler(event: RecursiveEvent) -> None:
            seen_levels.append(event.level)
            if event.level < event.max_level:
                await event.emit(RecursiveEvent(level=event.level + 1, max_level=event.max_level))

        bus.on(RecursiveEvent, handler)

        try:
            await bus.emit(RecursiveEvent(level=0, max_level=5))
            assert seen_levels == [0, 1, 2, 3, 4, 5]
        finally:
            await bus.destroy(clear=True)

    @pytest.mark.anyio
    async def test_default_handler_recursion_depth_still_catches_runaway_loops(self):
        """The default recursion guard should still raise on deeper self-reentry."""
        bus = EventBus(name='DefaultRecursionDepthBus')

        async def handler(event: RecursiveEvent) -> None:
            if event.level < event.max_level:
                await event.emit(RecursiveEvent(level=event.level + 1, max_level=event.max_level))

        bus.on(RecursiveEvent, handler)

        try:
            await bus.emit(RecursiveEvent(level=0, max_level=3))
            assert any(
                result.status == 'error' and 'Infinite loop detected' in str(result.error)
                for historical_event in bus.event_history.values()
                for result in historical_event.event_results.values()
            )
        finally:
            await bus.destroy(clear=True)

    async def test_auto_start_and_destroy(self):
        """Test auto-start functionality and destroying the event bus"""
        bus = EventBus()

        # Should not be running initially
        assert bus._is_running is False
        assert bus._runloop_task is None

        # Auto-start by emitting an event
        bus.emit(UserActionEvent(action='test', user_id='50d357df-e68c-7111-8a6c-7018569514b0'))
        await bus.wait_until_idle()

        # Should be running after auto-start
        assert bus._is_running is True
        assert bus._runloop_task is not None

        # Destroy the bus
        await bus.destroy()
        assert bus._is_running is False

    async def test_destroy_default_clear_is_terminal_and_frees_bus_state(self):
        """destroy() defaults to clear=True: bus-owned state is released and use is terminal."""

        class DestroyEvent(BaseEvent[str]):
            pass

        bus = EventBus(name='DestroyDefaultClearBus')
        bus.on(DestroyEvent, lambda _event: 'done')

        event = await bus.emit(DestroyEvent())
        assert await event.event_result() == 'done'

        await bus.destroy()

        assert bus._is_running is False
        assert bus.pending_event_queue is None
        assert bus._on_idle is None
        assert len(bus.handlers) == 0
        assert len(bus.handlers_by_key) == 0
        assert len(bus.event_history) == 0
        assert len(bus.in_flight_event_ids) == 0
        assert len(bus.processing_event_ids) == 0
        assert len(bus.find_waiters) == 0
        assert bus not in type(bus).all_instances

        with pytest.raises(RuntimeError, match='destroyed'):
            bus.on(DestroyEvent, lambda _event: 'again')
        with pytest.raises(RuntimeError, match='destroyed'):
            bus.emit(DestroyEvent())
        with pytest.raises(RuntimeError, match='destroyed'):
            await bus.find(DestroyEvent, future=False)

    async def test_destroy_clear_false_preserves_handlers_and_history_resolves_waiters_and_is_terminal(self):
        """destroy(clear=False) stops runtime work, resolves waiters, preserves inspectable state, and is terminal."""

        class TerminalEvent(BaseEvent[str]):
            pass

        bus = EventBus(name='DestroyClearFalseTerminalBus')
        calls: list[str] = []

        async def handler(event: TerminalEvent) -> str:
            calls.append(event.event_id)
            return f'handled:{len(calls)}'

        bus.on(TerminalEvent, handler)

        first = await bus.emit(TerminalEvent())
        assert await first.event_result() == 'handled:1'

        waiter_task = asyncio.create_task(bus.find('NeverHappens', past=False, future=True))
        await asyncio.sleep(0)

        await bus.destroy(clear=False)

        assert await asyncio.wait_for(waiter_task, timeout=1.0) is None
        assert bus._is_running is False
        assert bus.pending_event_queue is None
        assert len(bus.handlers) == 1
        assert len(bus.event_history) == 1
        assert bus not in type(bus).all_instances
        assert bus._destroyed is True

        with pytest.raises(RuntimeError, match='destroyed'):
            bus.on(TerminalEvent, handler)
        with pytest.raises(RuntimeError, match='destroyed'):
            bus.emit(TerminalEvent())
        with pytest.raises(RuntimeError, match='destroyed'):
            await bus.find(TerminalEvent, future=False)

        await bus.destroy(clear=True)

    async def test_destroying_one_bus_does_not_break_shared_handlers_or_forward_targets(self):
        """A terminal destroy only clears the selected bus, not shared callables or peer buses."""

        class SharedDestroyEvent(BaseEvent[str]):
            pass

        source = EventBus(name='DestroySharedSourceBus')
        target = EventBus(name='DestroySharedTargetBus')
        seen: list[str] = []

        def shared_handler(event: SharedDestroyEvent) -> str:
            seen.append(event.event_type)
            return 'shared'

        source.on(SharedDestroyEvent, shared_handler)
        source.on('*', target.emit)
        target.on(SharedDestroyEvent, shared_handler)

        forwarded = await source.emit(SharedDestroyEvent())
        assert await forwarded.event_results_list(raise_if_any=False) == ['shared', 'shared']

        await source.destroy(clear=True)

        direct = await target.emit(SharedDestroyEvent())
        assert await direct.event_result() == 'shared'
        assert len(target.handlers) == 1
        assert len(target.event_history) >= 1
        assert len(seen) == 3

        await target.destroy(clear=True)

    async def test_wait_until_idle_recovers_when_idle_flag_was_cleared(self):
        """wait_until_idle should not hang if _on_idle was cleared after work finished."""
        bus = EventBus()

        async def handler(_event: UserActionEvent) -> None:
            return None

        bus.on(UserActionEvent, handler)

        try:
            await bus.emit(UserActionEvent(action='test', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
            await bus.wait_until_idle()

            assert bus._on_idle is not None
            bus._on_idle.clear()

            await asyncio.wait_for(bus.wait_until_idle(), timeout=1.0)
        finally:
            await bus.destroy()

        # Destroy again should be idempotent
        await bus.destroy()
        assert bus._is_running is False


class TestEventEnqueueing:
    """Test event enqueueing functionality"""

    async def test_emit_and_result(self, eventbus):
        """Test event emission in async and sync contexts, and result() pattern"""
        # Test async emission
        event = UserActionEvent(action='login', user_id='50d357df-e68c-7111-8a6c-7018569514b0', event_timeout=1)
        queued = eventbus.emit(event)

        # Check immediate result
        assert isinstance(queued, UserActionEvent)
        assert queued.event_type == 'UserActionEvent'
        assert queued.action == 'login'
        assert queued.user_id == '50d357df-e68c-7111-8a6c-7018569514b0'
        assert queued.event_id is not None
        assert queued.event_created_at is not None
        assert queued.event_started_at is None  # Not started yet
        assert queued.event_completed_at is None  # Not completed yet
        assert queued.event_status == 'pending'

        # Test result() pattern
        processed = await queued
        assert processed.event_started_at is not None
        assert processed.event_completed_at is not None
        assert processed.event_status == 'completed'
        # Check that we have no results (no default handler anymore)
        assert len(processed.event_results) == 0

        # Check event history
        assert len(eventbus.event_history) == 1

    def test_emit_sync(self):
        """Test sync event emission"""
        bus = EventBus()
        event = SystemEventModel(name='startup', severity='info')

        with pytest.raises(RuntimeError) as e:
            bus.emit(event)

        assert 'no event loop is running' in str(e.value)
        assert len(bus.event_history) == 0

    async def test_emit_alias_dispatches_event(self, eventbus):
        """Test EventBus.emit() alias dispatches and processes events."""
        handled_event_ids: list[str] = []

        async def user_handler(event: UserActionEvent) -> str:
            handled_event_ids.append(event.event_id)
            return 'handled'

        eventbus.on(UserActionEvent, user_handler)

        event = UserActionEvent(action='alias', user_id='50d357df-e68c-7111-8a6c-7018569514b0')
        queued = eventbus.emit(event)

        assert queued is event
        completed = await queued
        assert completed.event_status == 'completed'
        assert handled_event_ids == [event.event_id]
        assert eventbus.label in completed.event_path

    async def test_unbounded_history_disables_history_rejection(self):
        """When max_history_size=None, dispatch should not reject on history size."""
        bus = EventBus(name='NoLimitBus', max_history_size=None)

        processed = 0

        async def slow_handler(event: BaseEvent) -> None:
            nonlocal processed
            await asyncio.sleep(0.01)
            processed += 1

        bus.on('SlowEvent', slow_handler)

        events: list[BaseEvent] = []

        try:
            for _ in range(150):
                events.append(bus.emit(BaseEvent(event_type='SlowEvent')))

            await asyncio.gather(*events)
            await bus.wait_until_idle()
            assert processed == 150
        finally:
            await bus.destroy(clear=True)

    async def test_zero_history_size_keeps_inflight_and_drops_on_completion(self):
        """max_history_size=0 keeps in-flight events but removes them as soon as they complete."""
        bus = EventBus(name='ZeroHistoryBus', max_history_size=0, max_history_drop=False)

        first_handler_started = asyncio.Event()
        release_handlers = asyncio.Event()

        async def slow_handler(_event: BaseEvent[Any]) -> None:
            first_handler_started.set()
            await release_handlers.wait()

        bus.on('SlowEvent', slow_handler)

        try:
            first = bus.emit(BaseEvent(event_type='SlowEvent'))
            await asyncio.wait_for(first_handler_started.wait(), timeout=1.0)
            second = bus.emit(BaseEvent(event_type='SlowEvent'))

            assert first.event_id in bus.event_history
            assert second.event_id in bus.event_history

            release_handlers.set()
            await asyncio.gather(first, second)
            await bus.wait_until_idle()

            assert len(bus.event_history) == 0
        finally:
            await bus.destroy(clear=True)


class TestHandlerRegistration:
    """Test handler registration and execution"""

    async def test_handler_registration(self, eventbus):
        """Test handler registration via string, model class, and wildcard"""
        results = {'specific': [], 'model': [], 'universal': []}

        # Handler for specific event type by string
        async def user_handler(event: UserActionEvent) -> str:
            results['specific'].append(event.action)
            return 'user_handled'

        # Handler for event type by model class
        async def system_handler(event: SystemEventModel) -> str:
            results['model'].append(event.name)
            return 'system_handled'

        # Universal handler
        async def universal_handler(event: BaseEvent) -> str:
            results['universal'].append(event.event_type)
            return 'universal'

        # Register handlers
        eventbus.on('UserActionEvent', user_handler)
        eventbus.on(SystemEventModel, system_handler)
        eventbus.on('*', universal_handler)

        # Emit events
        eventbus.emit(UserActionEvent(action='login', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        eventbus.emit(SystemEventModel(name='startup'))
        await eventbus.wait_until_idle()

        # Verify all handlers were called correctly
        assert results['specific'] == ['login']
        assert results['model'] == ['startup']
        assert set(results['universal']) == {'UserActionEvent', 'SystemEventModel'}

    async def test_class_matcher_matches_generic_base_event_by_event_type(self, eventbus):
        """Class listeners should still match generic BaseEvent payloads by event_type string."""

        class DifferentNameFromClass(BaseEvent):
            pass

        seen: list[str] = []

        async def class_handler(event: BaseEvent) -> None:
            seen.append(f'class:{event.event_type}')

        async def string_handler(event: BaseEvent) -> None:
            seen.append(f'string:{event.event_type}')

        async def wildcard_handler(event: BaseEvent) -> None:
            seen.append(f'wildcard:{event.event_type}')

        eventbus.on(DifferentNameFromClass, class_handler)
        eventbus.on('DifferentNameFromClass', string_handler)
        eventbus.on('*', wildcard_handler)

        eventbus.emit(BaseEvent(event_type='DifferentNameFromClass'))
        await eventbus.wait_until_idle()

        assert seen == [
            'class:DifferentNameFromClass',
            'string:DifferentNameFromClass',
            'wildcard:DifferentNameFromClass',
        ]
        assert len(eventbus.handlers_by_key.get('DifferentNameFromClass', [])) == 2

    async def test_multiple_handlers_parallel(self, parallel_eventbus):
        """Test that multiple handlers run in parallel"""
        eventbus = parallel_eventbus
        start_times = []
        end_times = []

        async def slow_handler_1(event: BaseEvent) -> str:
            start_times.append(('h1', time.time()))
            await asyncio.sleep(0.1)
            end_times.append(('h1', time.time()))
            return 'handler1'

        async def slow_handler_2(event: BaseEvent) -> str:
            start_times.append(('h2', time.time()))
            await asyncio.sleep(0.1)
            end_times.append(('h2', time.time()))
            return 'handler2'

        # Subscribe both handlers
        eventbus.on('UserActionEvent', slow_handler_1)
        eventbus.on('UserActionEvent', slow_handler_2)

        # Emit event and wait
        start = time.time()
        event = await eventbus.emit(UserActionEvent(action='test', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        duration = time.time() - start

        # Check handlers ran in parallel (should take ~0.1s, not 0.2s)
        assert duration < 0.15
        assert len(start_times) == 2
        assert len(end_times) == 2

        # Check results
        handler1_result = next((r for r in event.event_results.values() if r.handler_name.endswith('slow_handler_1')), None)
        handler2_result = next((r for r in event.event_results.values() if r.handler_name.endswith('slow_handler_2')), None)
        assert handler1_result is not None and handler1_result.result == 'handler1'
        assert handler2_result is not None and handler2_result.result == 'handler2'

    def test_handler_can_be_sync_or_async(self):
        """Test that both sync and async handlers are accepted"""
        bus = EventBus()

        def sync_handler(event: BaseEvent) -> str:
            return 'sync'

        async def async_handler(event: BaseEvent) -> str:
            return 'async'

        # Both should work
        bus.on('TestEvent', sync_handler)
        bus.on('TestEvent', async_handler)

        # Check both were registered
        assert len(bus.handlers_by_key.get('TestEvent', [])) == 2

    async def test_class_and_instance_method_handlers(self, eventbus):
        """Test using class and instance methods as handlers"""
        results = []

        class EventProcessor:
            def __init__(self, name: str, value: int):
                self.name = name
                self.value = value

            def sync_method_handler(self, event: UserActionEvent) -> dict:
                """Sync instance method handler"""
                results.append(f'{self.name}_sync')
                return {'processor': self.name, 'value': self.value, 'action': event.action}

            async def async_method_handler(self, event: UserActionEvent) -> dict:
                """Async instance method handler"""
                await asyncio.sleep(0.01)  # Simulate some async work
                results.append(f'{self.name}_async')
                return {'processor': self.name, 'value': self.value * 2, 'action': event.action}

            @classmethod
            def class_method_handler(cls, event: UserActionEvent) -> str:
                """Class method handler"""
                results.append('classmethod')
                return f'Handled by {cls.__name__}'

            @staticmethod
            def static_method_handler(event: UserActionEvent) -> str:
                """Static method handler"""
                results.append('staticmethod')
                return 'Handled by static method'

        # Create instances
        processor1 = EventProcessor('Processor1', 10)
        processor2 = EventProcessor('Processor2', 20)

        # Register instance methods (suppress warning about same-named handlers from different instances)
        import warnings

        eventbus.on(UserActionEvent, processor1.sync_method_handler)
        eventbus.on(UserActionEvent, processor1.async_method_handler)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            eventbus.on(UserActionEvent, processor2.sync_method_handler)

        # Register class and static methods
        eventbus.on('UserActionEvent', EventProcessor.class_method_handler)
        eventbus.on('UserActionEvent', EventProcessor.static_method_handler)

        # Dispatch event
        event = UserActionEvent(action='test_methods', user_id='dab45f48-9e3a-7042-80f8-ac8f07b6cfe3')
        completed_event = await eventbus.emit(event)

        # Verify all handlers were called
        assert len(results) == 5
        assert 'Processor1_sync' in results
        assert 'Processor1_async' in results
        assert 'Processor2_sync' in results
        assert 'classmethod' in results
        assert 'staticmethod' in results

        # Verify results contain expected data
        results_list = await completed_event.event_results_list()

        # Find processor1 sync result
        p1_sync_result = next(
            r for r in results_list if isinstance(r, dict) and r.get('processor') == 'Processor1' and r.get('value') == 10
        )
        assert p1_sync_result['action'] == 'test_methods'

        # Find processor1 async result (value doubled)
        p1_async_result = next(
            r for r in results_list if isinstance(r, dict) and r.get('processor') == 'Processor1' and r.get('value') == 20
        )
        assert p1_async_result['action'] == 'test_methods'

        # Find processor2 sync result
        p2_sync_result = next(r for r in results_list if isinstance(r, dict) and r.get('processor') == 'Processor2')
        assert p2_sync_result['value'] == 20
        assert p2_sync_result['action'] == 'test_methods'

        # Verify class and static method results
        assert 'Handled by EventProcessor' in results_list
        assert 'Handled by static method' in results_list


class TestEventForwarding:
    """Tests for event forwarding between buses."""

    @pytest.mark.asyncio
    async def test_forwarding_loop_prevention(self):
        bus_a = EventBus(name='ForwardBusA')
        bus_b = EventBus(name='ForwardBusB')
        bus_c = EventBus(name='ForwardBusC')

        class LoopEvent(BaseEvent[str]):
            pass

        seen: dict[str, int] = {'A': 0, 'B': 0, 'C': 0}

        async def handler_a(event: LoopEvent) -> str:
            seen['A'] += 1
            return 'handled-a'

        async def handler_b(event: LoopEvent) -> str:
            seen['B'] += 1
            return 'handled-b'

        async def handler_c(event: LoopEvent) -> str:
            seen['C'] += 1
            return 'handled-c'

        bus_a.on(LoopEvent, handler_a)
        bus_b.on(LoopEvent, handler_b)
        bus_c.on(LoopEvent, handler_c)

        # Create a forwarding cycle A -> B -> C -> A, which should be broken automatically.
        bus_a.on('*', bus_b.emit)
        bus_b.on('*', bus_c.emit)
        bus_c.on('*', bus_a.emit)

        try:
            event = await bus_a.emit(LoopEvent())

            await bus_a.wait_until_idle()
            await bus_b.wait_until_idle()
            await bus_c.wait_until_idle()

            assert seen == {'A': 1, 'B': 1, 'C': 1}
            assert event.event_path == [bus_a.label, bus_b.label, bus_c.label]
        finally:
            await bus_a.destroy(clear=True)
            await bus_b.destroy(clear=True)
            await bus_c.destroy(clear=True)


class TestFIFOOrdering:
    """Test FIFO event processing"""

    async def test_fifo_with_varying_handler_delays(self, eventbus):
        """Test FIFO order is maintained with varying handler processing times"""
        processed_order = []
        handler_start_times = []

        async def handler(event: UserActionEvent) -> int:
            order = event.metadata.get('order', -1)
            handler_start_times.append((order, asyncio.get_event_loop().time()))
            # Variable delays to test ordering
            if order % 2 == 0:
                await asyncio.sleep(0.05)  # Even events take longer
            else:
                await asyncio.sleep(0.01)  # Odd events are quick
            processed_order.append(order)
            return order

        eventbus.on('UserActionEvent', handler)

        # Emit 20 events rapidly
        for i in range(20):
            eventbus.emit(
                UserActionEvent(action=f'test_{i}', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced', metadata={'order': i})
            )

        await eventbus.wait_until_idle()

        # Verify FIFO order maintained
        assert processed_order == list(range(20))
        # Verify handler start times are in order
        for i in range(1, len(handler_start_times)):
            assert handler_start_times[i][1] >= handler_start_times[i - 1][1]


class TestErrorHandling:
    """Test error handling in handlers"""

    async def test_error_handling(self, eventbus):
        """Test handler error capture and isolation"""
        results = []

        async def failing_handler(event: BaseEvent) -> str:
            raise ValueError('Expected to fail - testing error handling in event handlers')

        async def working_handler(event: BaseEvent) -> str:
            results.append('success')
            return 'worked'

        # Register both handlers
        eventbus.on('UserActionEvent', failing_handler)
        eventbus.on('UserActionEvent', working_handler)

        # Emit and wait for result
        event = await eventbus.emit(UserActionEvent(action='test', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))

        # Verify error capture and isolation
        failing_result = next((r for r in event.event_results.values() if r.handler_name.endswith('failing_handler')), None)
        assert failing_result is not None
        assert failing_result.status == 'error'
        assert 'Expected to fail' in str(failing_result.error)
        working_result = next((r for r in event.event_results.values() if r.handler_name.endswith('working_handler')), None)
        assert working_result is not None
        assert working_result.result == 'worked'
        assert results == ['success']

    async def test_event_result_raises_exception_group_when_multiple_handlers_fail(self, eventbus):
        """event_result() should raise ExceptionGroup when multiple handler failures exist."""

        async def failing_handler_one(event: BaseEvent) -> str:
            raise ValueError('first failure')

        async def failing_handler_two(event: BaseEvent) -> str:
            raise RuntimeError('second failure')

        eventbus.on('UserActionEvent', failing_handler_one)
        eventbus.on('UserActionEvent', failing_handler_two)

        event = await eventbus.emit(UserActionEvent(action='test', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))

        with pytest.raises(ExceptionGroup) as exc_info:
            await event.event_result()

        grouped_errors = exc_info.value.exceptions
        assert len(grouped_errors) == 2
        assert {type(err) for err in grouped_errors} == {ValueError, RuntimeError}

    async def test_event_result_single_handler_error_raises_original_exception(self, eventbus):
        """event_result() should preserve original exception type when only one handler fails."""

        async def failing_handler(event: BaseEvent) -> str:
            raise ValueError('single failure')

        eventbus.on('UserActionEvent', failing_handler)

        event = await eventbus.emit(UserActionEvent(action='test', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))

        with pytest.raises(ValueError, match='single failure'):
            await event.event_result()


class TestBatchOperations:
    """Test batch event operations"""

    async def test_batch_emit_with_gather(self, eventbus):
        """Test batch event emission with asyncio.gather"""
        events = [
            UserActionEvent(action='login', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'),
            SystemEventModel(name='startup'),
            UserActionEvent(action='logout', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'),
        ]

        # Enqueue batch
        emitted_events = [eventbus.emit(event) for event in events]
        results = await asyncio.gather(*emitted_events)

        # Check all processed
        assert len(results) == 3
        for result in results:
            assert result.event_completed_at is not None


class TestWriteAheadLog:
    """Test write-ahead logging functionality"""

    async def test_write_ahead_log_captures_all_events(self, eventbus):
        """Test that all events are captured in write-ahead log"""
        # Emit several events
        events = []
        for i in range(5):
            event = UserActionEvent(action=f'action_{i}', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced')
            events.append(eventbus.emit(event))

        await eventbus.wait_until_idle()

        # Check write-ahead log
        log = eventbus.event_history.copy()
        assert len(log) == 5
        for i, event in enumerate(log.values()):
            assert event.action == f'action_{i}'

        # Check event state properties
        completed = eventbus.events_completed
        pending = eventbus.events_pending
        processing = eventbus.events_started
        assert len(completed) + len(pending) + len(processing) == len(log)
        assert len(completed) == 5  # All events should be completed
        assert len(pending) == 0  # No events should be pending
        assert len(processing) == 0  # No events should be processing


class TestEventCompletion:
    """Test event completion tracking"""

    async def test_wait_for_result(self, eventbus):
        """Test waiting for event completion"""
        completion_order = []

        async def slow_handler(event: BaseEvent) -> str:
            await asyncio.sleep(0.1)
            completion_order.append('handler_done')
            return 'done'

        eventbus.on('UserActionEvent', slow_handler)

        # Enqueue without waiting
        event = eventbus.emit(UserActionEvent(action='test', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        completion_order.append('enqueue_done')

        # Wait for completion
        event = await event
        completion_order.append('wait_done')

        # Check order
        assert completion_order == ['enqueue_done', 'handler_done', 'wait_done']
        assert event.event_completed_at is not None


class TestEdgeCases:
    """Test edge cases and special scenarios"""

    async def test_destroy_with_pending_events(self):
        """Test destroying event bus with events still in queue"""
        bus = EventBus()

        # Add a slow handler
        async def slow_handler(event: BaseEvent) -> str:
            await asyncio.sleep(1)
            return 'done'

        bus.on('*', slow_handler)

        # Enqueue events but don't wait
        for i in range(5):
            bus.emit(UserActionEvent(action=f'action_{i}', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))

        # Destroy immediately
        await bus.destroy()

        # Bus should destroy even with pending events
        assert not bus._is_running

    async def test_event_with_complex_data(self, eventbus):
        """Test events with complex nested data"""
        complex_data = {
            'nested': {
                'list': [1, 2, {'inner': 'value'}],
                'datetime': datetime.now(timezone.utc),
                'none': None,
            }
        }

        event = SystemEventModel(name='complex', details=complex_data)

        result = await eventbus.emit(event)

        # Check data preserved
        assert result.details['nested']['list'][2]['inner'] == 'value'

    async def test_concurrent_emit_calls(self, eventbus):
        """Test multiple concurrent emit calls"""
        # Create many events concurrently in batches to keep this test deterministic.
        total_events = 100
        batch_size = 50
        all_tasks = []

        for batch_start in range(0, total_events, batch_size):
            batch_end = min(batch_start + batch_size, total_events)
            batch_tasks = []

            for i in range(batch_start, batch_end):
                event = UserActionEvent(action=f'concurrent_{i}', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced')
                # Emit returns the event syncresultsonously, but we need to wait for completion
                emitted_event = eventbus.emit(event)
                batch_tasks.append(emitted_event)

            # Wait for this batch to complete before starting the next
            await asyncio.gather(*batch_tasks)
            all_tasks.extend(batch_tasks)

        # Wait for processing
        await eventbus.wait_until_idle()

        # Check all events in log
        log = eventbus.event_history.copy()
        assert len(log) == 100

    async def test_mixed_delay_handlers_maintain_order(self, eventbus):
        """Test that events with different handler delays still maintain FIFO order"""
        collected_orders = []
        handler_start_times = []

        async def handler(event: UserActionEvent):
            order = event.metadata.get('order', -1)
            handler_start_times.append((order, asyncio.get_event_loop().time()))
            # Simulate varying processing times
            if order % 2 == 0:
                await asyncio.sleep(0.05)  # Even events take longer
            else:
                await asyncio.sleep(0.01)  # Odd events are quick
            collected_orders.append(order)
            return f'handled_{order}'

        eventbus.on('UserActionEvent', handler)

        # Emit events
        num_events = 20
        for i in range(num_events):
            event = UserActionEvent(action=f'mixed_{i}', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced', metadata={'order': i})
            eventbus.emit(event)

        # Wait for all events to process
        await eventbus.wait_until_idle()

        # Verify exact FIFO order despite different processing times
        assert collected_orders == list(range(num_events)), f'Events processed out of order: {collected_orders}'

        # Verify handler start times are in order (events are dequeued in FIFO order)
        for i in range(1, len(handler_start_times)):
            prev_order, prev_time = handler_start_times[i - 1]
            curr_order, curr_time = handler_start_times[i]
            assert curr_time >= prev_time, f'Event {curr_order} started before event {prev_order}'


class TestEventTypeOverride:
    """Test that Event subclasses properly override event_type"""

    async def test_event_subclass_type(self, eventbus):
        """Test that event subclasses maintain their type"""

        # Create a specific event type
        event = CreateAgentTaskEvent(
            user_id='371bbd3c-5231-7ff0-8aef-e63732a8d40f',
            agent_session_id='12345678-1234-5678-1234-567812345678',
            llm_model='test-model',
            task='test task',
        )

        # Enqueue it
        result = eventbus.emit(event)

        # Check type is preserved - should be class name
        assert result.event_type == 'CreateAgentTaskEvent'
        assert isinstance(result, BaseEvent)

    async def test_event_type_and_version_identity_fields(self, eventbus):
        """event_type + event_version identify payload shape"""
        base_event = BaseEvent(event_type='TestEvent')
        assert base_event.event_type == 'TestEvent'
        assert base_event.event_version == '0.0.1'

        task_event = CreateAgentTaskEvent(
            user_id='371bbd3c-5231-7ff0-8aef-e63732a8d40f',
            agent_session_id='12345678-1234-5678-1234-567812345678',
            llm_model='test-model',
            task='test task',
        )
        assert task_event.event_type == 'CreateAgentTaskEvent'
        assert task_event.event_version == '0.0.1'

        # Check identity fields are preserved after emit
        result = eventbus.emit(task_event)
        assert result.event_type == task_event.event_type
        assert result.event_version == task_event.event_version

    async def test_event_version_defaults_and_overrides(self, eventbus):
        """event_version supports class defaults, runtime override, and JSON roundtrip."""

        base_event = BaseEvent(event_type='TestVersionEvent')
        assert base_event.event_version == '0.0.1'

        class VersionedEvent(BaseEvent):
            event_version = '1.2.3'
            data: str

        class_default = VersionedEvent(data='x')
        assert class_default.event_version == '1.2.3'

        runtime_override = VersionedEvent(data='x', event_version='9.9.9')
        assert runtime_override.event_version == '9.9.9'

        dispatched = eventbus.emit(VersionedEvent(data='queued'))
        assert dispatched.event_version == '1.2.3'

        restored = BaseEvent.model_validate(dispatched.model_dump(mode='json'))
        assert restored.event_version == '1.2.3'

    async def test_automatic_event_type_derivation(self, eventbus):
        """Test that event_type is automatically derived from class name when not specified"""

        # Test automatic derivation
        event = UserActionEvent(action='test', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced')
        assert event.event_type == 'UserActionEvent'

        event2 = SystemEventModel(name='startup')
        assert event2.event_type == 'SystemEventModel'

        # Create inline event class without explicit event_type
        class InlineTestEvent(BaseEvent):
            data: str

        inline_event = InlineTestEvent(data='test')
        assert inline_event.event_type == 'InlineTestEvent'

        # Test with EventBus
        received = []

        async def handler(event):
            received.append(event)

        eventbus.on('UserActionEvent', handler)
        eventbus.on('InlineTestEvent', handler)

        await eventbus.emit(event)
        await eventbus.emit(inline_event)
        await eventbus.wait_until_idle()

        assert len(received) == 2
        assert received[0].event_type == 'UserActionEvent'
        assert received[1].event_type == 'InlineTestEvent'

    async def test_explicit_event_type_override(self, eventbus):
        """Test that explicit event_type can still override the automatic derivation"""

        # Create event with explicit event_type override
        class OverrideEvent(BaseEvent):
            event_type: str = Field(default='CustomEventType', frozen=True)
            data: str

        event = OverrideEvent(data='test')
        assert event.event_type == 'CustomEventType'  # Not 'OverrideEvent'

        # Test with EventBus
        received = []

        async def handler(event):
            received.append(event)

        eventbus.on('CustomEventType', handler)
        eventbus.on('OverrideEvent', handler)  # This won't match

        await eventbus.emit(event)
        await eventbus.wait_until_idle()

        assert len(received) == 1
        assert received[0].event_type == 'CustomEventType'


class TestEventBusHierarchy:
    """Test hierarchical EventBus subscription patterns"""

    async def test_tresultsee_level_hierarchy_bubbling(self):
        """Test that events bubble up tresultsough a 3-level hierarchy and event_path is correct"""
        # Create tresultsee EventBus instances in a hierarchy
        parent_bus = EventBus(name='ParentBus')
        child_bus = EventBus(name='ChildBus')
        subchild_bus = EventBus(name='SubchildBus')

        # Track events received at each level
        events_at_parent = []
        events_at_child = []
        events_at_subchild = []

        async def parent_handler(event: BaseEvent) -> str:
            events_at_parent.append(event)
            return 'parent_received'

        async def child_handler(event: BaseEvent) -> str:
            events_at_child.append(event)
            return 'child_received'

        async def subchild_handler(event: BaseEvent) -> str:
            events_at_subchild.append(event)
            return 'subchild_received'

        # Register handlers
        parent_bus.on('*', parent_handler)
        child_bus.on('*', child_handler)
        subchild_bus.on('*', subchild_handler)

        # Subscribe buses to each other: parent <- child <- subchild
        # Child forwards events to parent
        child_bus.on('*', parent_bus.emit)
        # Subchild forwards events to child
        subchild_bus.on('*', child_bus.emit)

        try:
            # Emit event from the bottom of hierarchy
            event = UserActionEvent(action='bubble_test', user_id='371bbd3c-5231-7ff0-8aef-e63732a8d40f')
            emitted = subchild_bus.emit(event)

            # Wait for event to bubble up
            await subchild_bus.wait_until_idle()
            await child_bus.wait_until_idle()
            await parent_bus.wait_until_idle()

            # Verify event was received at all levels
            assert len(events_at_subchild) == 1
            assert len(events_at_child) == 1
            assert len(events_at_parent) == 1

            # Verify event_path shows the complete journey
            final_event = events_at_parent[0]
            assert final_event.event_path == [subchild_bus.label, child_bus.label, parent_bus.label]

            # Verify it's the same event content
            assert final_event.action == 'bubble_test'
            assert final_event.user_id == '371bbd3c-5231-7ff0-8aef-e63732a8d40f'
            assert final_event.event_id == emitted.event_id

            # Test event emitted at middle level
            events_at_parent.clear()
            events_at_child.clear()
            events_at_subchild.clear()

            middle_event = SystemEventModel(name='middle_test')
            child_bus.emit(middle_event)

            await child_bus.wait_until_idle()
            await parent_bus.wait_until_idle()

            # Should only reach child and parent, not subchild
            assert len(events_at_subchild) == 0
            assert len(events_at_child) == 1
            assert len(events_at_parent) == 1
            assert events_at_parent[0].event_path == [child_bus.label, parent_bus.label]

        finally:
            await parent_bus.destroy()
            await child_bus.destroy()
            await subchild_bus.destroy()

    async def test_circular_subscription_prevention(self):
        """Test that circular EventBus subscriptions don't create infinite loops"""
        # Create tresultsee peer EventBus instances
        peer1 = EventBus(name='Peer1')
        peer2 = EventBus(name='Peer2')
        peer3 = EventBus(name='Peer3')

        # Track events at each peer
        events_at_peer1 = []
        events_at_peer2 = []
        events_at_peer3 = []

        async def peer1_handler(event: BaseEvent) -> str:
            events_at_peer1.append(event)
            return 'peer1_received'

        async def peer2_handler(event: BaseEvent) -> str:
            events_at_peer2.append(event)
            return 'peer2_received'

        async def peer3_handler(event: BaseEvent) -> str:
            events_at_peer3.append(event)
            return 'peer3_received'

        # Register handlers
        peer1.on('*', peer1_handler)
        peer2.on('*', peer2_handler)
        peer3.on('*', peer3_handler)

        # Create circular subscription: peer1 -> peer2 -> peer3 -> peer1
        peer1.on('*', peer2.emit)
        peer2.on('*', peer3.emit)
        peer3.on('*', peer1.emit)  # This completes the circle

        def dump_bus_state() -> str:
            buses = [peer1, peer2, peer3]
            lines: list[str] = []
            for bus in buses:
                queue_size = bus.pending_event_queue.qsize() if bus.pending_event_queue else 0
                lines.append(
                    f'{bus.label} queue={queue_size} active={len(bus.in_flight_event_ids)} processing={len(bus.processing_event_ids)} history={len(bus.event_history)}'
                )
            lines.append('--- peer1.log_tree() ---')
            lines.append(peer1.log_tree())
            lines.append('--- peer2.log_tree() ---')
            lines.append(peer2.log_tree())
            lines.append('--- peer3.log_tree() ---')
            lines.append(peer3.log_tree())
            return '\n'.join(lines)

        try:
            # Emit event from peer1
            event = UserActionEvent(action='circular_test', user_id='371bbd3c-5231-7ff0-8aef-e63732a8d40f')
            emitted = peer1.emit(event)

            # Wait for all processing to complete
            await asyncio.sleep(0.2)  # Give time for any potential loops
            try:
                await asyncio.wait_for(peer1.wait_until_idle(), timeout=5)
                await asyncio.wait_for(peer2.wait_until_idle(), timeout=5)
                await asyncio.wait_for(peer3.wait_until_idle(), timeout=5)
            except TimeoutError:
                pytest.fail(f'Circular test stalled during first propagation.\n{dump_bus_state()}')

            # Each peer should receive the event exactly once
            assert len(events_at_peer1) == 1
            assert len(events_at_peer2) == 1
            assert len(events_at_peer3) == 1

            # Check event paths show the propagation but no loops
            assert events_at_peer1[0].event_path == [peer1.label, peer2.label, peer3.label]
            assert events_at_peer2[0].event_path == [peer1.label, peer2.label, peer3.label]
            assert events_at_peer3[0].event_path == [peer1.label, peer2.label, peer3.label]

            # The event should NOT come back to peer1 from peer3
            # because peer3's emit handler will detect peer1 is already in the path

            # Verify all events have the same ID (same event, not duplicates)
            assert all(e.event_id == emitted.event_id for e in [events_at_peer1[0], events_at_peer2[0], events_at_peer3[0]])

            # Test starting from a different peer
            events_at_peer1.clear()
            events_at_peer2.clear()
            events_at_peer3.clear()

            event2 = SystemEventModel(name='circular_test_2')
            peer2.emit(event2)

            await asyncio.sleep(0.2)
            try:
                await asyncio.wait_for(peer1.wait_until_idle(), timeout=5)
                await asyncio.wait_for(peer2.wait_until_idle(), timeout=5)
                await asyncio.wait_for(peer3.wait_until_idle(), timeout=5)
            except TimeoutError:
                pytest.fail(f'Circular test stalled during second propagation.\n{dump_bus_state()}')

            # Should visit peer2 -> peer3 -> peer1, then stop
            assert len(events_at_peer1) == 1
            assert len(events_at_peer2) == 1
            assert len(events_at_peer3) == 1

            assert events_at_peer2[0].event_path == [peer2.label, peer3.label, peer1.label]
            assert events_at_peer3[0].event_path == [peer2.label, peer3.label, peer1.label]
            assert events_at_peer1[0].event_path == [peer2.label, peer3.label, peer1.label]

        finally:
            await peer1.destroy()
            await peer2.destroy()
            await peer3.destroy()


class TestFindMethod:
    """Test find() behavior for future waits and filtering."""

    async def test_find_future_basic(self, eventbus):
        """Test basic future find functionality."""
        # Start waiting for an event that hasn't been dispatched yet
        find_task = asyncio.create_task(eventbus.find('UserActionEvent', past=False, future=1.0))

        # Give find time to register waiter
        await asyncio.sleep(0.01)

        # Dispatch the event
        dispatched = eventbus.emit(UserActionEvent(action='login', user_id='50d357df-e68c-7111-8a6c-7018569514b0'))

        # Wait for find to resolve
        received = await find_task

        # Verify we got the right event
        assert received.event_type == 'UserActionEvent'
        assert received.action == 'login'
        assert received.user_id == '50d357df-e68c-7111-8a6c-7018569514b0'
        assert received.event_id == dispatched.event_id

    async def test_find_future_with_predicate(self, eventbus):
        """Test future find with where predicate filtering."""
        # Dispatch some events that don't match
        eventbus.emit(UserActionEvent(action='logout', user_id='eab58ec9-90ea-7758-893f-afed99518f43'))
        eventbus.emit(UserActionEvent(action='login', user_id='dce05df3-8e9b-7159-84f9-5ab894dddbd7'))

        find_task = asyncio.create_task(
            eventbus.find(
                'UserActionEvent', where=lambda e: e.user_id == '50d357df-e68c-7111-8a6c-7018569514b0', past=False, future=1.0
            )
        )

        # Give find time to register
        await asyncio.sleep(0.01)

        # Dispatch more events
        eventbus.emit(UserActionEvent(action='update', user_id='eab58ec9-90ea-7758-893f-afed99518f43'))
        target_event = eventbus.emit(UserActionEvent(action='login', user_id='50d357df-e68c-7111-8a6c-7018569514b0'))
        eventbus.emit(UserActionEvent(action='delete', user_id='dce05df3-8e9b-7159-84f9-5ab894dddbd7'))

        # Wait for the matching event
        received = await find_task

        # Should get the event matching the predicate
        assert received.user_id == '50d357df-e68c-7111-8a6c-7018569514b0'
        assert received.event_id == target_event.event_id

    async def test_find_future_timeout(self, eventbus):
        """Test future find timeout behavior."""
        result = await eventbus.find('NonExistentEvent', past=False, future=0.1)
        assert result is None

    async def test_find_future_with_model_class(self, eventbus):
        """Test future find with model class instead of string."""
        find_task = asyncio.create_task(eventbus.find(SystemEventModel, past=False, future=1.0))

        await asyncio.sleep(0.01)

        # Dispatch different event types
        eventbus.emit(UserActionEvent(action='test', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        target = eventbus.emit(SystemEventModel(name='startup', severity='info'))

        # Should receive the SystemEventModel
        received = await find_task
        assert isinstance(received, SystemEventModel)
        assert received.name == 'startup'
        assert received.event_id == target.event_id

    async def test_multiple_concurrent_future_finds(self, eventbus):
        """Test multiple concurrent future find calls."""
        find1 = asyncio.create_task(
            eventbus.find('UserActionEvent', where=lambda e: e.action == 'normal', past=False, future=2.0)
        )
        find2 = asyncio.create_task(eventbus.find('SystemEventModel', past=False, future=2.0))
        find3 = asyncio.create_task(
            eventbus.find('UserActionEvent', where=lambda e: e.action == 'special', past=False, future=2.0)
        )

        await asyncio.sleep(0.1)  # Give more time for handlers to register

        # Dispatch events
        e1 = eventbus.emit(UserActionEvent(action='normal', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        e2 = eventbus.emit(SystemEventModel(name='test'))
        e3 = eventbus.emit(UserActionEvent(action='special', user_id='2a312e4d-3035-7883-86b9-578ce47046b2'))

        # Wait for all events to be processed
        await eventbus.wait_until_idle()

        # Wait for all find tasks
        r1, r2, r3 = await asyncio.gather(find1, find2, find3)

        # Verify results
        assert r1.event_id == e1.event_id  # Normal UserActionEvent
        assert r2.event_id == e2.event_id  # SystemEventModel
        assert r3.event_id == e3.event_id  # Special UserActionEvent

    async def test_find_waiter_cleanup(self, eventbus):
        """Test that temporary find waiters are properly cleaned up."""
        initial_waiters = len(eventbus.find_waiters)
        result = await eventbus.find('TestEvent', past=False, future=0.1)
        assert result is None
        assert len(eventbus.find_waiters) == initial_waiters

        find_task = asyncio.create_task(eventbus.find('TestEvent2', past=False, future=1.0))
        await asyncio.sleep(0.01)
        eventbus.emit(BaseEvent(event_type='TestEvent2'))
        await find_task
        assert len(eventbus.find_waiters) == initial_waiters

    async def test_find_future_receives_dispatched_event_before_completion(self, eventbus):
        """Test that future find resolves before slow handlers complete."""
        processing_complete = False

        async def slow_handler(event: BaseEvent) -> str:
            await asyncio.sleep(0.1)
            nonlocal processing_complete
            processing_complete = True
            return 'done'

        # Register a slow handler
        eventbus.on('SlowEvent', slow_handler)

        # Start future find
        find_task = asyncio.create_task(eventbus.find('SlowEvent', past=False, future=1.0))

        await asyncio.sleep(0.01)

        # Dispatch event
        eventbus.emit(BaseEvent(event_type='SlowEvent'))

        # Wait for find
        received = await find_task

        assert received.event_type == 'SlowEvent'
        assert processing_complete is False

        # Find resolves on dispatch; handler result entries may or may not exist yet.
        slow_result = next(
            (res for res in received.event_results.values() if res.handler_name.endswith('slow_handler')),
            None,
        )
        if slow_result is not None:
            assert slow_result.status != 'completed'

        await eventbus.wait_until_idle()
        assert processing_complete is True


class TestFindPastMethod:
    """Tests for history-only find behavior."""

    async def test_find_past_returns_most_recent(self, eventbus):
        # Dispatch two events and ensure the newest is returned
        eventbus.emit(UserActionEvent(action='first', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        latest = eventbus.emit(UserActionEvent(action='second', user_id='2a312e4d-3035-7883-86b9-578ce47046b2'))
        await eventbus.wait_until_idle()

        match = await eventbus.find('UserActionEvent', past=10, future=False)
        assert match is not None
        assert match.event_id == latest.event_id

    async def test_find_past_respects_time_window(self, eventbus):
        event = eventbus.emit(UserActionEvent(action='old', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        await eventbus.wait_until_idle()
        old_created_at = datetime.fromisoformat(event.event_created_at) - timedelta(seconds=30)
        event.event_created_at = monotonic_datetime(old_created_at.isoformat().replace('+00:00', 'Z'))

        match = await eventbus.find('UserActionEvent', past=10, future=False)
        assert match is None

    async def test_find_past_can_match_incomplete_events(self, eventbus):
        processing = asyncio.Event()

        async def slow_handler(evt: UserActionEvent) -> None:
            await asyncio.sleep(0.05)
            processing.set()

        eventbus.on('UserActionEvent', slow_handler)

        pending_event = eventbus.emit(UserActionEvent(action='slow', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))

        # While handler is running, past find can still match in-flight events
        in_flight = await eventbus.find('UserActionEvent', past=10, future=False)
        assert in_flight is not None
        assert in_flight.event_id == pending_event.event_id

        await pending_event
        await processing.wait()

        match = await eventbus.find('UserActionEvent', past=10, future=False)
        assert match is not None
        assert match.event_id == pending_event.event_id


class TestFilterMethod:
    """Tests for filter() which returns all matching events newest-to-oldest."""

    async def test_filter_past_returns_all_matches_newest_first(self, eventbus):
        first = eventbus.emit(UserActionEvent(action='first', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        second = eventbus.emit(UserActionEvent(action='second', user_id='2a312e4d-3035-7883-86b9-578ce47046b2'))
        third = eventbus.emit(UserActionEvent(action='third', user_id='6eb8a717-e19d-728b-8905-97f7e20c002e'))
        await eventbus.wait_until_idle()

        matches = await eventbus.filter('UserActionEvent', past=10, future=False)
        assert [m.event_id for m in matches] == [third.event_id, second.event_id, first.event_id]

    async def test_filter_returns_empty_list_when_no_matches(self, eventbus):
        result = await eventbus.filter('NonExistentEvent', past=True, future=False)
        assert result == []

    async def test_filter_respects_limit(self, eventbus):
        eventbus.emit(UserActionEvent(action='a', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        second = eventbus.emit(UserActionEvent(action='b', user_id='2a312e4d-3035-7883-86b9-578ce47046b2'))
        third = eventbus.emit(UserActionEvent(action='c', user_id='6eb8a717-e19d-728b-8905-97f7e20c002e'))
        await eventbus.wait_until_idle()

        matches = await eventbus.filter('UserActionEvent', past=10, future=False, limit=2)
        assert [m.event_id for m in matches] == [third.event_id, second.event_id]

    async def test_filter_with_where_predicate(self, eventbus):
        eventbus.emit(UserActionEvent(action='login', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        target = eventbus.emit(UserActionEvent(action='logout', user_id='2a312e4d-3035-7883-86b9-578ce47046b2'))
        eventbus.emit(UserActionEvent(action='login', user_id='6eb8a717-e19d-728b-8905-97f7e20c002e'))
        await eventbus.wait_until_idle()

        matches = await eventbus.filter('UserActionEvent', where=lambda e: e.action == 'logout', past=10, future=False)
        assert [m.event_id for m in matches] == [target.event_id]

    async def test_filter_with_field_equality(self, eventbus):
        eventbus.emit(UserActionEvent(action='a', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        target = eventbus.emit(UserActionEvent(action='b', user_id='2a312e4d-3035-7883-86b9-578ce47046b2'))
        await eventbus.wait_until_idle()

        matches = await eventbus.filter('UserActionEvent', past=10, future=False, action='b')
        assert [m.event_id for m in matches] == [target.event_id]

    async def test_filter_with_model_class(self, eventbus):
        eventbus.emit(UserActionEvent(action='ignore', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        sys1 = eventbus.emit(SystemEventModel(name='one'))
        sys2 = eventbus.emit(SystemEventModel(name='two'))
        await eventbus.wait_until_idle()

        matches = await eventbus.filter(SystemEventModel, past=10, future=False)
        assert [m.event_id for m in matches] == [sys2.event_id, sys1.event_id]
        assert all(isinstance(m, SystemEventModel) for m in matches)

    async def test_filter_wildcard_matches_all_event_types(self, eventbus):
        ev_a = eventbus.emit(UserActionEvent(action='a', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        ev_b = eventbus.emit(SystemEventModel(name='b'))
        await eventbus.wait_until_idle()

        matches = await eventbus.filter('*', past=10, future=False)
        assert [m.event_id for m in matches] == [ev_b.event_id, ev_a.event_id]

    async def test_filter_past_respects_time_window(self, eventbus):
        old = eventbus.emit(UserActionEvent(action='old', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        await eventbus.wait_until_idle()
        old_created_at = datetime.fromisoformat(old.event_created_at) - timedelta(seconds=30)
        old.event_created_at = monotonic_datetime(old_created_at.isoformat().replace('+00:00', 'Z'))
        recent = eventbus.emit(UserActionEvent(action='recent', user_id='2a312e4d-3035-7883-86b9-578ce47046b2'))
        await eventbus.wait_until_idle()

        matches = await eventbus.filter('UserActionEvent', past=10, future=False)
        assert [m.event_id for m in matches] == [recent.event_id]

    async def test_filter_past_false_future_false_returns_empty(self, eventbus):
        eventbus.emit(UserActionEvent(action='x', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        await eventbus.wait_until_idle()
        result = await eventbus.filter('UserActionEvent', past=False, future=False)
        assert result == []

    async def test_filter_future_appends_match(self, eventbus):
        prior = eventbus.emit(UserActionEvent(action='past', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        await eventbus.wait_until_idle()

        async def emit_after_delay():
            await asyncio.sleep(0.02)
            return eventbus.emit(UserActionEvent(action='future', user_id='2a312e4d-3035-7883-86b9-578ce47046b2'))

        emit_task = asyncio.create_task(emit_after_delay())
        matches = await eventbus.filter('UserActionEvent', past=True, future=0.5)
        future_event = await emit_task

        # Past match comes first (returned immediately since limit not specified means we still look),
        # but with no limit and future set, we collect past matches then wait for one future event.
        assert len(matches) == 2
        assert matches[0].event_id == prior.event_id
        assert matches[1].event_id == future_event.event_id

    async def test_filter_limit_short_circuits_future_wait(self, eventbus):
        first = eventbus.emit(UserActionEvent(action='a', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        await eventbus.wait_until_idle()

        start = time.monotonic()
        matches = await eventbus.filter('UserActionEvent', past=True, future=2.0, limit=1)
        elapsed = time.monotonic() - start

        assert [m.event_id for m in matches] == [first.event_id]
        assert elapsed < 0.5

    async def test_filter_future_only_returns_dispatched_event(self, eventbus):
        async def emit_after_delay():
            await asyncio.sleep(0.02)
            return eventbus.emit(UserActionEvent(action='future', user_id='2a312e4d-3035-7883-86b9-578ce47046b2'))

        emit_task = asyncio.create_task(emit_after_delay())
        matches = await eventbus.filter('UserActionEvent', past=False, future=0.5)
        dispatched = await emit_task

        assert [m.event_id for m in matches] == [dispatched.event_id]

    async def test_filter_future_only_times_out(self, eventbus):
        matches = await eventbus.filter('NonExistentEvent', past=False, future=0.05)
        assert matches == []

    async def test_find_returns_first_filter_result(self, eventbus):
        eventbus.emit(UserActionEvent(action='first', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        latest = eventbus.emit(UserActionEvent(action='second', user_id='2a312e4d-3035-7883-86b9-578ce47046b2'))
        await eventbus.wait_until_idle()

        find_match = await eventbus.find('UserActionEvent', past=10, future=False)
        filter_matches = await eventbus.filter('UserActionEvent', past=10, future=False, limit=1)

        assert find_match is not None
        assert len(filter_matches) == 1
        assert find_match.event_id == filter_matches[0].event_id == latest.event_id

    async def test_filter_zero_limit_returns_empty_without_future_wait(self, eventbus):
        eventbus.emit(UserActionEvent(action='x', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        await eventbus.wait_until_idle()

        start = time.monotonic()
        matches = await eventbus.filter('UserActionEvent', past=True, future=2.0, limit=0)
        elapsed = time.monotonic() - start

        assert matches == []
        assert elapsed < 0.5

    async def test_filter_negative_limit_returns_empty(self, eventbus):
        eventbus.emit(UserActionEvent(action='x', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        await eventbus.wait_until_idle()
        matches = await eventbus.filter('UserActionEvent', past=True, future=False, limit=-1)
        assert matches == []

    async def test_find_treats_limit_kwarg_as_field_filter(self, eventbus):
        class LimitFieldEvent(BaseEvent):
            limit: int

        no_match = eventbus.emit(LimitFieldEvent(limit=3))
        target = eventbus.emit(LimitFieldEvent(limit=5))
        await eventbus.wait_until_idle()

        match = await eventbus.find(LimitFieldEvent, past=True, future=False, limit=5)
        assert match is not None
        assert match.event_id == target.event_id
        assert match.event_id != no_match.event_id


class TestDebouncePatterns:
    """End-to-end scenarios for debounce-style flows."""

    class DebounceEvent(BaseEvent):
        user_id: int

    async def test_debounce_prefers_recent_history(self, eventbus):
        # First event completes
        initial = await eventbus.emit(self.DebounceEvent(user_id=123))
        await eventbus.wait_until_idle()

        # Compose the debounce pattern: find(past) -> find(future) -> dispatch
        resolved = (
            await eventbus.find(self.DebounceEvent, past=10, future=False)
            or await eventbus.find(self.DebounceEvent, past=False, future=0.05)
            or await eventbus.emit(self.DebounceEvent(user_id=123))
        )

        assert resolved is not None
        assert resolved.event_id == initial.event_id

        total_events = sum(1 for event in eventbus.event_history.values() if isinstance(event, self.DebounceEvent))
        assert total_events == 1

    async def test_debounce_dispatches_when_recent_missing(self, eventbus):
        resolved = (
            await eventbus.find(self.DebounceEvent, past=1, future=False)
            or await eventbus.find(self.DebounceEvent, past=False, future=0.05)
            or await eventbus.emit(self.DebounceEvent(user_id=999))
        )

        assert resolved is not None
        assert isinstance(resolved, self.DebounceEvent)
        assert resolved.user_id == 999

        await eventbus.wait_until_idle()

        total_events = sum(1 for event in eventbus.event_history.values() if isinstance(event, self.DebounceEvent))
        assert total_events == 1

    async def test_debounce_uses_future_match_before_dispatch_fallback(self, eventbus):
        async def dispatch_after_delay() -> BaseEvent:
            await asyncio.sleep(0.02)
            return eventbus.emit(self.DebounceEvent(user_id=555))

        dispatch_task = asyncio.create_task(dispatch_after_delay())

        resolved = (
            await eventbus.find(self.DebounceEvent, past=1, future=False)
            or await eventbus.find(self.DebounceEvent, past=False, future=0.1)
            or await eventbus.emit(self.DebounceEvent(user_id=999))
        )

        dispatched = await dispatch_task
        assert resolved is not None
        assert isinstance(resolved, self.DebounceEvent)
        assert resolved.event_id == dispatched.event_id
        assert resolved.user_id == 555

        await eventbus.wait_until_idle()
        total_events = sum(1 for event in eventbus.event_history.values() if isinstance(event, self.DebounceEvent))
        assert total_events == 1

    async def test_find_with_complex_predicate(self, eventbus):
        """Test future find with complex predicate logic."""
        events_seen = []

        def complex_predicate(event: UserActionEvent) -> bool:
            if hasattr(event, 'action'):
                # Only match after seeing at least 3 events and action is 'target'
                result = len(events_seen) >= 3 and event.action == 'target'
                events_seen.append(event.action)
                return result
            return False

        find_task = asyncio.create_task(eventbus.find('UserActionEvent', where=complex_predicate, past=False, future=1.0))

        await asyncio.sleep(0.01)

        # Dispatch events
        eventbus.emit(UserActionEvent(action='first', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        eventbus.emit(UserActionEvent(action='second', user_id='2a312e4d-3035-7883-86b9-578ce47046b2'))
        eventbus.emit(UserActionEvent(action='target', user_id='6eb8a717-e19d-728b-8905-97f7e20c002e'))  # Won't match yet
        eventbus.emit(UserActionEvent(action='target', user_id='840ea1d0-3500-7be5-8f73-5fd29bb46e89'))  # This should match

        received = await find_task

        assert received.user_id == '840ea1d0-3500-7be5-8f73-5fd29bb46e89'
        assert len(events_seen) == 4


class TestEventResults:
    """Test the event results functionality on BaseEvent"""

    async def test_event_results_list_defaults_filter_empty_values_raise_errors_and_options_override(self, eventbus):
        """EventBus result helpers default to valid payloads, then expose explicit override knobs."""

        async def ok_handler(event):
            return 'ok'

        async def none_handler(event):
            return None

        async def forwarded_event_handler(event):
            return BaseEvent(event_type='ForwardedResult')

        eventbus.on('ResultOptionsDefaultEvent', ok_handler)
        eventbus.on('ResultOptionsDefaultEvent', none_handler)
        eventbus.on('ResultOptionsDefaultEvent', forwarded_event_handler)

        event = eventbus.emit(BaseEvent(event_type='ResultOptionsDefaultEvent'))
        await event.now()
        assert await event.event_results_list() == ['ok']

        async def error_ok_handler(event):
            return 'ok'

        async def error_handler(event):
            raise ValueError('boom')

        eventbus.on('ResultOptionsErrorEvent', error_ok_handler)
        eventbus.on('ResultOptionsErrorEvent', error_handler)

        error_event = eventbus.emit(BaseEvent(event_type='ResultOptionsErrorEvent'))
        await error_event.now()
        with pytest.raises(ValueError, match='boom'):
            await error_event.event_results_list()
        assert await error_event.event_results_list(raise_if_any=False, raise_if_none=True) == ['ok']

        eventbus.on('ResultOptionsEmptyEvent', none_handler)
        empty_event = eventbus.emit(BaseEvent(event_type='ResultOptionsEmptyEvent'))
        await empty_event.now()
        with pytest.raises(ValueError, match='Expected at least one handler'):
            await empty_event.event_results_list(raise_if_none=True)
        assert await empty_event.event_results_list(raise_if_any=False, raise_if_none=False) == []

        async def keep_handler(event):
            return 'keep'

        async def drop_handler(event):
            return 'drop'

        eventbus.on('ResultOptionsIncludeEvent', keep_handler)
        eventbus.on('ResultOptionsIncludeEvent', drop_handler)
        seen_handler_names: list[str] = []

        def include_keep(result: Any, event_result: Any) -> bool:
            seen_handler_names.append(event_result.handler_name)
            return result == 'keep'

        included_event = eventbus.emit(BaseEvent(event_type='ResultOptionsIncludeEvent'))
        await included_event.now()
        filtered_values = await included_event.event_results_list(
            include=include_keep,
            raise_if_any=False,
            raise_if_none=True,
        )
        assert filtered_values == ['keep']
        assert len(seen_handler_names) == 2

    async def test_dispatch_returns_event_results(self, eventbus):
        """Test that dispatch returns BaseEvent with result methods"""

        # Register a specific handler
        async def test_handler(event):
            return {'result': 'test_result'}

        eventbus.on('UserActionEvent', test_handler)

        result = eventbus.emit(UserActionEvent(action='test', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
        assert isinstance(result, BaseEvent)

        # Wait for completion
        await result
        all_results = await result.event_results_list()
        assert isinstance(all_results, list)
        assert all_results == [{'result': 'test_result'}]

        # Test with no specific handlers
        result_no_handlers = eventbus.emit(BaseEvent(event_type='NoHandlersEvent'))
        await result_no_handlers
        # Should have no handlers
        assert len(result_no_handlers.event_results) == 0

    async def test_event_results_indexing(self, eventbus):
        """Test indexing by handler name and ID"""
        order = []

        async def handler1(event):
            order.append(1)
            return 'first'

        async def handler2(event):
            order.append(2)
            return 'second'

        async def handler3(event):
            order.append(3)
            return 'third'

        eventbus.on('TestEvent', handler1)
        eventbus.on('TestEvent', handler2)
        eventbus.on('TestEvent', handler3)

        # Test indexing
        event = await eventbus.emit(BaseEvent(event_type='TestEvent'))

        # Get results by handler name
        handler1_result = next((r for r in event.event_results.values() if r.handler_name.endswith('handler1')), None)
        handler2_result = next((r for r in event.event_results.values() if r.handler_name.endswith('handler2')), None)
        handler3_result = next((r for r in event.event_results.values() if r.handler_name.endswith('handler3')), None)

        assert handler1_result is not None and handler1_result.result == 'first'
        assert handler2_result is not None and handler2_result.result == 'second'
        assert handler3_result is not None and handler3_result.result == 'third'

    async def test_event_results_access(self, eventbus):
        """Test accessing event results"""

        async def early_handler(event):
            return 'early'

        async def late_handler(event):
            await asyncio.sleep(0.01)
            return 'late'

        eventbus.on('TestEvent', early_handler)
        eventbus.on('TestEvent', late_handler)

        result = await eventbus.emit(BaseEvent(event_type='TestEvent'))

        # Check both handlers ran
        assert len(result.event_results) == 2
        early_result = next((r for r in result.event_results.values() if r.handler_name.endswith('early_handler')), None)
        late_result = next((r for r in result.event_results.values() if r.handler_name.endswith('late_handler')), None)
        assert early_result is not None and early_result.result == 'early'
        assert late_result is not None and late_result.result == 'late'

        # With empty handlers
        eventbus.handlers_by_key['EmptyEvent'] = []
        results_empty = eventbus.emit(BaseEvent(event_type='EmptyEvent'))
        await results_empty
        # Should have no handlers
        assert len(results_empty.event_results) == 0

    async def test_by_handler_name(self, eventbus):
        """Test handler results with duplicate names"""

        async def process_data(event):
            return 'version1'

        async def process_data2(event):  # Different function, same __name__
            return 'version2'

        process_data2.__name__ = 'process_data'  # Same name!

        async def unique_handler(event):
            return 'unique'

        # Should get warning about duplicate name
        with pytest.warns(UserWarning, match='already registered'):
            eventbus.on('TestEvent', process_data)
            eventbus.on('TestEvent', process_data2)
        eventbus.on('TestEvent', unique_handler)

        event = await eventbus.emit(BaseEvent(event_type='TestEvent'))

        # Check results - with duplicate names, both handlers run
        process_results = [r for r in event.event_results.values() if r.handler_name.endswith('process_data')]
        assert len(process_results) == 2
        assert {r.result for r in process_results} == {'version1', 'version2'}

        unique_result = next((r for r in event.event_results.values() if r.handler_name.endswith('unique_handler')), None)
        assert unique_result is not None and unique_result.result == 'unique'

    async def test_by_handler_id(self, eventbus):
        """Test that all handlers run with unique IDs even with same name"""

        async def handler1(event):
            return 'v1'

        async def handler2(event):
            return 'v2'

        # Give them the same name for the test
        handler1.__name__ = 'handler'
        handler2.__name__ = 'handler'

        with pytest.warns(UserWarning, match='already registered'):
            eventbus.on('TestEvent', handler1)
            eventbus.on('TestEvent', handler2)

        event = await eventbus.emit(BaseEvent(event_type='TestEvent'))

        await event.event_results_list()
        results = {handler_id: result.result for handler_id, result in event.event_results.items()}

        # All handlers present with unique IDs even with same name
        # Should have 2 results: handler1, handler2
        assert len(results) == 2
        assert 'v1' in results.values()
        assert 'v2' in results.values()

    async def test_manual_dict_merge(self, eventbus):
        """Users can merge dict handler results manually from event_results_list()."""

        async def config_base(event):
            return {'debug': False, 'port': 8080, 'name': 'base'}

        async def config_override(event):
            return {'debug': True, 'timeout': 30, 'name': 'override'}

        eventbus.on('GetConfig', config_base)
        eventbus.on('GetConfig', config_override)

        event = await eventbus.emit(BaseEvent(event_type='GetConfig'))
        merged = {}
        for result in await event.event_results_list(include=lambda result, _: isinstance(result, dict), raise_if_any=False):
            assert isinstance(result, dict)
            merged.update(result)

        # Later handlers override earlier ones
        assert merged == {
            'debug': True,  # Overridden
            'port': 8080,  # From base
            'timeout': 30,  # From override
            'name': 'override',  # Overridden
        }

        # Test non-dict handler (should be skipped)
        async def bad_handler(event):
            return 'not a dict'

        eventbus.on('BadConfig', bad_handler)
        event_bad = await eventbus.emit(BaseEvent(event_type='BadConfig'))

        merged_bad = {}
        for result in await event_bad.event_results_list(
            include=lambda result, _: isinstance(result, dict),
            raise_if_any=False,
            raise_if_none=False,
        ):
            assert isinstance(result, dict)
            merged_bad.update(result)
        assert merged_bad == {}  # Empty dict since no dict results

    async def test_manual_dict_merge_conflicts_last_write_wins(self, eventbus):
        """Manual dict merge from results is explicit and uses user-defined conflict behavior."""

        async def handler_one(event):
            return {'shared': 1, 'unique1': 'a'}

        async def handler_two(event):
            return {'shared': 2, 'unique2': 'b'}

        eventbus.on('ConflictEvent', handler_one)
        eventbus.on('ConflictEvent', handler_two)

        event = await eventbus.emit(BaseEvent(event_type='ConflictEvent'))

        merged = {}
        for result in await event.event_results_list(include=lambda result, _: isinstance(result, dict), raise_if_any=False):
            assert isinstance(result, dict)
            merged.update(result)

        assert merged['shared'] == 2
        assert merged['unique1'] == 'a'
        assert merged['unique2'] == 'b'

    async def test_manual_list_flatten(self, eventbus):
        """Users can flatten list handler results manually from event_results_list()."""

        async def errors1(event):
            return ['error1', 'error2']

        async def errors2(event):
            return ['error3']

        async def errors3(event):
            return ['error4', 'error5']

        eventbus.on('GetErrors', errors1)
        eventbus.on('GetErrors', errors2)
        eventbus.on('GetErrors', errors3)

        event = await eventbus.emit(BaseEvent(event_type='GetErrors'))
        all_errors = [
            item
            for result in await event.event_results_list(include=lambda result, _: isinstance(result, list), raise_if_any=False)
            if isinstance(result, list)
            for item in result
        ]

        # Check that all errors are collected (order may vary due to handler execution)
        assert all_errors == ['error1', 'error2', 'error3', 'error4', 'error5']

        # Test with non-list handler
        async def single_value(event):
            return 'single'

        eventbus.on('GetSingle', single_value)
        event_single = await eventbus.emit(BaseEvent(event_type='GetSingle'))

        result = [
            item
            for nested in await event_single.event_results_list(
                include=lambda result, _: isinstance(result, list),
                raise_if_any=False,
                raise_if_none=False,
            )
            if isinstance(nested, list)
            for item in nested
        ]
        assert 'single' not in result  # Single values should be skipped, as they are not lists
        assert len(result) == 0

    async def test_by_handler_name_access(self, eventbus):
        """Test accessing results by handler name"""

        async def handler_a(event):
            return 'result_a'

        async def handler_b(event):
            return 'result_b'

        eventbus.on('TestEvent', handler_a)
        eventbus.on('TestEvent', handler_b)

        event = await eventbus.emit(BaseEvent(event_type='TestEvent'))

        # Access results by handler name
        handler_a_result = next((r for r in event.event_results.values() if r.handler_name.endswith('handler_a')), None)
        handler_b_result = next((r for r in event.event_results.values() if r.handler_name.endswith('handler_b')), None)

        assert handler_a_result is not None and handler_a_result.result == 'result_a'
        assert handler_b_result is not None and handler_b_result.result == 'result_b'

    async def test_string_indexing(self, eventbus):
        """Test accessing handler results"""

        async def my_handler(event):
            return 'my_result'

        eventbus.on('TestEvent', my_handler)
        event = await eventbus.emit(BaseEvent(event_type='TestEvent'))

        # Access result by handler name
        my_handler_result = next((r for r in event.event_results.values() if r.handler_name.endswith('my_handler')), None)
        assert my_handler_result is not None and my_handler_result.result == 'my_result'

        # Check missing handler returns None
        missing_result = next((r for r in event.event_results.values() if r.handler_name.endswith('missing')), None)
        assert missing_result is None


class TestEventBusForwarding:
    """Test event forwarding between buses with new EventResults"""

    async def test_forwarding_flattens_results(self):
        """Test that forwarding events between buses flattens all results"""
        bus1 = EventBus(name='Bus1')
        bus2 = EventBus(name='Bus2')
        bus3 = EventBus(name='Bus3')

        results = []

        async def bus1_handler(event):
            results.append('bus1')
            return 'from_bus1'

        async def bus2_handler(event):
            results.append('bus2')
            return 'from_bus2'

        async def bus3_handler(event):
            results.append('bus3')
            return 'from_bus3'

        # Register handlers
        bus1.on('TestEvent', bus1_handler)
        bus2.on('TestEvent', bus2_handler)
        bus3.on('TestEvent', bus3_handler)

        # Set up forwarding chain
        bus1.on('*', bus2.emit)
        bus2.on('*', bus3.emit)

        try:
            # Dispatch from bus1
            event = bus1.emit(BaseEvent(event_type='TestEvent'))

            # Wait for all buses to complete processing
            await bus1.wait_until_idle()
            await bus2.wait_until_idle()
            await bus3.wait_until_idle()

            # Wait for event completion
            event = await event

            # All handlers from all buses should be visible
            bus1_result = next((r for r in event.event_results.values() if r.handler_name.endswith('bus1_handler')), None)
            bus2_result = next((r for r in event.event_results.values() if r.handler_name.endswith('bus2_handler')), None)
            bus3_result = next((r for r in event.event_results.values() if r.handler_name.endswith('bus3_handler')), None)

            assert bus1_result is not None and bus1_result.result == 'from_bus1'
            assert bus2_result is not None and bus2_result.result == 'from_bus2'
            assert bus3_result is not None and bus3_result.result == 'from_bus3'

            # Check execution order
            assert results == ['bus1', 'bus2', 'bus3']

        finally:
            await bus1.destroy()
            await bus2.destroy()
            await bus3.destroy()

    async def test_by_eventbus_id_and_path(self):
        """Test by_eventbus_id() and by_path() with forwarding"""
        bus1 = EventBus(name='MainBus')
        bus2 = EventBus(name='PluginBus')

        async def main_handler(event):
            return 'main_result'

        async def plugin_handler1(event):
            return 'plugin_result1'

        async def plugin_handler2(event):
            return 'plugin_result2'

        bus1.on('DataEvent', main_handler)
        bus2.on('DataEvent', plugin_handler1)
        bus2.on('DataEvent', plugin_handler2)

        # Forward from bus1 to bus2
        bus1.on('*', bus2.emit)

        try:
            event = bus1.emit(BaseEvent(event_type='DataEvent'))

            # Wait for processing
            await bus1.wait_until_idle()
            await bus2.wait_until_idle()
            event = await event

            # Check results from both buses
            main_result = next((r for r in event.event_results.values() if r.handler_name.endswith('main_handler')), None)
            plugin1_result = next((r for r in event.event_results.values() if r.handler_name.endswith('plugin_handler1')), None)
            plugin2_result = next((r for r in event.event_results.values() if r.handler_name.endswith('plugin_handler2')), None)

            assert main_result is not None and main_result.result == 'main_result'
            assert plugin1_result is not None and plugin1_result.result == 'plugin_result1'
            assert plugin2_result is not None and plugin2_result.result == 'plugin_result2'

            # Check event path shows forwarding
            assert event.event_path == [bus1.label, bus2.label]

        finally:
            await bus1.destroy()
            await bus2.destroy()


class TestComplexIntegration:
    """Complex integration test with all features"""

    async def test_complex_multi_bus_scenario(self, caplog):
        """Test complex scenario with multiple buses, duplicate names, and lookup flows"""
        # Create a hierarchy of buses
        app_bus = EventBus(name='AppBus')
        auth_bus = EventBus(name='AuthBus')
        data_bus = EventBus(name='DataBus')

        # Handlers with conflicting names
        async def app_validate(event):
            """App validation"""
            return {'app_valid': True, 'timestamp': 1000}

        app_validate.__name__ = 'validate'

        async def auth_validate(event):
            """Auth validation"""
            return {'auth_valid': True, 'user': 'alice'}

        auth_validate.__name__ = 'validate'

        async def data_validate(event):
            """Data validation"""
            return {'data_valid': True, 'schema': 'v2'}

        data_validate.__name__ = 'validate'

        async def auth_process(event):
            """Auth processing"""
            return ['auth_log_1', 'auth_log_2']

        auth_process.__name__ = 'process'

        async def data_process(event):
            """Data processing"""
            return ['data_log_1', 'data_log_2', 'data_log_3']

        data_process.__name__ = 'process'

        # Register handlers with same names on different buses
        app_bus.on('ValidationRequest', app_validate)
        auth_bus.on('ValidationRequest', auth_validate)
        auth_bus.on('ValidationRequest', auth_process)  # Different return type!
        data_bus.on('ValidationRequest', data_validate)
        data_bus.on('ValidationRequest', data_process)

        # Set up forwarding
        app_bus.on('*', auth_bus.emit)
        auth_bus.on('*', data_bus.emit)

        try:
            # Dispatch event
            event = app_bus.emit(BaseEvent(event_type='ValidationRequest'))

            # Wait for all processing
            await app_bus.wait_until_idle()
            await auth_bus.wait_until_idle()
            await data_bus.wait_until_idle()
            event = await event

            # Test that all handlers ran
            # Count handlers by name
            validate_results = [r for r in event.event_results.values() if r.handler_name.endswith('validate')]
            process_results = [r for r in event.event_results.values() if r.handler_name.endswith('process')]

            # Should have multiple validate and process handlers from different buses
            assert len(validate_results) >= 3  # One per bus
            assert len(process_results) >= 2  # Auth and Data buses

            # Check event path shows forwarding through all buses
            assert app_bus.label in event.event_path
            assert auth_bus.label in event.event_path
            assert data_bus.label in event.event_path

            dict_result: dict[str, Any] = {}
            for result in await event.event_results_list(include=lambda result, _: isinstance(result, dict), raise_if_any=False):
                assert isinstance(result, dict)
                dict_result.update(result)
            # Should have merged all dict returns
            assert 'app_valid' in dict_result and 'auth_valid' in dict_result and 'data_valid' in dict_result

            list_result = [
                item
                for result in await event.event_results_list(
                    include=lambda result, _: isinstance(result, list), raise_if_any=False
                )
                if isinstance(result, list)
                for item in result
            ]
            # Should include all list items
            assert any('log' in str(item) for item in list_result)

        finally:
            await app_bus.destroy(clear=True)
            await auth_bus.destroy(clear=True)
            await data_bus.destroy(clear=True)

    async def test_event_result_type_enforcement_with_dict(self):
        """Test that handlers returning wrong types get errors when event expects dict result."""
        bus = EventBus(name='TestBus')

        # Create an event that expects dict results
        class DictResultEvent(BaseEvent[dict]):
            pass

        # Create handlers with different return types
        async def dict_handler1(event):
            return {'key1': 'value1'}

        async def dict_handler2(event):
            return {'key2': 'value2'}

        async def string_handler(event):
            return 'this is a string, not a dict'

        async def int_handler(event):
            return 42

        async def list_handler(event):
            return [1, 2, 3]

        # Register all handlers
        bus.on('DictResultEvent', dict_handler1)
        bus.on('DictResultEvent', dict_handler2)
        bus.on('DictResultEvent', string_handler)
        bus.on('DictResultEvent', int_handler)
        bus.on('DictResultEvent', list_handler)

        try:
            # Dispatch event
            event = bus.emit(DictResultEvent())
            await bus.wait_until_idle()
            event = await event

            # Check that handlers returning dicts succeeded
            dict_results = [r for r in event.event_results.values() if r.handler_name in ['dict_handler1', 'dict_handler2']]
            assert all(r.status == 'completed' for r in dict_results)
            assert all(isinstance(r.result, dict) for r in dict_results)

            # Check that handlers returning wrong types have errors
            wrong_type_results = [
                r for r in event.event_results.values() if r.handler_name in ['string_handler', 'int_handler', 'list_handler']
            ]
            assert all(r.status == 'error' for r in wrong_type_results)
            assert all(r.error is not None for r in wrong_type_results)

            # Check error messages mention type mismatch
            for result in wrong_type_results:
                error_msg = str(result.error)
                assert 'did not match expected event_result_type' in error_msg
                assert 'dict' in error_msg

            dict_result: dict[str, Any] = {}
            for result in await event.event_results_list(
                include=lambda result, _: isinstance(result, dict),
                raise_if_any=False,
                raise_if_none=False,
            ):
                assert isinstance(result, dict)
                dict_result.update(result)
            assert 'key1' in dict_result and 'key2' in dict_result
            assert len(dict_result) == 2  # Only the two dict results

        finally:
            await bus.destroy(clear=True)

    async def test_event_result_type_enforcement_with_list(self):
        """Test that handlers returning wrong types get errors when event expects list result."""
        bus = EventBus(name='TestBus')

        # Create an event that expects list results
        class ListResultEvent(BaseEvent[list]):
            pass

        # Create handlers with different return types
        async def list_handler1(event):
            return [1, 2, 3]

        async def list_handler2(event):
            return ['a', 'b', 'c']

        async def dict_handler(event):
            return {'key': 'value'}

        async def string_handler(event):
            return 'not a list'

        async def int_handler(event):
            return 99

        # Register all handlers
        bus.on('ListResultEvent', list_handler1)
        bus.on('ListResultEvent', list_handler2)
        bus.on('ListResultEvent', dict_handler)
        bus.on('ListResultEvent', string_handler)
        bus.on('ListResultEvent', int_handler)

        try:
            # Dispatch event
            event = bus.emit(ListResultEvent())
            await bus.wait_until_idle()
            event = await event

            # Check that handlers returning lists succeeded
            list_results = [r for r in event.event_results.values() if r.handler_name in ['list_handler1', 'list_handler2']]
            assert all(r.status == 'completed' for r in list_results)
            assert all(isinstance(r.result, list) for r in list_results)

            # Check that handlers returning wrong types have errors
            wrong_type_results = [
                r for r in event.event_results.values() if r.handler_name in ['dict_handler', 'string_handler', 'int_handler']
            ]
            assert all(r.status == 'error' for r in wrong_type_results)
            assert all(r.error is not None for r in wrong_type_results)

            # Check error messages mention type mismatch
            for result in wrong_type_results:
                error_msg = str(result.error)
                assert 'did not match expected event_result_type' in error_msg
                assert 'list' in error_msg

            list_result = [
                item
                for result in await event.event_results_list(
                    include=lambda result, _: isinstance(result, list),
                    raise_if_any=False,
                    raise_if_none=False,
                )
                if isinstance(result, list)
                for item in result
            ]
            assert list_result == [1, 2, 3, 'a', 'b', 'c']  # Flattened from both list handlers

        finally:
            await bus.destroy(clear=True)


# Folded from test_eventbus_edge_cases.py to keep test layout class-based.

import pytest

from abxbus import BaseEvent, EventStatus


class ResetCoverageEvent(BaseEvent[None]):
    label: str


class IdleTimeoutCoverageEvent(BaseEvent[None]):
    label: str = 'slow'


class DestroyCoverageEvent(BaseEvent[None]):
    label: str = 'destroy'


@pytest.mark.asyncio
async def test_event_reset_creates_fresh_pending_event_for_cross_bus_dispatch():
    bus_a = EventBus(name='ResetCoverageBusA')
    bus_b = EventBus(name='ResetCoverageBusB')
    seen_a: list[str] = []
    seen_b: list[str] = []

    bus_a.on(ResetCoverageEvent, lambda event: seen_a.append(event.label))
    bus_b.on(ResetCoverageEvent, lambda event: seen_b.append(event.label))

    completed = await bus_a.emit(ResetCoverageEvent(label='hello'))
    assert completed.event_status == EventStatus.COMPLETED
    assert len(completed.event_results) == 1

    fresh = completed.event_reset()
    assert fresh.event_id != completed.event_id
    assert fresh.event_status == EventStatus.PENDING
    assert fresh.event_completed_at is None
    assert fresh.event_results == {}

    forwarded = await bus_b.emit(fresh)
    assert forwarded.event_status == EventStatus.COMPLETED
    assert seen_a == ['hello']
    assert seen_b == ['hello']
    assert any(path.startswith('ResetCoverageBusA#') for path in forwarded.event_path)
    assert any(path.startswith('ResetCoverageBusB#') for path in forwarded.event_path)

    await bus_a.destroy(clear=True)
    await bus_b.destroy(clear=True)


@pytest.mark.asyncio
async def test_wait_until_idle_timeout_path_recovers_after_inflight_handler_finishes():
    bus = EventBus(name='IdleTimeoutCoverageBus')
    handler_started = asyncio.Event()
    release_handler = asyncio.Event()

    async def slow_handler(event: IdleTimeoutCoverageEvent) -> None:
        handler_started.set()
        await release_handler.wait()

    bus.on(IdleTimeoutCoverageEvent, slow_handler)
    pending = bus.emit(IdleTimeoutCoverageEvent())
    await handler_started.wait()

    start = time.perf_counter()
    await bus.wait_until_idle(timeout=0.01)
    elapsed = time.perf_counter() - start
    assert elapsed < 0.5
    assert pending.event_status != EventStatus.COMPLETED

    release_handler.set()
    await pending
    await bus.wait_until_idle(timeout=1.0)
    assert pending.event_status == EventStatus.COMPLETED

    await bus.destroy(clear=True)


@pytest.mark.asyncio
async def test_destroy_is_immediate_and_rejects_late_handler_emits():
    bus = EventBus(name='DestroyImmediateBus')
    handler_started = asyncio.Event()
    release_handler = asyncio.Event()
    late_emit_rejected = asyncio.get_running_loop().create_future()

    async def slow_handler(event: DestroyCoverageEvent) -> None:
        handler_started.set()
        try:
            await release_handler.wait()
        except asyncio.CancelledError:
            pass
        try:
            bus.emit(DestroyCoverageEvent())
        except RuntimeError:
            if not late_emit_rejected.done():
                late_emit_rejected.set_result(True)
        else:
            if not late_emit_rejected.done():
                late_emit_rejected.set_result(False)

    bus.on(DestroyCoverageEvent, slow_handler)
    _pending = bus.emit(DestroyCoverageEvent())
    await handler_started.wait()

    start = time.perf_counter()
    await bus.destroy(clear=False)
    elapsed = time.perf_counter() - start

    assert elapsed < 0.05
    assert bus._is_running is False
    assert len(bus.event_history) == 1
    assert bus._destroyed is True

    with pytest.raises(RuntimeError, match='destroyed'):
        bus.emit(DestroyCoverageEvent())

    release_handler.set()
    assert await asyncio.wait_for(late_emit_rejected, timeout=1.0) is True
    await bus.destroy(clear=True)


# Folded from test_eventbus_middleware.py to keep test layout class-based.
# pyright: basic
"""Consolidated middleware tests."""

import json
import multiprocessing
import sqlite3
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from pydantic import Field

from abxbus import SQLiteHistoryMirrorMiddleware
from abxbus.middlewares import (
    AutoErrorEventMiddleware,
    AutoHandlerChangeEventMiddleware,
    AutoReturnEventMiddleware,
    BusHandlerRegisteredEvent,
    BusHandlerUnregisteredEvent,
    EventBusMiddleware,
    LoggerEventBusMiddleware,
    OtelTracingMiddleware,
    WALEventBusMiddleware,
)


class TestWALPersistence:
    """Test automatic WAL persistence functionality"""

    async def test_wal_persistence_handler(self, tmp_path):
        """Test that events are automatically persisted to WAL file"""
        # Create event bus with WAL path
        wal_path = tmp_path / 'test_events.jsonl'
        bus = EventBus(name='TestBus', middlewares=[WALEventBusMiddleware(wal_path)])

        try:
            # Emit some events
            events = []
            for i in range(3):
                event = UserActionEvent(action=f'action_{i}', user_id=f'user_{i}')
                emitted_event = bus.emit(event)
                completed_event = await emitted_event
                events.append(completed_event)

            # Wait for processing
            await bus.wait_until_idle()

            # Check WAL file exists
            assert wal_path.exists()

            # Read and verify JSONL content
            lines = wal_path.read_text().strip().split('\n')
            assert len(lines) == 3

            # Parse each line as JSON
            for i, line in enumerate(lines):
                data = json.loads(line)
                assert data['action'] == f'action_{i}'
                assert data['user_id'] == f'user_{i}'
                assert data['event_type'] == 'UserActionEvent'
                assert isinstance(data['event_created_at'], str)
                datetime.fromisoformat(data['event_created_at'])

        finally:
            await bus.destroy()

    async def test_wal_persistence_creates_parent_dir(self, tmp_path):
        """Test that WAL persistence creates parent directories"""
        # Use a nested path that doesn't exist
        wal_path = tmp_path / 'nested' / 'dirs' / 'events.jsonl'
        assert not wal_path.parent.exists()

        # Create event bus
        bus = EventBus(name='TestBus', middlewares=[WALEventBusMiddleware(wal_path)])

        try:
            # Emit an event
            event = bus.emit(UserActionEvent(action='test', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
            await event

            # Wait for WAL persistence to complete
            await bus.wait_until_idle()

            # Parent directory should be created after event is processed
            assert wal_path.parent.exists()

            # Check file was created
            assert wal_path.exists()
        finally:
            await bus.destroy()

    async def test_wal_persistence_skips_incomplete_events(self, tmp_path):
        """Test that WAL persistence only writes completed events"""
        wal_path = tmp_path / 'incomplete_events.jsonl'
        bus = EventBus(name='TestBus', middlewares=[WALEventBusMiddleware(wal_path)])

        try:
            # Add a slow handler that will delay completion
            async def slow_handler(event: BaseEvent) -> str:
                await asyncio.sleep(0.1)
                return 'slow'

            bus.on('UserActionEvent', slow_handler)

            # Emit event without waiting
            event = bus.emit(UserActionEvent(action='test', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))

            # Check file doesn't exist yet (event not completed)
            assert not wal_path.exists()

            # Wait for completion
            event = await event
            await bus.wait_until_idle()

            # Now file should exist with completed event
            assert wal_path.exists()
            lines = wal_path.read_text().strip().split('\n')
            assert len(lines) == 1
            data = json.loads(lines[0])
            assert data['event_type'] == 'UserActionEvent'
            # The WAL should have been written after the event completed
            assert data['action'] == 'test'
            assert data['user_id'] == 'e692b6cb-ae63-773b-8557-3218f7ce5ced'

        finally:
            await bus.destroy()


class TestHandlerMiddleware:
    """Tests for the handler middleware pipeline."""

    async def test_middleware_constructor_auto_inits_classes_and_keeps_hook_order(self):
        calls: list[str] = []

        class ClassMiddleware(EventBusMiddleware):
            def __init__(self):
                calls.append('class:init')

            async def on_event_result_change(self, eventbus: EventBus, event: BaseEvent, event_result, status):
                if status == 'started':
                    calls.append('class:started')
                elif status == 'completed':
                    calls.append('class:completed')

        class InstanceMiddleware(EventBusMiddleware):
            async def on_event_result_change(self, eventbus: EventBus, event: BaseEvent, event_result, status):
                if status == 'started':
                    calls.append('instance:started')
                elif status == 'completed':
                    calls.append('instance:completed')

        instance_middleware = InstanceMiddleware()
        bus = EventBus(middlewares=[ClassMiddleware, instance_middleware])
        bus.on('UserActionEvent', lambda event: 'ok')

        try:
            completed = await bus.emit(UserActionEvent(action='test', user_id='d592b79f-4dd9-7d4d-88b1-0d0db7d84fcf'))
            await bus.wait_until_idle()

            assert isinstance(bus.middlewares[0], ClassMiddleware)
            assert bus.middlewares[1] is instance_middleware
            assert completed.event_results
            assert calls == [
                'class:init',
                'class:started',
                'instance:started',
                'class:completed',
                'instance:completed',
            ]
        finally:
            await bus.destroy()

    async def test_middleware_wraps_successful_handler(self):
        calls: list[tuple[str, str]] = []

        class TrackingMiddleware(EventBusMiddleware):
            def __init__(self, call_log: list[tuple[str, str]]):
                self.call_log = call_log

            async def on_event_result_change(self, eventbus: EventBus, event: BaseEvent, event_result, status):
                if status == 'started':
                    self.call_log.append(('before', event_result.status))
                elif status == 'completed':
                    self.call_log.append(('after', event_result.status))

        bus = EventBus(middlewares=[TrackingMiddleware(calls)])
        bus.on('UserActionEvent', lambda event: 'ok')

        try:
            completed = await bus.emit(UserActionEvent(action='test', user_id='d592b79f-4dd9-7d4d-88b1-0d0db7d84fcf'))
            await bus.wait_until_idle()

            assert completed.event_results
            result = next(iter(completed.event_results.values()))
            assert result.status == 'completed'
            assert result.result == 'ok'
            assert calls == [('before', 'started'), ('after', 'completed')]
        finally:
            await bus.destroy()

    async def test_middleware_observes_handler_errors(self):
        observations: list[tuple[str, str]] = []

        class ErrorMiddleware(EventBusMiddleware):
            def __init__(self, log: list[tuple[str, str]]):
                self.log = log

            async def on_event_result_change(self, eventbus: EventBus, event: BaseEvent, event_result, status):
                if status == 'started':
                    self.log.append(('before', event_result.status))
                elif status == 'completed' and event_result.error:
                    self.log.append(('error', type(event_result.error).__name__))

        async def failing_handler(event: BaseEvent) -> None:
            raise ValueError('boom')

        bus = EventBus(middlewares=[ErrorMiddleware(observations)])
        bus.on('UserActionEvent', failing_handler)

        try:
            event = await bus.emit(UserActionEvent(action='fail', user_id='16599da2-bf1d-7a5d-8e6e-ba01f216519a'))
            await bus.wait_until_idle()

            result = next(iter(event.event_results.values()))
            assert result.status == 'error'
            assert isinstance(result.error, ValueError)
            assert observations == [('before', 'started'), ('error', 'ValueError')]
        finally:
            await bus.destroy()

    async def test_middleware_hook_statuses_never_emit_error(self):
        observed_event_statuses: list[str] = []
        observed_result_hook_statuses: list[str] = []
        observed_result_runtime_statuses: list[str] = []

        class LifecycleMiddleware(EventBusMiddleware):
            async def on_event_change(self, eventbus: EventBus, event: BaseEvent, status):
                observed_event_statuses.append(str(status))

            async def on_event_result_change(self, eventbus: EventBus, event: BaseEvent, event_result, status):
                observed_result_hook_statuses.append(str(status))
                observed_result_runtime_statuses.append(event_result.status)

        async def failing_handler(event: BaseEvent) -> None:
            raise ValueError('boom')

        bus = EventBus(middlewares=[LifecycleMiddleware()], max_history_size=None)
        bus.on(UserActionEvent, failing_handler)

        try:
            event = await bus.emit(UserActionEvent(action='fail', user_id='2a312e4d-3035-7883-86b9-578ce47046b2'))
            await bus.wait_until_idle()

            result = next(iter(event.event_results.values()))
            assert result.status == 'error'
            assert isinstance(result.error, ValueError)

            assert observed_event_statuses == ['pending', 'started', 'completed']
            assert observed_result_hook_statuses == ['pending', 'started', 'completed']
            assert observed_result_runtime_statuses[-1] == 'error'
            assert 'error' not in observed_event_statuses
            assert 'error' not in observed_result_hook_statuses
        finally:
            await bus.destroy()

    async def test_middleware_event_status_order_is_deterministic_for_each_event(self):
        event_statuses_by_id: dict[str, list[str]] = {}

        class LifecycleMiddleware(EventBusMiddleware):
            async def on_event_change(self, eventbus: EventBus, event: BaseEvent, status):
                event_statuses_by_id.setdefault(event.event_id, []).append(str(status))

        async def handler(_event: UserActionEvent) -> str:
            await asyncio.sleep(0)
            return 'ok'

        bus = EventBus(middlewares=[LifecycleMiddleware()], max_history_size=None)
        bus.on(UserActionEvent, handler)

        batch_count = 5
        events_per_batch = 50
        try:
            for batch_index in range(batch_count):
                events = [
                    bus.emit(
                        UserActionEvent(
                            action='deterministic',
                            user_id=f'u-{batch_index}-{event_index}',
                        )
                    )
                    for event_index in range(events_per_batch)
                ]
                await asyncio.gather(*events)
                await bus.wait_until_idle()

                for event in events:
                    assert event_statuses_by_id[event.event_id] == ['pending', 'started', 'completed']

            assert len(event_statuses_by_id) == batch_count * events_per_batch
        finally:
            await bus.destroy()

    async def test_middleware_event_and_result_lifecycle_remains_monotonic_on_timeout(self):
        observed_event_statuses: list[str] = []
        observed_result_transitions: list[tuple[str, str, str]] = []

        class LifecycleMiddleware(EventBusMiddleware):
            async def on_event_change(self, eventbus: EventBus, event: BaseEvent, status):
                observed_event_statuses.append(str(status))

            async def on_event_result_change(self, eventbus: EventBus, event: BaseEvent, event_result, status):
                observed_result_transitions.append((event_result.handler_name, str(status), event_result.status))

        class TimeoutLifecycleEvent(BaseEvent[str]):
            event_timeout: float | None = 0.02

        async def slow_handler(_event: TimeoutLifecycleEvent) -> str:
            await asyncio.sleep(0.05)
            return 'slow'

        async def pending_handler(_event: TimeoutLifecycleEvent) -> str:
            return 'pending'

        bus = EventBus(middlewares=[LifecycleMiddleware()])
        bus.on(TimeoutLifecycleEvent, slow_handler)
        bus.on(TimeoutLifecycleEvent, pending_handler)

        try:
            await bus.emit(TimeoutLifecycleEvent())
            await bus.wait_until_idle()

            assert observed_event_statuses == ['pending', 'started', 'completed']

            slow_transitions = [entry for entry in observed_result_transitions if entry[0].endswith('slow_handler')]
            pending_transitions = [entry for entry in observed_result_transitions if entry[0].endswith('pending_handler')]

            assert [status for _, status, _ in slow_transitions] == ['pending', 'started', 'completed']
            assert [result_status for _, _, result_status in slow_transitions] == ['pending', 'started', 'error']

            assert [status for _, status, _ in pending_transitions] == ['pending', 'completed']
            assert [result_status for _, _, result_status in pending_transitions] == ['pending', 'error']
        finally:
            await bus.destroy()

    async def test_auto_error_event_middleware_emits_and_guards_recursion(self):
        seen: list[tuple[str, str]] = []
        bus = EventBus(middlewares=[AutoErrorEventMiddleware()])

        class UserActionEventErrorEvent(BaseEvent[None]):
            error_type: str

        async def fail_handler(event: BaseEvent) -> None:
            raise ValueError('boom')

        async def fail_auto(event: UserActionEventErrorEvent) -> None:
            raise RuntimeError('nested')

        async def on_auto_error_event(event: UserActionEventErrorEvent) -> None:
            seen.append((event.event_type, event.error_type))

        bus.on(UserActionEvent, fail_handler)
        bus.on(UserActionEventErrorEvent, on_auto_error_event)
        bus.on(UserActionEventErrorEvent, fail_auto)

        try:
            await bus.emit(UserActionEvent(action='fail', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
            await bus.wait_until_idle()
            assert seen == [('UserActionEventErrorEvent', 'ValueError')]
            assert await bus.find('UserActionEventErrorEventErrorEvent', past=True, future=False) is None
        finally:
            await bus.destroy()

    async def test_auto_return_event_middleware_emits_and_guards_recursion(self):
        seen: list[tuple[str, Any]] = []
        bus = EventBus(middlewares=[AutoReturnEventMiddleware()])

        class UserActionEventResultEvent(BaseEvent[None]):
            data: Any

        async def ok_handler(event: BaseEvent) -> int:
            return 123

        async def non_none_auto(event: UserActionEventResultEvent) -> str:
            return 'nested'

        async def on_auto_result_event(event: UserActionEventResultEvent) -> None:
            seen.append((event.event_type, event.data))

        bus.on(UserActionEvent, ok_handler)
        bus.on(UserActionEventResultEvent, on_auto_result_event)
        bus.on(UserActionEventResultEvent, non_none_auto)

        try:
            await bus.emit(UserActionEvent(action='ok', user_id='2a312e4d-3035-7883-86b9-578ce47046b2'))
            await bus.wait_until_idle()
            assert seen == [('UserActionEventResultEvent', 123)]
            assert await bus.find('UserActionEventResultEventResultEvent', past=True, future=False) is None
        finally:
            await bus.destroy()

    async def test_auto_return_event_middleware_skips_baseevent_returns(self):
        seen: list[tuple[str, Any]] = []
        bus = EventBus(middlewares=[AutoReturnEventMiddleware()])

        class UserActionEventResultEvent(BaseEvent[None]):
            data: Any

        class ReturnedEvent(BaseEvent):
            value: int

        async def returns_event(event: BaseEvent) -> ReturnedEvent:
            return ReturnedEvent(value=7)

        async def on_auto_result_event(event: UserActionEventResultEvent) -> None:
            seen.append((event.event_type, event.data))

        bus.on(UserActionEvent, returns_event)
        bus.on(UserActionEventResultEvent, on_auto_result_event)

        try:
            parent = await bus.emit(UserActionEvent(action='ok', user_id='6eb8a717-e19d-728b-8905-97f7e20c002e'))
            await bus.wait_until_idle()
            assert len(parent.event_results) == 1
            only_result = next(iter(parent.event_results.values()))
            assert isinstance(only_result.result, ReturnedEvent)
            assert seen == []
            assert await bus.find('UserActionEventResultEvent', past=True, future=False) is None
        finally:
            await bus.destroy()

    async def test_auto_handler_change_event_middleware_emits_registered_and_unregistered(self):
        registered: list[BusHandlerRegisteredEvent] = []
        unregistered: list[BusHandlerUnregisteredEvent] = []
        bus = EventBus(middlewares=[AutoHandlerChangeEventMiddleware()])

        def on_registered(event: BusHandlerRegisteredEvent) -> None:
            registered.append(event)

        def on_unregistered(event: BusHandlerUnregisteredEvent) -> None:
            unregistered.append(event)

        bus.on(BusHandlerRegisteredEvent, on_registered)
        bus.on(BusHandlerUnregisteredEvent, on_unregistered)

        async def target_handler(event: UserActionEvent) -> None:
            return None

        try:
            handler_entry = bus.on(UserActionEvent, target_handler)
            await bus.wait_until_idle()

            bus.off(UserActionEvent, handler_entry)
            await bus.wait_until_idle()

            matching_registered = [event for event in registered if event.handler.id == handler_entry.id]
            matching_unregistered = [event for event in unregistered if event.handler.id == handler_entry.id]
            assert matching_registered
            assert matching_unregistered
            assert matching_registered[-1].handler.eventbus_id == bus.id
            assert matching_registered[-1].handler.eventbus_name == bus.name
            assert matching_registered[-1].handler.event_pattern == 'UserActionEvent'
            assert matching_unregistered[-1].handler.event_pattern == 'UserActionEvent'
        finally:
            await bus.destroy()

    async def test_otel_tracing_middleware_tracks_parent_event_and_handler_spans(self):
        class RootEvent(BaseEvent):
            pass

        class ChildEvent(BaseEvent):
            pass

        class FakeSpan:
            def __init__(self, name: str, context: Any = None, start_time: int | None = None):
                self.name = name
                self.context = context
                self.start_time = start_time
                self.end_time: int | None = None
                self.attrs: dict[str, Any] = {}
                self.errors: list[str] = []
                self.ended = False

            def set_attribute(self, key: str, value: Any):
                self.attrs[key] = value

            def record_exception(self, error: BaseException):
                self.errors.append(type(error).__name__)

            def end(self, end_time: int | None = None):
                self.end_time = end_time
                self.ended = True

        class FakeTracer:
            def __init__(self):
                self.spans: list[FakeSpan] = []

            def start_span(self, name: str, context: Any = None, start_time: int | None = None):
                span = FakeSpan(name, context=context, start_time=start_time)
                self.spans.append(span)
                return span

        class FakeTraceAPI:
            @staticmethod
            def set_span_in_context(span: FakeSpan):
                return {'parent': span}

        tracer = FakeTracer()
        bus = EventBus(middlewares=[OtelTracingMiddleware(tracer=tracer, trace_api=FakeTraceAPI())], name='TraceBus')

        async def child_handler(event: ChildEvent) -> None:
            return None

        async def root_handler(event: RootEvent) -> None:
            child = event.emit(ChildEvent())
            await child

        bus.on(RootEvent, root_handler)
        bus.on(ChildEvent, child_handler)

        try:
            await bus.emit(RootEvent())
            await bus.wait_until_idle()

            root_event_span = next(span for span in tracer.spans if span.attrs.get('abxbus.event_type') == 'RootEvent')
            root_handler_span = next(
                span for span in tracer.spans if str(span.attrs.get('abxbus.handler_name', '')).endswith('root_handler')
            )
            child_event_span = next(span for span in tracer.spans if span.attrs.get('abxbus.event_type') == 'ChildEvent')
            child_handler_span = next(
                span for span in tracer.spans if str(span.attrs.get('abxbus.handler_name', '')).endswith('child_handler')
            )

            assert [span.name for span in tracer.spans] == [
                'TraceBus.emit(RootEvent)',
                f'{root_handler_span.attrs["abxbus.handler_name"]}(RootEvent)',
                'TraceBus.emit(ChildEvent)',
                f'{child_handler_span.attrs["abxbus.handler_name"]}(ChildEvent)',
            ]
            assert root_event_span.context is None
            assert root_event_span.attrs.get('abxbus.trace.root') is True
            assert root_handler_span.context['parent'] is root_event_span
            assert child_event_span.context['parent'] is root_handler_span
            assert child_handler_span.context['parent'] is child_event_span
            assert root_event_span.attrs.get('abxbus.event_bus.name') == bus.name
            assert root_handler_span.attrs.get('abxbus.event_bus.name') == bus.name
            assert child_event_span.attrs.get('abxbus.event_bus.name') == bus.name
            assert child_handler_span.attrs.get('abxbus.event_bus.name') == bus.name
            assert child_event_span.attrs.get('abxbus.event_parent_id') == root_event_span.attrs.get('abxbus.event_id')
            assert child_event_span.attrs.get('abxbus.event_emitted_by_handler_id') == root_handler_span.attrs.get(
                'abxbus.handler_id'
            )
            assert all(span.ended for span in tracer.spans)
            assert all(span.start_time is not None for span in tracer.spans)
            assert all(span.end_time is not None for span in tracer.spans)
            assert all(span.end_time > span.start_time for span in tracer.spans if span.end_time and span.start_time)
        finally:
            await bus.destroy()


class TestSQLiteHistoryMirror:
    async def test_sqlite_history_persists_events_and_results(self, tmp_path):
        db_path = tmp_path / 'events.sqlite'
        middleware = SQLiteHistoryMirrorMiddleware(db_path)
        bus = EventBus(middlewares=[middleware])

        async def handler(event: BaseEvent) -> str:
            return 'ok'

        bus.on('UserActionEvent', handler)

        try:
            await bus.emit(UserActionEvent(action='ping', user_id='b57fcb67-faeb-7a56-8907-116d8cbb1472'))
            await bus.wait_until_idle()

            conn = sqlite3.connect(db_path)
            events = conn.execute('SELECT phase, event_status FROM events_log ORDER BY id').fetchall()
            assert [phase for phase, _ in events] == ['pending', 'started', 'completed']
            assert [status for _, status in events] == ['pending', 'started', 'completed']

            result_rows = conn.execute(
                'SELECT phase, status, result_repr, error_repr FROM event_results_log ORDER BY id'
            ).fetchall()
            conn.close()

            assert [phase for phase, *_ in result_rows] == ['pending', 'started', 'completed']
            assert [status for _, status, *_ in result_rows] == ['pending', 'started', 'completed']
            assert result_rows[-1][2] == "'ok'"
            assert result_rows[-1][3] is None
        finally:
            await bus.destroy()

    def test_sqlite_history_close_is_idempotent(self, tmp_path):
        db_path = tmp_path / 'events.sqlite'
        middleware = SQLiteHistoryMirrorMiddleware(db_path)

        middleware.close()
        middleware.close()

        with pytest.raises(sqlite3.ProgrammingError):
            middleware._conn.execute('SELECT 1')


class TestLoggerMiddleware:
    async def test_logger_middleware_writes_file(self, tmp_path):
        log_path = tmp_path / 'events.log'
        bus = EventBus(middlewares=[LoggerEventBusMiddleware(log_path)])

        async def handler(event: BaseEvent) -> str:
            return 'logged'

        bus.on('UserActionEvent', handler)

        try:
            await bus.emit(UserActionEvent(action='log', user_id='1d4087d7-e791-702f-80b9-0fb09b726bc6'))
            await bus.wait_until_idle()

            assert log_path.exists()
            contents = log_path.read_text().strip().splitlines()
            assert contents
            assert 'UserActionEvent' in contents[-1]
        finally:
            await bus.destroy()

    async def test_logger_middleware_stdout_only(self, capsys):
        bus = EventBus(middlewares=[LoggerEventBusMiddleware()])

        async def handler(event: BaseEvent) -> str:
            return 'stdout'

        bus.on('UserActionEvent', handler)

        try:
            await bus.emit(UserActionEvent(action='log', user_id='1d4087d7-e791-702f-80b9-0fb09b726bc6'))
            await bus.wait_until_idle()

            captured = capsys.readouterr()
            assert 'UserActionEvent' in captured.out
            assert 'stdout' not in captured.err
        finally:
            await bus.destroy()

    async def test_sqlite_history_records_errors(self, tmp_path):
        db_path = tmp_path / 'events.sqlite'
        middleware = SQLiteHistoryMirrorMiddleware(db_path)
        bus = EventBus(middlewares=[middleware])

        async def failing_handler(event: BaseEvent) -> None:
            raise RuntimeError('handler boom')

        bus.on('UserActionEvent', failing_handler)

        try:
            await bus.emit(UserActionEvent(action='boom', user_id='28536f9b-4031-7f53-827f-98c24c1b3839'))
            await bus.wait_until_idle()

            conn = sqlite3.connect(db_path)
            result_rows = conn.execute('SELECT phase, status, error_repr FROM event_results_log ORDER BY id').fetchall()
            events = conn.execute('SELECT phase, event_status FROM events_log ORDER BY id').fetchall()
            conn.close()

            assert [phase for phase, *_ in result_rows] == ['pending', 'started', 'completed']
            assert [status for _, status, *_ in result_rows] == ['pending', 'started', 'error']
            assert 'RuntimeError' in result_rows[-1][2]
            assert [phase for phase, _ in events] == ['pending', 'started', 'completed']
            assert [status for _, status in events] == ['pending', 'started', 'completed']
        finally:
            await bus.destroy()


class MiddlewarePatternEvent(BaseEvent[str]):
    pass


async def _flush_hook_tasks(ticks: int = 6) -> None:
    for _ in range(ticks):
        await asyncio.sleep(0)


async def test_middleware_hooks_cover_class_string_and_wildcard_patterns() -> None:
    event_statuses_by_id: dict[str, list[str]] = {}
    result_hook_statuses_by_handler: dict[str, list[str]] = {}
    result_runtime_statuses_by_handler: dict[str, list[str]] = {}
    handler_change_records: list[dict[str, Any]] = []

    class RecordingMiddleware(EventBusMiddleware):
        async def on_event_change(self, eventbus: EventBus, event: BaseEvent[Any], status) -> None:
            event_statuses_by_id.setdefault(event.event_id, []).append(str(status))

        async def on_event_result_change(self, eventbus: EventBus, event: BaseEvent[Any], event_result, status) -> None:
            handler_id = event_result.handler_id
            result_hook_statuses_by_handler.setdefault(handler_id, []).append(str(status))
            result_runtime_statuses_by_handler.setdefault(handler_id, []).append(event_result.status)

        async def on_bus_handlers_change(self, eventbus: EventBus, handler, registered: bool) -> None:
            handler_change_records.append(
                {
                    'handler_id': handler.id,
                    'event_pattern': handler.event_pattern,
                    'registered': registered,
                    'eventbus_id': handler.eventbus_id,
                }
            )

    bus = EventBus(name='MiddlewareHookPatternParityBus', middlewares=[RecordingMiddleware()])

    async def class_handler(event: MiddlewarePatternEvent) -> str:
        return 'class-result'

    async def string_handler(event: BaseEvent[Any]) -> str:
        assert event.event_type == 'MiddlewarePatternEvent'
        return 'string-result'

    async def wildcard_handler(event: BaseEvent[Any]) -> str:
        return f'wildcard:{event.event_type}'

    class_entry = bus.on(MiddlewarePatternEvent, class_handler)
    string_entry = bus.on('MiddlewarePatternEvent', string_handler)
    wildcard_entry = bus.on('*', wildcard_handler)

    try:
        await _flush_hook_tasks()

        registered_records = [record for record in handler_change_records if record['registered'] is True]
        assert len(registered_records) == 3

        expected_patterns = {
            class_entry.id: 'MiddlewarePatternEvent',
            string_entry.id: 'MiddlewarePatternEvent',
            wildcard_entry.id: '*',
        }
        assert {record['handler_id'] for record in registered_records} == set(expected_patterns)
        for record in registered_records:
            assert record['event_pattern'] == expected_patterns[record['handler_id']]
            assert record['eventbus_id'] == bus.id

        event = await bus.emit(MiddlewarePatternEvent(event_timeout=0.2))
        await bus.wait_until_idle()

        assert str(event.event_status) == 'completed'
        assert event_statuses_by_id[event.event_id] == ['pending', 'started', 'completed']
        assert set(event.event_results) == set(expected_patterns)

        for handler_id in expected_patterns:
            assert result_hook_statuses_by_handler[handler_id] == ['pending', 'started', 'completed']
            assert result_runtime_statuses_by_handler[handler_id] == ['pending', 'started', 'completed']

        assert event.event_results[class_entry.id].result == 'class-result'
        assert event.event_results[string_entry.id].result == 'string-result'
        assert event.event_results[wildcard_entry.id].result == 'wildcard:MiddlewarePatternEvent'

        bus.off(MiddlewarePatternEvent, class_entry)
        bus.off('MiddlewarePatternEvent', string_entry)
        bus.off('*', wildcard_entry)
        await _flush_hook_tasks()

        unregistered_records = [record for record in handler_change_records if record['registered'] is False]
        assert len(unregistered_records) == 3
        assert {record['handler_id'] for record in unregistered_records} == set(expected_patterns)
        for record in unregistered_records:
            assert record['event_pattern'] == expected_patterns[record['handler_id']]
    finally:
        await bus.destroy()


async def test_middleware_hooks_cover_string_and_wildcard_patterns_for_ad_hoc_baseevent() -> None:
    event_statuses_by_id: dict[str, list[str]] = {}
    result_hook_statuses_by_handler: dict[str, list[str]] = {}
    result_runtime_statuses_by_handler: dict[str, list[str]] = {}
    handler_change_records: list[dict[str, Any]] = []

    class RecordingMiddleware(EventBusMiddleware):
        async def on_event_change(self, eventbus: EventBus, event: BaseEvent[Any], status) -> None:
            event_statuses_by_id.setdefault(event.event_id, []).append(str(status))

        async def on_event_result_change(self, eventbus: EventBus, event: BaseEvent[Any], event_result, status) -> None:
            handler_id = event_result.handler_id
            result_hook_statuses_by_handler.setdefault(handler_id, []).append(str(status))
            result_runtime_statuses_by_handler.setdefault(handler_id, []).append(event_result.status)

        async def on_bus_handlers_change(self, eventbus: EventBus, handler, registered: bool) -> None:
            handler_change_records.append(
                {
                    'handler_id': handler.id,
                    'event_pattern': handler.event_pattern,
                    'registered': registered,
                    'eventbus_id': handler.eventbus_id,
                }
            )

    bus = EventBus(name='MiddlewareHookStringPatternParityBus', middlewares=[RecordingMiddleware()])
    ad_hoc_event_type = 'AdHocPatternEvent'

    async def string_handler(event: BaseEvent[Any]) -> str:
        assert event.event_type == ad_hoc_event_type
        return f'string:{event.event_type}'

    async def wildcard_handler(event: BaseEvent[Any]) -> str:
        return f'wildcard:{event.event_type}'

    string_entry = bus.on(ad_hoc_event_type, string_handler)
    wildcard_entry = bus.on('*', wildcard_handler)

    try:
        await _flush_hook_tasks()

        registered_records = [record for record in handler_change_records if record['registered'] is True]
        assert len(registered_records) == 2

        expected_patterns = {
            string_entry.id: ad_hoc_event_type,
            wildcard_entry.id: '*',
        }
        assert {record['handler_id'] for record in registered_records} == set(expected_patterns)
        for record in registered_records:
            assert record['event_pattern'] == expected_patterns[record['handler_id']]
            assert record['eventbus_id'] == bus.id

        event = await bus.emit(BaseEvent(event_type=ad_hoc_event_type, event_timeout=0.2))
        await bus.wait_until_idle()

        assert str(event.event_status) == 'completed'
        assert event_statuses_by_id[event.event_id] == ['pending', 'started', 'completed']
        assert set(event.event_results) == set(expected_patterns)

        for handler_id in expected_patterns:
            assert result_hook_statuses_by_handler[handler_id] == ['pending', 'started', 'completed']
            assert result_runtime_statuses_by_handler[handler_id] == ['pending', 'started', 'completed']

        assert event.event_results[string_entry.id].result == f'string:{ad_hoc_event_type}'
        assert event.event_results[wildcard_entry.id].result == f'wildcard:{ad_hoc_event_type}'

        bus.off(ad_hoc_event_type, string_entry)
        bus.off('*', wildcard_entry)
        await _flush_hook_tasks()

        unregistered_records = [record for record in handler_change_records if record['registered'] is False]
        assert len(unregistered_records) == 2
        assert {record['handler_id'] for record in unregistered_records} == set(expected_patterns)
        for record in unregistered_records:
            assert record['event_pattern'] == expected_patterns[record['handler_id']]
    finally:
        await bus.destroy()


class HistoryTestEvent(BaseEvent):
    """Event for verifying middleware mirroring behaviour."""

    payload: str
    should_fail: bool = False


def _summarize_history(history: dict[str, BaseEvent[Any]]) -> list[dict[str, Any]]:
    """Collect comparable information about events stored in history."""
    summary: list[dict[str, Any]] = []
    for event in history.values():
        handler_results = [
            {
                'handler_name': result.handler_name.rsplit('.', 1)[-1],
                'status': result.status,
                'result': result.result,
                'error': repr(result.error) if result.error else None,
            }
            for result in sorted(event.event_results.values(), key=lambda r: r.handler_name)
        ]
        summary.append(
            {
                'event_type': event.event_type,
                'event_status': event.event_status,
                'event_path_length': len(event.event_path),
                'children': sorted(child.event_type for child in event.event_children),
                'handler_results': handler_results,
            }
        )
    return sorted(summary, key=lambda record: record['event_type'])


async def _run_scenario(
    *,
    middlewares: Sequence[Any] = (),
    should_fail: bool = False,
) -> list[dict[str, Any]]:
    """Execute a simple scenario and return the history summary."""
    bus = EventBus(middlewares=list(middlewares))

    async def ok_handler(event: HistoryTestEvent) -> str:
        return f'ok-{event.payload}'

    async def conditional_handler(event: HistoryTestEvent) -> str:
        if event.should_fail:
            raise RuntimeError('boom')
        return 'fine'

    bus.on('HistoryTestEvent', ok_handler)
    bus.on('HistoryTestEvent', conditional_handler)

    try:
        await bus.emit(HistoryTestEvent(payload='payload', should_fail=should_fail))
        await bus.wait_until_idle()
    finally:
        summary = _summarize_history(bus.event_history)
        await bus.destroy()

    return summary


@pytest.mark.asyncio
async def test_sqlite_mirror_matches_inmemory_success(tmp_path: Path) -> None:
    db_path = tmp_path / 'events_success.sqlite'
    in_memory_result = await _run_scenario()
    sqlite_result = await _run_scenario(middlewares=[SQLiteHistoryMirrorMiddleware(db_path)])
    assert sqlite_result == in_memory_result

    conn = sqlite3.connect(db_path)
    event_phases = conn.execute('SELECT phase FROM events_log ORDER BY id').fetchall()
    conn.close()
    assert {phase for (phase,) in event_phases} >= {'pending', 'started', 'completed'}


@pytest.mark.asyncio
async def test_sqlite_mirror_matches_inmemory_error(tmp_path: Path) -> None:
    db_path = tmp_path / 'events_error.sqlite'
    in_memory_result = await _run_scenario(should_fail=True)
    sqlite_result = await _run_scenario(
        middlewares=[SQLiteHistoryMirrorMiddleware(db_path)],
        should_fail=True,
    )
    assert sqlite_result == in_memory_result

    conn = sqlite3.connect(db_path)
    phases = conn.execute('SELECT DISTINCT phase FROM events_log').fetchall()
    conn.close()
    assert {phase for (phase,) in phases} >= {'pending', 'started', 'completed'}


def _worker_dispatch(db_path: str, worker_id: int) -> None:
    """Process entrypoint for exercising concurrent writes."""

    async def run() -> None:
        middleware = SQLiteHistoryMirrorMiddleware(Path(db_path))
        bus = EventBus(name=f'WorkerBus{worker_id}', middlewares=[middleware])

        async def handler(event: HistoryTestEvent) -> str:
            return f'worker-{worker_id}'

        bus.on('HistoryTestEvent', handler)
        try:
            await bus.emit(HistoryTestEvent(payload=f'worker-{worker_id}'))
            await bus.wait_until_idle()
        finally:
            await bus.destroy()

    asyncio.run(run())


def test_sqlite_mirror_supports_concurrent_processes(tmp_path: Path) -> None:
    db_path = tmp_path / 'shared_history.sqlite'
    ctx = multiprocessing.get_context('spawn')
    processes = [ctx.Process(target=_worker_dispatch, args=(str(db_path), idx)) for idx in range(3)]
    for proc in processes:
        proc.start()
    for proc in processes:
        proc.join(timeout=20)
        assert proc.exitcode == 0

    conn = sqlite3.connect(db_path)
    events = conn.execute('SELECT DISTINCT eventbus_name FROM events_log').fetchall()
    results_count = conn.execute('SELECT COUNT(*) FROM event_results_log').fetchone()
    conn.close()

    bus_labels = {name for (name,) in events}
    assert len(bus_labels) == 3
    for idx in range(3):
        assert any(label.startswith(f'WorkerBus{idx}#') and len(label.rsplit('#', 1)[-1]) == 4 for label in bus_labels)
    assert results_count is not None
    # Each worker records pending/started/completed for its single handler
    assert results_count[0] == 9


# Folded from test_eventbus_name_conflict_gc.py to keep test layout class-based.
# pyright: basic
"""
Tests for EventBus name conflict resolution with garbage collection.

Tests that EventBus instances that would be garbage collected don't cause
name conflicts when creating new instances with the same name.
"""

import gc
import weakref

import pytest

from abxbus import BaseEvent


class TestNameConflictGC:
    """Test EventBus name conflict resolution with garbage collection"""

    def test_name_conflict_with_live_reference(self):
        """Test that name conflict generates a warning and auto-generates a unique name"""
        # Create an EventBus with a specific name
        bus1 = EventBus(name='GCTestConflict')

        # Try to create another with the same name - should warn and auto-generate unique name
        with pytest.warns(UserWarning, match='EventBus with name "GCTestConflict" already exists'):
            bus2 = EventBus(name='GCTestConflict')

        # The second bus should have a unique name
        assert bus2.name.startswith('GCTestConflict_')
        assert bus2.name != 'GCTestConflict'
        assert len(bus2.name) == len('GCTestConflict_') + 8  # Original name + underscore + 8 char suffix

    def test_name_no_conflict_after_deletion(self):
        """Test that name conflict is NOT raised after the existing bus is deleted and GC runs"""
        import gc

        # Create an EventBus with a specific name
        bus1 = EventBus(name='GCTestBus1')

        # Delete the reference and force GC
        del bus1
        gc.collect()  # Force garbage collection to release the WeakSet reference

        # Creating another with the same name should work since the first one was collected
        bus2 = EventBus(name='GCTestBus1')
        assert bus2.name == 'GCTestBus1'

    def test_name_no_conflict_with_no_reference(self):
        """Test that name conflict is NOT raised when the existing bus was never assigned"""
        import gc

        # Create an EventBus with a specific name but don't keep a reference
        EventBus(name='GCTestBus2')  # No assignment, will be garbage collected
        gc.collect()  # Force garbage collection

        # Creating another with the same name should work since the first one is gone
        bus2 = EventBus(name='GCTestBus2')
        assert bus2.name == 'GCTestBus2'

    def test_name_conflict_with_weak_reference_only(self):
        """Test that name conflict is NOT raised when only weak references exist"""
        import gc

        # Create an EventBus and keep only a weak reference
        bus1 = EventBus(name='GCTestBus3')
        weak_ref = weakref.ref(bus1)

        # Verify the weak reference works
        assert weak_ref() is bus1

        # Delete the strong reference and force GC
        del bus1
        gc.collect()  # Force garbage collection

        # At this point, only the weak reference exists (and the WeakSet reference)
        # Creating another with the same name should work
        bus2 = EventBus(name='GCTestBus3')
        assert bus2.name == 'GCTestBus3'

        # The weak reference should now return None
        assert weak_ref() is None

    def test_multiple_buses_with_gc(self):
        """Test multiple EventBus instances with some being garbage collected"""
        import gc

        # Create multiple buses, some with strong refs, some without
        bus1 = EventBus(name='GCMulti1')
        EventBus(name='GCMulti2')  # Will be GC'd
        bus3 = EventBus(name='GCMulti3')
        EventBus(name='GCMulti4')  # Will be GC'd

        gc.collect()  # Force garbage collection

        # Should be able to create new buses with the names of GC'd buses
        bus2_new = EventBus(name='GCMulti2')
        bus4_new = EventBus(name='GCMulti4')

        # But not with names of buses that still exist - they get auto-generated names
        with pytest.warns(UserWarning, match='EventBus with name "GCMulti1" already exists'):
            bus1_conflict = EventBus(name='GCMulti1')
        assert bus1_conflict.name.startswith('GCMulti1_')

        with pytest.warns(UserWarning, match='EventBus with name "GCMulti3" already exists'):
            bus3_conflict = EventBus(name='GCMulti3')
        assert bus3_conflict.name.startswith('GCMulti3_')

    @pytest.mark.asyncio
    async def test_name_conflict_after_destroy_and_clear(self):
        """Test that clearing an EventBus allows reusing its name"""
        import gc

        # Create an EventBus
        bus1 = EventBus(name='GCDestroyClear')

        # Destroy and clear it (this renames the bus to _destroyed_* and removes from all_instances)
        await bus1.destroy(clear=True)

        # Delete the reference and force GC
        del bus1
        gc.collect()

        # Now we should be able to create a new one with the same name
        bus2 = EventBus(name='GCDestroyClear')
        assert bus2.name == 'GCDestroyClear'

    def test_weakset_behavior(self):
        """Test that the WeakSet properly tracks EventBus instances"""
        initial_count = len(EventBus.all_instances)

        # Create some buses
        bus1 = EventBus(name='WeakTest1')
        bus2 = EventBus(name='WeakTest2')
        bus3 = EventBus(name='WeakTest3')

        # Check they're tracked
        assert len(EventBus.all_instances) == initial_count + 3

        # Delete one
        del bus2

        # The WeakSet should automatically remove it (no gc.collect needed)
        # But we need to check the actual buses in the set, not just the count
        names = {bus.name for bus in EventBus.all_instances if hasattr(bus, 'name') and bus.name.startswith('WeakTest')}
        assert 'WeakTest1' in names
        assert 'WeakTest3' in names
        # WeakTest2 might still be there until the next iteration

    def test_eventbus_removed_from_weakset(self):
        """Test that dead EventBus instances are removed from WeakSet after GC"""
        import gc

        # Create a bus that will be "dead" (no strong references)
        EventBus(name='GCDeadBus')
        gc.collect()  # Force garbage collection

        # When we try to create a new bus with the same name, it should work
        bus = EventBus(name='GCDeadBus')
        assert bus.name == 'GCDeadBus'

        # The dead bus should have been removed from all_instances
        names = [b.name for b in EventBus.all_instances if hasattr(b, 'name') and b.name == 'GCDeadBus']
        assert len(names) == 1  # Only the new one

    def test_concurrent_name_creation(self):
        """Test that concurrent creation with same name generates warning and unique name"""
        # This tests the edge case where two buses might be created nearly simultaneously
        bus1 = EventBus(name='ConcurrentTest')

        # Even if we're in the middle of checking, the second one should get a unique name
        with pytest.warns(UserWarning, match='EventBus with name "ConcurrentTest" already exists'):
            bus2 = EventBus(name='ConcurrentTest')

        assert bus1.name == 'ConcurrentTest'
        assert bus2.name.startswith('ConcurrentTest_')
        assert bus2.name != bus1.name

    @pytest.mark.asyncio
    async def test_unreferenced_buses_with_history_can_be_cleaned_without_instance_leak(self):
        """
        Buses with populated history may outlive local scope while runloops are still active,
        but they must be releasable via explicit cleanup without leaking all_instances.
        """
        import gc

        class GcHistoryEvent(BaseEvent[str]):
            pass

        baseline_instances = len(EventBus.all_instances)
        refs: list[weakref.ReferenceType[EventBus]] = []

        async def create_and_fill_bus(index: int) -> weakref.ReferenceType[EventBus]:
            bus = EventBus(name=f'GCNoDestroyBus_{index}')
            bus.on(GcHistoryEvent, lambda e: 'ok')
            for _ in range(40):
                await bus.emit(GcHistoryEvent())
            await bus.wait_until_idle()
            return weakref.ref(bus)

        for i in range(30):
            refs.append(await create_and_fill_bus(i))

        # Encourage GC/finalization first (best effort without explicit destroy()).
        for _ in range(20):
            gc.collect()
            await asyncio.sleep(0.02)

        alive_buses = [ref() for ref in refs if ref() is not None]
        still_live = [bus for bus in alive_buses if bus is not None]

        # Deterministically clean up anything still alive.
        for bus in still_live:
            await bus.destroy(clear=True)
        # Loop variable keeps a strong ref to the last bus in CPython.
        if still_live:
            del bus
        del still_live
        del alive_buses

        # Final GC and WeakSet purge.
        for _ in range(10):
            gc.collect()
            await asyncio.sleep(0.01)
        _ = list(EventBus.all_instances)

        assert all(ref() is None for ref in refs), 'all buses should be collectable after cleanup'
        assert len(EventBus.all_instances) <= baseline_instances

    @pytest.mark.asyncio
    async def test_unreferenced_buses_with_history_are_collected_without_destroy(self):
        """
        Unreferenced buses should be collectable without explicit destroy(clear=True),
        even after processing events and populating history.
        """
        import gc

        class GcImplicitEvent(BaseEvent[str]):
            pass

        baseline_instances = len(EventBus.all_instances)
        refs: list[weakref.ReferenceType[EventBus]] = []

        async def create_and_fill_bus(index: int) -> weakref.ReferenceType[EventBus]:
            bus = EventBus(name=f'GCImplicitNoDestroy_{index}')
            bus.on(GcImplicitEvent, lambda e: 'ok')
            for _ in range(30):
                await bus.emit(GcImplicitEvent())
            await bus.wait_until_idle()
            return weakref.ref(bus)

        for i in range(20):
            refs.append(await create_and_fill_bus(i))

        for _ in range(80):
            gc.collect()
            await asyncio.sleep(0.02)
            if all(ref() is None for ref in refs):
                break

        # Force WeakSet iteration to purge any dead refs.
        _ = list(EventBus.all_instances)

        assert all(ref() is None for ref in refs), 'all unreferenced buses should be collected without destroy()'
        assert len(EventBus.all_instances) <= baseline_instances

    def test_subclass_registry_and_global_lock_are_collected_with_subclass(self):
        """
        When a temporary EventBus subclass goes out of scope, its class-scoped
        all_instances registry and global-serial lock should be collectable too.
        """
        subclass_ref = None
        registry_ref = None
        lock_ref = None
        bus_ref = None

        def create_scoped_subclass() -> None:
            class ScopedSubclassBus(EventBus):
                pass

            bus = ScopedSubclassBus(name='ScopedSubclassBus', event_concurrency='global-serial')
            nonlocal subclass_ref, registry_ref, lock_ref, bus_ref
            subclass_ref = weakref.ref(ScopedSubclassBus)
            registry_ref = weakref.ref(ScopedSubclassBus.all_instances)
            lock_ref = weakref.ref(bus.event_global_serial_lock)
            bus_ref = weakref.ref(bus)

        create_scoped_subclass()
        assert subclass_ref is not None
        assert registry_ref is not None
        assert lock_ref is not None
        assert bus_ref is not None

        for _ in range(500):
            gc.collect()
            if subclass_ref() is None and registry_ref() is None and lock_ref() is None and bus_ref() is None:
                break

        assert bus_ref() is None, 'subclass bus instance should be collectable'
        assert subclass_ref() is None, 'subclass type should be collectable'
        assert registry_ref() is None, 'subclass all_instances registry should be collectable'
        assert lock_ref() is None, 'subclass global-serial lock should be collectable'


# Folded from test_eventbus_retry_integration.py to keep test layout class-based.

from abxbus import BaseEvent
from abxbus.retry import retry


class TestRetryWithEventBus:
    """Test @retry decorator with EventBus handlers."""

    async def test_retry_decorator_on_eventbus_handler(self):
        """Test that @retry decorator works correctly when applied to EventBus handlers."""
        handler_calls: list[tuple[str, float]] = []

        class TestEvent(BaseEvent[str]):
            """Simple test event."""

            message: str

        bus = EventBus(name='test_retry_bus')

        @retry(
            max_attempts=3,
            retry_after=0.1,
            timeout=1.0,
            semaphore_limit=1,
            semaphore_scope='global',
        )
        async def retrying_handler(event: TestEvent) -> str:
            call_time = time.time()
            handler_calls.append(('called', call_time))

            if len(handler_calls) < 3:
                raise ValueError(f'Attempt {len(handler_calls)} failed')

            return f'Success: {event.message}'

        bus.on('TestEvent', retrying_handler)

        event = TestEvent(message='Hello retry!')
        completed_event = await bus.emit(event)
        await bus.wait_until_idle(timeout=5)

        assert len(handler_calls) == 3, f'Expected 3 attempts, got {len(handler_calls)}'
        for i in range(1, len(handler_calls)):
            delay = handler_calls[i][1] - handler_calls[i - 1][1]
            assert delay >= 0.08, f'Retry delay {i} was {delay:.3f}s, expected >= 0.08s'

        assert completed_event.event_status == 'completed'
        handler_result = await completed_event.event_result()
        assert handler_result == 'Success: Hello retry!'

        await bus.destroy()

    async def test_retry_with_semaphore_on_multiple_handlers(self):
        """Test @retry decorator with semaphore limiting concurrent handler executions."""
        active_handlers: list[int] = []
        max_concurrent = 0
        handler_results: dict[int, list[tuple[str, float]]] = {1: [], 2: [], 3: [], 4: []}

        class WorkEvent(BaseEvent[str]):
            """Event that triggers work."""

            work_id: int

        bus = EventBus(name='test_concurrent_bus', event_handler_concurrency='parallel')

        def create_handler(handler_id: int):
            @retry(
                max_attempts=1,
                timeout=5.0,
                semaphore_limit=2,
                semaphore_name='test_handler_sem',
                semaphore_scope='global',
            )
            async def limited_handler(event: WorkEvent) -> str:
                nonlocal max_concurrent
                active_handlers.append(handler_id)
                handler_results[handler_id].append(('started', time.time()))

                current_concurrent = len(active_handlers)
                max_concurrent = max(max_concurrent, current_concurrent)
                await asyncio.sleep(0.2)

                active_handlers.remove(handler_id)
                handler_results[handler_id].append(('completed', time.time()))
                return f'Handler {handler_id} processed work {event.work_id}'

            limited_handler.__name__ = f'limited_handler_{handler_id}'
            return limited_handler

        for i in range(1, 5):
            handler = create_handler(i)
            bus.on('WorkEvent', handler)

        event = WorkEvent(work_id=1)
        await bus.emit(event)
        await bus.wait_until_idle(timeout=3)

        assert max_concurrent == 2, f'Max concurrent was {max_concurrent}, expected exactly 2 with semaphore_limit=2'
        for handler_id in range(1, 5):
            assert len(handler_results[handler_id]) == 2, f'Handler {handler_id} should have started and completed'

        await bus.destroy()

    async def test_retry_timeout_with_eventbus_handler(self):
        """Test that retry timeout works correctly with EventBus handlers."""

        class TimeoutEvent(BaseEvent[str]):
            """Event for timeout testing."""

            test_id: str
            event_timeout: float | None = 1

        bus = EventBus(name='test_timeout_bus')
        handler_started = False

        @retry(
            max_attempts=1,
            timeout=0.2,
        )
        async def wrapped_handler(event: TimeoutEvent) -> str:
            nonlocal handler_started
            handler_started = True
            await asyncio.sleep(5)
            return 'Should not reach here'

        bus.on(TimeoutEvent, wrapped_handler)

        event = TimeoutEvent(test_id='7ebbd9f4-755a-7f13-828a-183dfe2d4302')
        await bus.emit(event)
        await bus.wait_until_idle(timeout=2)

        assert handler_started, 'Handler should have started'
        assert len(event.event_results) == 1
        result = next(iter(event.event_results.values()))
        assert result.status == 'error'
        assert result.error is not None
        assert isinstance(result.error, TimeoutError)

        await bus.destroy()

    async def test_retry_with_event_type_filter(self):
        """Test retry decorator with specific exception types."""

        class RetryTestEvent(BaseEvent[str]):
            """Event for testing retry on specific exceptions."""

            attempt_limit: int

        bus = EventBus(name='test_exception_filter_bus')
        attempt_count = 0

        @retry(
            max_attempts=4,
            retry_after=0.05,
            timeout=1.0,
            retry_on_errors=[ValueError, RuntimeError],
        )
        async def selective_retry_handler(event: RetryTestEvent) -> str:
            nonlocal attempt_count
            attempt_count += 1

            if attempt_count == 1:
                raise ValueError('This should be retried')
            if attempt_count == 2:
                raise RuntimeError('This should also be retried')
            if attempt_count == 3:
                raise TypeError('This should NOT be retried')

            return 'Success'

        bus.on('RetryTestEvent', selective_retry_handler)

        event = RetryTestEvent(attempt_limit=3)
        await bus.emit(event)
        await bus.wait_until_idle(timeout=2)

        assert attempt_count == 3, f'Expected 3 attempts, got {attempt_count}'
        handler_id = list(event.event_results.keys())[0]
        result = event.event_results[handler_id]
        assert result.status == 'error'
        assert isinstance(result.error, TypeError)
        assert 'This should NOT be retried' in str(result.error)

        await bus.destroy()

    async def test_retry_decorated_method_class_scope_serializes_across_instances(self):
        """Class scope semaphore should serialize bound method handlers across instances."""

        class ScopeClassEvent(BaseEvent[str]):
            pass

        bus = EventBus(name='test_scope_class_bus', event_handler_concurrency='parallel')
        active = 0
        max_active = 0

        class SomeService:
            @retry(
                max_attempts=1,
                semaphore_scope='class',
                semaphore_limit=1,
                semaphore_name='on_scope_class_event',
            )
            async def on_scope_class_event(self, _event: ScopeClassEvent) -> str:
                nonlocal active, max_active
                active += 1
                max_active = max(max_active, active)
                await asyncio.sleep(0.05)
                active -= 1
                return 'ok'

        service_a = SomeService()
        service_b = SomeService()
        bus.on(ScopeClassEvent, service_a.on_scope_class_event)
        bus.on(ScopeClassEvent, service_b.on_scope_class_event)

        event = await bus.emit(ScopeClassEvent())
        await event.wait()

        assert max_active == 1, f'class scope should serialize across instances, got max_active={max_active}'
        await bus.destroy()

    async def test_retry_decorated_method_instance_scope_allows_parallel_across_instances(self):
        """Instance scope semaphore should allow bound handlers from different instances to overlap."""

        class ScopeInstanceEvent(BaseEvent[str]):
            pass

        bus = EventBus(name='test_scope_instance_bus', event_handler_concurrency='parallel')
        active = 0
        max_active = 0
        calls = 0

        class SomeService:
            @retry(
                max_attempts=1,
                semaphore_scope='instance',
                semaphore_limit=1,
                semaphore_name='on_scope_instance_event',
            )
            async def on_scope_instance_event(self, _event: ScopeInstanceEvent) -> str:
                nonlocal active, max_active, calls
                active += 1
                max_active = max(max_active, active)
                calls += 1
                await asyncio.sleep(0.05)
                active -= 1
                return 'ok'

        service_a = SomeService()
        service_b = SomeService()
        bus.on(ScopeInstanceEvent, service_a.on_scope_instance_event)
        bus.on(ScopeInstanceEvent, service_b.on_scope_instance_event)

        event = await bus.emit(ScopeInstanceEvent())
        await event.wait()

        assert calls == 2, f'expected both handlers to run, got calls={calls}'
        assert max_active == 2, f'instance scope should allow overlap across instances, got max_active={max_active}'
        await bus.destroy()

    async def test_retry_decorated_method_global_scope_serializes_all_bound_handlers(self):
        """Global scope semaphore should serialize bound method handlers across all instances."""

        class ScopeGlobalEvent(BaseEvent[str]):
            pass

        bus = EventBus(name='test_scope_global_bus', event_handler_concurrency='parallel')
        active = 0
        max_active = 0

        class SomeService:
            @retry(
                max_attempts=1,
                semaphore_scope='global',
                semaphore_limit=1,
                semaphore_name='on_scope_global_event',
            )
            async def on_scope_global_event(self, _event: ScopeGlobalEvent) -> str:
                nonlocal active, max_active
                active += 1
                max_active = max(max_active, active)
                await asyncio.sleep(0.05)
                active -= 1
                return 'ok'

        service_a = SomeService()
        service_b = SomeService()
        bus.on(ScopeGlobalEvent, service_a.on_scope_global_event)
        bus.on(ScopeGlobalEvent, service_b.on_scope_global_event)

        event = await bus.emit(ScopeGlobalEvent())
        await event.wait()

        assert max_active == 1, f'global scope should serialize all handlers, got max_active={max_active}'
        await bus.destroy()

    async def test_retry_hof_bind_after_wrap_instance_scope_preserves_instance_isolation(self):
        """HOF pattern retry(...)(fn) then bind to instances should keep instance-scope isolation."""

        class HofBindEvent(BaseEvent[str]):
            pass

        bus = EventBus(name='test_hof_bind_bus', event_handler_concurrency='parallel')
        active = 0
        max_active = 0

        @retry(
            max_attempts=1,
            semaphore_scope='instance',
            semaphore_limit=1,
            semaphore_name='hof_bind_handler',
        )
        async def handler(self: object, _event: HofBindEvent) -> str:
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.05)
            active -= 1
            return 'ok'

        class Holder:
            pass

        holder_a = Holder()
        holder_b = Holder()
        bus.on(HofBindEvent, handler.__get__(holder_a, Holder))
        bus.on(HofBindEvent, handler.__get__(holder_b, Holder))

        event = await bus.emit(HofBindEvent())
        await event.wait()

        assert max_active == 2, f'bind-after-wrap instance scope should allow overlap, got max_active={max_active}'
        await bus.destroy()

    async def test_retry_wrapping_emit_retries_full_dispatch_cycle(self):
        """Retry wrapper around emit+wait should retry full event dispatch when handler errors."""

        class TabsEvent(BaseEvent[str]):
            pass

        class DOMEvent(BaseEvent[str]):
            pass

        class ScreenshotEvent(BaseEvent[str]):
            pass

        bus = EventBus(name='test_retry_emit_bus', event_handler_concurrency='parallel')
        tabs_attempts = 0
        dom_calls = 0
        screenshot_calls = 0

        async def tabs_handler(_event: TabsEvent) -> str:
            nonlocal tabs_attempts
            tabs_attempts += 1
            if tabs_attempts < 3:
                raise RuntimeError(f'tabs fail attempt {tabs_attempts}')
            return 'tabs ok'

        async def dom_handler(_event: DOMEvent) -> str:
            nonlocal dom_calls
            dom_calls += 1
            return 'dom ok'

        async def screenshot_handler(_event: ScreenshotEvent) -> str:
            nonlocal screenshot_calls
            screenshot_calls += 1
            return 'screenshot ok'

        bus.on(TabsEvent, tabs_handler)
        bus.on(DOMEvent, dom_handler)
        bus.on(ScreenshotEvent, screenshot_handler)

        @retry(max_attempts=4)
        async def emit_tabs_with_retry() -> TabsEvent:
            tabs_event = await bus.emit(TabsEvent())
            await tabs_event.wait()
            failed_results = [result for result in tabs_event.event_results.values() if result.status == 'error']
            if failed_results:
                first_error = failed_results[0].error
                if isinstance(first_error, Exception):
                    raise first_error
                raise RuntimeError(f'tabs emit failed with non-exception error payload: {first_error!r}')
            return tabs_event

        async def emit_and_wait(event: BaseEvent[str]):
            emitted = await bus.emit(event)
            await emitted.wait()
            return emitted

        tabs_event, dom_event, screenshot_event = await asyncio.gather(
            emit_tabs_with_retry(),
            emit_and_wait(DOMEvent()),
            emit_and_wait(ScreenshotEvent()),
        )

        assert tabs_attempts == 3, f'expected 3 attempts for tabs flow, got {tabs_attempts}'
        assert tabs_event.event_status == 'completed'
        assert dom_calls == 1
        assert screenshot_calls == 1
        assert dom_event.event_status == 'completed'
        assert screenshot_event.event_status == 'completed'
        await bus.destroy()


# Folded from test_eventbus_subclass_isolation.py to keep test layout class-based.
from abxbus import BaseEvent


def test_eventbus_subclasses_isolate_registries_and_global_serial_locks() -> None:
    class IsolatedBusA(EventBus):
        pass

    class IsolatedBusB(EventBus):
        pass

    bus_a1 = IsolatedBusA('IsolatedBusA1', event_concurrency='global-serial')
    bus_a2 = IsolatedBusA('IsolatedBusA2', event_concurrency='global-serial')
    bus_b1 = IsolatedBusB('IsolatedBusB1', event_concurrency='global-serial')

    assert bus_a1 in IsolatedBusA.all_instances
    assert bus_a2 in IsolatedBusA.all_instances
    assert bus_b1 not in IsolatedBusA.all_instances
    assert bus_b1 in IsolatedBusB.all_instances
    assert bus_a1 not in IsolatedBusB.all_instances
    assert bus_a1 not in EventBus.all_instances
    assert bus_b1 not in EventBus.all_instances

    lock_a1 = bus_a1.locks.get_lock_for_event(bus_a1, BaseEvent())
    lock_a2 = bus_a2.locks.get_lock_for_event(bus_a2, BaseEvent())
    lock_b1 = bus_b1.locks.get_lock_for_event(bus_b1, BaseEvent())

    assert lock_a1 is not None
    assert lock_a2 is not None
    assert lock_b1 is not None
    assert lock_a1 is lock_a2
    assert lock_a1 is not lock_b1


# Folded from test_optional_dependencies.py to keep test layout class-based.
import ast
import os
import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


def _ast_import_roots(path: Path) -> set[str]:
    parsed = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    roots: set[str] = set()
    for node in ast.walk(parsed):
        if isinstance(node, ast.Import):
            for alias in node.names:
                roots.add(alias.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            roots.add(node.module.split('.')[0])
    return roots


def test_bridge_modules_do_not_eager_import_optional_packages() -> None:
    bridge_modules = {
        _ROOT / 'abxbus' / 'bridge_postgres.py': {'asyncpg'},
        _ROOT / 'abxbus' / 'bridge_nats.py': {'nats'},
        _ROOT / 'abxbus' / 'bridge_redis.py': {'redis'},
        _ROOT / 'abxbus' / 'bridge_tachyon.py': {'tachyon'},
    }

    for path, forbidden_roots in bridge_modules.items():
        imported_roots = _ast_import_roots(path)
        assert forbidden_roots.isdisjoint(imported_roots), f'{path} eagerly imports {forbidden_roots & imported_roots}'


def test_root_import_excludes_optional_integrations_while_namespaced_imports_resolve() -> None:
    code = """
import sys

import abxbus

assert hasattr(abxbus, 'EventBus')
assert hasattr(abxbus, 'EventBusMiddleware')
assert hasattr(abxbus, 'EventBridge')
assert hasattr(abxbus, 'HTTPEventBridge')
assert hasattr(abxbus, 'JSONLEventBridge')
assert hasattr(abxbus, 'SQLiteEventBridge')

assert not hasattr(abxbus, 'PostgresEventBridge')
assert not hasattr(abxbus, 'RedisEventBridge')
assert not hasattr(abxbus, 'NATSEventBridge')
assert not hasattr(abxbus, 'TachyonEventBridge')
assert not hasattr(abxbus, 'OtelTracingMiddleware')

assert 'asyncpg' not in sys.modules
assert 'redis' not in sys.modules
assert 'nats' not in sys.modules
assert 'tachyon' not in sys.modules
assert not any(name == 'opentelemetry' or name.startswith('opentelemetry.') for name in sys.modules)

from abxbus.bridges import PostgresEventBridge, TachyonEventBridge
from abxbus.middlewares import OtelTracingMiddleware

assert PostgresEventBridge.__name__ == 'PostgresEventBridge'
assert TachyonEventBridge.__name__ == 'TachyonEventBridge'
assert OtelTracingMiddleware.__name__ == 'OtelTracingMiddleware'
"""

    result = subprocess.run(
        [sys.executable, '-c', code],
        cwd=_ROOT,
        env={**os.environ, 'PYDANTIC_DISABLE_PLUGINS': '__all__'},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout
