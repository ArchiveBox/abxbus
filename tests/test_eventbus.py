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
    await bus.stop()


@pytest.fixture
async def parallel_eventbus():
    """Create an event bus with parallel handler execution"""
    bus = EventBus(event_handler_concurrency='parallel')
    yield bus
    await bus.stop()


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
            await bus.stop(clear=True)

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
            await bus.stop(clear=True)

    async def test_auto_start_and_stop(self):
        """Test auto-start functionality and stopping the event bus"""
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

        # Stop the bus
        await bus.stop()
        assert bus._is_running is False

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
            await bus.stop()

        # Stop again should be idempotent
        await bus.stop()
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
            await bus.stop(clear=True)

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
            await bus.stop(clear=True)


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
            await bus_a.stop(clear=True)
            await bus_b.stop(clear=True)
            await bus_c.stop(clear=True)


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

    async def test_stop_with_pending_events(self):
        """Test stopping event bus with events still in queue"""
        bus = EventBus()

        # Add a slow handler
        async def slow_handler(event: BaseEvent) -> str:
            await asyncio.sleep(1)
            return 'done'

        bus.on('*', slow_handler)

        # Enqueue events but don't wait
        for i in range(5):
            bus.emit(UserActionEvent(action=f'action_{i}', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))

        # Stop immediately
        await bus.stop()

        # Bus should stop even with pending events
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
            await parent_bus.stop()
            await child_bus.stop()
            await subchild_bus.stop()

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
            await peer1.stop()
            await peer2.stop()
            await peer3.stop()


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
        for result in await event.event_results_list(include=lambda r: isinstance(r.result, dict), raise_if_any=False):
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
            include=lambda r: isinstance(r.result, dict),
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
        for result in await event.event_results_list(include=lambda r: isinstance(r.result, dict), raise_if_any=False):
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
            for result in await event.event_results_list(include=lambda r: isinstance(r.result, list), raise_if_any=False)
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
                include=lambda r: isinstance(r.result, list),
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
            await bus1.stop()
            await bus2.stop()
            await bus3.stop()

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
            await bus1.stop()
            await bus2.stop()


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
            for result in await event.event_results_list(include=lambda r: isinstance(r.result, dict), raise_if_any=False):
                assert isinstance(result, dict)
                dict_result.update(result)
            # Should have merged all dict returns
            assert 'app_valid' in dict_result and 'auth_valid' in dict_result and 'data_valid' in dict_result

            list_result = [
                item
                for result in await event.event_results_list(include=lambda r: isinstance(r.result, list), raise_if_any=False)
                if isinstance(result, list)
                for item in result
            ]
            # Should include all list items
            assert any('log' in str(item) for item in list_result)

        finally:
            await app_bus.stop(timeout=0, clear=True)
            await auth_bus.stop(timeout=0, clear=True)
            await data_bus.stop(timeout=0, clear=True)

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
                include=lambda r: isinstance(r.result, dict),
                raise_if_any=False,
                raise_if_none=False,
            ):
                assert isinstance(result, dict)
                dict_result.update(result)
            assert 'key1' in dict_result and 'key2' in dict_result
            assert len(dict_result) == 2  # Only the two dict results

        finally:
            await bus.stop(timeout=0, clear=True)

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
                    include=lambda r: isinstance(r.result, list),
                    raise_if_any=False,
                    raise_if_none=False,
                )
                if isinstance(result, list)
                for item in result
            ]
            assert list_result == [1, 2, 3, 'a', 'b', 'c']  # Flattened from both list handlers

        finally:
            await bus.stop(timeout=0, clear=True)
