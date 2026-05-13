"""
Test parent event tracking functionality in EventBus.
"""

import asyncio
from typing import Any

import pytest

from abxbus import BaseEvent, EventBus


class ParentEvent(BaseEvent[str]):
    """Parent event that triggers child events"""

    message: str


class ChildEvent(BaseEvent[str]):
    """Child event triggered by parent"""

    data: str


class GrandchildEvent(BaseEvent[str]):
    """Grandchild event triggered by child"""

    value: int


@pytest.fixture
async def eventbus():
    """Create an event bus for testing"""
    bus = EventBus(name='TestBus')
    yield bus
    await bus.destroy(clear=True)


class TestParentEventTracking:
    """Test automatic parent event ID tracking"""

    async def test_basic_parent_tracking(self, eventbus: EventBus):
        """Test that child events automatically get event_parent_id"""
        event_children: list[BaseEvent[Any]] = []

        async def parent_handler(event: ParentEvent) -> str:
            # Handler that dispatches a child event
            child = ChildEvent(data=f'child_of_{event.message}')
            event.emit(child)
            event_children.append(child)
            return 'parent_handled'

        eventbus.on('ParentEvent', parent_handler)

        # Dispatch parent event
        parent = ParentEvent(message='test_parent')
        parent_result = eventbus.emit(parent)

        # Wait for processing
        await eventbus.wait_until_idle()

        # Verify parent processed
        await parent_result
        parent_handler_result = next(
            (r for r in parent_result.event_results.values() if r.handler_name.endswith('parent_handler')), None
        )
        assert parent_handler_result is not None and parent_handler_result.result == 'parent_handled'

        # Verify child has event_parent_id set
        assert len(event_children) == 1
        child = event_children[0]
        assert child.event_parent_id == parent.event_id

    async def test_multi_level_parent_tracking(self, eventbus: EventBus):
        """Test parent tracking across multiple levels"""
        events_by_level: dict[str, BaseEvent[Any] | None] = {'parent': None, 'child': None, 'grandchild': None}

        async def parent_handler(event: BaseEvent[str]) -> str:
            events_by_level['parent'] = event
            child = ChildEvent(data='child_data')
            event.emit(child)
            return 'parent'

        async def child_handler(event: BaseEvent[str]) -> str:
            events_by_level['child'] = event
            grandchild = GrandchildEvent(value=42)
            event.emit(grandchild)
            return 'child'

        async def grandchild_handler(event: BaseEvent[str]) -> str:
            events_by_level['grandchild'] = event
            return 'grandchild'

        # Register handlers
        eventbus.on('ParentEvent', parent_handler)
        eventbus.on('ChildEvent', child_handler)
        eventbus.on('GrandchildEvent', grandchild_handler)

        # Start the chain
        parent = ParentEvent(message='root')
        eventbus.emit(parent)

        # Wait for all processing
        await eventbus.wait_until_idle()

        # Verify the parent chain
        assert events_by_level['parent'] is not None
        assert events_by_level['child'] is not None
        assert events_by_level['grandchild'] is not None

        # Verify the parent chain
        assert events_by_level['parent'].event_parent_id is None  # Root has no parent
        assert events_by_level['child'].event_parent_id == parent.event_id
        assert events_by_level['grandchild'].event_parent_id == events_by_level['child'].event_id

    async def test_multiple_children_same_parent(self, eventbus: EventBus):
        """Test multiple child events from same parent"""
        event_children: list[BaseEvent[Any]] = []

        async def parent_handler(event: BaseEvent[str]) -> str:
            # Dispatch multiple children
            for i in range(3):
                child = ChildEvent(data=f'child_{i}')
                event.emit(child)
                event_children.append(child)
            return 'spawned_children'

        eventbus.on('ParentEvent', parent_handler)

        # Dispatch parent
        parent = ParentEvent(message='multi_child_parent')
        eventbus.emit(parent)

        await eventbus.wait_until_idle()

        # All children should have same parent
        assert len(event_children) == 3
        for child in event_children:
            assert child.event_parent_id == parent.event_id

    async def test_parallel_handler_concurrency_parent_tracking(self, eventbus: EventBus):
        """Test parent tracking with parallel handler concurrency mode."""
        events_from_handlers: dict[str, list[BaseEvent[Any]]] = {'h1': [], 'h2': []}

        async def handler1(event: BaseEvent[str]) -> str:
            await asyncio.sleep(0.01)  # Simulate work
            child = ChildEvent(data='from_h1')
            event.emit(child)
            events_from_handlers['h1'].append(child)
            return 'h1'

        async def handler2(event: BaseEvent[str]) -> str:
            await asyncio.sleep(0.02)  # Different timing
            child = ChildEvent(data='from_h2')
            event.emit(child)
            events_from_handlers['h2'].append(child)
            return 'h2'

        # Both handlers respond to same event
        eventbus.on('ParentEvent', handler1)
        eventbus.on('ParentEvent', handler2)

        # Dispatch parent
        parent = ParentEvent(message='parallel_test')
        eventbus.emit(parent)

        await eventbus.wait_until_idle()

        # Both children should have same parent despite parallel execution
        assert len(events_from_handlers['h1']) == 1
        assert len(events_from_handlers['h2']) == 1
        assert events_from_handlers['h1'][0].event_parent_id == parent.event_id
        assert events_from_handlers['h2'][0].event_parent_id == parent.event_id

    async def test_explicit_parent_not_overridden(self, eventbus: EventBus):
        """Test that explicitly set event_parent_id is not overridden"""
        captured_child = None

        async def parent_handler(event: BaseEvent[Any]) -> str:
            nonlocal captured_child
            # Create child with explicit event_parent_id
            explicit_parent_id = '018f8e40-1234-7000-8000-000000001234'
            child = ChildEvent(data='explicit', event_parent_id=explicit_parent_id)
            event.emit(child)
            captured_child = child
            return 'dispatched'

        eventbus.on('ParentEvent', parent_handler)

        parent = ParentEvent(message='test')
        eventbus.emit(parent)

        await eventbus.wait_until_idle()

        # Explicit event_parent_id should be preserved
        assert captured_child is not None
        assert captured_child.event_parent_id == '018f8e40-1234-7000-8000-000000001234'
        assert captured_child.event_parent_id != parent.event_id

    async def test_cross_eventbus_parent_tracking(self):
        """Test parent tracking across multiple EventBuses"""
        bus1 = EventBus(name='Bus1')
        bus2 = EventBus(name='Bus2')

        captured_events: list[tuple[str, BaseEvent[Any], BaseEvent[Any] | None]] = []

        async def bus1_handler(event: BaseEvent[Any]) -> str:
            # Explicitly link a child, then forward it to bus2.
            child = ChildEvent(data='cross_bus_child')
            event.emit(child)
            bus2.emit(child)
            captured_events.append(('bus1', event, child))
            return 'bus1_handled'

        async def bus2_handler(event: BaseEvent[str]) -> str:
            captured_events.append(('bus2', event, None))
            return 'bus2_handled'

        bus1.on('ParentEvent', bus1_handler)
        bus2.on('ChildEvent', bus2_handler)

        try:
            # Dispatch parent to bus1
            parent = ParentEvent(message='cross_bus_test')
            bus1.emit(parent)

            await bus1.wait_until_idle()
            await bus2.wait_until_idle()

            # Verify parent tracking works across buses
            assert len(captured_events) == 2
            _, _parent_event, child_event = captured_events[0]
            _, received_child, _ = captured_events[1]

            assert child_event is not None and child_event.event_parent_id == parent.event_id
            assert received_child.event_parent_id == parent.event_id

        finally:
            await bus1.destroy(clear=True)
            await bus2.destroy(clear=True)

    async def test_sync_handler_parent_tracking(self, eventbus: EventBus):
        """Test parent tracking works with sync handlers"""
        event_children: list[BaseEvent[Any]] = []

        def sync_parent_handler(event: BaseEvent[str]) -> str:
            # Sync handler that dispatches child
            child = ChildEvent(data='from_sync')
            event.emit(child)
            event_children.append(child)
            return 'sync_handled'

        eventbus.on('ParentEvent', sync_parent_handler)

        parent = ParentEvent(message='sync_test')
        eventbus.emit(parent)

        await eventbus.wait_until_idle()

        # Parent tracking should work even with sync handlers
        assert len(event_children) == 1
        assert event_children[0].event_parent_id == parent.event_id

    async def test_error_handler_parent_tracking(self, eventbus: EventBus):
        """Test parent tracking when handler errors occur"""
        event_children: list[BaseEvent[Any]] = []

        async def failing_handler(event: BaseEvent[str]) -> str:
            # Dispatch child before failing
            child = ChildEvent(data='before_error')
            event.emit(child)
            event_children.append(child)
            raise ValueError(
                'Handler error - expected to fail - testing that parent event tracking works even when handlers error'
            )

        async def success_handler(event: BaseEvent[str]) -> str:
            # This should still run
            child = ChildEvent(data='after_error')
            event.emit(child)
            event_children.append(child)
            return 'success'

        eventbus.on('ParentEvent', failing_handler)
        eventbus.on('ParentEvent', success_handler)

        parent = ParentEvent(message='error_test')
        eventbus.emit(parent)

        await eventbus.wait_until_idle()

        # Both children should have event_parent_id despite error
        assert len(event_children) == 2
        for child in event_children:
            assert child.event_parent_id == parent.event_id

    async def test_event_children_tracking(self, eventbus: EventBus):
        """Test that child events are tracked in parent's event_children"""

        async def parent_handler(event: ParentEvent) -> str:
            # Dispatch multiple child events
            for i in range(3):
                child = ChildEvent(data=f'child_{i}')
                event.emit(child)
            return 'parent_done'

        async def child_handler(event: ChildEvent) -> str:
            # Handler for child events so they complete
            return f'handled_{event.data}'

        eventbus.on('ParentEvent', parent_handler)
        eventbus.on('ChildEvent', child_handler)

        # Dispatch parent event
        parent = ParentEvent(message='test_children_tracking')
        parent_event = eventbus.emit(parent)

        # Wait for all events to be processed
        await eventbus.wait_until_idle()

        # Now await the parent event
        await parent_event

        # Check that parent has child events tracked
        assert len(parent.event_children) == 3
        for i, child in enumerate(parent.event_children):
            assert isinstance(child, ChildEvent)
            assert child.data == f'child_{i}'
            assert child.event_parent_id == parent.event_id

    async def test_nested_event_children_tracking(self, eventbus: EventBus):
        """Test multi-level child event tracking"""

        async def parent_handler(event: ParentEvent) -> str:
            child = ChildEvent(data='level1')
            event.emit(child)
            return 'parent'

        async def child_handler(event: ChildEvent) -> str:
            grandchild = GrandchildEvent(value=42)
            event.emit(grandchild)
            return 'child'

        async def grandchild_handler(event: GrandchildEvent) -> str:
            return f'grandchild_{event.value}'

        eventbus.on('ParentEvent', parent_handler)
        eventbus.on('ChildEvent', child_handler)
        eventbus.on('GrandchildEvent', grandchild_handler)

        parent = ParentEvent(message='nested_test')
        parent_event = eventbus.emit(parent)
        await eventbus.wait_until_idle()
        await parent_event

        # Check parent has child
        assert len(parent.event_children) == 1
        child = parent.event_children[0]
        assert isinstance(child, ChildEvent)

        # Check child has grandchild
        assert len(child.event_children) == 1
        grandchild = child.event_children[0]
        assert isinstance(grandchild, GrandchildEvent)
        assert grandchild.value == 42

    async def test_multiple_handlers_event_children(self, eventbus: EventBus):
        """Test event_children tracking with multiple handlers"""

        async def handler1(event: ParentEvent) -> str:
            child1 = ChildEvent(data='from_handler1')
            event.emit(child1)
            return 'h1'

        async def handler2(event: ParentEvent) -> str:
            # Dispatch 2 children from this handler
            child2 = ChildEvent(data='from_handler2_a')
            child3 = ChildEvent(data='from_handler2_b')
            event.emit(child2)
            event.emit(child3)
            return 'h2'

        async def child_handler(event: ChildEvent) -> str:
            return f'handled_{event.data}'

        eventbus.on('ParentEvent', handler1)
        eventbus.on('ParentEvent', handler2)
        eventbus.on('ChildEvent', child_handler)

        parent = ParentEvent(message='multi_handler_test')
        parent_event = eventbus.emit(parent)
        await eventbus.wait_until_idle()
        await parent_event

        # Parent should have all 3 children from both handlers
        assert len(parent.event_children) == 3
        child_data = [child.data for child in parent.event_children if isinstance(child, ChildEvent)]
        assert 'from_handler1' in child_data
        assert 'from_handler2_a' in child_data
        assert 'from_handler2_b' in child_data

    async def test_event_children_empty_when_no_children(self, eventbus: EventBus):
        """Test event_children is empty when handler doesn't dispatch children"""

        async def handler(event: ParentEvent) -> str:
            # No child events dispatched
            return 'no_children'

        eventbus.on('ParentEvent', handler)

        parent = ParentEvent(message='no_children_test')
        parent_event = eventbus.emit(parent)
        await eventbus.wait_until_idle()
        await parent_event

        # Parent should have no children
        assert len(parent.event_children) == 0

    async def test_forwarded_events_not_counted_as_children(self, eventbus: EventBus):
        """Test that forwarded events (same event_id) are not counted as children"""
        bus2 = EventBus(name='Bus2')

        try:
            # Forward all events from bus1 to bus2
            eventbus.on('*', bus2.emit)

            parent = ParentEvent(message='forward_test')
            parent_event = eventbus.emit(parent)
            await eventbus.wait_until_idle()
            await bus2.wait_until_idle()
            await parent_event

            # Parent should have no children (forwarding doesn't create children)
            assert len(parent.event_children) == 0

        finally:
            await bus2.destroy(clear=True)

    async def test_parent_completion_waits_for_awaited_children(self, eventbus: EventBus):
        """Parent event completion should wait until awaited children complete."""
        completion_order: list[str] = []
        child_started = asyncio.Event()
        release_children = asyncio.Event()

        async def parent_handler(event: ParentEvent) -> str:
            child1 = ChildEvent(data='child1')
            child2 = ChildEvent(data='child2')
            emitted_child1 = event.emit(child1)
            emitted_child2 = event.emit(child2)
            assert emitted_child1.event_blocks_parent_completion is False
            assert emitted_child2.event_blocks_parent_completion is False
            await emitted_child1
            await emitted_child2
            completion_order.append('parent_handler')
            return 'parent'

        async def child_handler(event: ChildEvent) -> str:
            child_started.set()
            await release_children.wait()
            completion_order.append(f'child_handler_{event.data}')
            return f'handled_{event.data}'

        eventbus.on('ParentEvent', parent_handler)
        eventbus.on('ChildEvent', child_handler)

        parent = ParentEvent(message='completion_test')
        parent_event = eventbus.emit(parent)

        # Wait until at least one child handler has started and is blocked.
        await child_started.wait()
        assert len(parent.event_children) >= 1
        assert parent.event_completed_at is None
        assert parent.event_status != 'completed'

        release_children.set()
        await parent_event

        # Now all children should be complete
        assert parent.event_status == 'completed'
        assert len(parent.event_children) == 2
        for child in parent.event_children:
            assert child.event_blocks_parent_completion is True
            assert child.event_status == 'completed'

    async def test_event_emit_without_await_sets_parentage_without_blocking_completion(self, eventbus: EventBus):
        """Un-awaited event.emit() children keep lineage but do not keep the parent open."""
        child_started = asyncio.Event()
        release_child = asyncio.Event()
        captured_child: ChildEvent | None = None

        async def parent_handler(event: ParentEvent) -> str:
            nonlocal captured_child
            captured_child = event.emit(ChildEvent(data='unawaited_owned'))
            return 'parent'

        async def child_handler(event: ChildEvent) -> str:
            child_started.set()
            await release_child.wait()
            return f'handled_{event.data}'

        eventbus.on('ParentEvent', parent_handler)
        eventbus.on('ChildEvent', child_handler)

        parent = ParentEvent(message='unawaited_owned_completion_test')
        parent_event = eventbus.emit(parent)

        await child_started.wait()
        assert captured_child is not None
        assert captured_child.event_parent_id == parent.event_id
        assert captured_child.event_emitted_by_handler_id is not None
        assert captured_child.event_blocks_parent_completion is False
        assert captured_child in parent.event_children

        await asyncio.wait_for(parent_event.wait(), timeout=1.0)
        assert parent.event_status == 'completed'
        assert captured_child.event_status != 'completed'

        release_child.set()
        await eventbus.wait_until_idle()
        assert captured_child.event_status == 'completed'

    async def test_bus_emit_inside_handler_dispatches_detached_event_by_default(self, eventbus: EventBus):
        """bus.emit() inside a handler dispatches an independent event by default."""
        child_started = asyncio.Event()
        release_child = asyncio.Event()
        captured_child: ChildEvent | None = None

        async def parent_handler(event: ParentEvent) -> str:
            nonlocal captured_child
            captured_child = eventbus.emit(ChildEvent(data='detached'))
            return 'parent'

        async def child_handler(event: ChildEvent) -> str:
            child_started.set()
            await release_child.wait()
            return f'handled_{event.data}'

        eventbus.on('ParentEvent', parent_handler)
        eventbus.on('ChildEvent', child_handler)

        parent = ParentEvent(message='detached_completion_test')
        parent_event = eventbus.emit(parent)

        await child_started.wait()
        assert captured_child is not None
        assert captured_child.event_parent_id is None
        assert captured_child.event_emitted_by_handler_id is None
        assert captured_child.event_blocks_parent_completion is False
        assert captured_child not in parent.event_children

        await asyncio.wait_for(parent_event.wait(), timeout=1.0)
        assert parent.event_status == 'completed'
        assert captured_child.event_status != 'completed'

        release_child.set()
        await eventbus.wait_until_idle()
        assert captured_child.event_status == 'completed'

    async def test_awaited_event_emit_marks_child_as_parent_completion_blocking(self, eventbus: EventBus):
        captured_child: ChildEvent | None = None

        async def parent_handler(event: ParentEvent) -> str:
            nonlocal captured_child
            captured_child = event.emit(ChildEvent(data='owned'))
            assert captured_child.event_blocks_parent_completion is False
            await captured_child
            assert captured_child.event_blocks_parent_completion is True
            return 'parent'

        async def child_handler(event: ChildEvent) -> str:
            return f'handled_{event.data}'

        eventbus.on('ParentEvent', parent_handler)
        eventbus.on('ChildEvent', child_handler)

        parent = await eventbus.emit(ParentEvent(message='owned_completion_test'))
        assert captured_child is not None
        assert captured_child.event_parent_id == parent.event_id
        assert captured_child.event_blocks_parent_completion is True
        assert captured_child in parent.event_children

    async def test_awaiting_bus_emitted_child_keeps_independent_parentage(self, eventbus: EventBus):
        captured_child: ChildEvent | None = None

        async def parent_handler(event: ParentEvent) -> str:
            nonlocal captured_child
            captured_child = eventbus.emit(ChildEvent(data='awaited_detached'))
            assert captured_child.event_blocks_parent_completion is False
            await captured_child
            assert captured_child.event_blocks_parent_completion is False
            return 'parent'

        async def child_handler(event: ChildEvent) -> str:
            return f'handled_{event.data}'

        eventbus.on('ParentEvent', parent_handler)
        eventbus.on('ChildEvent', child_handler)

        parent = await eventbus.emit(ParentEvent(message='awaited_detached_test'))
        assert captured_child is not None
        assert captured_child.event_parent_id is None
        assert captured_child.event_emitted_by_handler_id is None
        assert captured_child.event_blocks_parent_completion is False
        assert captured_child not in parent.event_children
