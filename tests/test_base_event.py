import asyncio
import gc

import pytest
from pydantic import ValidationError

from abxbus import BaseEvent, EventBus


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
    await bus.stop()


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
    await bus.stop()


async def test_event_completed_waits_in_queue_order_inside_handler():
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
        await child.event_completed()
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
    await bus.stop()


async def test_reserved_runtime_fields_are_rejected():
    with pytest.raises(ValidationError, match='Field "bus" is reserved'):
        MainEvent.model_validate({'bus': 'payload_bus_field'})
    with pytest.raises(ValidationError, match='Field "first" is reserved'):
        MainEvent.model_validate({'first': 'payload_first_field'})
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

    await bus.stop()


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

    await bus.stop()


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

    await bus.stop()


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

    await bus.stop()


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

    await bus1.stop()
    await bus2.stop()


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

    await bus1.stop()
    await bus2.stop()


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

    await bus.stop()


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

    await bus.stop()


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
        await bus.stop()
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

    await new_bus.stop()


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

        # The child event should start processing immediately within our handler
        # (due to the deadlock prevention in BaseEvent.__await__)
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

    await bus.stop()


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

    await bus1.stop()
    await bus2.stop()
