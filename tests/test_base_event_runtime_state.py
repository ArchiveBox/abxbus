"""Test that the AttributeError bug related to 'event_completed_at' is fixed"""

import asyncio
from contextlib import suppress

from abxbus import BaseEvent, EventBus
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

    await bus.stop()


async def test_event_without_handlers():
    """Test that events without handlers still work with timestamp properties"""
    event = SampleEvent(data='no_handlers')
    bus = EventBus(name='TestBusNoHandlers')

    # Should not raise AttributeError when accessing these properties
    assert event.event_started_at is None  # No handlers started
    assert event.event_completed_at is None  # Not complete yet

    processed_event = await bus.emit(event)
    await bus.stop()

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
    await bus.stop()
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
        await bus.stop()
