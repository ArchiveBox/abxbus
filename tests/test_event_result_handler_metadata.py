from uuid import uuid4

import pytest

from abxbus.base_event import BaseEvent, EventResult
from abxbus.event_bus import EventBus
from abxbus.event_handler import EventHandler


class StandaloneEvent(BaseEvent[str]):
    data: str


@pytest.mark.asyncio
async def test_event_result_run_handler_with_base_event() -> None:
    """EventResult should run correctly when called directly with a real BaseEvent."""
    event = StandaloneEvent(data='ok')

    async def handler(_event: StandaloneEvent) -> str:
        return 'ok'

    handler_entry = EventHandler.from_callable(
        handler=handler,
        event_pattern='StandaloneEvent',
        eventbus_name='Standalone',
        eventbus_id='dafc8026-409b-7794-8067-62e302999216',
    )

    event_result: EventResult[str] = EventResult(
        event_id=event.event_id,
        handler=handler_entry,
        timeout=event.event_timeout,
        result_type=str,
    )

    test_bus = EventBus(name='StandaloneTest1')
    result_value = await event_result.run_handler(
        event,
        eventbus=test_bus,
        timeout=event.event_timeout,
    )

    assert result_value == 'ok'
    assert event_result.status == 'completed'
    assert event_result.result == 'ok'
    await test_bus.stop()


@pytest.mark.asyncio
async def test_event_and_result_without_eventbus() -> None:
    """Verify BaseEvent + EventResult work without instantiating an EventBus."""

    event = StandaloneEvent(data='message')

    def handler(evt: StandaloneEvent) -> str:
        return evt.data.upper()

    handler_entry = EventHandler.from_callable(
        handler=handler,
        event_pattern='StandaloneEvent',
        eventbus_name='EventBus',
        eventbus_id='00000000-0000-0000-0000-000000000000',
    )
    assert handler_entry.id is not None
    handler_id = handler_entry.id
    event_result = event.event_result_update(handler=handler_entry, status='pending')

    test_bus = EventBus(name='StandaloneTest2')
    value = await event_result.run_handler(
        event,
        eventbus=test_bus,
        timeout=event.event_timeout,
    )

    assert value == 'MESSAGE'
    assert event_result.status == 'completed'
    assert event.event_results[handler_id] is event_result

    await test_bus.emit(event).event_completed()
    assert event.event_completed_at is not None
    await test_bus.stop()


def test_event_handler_model_is_serializable() -> None:
    """EventHandler is a Pydantic model and can round-trip serialized metadata."""

    def handler(event: StandaloneEvent) -> str:
        return event.data

    entry = EventHandler.from_callable(
        handler=handler,
        event_pattern='StandaloneEvent',
        eventbus_name='StandaloneBus',
        eventbus_id='018f8e40-1234-7000-8000-000000001234',
    )

    dumped = entry.model_dump(mode='json')
    assert dumped['event_pattern'] == 'StandaloneEvent'
    assert dumped['eventbus_name'] == 'StandaloneBus'
    assert dumped.get('handler') is None

    loaded = EventHandler.model_validate(dumped)
    assert loaded.id == entry.id
    assert loaded.event_pattern == entry.event_pattern
    assert loaded.handler is None


def test_event_handler_id_matches_typescript_uuidv5_algorithm() -> None:
    expected_seed = '018f8e40-1234-7000-8000-000000001234|pkg.module.handler|~/project/app.py:123|2025-01-02T03:04:05.678901000Z|StandaloneEvent'
    expected_id = '0acdaf2c-a5b1-5785-8499-7c48b3c2c5d8'

    entry = EventHandler(
        handler_name='pkg.module.handler',
        handler_file_path='~/project/app.py:123',
        handler_registered_at='2025-01-02T03:04:05.678901000Z',
        event_pattern='StandaloneEvent',
        eventbus_name='StandaloneBus',
        eventbus_id='018f8e40-1234-7000-8000-000000001234',
    )

    assert (
        f'{entry.eventbus_id}|{entry.handler_name}|{entry.handler_file_path}|{entry.handler_registered_at}|{entry.event_pattern}'
        == expected_seed
    )
    assert entry.compute_handler_id() == expected_id
    assert entry.id == expected_id


def test_event_handler_model_detects_handler_file_path() -> None:
    def handler(event: StandaloneEvent) -> str:
        return event.data

    entry = EventHandler.from_callable(
        handler=handler,
        event_pattern='StandaloneEvent',
        eventbus_name='StandaloneBus',
        eventbus_id='018f8e40-1234-7000-8000-000000001234',
    )

    assert entry.handler_file_path is not None
    expected_suffix = f'test_event_result_handler_metadata.py:{handler.__code__.co_firstlineno}'
    assert entry.handler_file_path.endswith(expected_suffix)


def test_event_handler_from_callable_supports_id_override_and_detect_file_path_toggle() -> None:
    def handler(event: StandaloneEvent) -> str:
        return event.data

    explicit_id = '018f8e40-1234-7000-8000-000000009999'
    explicit = EventHandler.from_callable(
        handler=handler,
        id=explicit_id,
        event_pattern='StandaloneEvent',
        eventbus_name='StandaloneBus',
        eventbus_id='018f8e40-1234-7000-8000-000000001234',
        detect_handler_file_path=False,
    )
    assert explicit.id == explicit_id

    no_detect = EventHandler.from_callable(
        handler=handler,
        event_pattern='StandaloneEvent',
        eventbus_name='StandaloneBus',
        eventbus_id='018f8e40-1234-7000-8000-000000001234',
        detect_handler_file_path=False,
    )
    assert no_detect.handler_file_path is None


def test_event_result_update_keeps_consistent_ordering_semantics_for_status_result_error() -> None:
    def handler(event: StandaloneEvent) -> str:
        return event.data

    handler_entry = EventHandler.from_callable(
        handler=handler,
        event_pattern='StandaloneEvent',
        eventbus_name='StandaloneBus',
        eventbus_id='018f8e40-1234-7000-8000-000000001234',
    )
    event_result: EventResult[str] = EventResult(
        event_id=str(uuid4()),
        handler=handler_entry,
        timeout=None,
        result_type=str,
    )

    existing_error = RuntimeError('existing')
    event_result.error = existing_error
    event_result.update(status='completed')
    assert event_result.status == 'completed'
    assert event_result.error is existing_error

    event_result.update(status='error', result='seeded')
    assert event_result.result == 'seeded'
    assert event_result.status == 'error'


def test_event_result_serializes_handler_metadata_and_derived_fields() -> None:
    """EventResult stores handler metadata and derives convenience fields from it."""

    def handler(event: StandaloneEvent) -> str:
        return event.data

    entry = EventHandler.from_callable(
        handler=handler,
        event_pattern='StandaloneEvent',
        eventbus_name='StandaloneBus',
        eventbus_id='018f8e40-1234-7000-8000-000000001234',
    )

    result = EventResult(
        event_id=str(uuid4()),
        handler=entry,
    )
    payload = result.model_dump(mode='json')

    assert 'handler' not in payload
    assert 'result_type' not in payload
    assert payload['handler_id'] == entry.id
    assert payload['handler_name'] == entry.handler_name
    assert payload['handler_event_pattern'] == entry.event_pattern
    assert payload['eventbus_id'] == entry.eventbus_id
    assert payload['eventbus_name'] == entry.eventbus_name
