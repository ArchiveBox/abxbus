import asyncio
from collections import deque
from typing import Any, TypeAlias, cast

import pytest

from abxbus.base_event import BaseEvent, EventResult
from abxbus.event_bus import EventBus
from abxbus.helpers import CleanShutdownQueue


class SerializableEvent(BaseEvent[str]):
    value: str = 'payload'


class HandlerOrderEvent(BaseEvent[str]):
    value: str = 'order'


JsonShape: TypeAlias = str | list['JsonShape'] | dict[str, 'JsonShape']


def _json_shape(value: Any) -> JsonShape:
    if isinstance(value, list):
        return [_json_shape(item) for item in cast(list[Any], value)]
    if isinstance(value, dict):
        value_dict = cast(dict[Any, Any], value)
        return {str(key): _json_shape(item) for key, item in value_dict.items()}
    if value is None:
        return 'null'
    return type(value).__name__


def _make_bus_with_pending_event() -> tuple[EventBus, SerializableEvent, str]:
    bus = EventBus(
        name='SerializableBus',
        id='018f8e40-1234-7000-8000-000000001234',
        max_history_size=500,
        max_history_drop=False,
        event_concurrency='parallel',
        event_handler_concurrency='parallel',
        event_handler_completion='first',
        event_timeout=None,
        event_slow_timeout=34.0,
        event_handler_slow_timeout=12.0,
        event_handler_detect_file_paths=False,
    )

    def handler(event: SerializableEvent) -> str:
        return event.value

    handler_entry = bus.on(SerializableEvent, handler)
    assert handler_entry.id is not None
    handler_id = handler_entry.id

    event = SerializableEvent(value='roundtrip')
    event_result = EventResult[str](
        event_id=event.event_id,
        handler=handler_entry,
        status='completed',
        result='ok',
    )
    event.event_results[handler_id] = event_result
    bus.event_history[event.event_id] = event

    queue = CleanShutdownQueue[BaseEvent[Any]](maxsize=0)
    queue.put_nowait(event)
    bus.pending_event_queue = queue
    return bus, event, handler_id


def test_eventbus_model_dump_json_roundtrip_uses_id_keyed_structures() -> None:
    bus, event, handler_id = _make_bus_with_pending_event()

    payload = bus.model_dump()
    assert payload['handlers'].keys() == {handler_id}
    assert payload['handlers_by_key'].keys() == {'SerializableEvent'}
    assert payload['event_history'].keys() == {event.event_id}
    assert payload['pending_event_queue'] == [event.event_id]
    assert all(event_id in payload['event_history'] for event_id in payload['pending_event_queue'])

    restored = EventBus.validate(bus.model_dump_json())
    assert _json_shape(restored.model_dump()) == _json_shape(payload)
    assert restored.id == bus.id
    assert restored.name == bus.name
    assert restored.event_history.max_history_size == bus.event_history.max_history_size
    assert restored.event_history.max_history_drop == bus.event_history.max_history_drop
    assert str(restored.event_concurrency) == str(bus.event_concurrency)
    assert str(restored.event_handler_concurrency) == str(bus.event_handler_concurrency)
    assert str(restored.event_handler_completion) == str(bus.event_handler_completion)
    assert restored.event_timeout == bus.event_timeout
    assert restored.event_slow_timeout == bus.event_slow_timeout
    assert restored.event_handler_slow_timeout == bus.event_handler_slow_timeout
    assert restored.event_handler_detect_file_paths == bus.event_handler_detect_file_paths

    restored_event = restored.event_history[event.event_id]
    restored_result = restored_event.event_results[handler_id]
    assert restored_result.handler is restored.handlers[handler_id]
    assert restored_result.handler.handler is not None
    assert restored_result.handler(restored_event) is None

    assert restored.pending_event_queue is not None
    queue = cast(deque[BaseEvent[Any]], getattr(restored.pending_event_queue, '_queue'))
    assert len(queue) == 1
    assert queue[0] is restored_event


@pytest.mark.asyncio
async def test_eventbus_preserves_handler_registration_order_through_json_and_restore() -> None:
    bus = EventBus(
        name='HandlerOrderSourceBus',
        event_handler_concurrency='serial',
        event_handler_completion='all',
        event_handler_detect_file_paths=False,
    )
    original_order: list[str] = []

    async def first(event: HandlerOrderEvent) -> str:
        original_order.append('first')
        return event.value

    async def second(event: HandlerOrderEvent) -> str:
        original_order.append('second')
        return event.value

    first_entry = bus.on(HandlerOrderEvent, first)
    second_entry = bus.on(HandlerOrderEvent, second)
    assert first_entry.id is not None
    assert second_entry.id is not None
    expected_ids = [first_entry.id, second_entry.id]

    payload = bus.model_dump()
    assert list(payload['handlers'].keys()) == expected_ids
    assert payload['handlers_by_key']['HandlerOrderEvent'] == expected_ids

    await asyncio.wait_for(bus.emit(HandlerOrderEvent()), timeout=5)
    assert original_order == ['first', 'second']

    restored = EventBus.validate(payload)
    restored_payload = restored.model_dump()
    assert list(restored.handlers.keys()) == expected_ids
    assert restored.handlers_by_key['HandlerOrderEvent'] == expected_ids
    assert list(restored_payload['handlers'].keys()) == expected_ids
    assert restored_payload['handlers_by_key']['HandlerOrderEvent'] == expected_ids

    restored_order: list[str] = []

    async def restored_first(event: HandlerOrderEvent) -> str:
        restored_order.append('first')
        return event.value

    async def restored_second(event: HandlerOrderEvent) -> str:
        restored_order.append('second')
        return event.value

    restored.handlers[first_entry.id].handler = restored_first
    restored.handlers[second_entry.id].handler = restored_second

    await asyncio.wait_for(restored.emit(HandlerOrderEvent()), timeout=5)
    assert restored_order == ['first', 'second']

    await bus.stop(clear=True)
    await restored.stop(clear=True)


def test_baseevent_model_validate_roundtrips_runtime_json_shape() -> None:
    bus, event, _handler_id = _make_bus_with_pending_event()
    event_payload = bus.model_dump()['event_history'][event.event_id]

    restored_payload = BaseEvent.model_validate(event_payload).model_dump(mode='json')
    assert _json_shape(restored_payload) == _json_shape(event_payload)
    assert restored_payload == event_payload


def test_eventbus_validate_creates_missing_handler_entries_from_event_results() -> None:
    bus, event, handler_id = _make_bus_with_pending_event()
    payload = bus.model_dump()

    payload['handlers'] = {}
    payload['handlers_by_key'] = {}

    restored = EventBus.validate(payload)
    assert handler_id in restored.handlers
    assert 'SerializableEvent' in restored.handlers_by_key
    assert handler_id in restored.handlers_by_key['SerializableEvent']

    restored_result = restored.event_history[event.event_id].event_results[handler_id]
    assert restored_result.handler is restored.handlers[handler_id]
    assert restored_result.handler.handler is not None
    assert restored_result.handler(restored.event_history[event.event_id]) is None


def test_eventbus_model_dump_promotes_pending_events_into_event_history() -> None:
    bus = EventBus(name='QueueOnlyBus', event_handler_detect_file_paths=False)
    event = SerializableEvent(value='queued-only')

    queue = CleanShutdownQueue[BaseEvent[Any]](maxsize=0)
    queue.put_nowait(event)
    bus.pending_event_queue = queue

    payload = bus.model_dump()
    assert payload['pending_event_queue'] == [event.event_id]
    assert event.event_id in payload['event_history']
