"""Test typed event results with automatic casting."""

# pyright: reportAssertTypeFailure=false
# pyright: reportUnnecessaryIsInstance=false

import asyncio
import logging
from typing import Any, Literal, assert_type

from pydantic import BaseModel

from abxbus import BaseEvent, EventBus


class ScreenshotEventResult(BaseModel):
    screenshot_base64: bytes | None = None
    error: str | None = None


class ScreenshotEvent(BaseEvent[ScreenshotEventResult]):
    screenshot_width: int = 1080
    screenshot_height: int = 900


class StringEvent(BaseEvent[str]):
    pass


class IntEvent(BaseEvent[int]):
    pass


async def test_pydantic_model_result_casting():
    """Test that handler results are automatically cast to Pydantic models."""
    bus = EventBus(name='pydantic_test_bus')

    def screenshot_handler(event: ScreenshotEvent):
        # Return a dict that should be cast to ScreenshotEventResult
        return {'screenshot_base64': b'fake_screenshot_data', 'error': None}

    bus.on('ScreenshotEvent', screenshot_handler)

    event = ScreenshotEvent(screenshot_width=1920, screenshot_height=1080)
    await bus.emit(event)

    # Get the result
    result = await event.event_result()

    # Verify it was cast to the correct type
    assert isinstance(result, ScreenshotEventResult)
    assert result.screenshot_base64 == b'fake_screenshot_data'
    assert result.error is None

    await bus.stop(clear=True)


async def test_builtin_type_casting():
    """Test that handler results are automatically cast to built-in types."""
    bus = EventBus(name='builtin_test_bus')

    def string_handler(event: StringEvent):
        return '42'  # Return a proper string

    def int_handler(event: IntEvent):
        return 123  # Return a proper int

    bus.on('StringEvent', string_handler)
    bus.on('IntEvent', int_handler)

    # Test string validation
    string_event = StringEvent()
    await bus.emit(string_event)
    string_result = await string_event.event_result()
    assert isinstance(string_result, str)
    assert string_result == '42'

    # Test int validation
    int_event = IntEvent()
    await bus.emit(int_event)
    int_result = await int_event.event_result()
    assert isinstance(int_result, int)
    assert int_result == 123
    await bus.stop(clear=True)


async def test_casting_failure_handling():
    """Test that casting failures are handled gracefully."""
    bus = EventBus(name='failure_test_bus')

    def bad_handler(event: IntEvent):
        return 'not_a_number'  # Should fail validation as int

    bus.on('IntEvent', bad_handler)

    event = IntEvent()
    await bus.emit(event)

    # The event should complete but the result should be an error
    await event.event_results_list(raise_if_any=False, raise_if_none=False)
    handler_id = list(event.event_results.keys())[0]
    event_result = event.event_results[handler_id]

    assert event_result.status == 'error'
    assert isinstance(event_result.error, ValueError)
    assert 'expected event_result_type' in str(event_result.error)

    await bus.stop(clear=True)


async def test_no_casting_when_no_result_type():
    """Test that events without result_type work normally."""
    bus = EventBus(name='normal_test_bus')

    class NormalEvent(BaseEvent[None]):
        pass  # No event_result_type specified

    def normal_handler(event: NormalEvent):
        return {'raw': 'data'}

    bus.on('NormalEvent', normal_handler)

    event = NormalEvent()
    await bus.emit(event)

    result = await event.event_result()

    # Should remain as original dict, no casting
    assert isinstance(result, dict)
    assert result == {'raw': 'data'}

    await bus.stop(clear=True)


async def test_result_type_stored_in_event_result():
    """Test that result_type is stored in EventResult for inspection."""
    bus = EventBus(name='storage_test_bus')

    def handler(event: StringEvent):
        return '123'  # Already a string, will validate successfully

    bus.on('StringEvent', handler)

    event = StringEvent()
    await bus.emit(event)

    # Check that result_type is accessible
    handler_id = list(event.event_results.keys())[0]
    event_result = event.event_results[handler_id]

    assert event_result.result_type is str
    assert isinstance(event_result.result, str)
    assert event_result.result == '123'

    await bus.stop(clear=True)


async def test_typed_accessors_normalize_forwarded_event_results_to_none():
    """Typed accessors should not surface BaseEvent forwarding returns as typed payloads."""
    bus = EventBus(name='forwarded_result_normalization_bus')

    class ForwardingTypedEvent(BaseEvent[int]):
        pass

    def forward_handler(event: ForwardingTypedEvent):
        return BaseEvent(event_type='ForwardedEventFromHandler')

    bus.on(ForwardingTypedEvent, forward_handler)

    event = await bus.emit(ForwardingTypedEvent())

    def include_all(_: Any) -> bool:
        return True

    result = await event.event_result(include=include_all, raise_if_any=False, raise_if_none=False)
    results_list = await event.event_results_list(include=include_all, raise_if_any=False, raise_if_none=False)
    assert result is None
    assert results_list == [None]

    await bus.stop(clear=True)


async def test_event_result_returns_first_filtered_value_in_handler_registration_order():
    """Result accessors should use handler registration order, not completion timestamps."""
    bus = EventBus(name='event_result_registration_order_bus', event_handler_concurrency='parallel')

    class AccessorEvent(BaseEvent[str]):
        pass

    completed_order: list[str] = []

    async def null_handler(event: AccessorEvent) -> None:
        await asyncio.sleep(0.03)
        completed_order.append('null')
        return None

    async def winner_handler(event: AccessorEvent) -> str:
        await asyncio.sleep(0.02)
        completed_order.append('winner')
        return 'winner'

    async def late_handler(event: AccessorEvent) -> str:
        completed_order.append('late')
        return 'late'

    handlers = [
        bus.on(AccessorEvent, null_handler),
        bus.on(AccessorEvent, winner_handler),
        bus.on(AccessorEvent, late_handler),
    ]
    for handler in handlers:
        handler.handler_registered_at = '2026-01-01T00:00:00.000000Z'

    event = bus.emit(AccessorEvent())
    assert await event.event_result(raise_if_any=False, raise_if_none=True) == 'winner'
    assert await event.event_results_list(raise_if_any=False, raise_if_none=True) == ['winner', 'late']
    assert list(event.event_results) == [handler.id for handler in handlers]
    assert completed_order == ['late', 'winner', 'null']

    await bus.stop(clear=True)


async def test_run_handler_marks_started_after_handler_lock_entry():
    """Result status should remain pending while waiting on the handler lock."""
    bus = EventBus(name='handler_start_order_bus', event_handler_concurrency='serial')

    class LockOrderEvent(BaseEvent[str]):
        pass

    first_handler_started = asyncio.Event()
    release_first_handler = asyncio.Event()

    async def first_handler(_event: LockOrderEvent) -> str:
        first_handler_started.set()
        await release_first_handler.wait()
        return 'first'

    async def second_handler(_event: LockOrderEvent) -> str:
        return 'second'

    first_entry = bus.on(LockOrderEvent, first_handler)
    second_entry = bus.on(LockOrderEvent, second_handler)
    event = LockOrderEvent()
    pending_event = bus.emit(event)
    await first_handler_started.wait()

    assert first_entry.id in event.event_results
    assert second_entry.id in event.event_results
    assert event.event_results[first_entry.id].status == 'started'
    assert event.event_results[second_entry.id].status == 'pending'

    release_first_handler.set()
    await pending_event.event_completed()
    assert event.event_results[first_entry.id].status == 'completed'
    assert event.event_results[second_entry.id].status == 'completed'
    assert event.event_results[first_entry.id].result == 'first'
    assert event.event_results[second_entry.id].result == 'second'

    await bus.stop(clear=True)


async def test_run_handler_starts_slow_monitor_after_lock_wait(caplog: Any):
    """Slow handler warning should be based on handler runtime, not lock wait time."""
    bus = EventBus(
        name='handler_slow_monitor_start_order_bus',
        event_handler_concurrency='serial',
        event_handler_slow_timeout=1.0,
    )

    class SlowMonitorOrderEvent(BaseEvent[str]):
        pass

    first_handler_started = asyncio.Event()
    release_first_handler = asyncio.Event()

    async def first_handler(_event: SlowMonitorOrderEvent) -> str:
        first_handler_started.set()
        await release_first_handler.wait()
        return 'first'

    async def second_handler(_event: SlowMonitorOrderEvent) -> str:
        await asyncio.sleep(0.03)
        return 'ok'

    bus.on(SlowMonitorOrderEvent, first_handler)
    slow_entry = bus.on(SlowMonitorOrderEvent, second_handler)
    slow_entry.handler_slow_timeout = 0.01
    event = SlowMonitorOrderEvent()

    caplog.set_level(logging.WARNING, logger='abxbus')

    pending_event = bus.emit(event)
    await first_handler_started.wait()
    await asyncio.sleep(0.03)

    slow_handler_messages_before_release = [
        record.message
        for record in caplog.records
        if 'Slow event handler' in record.message and slow_entry.label in record.message
    ]
    assert slow_handler_messages_before_release == []

    release_first_handler.set()

    try:
        await pending_event.event_completed()
    finally:
        await bus.stop(clear=True)

    slow_handler_messages_after_release = [
        record.message
        for record in caplog.records
        if 'Slow event handler' in record.message and slow_entry.label in record.message
    ]
    assert slow_handler_messages_after_release


async def test_find_type_inference():
    """Test that EventBus.find() returns the correct typed event."""
    bus = EventBus(name='expect_type_test_bus')

    class CustomResult(BaseModel):
        data: str

    class SpecificEvent(BaseEvent[CustomResult]):
        request_id: str = 'd1ca37d6-fdda-7e2b-8658-c8bb34034376'

    # Validate inline isinstance usage works with await find()
    async def dispatch_inline_isinstance():
        await asyncio.sleep(0.01)
        bus.emit(SpecificEvent(request_id='57d2fad5-8864-7f52-89ea-e4200dbf3599'))

    inline_isinstance_task = asyncio.create_task(dispatch_inline_isinstance())
    assert isinstance(await bus.find(SpecificEvent, past=False, future=1.0), SpecificEvent)
    await inline_isinstance_task

    # Validate inline assert_type usage works with await find()
    async def dispatch_inline_assert_type():
        await asyncio.sleep(0.01)
        bus.emit(SpecificEvent(request_id='87d233ab-822c-71e7-8564-39cd69531436'))

    inline_type_task = asyncio.create_task(dispatch_inline_assert_type())
    assert_type(await bus.find(SpecificEvent, past=False, future=1.0), SpecificEvent | None)
    await inline_type_task

    # Validate assert_type with isinstance expression
    async def dispatch_inline_isinstance_type():
        await asyncio.sleep(0.01)
        bus.emit(SpecificEvent(request_id='9853009a-1c66-70fa-89da-e9407d0c66dc'))

    inline_isinstance_type_task = asyncio.create_task(dispatch_inline_isinstance_type())
    assert_type(isinstance(await bus.find(SpecificEvent, past=False, future=1.0), SpecificEvent), bool)
    await inline_isinstance_type_task

    # Start a task that will dispatch the event
    async def dispatch_later():
        await asyncio.sleep(0.01)
        bus.emit(SpecificEvent(request_id='34f39b71-07a5-719b-8734-a1b0ee5d5c27'))

    dispatch_task = asyncio.create_task(dispatch_later())

    # Use find with the event class - should return SpecificEvent type
    expected_event = await bus.find(SpecificEvent, past=False, future=1.0)
    assert expected_event is not None
    assert isinstance(expected_event, SpecificEvent)

    # Type checking - this should work without cast
    assert_type(expected_event, SpecificEvent)  # Verify type is SpecificEvent, not BaseEvent[Any]

    # Runtime check
    assert type(expected_event) is SpecificEvent
    assert expected_event.request_id == '34f39b71-07a5-719b-8734-a1b0ee5d5c27'

    # Test with filters - type should still be preserved
    async def dispatch_multiple():
        await asyncio.sleep(0.01)
        bus.emit(SpecificEvent(request_id='32b90140-a7ee-7ae7-830c-71a099e93cb3'))
        bus.emit(SpecificEvent(request_id='519664bf-c9fa-7654-896b-fb0cc5b6adab'))

    dispatch_task2 = asyncio.create_task(dispatch_multiple())

    # find with where filter
    def is_correct(event: SpecificEvent) -> bool:
        return event.request_id == '519664bf-c9fa-7654-896b-fb0cc5b6adab'

    filtered_event = await bus.find(
        SpecificEvent,
        where=is_correct,
        past=False,
        future=1.0,
    )
    assert filtered_event is not None

    assert_type(filtered_event, SpecificEvent)  # Should still be SpecificEvent
    assert isinstance(filtered_event, SpecificEvent)
    assert type(filtered_event) is SpecificEvent
    assert filtered_event.request_id == '519664bf-c9fa-7654-896b-fb0cc5b6adab'

    # Test with string event type - returns BaseEvent[Any]
    async def dispatch_string_event():
        await asyncio.sleep(0.01)
        bus.emit(BaseEvent(event_type='StringEvent'))

    dispatch_task3 = asyncio.create_task(dispatch_string_event())
    string_event = await bus.find('StringEvent', past=False, future=1.0)
    assert string_event is not None

    assert_type(string_event, BaseEvent[Any])  # Should be BaseEvent[Any]
    assert string_event.event_type == 'StringEvent'

    await dispatch_task
    await dispatch_task2
    await dispatch_task3

    await bus.stop(clear=True)


async def test_find_past_type_inference():
    """Test that EventBus.find() with past-window returns the correct typed event."""
    bus = EventBus(name='query_type_test_bus')

    class QueryEvent(BaseEvent[str]):
        pass

    # Dispatch an event so it appears in history
    event = bus.emit(QueryEvent())
    await bus.wait_until_idle()

    assert isinstance(await bus.find(QueryEvent, past=10, future=False), QueryEvent)
    assert_type(await bus.find(QueryEvent, past=10, future=False), QueryEvent | None)
    assert_type(isinstance(await bus.find(QueryEvent, past=10, future=False), QueryEvent), bool)
    queried = await bus.find(QueryEvent, past=10, future=False)

    assert queried is not None
    assert isinstance(queried, QueryEvent)
    assert_type(queried, QueryEvent)
    assert queried.event_id == event.event_id

    await bus.stop(clear=True)


async def test_dispatch_type_inference():
    """Test that EventBus.emit() returns the same type as its input."""
    bus = EventBus(name='type_inference_test_bus')

    class CustomResult(BaseModel):
        value: str

    class CustomEvent(BaseEvent[CustomResult]):
        pass

    # Create an event instance
    original_event = CustomEvent()

    # Dispatch should return the same type WITHOUT needing cast()
    dispatched_event = bus.emit(original_event)
    assert isinstance(dispatched_event, CustomEvent)

    # Type checking - this should work without cast
    assert_type(dispatched_event, CustomEvent)  # Should be CustomEvent, not BaseEvent[Any]

    # Runtime check
    assert type(dispatched_event) is CustomEvent
    assert dispatched_event is original_event  # Should be the same object

    # The returned event should be fully typed
    async def handler(event: CustomEvent) -> CustomResult:
        return CustomResult(value='test')

    bus.on('CustomEvent', handler)

    # Validate inline isinstance usage works with emit()
    another_event = CustomEvent()
    assert isinstance(bus.emit(another_event), CustomEvent)

    # Validate assert_type captures emit() return type when called inline
    type_event = CustomEvent()
    dispatched_type_event = bus.emit(type_event)
    assert_type(dispatched_type_event, CustomEvent)

    # Validate assert_type with isinstance expression using emit()
    isinstance_type_event = CustomEvent()
    assert_type(isinstance(bus.emit(isinstance_type_event), CustomEvent), Literal[True])

    # We should be able to use it without casting
    result = await dispatched_event.event_result()

    # Type checking for the result
    assert_type(result, CustomResult | None)  # Should be CustomResult | None

    # Test that we can access type-specific attributes without cast
    # This would fail type checking if dispatched_event was BaseEvent[Any]
    assert dispatched_event.event_type == 'CustomEvent'

    # Demonstrate the improvement - no cast needed!
    # Before: event = cast(CustomEvent, bus.emit(CustomEvent()))
    # After: event = bus.emit(CustomEvent())  # Type is preserved!

    await another_event.event_result()
    await type_event.event_result()
    await isinstance_type_event.event_result()

    await bus.stop(clear=True)


# Consolidated from tests/test_auto_event_result_schema.py

# Test automatic event_result_type extraction from Generic type parameters.

from dataclasses import dataclass

import pytest
from pydantic import BaseModel, TypeAdapter, ValidationError
from pydantic_core import to_jsonable_python
from typing_extensions import TypedDict

from abxbus.base_event import BaseEvent
from abxbus.helpers import extract_basemodel_generic_arg


def _to_plain(value: Any) -> Any:
    return to_jsonable_python(value)


def _event_result_schema_json(event: BaseEvent[Any]) -> dict[str, Any]:
    raw_schema = event.model_dump(mode='json')['event_result_type']
    return TypeAdapter(dict[str, Any]).validate_python(raw_schema)


class UserData(BaseModel):
    name: str
    age: int


class TaskResult(BaseModel):
    task_id: str
    status: str


class ModuleLevelResult(BaseModel):
    """Module-level result type for testing auto-detection."""

    result_id: str
    data: dict[str, Any]
    success: bool


class NestedModuleResult(BaseModel):
    """Another module-level type for testing complex generics."""

    items: list[str]
    metadata: dict[str, int]


class EmailMessage(BaseModel):
    """Module-level type for testing extract_basemodel_generic_arg."""

    subject: str
    body: str
    recipients: list[str]


class ProfileResult(TypedDict):
    user_id: str
    active: bool
    score: int


class OptionalProfileResult(TypedDict, total=False):
    nickname: str
    age: int


@dataclass
class DataClassResult:
    task_id: str
    priority: int


def test_builtin_types_auto_extraction():
    """Built-in Generic[T] values populate result schema."""

    class StringEvent(BaseEvent[str]):
        message: str = 'Hello'

    class IntEvent(BaseEvent[int]):
        number: int = 42

    class FloatEvent(BaseEvent[float]):
        value: float = 3.14

    string_event = StringEvent()
    int_event = IntEvent()
    float_event = FloatEvent()

    assert string_event.event_result_type is str
    assert int_event.event_result_type is int
    assert float_event.event_result_type is float


def test_custom_pydantic_models_auto_extraction():
    """Custom Pydantic result schemas are extracted from Generic[T]."""

    class UserEvent(BaseEvent[UserData]):
        user_id: str = 'fbf27f90-5cc9-798d-8f41-e09f2689f208'

    class TaskEvent(BaseEvent[TaskResult]):
        batch_id: str = 'b497c95e-a753-77e6-8739-e8a2d3d8ae42'

    user_event = UserEvent()
    task_event = TaskEvent()

    assert user_event.event_result_type is UserData
    assert task_event.event_result_type is TaskResult


def test_complex_generic_types_auto_extraction():
    """Complex Generic[T] values are extracted."""

    class ListEvent(BaseEvent[list[str]]):
        pass

    class DictEvent(BaseEvent[dict[str, int]]):
        pass

    class SetEvent(BaseEvent[set[int]]):
        pass

    list_event = ListEvent()
    dict_event = DictEvent()
    set_event = SetEvent()

    assert list_event.event_result_type == list[str]
    assert dict_event.event_result_type == dict[str, int]
    assert set_event.event_result_type == set[int]


def test_complex_generic_with_custom_types():
    """Test complex generics containing custom types."""

    class TaskListEvent(BaseEvent[list[TaskResult]]):
        batch_id: str = 'batch456'

    task_list_event = TaskListEvent()

    assert task_list_event.event_result_type == list[TaskResult]


@pytest.mark.parametrize(
    ('json_schema', 'expected_schema'),
    [
        ({'type': 'string'}, str),
        ({'type': 'number'}, float),
        ({'type': 'integer'}, int),
        ({'type': 'boolean'}, bool),
        ({'type': 'null'}, type(None)),
    ],
)
def test_json_schema_primitive_deserialization(json_schema: dict[str, str], expected_schema: Any):
    """Primitive JSON Schema payloads reconstruct to Python runtime types."""
    event = BaseEvent[Any].model_validate({'event_type': 'SchemaEvent', 'event_result_type': json_schema})

    assert event.event_result_type is expected_schema
    serialized_schema = _event_result_schema_json(event)
    assert serialized_schema.get('type') == json_schema['type']


def test_json_schema_list_of_models_deserialization():
    """Array schemas with $defs/$ref rehydrate into list[BaseModel]-compatible validators."""
    json_schema = TypeAdapter(list[UserData]).json_schema()
    event = BaseEvent[Any].model_validate({'event_type': 'SchemaEvent', 'event_result_type': json_schema})

    adapter = TypeAdapter(event.event_result_type)
    validated = TypeAdapter(list[Any]).validate_python(adapter.validate_python([{'name': 'alice', 'age': 33}]))
    assert len(validated) == 1
    assert isinstance(validated[0], BaseModel)
    assert validated[0].model_dump() == {'name': 'alice', 'age': 33}

    serialized_schema = _event_result_schema_json(event)
    assert serialized_schema.get('type') == 'array'
    assert '$defs' in serialized_schema


def test_json_schema_nested_object_collection_deserialization():
    """Nested dict[str, list[BaseModel]] schemas rehydrate into fully typed validators."""
    json_schema = TypeAdapter(dict[str, list[TaskResult]]).json_schema()
    event = BaseEvent[Any].model_validate({'event_type': 'SchemaEvent', 'event_result_type': json_schema})

    adapter = TypeAdapter(event.event_result_type)
    validated = adapter.validate_python({'batch_a': [{'task_id': '6b2e9266-87c4-7d4a-81e5-a6026165e14b', 'status': 'ok'}]})
    assert isinstance(validated, dict)
    assert isinstance(validated['batch_a'], list)
    assert isinstance(validated['batch_a'][0], BaseModel)
    assert validated['batch_a'][0].model_dump() == {'task_id': '6b2e9266-87c4-7d4a-81e5-a6026165e14b', 'status': 'ok'}

    serialized_schema = _event_result_schema_json(event)
    assert serialized_schema.get('type') == 'object'
    assert '$defs' in serialized_schema


@pytest.mark.parametrize(
    ('shape', 'payload'),
    [
        (list[str], ['a', 'b']),
        (tuple[str, int], ['a', 7]),
        (dict[str, list[int]], {'scores': [1, 2, 3]}),
        (list[tuple[str, int]], [['x', 1], ['y', 2]]),
        (list[UserData], [{'name': 'alice', 'age': 33}]),
        (dict[str, list[TaskResult]], {'batch_a': [{'task_id': '6b2e9266-87c4-7d4a-81e5-a6026165e14b', 'status': 'ok'}]}),
    ],
)
def test_json_schema_top_level_shape_deserialization_matrix(shape: Any, payload: Any):
    """Top-level collection shapes rehydrate into equivalent runtime validators."""
    json_schema = TypeAdapter(shape).json_schema()
    event = BaseEvent[Any].model_validate({'event_type': 'SchemaEvent', 'event_result_type': json_schema})

    hydrated_adapter = TypeAdapter(event.event_result_type)
    expected_adapter = TypeAdapter(shape)

    hydrated_value = hydrated_adapter.validate_python(payload)
    expected_value = expected_adapter.validate_python(payload)
    assert _to_plain(hydrated_value) == _to_plain(expected_value)

    serialized_schema = _event_result_schema_json(event)
    assert '$schema' in serialized_schema


def test_json_schema_typed_dict_rehydrates_to_pydantic_model():
    """TypedDict schemas rehydrate into dynamic pydantic models."""
    json_schema = TypeAdapter(ProfileResult).json_schema()
    event = BaseEvent[Any].model_validate({'event_type': 'SchemaEvent', 'event_result_type': json_schema})

    assert isinstance(event.event_result_type, type)
    assert issubclass(event.event_result_type, BaseModel)

    adapter = TypeAdapter(event.event_result_type)
    validated = adapter.validate_python({'user_id': 'e692b6cb-ae63-773b-8557-3218f7ce5ced', 'active': True, 'score': 9})
    assert isinstance(validated, BaseModel)
    assert validated.model_dump() == {'user_id': 'e692b6cb-ae63-773b-8557-3218f7ce5ced', 'active': True, 'score': 9}


def test_json_schema_optional_typed_dict_is_lax_on_missing_fields():
    """Non-required TypedDict fields should not fail hydration-time validation."""
    json_schema = TypeAdapter(OptionalProfileResult).json_schema()
    event = BaseEvent[Any].model_validate({'event_type': 'SchemaEvent', 'event_result_type': json_schema})

    adapter = TypeAdapter(event.event_result_type)
    empty_validated = adapter.validate_python({})
    assert isinstance(empty_validated, BaseModel)

    partial_validated = adapter.validate_python({'nickname': 'squash'})
    assert isinstance(partial_validated, BaseModel)
    assert partial_validated.model_dump(exclude_none=True) == {'nickname': 'squash'}


def test_json_schema_dataclass_rehydrates_to_pydantic_model():
    """Dataclass schemas rehydrate into dynamic pydantic models."""
    json_schema = TypeAdapter(DataClassResult).json_schema()
    event = BaseEvent[Any].model_validate({'event_type': 'SchemaEvent', 'event_result_type': json_schema})

    adapter = TypeAdapter(event.event_result_type)
    validated = adapter.validate_python({'task_id': '16272e4a-6936-7e87-872b-0eadeb911f9d', 'priority': 2})
    assert isinstance(validated, BaseModel)
    assert validated.model_dump() == {'task_id': '16272e4a-6936-7e87-872b-0eadeb911f9d', 'priority': 2}


def test_json_schema_list_of_dataclass_rehydrates_to_list_of_models():
    """Nested dataclass objects inside collections should rehydrate cleanly."""
    json_schema = TypeAdapter(list[DataClassResult]).json_schema()
    event = BaseEvent[Any].model_validate({'event_type': 'SchemaEvent', 'event_result_type': json_schema})

    adapter = TypeAdapter(event.event_result_type)
    validated = adapter.validate_python([{'task_id': '78cfaa39-d697-7ef5-8e62-19b94b2cb48e', 'priority': 5}])
    assert isinstance(validated, list)
    assert isinstance(validated[0], BaseModel)
    assert validated[0].model_dump() == {'task_id': '78cfaa39-d697-7ef5-8e62-19b94b2cb48e', 'priority': 5}


async def test_json_schema_nested_object_and_array_runtime_enforcement():
    """Nested object/array schemas reconstructed from JSON enforce handler return values."""
    from abxbus import EventBus

    nested_schema = {
        'type': 'object',
        'properties': {
            'items': {'type': 'array', 'items': {'type': 'integer'}},
            'meta': {'type': 'object', 'additionalProperties': {'type': 'boolean'}},
        },
        'required': ['items', 'meta'],
    }

    bus = EventBus(name='nested_schema_runtime_bus')

    async def valid_handler(event: BaseEvent[Any]) -> dict[str, Any]:
        return {'items': [1, 2, 3], 'meta': {'ok': True, 'cached': False}}

    bus.on('NestedSchemaEvent', valid_handler)

    valid_event = BaseEvent[Any].model_validate({'event_type': 'NestedSchemaEvent', 'event_result_type': nested_schema})
    await bus.emit(valid_event)
    valid_result = next(iter(valid_event.event_results.values()))
    assert valid_result.status == 'completed'
    assert valid_result.error is None
    assert isinstance(valid_result.result, BaseModel)
    assert valid_result.result.model_dump() == {'items': [1, 2, 3], 'meta': {'ok': True, 'cached': False}}

    bus.handlers.clear()

    async def invalid_handler(event: BaseEvent[Any]) -> dict[str, Any]:
        return {'items': ['not-an-int'], 'meta': {'ok': 'yes'}}

    bus.on('NestedSchemaEvent', invalid_handler)
    invalid_event = BaseEvent[Any].model_validate({'event_type': 'NestedSchemaEvent', 'event_result_type': nested_schema})
    await bus.emit(invalid_event)
    invalid_result = next(iter(invalid_event.event_results.values()))
    assert invalid_result.status == 'error'
    assert invalid_result.error is not None

    await bus.stop(clear=True)


def test_no_generic_parameter():
    """Test that events without generic parameters don't get auto-set types."""

    class PlainEvent(BaseEvent):
        message: str = 'plain'

    plain_event = PlainEvent()

    # Should remain None since no schema was provided
    assert plain_event.event_result_type is None


def test_none_generic_parameter():
    """Test that BaseEvent[None] results in None type."""

    class NoneEvent(BaseEvent[None]):
        message: str = 'none'

    none_event = NoneEvent()

    # Should remain unset
    assert none_event.event_result_type is None


def test_nested_inheritance():
    """Test that generic type extraction works with nested inheritance."""

    class BaseUserEvent(BaseEvent[UserData]):
        pass

    class SpecificUserEvent(BaseUserEvent):
        specific_field: str = 'specific'

    specific_event = SpecificUserEvent()

    # Should inherit schema/type metadata from parent generic.
    assert specific_event.event_result_type is UserData


def test_module_level_types_auto_extraction():
    """Test that module-level schemas are automatically detected."""

    class ModuleEvent(BaseEvent[ModuleLevelResult]):
        operation: str = 'test_op'

    class NestedModuleEvent(BaseEvent[NestedModuleResult]):
        batch_id: str = 'batch123'

    module_event = ModuleEvent()
    nested_event = NestedModuleEvent()

    # Should auto-detect module-level schemas.
    assert module_event.event_result_type is ModuleLevelResult
    assert nested_event.event_result_type is NestedModuleResult


def test_complex_module_level_generics():
    """Test complex generics with module-level types are auto-detected."""

    class ListModuleEvent(BaseEvent[list[ModuleLevelResult]]):
        batch_size: int = 10

    class DictModuleEvent(BaseEvent[dict[str, NestedModuleResult]]):
        mapping_type: str = 'result_map'

    list_event = ListModuleEvent()
    dict_event = DictModuleEvent()

    # Should auto-detect complex schemas.
    assert list_event.event_result_type == list[ModuleLevelResult]
    assert dict_event.event_result_type == dict[str, NestedModuleResult]


async def test_module_level_runtime_enforcement():
    """Test that module-level auto-detected types are enforced at runtime."""
    from abxbus import EventBus

    class RuntimeEvent(BaseEvent[ModuleLevelResult]):
        operation: str = 'runtime_test'

    # Verify auto-detection worked
    test_event = RuntimeEvent()
    assert test_event.event_result_type is ModuleLevelResult, f'Auto-detection failed: got {test_event.event_result_type}'

    bus = EventBus(name='runtime_test_bus')

    def correct_handler(event: RuntimeEvent):
        # Return dict that matches ModuleLevelResult schema
        return {'result_id': 'e1bb315c-472f-7bd1-8e72-c8502e1a9a36', 'data': {'key': 'value'}, 'success': True}

    def incorrect_handler(event: RuntimeEvent):
        # Return something that doesn't match ModuleLevelResult
        return {'wrong': 'format'}

    # Test correct handler
    bus.on('RuntimeEvent', correct_handler)

    event1 = RuntimeEvent()
    await bus.emit(event1)
    result1 = await event1.event_result()

    # Should be cast to ModuleLevelResult
    assert isinstance(result1, ModuleLevelResult)
    assert result1.result_id == 'e1bb315c-472f-7bd1-8e72-c8502e1a9a36'
    assert result1.data == {'key': 'value'}
    assert result1.success is True

    # Test incorrect handler
    bus.handlers.clear()  # Clear previous handler
    bus.on('RuntimeEvent', incorrect_handler)

    event2 = RuntimeEvent()
    await bus.emit(event2)

    # Should get an error due to validation failure
    handler_id = list(event2.event_results.keys())[0]
    event_result = event2.event_results[handler_id]

    assert event_result.status == 'error'
    assert isinstance(event_result.error, Exception)

    await bus.stop(clear=True)


def test_extract_basemodel_generic_arg_basic():
    """Test extract_basemodel_generic_arg with basic types."""

    # Test BaseEvent[int]
    class IntResultEvent(BaseEvent[int]):
        pass

    result = extract_basemodel_generic_arg(IntResultEvent)
    assert result is int


def test_extract_basemodel_generic_arg_dict():
    """Test extract_basemodel_generic_arg with dict types."""

    # Test BaseEvent[dict[str, int]]
    class DictIntEvent(BaseEvent[dict[str, int]]):
        pass

    result = extract_basemodel_generic_arg(DictIntEvent)
    assert result == dict[str, int]


def test_extract_basemodel_generic_arg_dict_with_module_type():
    """Test extract_basemodel_generic_arg with dict containing module-level type."""

    # Test BaseEvent[dict[str, EmailMessage]]
    class DictEmailEvent(BaseEvent[dict[str, EmailMessage]]):
        pass

    result = extract_basemodel_generic_arg(DictEmailEvent)
    assert result == dict[str, EmailMessage]


def test_extract_basemodel_generic_arg_dict_with_local_type():
    """Test extract_basemodel_generic_arg with dict containing locally defined type."""

    # Define local type
    class EmailAttachment(BaseModel):
        filename: str
        content: bytes
        mime_type: str

    # Test BaseEvent[dict[str, EmailAttachment]]
    class DictAttachmentEvent(BaseEvent[dict[str, EmailAttachment]]):
        pass

    result = extract_basemodel_generic_arg(DictAttachmentEvent)
    assert result == dict[str, EmailAttachment]


def test_extract_basemodel_generic_arg_no_generic():
    """Test extract_basemodel_generic_arg with BaseEvent (no generic parameter)."""

    # Test BaseEvent without generic parameter
    class PlainEvent(BaseEvent):
        pass

    result = extract_basemodel_generic_arg(PlainEvent)
    assert result is None


def test_type_adapter_validation():
    """Test that TypeAdapter can validate extracted types properly."""

    # Test dict[str, int] validation
    class DictIntEvent(BaseEvent[dict[str, int]]):
        pass

    extracted_type = extract_basemodel_generic_arg(DictIntEvent)
    adapter = TypeAdapter(extracted_type)

    # Valid data should work
    valid_data = {'abc': 123, 'def': 456}
    result = adapter.validate_python(valid_data)
    assert result == valid_data

    # Invalid data should raise ValidationError
    invalid_data = {'abc': 'badvalue'}
    with pytest.raises(ValidationError) as exc_info:
        adapter.validate_python(invalid_data)

    # Check that the error is about the wrong type
    errors = exc_info.value.errors()
    assert len(errors) > 0
    assert any('int' in str(error) for error in errors)


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])


# Consolidated from tests/test_simple_typed_results.py (rewritten for strict assertions)


async def test_simple_typed_result_model_roundtrip_and_status() -> None:
    bus = EventBus(name='typed_result_simple_bus')

    class SimpleResult(BaseModel):
        value: str
        count: int

    class SimpleTypedEvent(BaseEvent[SimpleResult]):
        event_result_type: Any = SimpleResult

    def handler(_event: SimpleTypedEvent) -> SimpleResult:
        return SimpleResult(value='hello', count=42)

    handler_entry = bus.on(SimpleTypedEvent, handler)

    try:
        completed_event = await bus.emit(SimpleTypedEvent())
        assert completed_event.event_status == 'completed'
        assert handler_entry.id in completed_event.event_results

        event_result = completed_event.event_results[handler_entry.id]
        assert event_result.status == 'completed'
        assert event_result.error is None
        assert isinstance(event_result.result, SimpleResult)
        assert event_result.result == SimpleResult(value='hello', count=42)
    finally:
        await bus.stop(clear=True)
