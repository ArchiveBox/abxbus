"""Static typing contracts for the event execution pipeline.

This module is never imported by runtime code. It exists so strict type checks
(`pyright`, `ty`) fail if the end-to-end event handler pipeline is weakened.
"""

from typing import Any, assert_type

from pydantic import BaseModel

from abxbus.base_event import BaseEvent, EventResult
from abxbus.event_bus import EventBus
from abxbus.event_handler import EventHandler


class TypeContractResult(BaseModel):
    message: str


class TypeContractEvent(BaseEvent[TypeContractResult]):
    pass


async def _contract_handler(event: TypeContractEvent) -> TypeContractResult:
    return TypeContractResult(message=event.event_type)


async def _assert_pipeline_types(bus: EventBus, event: TypeContractEvent) -> None:
    handler_entry = bus.on(TypeContractEvent, _contract_handler)
    assert_type(handler_entry, EventHandler)

    dispatched_event = bus.emit(event)
    assert_type(dispatched_event, TypeContractEvent)

    typed_pending_result = dispatched_event.event_result_update(handler_entry, eventbus=bus, status='pending')
    assert_type(typed_pending_result, EventResult[TypeContractResult])
    result_run_value = await typed_pending_result.run_handler(dispatched_event, eventbus=bus, timeout=event.event_timeout)
    assert_type(result_run_value, TypeContractResult | BaseEvent[Any] | None)
    assert_type(typed_pending_result.result, TypeContractResult | BaseEvent[Any] | None)

    emitted_event = bus.emit(TypeContractEvent())
    assert_type(emitted_event, TypeContractEvent)
    completed_event = await emitted_event.wait()
    assert_type(completed_event, TypeContractEvent)

    first_result = await (await completed_event.now(first_result=True)).event_result()
    assert_type(first_result, TypeContractResult | None)

    aggregated_result = await completed_event.event_result()
    assert_type(aggregated_result, TypeContractResult | None)

    all_values = await completed_event.event_results_list()
    assert_type(all_values, list[TypeContractResult | None])
    for handler_result in completed_event.event_results.values():
        assert_type(handler_result, EventResult[Any])


def test_typing_contracts_module_loads() -> None:
    """Runtime no-op so this file is a valid pytest module."""
    assert callable(_assert_pipeline_types)


# Consolidated from tests/test_handler_registration_typing.py

"""Static typing contracts for EventBus.on overload behavior.

This file is for static type checking only (pyright/ty), not runtime pytest execution.
"""

# pyright: strict, reportUnnecessaryTypeIgnoreComment=true

from typing import TYPE_CHECKING

from abxbus.base_event import BaseEvent
from abxbus.event_bus import EventBus


class _SomeEventClass(BaseEvent[str]):
    pass


class _OtherEventClass(BaseEvent[str]):
    pass


class _EventTypeA(BaseEvent[int]):
    field_a: int = 1234


class _EventTypeB(BaseEvent[int]):
    field_b: int = 5678


class _EventTypeSubclassOfA(_EventTypeA):
    field_sub: float = 123.123


def _some_handler(event: _SomeEventClass) -> str:
    return 'ok'


def _base_handler(event: BaseEvent[Any]) -> str:
    return 'ok'


def _other_handler(event: _OtherEventClass) -> str:
    return 'ok'


def _handler_for_a(event: _EventTypeA) -> int:
    return event.field_a


def _handler_for_specific_subclass(event: _EventTypeSubclassOfA) -> int:
    return int(event.field_sub)


if TYPE_CHECKING:
    _bus = EventBus()

    # Class pattern should preserve strict subclass typing.
    _class_entry = _bus.on(_SomeEventClass, _some_handler)
    assert_type(_class_entry, EventHandler)

    # String pattern is intentionally looser: BaseEvent handlers and subclass handlers are both accepted.
    _string_base_entry = _bus.on('SomeEventClass', _base_handler)
    assert_type(_string_base_entry, EventHandler)
    _string_subclass_entry = _bus.on('SomeEventClass', _some_handler)
    assert_type(_string_subclass_entry, EventHandler)

    # Expected static type errors:
    # 1) class pattern should reject a mismatched event subclass handler
    _bus.on(_SomeEventClass, _other_handler)  # pyright: ignore[reportCallIssue, reportArgumentType]  # ty: ignore[no-matching-overload]

    # Variance contracts for class patterns:
    # 2) unrelated class pattern should reject handler expecting a different event class
    _bus.on(_EventTypeB, _handler_for_a)  # type: ignore
    # 3) subclass pattern accepts base-class handler (contravariant safe)
    _subclass_ok = _bus.on(_EventTypeSubclassOfA, _handler_for_a)
    assert_type(_subclass_ok, EventHandler)
    # 4) base-class pattern rejects subclass-only handler
    _bus.on(_EventTypeA, _handler_for_specific_subclass)  # type: ignore
