from typing import Any
from uuid import uuid4

from abxbus import BaseEvent, EventStatus, RustCoreEventBus


class PyCoreTypedEvent(BaseEvent[int]):
    value: int


class PyCoreErrorEvent(BaseEvent[object]):
    value: int


def unique_bus_name(prefix: str) -> str:
    return f'{prefix}{uuid4().hex}'


def test_rust_core_event_bus_runs_python_handler_via_core_snapshot() -> None:
    bus = RustCoreEventBus(unique_bus_name('PyCoreBus'))
    try:

        def handler(event: BaseEvent[Any]) -> int:
            assert isinstance(event, PyCoreTypedEvent)
            return event.value + 1

        bus.on(PyCoreTypedEvent, handler)
        completed = bus.emit(PyCoreTypedEvent(value=41))
    finally:
        bus.stop()

    assert isinstance(completed, PyCoreTypedEvent)
    assert completed.event_status == EventStatus.COMPLETED
    assert completed.event_id
    assert len(completed.event_results) == 1
    result = next(iter(completed.event_results.values()))
    assert result.status == 'completed'
    assert result.result == 42


def test_rust_core_event_bus_commits_python_handler_error_through_core() -> None:
    bus = RustCoreEventBus(unique_bus_name('PyCoreErrorBus'))
    try:

        def fail(_event: BaseEvent[object]) -> object:
            raise RuntimeError('py boom')

        bus.on(PyCoreErrorEvent, fail)
        completed = bus.emit(PyCoreErrorEvent(value=41))
    finally:
        bus.stop()

    assert completed.event_status == EventStatus.COMPLETED
    result = next(iter(completed.event_results.values()))
    assert result.status == 'error'
    assert result.error is not None
    assert 'py boom' in str(result.error)


async def test_rust_core_event_bus_find_and_filter_read_from_core_snapshots() -> None:
    bus = RustCoreEventBus(unique_bus_name('PyCoreQueryBus'))
    try:

        def handler(event: BaseEvent[Any]) -> int:
            assert isinstance(event, PyCoreTypedEvent)
            return event.value + 10

        bus.on(PyCoreTypedEvent, handler)
        bus.emit(PyCoreTypedEvent(value=1))
        latest = bus.emit(PyCoreTypedEvent(value=2))

        found = await bus.find(PyCoreTypedEvent, value=2)
        matches = await bus.filter(PyCoreTypedEvent, limit=1)
    finally:
        bus.stop()

    assert isinstance(found, PyCoreTypedEvent)
    assert found.event_id == latest.event_id
    assert next(iter(found.event_results.values())).result == 12
    assert len(matches) == 1
    assert matches[0].event_id == latest.event_id


async def test_rust_core_event_bus_emitted_event_keeps_builtin_await_api() -> None:
    bus = RustCoreEventBus(unique_bus_name('PyCoreAwaitBus'))
    try:
        bus.on(PyCoreTypedEvent, lambda event: None)
        completed = await bus.emit(PyCoreTypedEvent(value=3))
    finally:
        bus.stop()

    assert completed.event_status == EventStatus.COMPLETED


def test_rust_core_event_bus_same_name_shares_bus_without_explicit_id() -> None:
    bus_name = unique_bus_name('PyCoreSharedRuntimeBus')
    emitter = RustCoreEventBus(bus_name, event_handler_concurrency='parallel')
    worker = RustCoreEventBus(bus_name, event_handler_concurrency='parallel')
    try:
        assert emitter.bus_id == worker.bus_id

        def local(event: BaseEvent[Any]) -> int:
            assert isinstance(event, PyCoreTypedEvent)
            return event.value + 1

        def remote(event: BaseEvent[Any]) -> int:
            assert isinstance(event, PyCoreTypedEvent)
            return event.value + 10

        emitter.on(PyCoreTypedEvent, local, handler_name='local')
        worker.on(PyCoreTypedEvent, remote, handler_name='remote')

        completed = emitter.emit(PyCoreTypedEvent(value=5))
    finally:
        worker.disconnect()
        emitter.stop()

    results = sorted(int(result.result) for result in completed.event_results.values() if isinstance(result.result, int))
    assert results == [6, 15]
