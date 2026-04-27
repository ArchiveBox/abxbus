import inspect
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any, assert_type

import pytest

from abxbus.base_event import BaseEvent
from abxbus.event_bus import EventBus
from abxbus.event_handler import EventHandler, _normalize_handler_callable  # pyright: ignore[reportPrivateUsage]
from abxbus.retry import retry

if TYPE_CHECKING:

    class _RegistryTypingEvent(BaseEvent[str]):
        pass

    def _typed_sync_handler(event: _RegistryTypingEvent) -> str:
        return event.event_type

    async def _typed_async_handler(event: _RegistryTypingEvent) -> str: ...

    _wrapped_sync = _normalize_handler_callable(_typed_sync_handler)
    _wrapped_async = _normalize_handler_callable(_typed_async_handler)
    assert_type(_wrapped_sync, Callable[[_RegistryTypingEvent], Coroutine[Any, Any, str]])
    assert_type(_wrapped_async, Callable[[_RegistryTypingEvent], Coroutine[Any, Any, str]])


@pytest.mark.asyncio
async def test_on_stores_eventhandler_entry_and_index() -> None:
    bus = EventBus(name='RegistryBus')

    def handler(event: BaseEvent[Any]) -> str:
        return event.event_type

    entry = bus.on('RegistryEvent', handler)

    assert isinstance(entry, EventHandler)
    assert entry.id is not None
    assert entry.id in bus.handlers
    assert bus.handlers[entry.id] is entry
    assert 'RegistryEvent' in bus.handlers_by_key
    assert entry.id in bus.handlers_by_key['RegistryEvent']

    dispatched = bus.emit(BaseEvent(event_type='RegistryEvent'))
    completed = await dispatched
    assert entry.id in completed.event_results
    assert completed.event_results[entry.id].handler.id == entry.id

    await bus.stop(clear=True)


@pytest.mark.asyncio
async def test_off_removes_by_callable_id_entry_or_all() -> None:
    bus = EventBus(name='RegistryOffBus')

    def handler_a(event: BaseEvent[Any]) -> None:
        return None

    def handler_b(event: BaseEvent[Any]) -> None:
        return None

    def handler_c(event: BaseEvent[Any]) -> None:
        return None

    entry_a = bus.on('RegistryEvent', handler_a)
    entry_b = bus.on('RegistryEvent', handler_b)
    entry_c = bus.on('RegistryEvent', handler_c)
    assert entry_a.id and entry_b.id and entry_c.id

    bus.off('RegistryEvent', handler_a)
    assert entry_a.id not in bus.handlers
    assert entry_a.id not in bus.handlers_by_key['RegistryEvent']
    assert entry_b.id in bus.handlers

    bus.off('RegistryEvent', entry_b.id)
    assert entry_b.id not in bus.handlers
    assert entry_b.id not in bus.handlers_by_key['RegistryEvent']
    assert entry_c.id in bus.handlers

    bus.off('RegistryEvent', entry_c)
    assert entry_c.id not in bus.handlers
    assert 'RegistryEvent' not in bus.handlers_by_key

    bus.on('RegistryEvent', handler_a)
    bus.on('RegistryEvent', handler_b)
    bus.off('RegistryEvent')
    assert 'RegistryEvent' not in bus.handlers_by_key
    assert all(entry.event_pattern != 'RegistryEvent' for entry in bus.handlers.values())

    await bus.stop(clear=True)


@pytest.mark.asyncio
async def test_on_normalizes_sync_handler_to_async_callable() -> None:
    bus = EventBus(name='RegistryNormalizeBus')

    class RegistryNormalizeEvent(BaseEvent[str]):
        event_timeout: float | None = 0.2

    calls: list[str] = []

    def sync_handler(event: RegistryNormalizeEvent) -> str:
        calls.append(event.event_id)
        return 'normalized'

    entry = bus.on(RegistryNormalizeEvent, sync_handler)

    assert entry.handler is sync_handler
    assert entry._handler_async is not None  # pyright: ignore[reportPrivateUsage]
    assert inspect.iscoroutinefunction(entry._handler_async)  # pyright: ignore[reportPrivateUsage]
    assert entry.handler_name.endswith('sync_handler')

    direct_result = await entry._handler_async(RegistryNormalizeEvent())  # pyright: ignore[reportPrivateUsage]
    assert direct_result == 'normalized'

    dispatched = bus.emit(RegistryNormalizeEvent())
    completed = await dispatched
    result = completed.event_results[entry.id]

    assert result.status == 'completed'
    assert result.result == 'normalized'
    assert len(calls) == 2

    await bus.stop(clear=True)


@pytest.mark.asyncio
async def test_on_keeps_async_handlers_normalized_through_handler_async() -> None:
    bus = EventBus(name='RegistryAsyncNormalizeBus')

    class RegistryAsyncNormalizeEvent(BaseEvent[str]):
        event_timeout: float | None = 0.2

    calls: list[str] = []

    async def async_handler(event: RegistryAsyncNormalizeEvent) -> str:
        calls.append(event.event_id)
        return 'async_normalized'

    entry = bus.on(RegistryAsyncNormalizeEvent, async_handler)

    assert entry.handler is async_handler
    assert entry._handler_async is async_handler  # pyright: ignore[reportPrivateUsage]
    assert inspect.iscoroutinefunction(entry._handler_async)  # pyright: ignore[reportPrivateUsage]

    direct_result = await entry._handler_async(RegistryAsyncNormalizeEvent())  # pyright: ignore[reportPrivateUsage]
    assert direct_result == 'async_normalized'

    dispatched = bus.emit(RegistryAsyncNormalizeEvent())
    completed = await dispatched
    result = completed.event_results[entry.id]

    assert result.status == 'completed'
    assert result.result == 'async_normalized'
    assert len(calls) == 2

    await bus.stop(clear=True)


@pytest.mark.asyncio
async def test_handler_async_preserves_typed_arg_return_contracts_for_sync_handlers() -> None:
    bus = EventBus(name='RegistryTypingSyncBus')

    class RegistryTypingEvent(BaseEvent[str]):
        required_token: str

    def typed_sync_handler(event: RegistryTypingEvent) -> str:
        return event.required_token

    entry = bus.on(RegistryTypingEvent, typed_sync_handler)
    handler_async = entry._handler_async  # pyright: ignore[reportPrivateUsage]
    assert handler_async is not None
    result = await handler_async(RegistryTypingEvent(required_token='sync'))
    assert isinstance(result, str)
    assert result == 'sync'

    await bus.stop(clear=True)


@pytest.mark.asyncio
async def test_handler_async_preserves_typed_arg_return_contracts_for_async_handlers() -> None:
    bus = EventBus(name='RegistryTypingAsyncBus')

    class RegistryTypingEvent(BaseEvent[str]):
        required_token: str

    async def typed_async_handler(event: RegistryTypingEvent) -> str:
        return event.required_token

    entry = bus.on(RegistryTypingEvent, typed_async_handler)
    handler_async = entry._handler_async  # pyright: ignore[reportPrivateUsage]
    assert handler_async is not None
    result = await handler_async(RegistryTypingEvent(required_token='async'))
    assert isinstance(result, str)
    assert result == 'async'

    await bus.stop(clear=True)


@pytest.mark.asyncio
async def test_on_names_bound_handlers_by_the_class_that_defined_the_method() -> None:
    bus = EventBus(name='RegistryDefinedClassNameBus')

    class DefinedClassNameEvent(BaseEvent[str]):
        pass

    class OriginalHandlerClass:
        entry: EventHandler | None = None

        def __init__(self, b: EventBus) -> None:
            self.entry = b.on(DefinedClassNameEvent, self.on_DefinedClassNameEvent)

        async def on_DefinedClassNameEvent(self, _event: DefinedClassNameEvent) -> str:
            return 'ok'

    class AttachedHandlerClass(OriginalHandlerClass):
        pass

    service = AttachedHandlerClass(bus)
    assert service.entry is not None
    assert service.entry.handler_name == 'OriginalHandlerClass.on_DefinedClassNameEvent'

    completed = await bus.emit(DefinedClassNameEvent())
    await completed
    output = bus.log_tree()

    assert f'{bus.name}#' in output
    assert f'{service.entry.handler_name}#' in output

    await bus.stop(clear=True)


@pytest.mark.asyncio
async def test_on_names_retry_decorated_bound_handlers_by_the_class_that_defined_the_method() -> None:
    bus = EventBus(name='RegistryDecoratedDefinedClassNameBus')

    class DecoratedDefinedClassNameEvent(BaseEvent[str]):
        pass

    class OriginalDecoratedHandlerClass:
        entry: EventHandler | None = None

        def __init__(self, b: EventBus) -> None:
            self.entry = b.on(DecoratedDefinedClassNameEvent, self.on_DecoratedDefinedClassNameEvent)

        @retry(max_attempts=1)
        async def on_DecoratedDefinedClassNameEvent(self, _event: DecoratedDefinedClassNameEvent) -> str:
            return 'ok'

    class AttachedDecoratedHandlerClass(OriginalDecoratedHandlerClass):
        pass

    service = AttachedDecoratedHandlerClass(bus)
    assert service.entry is not None
    assert service.entry.handler_name == 'OriginalDecoratedHandlerClass.on_DecoratedDefinedClassNameEvent'

    completed = await bus.emit(DecoratedDefinedClassNameEvent())
    await completed
    output = bus.log_tree()

    assert f'{bus.name}#' in output
    assert f'{service.entry.handler_name}#' in output

    await bus.stop(clear=True)
