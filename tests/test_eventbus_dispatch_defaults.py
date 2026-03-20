from abxbus import (
    BaseEvent,
    EventBus,
    EventConcurrencyMode,
    EventHandlerCompletionMode,
    EventHandlerConcurrencyMode,
)


class PropagationEvent(BaseEvent[str]):
    pass


class ConcurrencyOverrideEvent(BaseEvent[str]):
    event_concurrency: EventConcurrencyMode | None = EventConcurrencyMode.GLOBAL_SERIAL


class HandlerOverrideEvent(BaseEvent[str]):
    event_handler_concurrency: EventHandlerConcurrencyMode | None = EventHandlerConcurrencyMode.SERIAL
    event_handler_completion: EventHandlerCompletionMode | None = EventHandlerCompletionMode.ALL


async def test_event_concurrency_remains_unset_on_dispatch_and_resolves_during_processing() -> None:
    bus = EventBus(name='EventConcurrencyDefaultBus', event_concurrency='parallel')

    async def handler(_event: BaseEvent[str]) -> str:
        return 'ok'

    bus.on(PropagationEvent, handler)
    try:
        implicit = bus.emit(PropagationEvent())
        explicit_none = bus.emit(PropagationEvent(event_concurrency=None))

        assert implicit.event_concurrency is None
        assert explicit_none.event_concurrency is None

        await implicit
        await explicit_none
    finally:
        await bus.stop()


async def test_event_concurrency_class_override_beats_bus_default() -> None:
    bus = EventBus(name='EventConcurrencyOverrideBus', event_concurrency='parallel')

    async def handler(_event: BaseEvent[str]) -> str:
        return 'ok'

    bus.on(ConcurrencyOverrideEvent, handler)
    try:
        event = bus.emit(ConcurrencyOverrideEvent())
        assert event.event_concurrency == 'global-serial'
        await event
    finally:
        await bus.stop()


async def test_handler_defaults_remain_unset_on_dispatch_and_resolve_during_processing() -> None:
    bus = EventBus(
        name='HandlerDefaultsBus',
        event_handler_concurrency='parallel',
        event_handler_completion='first',
    )

    async def handler(_event: BaseEvent[str]) -> str:
        return 'ok'

    bus.on(PropagationEvent, handler)
    try:
        implicit = bus.emit(PropagationEvent())
        explicit_none = bus.emit(
            PropagationEvent(
                event_handler_concurrency=None,
                event_handler_completion=None,
            )
        )

        assert implicit.event_handler_concurrency is None
        assert implicit.event_handler_completion is None
        assert explicit_none.event_handler_concurrency is None
        assert explicit_none.event_handler_completion is None

        await implicit
        await explicit_none
    finally:
        await bus.stop()


async def test_handler_class_override_beats_bus_default() -> None:
    bus = EventBus(
        name='HandlerDefaultsOverrideBus',
        event_handler_concurrency='parallel',
        event_handler_completion='first',
    )

    async def handler(_event: BaseEvent[str]) -> str:
        return 'ok'

    bus.on(HandlerOverrideEvent, handler)
    try:
        event = bus.emit(HandlerOverrideEvent())
        assert event.event_handler_concurrency == 'serial'
        assert event.event_handler_completion == 'all'
        await event
    finally:
        await bus.stop()
