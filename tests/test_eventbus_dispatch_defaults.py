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


class ConfiguredEvent(BaseEvent[str]):
    event_version: str = '2.0.0'
    event_timeout: float | None = 12.0
    event_slow_timeout: float | None = 30.0
    event_handler_timeout: float | None = 3.0
    event_handler_slow_timeout: float | None = 4.0
    event_blocks_parent_completion: bool = True


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
        await bus.destroy()


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
        await bus.destroy()


async def test_event_instance_override_beats_event_class_defaults() -> None:
    bus = EventBus(name='EventInstanceOverrideBus')
    try:
        class_default = bus.emit(ConcurrencyOverrideEvent())
        assert class_default.event_concurrency == 'global-serial'

        instance_override = bus.emit(ConcurrencyOverrideEvent(event_concurrency=EventConcurrencyMode.PARALLEL))
        assert instance_override.event_concurrency == 'parallel'
    finally:
        await bus.destroy()


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
        await bus.destroy()


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
        await bus.destroy()


async def test_handler_instance_override_beats_event_class_defaults() -> None:
    bus = EventBus(name='HandlerInstanceOverrideBus')
    try:
        class_default = bus.emit(HandlerOverrideEvent())
        assert class_default.event_handler_concurrency == 'serial'
        assert class_default.event_handler_completion == 'all'

        instance_override = bus.emit(
            HandlerOverrideEvent(
                event_handler_concurrency=EventHandlerConcurrencyMode.PARALLEL,
                event_handler_completion=EventHandlerCompletionMode.FIRST,
            )
        )
        assert instance_override.event_handler_concurrency == 'parallel'
        assert instance_override.event_handler_completion == 'first'
    finally:
        await bus.destroy()


async def test_typed_event_config_defaults_populate_base_event_fields() -> None:
    bus = EventBus(name='ConfiguredEventDefaultsBus')
    try:
        event = bus.emit(ConfiguredEvent())
        assert event.event_version == '2.0.0'
        assert event.event_timeout == 12.0
        assert event.event_slow_timeout == 30.0
        assert event.event_handler_timeout == 3.0
        assert event.event_handler_slow_timeout == 4.0
        assert event.event_blocks_parent_completion is True
    finally:
        await bus.destroy()
