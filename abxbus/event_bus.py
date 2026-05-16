"""Public EventBus API backed by the Rust core."""

from __future__ import annotations

import asyncio
import warnings
import weakref
from collections.abc import Iterator, Sequence
from contextvars import ContextVar
from typing import Any, Literal, TypeVar, cast
from uuid import uuid4

from abxbus.base_event import (
    BaseEvent,
    EventConcurrencyMode,
    EventHandlerCompletionMode,
    EventHandlerConcurrencyMode,
    PythonIdentifierStr,
    UUIDStr,
)
from abxbus.core_bus import EventConcurrency, HandlerCompletion, HandlerConcurrency, RustCoreEventBus
from abxbus.event_history import EventHistory
from abxbus.lock_manager import ReentrantLock
from abxbus.middlewares import EventBusMiddleware

EventPatternType = PythonIdentifierStr | Literal['*'] | type[BaseEvent[Any]]
EventBusMiddlewareInput = EventBusMiddleware | type[EventBusMiddleware]
T_Event = TypeVar('T_Event', bound=BaseEvent[Any])


class GlobalEventBusRegistry:
    """Weak global registry of EventBus instances."""

    def __init__(self) -> None:
        self._buses: weakref.WeakSet[EventBus] = weakref.WeakSet()

    def add(self, bus: EventBus) -> None:
        self._buses.add(bus)

    def discard(self, bus: EventBus) -> None:
        self._buses.discard(bus)

    def has(self, bus: EventBus) -> bool:
        return bus in self._buses

    @property
    def size(self) -> int:
        return len(self._buses)

    def __iter__(self) -> Iterator[EventBus]:
        return iter(self._buses)

    def __len__(self) -> int:
        return len(self._buses)

    def __contains__(self, bus: object) -> bool:
        return bus in self._buses


def get_current_event() -> BaseEvent[Any] | None:
    """Return the currently active event in this async context."""

    return EventBus.current_event_context.get()


def get_current_handler_id() -> str | None:
    """Return the currently active handler id."""

    return EventBus.current_handler_id_context.get()


def get_current_eventbus() -> EventBus | None:
    """Return the currently active EventBus."""

    return EventBus.current_eventbus_context.get()


def in_handler_context() -> bool:
    """Return True when called inside a handler context."""

    return get_current_handler_id() is not None


class EventBus(RustCoreEventBus):
    """Async-compatible event bus facade; Rust core owns runtime state."""

    all_instances: GlobalEventBusRegistry = GlobalEventBusRegistry()
    current_event_context: ContextVar[BaseEvent[Any] | None] = ContextVar('current_event', default=None)
    current_handler_id_context: ContextVar[str | None] = ContextVar('current_handler_id', default=None)
    current_eventbus_context: ContextVar[EventBus | None] = ContextVar('current_eventbus', default=None)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.all_instances = GlobalEventBusRegistry()
        cls.current_event_context = ContextVar('current_event', default=None)
        cls.current_handler_id_context = ContextVar('current_handler_id', default=None)
        cls.current_eventbus_context = ContextVar('current_eventbus', default=None)
        cls._event_global_serial_lock = ReentrantLock()

    @classmethod
    def iter_all_instances(cls) -> Iterator[EventBus]:
        pending_classes: list[type[EventBus]] = [cls]
        seen_classes: set[type[EventBus]] = set()
        seen_buses: set[int] = set()
        while pending_classes:
            bus_class = pending_classes.pop()
            if bus_class in seen_classes:
                continue
            seen_classes.add(bus_class)
            pending_classes.extend(bus_class.__subclasses__())
            for bus in list(bus_class.all_instances):
                bus_object_id = id(bus)
                if bus_object_id in seen_buses:
                    continue
                seen_buses.add(bus_object_id)
                yield bus

    def __init__(
        self,
        name: PythonIdentifierStr | None = None,
        event_concurrency: EventConcurrencyMode | str | None = None,
        event_handler_concurrency: EventHandlerConcurrencyMode | str = EventHandlerConcurrencyMode.SERIAL,
        event_handler_completion: EventHandlerCompletionMode | str = EventHandlerCompletionMode.ALL,
        max_history_size: int | None = 100,
        max_history_drop: bool = False,
        event_timeout: float | None = 60.0,
        event_slow_timeout: float | None = 300.0,
        event_handler_timeout: float | None = None,
        event_handler_slow_timeout: float | None = 30.0,
        event_handler_detect_file_paths: bool = True,
        local_immediate_handlers: bool = False,
        local_immediate_skip_core_handlers: bool = False,
        local_immediate_child_events: bool = False,
        middlewares: Sequence[EventBusMiddlewareInput] | None = None,
        warn_on_duplicate_handler_names: bool = True,
        id: UUIDStr | None = None,
        max_handler_recursion_depth: int = 2,
        background_worker: bool = False,
    ) -> None:
        bus_name = name or f'{self.__class__.__name__}_{(str(id) if id is not None else uuid4().hex)[-8:]}'
        assert bus_name.isidentifier(), f'EventBus name must be a unique identifier string, got: {bus_name}'
        original_name = bus_name
        if any(existing_bus is not self and existing_bus.name == bus_name for existing_bus in list(type(self).all_instances)):
            bus_name = f'{original_name}_{uuid4().hex[-8:]}'
            warnings.warn(
                f'⚠️ EventBus with name "{original_name}" already exists. '
                f'Auto-generated unique name: "{bus_name}" to avoid conflicts. '
                f'Consider using unique names or stop(clear=True) on unused buses.',
                UserWarning,
                stacklevel=2,
            )
        super().__init__(
            name=bus_name,
            id=id,
            event_concurrency=cast(EventConcurrency, event_concurrency or EventConcurrencyMode.BUS_SERIAL),
            event_handler_concurrency=cast(HandlerConcurrency, event_handler_concurrency),
            event_handler_completion=cast(HandlerCompletion, event_handler_completion),
            max_history_size=max_history_size,
            max_history_drop=max_history_drop,
            event_timeout=event_timeout,
            event_slow_timeout=event_slow_timeout,
            event_handler_timeout=event_handler_timeout,
            event_handler_slow_timeout=event_handler_slow_timeout,
            event_handler_detect_file_paths=event_handler_detect_file_paths,
            local_immediate_handlers=local_immediate_handlers,
            local_immediate_skip_core_handlers=local_immediate_skip_core_handlers,
            local_immediate_child_events=local_immediate_child_events,
            middlewares=list(middlewares or []),
            background_worker=background_worker,
        )
        self.warn_on_duplicate_handler_names = warn_on_duplicate_handler_names
        self.max_handler_recursion_depth = max_handler_recursion_depth
        type(self).all_instances.add(self)
        self.all_instances = type(self).all_instances

    def disconnect(self) -> None:
        type(self).all_instances.discard(self)
        super().disconnect()

    def emit(self, event: T_Event) -> T_Event:
        try:
            asyncio.get_running_loop()
        except RuntimeError as exc:
            raise RuntimeError('no event loop is running') from exc
        return super().emit(event)


__all__ = [
    'EventBus',
    'EventHistory',
    'EventPatternType',
    'EventBusMiddlewareInput',
    'GlobalEventBusRegistry',
    'get_current_event',
    'get_current_handler_id',
    'get_current_eventbus',
    'in_handler_context',
]
