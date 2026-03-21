import asyncio
import contextvars
import inspect
import json
import logging
import warnings
import weakref
from collections import defaultdict
from collections.abc import Callable, Coroutine, Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Any, Literal, TypeVar, overload
from uuid import UUID

# Use Rust-accelerated UUID v7 when available (~3.6x faster)
try:
    from _abxbus_rust import uuid7str
except ImportError:
    from uuid_extensions import uuid7str

uuid7str: Callable[[], str] = uuid7str

from abxbus.base_event import (
    ABXBUS_LOGGING_LEVEL,
    BaseEvent,
    EventConcurrencyMode,
    EventHandlerCompletionMode,
    EventHandlerConcurrencyMode,
    EventResult,
    EventStatus,
    PythonIdentifierStr,
    PythonIdStr,
    T_Event,
    T_EventResultType,
    UUIDStr,
)
from abxbus.event_handler import (
    ContravariantEventHandlerCallable,
    EventHandler,
    EventHandlerAbortedError,
    EventHandlerCallable,
    EventHandlerCancelledError,
)
from abxbus.event_history import EventHistory
from abxbus.helpers import (
    CleanShutdownQueue,
    QueueShutDown,
    _run_with_slow_monitor,  # pyright: ignore[reportPrivateUsage]
    log_filtered_traceback,
)
from abxbus.lock_manager import LockManager, LockManagerProtocol, ReentrantLock
from abxbus.middlewares import EventBusMiddleware

logger = logging.getLogger('abxbus')
logger.setLevel(ABXBUS_LOGGING_LEVEL)

T_ExpectedEvent = TypeVar('T_ExpectedEvent', bound=BaseEvent[Any])
T_OnEvent = TypeVar('T_OnEvent', bound=BaseEvent[Any])

EventPatternType = PythonIdentifierStr | Literal['*'] | type[BaseEvent[Any]]
# Middleware entries can be provided as already-initialized instances or middleware classes.
EventBusMiddlewareInput = EventBusMiddleware | type[EventBusMiddleware]


def _as_any(value: Any) -> Any:
    return value


@dataclass(slots=True, eq=False)
class _FindWaiter:
    event_key: str
    matches: Callable[[BaseEvent[Any]], bool]
    future: asyncio.Future[BaseEvent[Any] | None]
    timeout_handle: asyncio.TimerHandle | None = None


class GlobalEventBusRegistry:
    """Weak global registry of EventBus instances."""

    def __init__(self) -> None:
        self._buses: weakref.WeakSet['EventBus'] = weakref.WeakSet()

    def add(self, bus: 'EventBus') -> None:
        self._buses.add(bus)

    def discard(self, bus: 'EventBus') -> None:
        self._buses.discard(bus)

    def has(self, bus: 'EventBus') -> bool:
        return bus in self._buses

    @property
    def size(self) -> int:
        return len(self._buses)

    def __iter__(self) -> Iterator['EventBus']:
        return iter(self._buses)

    def __len__(self) -> int:
        return len(self._buses)

    def __contains__(self, bus: object) -> bool:
        return bus in self._buses


def get_current_event() -> BaseEvent[Any] | None:
    """Return the currently active event in this async context, if any."""
    return EventBus.current_event_context.get()


def get_current_handler_id() -> str | None:
    """Return the currently active handler id in this async context, if any."""
    return EventBus.current_handler_id_context.get()


def get_current_eventbus() -> 'EventBus | None':
    """Return the currently active EventBus in this async context, if any."""
    return EventBus.current_eventbus_context.get()


def in_handler_context() -> bool:
    """Return True when called from inside an executing handler context."""
    return get_current_handler_id() is not None


class EventBus:
    """
    Async event bus with write-ahead logging and guaranteed FIFO processing.

    Features:
    - Enqueue events synchronously, await their results using 'await Event()'
    - FIFP Write-ahead logging with UUIDs and timestamps,
    - Serial event processing, configurable handler concurrency per event ('serial' | 'parallel')
    """

    # Track all EventBus instances (using weakrefs to allow garbage collection)
    all_instances: GlobalEventBusRegistry = GlobalEventBusRegistry()
    _lock_for_event_global_serial: ReentrantLock = ReentrantLock()

    # Per-loop EventBus registry and original close method tracking.
    _loop_eventbus_instances: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, weakref.WeakSet['EventBus']] = (
        weakref.WeakKeyDictionary()
    )
    _loop_original_close: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, Callable[[], None]] = weakref.WeakKeyDictionary()

    # Context variables for current event/handler/bus execution scope.
    current_event_context: ContextVar[BaseEvent[Any] | None] = ContextVar('current_event', default=None)
    current_handler_id_context: ContextVar[str | None] = ContextVar('current_handler_id', default=None)
    current_eventbus_context: ContextVar['EventBus | None'] = ContextVar('current_eventbus', default=None)

    # Class Attributes
    name: PythonIdentifierStr = 'EventBus'
    event_concurrency: EventConcurrencyMode = EventConcurrencyMode.BUS_SERIAL
    event_timeout: float | None = 60.0
    event_slow_timeout: float | None = 300.0
    event_handler_concurrency: EventHandlerConcurrencyMode = EventHandlerConcurrencyMode.SERIAL
    event_handler_completion: EventHandlerCompletionMode = EventHandlerCompletionMode.ALL
    event_handler_slow_timeout: float | None = 30.0
    event_handler_detect_file_paths: bool = True
    max_handler_recursion_depth: int = 2

    # Runtime State
    id: UUIDStr = '00000000-0000-0000-0000-000000000000'
    handlers: dict[PythonIdStr, EventHandler]
    handlers_by_key: dict[str, list[PythonIdStr]]
    pending_event_queue: CleanShutdownQueue[BaseEvent[Any]] | None
    event_history: EventHistory[BaseEvent[Any]]

    _is_running: bool = False
    _runloop_task: asyncio.Task[None] | None = None
    _parallel_event_tasks: set[asyncio.Task[None]]
    _pending_middleware_tasks: set[asyncio.Task[None]]
    _on_idle: asyncio.Event | None = None
    in_flight_event_ids: set[str]
    processing_event_ids: set[str]
    _duplicate_handler_name_check_limit: int = 256
    _pending_handler_changes: list[tuple[EventHandler, bool]]
    find_waiters: set[_FindWaiter]
    _lock_for_event_bus_serial: ReentrantLock
    locks: LockManagerProtocol

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Subclasses get isolated global registries/locks by default.
        cls.all_instances = GlobalEventBusRegistry()
        cls._lock_for_event_global_serial = ReentrantLock()
        cls._loop_eventbus_instances = weakref.WeakKeyDictionary()
        cls._loop_original_close = weakref.WeakKeyDictionary()

    @classmethod
    def iter_all_instances(cls) -> Iterator['EventBus']:
        """Iterate live EventBus instances for ``cls`` and all subclasses."""
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
                bus_id = id(bus)
                if bus_id in seen_buses:
                    continue
                seen_buses.add(bus_id)
                yield bus

    def __init__(
        self,
        name: PythonIdentifierStr | None = None,
        event_concurrency: EventConcurrencyMode | str | None = None,
        event_handler_concurrency: EventHandlerConcurrencyMode | str = EventHandlerConcurrencyMode.SERIAL,
        event_handler_completion: EventHandlerCompletionMode | str = EventHandlerCompletionMode.ALL,
        max_history_size: int | None = 100,  # Keep only 100 events in history
        max_history_drop: bool = False,
        event_timeout: float | None = 60.0,
        event_slow_timeout: float | None = 300.0,
        event_handler_slow_timeout: float | None = 30.0,
        event_handler_detect_file_paths: bool = True,
        max_handler_recursion_depth: int = 2,
        middlewares: Sequence[EventBusMiddlewareInput] | None = None,
        id: UUIDStr | str | None = None,
    ):
        self.id = str(UUID(str(id))) if id is not None else uuid7str()
        self.name = name or f'{self.__class__.__name__}_{self.id[-8:]}'
        assert self.name.isidentifier(), f'EventBus name must be a unique identifier string, got: {self.name}'

        # Force garbage collection to clean up any dead EventBus instances in the WeakSet
        # gc.collect()  # Commented out - this is expensive and causes 5s delays when creating many EventBus instances

        # Check for name uniqueness among existing instances
        # We'll collect potential conflicts and check if they're still alive
        original_name = self.name
        conflicting_buses: list[EventBus] = []

        for existing_bus in list(type(self).all_instances):  # Make a list copy to avoid modification during iteration
            if existing_bus is not self and existing_bus.name == self.name:
                # Since stop() renames buses to _stopped_{id}, any bus with a matching
                # user-specified name is either running or never-started - both should
                # be considered conflicts. This makes name conflict detection deterministic.
                conflicting_buses.append(existing_bus)

        # If we found conflicting buses, auto-generate a unique suffix
        if conflicting_buses:
            # Generate a unique suffix using the last 8 chars of a UUID
            unique_suffix = uuid7str()[-8:]
            self.name = f'{original_name}_{unique_suffix}'

            warnings.warn(
                f'⚠️ EventBus with name "{original_name}" already exists. '
                f'Auto-generated unique name: "{self.name}" to avoid conflicts. '
                f'Consider using unique names or stop(clear=True) on unused buses.',
                UserWarning,
                stacklevel=2,
            )

        self.pending_event_queue = None
        self.event_history = EventHistory(max_history_size=max_history_size, max_history_drop=max_history_drop)
        self.handlers = {}
        self.handlers_by_key = defaultdict(list)
        self._lock_for_event_bus_serial = ReentrantLock()
        self.locks = LockManager()
        self._parallel_event_tasks = set()
        self._pending_middleware_tasks = set()
        try:
            self.event_concurrency = EventConcurrencyMode(event_concurrency or EventConcurrencyMode.BUS_SERIAL)
        except ValueError as exc:
            raise AssertionError(
                f'event_concurrency must be "global-serial", "bus-serial", or "parallel", got: {event_concurrency!r}'
            ) from exc
        try:
            self.event_handler_concurrency = EventHandlerConcurrencyMode(
                event_handler_concurrency or EventHandlerConcurrencyMode.SERIAL
            )
        except ValueError as exc:
            raise AssertionError(
                f'event_handler_concurrency must be "serial" or "parallel", got: {event_handler_concurrency!r}'
            ) from exc
        try:
            self.event_handler_completion = EventHandlerCompletionMode(event_handler_completion or EventHandlerCompletionMode.ALL)
        except ValueError as exc:
            raise AssertionError(f'event_handler_completion must be "all" or "first", got: {event_handler_completion!r}') from exc
        self.event_timeout = event_timeout
        self.event_slow_timeout = event_slow_timeout
        self.event_handler_slow_timeout = event_handler_slow_timeout
        self.event_handler_detect_file_paths = bool(event_handler_detect_file_paths)
        self.max_handler_recursion_depth = int(max_handler_recursion_depth)
        assert self.event_timeout is None or self.event_timeout > 0, (
            f'event_timeout must be > 0 or None, got: {self.event_timeout!r}'
        )
        assert self.event_slow_timeout is None or self.event_slow_timeout > 0, (
            f'event_slow_timeout must be > 0 or None, got: {self.event_slow_timeout!r}'
        )
        assert self.event_handler_slow_timeout is None or self.event_handler_slow_timeout > 0, (
            f'event_handler_slow_timeout must be > 0 or None, got: {self.event_handler_slow_timeout!r}'
        )
        assert self.max_handler_recursion_depth >= 0, (
            f'max_handler_recursion_depth must be >= 0, got: {self.max_handler_recursion_depth!r}'
        )
        self._on_idle = None
        self.middlewares = self._normalize_middlewares(middlewares)
        self.in_flight_event_ids = set()
        self.processing_event_ids = set()
        self._pending_handler_changes = []
        self.find_waiters = set()

        # Initialize Rust scheduling core (owns all scheduling decisions)
        try:
            from _abxbus_rust import RustEventBusCore
            self._rust_core: 'RustEventBusCore | None' = RustEventBusCore(
                bus_id=self.id,
                bus_name=self.name,
                event_concurrency=str(self.event_concurrency),
                event_handler_concurrency=str(self.event_handler_concurrency),
                event_handler_completion=str(self.event_handler_completion),
                event_timeout=self.event_timeout,
                event_slow_timeout=self.event_slow_timeout,
                event_handler_slow_timeout=self.event_handler_slow_timeout,
                max_handler_recursion_depth=self.max_handler_recursion_depth,
                max_history_size=max_history_size,
                max_history_drop=max_history_drop,
            )
        except ImportError:
            self._rust_core = None

        # Register this instance
        type(self).all_instances.add(self)

    @staticmethod
    def _normalize_middlewares(middlewares: Sequence[EventBusMiddlewareInput] | None) -> list[EventBusMiddleware]:
        """Normalize middleware inputs to concrete middleware instances.

        Accepts mixed instance/class entries and instantiates class entries once
        during bus construction, preserving registration order.
        """
        normalized: list[EventBusMiddleware] = []
        for middleware in middlewares or ():
            if isinstance(middleware, EventBusMiddleware):
                normalized.append(middleware)
                continue
            normalized.append(middleware())
        return normalized

    def __del__(self):
        """Auto-cleanup on garbage collection"""
        # Most cleanup should have been done by the event loop close hook
        # This is just a fallback for any remaining cleanup

        # Signal the run loop to stop
        self._is_running = False
        if self.pending_event_queue:
            try:
                # Wake any blocked queue.get() in the weak runloop so it can exit.
                self.pending_event_queue.shutdown(immediate=True)
            except Exception:
                pass
        if self._runloop_task and not self._runloop_task.done():
            try:
                setattr(self._runloop_task, '_log_destroy_pending', False)
                self._runloop_task.cancel()
            except Exception:
                pass
        for task in tuple(self._parallel_event_tasks):
            if task.done():
                continue
            try:
                task.cancel()
            except Exception:
                pass
        for task in tuple(self._pending_middleware_tasks):
            if task.done():
                continue
            try:
                task.cancel()
            except Exception:
                pass

        # Our custom queue handles cleanup properly in shutdown()
        # No need for manual cleanup here

        # Check total memory usage across all EventBus instances
        try:
            self._check_total_memory_usage()
        except Exception:
            # Don't let memory check errors prevent cleanup
            pass

    def __str__(self) -> str:
        icon = '🟢' if self._is_running else '🔴'
        queue_size = self.pending_event_queue.qsize() if self.pending_event_queue else 0
        return f'{self.label}{icon}(queue={queue_size} active={len(self.in_flight_event_ids)} history={len(self.event_history)} handlers={len(self.handlers)})'

    @property
    def label(self) -> str:
        return f'{self.name}#{self.id[-4:]}'

    def __repr__(self) -> str:
        return str(self)

    @property
    def event_bus_serial_lock(self) -> ReentrantLock:
        """Public accessor for the bus-serial event lock used by LockManager."""
        return self._lock_for_event_bus_serial

    @property
    def event_global_serial_lock(self) -> ReentrantLock:
        """Class-scoped global-serial lock (isolated per EventBus subclass)."""
        return type(self)._lock_for_event_global_serial

    @staticmethod
    def _stub_handler_callable(_event: BaseEvent[Any]) -> None:
        return None

    @classmethod
    def _event_json_payload(cls, event: BaseEvent[Any]) -> dict[str, Any]:
        payload = event.model_dump(mode='json')
        payload['event_results'] = [result.model_dump(mode='json') for result in event.event_results.values()]
        return payload

    @staticmethod
    def _normalize_json_object(value: Any) -> dict[str, Any] | None:
        if not isinstance(value, dict):
            return None
        value_any = _as_any(value)
        normalized: dict[str, Any] = {}
        for raw_key, raw_value in value_any.items():
            key_value: Any = raw_key
            item_value: Any = raw_value
            if isinstance(key_value, str):
                normalized[key_value] = item_value
        return normalized

    @classmethod
    def _normalize_json_object_required(cls, value: Any, error_message: str) -> dict[str, Any]:
        normalized = cls._normalize_json_object(value)
        if normalized is None:
            raise TypeError(error_message)
        return normalized

    @staticmethod
    def _normalize_json_string_list(value: Any) -> list[str] | None:
        if not isinstance(value, list):
            return None
        value_any = _as_any(value)
        normalized: list[str] = []
        for raw_item in value_any:
            item_value: Any = raw_item
            if isinstance(item_value, str):
                normalized.append(item_value)
        return normalized

    @staticmethod
    def _upsert_handler_index(handlers_by_key: dict[str, list[PythonIdStr]], event_pattern: str, handler_id: PythonIdStr) -> None:
        ids = handlers_by_key.setdefault(event_pattern, [])
        if handler_id not in ids:
            ids.append(handler_id)

    @classmethod
    def _hydrate_handler_from_event_result(
        cls, bus: 'EventBus', event: BaseEvent[Any], result_payload: dict[str, Any]
    ) -> EventHandler:
        if isinstance(result_payload.get('handler'), dict):
            raise ValueError('Legacy nested EventResult.handler payload is not supported')
        raw_handler_id = result_payload.get('handler_id')
        handler_id: str | None = raw_handler_id if isinstance(raw_handler_id, str) and raw_handler_id else None
        if handler_id is None:
            raise ValueError('EventResult JSON payload must include handler_id')
        if handler_id in bus.handlers:
            return bus.handlers[handler_id]
        handler_payload: dict[str, Any] = {'id': handler_id}
        handler_payload.setdefault('handler_name', result_payload.get('handler_name') or 'anonymous')
        handler_payload.setdefault('handler_file_path', result_payload.get('handler_file_path'))
        if result_payload.get('handler_timeout') is not None:
            handler_payload.setdefault('handler_timeout', result_payload.get('handler_timeout'))
        if result_payload.get('handler_slow_timeout') is not None:
            handler_payload.setdefault('handler_slow_timeout', result_payload.get('handler_slow_timeout'))
        if result_payload.get('handler_registered_at') is not None:
            handler_payload.setdefault('handler_registered_at', result_payload.get('handler_registered_at'))
        handler_payload.setdefault('event_pattern', result_payload.get('handler_event_pattern') or event.event_type)
        handler_payload.setdefault('eventbus_name', result_payload.get('eventbus_name') or bus.name)
        handler_payload.setdefault('eventbus_id', result_payload.get('eventbus_id') or bus.id)

        handler_entry = EventHandler.model_validate({**handler_payload, 'handler': cls._stub_handler_callable})
        resolved_handler_id = handler_entry.id
        bus.handlers[resolved_handler_id] = handler_entry
        cls._upsert_handler_index(bus.handlers_by_key, handler_entry.event_pattern, resolved_handler_id)
        return handler_entry

    @classmethod
    def _hydrate_event_result_from_json(
        cls,
        *,
        bus: 'EventBus',
        event: BaseEvent[Any],
        result_payload: dict[str, Any],
    ) -> tuple[EventResult[Any], list[str]]:
        handler_entry = cls._hydrate_handler_from_event_result(bus, event, result_payload)
        model_payload = dict(result_payload)
        child_ids = [child_id for child_id in model_payload.pop('event_children', []) if isinstance(child_id, str)]
        has_result = 'result' in model_payload
        raw_result = model_payload.pop('result', None)
        for flat_key in (
            'handler_id',
            'handler_name',
            'handler_file_path',
            'handler_timeout',
            'handler_slow_timeout',
            'handler_registered_at',
            'handler_event_pattern',
            'eventbus_name',
            'eventbus_id',
        ):
            model_payload.pop(flat_key, None)
        model_payload['event_id'] = event.event_id
        model_payload['handler'] = handler_entry
        event_result = EventResult.model_validate(model_payload)
        if has_result:
            event_result.result = raw_result
        event_result.handler = handler_entry
        return event_result, child_ids

    def model_dump(self) -> dict[str, Any]:
        handlers_payload: dict[str, dict[str, Any]] = {}
        for handler_entry in self.handlers.values():
            handlers_payload[handler_entry.id] = handler_entry.model_dump(mode='json', exclude={'handler'})

        handlers_by_key_payload: dict[str, list[str]] = {
            str(key): [str(handler_id) for handler_id in handler_ids] for key, handler_ids in self.handlers_by_key.items()
        }
        for handler_entry in self.handlers.values():
            self._upsert_handler_index(handlers_by_key_payload, handler_entry.event_pattern, handler_entry.id)

        event_history_payload: dict[str, dict[str, Any]] = {}
        for event in self.event_history.values():
            event_history_payload[event.event_id] = self._event_json_payload(event)

        pending_event_ids: list[str] = []
        if self.pending_event_queue is not None:
            for queued_event in self.pending_event_queue.iter_items():
                event_id = queued_event.event_id
                if event_id not in event_history_payload:
                    event_history_payload[event_id] = self._event_json_payload(queued_event)
                pending_event_ids.append(event_id)

        return {
            'id': self.id,
            'name': self.name,
            'max_history_size': self.event_history.max_history_size,
            'max_history_drop': self.event_history.max_history_drop,
            'event_concurrency': str(self.event_concurrency),
            'event_timeout': self.event_timeout,
            'event_slow_timeout': self.event_slow_timeout,
            'event_handler_concurrency': str(self.event_handler_concurrency),
            'event_handler_completion': str(self.event_handler_completion),
            'event_handler_slow_timeout': self.event_handler_slow_timeout,
            'event_handler_detect_file_paths': self.event_handler_detect_file_paths,
            'handlers': handlers_payload,
            'handlers_by_key': handlers_by_key_payload,
            'event_history': event_history_payload,
            'pending_event_queue': pending_event_ids,
        }

    def model_dump_json(self, *, indent: int | None = None) -> str:
        return json.dumps(self.model_dump(), ensure_ascii=False, default=str, indent=indent)

    @classmethod
    def validate(cls, data: Any) -> 'EventBus':
        raw_payload: Any = data
        if isinstance(data, (bytes, bytearray)):
            raw_payload = data.decode('utf-8')
        if isinstance(raw_payload, str):
            raw_payload = json.loads(raw_payload)
        payload = cls._normalize_json_object(raw_payload)
        if payload is None:
            raise TypeError(f'EventBus.validate() expects dict or JSON string, got: {type(data).__name__}')
        name = payload.get('name')
        requested_name = str(name) if isinstance(name, str) else None
        bus = cls(
            name=None,
            event_concurrency=payload.get('event_concurrency'),
            event_handler_concurrency=payload.get('event_handler_concurrency') or EventHandlerConcurrencyMode.SERIAL,
            event_handler_completion=payload.get('event_handler_completion') or EventHandlerCompletionMode.ALL,
            max_history_size=payload.get('max_history_size')
            if isinstance(payload.get('max_history_size'), int) or payload.get('max_history_size') is None
            else 100,
            max_history_drop=bool(payload.get('max_history_drop', False)),
            event_timeout=payload.get('event_timeout')
            if isinstance(payload.get('event_timeout'), (int, float)) or payload.get('event_timeout') is None
            else None,
            event_slow_timeout=payload.get('event_slow_timeout')
            if isinstance(payload.get('event_slow_timeout'), (int, float)) or payload.get('event_slow_timeout') is None
            else 300.0,
            event_handler_slow_timeout=payload.get('event_handler_slow_timeout')
            if isinstance(payload.get('event_handler_slow_timeout'), (int, float))
            or payload.get('event_handler_slow_timeout') is None
            else 30.0,
            event_handler_detect_file_paths=bool(payload.get('event_handler_detect_file_paths', True)),
            id=payload.get('id') if isinstance(payload.get('id'), str) else None,
        )
        if requested_name is not None:
            bus.name = requested_name

        bus.handlers.clear()
        bus.handlers_by_key = defaultdict(list)
        bus.event_history.clear()

        raw_handlers = cls._normalize_json_object_required(
            payload.get('handlers'),
            'EventBus.validate() expects handlers to be an id-keyed object',
        )
        for raw_handler_id, raw_handler_payload in raw_handlers.items():
            handler_payload_json = cls._normalize_json_object(raw_handler_payload)
            if handler_payload_json is None:
                continue
            handler_payload: dict[str, Any] = dict(handler_payload_json)
            if 'id' not in handler_payload:
                handler_payload['id'] = raw_handler_id
            handler_entry = EventHandler.model_validate({**handler_payload, 'handler': cls._stub_handler_callable})
            bus.handlers[handler_entry.id] = handler_entry

        raw_handlers_by_key = cls._normalize_json_object_required(
            payload.get('handlers_by_key'),
            'EventBus.validate() expects handlers_by_key to be an object',
        )
        for raw_key, raw_ids in raw_handlers_by_key.items():
            handler_id_list = cls._normalize_json_string_list(raw_ids)
            if handler_id_list is None:
                continue
            handler_ids: list[PythonIdStr] = []
            for handler_id in handler_id_list:
                handler_ids.append(handler_id)
            bus.handlers_by_key[raw_key] = handler_ids

        pending_child_links: list[tuple[EventResult[Any], list[str]]] = []

        raw_event_history = cls._normalize_json_object_required(
            payload.get('event_history'),
            'EventBus.validate() expects event_history to be an id-keyed object',
        )
        history_items: list[tuple[str, Any]] = list(raw_event_history.items())

        for event_id_hint, event_payload_any in history_items:
            event_payload_json = cls._normalize_json_object(event_payload_any)
            if event_payload_json is None:
                continue
            event_payload: dict[str, Any] = dict(event_payload_json)
            raw_event_results = event_payload.pop('event_results', [])
            if 'event_id' not in event_payload or not isinstance(event_payload.get('event_id'), str):
                event_payload['event_id'] = event_id_hint
            try:
                event = BaseEvent[Any].model_validate(event_payload)
            except Exception:
                continue

            hydrated_results: dict[PythonIdStr, EventResult[Any]] = {}
            result_items: list[dict[str, Any]] = []
            if isinstance(raw_event_results, list):
                raw_event_results_any = _as_any(raw_event_results)
                for raw_item in raw_event_results_any:
                    result_payload = cls._normalize_json_object(raw_item)
                    if result_payload is None:
                        continue
                    result_items.append(dict(result_payload))

            for result_payload in result_items:
                try:
                    event_result, child_ids = cls._hydrate_event_result_from_json(
                        bus=bus,
                        event=event,
                        result_payload=result_payload,
                    )
                except Exception:
                    continue
                hydrated_results[event_result.handler_id] = event_result
                pending_child_links.append((event_result, child_ids))

            event.event_results = hydrated_results
            bus.event_history[event.event_id] = event

        pending_event_ids: list[str] = []
        raw_pending_queue = cls._normalize_json_string_list(payload.get('pending_event_queue'))
        if raw_pending_queue is None:
            raise TypeError('EventBus.validate() expects pending_event_queue to be a list of event ids')
        pending_event_ids.extend(raw_pending_queue)

        for event_result, child_ids in pending_child_links:
            event_result.event_children = [bus.event_history[child_id] for child_id in child_ids if child_id in bus.event_history]

        if pending_event_ids:
            queue = CleanShutdownQueue[BaseEvent[Any]](maxsize=0)
            for event_id in pending_event_ids:
                event = bus.event_history.get(event_id)
                if event is None:
                    continue
                queue.put_nowait(event)
            bus.pending_event_queue = queue
            bus._on_idle = asyncio.Event()
            if queue.qsize() == 0 and not bus._has_inflight_events_fast():
                bus._on_idle.set()
            else:
                bus._on_idle.clear()
        else:
            bus.pending_event_queue = None
            bus._on_idle = None

        bus._is_running = False
        bus._runloop_task = None
        bus._parallel_event_tasks = set()
        bus._pending_middleware_tasks = set()
        bus.in_flight_event_ids = set()
        bus.processing_event_ids = set()
        return bus

    async def on_event_change(self, event: BaseEvent[Any], status: EventStatus) -> None:
        if not self.middlewares:
            return
        for middleware in self.middlewares:
            await middleware.on_event_change(self, event, status)

    async def on_event_result_change(self, event: BaseEvent[Any], event_result: EventResult[Any], status: EventStatus) -> None:
        if not self.middlewares:
            return
        for middleware in self.middlewares:
            await middleware.on_event_result_change(self, event, event_result, status)

    def remove_event_from_pending_queue(self, event: BaseEvent[Any]) -> bool:
        if self.pending_event_queue is None:
            return False
        return self.pending_event_queue.remove_item(event)

    def mark_pending_queue_task_done(self) -> None:
        if self.pending_event_queue is None:
            return
        try:
            self.pending_event_queue.task_done()
        except ValueError:
            pass

    def queue_contains_event_id(self, event_id: str) -> bool:
        if self.pending_event_queue is None:
            return False
        for queued_event in self.pending_event_queue.iter_items():
            if queued_event.event_id == event_id:
                return True
        return False

    def is_event_inflight_or_queued(self, event_id: str) -> bool:
        if event_id in self.in_flight_event_ids:
            return True
        if event_id in self.processing_event_ids:
            return True
        return self.queue_contains_event_id(event_id)

    def is_event_processing(self, event_id: str) -> bool:
        return event_id in self.processing_event_ids

    def _resolve_find_waiters(self, event: BaseEvent[Any]) -> None:
        if not self.find_waiters:
            return
        for waiter in tuple(self.find_waiters):
            if (waiter.event_key != '*' and event.event_type != waiter.event_key) or not waiter.matches(event):
                continue
            if waiter.timeout_handle is not None:
                waiter.timeout_handle.cancel()
            self.find_waiters.discard(waiter)
            if not waiter.future.done():
                waiter.future.set_result(event)

    async def _wait_for_future_match(
        self,
        event_key: str,
        matches: Callable[[BaseEvent[Any]], bool],
        future: bool | float,
    ) -> BaseEvent[Any] | None:
        if future is False:
            return None
        event_match_future: asyncio.Future[BaseEvent[Any] | None] = asyncio.get_running_loop().create_future()
        waiter = _FindWaiter(event_key=event_key, matches=matches, future=event_match_future)
        if future is not True:
            timeout_seconds = float(future)

            def _on_wait_timeout() -> None:
                self.find_waiters.discard(waiter)
                if waiter.timeout_handle is not None:
                    waiter.timeout_handle.cancel()
                    waiter.timeout_handle = None
                if not event_match_future.done():
                    event_match_future.set_result(None)

            waiter.timeout_handle = asyncio.get_running_loop().call_later(timeout_seconds, _on_wait_timeout)
        self.find_waiters.add(waiter)
        try:
            return await event_match_future
        finally:
            self.find_waiters.discard(waiter)
            if waiter.timeout_handle is not None:
                waiter.timeout_handle.cancel()
                waiter.timeout_handle = None

    async def on_bus_handlers_change(self, handler: EventHandler, registered: bool) -> None:
        if not self.middlewares:
            return
        for middleware in self.middlewares:
            await middleware.on_bus_handlers_change(self, handler, registered)

    def _schedule_middleware_task(self, task: asyncio.Task[None]) -> None:
        self._pending_middleware_tasks.add(task)
        if self._on_idle is not None:
            self._on_idle.clear()

        def _on_done(done_task: asyncio.Task[None]) -> None:
            self._pending_middleware_tasks.discard(done_task)
            if self._on_idle and self.pending_event_queue:
                if not self._has_inflight_events_fast() and self.pending_event_queue.qsize() == 0:
                    self._on_idle.set()
            if done_task.cancelled():
                return
            exc = done_task.exception()
            if exc is not None:
                logger.error('❌ %s middleware task failed: %s(%r)', self, type(exc).__name__, exc)

        task.add_done_callback(_on_done)

    def _notify_handler_change(self, handler: EventHandler, registered: bool) -> None:
        if not self.middlewares:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Preserve .on()/.off() notifications registered before an event loop starts.
            self._pending_handler_changes.append((handler.model_copy(deep=False), registered))
            return
        task = loop.create_task(self.on_bus_handlers_change(handler, registered))
        self._schedule_middleware_task(task)

    def _flush_pending_handler_changes(self) -> None:
        if not self._pending_handler_changes or not self.middlewares:
            return
        loop = asyncio.get_running_loop()
        queued = list(self._pending_handler_changes)
        self._pending_handler_changes.clear()
        for handler, registered in queued:
            task = loop.create_task(self.on_bus_handlers_change(handler, registered))
            self._schedule_middleware_task(task)

    @staticmethod
    def _resolve_event_slow_timeout(event: BaseEvent[Any], eventbus: 'EventBus') -> float | None:
        if eventbus._rust_core is not None:
            return eventbus._rust_core.resolve_event_slow_timeout(event.event_slow_timeout)
        event_slow_timeout = event.event_slow_timeout
        if event_slow_timeout is not None:
            return event_slow_timeout
        return eventbus.event_slow_timeout

    @staticmethod
    def _resolve_handler_slow_timeout(event: BaseEvent[Any], handler: EventHandler, eventbus: 'EventBus') -> float | None:
        if eventbus._rust_core is not None:
            return eventbus._rust_core.resolve_handler_slow_timeout(
                handler_slow_timeout=handler.handler_slow_timeout,
                handler_slow_timeout_explicitly_set='handler_slow_timeout' in handler.model_fields_set,
                event_handler_slow_timeout=event.event_handler_slow_timeout,
                event_slow_timeout=event.event_slow_timeout,
            )
        if 'handler_slow_timeout' in handler.model_fields_set:
            return handler.handler_slow_timeout
        if event.event_handler_slow_timeout is not None:
            return event.event_handler_slow_timeout
        if event.event_slow_timeout is not None:
            return event.event_slow_timeout
        return eventbus.event_handler_slow_timeout

    @staticmethod
    def _resolve_handler_timeout(
        event: BaseEvent[Any],
        handler: EventHandler,
        eventbus: 'EventBus',
        timeout_override: float | None = None,
    ) -> float | None:
        if eventbus._rust_core is not None:
            return eventbus._rust_core.resolve_handler_timeout(
                handler_timeout=handler.handler_timeout,
                handler_timeout_explicitly_set='handler_timeout' in handler.model_fields_set,
                event_handler_timeout=event.event_handler_timeout,
                event_timeout=event.event_timeout,
                timeout_override=timeout_override,
            )
        if 'handler_timeout' in handler.model_fields_set:
            resolved_handler_timeout = handler.handler_timeout
        elif event.event_handler_timeout is not None:
            resolved_handler_timeout = event.event_handler_timeout
        else:
            resolved_handler_timeout = eventbus.event_timeout

        resolved_event_timeout = event.event_timeout if event.event_timeout is not None else eventbus.event_timeout

        if resolved_handler_timeout is None and resolved_event_timeout is None:
            resolved_timeout = None
        elif resolved_handler_timeout is None:
            resolved_timeout = resolved_event_timeout
        elif resolved_event_timeout is None:
            resolved_timeout = resolved_handler_timeout
        else:
            resolved_timeout = min(resolved_handler_timeout, resolved_event_timeout)

        if timeout_override is None:
            return resolved_timeout
        if resolved_timeout is None:
            return timeout_override
        return min(resolved_timeout, timeout_override)

    async def _slow_event_warning_monitor(self, event: BaseEvent[Any], event_slow_timeout: float) -> None:
        await asyncio.sleep(event_slow_timeout)
        if event.event_status == EventStatus.COMPLETED:
            return
        running_handler_count = sum(1 for result in event.event_results.values() if result.status == 'started')
        started_at = event.event_started_at or event.event_created_at
        started_at_dt = datetime.fromisoformat(started_at)
        elapsed_seconds = max(0.0, (datetime.now(UTC) - started_at_dt).total_seconds())
        logger.warning(
            '⚠️ Slow event processing: %s.on(%s#%s, %d handlers) still running after %.2fs',
            self.label,
            event.event_type,
            event.event_id[-4:],
            running_handler_count,
            elapsed_seconds,
        )

    def _has_inflight_events_fast(self) -> bool:
        return bool(
            self.in_flight_event_ids or self.processing_event_ids or self._parallel_event_tasks or self._pending_middleware_tasks
        )

    def _mark_event_complete_on_all_buses(self, event: BaseEvent[Any]) -> None:
        event_id = event.event_id
        for bus in list(type(self).all_instances):
            if bus:
                bus.in_flight_event_ids.discard(event_id)
                if bus._rust_core is not None:
                    bus._rust_core.clear_inflight(event_id)
                    bus._rust_core.update_event_status(event_id, 'completed')
                if bus.event_history.max_history_size == 0:
                    bus.event_history.remove_event(event_id)
                    if bus._rust_core is not None:
                        bus._rust_core.remove_event(event_id)

    @property
    def events_pending(self) -> list[BaseEvent[Any]]:
        """Get events that haven't started processing yet (does not include events still being enqueued in self.event_queue)."""
        return [
            event
            for event in self.event_history.values()
            if event.event_status != EventStatus.COMPLETED and event.event_status != EventStatus.STARTED
        ]

    @property
    def events_started(self) -> list[BaseEvent[Any]]:
        """Get events currently being processed"""
        return [
            event
            for event in self.event_history.values()
            if event.event_status != EventStatus.COMPLETED and event.event_status == EventStatus.STARTED
        ]

    @property
    def events_completed(self) -> list[BaseEvent[Any]]:
        """Get events that have completed processing"""
        return [event for event in self.event_history.values() if event.event_status == EventStatus.COMPLETED]

    # Overloads for typed event patterns with specific handler signatures
    # Order matters - more specific types must come before general ones

    # Class pattern registration keeps strict event typing.
    @overload
    def on(self, event_pattern: type[T_OnEvent], handler: ContravariantEventHandlerCallable[T_OnEvent]) -> EventHandler: ...

    # String and wildcard registration is looser: any BaseEvent subclass handler is allowed.
    @overload
    def on(
        self,
        event_pattern: PythonIdentifierStr | Literal['*'],
        handler: ContravariantEventHandlerCallable[T_OnEvent],
    ) -> EventHandler: ...

    # I dont think this is needed, but leaving it here for now
    # 9. Coroutine[Any, Any, Any] - direct coroutine
    # @overload
    # def on(self, event_pattern: EventPatternType, handler: Coroutine[Any, Any, Any]) -> None: ...

    def on(
        self,
        event_pattern: EventPatternType,
        handler: ContravariantEventHandlerCallable[T_OnEvent],
    ) -> EventHandler:
        """
        Subscribe to events matching a pattern, event type name, or event model class.
        Use event_pattern='*' to subscribe to all events. Handler can be sync or async function or method.

        Examples:
                eventbus.on('TaskStartedEvent', handler)  # Specific event type
                eventbus.on(TaskStartedEvent, handler)  # Event model class
                eventbus.on('*', handler)  # Subscribe to all events
                eventbus.on('*', other_eventbus.emit)  # Forward all events to another EventBus

        Note: When forwarding events between buses, all handler results are automatically
        flattened into the original event's results, so EventResults sees all handlers
        from all buses as a single flat collection.
        """
        assert isinstance(event_pattern, str) or isinstance(event_pattern, type), (
            f'Invalid event pattern: {event_pattern}, must be a string event type or subclass of BaseEvent'
        )
        assert inspect.isfunction(handler) or inspect.ismethod(handler) or inspect.iscoroutinefunction(handler), (
            f'Invalid handler: {handler}, must be a sync or async function or method'
        )

        # Normalize event key to string event_type or wildcard.
        event_key = EventHistory.normalize_event_pattern(event_pattern)

        # Ensure event_key is definitely a string at this point
        assert isinstance(event_key, str)

        # Check for duplicate handler names. Keep this bounded so large handler
        # registrations (e.g. perf scenarios with tens of thousands of handlers)
        # do not degrade into O(n^2) registration time.
        new_handler_name = EventHandler.get_callable_handler_name(handler)
        existing_handler_ids = self.handlers_by_key.get(event_key, [])
        if existing_handler_ids and len(existing_handler_ids) <= self._duplicate_handler_name_check_limit:
            for existing_handler_id in existing_handler_ids:
                existing_handler = self.handlers.get(existing_handler_id)
                if existing_handler and existing_handler.handler_name == new_handler_name:
                    warnings.warn(
                        f"⚠️ {self} Handler {new_handler_name} already registered for event '{event_key}'. "
                        f'This may make it difficult to filter event results by handler name. '
                        f'Consider using unique function names.',
                        UserWarning,
                        stacklevel=2,
                    )
                    break

        # Register handler entry and index it by event key.
        handler_entry = EventHandler.from_callable(
            handler=handler,
            event_pattern=event_key,
            eventbus_name=self.name,
            eventbus_id=self.id,
            detect_handler_file_path=self.event_handler_detect_file_paths,
        )
        assert handler_entry.id is not None
        self.handlers[handler_entry.id] = handler_entry
        self.handlers_by_key[event_key].append(handler_entry.id)
        # Mirror to Rust core for scheduling decisions
        if self._rust_core is not None:
            self._rust_core.register_handler(
                handler_id=handler_entry.id,
                handler_name=handler_entry.handler_name,
                event_pattern=event_key,
                eventbus_name=self.name,
                eventbus_id=self.id,
            )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                '👂 %s.on(%s, %s) Registered event handler #%s',
                self,
                event_key,
                handler_entry.handler_name,
                handler_entry.id[-4:],
            )
        self._notify_handler_change(handler_entry, registered=True)
        return handler_entry

    @overload
    def off(
        self, event_pattern: type[T_Event], handler: EventHandlerCallable | PythonIdStr | EventHandler | None = None
    ) -> None: ...

    @overload
    def off(
        self,
        event_pattern: PythonIdentifierStr | Literal['*'],
        handler: EventHandlerCallable | PythonIdStr | EventHandler | None = None,
    ) -> None: ...

    def off(
        self,
        event_pattern: EventPatternType,
        handler: EventHandlerCallable | PythonIdStr | EventHandler | None = None,
    ) -> None:
        """Deregister handlers for an event pattern by id, callable, EventHandler, or all."""
        event_key = EventHistory.normalize_event_pattern(event_pattern)
        indexed_ids = list(self.handlers_by_key.get(event_key, []))
        if not indexed_ids:
            return

        requested_id: str | None = None
        requested_callable: EventHandlerCallable | None = None
        if isinstance(handler, EventHandler):
            requested_id = handler.id
        elif isinstance(handler, str):
            requested_id = handler
        elif handler is not None:
            requested_callable = handler

        for handler_id in indexed_ids:
            entry = self.handlers.get(handler_id)
            if entry is None:
                self._remove_indexed_handler(event_key, handler_id)
                continue

            should_remove = False
            if handler is None:
                should_remove = True
            elif requested_id is not None and entry.id == requested_id:
                should_remove = True
            elif requested_callable is not None and entry.handler is requested_callable:
                should_remove = True

            if should_remove:
                self.handlers.pop(handler_id, None)
                self._remove_indexed_handler(event_key, handler_id)
                if self._rust_core is not None:
                    self._rust_core.unregister_handler(handler_id, event_key)
                self._notify_handler_change(entry, registered=False)

    def emit(self, event: T_ExpectedEvent) -> T_ExpectedEvent:
        """
        Enqueue an event for processing and immediately return an Event(status='pending') version (synchronous).
        You can await the returned Event(status='pending') object to block until it is done being executed aka Event(status='completed'),
        or you can interact with the unawaited Event(status='pending') before its handlers have finished.

        (The first EventBus.emit() call auto-starts the bus processing loop task if not already running)

        >>> completed_event = await eventbus.emit(SomeEvent())
                # 1. enqueues the event synchronously
                # 2. returns an awaitable SomeEvent() with pending results in .event_results
                # 3. awaits the SomeEvent() which waits until all pending results are complete and returns the completed SomeEvent()

        >>> result_value = await eventbus.emit(SomeEvent()).event_result()
                # 1. enqueues the event synchronously
                # 2. returns a pending SomeEvent() with pending results in .event_results
                # 3. awaiting .event_result() waits until all pending results are complete, and returns the raw result value of the first one
        """

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError(f'{self}.emit() called but no event loop is running! Event not queued: {event.event_type}')

        assert event.event_id, 'Missing event.event_id: UUIDStr = uuid7str()'
        assert event.event_created_at, 'Missing event.event_created_at: str = monotonic_datetime()'
        assert event.event_type and event.event_type.isidentifier(), 'Missing event.event_type: str'

        # Automatically set event_parent_id from context when emitting a NEW child event.
        # If we are forwarding the same event object from inside its own handler, keep the
        # existing parent linkage untouched to avoid self-parent cycles.
        if event.event_parent_id is None:
            current_event: BaseEvent[Any] | None = EventBus.current_event_context.get()
            if current_event is not None and event.event_id != current_event.event_id:
                event.event_parent_id = current_event.event_id

        # Capture emit-time context for propagation to handlers (GitHub issue #20)
        # This ensures ContextVars set before emit() are accessible in handlers
        if event._get_dispatch_context() is None:  # pyright: ignore[reportPrivateUsage]
            event._set_dispatch_context(contextvars.copy_context())  # pyright: ignore[reportPrivateUsage]

        # Track child events - if we're inside a handler, add this event to the handler's event_children list
        # Only track if this is a NEW event (not forwarding an existing event)
        current_handler_id = EventBus.current_handler_id_context.get()
        if current_handler_id is not None:
            current_event = EventBus.current_event_context.get()
            if current_event is not None and current_handler_id in current_event.event_results:
                # Only add as child if it's a different event (not forwarding the same event)
                if event.event_id != current_event.event_id:
                    if event.event_emitted_by_handler_id is None:
                        event.event_emitted_by_handler_id = current_handler_id
                    current_event.event_results[current_handler_id].event_children.append(event)

        # Add this EventBus label to the event_path if not already there.
        # If the bus is already in the path, this is a forwarding loop — reject
        # the event at this bus boundary instead of queuing it.
        if self.label not in event.event_path:
            event.event_path.append(self.label)
        else:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    '⚠️ %s.emit(%s) - Bus already in path, skipping to prevent forwarding loop. Path: %s',
                    self,
                    event.event_type,
                    event.event_path,
                )
            return event

        assert event.event_path, 'Missing event.event_path: list[str] (with at least one bus label recorded in it)'
        assert all(
            '#' in entry
            and entry.rsplit('#', 1)[0].isidentifier()
            and entry.rsplit('#', 1)[1].isalnum()
            and len(entry.rsplit('#', 1)[1]) == 4
            for entry in event.event_path
        ), f'Event.event_path must be a list of EventBus labels BusName#abcd, got: {event.event_path}'

        # NOTE:
        # emit() is intentionally synchronous and runs on the same event-loop
        # thread as the runloop task. Blocking here for "pressure" would deadlock
        # naive flood loops because the runloop cannot progress until emit() returns.
        # So pressure is handled by policy:
        #   - max_history_drop=True  -> absorb and trim oldest history entries
        #   - max_history_drop=False -> reject new emits at max_history_size
        if (
            self.event_history.max_history_size is not None
            and self.event_history.max_history_size > 0
            and not self.event_history.max_history_drop
        ):
            if len(self.event_history) >= self.event_history.max_history_size:
                # Before rejecting, opportunistically evict already-completed history entries.
                # This preserves max_history_drop=False semantics (never dropping in-flight events)
                # while avoiding needless backpressure when only completed entries are occupying the cap.
                self.event_history.trim_event_history(owner_label=str(self))
            if len(self.event_history) >= self.event_history.max_history_size:
                raise RuntimeError(
                    f'{self} history limit reached ({len(self.event_history)}/{self.event_history.max_history_size}); '
                    'set event_history.max_history_drop=True to drop old history instead of rejecting new events'
                )

        # Auto-start if needed
        self._flush_pending_handler_changes()
        self._start()
        # Ensure every emitted event has a completion signal tied to this loop.
        # Completion logic always sets this signal; consumers like event_results_* await it.
        _ = event.event_completed_signal

        # Put event in queue synchronously using put_nowait
        if self.pending_event_queue:
            try:
                self.pending_event_queue.put_nowait(event)
                # Only add to history after successfully queuing
                self.event_history[event.event_id] = event
                self.in_flight_event_ids.add(event.event_id)
                # Store event metadata in Rust core for scheduling decisions
                if self._rust_core is not None:
                    self._rust_core.store_event_meta(
                        event_id=event.event_id,
                        event_type=event.event_type,
                        event_path=list(event.event_path),
                        event_status=str(event.event_status),
                        event_parent_id=event.event_parent_id,
                        event_created_at=event.event_created_at,
                        event_concurrency=str(event.event_concurrency) if event.event_concurrency else None,
                        event_handler_concurrency=str(event.event_handler_concurrency) if event.event_handler_concurrency else None,
                        event_handler_completion=str(event.event_handler_completion) if event.event_handler_completion else None,
                        event_timeout=event.event_timeout,
                        event_handler_timeout=event.event_handler_timeout,
                        event_slow_timeout=event.event_slow_timeout,
                        event_handler_slow_timeout=event.event_handler_slow_timeout,
                        emitted_by_handler_id=event.event_emitted_by_handler_id,
                    )
                    self._rust_core.mark_inflight(event.event_id)
                    # Sync parent event result statuses so Rust can walk the ancestry chain
                    if event.event_parent_id and event.event_parent_id in self.event_history:
                        parent = self.event_history[event.event_parent_id]
                        for hid, result in parent.event_results.items():
                            self._rust_core.update_result_status(parent.event_id, hid, str(result.status))
                # Resolve future find waiters immediately on emit so callers
                # don't wait for queue position or handler execution.
                self._resolve_find_waiters(event)
                if logger.isEnabledFor(logging.INFO):
                    logger.info(
                        '🗣️ %s.emit(%s) ➡️ %s#%s (#%d %s)',
                        self,
                        event.event_type,
                        event.event_type,
                        event.event_id[-4:],
                        self.pending_event_queue.qsize(),
                        event.event_status,
                    )
            except asyncio.QueueFull:
                # Don't add to history if we can't queue it
                logger.error(
                    f'⚠️ {self} Event queue is full! Dropping event and aborting {event.event_type}:\n{event.model_dump_json()}'
                )
                raise  # could also block indefinitely until queue has space, but dont drop silently or delete events
        else:
            logger.warning('⚠️ %s.emit() called but event_queue is None! Event not queued: %s', self, event.event_type)

        # Note: We do NOT pre-create EventResults here anymore.
        # EventResults are created only when handlers actually start executing.
        # This avoids "orphaned" pending results for handlers that get filtered out later.

        # Amortize cleanup work by trimming only after a soft overage; this keeps
        # hot emit fast under large naive floods while still bounding memory.
        if (
            self.event_history.max_history_size is not None
            and self.event_history.max_history_size > 0
            and self.event_history.max_history_drop
        ):
            soft_limit = max(self.event_history.max_history_size, int(self.event_history.max_history_size * 1.2))
            if len(self.event_history) > soft_limit:
                self.event_history.trim_event_history(owner_label=str(self))

        return event

    def dispatch(self, event: T_ExpectedEvent) -> T_ExpectedEvent:
        """Convenience synonym for :meth:`emit`."""
        return self.emit(event)

    def _remove_indexed_handler(self, event_pattern: str, handler_id: PythonIdStr) -> None:
        ids = self.handlers_by_key.get(event_pattern)
        if not ids:
            return
        if handler_id in ids:
            ids.remove(handler_id)
        if not ids:
            self.handlers_by_key.pop(event_pattern, None)

    @overload
    async def find(
        self,
        event_type: type[T_ExpectedEvent],
        where: None = None,
        child_of: BaseEvent[Any] | None = None,
        past: bool | float | timedelta | None = None,
        future: bool | float | None = None,
        **event_fields: Any,
    ) -> T_ExpectedEvent | None: ...

    @overload
    async def find(
        self,
        event_type: type[T_ExpectedEvent],
        where: Callable[[T_ExpectedEvent], bool],
        child_of: BaseEvent[Any] | None = None,
        past: bool | float | timedelta | None = None,
        future: bool | float | None = None,
        **event_fields: Any,
    ) -> T_ExpectedEvent | None: ...

    @overload
    async def find(
        self,
        event_type: PythonIdentifierStr | Literal['*'],
        where: Callable[[BaseEvent[Any]], bool] | None = None,
        child_of: BaseEvent[Any] | None = None,
        past: bool | float | timedelta | None = None,
        future: bool | float | None = None,
        **event_fields: Any,
    ) -> BaseEvent[Any] | None: ...

    async def find(
        self,
        event_type: EventPatternType,
        where: Callable[[Any], bool] | None = None,
        child_of: BaseEvent[Any] | None = None,
        past: bool | float | timedelta | None = None,
        future: bool | float | None = None,
        **event_fields: Any,
    ) -> BaseEvent[Any] | None:
        """
        Find an event matching criteria in history and/or future.

        Mirrors TS `EventBus.find` behavior:
        - Default behavior with no options: `past=True`, `future=False`
        - Search history and return the most recent match
        - Optionally wait for future emits
        - Supports exact-match equality filters via keyword args for any event field

        Args:
            event_type: The event type string or model class to find
            where: Predicate function for filtering (default: lambda _: True)
            child_of: Only match events that are descendants of this parent event
            past: Controls history search behavior:
                - True: search all history
                - False: skip history search
                - float: search events from last N seconds only
                - timedelta: search events from last N seconds
            future: Controls future wait behavior:
                - True: wait forever for matching event
                - False: don't wait for future events
                - float: wait up to N seconds for matching event
            **event_fields: Optional exact-match filters for any event field
                (for example `event_status='completed'`, `user_id='u-1'`)

        Returns:
            Matching event or None if not found/timeout
        """
        return await self.event_history.find(
            event_type,
            where=where,
            child_of=child_of,
            past=past,
            future=future,
            event_is_child_of=self.event_is_child_of,
            wait_for_future_match=self._wait_for_future_match,
            **event_fields,
        )

    def event_is_child_of(self, event: BaseEvent[Any], ancestor: BaseEvent[Any]) -> bool:
        """
        Check if event is a descendant of ancestor (child, grandchild, etc.).

        Walks up the parent chain from event looking for ancestor.
        Returns True if ancestor is found in the chain, False otherwise.
        """
        # Delegate to Rust core when available (fast parent-chain walk)
        if self._rust_core is not None and self._rust_core.has_event(event.event_id):
            return self._rust_core.is_child_of(event.event_id, ancestor.event_id)

        current_id = event.event_parent_id
        visited: set[str] = set()

        while current_id and current_id not in visited:
            if current_id == ancestor.event_id:
                return True
            visited.add(current_id)

            parent = self.event_history.get(current_id)
            if parent is None:
                for bus in list(type(self).all_instances):
                    if bus is not self and current_id in bus.event_history:
                        parent = bus.event_history[current_id]
                        break
            if parent is None:
                break
            current_id = parent.event_parent_id

        return False

    def event_is_parent_of(self, event: BaseEvent[Any], descendant: BaseEvent[Any]) -> bool:
        return self.event_is_child_of(descendant, event)

    def _start(self) -> None:
        """Start the event bus if not already running"""
        if not self._is_running:
            try:
                loop = asyncio.get_running_loop()

                # Hook into the event loop's close method to cleanup before it closes
                # this is necessary to silence "RuntimeError: no running event loop" and "event loop is closed" errors on shutdown
                eventbus_class = type(self)
                registered_eventbuses: weakref.WeakSet[EventBus] | None = eventbus_class._loop_eventbus_instances.get(loop)
                if registered_eventbuses is None:
                    registered_eventbuses = weakref.WeakSet()
                    eventbus_class._loop_eventbus_instances[loop] = registered_eventbuses

                if loop not in eventbus_class._loop_original_close:
                    original_close = loop.close
                    eventbus_class._loop_original_close[loop] = original_close

                    def close_with_cleanup() -> None:
                        empty_eventbuses: weakref.WeakSet[EventBus] = weakref.WeakSet()
                        # Clean up all registered EventBuses before closing the loop
                        for eventbus in list(eventbus_class._loop_eventbus_instances.get(loop, empty_eventbuses)):
                            try:
                                # Stop the eventbus while loop is still running
                                if eventbus._is_running:
                                    eventbus._is_running = False

                                    # Shutdown the queue properly - our custom queue will handle cleanup
                                    if eventbus.pending_event_queue:
                                        eventbus.pending_event_queue.shutdown(immediate=True)

                                    if eventbus._runloop_task and not eventbus._runloop_task.done():
                                        # Suppress warning before cancelling
                                        setattr(eventbus._runloop_task, '_log_destroy_pending', False)
                                        eventbus._runloop_task.cancel()
                            except Exception:
                                pass

                        # Now close the loop
                        original_close()

                    setattr(loop, 'close', close_with_cleanup)

                # Register this EventBus instance in the per-loop WeakSet.
                registered_eventbuses.add(self)

                # Create async objects if needed
                if self.pending_event_queue is None:
                    # Keep queue unbounded so naive emit floods can enqueue without
                    # artificial queue caps; queue stores event object references.
                    self.pending_event_queue = CleanShutdownQueue[BaseEvent[Any]](maxsize=0)
                    self._on_idle = asyncio.Event()
                    if not self._has_inflight_events_fast() and self.pending_event_queue.qsize() == 0:
                        self._on_idle.set()
                    else:
                        self._on_idle.clear()

                # Create and start the run loop task.
                # Use a weakref-based runner so an unreferenced EventBus can be GC'd
                # without requiring explicit stop(clear=True) by callers.
                # Run loops must start with a clean context. If emit() is called
                # from inside a handler, lock-depth ContextVars would otherwise leak
                # into the new task and bypass event lock acquisition.
                self._runloop_task = loop.create_task(
                    EventBus._run_loop_weak(weakref.ref(self)),
                    name=f'{self}._run_loop',
                    context=contextvars.Context(),
                )
                self._is_running = True
            except RuntimeError:
                # No event loop - will start when one becomes available
                pass

    async def stop(self, timeout: float | None = None, clear: bool = False) -> None:
        """Stop the event bus, optionally waiting for events to complete

        Args:
            timeout: Maximum time to wait for pending events to complete
            clear: If True, clear event history and remove from global tracking to free memory
        """
        if not self._is_running and not self._parallel_event_tasks:
            for waiter in tuple(self.find_waiters):
                self.find_waiters.discard(waiter)
                if waiter.timeout_handle is not None:
                    waiter.timeout_handle.cancel()
                if not waiter.future.done():
                    waiter.future.set_result(None)
            return

        # Wait for completion if timeout specified and > 0
        # timeout=0 means "don't wait", so skip the wait entirely
        if timeout is not None and timeout > 0:
            try:
                await self.wait_until_idle(timeout=timeout)
            except TimeoutError:
                pass

        queue_size = self.pending_event_queue.qsize() if self.pending_event_queue else 0
        has_inflight = self._has_inflight_events_fast()
        if queue_size or has_inflight:
            logger.debug(
                '⚠️ %s stopping with pending events: queue=%d inflight=%s history=%d',
                self,
                queue_size,
                has_inflight,
                len(self.event_history),
            )

        # Signal shutdown
        self._is_running = False

        # Shutdown the queue to unblock any pending get() operations
        if self.pending_event_queue:
            self.pending_event_queue.shutdown()

        # print('STOPPING', self.event_history)

        # Wait for the run loop task to finish / force-cancel it if it's hanging
        if self._runloop_task and not self._runloop_task.done():
            await asyncio.wait({self._runloop_task}, timeout=0.1)
            try:
                self._runloop_task.cancel()
            except Exception:
                pass

        if self._parallel_event_tasks:
            for task in list(self._parallel_event_tasks):
                if not task.done():
                    task.cancel()
            await asyncio.gather(*list(self._parallel_event_tasks), return_exceptions=True)
            self._parallel_event_tasks.clear()

        # Clear references
        self._runloop_task = None
        self.in_flight_event_ids.clear()
        self.processing_event_ids.clear()
        for waiter in tuple(self.find_waiters):
            self.find_waiters.discard(waiter)
            if waiter.timeout_handle is not None:
                waiter.timeout_handle.cancel()
            if not waiter.future.done():
                waiter.future.set_result(None)
        if self._on_idle:
            self._on_idle.set()

        # Rename the bus to release the name.
        self.name = f'_stopped_{self.id[-8:]}'
        if self._rust_core is not None:
            self._rust_core.bus_name = self.name

        # Clear event history and handlers if requested (for memory cleanup)
        if clear:
            self.event_history.clear()
            self.handlers.clear()
            self.handlers_by_key.clear()
            self.in_flight_event_ids.clear()
            if self._rust_core is not None:
                self._rust_core.clear_all()

            # Remove from global instance tracking
            if self in type(self).all_instances:
                type(self).all_instances.discard(self)

            # Remove from event loop's tracking if present
            try:
                loop = asyncio.get_running_loop()
                registered_eventbuses = type(self)._loop_eventbus_instances.get(loop)
                if registered_eventbuses is not None:
                    registered_eventbuses.discard(self)
            except RuntimeError:
                # No running loop, that's fine
                pass

            logger.debug('🧹 %s cleared event history and removed from global tracking', self)

        logger.debug('🛑 %s shut down %s', self, 'gracefully' if timeout is not None else 'immediately')

        # Check total memory usage across all instances
        try:
            self._check_total_memory_usage()
        except Exception:
            # Don't let memory check errors prevent shutdown
            pass

    async def wait_until_idle(self, timeout: float | None = None) -> None:
        """Wait until the event bus is idle (no events being processed and all handlers completed)"""

        self._start()
        assert self._on_idle and self.pending_event_queue, 'EventBus._start() must be called before wait_until_idle() is reached'

        start_time = asyncio.get_event_loop().time()
        remaining_timeout = timeout

        try:
            # Wait until both queue and inflight execution are empty.
            # Avoid relying on queue.join() because unfinished-task counters can
            # drift under queue-jump paths while observable runtime state is idle.
            while True:
                queue_empty = self.pending_event_queue.qsize() == 0
                has_inflight = self._has_inflight_events_fast()
                if queue_empty and not has_inflight:
                    self._on_idle.set()
                    break

                if timeout is not None:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    remaining_timeout = max(0, timeout - elapsed)
                    if remaining_timeout <= 0:
                        raise TimeoutError()

                # Wait again for an idle transition.
                idle_task = asyncio.create_task(self._on_idle.wait())
                await asyncio.wait_for(idle_task, timeout=remaining_timeout)
                await asyncio.sleep(0)  # Yield again

        except TimeoutError:
            logger.warning(
                '⌛️ %s Timeout waiting for event bus to be idle after %ss (history=%d)',
                self,
                timeout,
                len(self.event_history),
            )

    @staticmethod
    async def _run_loop_weak(bus_ref: 'weakref.ReferenceType[EventBus]') -> None:
        """
        Weakref-based run loop.

        This runner avoids holding a strong EventBus reference while idle,
        allowing unreferenced buses to be garbage-collected naturally without
        an explicit stop().
        """
        try:
            while True:
                bus = bus_ref()
                if bus is None or not bus._is_running:
                    break

                queue = bus.pending_event_queue
                on_idle = bus._on_idle
                del bus

                if queue is None or on_idle is None:
                    await asyncio.sleep(0.01)
                    continue

                event: BaseEvent[Any] | None = None
                try:
                    event = await queue.get()
                except QueueShutDown:
                    break
                except asyncio.CancelledError:
                    break
                except RuntimeError as e:
                    if 'Event loop is closed' in str(e) or 'no running event loop' in str(e):
                        break
                    logger.exception(f'❌ Weak run loop runtime error: {type(e).__name__} {e}', exc_info=True)
                    continue
                except Exception as e:
                    logger.exception(f'❌ Weak run loop error: {type(e).__name__} {e}', exc_info=True)
                    continue

                bus = bus_ref()
                if bus is None:
                    try:
                        queue.task_done()
                    except Exception:
                        pass
                    break

                try:
                    if bus._on_idle:
                        bus._on_idle.clear()

                    bus.processing_event_ids.add(event.event_id)

                    event_lock = bus.locks.get_lock_for_event(bus, event)
                    if event_lock is None:

                        async def process_parallel_event(
                            bus: 'EventBus' = bus,
                            event: BaseEvent[Any] = event,
                            queue: CleanShutdownQueue[BaseEvent[Any]] = queue,
                        ) -> None:
                            try:
                                await bus.step(event=event)
                            finally:
                                try:
                                    queue.task_done()
                                except ValueError:
                                    pass

                        task = asyncio.create_task(
                            process_parallel_event(),
                            name=f'{bus}._process_event({event.event_id[-4:]})',
                        )
                        bus._parallel_event_tasks.add(task)

                        def _on_done(
                            done_task: asyncio.Task[None], *, bus_ref: 'weakref.ReferenceType[EventBus]' = weakref.ref(bus)
                        ):
                            live_bus = bus_ref()
                            if live_bus is not None:
                                live_bus._parallel_event_tasks.discard(done_task)
                                if (
                                    live_bus._on_idle
                                    and live_bus.pending_event_queue
                                    and not live_bus._has_inflight_events_fast()
                                    and live_bus.pending_event_queue.qsize() == 0
                                ):
                                    live_bus._on_idle.set()
                            if done_task.cancelled():
                                return
                            try:
                                exc = done_task.exception()
                            except asyncio.CancelledError:
                                return
                            if exc is not None:
                                logger.exception('❌ Weak run loop parallel event task error: %s %s', type(exc).__name__, exc)

                        task.add_done_callback(_on_done)
                    else:
                        try:
                            await bus.step(event=event)
                        finally:
                            try:
                                queue.task_done()
                            except ValueError:
                                pass
                        if bus._on_idle and bus.pending_event_queue:
                            if not bus._has_inflight_events_fast() and bus.pending_event_queue.qsize() == 0:
                                bus._on_idle.set()
                except QueueShutDown:
                    break
                except asyncio.CancelledError:
                    break
                except RuntimeError as e:
                    if 'Event loop is closed' in str(e) or 'no running event loop' in str(e):
                        break
                    logger.exception(f'❌ Weak run loop runtime error: {type(e).__name__} {e}', exc_info=True)
                except Exception as e:
                    logger.exception(f'❌ Weak run loop error: {type(e).__name__} {e}', exc_info=True)
                finally:
                    del bus
        finally:
            bus = bus_ref()
            if bus is not None:
                bus._is_running = False

    async def _get_next_event(self, wait_for_timeout: float = 0.1) -> 'BaseEvent[Any] | None':
        """Get the next event from the queue"""

        assert self._on_idle and self.pending_event_queue, 'EventBus._start() must be called before _get_next_event()'
        if not self._is_running:
            return None

        try:
            # Create a task for queue.get() so we can cancel it cleanly
            get_next_queued_event = asyncio.create_task(self.pending_event_queue.get())
            setattr(get_next_queued_event, '_log_destroy_pending', False)  # Suppress warnings on cleanup

            # Wait for next event with timeout
            has_next_event, _pending = await asyncio.wait({get_next_queued_event}, timeout=wait_for_timeout)
            if has_next_event:
                # Check if we're still running before returning the event
                if not self._is_running:
                    get_next_queued_event.cancel()
                    return None
                return await get_next_queued_event  # await to actually resolve it to the next event
            else:
                # Get task timed out, cancel it cleanly to suppress warnings
                get_next_queued_event.cancel()

                # Check if we're idle, if so, set the idle flag
                if not self._has_inflight_events_fast() and self.pending_event_queue.qsize() == 0:
                    self._on_idle.set()
                return None

        except (asyncio.CancelledError, RuntimeError, QueueShutDown):
            # Clean cancellation during shutdown or queue was shut down
            return None

    async def _finalize_local_event_processing(self, event: BaseEvent[Any]) -> None:
        """
        Clear local in-flight markers and run completion propagation exactly once.

        This is shared by both `step()` and the weak runloop path so completion
        semantics stay identical regardless of which runner consumed the event.
        """
        self.processing_event_ids.discard(event.event_id)
        self.in_flight_event_ids.discard(event.event_id)
        # Sync to Rust core
        if self._rust_core is not None:
            self._rust_core.clear_processing(event.event_id)
            self._rust_core.clear_inflight(event.event_id)
            self._rust_core.update_event_status(event.event_id, str(event.event_status))

        newly_completed_events = self._mark_event_tree_complete_if_ready(event)
        for completed_event in newly_completed_events:
            await self.on_event_change(completed_event, EventStatus.COMPLETED)

    def _mark_event_tree_complete_if_ready(self, root_event: BaseEvent[Any]) -> list[BaseEvent[Any]]:
        """
        Re-check completion for `root_event` and descendants in post-order.

        Timeout/cancellation paths can update child result statuses after an
        earlier completion check. Running this post-order pass ensures children
        are marked complete before their parents are re-evaluated.
        """
        newly_completed: list[BaseEvent[Any]] = []
        visited_event_ids: set[str] = set()

        def visit(event: BaseEvent[Any]) -> None:
            if event.event_id in visited_event_ids:
                return
            visited_event_ids.add(event.event_id)

            for child_event in event.event_children:
                visit(child_event)

            was_complete = event.event_status == EventStatus.COMPLETED
            # Only the root event may still appear "in-flight" on this bus during finalization.
            # Descendants are not currently being processed in this frame, so they must consider
            # queues on this bus too (otherwise queued children can be marked complete too early).
            current_bus = self if event.event_id == root_event.event_id else None
            event._mark_completed(current_bus=current_bus)  # pyright: ignore[reportPrivateUsage]
            just_completed = (not was_complete) and event.event_status == EventStatus.COMPLETED
            if just_completed:
                self._mark_event_complete_on_all_buses(event)
                newly_completed.append(event)

        visit(root_event)
        return newly_completed

    async def step(
        self, event: 'BaseEvent[Any] | None' = None, timeout: float | None = None, wait_for_timeout: float = 0.1
    ) -> 'BaseEvent[Any] | None':
        """
        Consume and process a single event from the queue (one iteration of the run loop).

        This is the high-level "consumer" method that:
        1. Dequeues the next event (or uses one passed in)
        2. Acquires the event lock selected by concurrency mode
        3. Calls _process_event() to execute handlers
        4. Marks the queue task as done (only if event came from queue)
        5. Manages idle state signaling

        Use this method when manually driving the event loop (e.g., in tests).
        For automatic processing, use emit() which queues events for the run loop.

        Args:
            event: Optional event to process directly (bypasses queue if provided)
            timeout: Handler execution timeout in seconds
            wait_for_timeout: How long to wait for next event from queue (default: 0.1s)

        Returns:
            The processed event, or None if queue was empty/shutdown

        Warning:
            Passing an event directly (bypassing the queue) is for advanced use only, be aware if:

            - **Event not in queue**: Works fine, handlers execute normally.
            - **Event already completed**: Handlers will run AGAIN, overwriting previous
              results. No guard against double-processing.
            - **Event in queue but not next**: Event processes immediately, but STAYS
              in queue. The run loop will process it again later (double-processing).

        See Also:
            emit: Queues an event for normal async processing by the bus's existing run loop (recommended)
            _process_event: Lower-level method that executes handlers (called by step)
        """
        assert self._on_idle and self.pending_event_queue, 'EventBus._start() must be called before step()'

        # Track if we got the event from the queue
        from_queue = False

        # Wait for next event with timeout to periodically check idle state
        if event is None:
            event = await self._get_next_event(wait_for_timeout=wait_for_timeout)
            from_queue = True
        if event is None:
            return None

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('🏃 %s.step(%s) STARTING', self, event)

        # Lifecycle note:
        # - Every `step()` transition starts in "not idle" state.
        # - The caller may be the runloop (`from_queue=True`) or a direct queue-jump
        #   call (`from_queue=False`) from `BaseEvent._process_self_on_all_buses()`.
        # - Idle can be restored only after queue bookkeeping and in-flight markers
        #   are both fully reconciled in `finally`.
        self._on_idle.clear()

        # Acquire the event lock selected by event/bus concurrency policy.
        self.processing_event_ids.add(event.event_id)
        try:
            async with self.locks._run_with_event_lock(self, event):  # pyright: ignore[reportPrivateUsage]
                # Process the event
                if event.event_status != EventStatus.COMPLETED:
                    await self._process_event(event, timeout=timeout)

                # Queue lifecycle:
                # - `queue.get()` increments `_unfinished_tasks`.
                # - We must call `task_done()` exactly once for that consume path.
                # - Direct `step(event=...)` calls bypass `queue.get()` and therefore
                #   must not call `task_done()`.
                if from_queue:
                    self.pending_event_queue.task_done()
        finally:
            await self._finalize_local_event_processing(event)
            # Idle lifecycle reconciliation:
            # - The runloop normally restores `_on_idle` after each queue turn.
            # - Direct `step(event=...)` calls have no subsequent runloop turn, so if
            #   this step drained the last in-flight work, `_on_idle` could remain
            #   permanently cleared and `wait_until_idle()` would block forever.
            # - Setting `_on_idle` here when queue+inflight are both empty keeps idle
            #   semantics identical for runloop and direct-step execution paths.
            if self._on_idle and self.pending_event_queue:
                if not self._has_inflight_events_fast() and self.pending_event_queue.qsize() == 0:
                    self._on_idle.set()

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('✅ %s.step(%s) COMPLETE', self, event)
        return event

    def _create_slow_event_warning_timer(
        self,
        event: BaseEvent[Any],
    ) -> Callable[[], Coroutine[Any, Any, None]] | None:
        event_slow_timeout = self._resolve_event_slow_timeout(event, self)
        if event_slow_timeout is None:
            return None
        return partial(self._slow_event_warning_monitor, event, event_slow_timeout)

    async def _mark_event_complete_if_ready(self, event: BaseEvent[Any]) -> None:
        was_complete = event.event_status == EventStatus.COMPLETED
        event._mark_completed(current_bus=self)  # pyright: ignore[reportPrivateUsage]
        just_completed = (not was_complete) and event.event_status == EventStatus.COMPLETED
        if just_completed:
            self._mark_event_complete_on_all_buses(event)
            await self.on_event_change(event, EventStatus.COMPLETED)

    async def _propagate_parent_completion(self, event: BaseEvent[Any]) -> None:
        current = event
        checked_ids: set[str] = set()

        while current.event_parent_id and current.event_parent_id not in checked_ids:
            checked_ids.add(current.event_parent_id)

            parent_event = None
            parent_bus: EventBus | None = None
            for bus in list(type(self).all_instances):
                if bus and current.event_parent_id in bus.event_history:
                    parent_event = bus.event_history[current.event_parent_id]
                    parent_bus = bus
                    break

            if not parent_event:
                break

            was_complete = parent_event.event_status == EventStatus.COMPLETED
            if not was_complete:
                parent_event._mark_completed(current_bus=parent_bus)  # pyright: ignore[reportPrivateUsage]
            just_completed = (not was_complete) and parent_event.event_status == EventStatus.COMPLETED
            if parent_bus and just_completed:
                self._mark_event_complete_on_all_buses(parent_event)
                await parent_bus.on_event_change(parent_event, EventStatus.COMPLETED)

            current = parent_event

    def _trim_event_history_if_needed(self) -> None:
        if (
            self.event_history.max_history_size is not None
            and self.event_history.max_history_size > 0
            and self.event_history.max_history_drop
            and len(self.event_history) > self.event_history.max_history_size
        ):
            self.event_history.trim_event_history(owner_label=str(self))
            # Sync removals to Rust core
            if self._rust_core is not None:
                removed = self._rust_core.trim_history(str(self))
                # Rust core and Python history are now in sync

    async def _process_event(self, event: BaseEvent[Any], timeout: float | None = None) -> None:
        """
        Execute all applicable handlers for an event (low-level, assumes lock is held).

        This is the core event handling method that:
        1. Finds all applicable handlers (type-specific + wildcard)
        2. Creates pending EventResult placeholders
        3. Executes handlers (serially or in parallel based on bus config)
        4. Marks the event as complete when all handlers finish
        5. Propagates completion status up the parent event chain
        6. Cleans up event history if over size limit

        IMPORTANT: This method assumes the caller already applied `locks._run_with_event_lock(...)`
        for the event execution.
        For safe external use, call step() instead which handles locking.

        Args:
            event: The event to handle
            timeout: Handler execution timeout in seconds (defaults to event.event_timeout)

        Warning:
            This is a low-level method with no safety guards. Behavior in edge cases:

            - **Event not in queue**: Works fine, handlers execute normally. This method
              does not interact with the queue at all.
            - **Event already completed**: Handlers run AGAIN, ``_create_pending_handler_results()``
              overwrites previous results. No guard against double-processing.
            - **Event in queue but not next**: Works fine for this call, but event stays
              in queue and will be processed again later by the run loop.
            - **Another event being processed (lock held elsewhere)**: If called without
              holding the lock, concurrent handler execution may cause race conditions.
              If called from within a handler (lock is re-entrant), causes nested processing.
            - **This exact event already being processed**: Recursive/re-entrant processing.
              Handlers run again while already running, results overwritten mid-execution.
              Likely to cause undefined behavior.

        See Also:
            step: High-level method that acquires lock and calls _process_event
            emit: Queues an event for async processing (recommended)
        """
        # Get applicable handlers
        applicable_handlers = self._get_handlers_for_event(event)
        slow_event_monitor_factory = self._create_slow_event_warning_timer(event)
        resolved_event_timeout = (
            timeout if timeout is not None else (event.event_timeout if event.event_timeout is not None else self.event_timeout)
        )

        await self.on_event_change(event, EventStatus.PENDING)

        # Execute handlers
        timeout_scope = asyncio.timeout(resolved_event_timeout)
        try:
            async with timeout_scope:
                async with _run_with_slow_monitor(
                    slow_event_monitor_factory,
                    task_name=f'{self}.slow_event_monitor({event})',
                ):
                    await event._run_handlers(  # pyright: ignore[reportPrivateUsage]
                        eventbus=self,
                        handlers=applicable_handlers,
                        timeout=resolved_event_timeout,
                    )
        except TimeoutError:
            if not timeout_scope.expired():
                raise
            assert resolved_event_timeout is not None
            await self._finalize_event_timeout(event, resolved_event_timeout)

        await self._mark_event_complete_if_ready(event)
        await self._propagate_parent_completion(event)
        self._trim_event_history_if_needed()

    def _get_handlers_for_event(self, event: BaseEvent[Any]) -> dict[PythonIdStr, EventHandler]:
        """Get all handlers that should process the given event, filtering out those that would create loops"""
        # Delegate handler matching and loop detection to Rust core when available
        if self._rust_core is not None:
            # Sync event metadata to Rust (status/path may have changed since emit)
            self._rust_core.update_event_status(event.event_id, str(event.event_status))
            self._rust_core.update_event_path(event.event_id, list(event.event_path))
            for handler_id, result in event.event_results.items():
                self._rust_core.update_result_status(event.event_id, handler_id, str(result.status))
            # Sync all ancestor result statuses for recursion depth checking
            current_pid = event.event_parent_id
            visited: set[str] = set()
            while current_pid and current_pid not in visited:
                visited.add(current_pid)
                parent_ev = self.event_history.get(current_pid)
                if parent_ev is None:
                    # Check other buses
                    for bus in list(type(self).all_instances):
                        if bus is not self and current_pid in bus.event_history:
                            parent_ev = bus.event_history[current_pid]
                            break
                if parent_ev is None:
                    break
                for hid, res in parent_ev.event_results.items():
                    self._rust_core.update_result_status(parent_ev.event_id, hid, str(res.status))
                current_pid = parent_ev.event_parent_id

            existing_ids = list(event.event_results.keys())
            existing_statuses = {hid: str(r.status) for hid, r in event.event_results.items()}
            handler_ids = self._rust_core.get_handlers_for_event(
                event.event_id, existing_ids, existing_statuses
            )
            filtered: dict[PythonIdStr, EventHandler] = {}
            for hid in handler_ids:
                entry = self.handlers.get(hid)
                if entry is not None:
                    # Still need Python-side _would_create_loop for inspect-based checks
                    # that Rust can't do (checking actual callable identity)
                    if not self._would_create_loop(event, entry):
                        filtered[hid] = entry
            return filtered

        # Pure-Python fallback
        applicable_handlers: list[EventHandler] = []

        for key in (event.event_type, '*'):
            indexed_ids = self.handlers_by_key.get(key, [])
            if not indexed_ids:
                continue
            for handler_id in indexed_ids:
                handler_entry = self.handlers.get(handler_id)
                if handler_entry:
                    applicable_handlers.append(handler_entry)

        filtered_handlers: dict[PythonIdStr, EventHandler] = {}
        for handler_entry in applicable_handlers:
            if self._would_create_loop(event, handler_entry):
                continue
            else:
                assert handler_entry.id is not None
                filtered_handlers[handler_entry.id] = handler_entry

        return filtered_handlers

    @contextmanager
    def _run_with_handler_dispatch_context(self, event: BaseEvent[T_EventResultType], handler_id: str):
        """Scope ContextVar state for one handler execution.

        This is the single handler execution context manager used by
        ``EventResult.run_handler(...)`` for both sync and async handlers. It sets
        current event/handler/bus ContextVars and mirrors event-lock ownership
        into copied dispatch contexts via ``locks._run_with_handler_dispatch_context(...)``.
        """
        event_token = EventBus.current_event_context.set(event)
        current_handler_token = EventBus.current_handler_id_context.set(handler_id)
        current_eventbus_token = EventBus.current_eventbus_context.set(self)
        try:
            with self.locks._run_with_handler_dispatch_context(self, event):  # pyright: ignore[reportPrivateUsage]
                yield
        finally:
            EventBus.current_event_context.reset(event_token)
            EventBus.current_handler_id_context.reset(current_handler_token)
            EventBus.current_eventbus_context.reset(current_eventbus_token)

    async def _finalize_event_timeout(self, event: BaseEvent[Any], timeout_seconds: float) -> None:
        """Finalize event-level hard timeout across pending/started handler results.

        - pending results become ``EventHandlerCancelledError``
        - started results become ``EventHandlerAbortedError``
        - child event processing is cancelled through event-level propagation
        """
        timeout_error = TimeoutError(
            f'Event {self.label}.on({event.event_type}#{event.event_id[-4:]}) timed out after {timeout_seconds}s'
        )
        event._cancel_pending_child_processing(timeout_error)  # pyright: ignore[reportPrivateUsage]
        for event_result in event.event_results.values():
            if event_result.status == 'pending':
                event_result.update(
                    error=EventHandlerCancelledError(
                        f'Cancelled: event {event.event_type}#{event.event_id[-4:]} timed out after {timeout_seconds}s'
                    )
                )
                await self.on_event_result_change(event, event_result, EventStatus.COMPLETED)
            elif event_result.status == 'started':
                event_result.update(
                    error=EventHandlerAbortedError(
                        f'Event handler {event_result.handler.label}({event}) was interrupted because the event timed out after {timeout_seconds}s'
                    )
                )
                await self.on_event_result_change(event, event_result, EventStatus.COMPLETED)

    async def _run_handler(
        self,
        event: 'BaseEvent[T_EventResultType]',
        handler_entry: EventHandler,
        timeout: float | None = None,
    ) -> T_EventResultType | BaseEvent[Any] | None:
        """Safely execute a single handler with middleware support and EventResult orchestration."""

        handler_id = handler_entry.id
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                ' ↳ %s._run_handler(%s, handler=%s#%s)',
                self,
                event,
                handler_entry.handler_name,
                handler_id[-4:],
            )

        resolved_timeout = self._resolve_handler_timeout(event, handler_entry, self, timeout_override=timeout)
        resolved_slow_timeout = self._resolve_handler_slow_timeout(event, handler_entry, self)

        if handler_id not in event.event_results:
            new_results = event._create_pending_handler_results(  # pyright: ignore[reportPrivateUsage]
                {handler_id: handler_entry}, eventbus=self, timeout=resolved_timeout
            )
            for pending_result in new_results.values():
                await self.on_event_result_change(event, pending_result, EventStatus.PENDING)

        first_handler_id = next(iter(event.event_results), None)
        event_result = event.event_results[handler_id]

        try:
            result_value = await event_result.run_handler(
                event,
                eventbus=self,
                timeout=resolved_timeout,
                handler_slow_timeout=resolved_slow_timeout,
                notify_event_started=(first_handler_id == handler_id),
                format_exception_for_log=log_filtered_traceback,
            )

            result_type_name = type(result_value).__name__ if result_value is not None else 'None'
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    '    ↳ Handler %s#%s returned: %s',
                    handler_entry.handler_name,
                    handler_id[-4:],
                    result_type_name,
                )

            await self.on_event_result_change(event, event_result, EventStatus.COMPLETED)
            return result_value

        except asyncio.CancelledError:
            await self.on_event_result_change(event, event_result, EventStatus.COMPLETED)
            raise
        except Exception:
            await self.on_event_result_change(event, event_result, EventStatus.COMPLETED)
            raise

    def _would_create_loop(self, event: BaseEvent[Any], handler_entry: EventHandler) -> bool:
        """Check if calling this handler would create a loop.

        Note: Forwarding loop prevention (bus A → B → C → A) is handled at emit()
        time by checking event.event_path, not here. This method only checks
        handler re-entrancy and recursion depth.
        """
        handler = handler_entry.handler
        if handler is None:
            return False

        # Check 1: if this handler already has an in-flight/completed result for this
        # event on this bus, avoid re-entrancy. Rehydrated events can legitimately contain
        # pending placeholders from a previous process; those must remain runnable.
        handler_id = handler_entry.id
        if handler_id in event.event_results:
            existing_result = event.event_results[handler_id]
            if existing_result.status == 'started':
                logger.debug(
                    f'⚠️ {self} handler {handler_entry.label}({event}) is already {existing_result.status} for event {event.event_id} (preventing recursive call)'
                )
                return True
            if existing_result.status == 'pending' and event.event_status == EventStatus.STARTED:
                logger.debug(
                    f'⚠️ {self} handler {handler_entry.label}({event}) is already pending while event is started for event {event.event_id} (preventing recursive call)'
                )
                return True
            if existing_result.status in ('completed', 'error') or existing_result.completed_at is not None:
                logger.debug(
                    f'⚠️ {self} handler {handler_entry.label}({event}) already completed @ {existing_result.completed_at} for event {event.event_id} (will not re-run)'
                )
                return True

        # Check 2: For non-forwarding handlers, check recursion depth.
        # Forwarding handlers are exempt since loop prevention is at emit() level.
        is_forwarding_handler = (
            inspect.ismethod(handler) and isinstance(handler.__self__, EventBus) and handler.__name__ in ('emit', 'dispatch')
        )

        if not is_forwarding_handler:
            recursion_depth = self._handler_dispatched_ancestor(event, handler_id)
            if recursion_depth > self.max_handler_recursion_depth:
                raise RuntimeError(
                    f'Infinite loop detected: Handler {handler_entry.label} '
                    f'has recursively processed {recursion_depth} levels of events. '
                    f'Current event: {event}, Handler: {handler_id}'
                )
            elif recursion_depth == self.max_handler_recursion_depth:
                logger.warning(
                    f'⚠️ {self} handler {handler_entry.label} '
                    f'at maximum recursion depth ({self.max_handler_recursion_depth} levels) - next level will raise exception'
                )

        return False

    def _handler_dispatched_ancestor(
        self, event: BaseEvent[Any], handler_id: str, visited: set[str] | None = None, depth: int = 0
    ) -> int:
        """Check how many times this handler appears in the ancestry chain. Returns the depth count."""
        # Delegate to Rust core when available (fast parent-chain walk)
        if self._rust_core is not None and self._rust_core.has_event(event.event_id):
            return self._rust_core.handler_recursion_depth(event.event_id, handler_id)

        # Pure-Python fallback
        if visited is None:
            visited = set()
        if event.event_id in visited:
            return depth
        visited.add(event.event_id)

        # If this event has no parent, it's a root event - no ancestry to check
        if not event.event_parent_id:
            return depth

        # Find parent event in any bus's history
        parent_event = None
        # Create a list copy to avoid "Set changed size during iteration" error
        for bus in list(type(self).all_instances):
            if event.event_parent_id in bus.event_history:
                parent_event = bus.event_history[event.event_parent_id]
                break

        if not parent_event:
            return depth

        # Check if this handler processed the parent event
        if handler_id in parent_event.event_results:
            result = parent_event.event_results[handler_id]
            if result.status in ('pending', 'started', 'completed'):
                # This handler processed the parent event, increment depth
                depth += 1

        # Recursively check the parent's ancestry
        return self._handler_dispatched_ancestor(parent_event, handler_id, visited, depth)

    def log_tree(self) -> str:
        """Print a nice pretty formatted tree view of all events in the history including their results and child events recursively"""
        from abxbus.logging import log_eventbus_tree

        return log_eventbus_tree(self)

    def _check_total_memory_usage(self) -> None:
        """Check total memory usage across all EventBus instances and warn if >50MB"""
        import sys

        total_bytes = 0
        bus_details: list[tuple[str, int, int, int]] = []

        # Iterate through all EventBus instances
        # Create a list copy to avoid "Set changed size during iteration" error
        for bus in list(type(self).all_instances):
            try:
                bus_bytes = 0

                # Count events in history
                for event in bus.event_history.values():
                    bus_bytes += sys.getsizeof(event)
                    # Also count the event's data
                    for attr_value in event.__dict__.values():
                        if isinstance(attr_value, (str, bytes, list, dict)):
                            bus_bytes += sys.getsizeof(_as_any(attr_value))

                # Count events in queue
                if bus.pending_event_queue:
                    for event in bus.pending_event_queue.iter_items():
                        bus_bytes += sys.getsizeof(event)
                        for attr_value in event.__dict__.values():
                            if isinstance(attr_value, (str, bytes, list, dict)):
                                bus_bytes += sys.getsizeof(_as_any(attr_value))

                total_bytes += bus_bytes
                bus_details.append(
                    (
                        bus.label,
                        bus_bytes,
                        len(bus.event_history),
                        bus.pending_event_queue.qsize() if bus.pending_event_queue else 0,
                    )
                )
            except Exception:
                # Skip buses that can't be measured
                continue

        total_mb = total_bytes / (1024 * 1024)

        if total_mb > 50:
            # Build detailed breakdown
            details: list[str] = []
            for name, bytes_used, history_size, queue_size in sorted(bus_details, key=lambda x: x[1], reverse=True):
                mb = bytes_used / (1024 * 1024)
                if mb > 0.1:  # Only show buses using >0.1MB
                    details.append(f'  - {name}: {mb:.1f}MB (history={history_size}, queue={queue_size})')

            warning_msg = (
                f'\n⚠️  WARNING: Total EventBus memory usage is {total_mb:.1f}MB (>50MB limit)\n'
                f'Active EventBus instances: {len(type(self).all_instances)}\n'
            )
            if details:
                warning_msg += 'Memory breakdown:\n' + '\n'.join(details[:5])  # Show top 5
                if len(details) > 5:
                    warning_msg += f'\n  ... and {len(details) - 5} more'

            warning_msg += '\nConsider:\n'
            warning_msg += '  - Reducing max_history_size\n'
            warning_msg += '  - Clearing completed EventBus instances with stop(clear=True)\n'
            warning_msg += '  - Reducing event payload sizes\n'

            logger.warning(warning_msg)
