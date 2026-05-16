"""Core-backed EventBus wrapper.

This module intentionally does not implement locks, queues, scheduling, or
completion rules. It keeps only Python handler callables and delegates runtime
state transitions to the Rust core protocol.
"""

# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportUnknownLambdaType=false, reportPrivateUsage=false, reportOptionalMemberAccess=false, reportReturnType=false, reportUnnecessaryComparison=false, reportUnnecessaryCast=false, reportUnnecessaryIsInstance=false, reportRedeclaration=false, reportAssignmentType=false, reportArgumentType=false, reportUnusedVariable=false

from __future__ import annotations

import asyncio
import contextvars
import inspect
import json
import logging
import math
import threading
import time
import warnings
import weakref
from collections.abc import Awaitable, Callable, Coroutine
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, TypedDict, TypeVar, cast, overload
from uuid import NAMESPACE_DNS, uuid5

from .async_context import ActiveAbortContext, reset_active_abort_context, set_active_abort_context
from .base_event import BaseEvent, EventResult, EventStatus
from .core_client import RustCoreClient
from .event_handler import (
    ContravariantEventHandlerCallable,
    EventHandler,
    EventHandlerAbortedError,
    EventHandlerCancelledError,
    EventHandlerResultSchemaError,
    EventHandlerTimeoutError,
)
from .event_history import EventHistory
from .helpers import CleanShutdownQueue, monotonic_datetime
from .jsonschema import pydantic_model_from_json_schema, pydantic_model_to_json_schema
from .lock_manager import LockManager, ReentrantLock

CoreHandler = Callable[[Any], Any]
T_Event = TypeVar('T_Event', bound=BaseEvent[Any])
EventConcurrency = Literal['global-serial', 'bus-serial', 'parallel']
HandlerConcurrency = Literal['serial', 'parallel']
HandlerCompletion = Literal['all', 'first']

logger = logging.getLogger('abxbus')


class CoreOutcomeBatch(TypedDict):
    outcomes: list[dict[str, Any]]
    route_id: str | None


_core_outcome_batch_context: contextvars.ContextVar[CoreOutcomeBatch | None] = contextvars.ContextVar(
    'abxbus_core_outcome_batch',
    default=None,
)


def _option_value(value: Any) -> Any:
    return getattr(value, 'value', value)


class CoreBusDefaults(TypedDict):
    event_concurrency: EventConcurrency
    event_handler_concurrency: HandlerConcurrency
    event_handler_completion: HandlerCompletion
    event_timeout: float | None
    event_slow_timeout: float | None
    event_handler_timeout: float | None
    event_handler_slow_timeout: float | None


class _CompletedAwaitable:
    def __init__(
        self,
        tasks: list[asyncio.Future[Any]] | None = None,
        cleanup: Callable[[], None] | None = None,
    ) -> None:
        self.tasks = tasks or []
        self.cleanup = cleanup

    def __await__(self) -> Any:
        async def wait_for_cancelled_tasks() -> None:
            if self.tasks:
                await asyncio.gather(*self.tasks, return_exceptions=True)
            if self.cleanup is not None:
                self.cleanup()

        return wait_for_cancelled_tasks().__await__()


class _HandlerRegistry(dict[str, EventHandler]):
    def __init__(self, owner: RustCoreEventBus) -> None:
        super().__init__()
        self._owner_ref = weakref.ref(owner)

    def __delitem__(self, key: str) -> None:
        super().__delitem__(key)
        owner = self._owner_ref()
        if owner is not None:
            owner._unregister_handler_from_core(key)

    def clear(self) -> None:
        handler_ids = list(self.keys())
        super().clear()
        owner = self._owner_ref()
        if owner is not None:
            for handler_id in handler_ids:
                owner._unregister_handler_from_core(handler_id)

    def pop(self, key: str, default: Any = None) -> Any:
        if key not in self:
            return default
        value = super().pop(key)
        owner = self._owner_ref()
        if owner is not None:
            owner._unregister_handler_from_core(key)
        return value

    def popitem(self) -> tuple[str, EventHandler]:
        key, value = super().popitem()
        owner = self._owner_ref()
        if owner is not None:
            owner._unregister_handler_from_core(key)
        return key, value


class RustCoreEventBus:
    _instances: weakref.WeakSet[RustCoreEventBus] = weakref.WeakSet()
    _event_global_serial_lock: ReentrantLock = ReentrantLock()
    _INLINE_SYNC_INVOCATION_THRESHOLD = 64
    _CORE_EMIT_BATCH_SIZE = 512
    _CORE_ROUTE_SLICE_LIMIT = 65_536

    event_concurrency: EventConcurrency
    event_handler_concurrency: HandlerConcurrency
    event_handler_completion: HandlerCompletion
    event_timeout: float | None
    event_slow_timeout: float | None
    event_handler_timeout: float | None
    event_handler_slow_timeout: float | None

    def __init__(
        self,
        name: str = 'EventBus',
        *,
        id: str | None = None,
        core: RustCoreClient | None = None,
        event_concurrency: EventConcurrency = 'bus-serial',
        event_handler_concurrency: HandlerConcurrency = 'serial',
        event_handler_completion: HandlerCompletion = 'all',
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
        middlewares: list[Any] | None = None,
        background_worker: bool = False,
    ) -> None:
        self._uses_shared_core = core is None
        self._owns_core_transport = False
        self.core = core or RustCoreClient.acquire_named(name)
        self.name = name
        self.bus_id = id or stable_core_bus_id(name)
        self.label = f'{name}#{self.bus_id[-4:]}'
        self.event_concurrency = cast(EventConcurrency, _option_value(event_concurrency))
        self.event_handler_concurrency = cast(HandlerConcurrency, _option_value(event_handler_concurrency))
        self.event_handler_completion = cast(HandlerCompletion, _option_value(event_handler_completion))
        self.event_handler_detect_file_paths = event_handler_detect_file_paths
        self.local_immediate_handlers = local_immediate_handlers
        self.local_immediate_skip_core_handlers = local_immediate_skip_core_handlers
        self.local_immediate_child_events = local_immediate_child_events
        self.event_timeout = event_timeout
        self.event_slow_timeout = event_slow_timeout
        self.event_handler_timeout = event_handler_timeout
        self.event_handler_slow_timeout = event_handler_slow_timeout
        self.id = self.bus_id
        self.locks = LockManager()
        if not hasattr(type(self), '_event_global_serial_lock'):
            type(self)._event_global_serial_lock = ReentrantLock()
        self.event_global_serial_lock = type(self)._event_global_serial_lock
        self.event_bus_serial_lock = ReentrantLock()
        self._handlers: _HandlerRegistry = _HandlerRegistry(self)
        self._handler_core_options: dict[str, dict[str, HandlerConcurrency | HandlerCompletion | None]] = {}
        self._registered_core_handler_ids: set[str] = set()
        self._registered_core_handler_records: dict[str, dict[str, Any]] = {}
        self._core_finalizer = weakref.finalize(
            self,
            type(self)._finalize_core_client,
            self.core,
            self._registered_core_handler_ids,
            self._uses_shared_core,
            self._owns_core_transport,
        )
        self.handlers_by_key: dict[str, list[str]] = {}
        self._handler_names_by_key: dict[str, set[str]] = {}
        self._events: dict[str, BaseEvent[Any]] = {}
        self._event_ids_without_results: set[str] = set()
        self.event_history = EventHistory[BaseEvent[Any]](max_history_size=max_history_size, max_history_drop=max_history_drop)
        self.pending_event_queue: CleanShutdownQueue[BaseEvent[Any]] = CleanShutdownQueue(maxsize=0)
        self.middlewares = [middleware() if isinstance(middleware, type) else middleware for middleware in (middlewares or [])]
        self._middleware_event_statuses: set[tuple[str, str]] = set()
        self._middleware_result_statuses: set[tuple[str, str, str]] = set()
        self._event_types_by_pattern: dict[str, str | type[BaseEvent[Any]]] = {}
        self._closed = False
        self._destroyed = False
        self._is_running = False
        self._runloop_task: asyncio.Task[Any] | None = None
        self._on_idle = asyncio.Event()
        self._on_idle.set()
        self._worker_thread: threading.Thread | None = None
        self._worker_ready = threading.Event()
        self._background_worker = background_worker
        self._processing_tasks: set[asyncio.Task[Any]] = set()
        self._local_invocation_tasks: set[asyncio.Task[Any]] = set()
        self._middleware_hook_tasks: set[asyncio.Future[Any]] = set()
        self._outcome_commit_tasks_by_event_id: dict[str, set[asyncio.Task[None]]] = {}
        self._pending_core_emits: list[dict[str, Any]] = []
        self._scheduled_core_messages: list[dict[str, Any]] = []
        self._scheduled_core_messages_task: asyncio.Task[None] | None = None
        self._background_drive_task: asyncio.Task[None] | None = None
        self._background_drive_target_event_id: str | None = None
        self._processing_event_ids: set[str] = set()
        self._drain_pending_on_await_event_ids: set[str] = set()
        self._pending_event_ids: set[str] = set()
        self._queue_jump_event_ids: set[str] = set()
        self._queue_jump_parent_invocation_by_event_id: dict[str, str] = {}
        self._explicit_await_event_ids: set[str] = set()
        self._hydrating_completed_event_ids: set[str] = set()
        self._locally_completed_without_core_patch: set[str] = set()
        self._local_only_event_ids: set[str] = set()
        self._event_slow_warning_timers: dict[str, asyncio.TimerHandle | asyncio.Task[None] | threading.Timer] = {}
        self._async_handler_slow_warning_entries: dict[tuple[str, str], tuple[float, BaseEvent[Any], EventHandler]] = {}
        self._async_handler_slow_warning_timer: asyncio.TimerHandle | None = None
        self._async_handler_slow_warning_deadline: float | None = None
        self._async_default_handler_slow_warning_timer: asyncio.TimerHandle | None = None
        self._async_default_handler_slow_warning_logged: set[tuple[str, str]] = set()
        self._async_default_handler_slow_warning_running = 0
        self._find_waiters: list[tuple[Callable[[BaseEvent[Any]], bool], asyncio.Future[BaseEvent[Any] | None]]] = []
        self._suppress_auto_driver = 0
        self._suppress_post_outcome_available_processing = 0
        self._passive_handler_wait_depth = 0
        self.in_flight_event_ids: set[str] = set()
        self._invocation_by_result_id: dict[str, str] = {}
        self._event_id_by_result_id: dict[str, str] = {}
        self._child_event_ids_by_result_id: dict[str, set[str]] = {}
        self._child_event_ids_by_parent_handler: dict[tuple[str, str], set[str]] = {}
        self._registered_max_history_size: int | None | object = object()
        self._registered_max_history_drop: bool | object = object()
        self._register_bus_to_core()
        type(self)._instances.add(self)

    @staticmethod
    def _finalize_core_client(
        core: RustCoreClient,
        handler_ids: set[str],
        uses_shared_core: bool,
        owns_core_transport: bool,
    ) -> None:
        handler_ids.clear()
        try:
            if uses_shared_core:
                RustCoreClient.release_named_from_finalizer(core)
            elif owns_core_transport:
                core.abandon(terminate_process=True)
        except Exception:
            pass

    def defaults_record(self) -> CoreBusDefaults:
        return {
            'event_concurrency': self.event_concurrency,
            'event_handler_concurrency': self.event_handler_concurrency,
            'event_handler_completion': self.event_handler_completion,
            'event_timeout': self.event_timeout,
            'event_slow_timeout': self.event_slow_timeout,
            'event_handler_timeout': self.event_handler_timeout,
            'event_handler_slow_timeout': self.event_handler_slow_timeout,
        }

    def _bus_record(self) -> dict[str, Any]:
        return {
            'bus_id': self.bus_id,
            'name': self.name,
            'label': self.label,
            'host_id': self.core.session_id,
            'defaults': self.defaults_record(),
            'max_history_size': self.event_history.max_history_size,
            'max_history_drop': self.event_history.max_history_drop,
        }

    def _register_bus_to_core(self) -> None:
        self.core.register_bus(self._bus_record())
        self._registered_max_history_size = self.event_history.max_history_size
        self._registered_max_history_drop = self.event_history.max_history_drop

    def _sync_bus_history_policy(self) -> None:
        if (
            self._registered_max_history_size == self.event_history.max_history_size
            and self._registered_max_history_drop == self.event_history.max_history_drop
        ):
            return
        self._register_bus_to_core()

    def _release_core_transport_when_idle(self) -> None:
        if any(not bus._closed and bus.core is self.core and bus.in_flight_event_ids for bus in list(type(self)._instances)):
            return
        self.core.release_transport_soon()

    def _on_history_remove(self, event: BaseEvent[Any]) -> None:
        if event.event_status != EventStatus.COMPLETED:
            return
        self._events.pop(event.event_id, None)
        self._event_ids_without_results.discard(event.event_id)
        self.in_flight_event_ids.discard(event.event_id)
        self._drain_pending_on_await_event_ids.discard(event.event_id)
        self._queue_jump_event_ids.discard(event.event_id)
        self._queue_jump_parent_invocation_by_event_id.pop(event.event_id, None)
        self._local_only_event_ids.discard(event.event_id)
        self._cancel_event_slow_warning_timer(event.event_id)

    def _forget_completed_event_if_unretained(self, event: BaseEvent[Any]) -> None:
        max_size = self.event_history.max_history_size
        if event.event_status != EventStatus.COMPLETED or max_size is None:
            return
        if max_size == 0:
            self.event_history.pop(event.event_id, None)
            self._on_history_remove(event)
            return
        if self.event_history.max_history_drop and event.event_id not in self.event_history:
            self._on_history_remove(event)

    def _index_event_relationships(self, event: BaseEvent[Any]) -> None:
        if event.event_emitted_by_result_id is not None:
            self._child_event_ids_by_result_id.setdefault(event.event_emitted_by_result_id, set()).add(event.event_id)
        if event.event_parent_id is not None and event.event_emitted_by_handler_id is not None:
            key = (event.event_parent_id, event.event_emitted_by_handler_id)
            self._child_event_ids_by_parent_handler.setdefault(key, set()).add(event.event_id)
            parent = self._events.get(event.event_parent_id) or self.event_history.get(event.event_parent_id)
            if parent is not None:
                for result in parent.event_results.values():
                    if result.handler_id != event.event_emitted_by_handler_id and result.id != event.event_emitted_by_result_id:
                        continue
                    self._attach_child_to_result(parent, result, event)
                self._merge_referenced_event_instance(parent)

    def _trim_event_history_if_needed(self, *, force: bool = False, strict: bool = False) -> None:
        max_size = self.event_history.max_history_size
        if max_size is None:
            return
        if max_size == 0:
            for event_id, event in list(self.event_history.items()):
                if event.event_status == EventStatus.COMPLETED:
                    self.event_history.pop(event_id, None)
                    self._on_history_remove(event)
            return
        if not self.event_history.max_history_drop:
            return
        soft_limit = max(max_size, int(max_size * 1.2))
        limit = max_size if strict else soft_limit
        if len(self.event_history) > limit:
            self.event_history.trim_event_history(on_remove=self._on_history_remove, owner_label=str(self))

    def _mark_idle(self) -> None:
        self._trim_event_history_if_needed(force=True, strict=True)
        on_idle = self._on_idle
        if on_idle is not None:
            on_idle.set()

    def _event_slow_timeout_for_event(self, event: BaseEvent[Any]) -> float | None:
        raw_slow_timeout = _option_value(event.event_slow_timeout)
        if raw_slow_timeout is None:
            raw_slow_timeout = _option_value(self.event_slow_timeout)
        slow_timeout = self._positive_timeout_option(raw_slow_timeout)
        if slow_timeout is None:
            return None
        event_timeout = self._number_option(event.event_timeout)
        if event_timeout is None:
            event_timeout = self._number_option(self.event_timeout)
        if event_timeout is not None and event_timeout <= slow_timeout:
            return None
        return slow_timeout

    def _log_running_event_slow_warning(self, event: BaseEvent[Any]) -> None:
        if event.event_status == EventStatus.COMPLETED:
            return
        running_handler_count = sum(1 for result in event.event_results.values() if result.status == 'started')
        started_at = event.event_started_at or event.event_created_at
        elapsed_seconds = self._elapsed_since_timestamp(started_at)
        logger.warning(
            '⚠️ Slow event processing: %s.on(%s#%s, %s handlers) still running after %.2fs',
            self.name,
            event.event_type,
            event.event_id[-4:],
            running_handler_count,
            elapsed_seconds,
        )

    def _start_event_slow_warning_timer(self, event: BaseEvent[Any]) -> None:
        self._cancel_event_slow_warning_timer(event.event_id)
        slow_timeout = self._event_slow_timeout_for_event(event)
        if slow_timeout is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            timer = threading.Timer(slow_timeout, self._log_running_event_slow_warning, args=(event,))
            timer.daemon = True
            timer.start()
            self._event_slow_warning_timers[event.event_id] = timer
            return

        self._event_slow_warning_timers[event.event_id] = loop.call_later(
            slow_timeout,
            self._log_running_event_slow_warning,
            event,
        )

    def _cancel_event_slow_warning_timer(self, event_id: str) -> None:
        timer = self._event_slow_warning_timers.pop(event_id, None)
        if isinstance(timer, asyncio.Task):
            timer.cancel()
        elif timer is not None:
            timer.cancel()

    @property
    def handlers(self) -> dict[str, EventHandler]:
        return self._handlers

    @property
    def events_pending(self) -> list[BaseEvent[Any]]:
        return [event for event in self.event_history.values() if event.event_status == EventStatus.PENDING]

    @property
    def events_started(self) -> list[BaseEvent[Any]]:
        return [event for event in self.event_history.values() if event.event_status == EventStatus.STARTED]

    @property
    def events_completed(self) -> list[BaseEvent[Any]]:
        return [event for event in self.event_history.values() if event.event_status == EventStatus.COMPLETED]

    @property
    def processing_event_ids(self) -> set[str]:
        return set(self._processing_event_ids)

    @property
    def find_waiters(self) -> list[tuple[Callable[[BaseEvent[Any]], bool], asyncio.Future[BaseEvent[Any] | None]]]:
        return self._find_waiters

    @overload
    def on(
        self,
        event_type: type[T_Event],
        handler: ContravariantEventHandlerCallable[T_Event],
        *,
        handler_name: str | None = None,
        handler_timeout: float | None = None,
        handler_slow_timeout: float | None = None,
        handler_concurrency: HandlerConcurrency | None = None,
        handler_completion: HandlerCompletion | None = None,
    ) -> EventHandler: ...

    @overload
    def on(
        self,
        event_type: str,
        handler: ContravariantEventHandlerCallable[Any],
        *,
        handler_name: str | None = None,
        handler_timeout: float | None = None,
        handler_slow_timeout: float | None = None,
        handler_concurrency: HandlerConcurrency | None = None,
        handler_completion: HandlerCompletion | None = None,
    ) -> EventHandler: ...

    def on(
        self,
        event_type: str | type[BaseEvent[Any]],
        handler: ContravariantEventHandlerCallable[Any],
        *,
        handler_name: str | None = None,
        handler_timeout: float | None = None,
        handler_slow_timeout: float | None = None,
        handler_concurrency: HandlerConcurrency | None = None,
        handler_completion: HandlerCompletion | None = None,
    ) -> EventHandler:
        self._raise_if_destroyed()
        event_pattern = EventHistory.normalize_event_pattern(event_type)
        self._event_types_by_pattern[event_pattern] = event_type
        handler_entry = EventHandler.from_callable(
            handler=handler,
            event_pattern=event_pattern,
            eventbus_name=self.name,
            eventbus_id=self.bus_id,
            detect_handler_file_path=self.event_handler_detect_file_paths,
            handler_timeout=handler_timeout,
            handler_slow_timeout=handler_slow_timeout,
        )
        if handler_name is not None:
            handler_entry.handler_name = handler_name
            handler_entry.id = handler_entry.compute_handler_id()
        if getattr(self, 'warn_on_duplicate_handler_names', True):
            if handler_entry.handler_name in self._handler_names_by_key.get(event_pattern, set()):
                warnings.warn(
                    f'Event handler {handler_entry.handler_name} already registered for {event_pattern}',
                    UserWarning,
                    stacklevel=2,
                )
        handler_id = handler_entry.id
        self._handler_core_options[handler_id] = {
            'handler_concurrency': handler_concurrency,
            'handler_completion': handler_completion,
        }
        handler_entry._mutation_callback = self._sync_handler_to_core
        dict.__setitem__(self._handlers, handler_id, handler_entry)
        self.handlers_by_key.setdefault(event_pattern, []).append(handler_id)
        self._handler_names_by_key.setdefault(event_pattern, set()).add(handler_entry.handler_name)
        self._sync_handler_to_core(handler_entry)
        self._emit_bus_handlers_change(handler_entry, True)
        if self._background_worker:
            self._start_worker()
        return handler_entry

    def _emit_bus_handlers_change(self, handler: EventHandler, registered: bool) -> None:
        if not self.middlewares:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self.on_bus_handlers_change(handler, registered))
            return
        if loop.is_running():
            task = asyncio.create_task(self.on_bus_handlers_change(handler, registered))
            self._processing_tasks.add(task)
            task.add_done_callback(self._processing_tasks.discard)
        else:
            loop.run_until_complete(self.on_bus_handlers_change(handler, registered))

    def _handler_core_record(self, handler: EventHandler) -> dict[str, Any]:
        options = self._handler_core_options.get(handler.id, {})
        return {
            'handler_id': handler.id,
            'bus_id': self.bus_id,
            'host_id': self.core.session_id,
            'event_pattern': handler.event_pattern,
            'handler_name': handler.handler_name,
            'handler_file_path': handler.handler_file_path,
            'handler_registered_at': handler.handler_registered_at,
            'handler_timeout': handler.handler_timeout,
            'handler_slow_timeout': handler.handler_slow_timeout,
            'handler_concurrency': options.get('handler_concurrency'),
            'handler_completion': options.get('handler_completion'),
        }

    def _sync_handler_to_core(self, handler: EventHandler) -> None:
        if self.local_immediate_skip_core_handlers and (
            self._handler_can_run_local_immediate(handler)
            or (
                self.local_immediate_handlers
                and not self.middlewares
                and not self._background_worker
                and self._handler_can_run_local_async(handler)
            )
        ):
            self._registered_core_handler_ids.discard(handler.id)
            self._registered_core_handler_records.pop(handler.id, None)
            return
        handler_record = self._handler_core_record(handler)
        if self._registered_core_handler_records.get(handler.id) == handler_record:
            return
        self.core.register_handler(handler_record)
        self._registered_core_handler_ids.add(handler.id)
        self._registered_core_handler_records[handler.id] = dict(handler_record)
        if self._has_events_replayable_for_new_handler():
            self._restart_pending_events_for_handler(handler)

    @staticmethod
    def _handler_can_run_local_immediate(handler: EventHandler) -> bool:
        callable_handler = handler.handler
        if callable_handler is None:
            return False
        if inspect.iscoroutinefunction(callable_handler):
            return False
        if isinstance(getattr(callable_handler, '__self__', None), RustCoreEventBus):
            return False
        return True

    @staticmethod
    def _handler_can_run_local_async(handler: EventHandler) -> bool:
        callable_handler = handler.handler
        if callable_handler is None:
            return False
        if isinstance(getattr(callable_handler, '__self__', None), RustCoreEventBus):
            return False
        return inspect.iscoroutinefunction(callable_handler)

    def _unregister_handler_from_core(self, handler_id: str) -> None:
        self._handler_core_options.pop(handler_id, None)
        for event_pattern, handler_ids in list(self.handlers_by_key.items()):
            retained = [candidate for candidate in handler_ids if candidate != handler_id]
            if retained:
                self.handlers_by_key[event_pattern] = retained
            else:
                self.handlers_by_key.pop(event_pattern, None)
        if handler_id in self._registered_core_handler_ids:
            self.core.unregister_handler(handler_id)
        self._registered_core_handler_ids.discard(handler_id)
        self._registered_core_handler_records.pop(handler_id, None)

    def _sync_handler_removals_to_core(self) -> None:
        active_handler_ids = set(self._handlers)
        for event_pattern, handler_ids in list(self.handlers_by_key.items()):
            retained = [handler_id for handler_id in handler_ids if handler_id in active_handler_ids]
            if retained:
                self.handlers_by_key[event_pattern] = retained
            else:
                self.handlers_by_key.pop(event_pattern, None)
        self._rebuild_handler_name_index()
        for handler_id in list(self._registered_core_handler_ids - active_handler_ids):
            self._unregister_handler_from_core(handler_id)

    def _remove_handler_name_from_key(self, event_pattern: str, handler_name: str) -> None:
        names = self._handler_names_by_key.get(event_pattern)
        if not names or handler_name not in names:
            return
        if any(
            (entry := self._handlers.get(handler_id)) is not None and entry.handler_name == handler_name
            for handler_id in self.handlers_by_key.get(event_pattern, [])
        ):
            return
        names.discard(handler_name)
        if not names:
            self._handler_names_by_key.pop(event_pattern, None)

    def _rebuild_handler_name_index(self) -> None:
        self._handler_names_by_key.clear()
        for event_pattern, handler_ids in self.handlers_by_key.items():
            names = self._handler_names_by_key.setdefault(event_pattern, set())
            for handler_id in handler_ids:
                entry = self._handlers.get(handler_id)
                if entry is not None:
                    names.add(entry.handler_name)
            if not names:
                self._handler_names_by_key.pop(event_pattern, None)

    def _can_batch_core_emit(
        self,
        event_obj: BaseEvent[Any],
        *,
        active_handler_id: str | None,
        forwarding_existing_event: bool,
        parent_invocation_id: str | None,
        explicit_active_child_emit: bool,
    ) -> bool:
        return (
            not forwarding_existing_event
            and self._can_schedule_async_processing()
            and not self._background_worker
            and self._suppress_auto_driver <= 0
            and active_handler_id is None
            and parent_invocation_id is None
            and not explicit_active_child_emit
            and event_obj.event_parent_id is None
            and event_obj.event_emitted_by_result_id is None
            and event_obj.event_emitted_by_handler_id is None
        )

    def _can_run_local_immediate_handlers(
        self,
        event_obj: BaseEvent[Any],
        *,
        active_bus: RustCoreEventBus | None,
        active_event: BaseEvent[Any] | None,
        active_handler_id: str | None,
        forwarding_existing_event: bool,
        had_existing_pending: bool,
        parent_invocation_id: str | None,
        explicit_active_child_emit: bool,
    ) -> bool:
        if not self.local_immediate_handlers:
            return False
        if self.middlewares or self._background_worker:
            return False
        completion = _option_value(event_obj.event_handler_completion) or self.event_handler_completion
        if completion != 'all':
            return False
        handlers = self._matching_local_handlers_for_event(event_obj)
        child_fast_path = (
            self.local_immediate_child_events
            and explicit_active_child_emit
            and active_bus is self
            and active_event is not None
            and event_obj.event_parent_id == active_event.event_id
            and event_obj.event_emitted_by_result_id is not None
        )
        no_handler_child_fast_path = (
            not handlers
            and explicit_active_child_emit
            and active_event is not None
            and event_obj.event_parent_id == active_event.event_id
            and event_obj.event_emitted_by_result_id is not None
            and not forwarding_existing_event
            and parent_invocation_id is None
        )
        top_level_fast_path = (
            active_handler_id is None
            and not forwarding_existing_event
            and not had_existing_pending
            and parent_invocation_id is None
            and not explicit_active_child_emit
            and event_obj.event_parent_id is None
            and event_obj.event_emitted_by_result_id is None
            and event_obj.event_emitted_by_handler_id is None
        )
        if child_fast_path:
            if forwarding_existing_event or parent_invocation_id is not None:
                return False
        elif no_handler_child_fast_path:
            pass
        else:
            if (
                active_handler_id is not None
                or forwarding_existing_event
                or had_existing_pending
                or parent_invocation_id is not None
                or explicit_active_child_emit
                or event_obj.event_parent_id is not None
                or event_obj.event_emitted_by_result_id is not None
                or event_obj.event_emitted_by_handler_id is not None
            ):
                return False
        if not (child_fast_path or no_handler_child_fast_path) and not top_level_fast_path:
            return False
        active_processing_tasks = {
            task for task in self._processing_tasks if task is not asyncio.current_task() and not task.done()
        }
        active_invocation_tasks = {
            task for task in self._local_invocation_tasks if task is not asyncio.current_task() and not task.done()
        }
        if self._pending_core_emits:
            return False
        if not (child_fast_path or no_handler_child_fast_path) and (active_processing_tasks or active_invocation_tasks):
            return False
        if not (child_fast_path or no_handler_child_fast_path) and self.in_flight_event_ids:
            return False
        if any(tasks for tasks in self._outcome_commit_tasks_by_event_id.values()):
            return False
        if handlers and not (child_fast_path or top_level_fast_path):
            for bus in list(type(self)._instances):
                if bus is self or bus._closed:
                    continue
                if bus.core is self.core:
                    return False
        for handler in handlers:
            if not self._handler_can_run_local_immediate(handler):
                return False
        return True

    def _run_local_immediate_handlers(self, event: BaseEvent[Any]) -> bool:
        handlers = self._matching_local_handlers_for_event(event)
        self._resolve_find_waiters(event)
        if not handlers:
            event._mark_completed(current_bus=cast(Any, self))  # pyright: ignore[reportPrivateUsage]
            self._track_event_replayability_for_new_handlers(event)
            self._mark_idle()
            return True

        for handler in handlers:
            result = event.event_results.get(handler.id)
            if result is None:
                result = self._create_pending_result_fast(event, handler)
            self._event_id_by_result_id[result.id] = event.event_id
            result.update(status='started')
            event._mark_started(result.started_at)  # pyright: ignore[reportPrivateUsage]
            try:
                with self._run_with_handler_dispatch_context(event, handler.id):
                    value = handler(event)
                if inspect.isawaitable(value) and not isinstance(value, BaseEvent):
                    raise RuntimeError(
                        'local_immediate_handlers only supports synchronous handlers; '
                        f'{handler.handler_name} returned an awaitable'
                    )
                result.update(result=value)
            except BaseException as exc:
                result.update(error=exc)
        self._track_event_replayability_for_new_handlers(event)
        self._complete_event_if_local_results_terminal(event)
        if event.event_status == EventStatus.COMPLETED:
            self._mark_idle()
            return True
        return False

    def _can_run_local_async_handlers(
        self,
        event_obj: BaseEvent[Any],
        *,
        active_bus: RustCoreEventBus | None,
        active_event: BaseEvent[Any] | None,
        active_handler_id: str | None,
        forwarding_existing_event: bool,
        had_existing_pending: bool,
        parent_invocation_id: str | None,
        explicit_active_child_emit: bool,
    ) -> bool:
        if not self.local_immediate_handlers:
            return False
        if self.middlewares or self._background_worker:
            return False
        if forwarding_existing_event or parent_invocation_id is not None:
            return False
        if self._pending_core_emits or any(tasks for tasks in self._outcome_commit_tasks_by_event_id.values()):
            return False
        if not self._can_schedule_async_processing():
            return False
        if (_option_value(event_obj.event_handler_completion) or self.event_handler_completion) != 'all':
            return False
        if (_option_value(event_obj.event_handler_concurrency) or self.event_handler_concurrency) != 'serial':
            return False
        handlers = self._matching_local_handlers_for_event(event_obj)
        if not handlers or not any(self._handler_can_run_local_async(handler) for handler in handlers):
            return False
        if not all(
            self._handler_can_run_local_async(handler) or self._handler_can_run_local_immediate(handler) for handler in handlers
        ):
            return False

        top_level_fast_path = (
            active_handler_id is None
            and active_bus is None
            and active_event is None
            and not had_existing_pending
            and not explicit_active_child_emit
            and event_obj.event_parent_id is None
            and event_obj.event_emitted_by_result_id is None
            and event_obj.event_emitted_by_handler_id is None
            and not self.in_flight_event_ids
            and (self.local_immediate_skip_core_handlers or not self._active_local_tasks())
        )
        if top_level_fast_path:
            return True

        return (
            self.local_immediate_child_events
            and explicit_active_child_emit
            and active_bus is not None
            and active_bus is not self
            and active_event is not None
            and active_event.event_id in active_bus._local_only_event_ids
            and event_obj.event_parent_id == active_event.event_id
            and event_obj.event_emitted_by_result_id is not None
        )

    @staticmethod
    def _deadline_at_after(seconds: float | None) -> str | None:
        if seconds is None:
            return None
        return (datetime.now(UTC) + timedelta(seconds=max(0.0, seconds))).isoformat().replace('+00:00', 'Z')

    def _local_invocation_record(
        self,
        event: BaseEvent[Any],
        handler: EventHandler,
        result: EventResult[Any],
    ) -> dict[str, Any]:
        handler_timeout = self._number_option(handler.handler_timeout)
        if handler_timeout is None:
            handler_timeout = self._number_option(event.event_handler_timeout)
        if handler_timeout is None:
            handler_timeout = self._number_option(self.event_handler_timeout)
        event_timeout = self._number_option(event.event_timeout)
        if event_timeout is None:
            event_timeout = self._number_option(self.event_timeout)
        if event_timeout is not None and event_timeout <= 0:
            event_timeout = None
        return {
            'handler_id': handler.id,
            'event_id': event.event_id,
            'result_id': result.id,
            'invocation_id': f'local:{result.id}',
            'fence': 0,
            'event_snapshot': {
                'event_timeout': event_timeout,
                'event_handler_completion': _option_value(event.event_handler_completion) or self.event_handler_completion,
            },
            'result_snapshot': {
                'timeout': handler_timeout,
                'handler_timeout': handler_timeout,
            },
            'deadline_at': self._deadline_at_after(handler_timeout),
            'event_deadline_at': self._deadline_at_after(event_timeout),
        }

    def _run_local_async_handlers(self, event: BaseEvent[Any]) -> bool:
        handlers = self._matching_local_handlers_for_event(event)
        if not handlers:
            return False
        self._start_event_slow_warning_timer(event)
        self._resolve_find_waiters(event)
        self._local_only_event_ids.add(event.event_id)
        self.in_flight_event_ids.add(event.event_id)
        on_idle = self._on_idle
        if on_idle is not None:
            on_idle.clear()
        event._core_known = False  # pyright: ignore[reportPrivateUsage]
        dispatch_context = event._get_dispatch_context()  # pyright: ignore[reportPrivateUsage]
        context = dispatch_context.copy() if dispatch_context is not None else self._without_core_outcome_batch_context()
        task = asyncio.create_task(self._run_local_async_handlers_task(event, handlers), context=context)
        self._processing_tasks.add(task)
        task.add_done_callback(self._processing_tasks.discard)
        return True

    async def _run_local_async_handlers_task(
        self,
        event: BaseEvent[Any],
        handlers: list[EventHandler],
    ) -> None:
        expected_handler_ids = {handler.id for handler in handlers}
        try:
            for handler in handlers:
                pending_result, _notify_pending = self._pending_result_for_invocation(event, handler)
                active_result = self._start_result_fast(event, handler)
                active_result.id = pending_result.id
                self._event_id_by_result_id[active_result.id] = event.event_id
                invocation = self._local_invocation_record(event, handler, active_result)
                self._apply_effective_timeout_to_result(invocation, active_result, event, handler)
                abort_context = ActiveAbortContext()
                try:
                    tokens = self._set_handler_context(event, handler.id)
                    abort_token = set_active_abort_context(abort_context)
                    try:
                        with self._handler_event_path_context(event):
                            result_value = await self._invoke_handler_value_async(invocation, event, handler, abort_context)
                    finally:
                        reset_active_abort_context(abort_token)
                        self._reset_handler_context(tokens)
                except EventHandlerAbortedError as exc:
                    local_result = self._update_event_result_for_handler(event, handler, status='started')
                    local_result.id = active_result.id
                    self._apply_effective_timeout_to_result(invocation, local_result, event, handler)
                    local_result.update(error=exc)
                except Exception as exc:
                    local_result = self._update_event_result_for_handler(event, handler, status='started')
                    local_result.id = active_result.id
                    self._apply_effective_timeout_to_result(invocation, local_result, event, handler)
                    local_result.update(error=exc)
                else:
                    completed_result = self._complete_untyped_result_fast(event, handler, result_value)
                    if completed_result is None:
                        completed_result = self._update_event_result_for_handler(event, handler, result=result_value)
                    completed_result.id = active_result.id
                    self._event_id_by_result_id[completed_result.id] = event.event_id
            self._track_event_replayability_for_new_handlers(event)
            if not self._complete_local_only_event_if_terminal(event, expected_handler_ids):
                await asyncio.sleep(0)
                self._complete_local_only_event_if_terminal(event, expected_handler_ids)
        finally:
            if event.event_status == EventStatus.COMPLETED:
                self._local_only_event_ids.discard(event.event_id)
            if not self._event_has_unfinished_results(event):
                self.in_flight_event_ids.discard(event.event_id)
            if not self.in_flight_event_ids:
                self._mark_idle()

    async def _process_event_now_locally_if_possible(self, event: BaseEvent[Any]) -> bool:
        if self.middlewares or self._background_worker:
            return False
        if event.event_emitted_by_result_id is not None or event.event_parent_id is not None:
            return False
        if self._find_parent_invocation_for_event(event) is not None:
            return False
        pending_records = [record for record in self._pending_core_emits if record.get('event_id') == event.event_id]
        if not pending_records and event._core_known:  # pyright: ignore[reportPrivateUsage]
            return False
        handlers = self._matching_local_handlers_for_event(event)
        if not handlers:
            return False
        if not all(
            handler.handler is not None and not isinstance(getattr(handler.handler, '__self__', None), RustCoreEventBus)
            for handler in handlers
        ):
            return False

        self._pending_core_emits = [record for record in self._pending_core_emits if record.get('event_id') != event.event_id]
        self._local_only_event_ids.add(event.event_id)
        event._core_known = False  # pyright: ignore[reportPrivateUsage]
        self._remove_pending_event(event.event_id)
        self.in_flight_event_ids.add(event.event_id)
        on_idle = self._on_idle
        if on_idle is not None:
            on_idle.clear()

        expected_handler_ids = {handler.id for handler in handlers}
        for handler in handlers:
            pending_result, _notify_pending = self._pending_result_for_invocation(event, handler)
            active_result = self._start_result_fast(event, handler)
            active_result.id = pending_result.id
            self._event_id_by_result_id[active_result.id] = event.event_id
            invocation = self._local_invocation_record(event, handler, active_result)
            self._apply_effective_timeout_to_result(invocation, active_result, event, handler)
            abort_context = ActiveAbortContext()
            try:
                tokens = self._set_handler_context(event, handler.id)
                abort_token = set_active_abort_context(abort_context)
                try:
                    with self._handler_event_path_context(event):
                        result_value = await self._invoke_handler_value_async(invocation, event, handler, abort_context)
                finally:
                    reset_active_abort_context(abort_token)
                    self._reset_handler_context(tokens)
            except EventHandlerAbortedError as exc:
                local_result = self._update_event_result_for_handler(event, handler, status='started')
                local_result.id = active_result.id
                self._apply_effective_timeout_to_result(invocation, local_result, event, handler)
                local_result.update(error=exc)
            except Exception as exc:
                local_result = self._update_event_result_for_handler(event, handler, status='started')
                local_result.id = active_result.id
                self._apply_effective_timeout_to_result(invocation, local_result, event, handler)
                local_result.update(error=exc)
            else:
                completed_result = self._complete_untyped_result_fast(event, handler, result_value)
                if completed_result is None:
                    completed_result = self._update_event_result_for_handler(event, handler, result=result_value)
                completed_result.id = active_result.id
                self._event_id_by_result_id[completed_result.id] = event.event_id
            if (_option_value(event.event_handler_completion) or self.event_handler_completion) == 'first':
                if any(result.status == 'completed' and result.result is not None for result in event.event_results.values()):
                    expected_handler_ids = set(event.event_results.keys())
                    break

        self._track_event_replayability_for_new_handlers(event)
        self._complete_local_only_event_if_terminal(event, expected_handler_ids)
        if event.event_status == EventStatus.COMPLETED:
            self._local_only_event_ids.discard(event.event_id)
        if not self._event_has_unfinished_results(event):
            self.in_flight_event_ids.discard(event.event_id)
        if not self.in_flight_event_ids:
            self._mark_idle()
        return event.event_status == EventStatus.COMPLETED

    def off(
        self,
        event_type: str | type[BaseEvent[Any]],
        handler: CoreHandler | str | EventHandler | None = None,
    ) -> None:
        event_pattern = EventHistory.normalize_event_pattern(event_type)
        handler_id_arg = handler.id if isinstance(handler, EventHandler) else handler
        for handler_id, entry in list(self._handlers.items()):
            if entry.event_pattern != event_pattern:
                continue
            if handler_id_arg is not None:
                if isinstance(handler_id_arg, str) and handler_id != handler_id_arg:
                    continue
                if callable(handler_id_arg) and entry.handler is not handler_id_arg:
                    continue
            dict.__delitem__(self._handlers, handler_id)
            ids = self.handlers_by_key.get(event_pattern)
            if ids and handler_id in ids:
                ids.remove(handler_id)
                if not ids:
                    self.handlers_by_key.pop(event_pattern, None)
            self._emit_bus_handlers_change(entry, False)
            self._remove_handler_name_from_key(event_pattern, entry.handler_name)
            self._unregister_handler_from_core(handler_id)

    def emit(self, event: T_Event) -> T_Event:
        self._raise_if_destroyed()
        if self.pending_event_queue is None:  # type: ignore[comparison-overlap]
            self.pending_event_queue = CleanShutdownQueue(maxsize=0)
        event_obj = event
        active_handler_id = self._active_handler_id()
        active_bus = self._active_eventbus()
        active_event = self._active_event()
        if (
            active_handler_id is not None
            and active_bus is self
            and active_event is not None
            and active_event.event_id != event_obj.event_id
            and event_obj.event_parent_id == active_event.event_id
        ):
            self._raise_if_recursive_self_emit_exceeds_limit(active_event, event_obj)
            handler_entry = self._handlers.get(active_handler_id)
            if handler_entry is not None and event_obj.event_emitted_by_result_id is not None:
                active_result = self._update_event_result_for_handler(active_event, handler_entry, status='started')
                if event_obj.event_emitted_by_result_id == active_result.id and not any(
                    child.event_id == event_obj.event_id for child in active_result.event_children
                ):
                    active_result.event_children.append(event_obj)
        if (
            self.event_history.max_history_size is not None
            and self.event_history.max_history_size > 0
            and not self.event_history.max_history_drop
            and event_obj.event_id not in self.event_history
            and len(self.event_history) >= self.event_history.max_history_size
        ):
            raise RuntimeError(
                f'{self} history limit reached ({len(self.event_history)}/{self.event_history.max_history_size}); '
                'set event_history.max_history_drop=True to drop old history instead of rejecting new events'
            )
        if event_obj._core_known and self.label in event_obj.event_path:  # pyright: ignore[reportPrivateUsage]
            return event_obj
        forwarding_existing_event = event_obj._core_known  # pyright: ignore[reportPrivateUsage]
        if self.label not in event_obj.event_path:
            event_obj.event_path.append(self.label)
        if event_obj._get_dispatch_context() is None:  # pyright: ignore[reportPrivateUsage]
            event_obj._set_dispatch_context(self._without_core_outcome_batch_context())  # pyright: ignore[reportPrivateUsage]
        event_obj.event_status = EventStatus.PENDING
        on_idle = self._on_idle
        if on_idle is not None:
            on_idle.clear()
        self._events[event_obj.event_id] = event_obj
        self._local_only_event_ids.discard(event_obj.event_id)
        event_obj._core_event_bus_ref = weakref.ref(self)  # pyright: ignore[reportPrivateUsage]
        self.event_history[event_obj.event_id] = event_obj
        self._trim_event_history_if_needed()
        self._index_event_relationships(event_obj)
        if self.middlewares:
            self._ensure_local_pending_results_for_event(event_obj)
        self._track_event_replayability_for_new_handlers(event_obj)
        had_existing_pending = any(event_id != event_obj.event_id for event_id in self.in_flight_event_ids)
        resolved_event_concurrency = _option_value(event_obj.event_concurrency) or self.event_concurrency
        parent_invocation_id: str | None = None
        explicit_active_child_emit = (
            active_handler_id is not None
            and active_bus is not None
            and active_event is not None
            and event_obj.event_id != active_event.event_id
            and event_obj.event_parent_id == active_event.event_id
            and event_obj.event_emitted_by_result_id is not None
        )
        if self._can_run_local_immediate_handlers(
            event_obj,
            active_bus=active_bus,
            active_event=active_event,
            active_handler_id=active_handler_id,
            forwarding_existing_event=forwarding_existing_event,
            had_existing_pending=had_existing_pending,
            parent_invocation_id=parent_invocation_id,
            explicit_active_child_emit=explicit_active_child_emit,
        ) and self._run_local_immediate_handlers(event_obj):
            return event_obj
        if self._can_run_local_async_handlers(
            event_obj,
            active_bus=active_bus,
            active_event=active_event,
            active_handler_id=active_handler_id,
            forwarding_existing_event=forwarding_existing_event,
            had_existing_pending=had_existing_pending,
            parent_invocation_id=parent_invocation_id,
            explicit_active_child_emit=explicit_active_child_emit,
        ) and self._run_local_async_handlers(event_obj):
            return event_obj
        self._start_event_slow_warning_timer(event_obj)
        self.in_flight_event_ids.add(event_obj.event_id)
        if event_obj.event_id not in self._pending_event_ids:
            self.pending_event_queue.put_nowait(event_obj)
            self._pending_event_ids.add(event_obj.event_id)
        if had_existing_pending and (
            resolved_event_concurrency != 'parallel'
            or active_handler_id is not None
            or forwarding_existing_event
            or event_obj.event_parent_id is not None
            or event_obj.event_emitted_by_result_id is not None
            or event_obj.event_emitted_by_handler_id is not None
        ):
            self._drain_pending_on_await_event_ids.add(event_obj.event_id)
        self._sync_bus_history_policy()
        event_record = self._event_record_for_core(event_obj)
        event_record.pop('event_pending_bus_count', None)
        event_record.pop('event_results', None)
        event_record['event_status'] = 'pending'
        event_record['event_started_at'] = None
        event_record['event_completed_at'] = None
        defer_start = (
            had_existing_pending
            and self._can_schedule_async_processing()
            and active_handler_id is None
            and event_obj.event_emitted_by_result_id is None
            and resolved_event_concurrency != 'parallel'
        )
        cross_bus_explicit_child_emit = explicit_active_child_emit and active_bus is not self
        if cross_bus_explicit_child_emit:
            parent_invocation_id = self._find_parent_invocation_for_event(event_obj)
            if parent_invocation_id is None:
                raise RuntimeError('Missing active core invocation for cross-bus child emit')
            defer_start = True
        elif explicit_active_child_emit and active_bus is self and resolved_event_concurrency != 'parallel':
            defer_start = True
        if forwarding_existing_event:
            self._flush_related_pending_core_emits_sync()
        if self._can_batch_core_emit(
            event_obj,
            active_handler_id=active_handler_id,
            forwarding_existing_event=forwarding_existing_event,
            parent_invocation_id=parent_invocation_id,
            explicit_active_child_emit=explicit_active_child_emit,
        ):
            self._enqueue_core_emit(event_record)
            event_obj._core_known = True  # pyright: ignore[reportPrivateUsage]
            self._resolve_find_waiters(event_obj)
            if (
                resolved_event_concurrency == 'parallel'
                and had_existing_pending
                and active_handler_id is None
                and event_obj.event_emitted_by_result_id is None
                and self._can_schedule_async_processing()
            ):
                task = asyncio.create_task(
                    self._process_event_now_locally_if_possible(event_obj),
                    context=self._without_core_outcome_batch_context(),
                )
                self._processing_tasks.add(task)
                task.add_done_callback(self._processing_tasks.discard)
            self._ensure_background_driver(None)
            return event_obj
        if forwarding_existing_event:
            core_messages = self.core.forward_event(
                event_obj.event_id,
                self.bus_id,
                defer_start=defer_start,
                compact_response=True,
                parent_invocation_id=parent_invocation_id,
                block_parent_completion=False,
                pause_parent_route=False,
                event_options=self._event_control_options_for_core(event_obj),
            )
        else:
            core_messages = self.core.emit_event(
                event_record,
                self.bus_id,
                defer_start=defer_start,
                compact_response=True,
                parent_invocation_id=parent_invocation_id,
                block_parent_completion=False,
                pause_parent_route=False,
            )
        event_obj._core_known = True  # pyright: ignore[reportPrivateUsage]
        core_started_work = self._core_messages_start_work(core_messages)
        if core_started_work:
            self._is_running = True
        self._resolve_find_waiters(event_obj)
        if active_handler_id is not None or event_obj.event_emitted_by_result_id is not None:
            if (
                self._can_schedule_async_processing()
                and core_started_work
                and (resolved_event_concurrency == 'parallel' or (active_handler_id is not None and active_bus is not self))
            ):
                driver_target_event_id = event_obj.event_id
                if (
                    active_handler_id is not None
                    and active_bus is not self
                    and event_obj.event_emitted_by_result_id is None
                    and event_obj.event_parent_id is None
                ):
                    driver_target_event_id = None
                self._ensure_background_driver(driver_target_event_id, initial_messages=core_messages)
            elif core_messages:
                if self._can_schedule_async_processing():
                    self._schedule_core_messages(core_messages)
                    self._ensure_background_driver(None)
                else:
                    self._apply_messages_and_run_invocations(core_messages)
            return event_obj
        if (
            self._can_schedule_async_processing()
            and self._suppress_auto_driver <= 0
            and (core_started_work or had_existing_pending)
        ):
            self._ensure_background_driver(None, initial_messages=core_messages)
        else:
            if self._can_schedule_async_processing():
                if core_messages:
                    self._schedule_core_messages(core_messages)
                return event_obj
            self._apply_messages_and_run_invocations(core_messages)
            return cast(T_Event, self.run_until_event_completed(event_obj.event_id))
        return event_obj

    def _raise_if_recursive_self_emit_exceeds_limit(
        self,
        active_event: BaseEvent[Any],
        event_obj: BaseEvent[Any],
    ) -> None:
        if active_event.event_type != event_obj.event_type:
            return
        max_depth = getattr(self, 'max_handler_recursion_depth', 2)
        if not isinstance(max_depth, int) or max_depth < 1:
            return
        depth = 0
        current: BaseEvent[Any] | None = active_event
        seen: set[str] = set()
        while current is not None and current.event_id not in seen:
            seen.add(current.event_id)
            if current.event_type == event_obj.event_type:
                depth += 1
            parent_id = current.event_parent_id
            if parent_id is None:
                break
            current = self._events.get(parent_id) or self.event_history.get(parent_id)
        if depth > max_depth:
            raise RuntimeError(
                f'Infinite loop detected while emitting {event_obj.event_type}: '
                f'handler recursion depth {depth} exceeds max_handler_recursion_depth={max_depth}'
            )

    @contextmanager
    def _without_auto_driver(self) -> Any:
        self._suppress_auto_driver += 1
        try:
            yield
        finally:
            self._suppress_auto_driver -= 1

    def _schedule_core_messages(self, messages: list[dict[str, Any]]) -> None:
        if not messages:
            return
        self._scheduled_core_messages.extend(messages)
        task = self._scheduled_core_messages_task
        if task is not None and not task.done():
            return
        task = asyncio.create_task(
            self._drain_scheduled_core_messages(),
            context=self._without_core_outcome_batch_context(),
        )
        self._scheduled_core_messages_task = task
        self._is_running = True
        if self._runloop_task is None or self._runloop_task.done():
            self._runloop_task = task
        self._processing_tasks.add(task)
        task.add_done_callback(self._processing_tasks.discard)

    async def _drain_scheduled_core_messages(self) -> None:
        try:
            while self._scheduled_core_messages:
                messages = self._scheduled_core_messages
                self._scheduled_core_messages = []
                await self._apply_messages_and_run_invocations_async(messages)
        finally:
            if self._scheduled_core_messages_task is asyncio.current_task():
                self._scheduled_core_messages_task = None
            if self._scheduled_core_messages and not self._closed:
                task = asyncio.create_task(
                    self._drain_scheduled_core_messages(),
                    context=self._without_core_outcome_batch_context(),
                )
                self._scheduled_core_messages_task = task
                self._processing_tasks.add(task)
                task.add_done_callback(self._processing_tasks.discard)

    async def _drain_scheduled_core_messages_for_passive_wait(self) -> None:
        task = self._scheduled_core_messages_task
        if task is not None and task is not asyncio.current_task() and not task.done():
            await asyncio.shield(task)
            return
        if self._scheduled_core_messages:
            await self._drain_scheduled_core_messages()

    async def _drain_background_driver_for_passive_wait(self) -> None:
        task = self._background_drive_task
        if task is not None and task is not asyncio.current_task() and not task.done():
            await asyncio.shield(task)

    def _enqueue_core_emit(self, event_record: dict[str, Any]) -> None:
        self._pending_core_emits.append(event_record)
        self._is_running = True

    def _flush_pending_core_emits_sync(self) -> bool:
        progressed = False
        while self._pending_core_emits:
            events = self._pending_core_emits[: self._CORE_EMIT_BATCH_SIZE]
            del self._pending_core_emits[: self._CORE_EMIT_BATCH_SIZE]
            messages = self.core.emit_events(events, self.bus_id, defer_start=True, compact_response=True)
            progressed = True
            if not messages:
                continue
            if self._can_schedule_async_processing():
                self._schedule_core_messages(messages)
            else:
                self._apply_messages_and_run_invocations(messages)
        return progressed

    async def _flush_pending_core_emits_async(self) -> bool:
        progressed = False
        while self._pending_core_emits:
            events = self._pending_core_emits[: self._CORE_EMIT_BATCH_SIZE]
            del self._pending_core_emits[: self._CORE_EMIT_BATCH_SIZE]
            messages = self.core.emit_events(events, self.bus_id, defer_start=True, compact_response=True)
            progressed = True
            if messages:
                await self._apply_messages_and_run_invocations_async(messages)
        return progressed

    def _flush_related_pending_core_emits_sync(self) -> bool:
        progressed = False
        for bus in list(type(self)._instances):
            if bus._closed or bus.core is not self.core:
                continue
            if bus._flush_pending_core_emits_sync():
                progressed = True
        return progressed

    async def _flush_related_pending_core_emits_async(self) -> bool:
        progressed = False
        for bus in list(type(self)._instances):
            if bus._closed or bus.core is not self.core:
                continue
            if await bus._flush_pending_core_emits_async():
                progressed = True
        return progressed

    def _active_local_tasks(self) -> set[asyncio.Future[Any]]:
        current_task = asyncio.current_task()
        return {
            task
            for task in [*self._processing_tasks, *self._local_invocation_tasks, *self._middleware_hook_tasks]
            if task is not current_task and not task.done()
        }

    @staticmethod
    def _without_core_outcome_batch_context() -> contextvars.Context:
        context = contextvars.copy_context()
        context.run(_core_outcome_batch_context.set, None)
        return context

    @staticmethod
    def _invocation_messages_from_core_message(message: dict[str, Any]) -> list[dict[str, Any]]:
        if message.get('type') == 'invoke_handler':
            return [message]
        if message.get('type') == 'invoke_handlers_compact':
            raw_invocations = message.get('invocations')
            if not isinstance(raw_invocations, list):
                return []
            route_id = message.get('route_id')
            event_id = message.get('event_id')
            bus_id = message.get('bus_id')
            event_deadline_at = message.get('event_deadline_at')
            route_paused = message.get('route_paused') is True
            invocations: list[dict[str, Any]] = []
            for entry in raw_invocations:
                if not isinstance(entry, list) or len(entry) < 5:
                    continue
                invocation_id, result_id, handler_id, fence, deadline_at = entry[:5]
                if not isinstance(invocation_id, str) or not isinstance(result_id, str) or not isinstance(handler_id, str):
                    continue
                invocation: dict[str, Any] = {
                    'type': 'invoke_handler',
                    'invocation_id': invocation_id,
                    'result_id': result_id,
                    'route_id': route_id,
                    'event_id': event_id,
                    'bus_id': bus_id,
                    'handler_id': handler_id,
                    'fence': fence,
                    'route_paused': route_paused,
                }
                if isinstance(deadline_at, str):
                    invocation['deadline_at'] = deadline_at
                if isinstance(event_deadline_at, str):
                    invocation['event_deadline_at'] = event_deadline_at
                invocations.append(invocation)
            return invocations
        return []

    @classmethod
    def _invocation_messages_from_core_messages(cls, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        invocations: list[dict[str, Any]] = []
        for message in messages:
            invocations.extend(cls._invocation_messages_from_core_message(message))
        return invocations

    def _messages_include_invocation(self, messages: list[dict[str, Any]]) -> bool:
        return any(self._invocation_messages_from_core_message(message) for message in messages)

    async def _process_own_core_route_work_once_async(self) -> bool:
        messages = self.core.process_next_route(
            self.bus_id,
            limit=self._CORE_ROUTE_SLICE_LIMIT,
            compact_response=True,
        )
        progressed = bool(messages)
        if await self._apply_messages_and_run_invocations_async(messages):
            progressed = True
        return progressed

    def _apply_non_invocation_messages(self, messages: list[dict[str, Any]]) -> None:
        for message in messages:
            if not self._invocation_messages_from_core_message(message):
                self._apply_core_message(message)

    @classmethod
    def _core_messages_start_work(cls, messages: list[dict[str, Any]]) -> bool:
        for message in messages:
            if cls._invocation_messages_from_core_message(message):
                return True
            if message.get('type') != 'patch':
                continue
            patch = message.get('patch')
            if not isinstance(patch, dict):
                continue
            if patch.get('type') != 'event_emitted':
                return True
        return False

    def _queue_jump_target_from_core_messages(self, messages: list[dict[str, Any]] | None) -> str | None:
        if not messages:
            return None
        invocation_event_ids = {
            event_id
            for invocation in self._invocation_messages_from_core_messages(messages)
            for event_id in [invocation.get('event_id')]
            if isinstance(event_id, str)
        }
        if len(invocation_event_ids) != 1:
            return None
        event_id = next(iter(invocation_event_ids))
        return event_id if self._is_queue_jump_event_id(event_id) else None

    @classmethod
    def _is_queue_jump_event_id(cls, event_id: str) -> bool:
        return any(event_id in bus._queue_jump_event_ids for bus in list(cls._instances))

    def _ensure_background_driver(
        self,
        event_id: str | None = None,
        *,
        initial_messages: list[dict[str, Any]] | None = None,
    ) -> None:
        event_id = event_id or self._queue_jump_target_from_core_messages(initial_messages)
        if self._background_drive_task is not None and not self._background_drive_task.done():
            if event_id is None:
                self._background_drive_target_event_id = None
            elif (
                event_id is not None
                and self._background_drive_target_event_id is None
                and type(self)._is_queue_jump_event_id(event_id)
            ):
                self._background_drive_target_event_id = event_id
            if initial_messages:
                self._schedule_core_messages(initial_messages)
            return
        driver_initial_messages = initial_messages
        if (
            initial_messages
            and event_id is not None
            and self._messages_include_invocation(initial_messages)
            and not type(self)._is_queue_jump_event_id(event_id)
        ):
            self._schedule_core_messages(initial_messages)
            driver_initial_messages = None
        task = asyncio.create_task(
            self._drive_local_until_idle(event_id, initial_messages=driver_initial_messages),
            context=self._without_core_outcome_batch_context(),
        )
        self._background_drive_task = task
        self._background_drive_target_event_id = event_id
        self._is_running = True
        self._runloop_task = task
        self._processing_tasks.add(task)
        task.add_done_callback(self._processing_tasks.discard)

    async def _drive_local_until_idle(
        self,
        target_event_id: str | None = None,
        *,
        allow_local_terminal_completion: bool = False,
        initial_messages: list[dict[str, Any]] | None = None,
    ) -> None:
        try:
            empty_route_polls = 0
            for _ in range(100_000):
                if initial_messages is not None:
                    await asyncio.sleep(0)
                active_target_event_id = target_event_id
                if self._background_drive_task is asyncio.current_task():
                    active_target_event_id = self._background_drive_target_event_id
                if self._pending_core_emits:
                    await self._flush_pending_core_emits_async()
                active_target_event = self._events.get(active_target_event_id) if active_target_event_id is not None else None
                active_target_is_parallel = (
                    active_target_event is not None
                    and (_option_value(active_target_event.event_concurrency) or self.event_concurrency) == 'parallel'
                )
                if (
                    not active_target_is_parallel
                    and any(bus._queue_jump_event_ids for bus in list(type(self)._instances))
                    and (active_target_event_id is None or not type(self)._is_queue_jump_event_id(active_target_event_id))
                ):
                    await asyncio.sleep(0.001)
                    continue
                if active_target_event_id is not None:
                    target_event = active_target_event
                    if target_event is None or target_event.event_status == EventStatus.COMPLETED:
                        if not any(
                            active_target_event_id in bus._explicit_await_event_ids
                            for bus in list(type(self)._instances)
                            if not bus._closed
                        ):
                            self._schedule_background_drain_if_pending(active_target_event_id)
                        return
                if initial_messages is not None:
                    messages = initial_messages
                    initial_messages = None
                    pre_progressed = False
                else:
                    if not self.in_flight_event_ids:
                        if await self._process_own_core_route_work_once_async():
                            continue
                        self._mark_idle()
                        return
                    active_processing_tasks = {
                        task for task in self._processing_tasks if task is not asyncio.current_task() and not task.done()
                    }
                    if active_processing_tasks and self._passive_handler_wait_depth <= 0:
                        await asyncio.wait(active_processing_tasks, timeout=0.001, return_when=asyncio.FIRST_COMPLETED)
                        continue
                    active_invocation_tasks = {task for task in self._local_invocation_tasks if not task.done()}
                    if active_invocation_tasks and self._passive_handler_wait_depth <= 0:
                        await asyncio.wait(active_invocation_tasks, timeout=0.001, return_when=asyncio.FIRST_COMPLETED)
                        continue
                    for event_id in list(self.in_flight_event_ids):
                        event = self._events.get(event_id)
                        if event is None or event.event_status == EventStatus.COMPLETED:
                            self.in_flight_event_ids.discard(event_id)
                    if not self.in_flight_event_ids:
                        if await self._process_own_core_route_work_once_async():
                            continue
                        self._mark_idle()
                        return
                    if active_target_event_id is not None:
                        messages = []
                        pre_progressed = False
                        live_buses = self._live_buses_for_event(
                            active_target_event_id,
                            event_type=active_target_event.event_type if active_target_event is not None else None,
                        )
                        for bus in live_buses:
                            bus_messages = bus.core.process_next_route(
                                bus.bus_id,
                                limit=self._CORE_ROUTE_SLICE_LIMIT,
                                compact_response=True,
                            )
                            if bus_messages:
                                pre_progressed = True
                            with bus._targeted_core_drive_scope():
                                if await bus._apply_messages_and_run_invocations_async(bus_messages):
                                    pre_progressed = True
                    else:
                        pre_progressed = False
                        messages = []
                        live_core_buses = [
                            bus for bus in list(type(self)._instances) if not bus._closed and bus.core is self.core
                        ]
                        has_related_bus_pending = len(live_core_buses) > 1 and any(
                            len(self._live_buses_for_event(event_id)) > 1 for event_id in self.in_flight_event_ids
                        )
                        if has_related_bus_pending:
                            for bus in live_core_buses:
                                bus_messages = bus.core.process_next_route(
                                    bus.bus_id,
                                    limit=self._CORE_ROUTE_SLICE_LIMIT,
                                    compact_response=True,
                                )
                                if bus_messages:
                                    pre_progressed = True
                                with bus._targeted_core_drive_scope():
                                    if await bus._apply_messages_and_run_invocations_async(bus_messages):
                                        pre_progressed = True
                        else:
                            messages = self.core.process_next_route(
                                self.bus_id,
                                limit=self._CORE_ROUTE_SLICE_LIMIT,
                                compact_response=True,
                            )
                progressed = pre_progressed
                if messages:
                    progressed = True
                apply_scope = self._targeted_core_drive_scope() if active_target_event_id is not None else nullcontext()
                with apply_scope:
                    invocation_progressed = await self._apply_messages_and_run_invocations_async(messages)
                if invocation_progressed:
                    progressed = True
                if progressed:
                    for event_id in list(self.in_flight_event_ids):
                        event = self._events.get(event_id)
                        if (
                            event is not None
                            and event.event_status == EventStatus.COMPLETED
                            and not self._event_has_unfinished_results(event)
                        ):
                            self.in_flight_event_ids.discard(event_id)
                            self._remove_pending_event(event_id)
                if progressed:
                    empty_route_polls = 0
                if active_target_event_id is not None and self._completed_explicit_await_event_id() is not None:
                    return
                if active_target_event_id is not None:
                    target_event = self._events.get(active_target_event_id)
                    if target_event is None or target_event.event_status == EventStatus.COMPLETED:
                        if not any(
                            active_target_event_id in bus._explicit_await_event_ids
                            for bus in list(type(self)._instances)
                            if not bus._closed
                        ):
                            self._schedule_background_drain_if_pending(active_target_event_id)
                        return
                if not messages:
                    pending_ids = list(self.in_flight_event_ids)
                    for event_id in pending_ids:
                        event = self._events.get(event_id)
                        if event is None or event.event_status == EventStatus.COMPLETED:
                            self.in_flight_event_ids.discard(event_id)
                            continue
                        if (
                            allow_local_terminal_completion
                            and event.event_status != EventStatus.COMPLETED
                            and self._complete_event_if_local_results_terminal(event)
                        ):
                            progressed = True
                            self.in_flight_event_ids.discard(event_id)
                            continue
                    if not self.in_flight_event_ids:
                        if await self._process_own_core_route_work_once_async():
                            continue
                        self._mark_idle()
                        return
                    if progressed:
                        empty_route_polls = 0
                        continue
                    related_active_tasks: set[asyncio.Future[Any]] = set()
                    live_core_buses = [bus for bus in list(type(self)._instances) if not bus._closed and bus.core is self.core]
                    if len(live_core_buses) > 1:
                        for event_id in list(self.in_flight_event_ids):
                            live_buses = self._live_buses_for_event(event_id)
                            if len(live_buses) <= 1:
                                continue
                            for bus in live_buses:
                                related_active_tasks.update(
                                    task for task in bus._active_local_tasks() if task is not bus._background_drive_task
                                )
                    if related_active_tasks:
                        done, _pending = await asyncio.wait(
                            related_active_tasks,
                            timeout=0.001,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if done:
                            await asyncio.gather(*done, return_exceptions=True)
                        continue
                    active_commit_tasks = {
                        task
                        for event_id in list(self.in_flight_event_ids)
                        for task in self._outcome_commit_tasks_by_event_id.get(event_id, set())
                        if task is not asyncio.current_task() and not task.done()
                    }
                    if active_commit_tasks:
                        done, _pending = await asyncio.wait(
                            active_commit_tasks,
                            timeout=0.001,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if done:
                            await asyncio.gather(*done, return_exceptions=True)
                        continue
                    empty_route_polls += 1
                    if empty_route_polls < 2:
                        await asyncio.sleep(0)
                        continue
                    empty_route_polls = 0
                    for event_id in list(self.in_flight_event_ids):
                        event = self._events.get(event_id)
                        if event is None or event.event_status == EventStatus.COMPLETED:
                            self.in_flight_event_ids.discard(event_id)
                            continue
                        snapshot = await self._get_event_snapshot_from_core_async(event_id)
                        if isinstance(snapshot, dict):
                            for bus in self._live_buses_for_event(event_id):
                                event_obj = bus._events.get(event_id)
                                if event_obj is None:
                                    continue
                                before_status = event_obj.event_status
                                before_results = {
                                    result_id: result.status for result_id, result in event_obj.event_results.items()
                                }
                                await bus._apply_core_snapshot_to_event_async(event_obj, snapshot)
                                after_results = {
                                    result_id: result.status for result_id, result in event_obj.event_results.items()
                                }
                                if event_obj.event_status != before_status or after_results != before_results:
                                    progressed = True
                            event = self._events.get(event_id)
                        if event is None or event.event_status == EventStatus.COMPLETED:
                            self.in_flight_event_ids.discard(event_id)
                    if not self.in_flight_event_ids:
                        if await self._process_own_core_route_work_once_async():
                            continue
                        self._mark_idle()
                        return
                if not progressed:
                    await asyncio.sleep(0.001)
            raise RuntimeError(f'bus did not become idle within iteration limit: {self.bus_id}')
        finally:
            if self._background_drive_task is asyncio.current_task():
                self._background_drive_task = None
                self._background_drive_target_event_id = None

    def _schedule_background_drain_if_pending(self, completed_target_event_id: str) -> None:
        if self._queue_jump_event_ids:
            return
        if not self._has_incomplete_pending_work(exclude_event_id=completed_target_event_id):
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.call_soon(self._ensure_background_driver, None)

    def _complete_event_if_local_results_terminal(self, event: BaseEvent[Any]) -> bool:
        expected_handler_ids = self._expected_live_handler_ids(event)
        if not expected_handler_ids:
            return False
        if not expected_handler_ids.issubset(event.event_results.keys()):
            return False
        if any(event.event_results[handler_id].status not in ('completed', 'error') for handler_id in expected_handler_ids):
            return False
        for handler_id in expected_handler_ids:
            result = event.event_results[handler_id]
            if not isinstance(result.result, BaseEvent):
                continue
            child_event = result.result
            if child_event.event_id == event.event_id:
                continue
            if child_event.event_status == EventStatus.COMPLETED:
                continue
            completed_child = self._completed_live_event(child_event.event_id)
            if completed_child is not None:
                result.result = completed_child
                continue
            return False
        if event._is_queued_on_any_bus(ignore_bus=cast(Any, self)):  # pyright: ignore[reportPrivateUsage]
            return False
        if not event._are_all_children_complete():  # pyright: ignore[reportPrivateUsage]
            return False
        latest_completed: str | None = None
        for result in event.event_results.values():
            if result.completed_at is not None and (latest_completed is None or result.completed_at > latest_completed):
                latest_completed = result.completed_at
        event.event_completed_at = latest_completed or event.event_completed_at or monotonic_datetime()
        if event.event_started_at is None:
            event.event_started_at = event.event_completed_at
        event.event_status = EventStatus.COMPLETED
        event._event_is_complete_flag = True  # pyright: ignore[reportPrivateUsage]
        completed_signal = event.event_completed_signal
        if completed_signal is not None:
            completed_signal.set()
        self.event_history[event.event_id] = event
        self._trim_event_history_if_needed(force=True)
        self._forget_completed_event_if_unretained(event)
        self._remove_pending_event(event.event_id)
        self.in_flight_event_ids.discard(event.event_id)
        self._local_only_event_ids.discard(event.event_id)
        return True

    def _complete_local_only_event_if_terminal(
        self,
        event: BaseEvent[Any],
        expected_handler_ids: set[str],
    ) -> bool:
        if not expected_handler_ids:
            return False
        if not expected_handler_ids.issubset(event.event_results.keys()):
            return False
        if any(event.event_results[handler_id].status not in ('completed', 'error') for handler_id in expected_handler_ids):
            return False
        if not event._are_all_children_complete():  # pyright: ignore[reportPrivateUsage]
            return False
        latest_completed: str | None = None
        for handler_id in expected_handler_ids:
            completed_at = event.event_results[handler_id].completed_at
            if completed_at is not None and (latest_completed is None or completed_at > latest_completed):
                latest_completed = completed_at
        event.event_completed_at = latest_completed or event.event_completed_at or monotonic_datetime()
        if event.event_started_at is None:
            event.event_started_at = event.event_completed_at
        event.event_status = EventStatus.COMPLETED
        event._event_is_complete_flag = True  # pyright: ignore[reportPrivateUsage]
        completed_signal = event.event_completed_signal
        if completed_signal is not None:
            completed_signal.set()
        self.event_history[event.event_id] = event
        self._trim_event_history_if_needed(force=True)
        self._forget_completed_event_if_unretained(event)
        self._remove_pending_event(event.event_id)
        self.in_flight_event_ids.discard(event.event_id)
        return True

    @staticmethod
    def _active_handler_id() -> str | None:
        try:
            from abxbus.event_bus import get_current_handler_id
        except Exception:
            return None
        return get_current_handler_id()

    @staticmethod
    def _active_eventbus() -> Any | None:
        try:
            from abxbus.event_bus import get_current_eventbus
        except Exception:
            return None
        return get_current_eventbus()

    @staticmethod
    def _active_event() -> BaseEvent[Any] | None:
        try:
            from abxbus.event_bus import get_current_event
        except Exception:
            return None
        return get_current_event()

    @contextmanager
    def _targeted_core_drive_scope(self) -> Any:
        self._suppress_post_outcome_available_processing += 1
        try:
            yield
        finally:
            self._suppress_post_outcome_available_processing -= 1

    def _process_available_after_handler_outcome(self, invocation: dict[str, Any] | None = None) -> bool:
        if self._suppress_post_outcome_available_processing > 0:
            return False
        if invocation is None:
            return True
        event_id = invocation.get('event_id')
        if not isinstance(event_id, str):
            return True
        return not any(event_id in bus._explicit_await_event_ids for bus in list(type(self)._instances) if not bus._closed)

    def _process_route_after_handler_outcome(self, invocation: dict[str, Any]) -> bool:
        event_id = invocation.get('event_id')
        if not isinstance(event_id, str):
            return True
        return not any(event_id in bus._explicit_await_event_ids for bus in list(type(self)._instances) if not bus._closed)

    @classmethod
    def _active_explicit_await_event_ids(cls) -> set[str]:
        return {event_id for bus in list(cls._instances) if not bus._closed for event_id in bus._explicit_await_event_ids}

    def _release_queue_jump_children_for_invocation(self, invocation_id: Any) -> None:
        if not isinstance(invocation_id, str):
            return
        for bus in list(type(self)._instances):
            released_event_ids = [
                event_id
                for event_id, parent_invocation_id in bus._queue_jump_parent_invocation_by_event_id.items()
                if parent_invocation_id == invocation_id
            ]
            for event_id in released_event_ids:
                bus._queue_jump_event_ids.discard(event_id)
                bus._queue_jump_parent_invocation_by_event_id.pop(event_id, None)

    def _event_completion_wait_deadline(self, event: BaseEvent[Any] | None = None) -> float | None:
        raw_timeout = _option_value(event.event_timeout) if event is not None else None
        if raw_timeout is None:
            raw_timeout = self.event_timeout
        if not isinstance(raw_timeout, int | float) or raw_timeout <= 0:
            return None
        return time.monotonic() + float(raw_timeout) + 5.0

    @staticmethod
    def _deadline_active(deadline: float | None) -> bool:
        return deadline is None or time.monotonic() < deadline

    @staticmethod
    def _short_wait_timeout(deadline: float | None) -> float:
        if deadline is None:
            return 0.05
        return min(0.05, max(0.001, deadline - time.monotonic()))

    def dispatch(self, event: BaseEvent[Any]) -> BaseEvent[Any]:
        return self.emit(event)

    def sync_event_state_to_core(self, event: BaseEvent[Any]) -> None:
        self._sync_handler_removals_to_core()
        self._flush_pending_core_emits_sync()
        event_record = self._event_record_for_core(event)
        event_record.pop('event_pending_bus_count', None)
        event_record.pop('event_results', None)
        for message in self.core.emit_event(event_record, self.bus_id, compact_response=True):
            self._apply_core_message(message)

    @staticmethod
    def _event_record_for_core(event: BaseEvent[Any]) -> dict[str, Any]:
        record: dict[str, Any] = {}
        values = event.__dict__
        for field_name, value in values.items():
            if field_name == 'event_results':
                continue
            if field_name == 'event_result_type':
                record[field_name] = pydantic_model_to_json_schema(value)
            else:
                record[field_name] = EventResult._serialize_jsonable(value)
        for field_name in type(event).model_fields:
            if field_name in values or field_name == 'event_results':
                continue
            value = getattr(event, field_name)
            if field_name == 'event_result_type':
                record[field_name] = pydantic_model_to_json_schema(value)
            else:
                record[field_name] = EventResult._serialize_jsonable(value)
        return record

    @staticmethod
    def _event_control_options_for_core(event: BaseEvent[Any]) -> dict[str, Any]:
        options: dict[str, Any] = {}
        for field_name in (
            'event_timeout',
            'event_slow_timeout',
            'event_concurrency',
            'event_handler_timeout',
            'event_handler_slow_timeout',
            'event_handler_concurrency',
            'event_handler_completion',
        ):
            value = getattr(event, field_name)
            if value is not None:
                options[field_name] = EventResult._serialize_jsonable(value)
        if event.event_blocks_parent_completion:
            options['event_blocks_parent_completion'] = True
        return options

    @staticmethod
    def _event_snapshot_from_messages(messages: list[dict[str, Any]]) -> dict[str, Any] | None:
        for message in messages:
            if message.get('type') == 'event_snapshot':
                event = message.get('event')
                return cast(dict[str, Any], event) if isinstance(event, dict) else None
        return None

    def _get_event_snapshot_from_core(self, event_id: str) -> dict[str, Any] | None:
        messages = self.core.request_messages({'type': 'get_event', 'event_id': event_id})
        snapshot = self._event_snapshot_from_messages(messages)
        for message in messages:
            self._apply_core_message(message)
        return snapshot

    async def _get_event_snapshot_from_core_async(self, event_id: str) -> dict[str, Any] | None:
        messages = await asyncio.to_thread(self.core.request_messages, {'type': 'get_event', 'event_id': event_id})
        snapshot = self._event_snapshot_from_messages(messages)
        for message in messages:
            await self._apply_core_message_async(message)
        return snapshot

    def run_until_event_completed(self, event_id: str) -> BaseEvent[Any]:
        if event_id in self._processing_event_ids:
            event_obj = self._events.get(event_id)
            if event_obj is None:
                raise RuntimeError(f'missing event: {event_id}')
            return event_obj
        self._processing_event_ids.add(event_id)
        try:
            with self._targeted_core_drive_scope():
                for _ in range(1000):
                    self._flush_related_pending_core_emits_sync()
                    buses = self._live_buses_for_event(event_id)
                    for bus in buses:
                        messages = bus.core.process_next_route(
                            bus.bus_id,
                            limit=self._CORE_ROUTE_SLICE_LIMIT,
                            compact_response=True,
                        )
                        for message in messages:
                            if not bus._invocation_messages_from_core_message(message):
                                bus._apply_core_message(message)
                        for invocation in bus._invocation_messages_from_core_messages(messages):
                            for produced in bus._run_invocation(invocation):
                                if not bus._invocation_messages_from_core_message(produced):
                                    bus._apply_core_message(produced)
                    completed_event = self._completed_live_event(event_id)
                    if completed_event is not None:
                        return self._completed_snapshot_or_event(completed_event)
                    snapshot = self._get_event_snapshot_from_core(event_id)
                    if isinstance(snapshot, dict) and snapshot.get('event_status') == 'completed':
                        for bus in buses:
                            event_obj = bus._events.get(event_id)
                            if event_obj is None:
                                continue
                            bus._apply_core_snapshot_to_event(event_obj, snapshot)
                            return event_obj
                        return self._event_from_core_record(
                            self._event_types_by_pattern.get(str(snapshot.get('event_type')), '*'),
                            snapshot,
                        )
                    time.sleep(0.001)
            raise RuntimeError(f'event did not complete within iteration limit: {event_id}')
        finally:
            self._processing_event_ids.discard(event_id)

    async def process_until_event_completed(self, event_id: str) -> BaseEvent[Any]:
        if event_id in self._processing_event_ids:
            event_obj = self._events.get(event_id)
            if event_obj is None:
                raise RuntimeError(f'missing event: {event_id}')
            return await event_obj.wait()
        self._processing_event_ids.add(event_id)
        try:
            with self._targeted_core_drive_scope():
                for _ in range(1000):
                    await self._flush_pending_core_emits_async()
                    messages = self.core.process_next_route(
                        self.bus_id,
                        limit=self._CORE_ROUTE_SLICE_LIMIT,
                        compact_response=True,
                    )
                    await self._apply_messages_and_run_invocations_async(messages)
                    completed_event = self._completed_live_event(event_id)
                    if completed_event is not None:
                        return self._completed_snapshot_or_event(completed_event)
                    if not messages:
                        snapshot = await self._get_event_snapshot_from_core_async(event_id)
                        if isinstance(snapshot, dict):
                            existing_event = self._events.get(event_id)
                            if existing_event is not None:
                                if snapshot.get('event_status') == 'completed':
                                    return self._completed_snapshot_or_event(existing_event)
                                await asyncio.sleep(0.001)
                                continue
                            event_obj = self._event_from_core_record(
                                self._event_types_by_pattern.get(str(snapshot.get('event_type')), '*'),
                                snapshot,
                            )
                            self._events[event_id] = event_obj
                            self.event_history[event_id] = event_obj
                            self._trim_event_history_if_needed(force=event_obj.event_status == EventStatus.COMPLETED)
                            self._forget_completed_event_if_unretained(event_obj)
                            if event_obj.event_status == EventStatus.COMPLETED:
                                self._remove_pending_event(event_id)
                                return self._completed_snapshot_or_event(event_obj)
                        event_obj = self._events.get(event_id)
                        if event_obj is not None and event_obj.event_status == EventStatus.COMPLETED:
                            return self._completed_snapshot_or_event(event_obj)
                        await asyncio.sleep(0.001)
            raise RuntimeError(f'event did not complete within iteration limit: {event_id}')
        finally:
            self._processing_event_ids.discard(event_id)

    async def process_event_immediately(self, event: BaseEvent[Any]) -> BaseEvent[Any]:
        event_id = event.event_id
        if event_id in self._local_only_event_ids:
            if event.event_status != EventStatus.COMPLETED and event.event_completed_signal is not None:
                await event.event_completed_signal.wait()
            parent_result_id = event.event_emitted_by_result_id
            if parent_result_id is not None:
                for bus in list(type(self)._instances):
                    parent_event_id = bus._event_id_by_result_id.get(parent_result_id)
                    parent_event = bus._events.get(parent_event_id) if parent_event_id is not None else None
                    if parent_event is None or parent_event.event_id not in bus._local_only_event_ids:
                        continue
                    parent_handler_id = parent_event.event_emitted_by_handler_id
                    if parent_handler_id is None and parent_event.event_results:
                        parent_handler_id = next(iter(parent_event.event_results))
                    if parent_handler_id is not None:
                        bus._complete_local_only_event_if_terminal(parent_event, {parent_handler_id})
            return event
        parent_invocation_id = self._find_parent_invocation_for_event(event)
        block_parent_completion = parent_invocation_id is not None
        if parent_invocation_id is None:
            parent_invocation_id = self._active_invocation_id()
            block_parent_completion = False
        guard_queue_order = (_option_value(event.event_concurrency) or self.event_concurrency) != 'parallel'
        if guard_queue_order and event_id in self._queue_jump_event_ids:
            await event.wait()
            return event
        if guard_queue_order:
            self._queue_jump_event_ids.add(event_id)
            if parent_invocation_id is not None:
                self._queue_jump_parent_invocation_by_event_id[event_id] = parent_invocation_id
            for bus in list(type(self)._instances):
                task = bus._background_drive_task
                if (
                    task is not None
                    and not task.done()
                    and bus._background_drive_target_event_id is None
                    and event_id in bus._events
                ):
                    bus._background_drive_target_event_id = event_id
        batch_token = _core_outcome_batch_context.set(None)
        try:
            if parent_invocation_id is not None and event.event_status == EventStatus.PENDING:
                with self._targeted_core_drive_scope():
                    await self._apply_messages_and_run_invocations_async(
                        self.core.queue_jump_event(
                            event_id,
                            parent_invocation_id,
                            block_parent_completion=block_parent_completion,
                        )
                    )
                if event.event_status == EventStatus.COMPLETED:
                    return event
            elif parent_invocation_id is not None and event.event_status != EventStatus.PENDING:
                return await self._drive_all_buses_until_event_completed(event_id, drain_pending=False, event_obj=event)
            return await self._drive_all_buses_until_event_completed(event_id, drain_pending=False, event_obj=event)
        finally:
            _core_outcome_batch_context.reset(batch_token)
            if guard_queue_order and parent_invocation_id is None:
                self._queue_jump_event_ids.discard(event_id)
                self._queue_jump_parent_invocation_by_event_id.pop(event_id, None)

    def _find_parent_invocation_for_event(self, event: BaseEvent[Any]) -> str | None:
        result_id = event.event_emitted_by_result_id
        if result_id is None:
            return None
        for bus in list(type(self)._instances):
            invocation_id = bus._invocation_by_result_id.get(result_id)
            if invocation_id is not None:
                return invocation_id
        return None

    def _active_invocation_id(self) -> str | None:
        active_event = self._active_event()
        active_handler_id = self._active_handler_id()
        if active_event is None or active_handler_id is None:
            return None
        active_result = active_event.event_results.get(active_handler_id)
        if active_result is None:
            return None
        for bus in list(type(self)._instances):
            invocation_id = bus._invocation_by_result_id.get(active_result.id)
            if invocation_id is not None:
                return invocation_id
        return None

    async def _drive_all_buses_until_event_completed(
        self,
        event_id: str,
        *,
        drain_pending: bool = True,
        event_obj: BaseEvent[Any] | None = None,
        allow_queue_jump: bool = True,
    ) -> BaseEvent[Any]:
        if event_obj is not None and self._events.get(event_id) is None:
            self._events[event_id] = event_obj
        local_event = event_obj or self._events.get(event_id)
        if event_id in self._local_only_event_ids and local_event is not None:
            if local_event.event_status != EventStatus.COMPLETED and local_event.event_completed_signal is not None:
                await local_event.event_completed_signal.wait()
            return self._completed_snapshot_or_event(local_event)
        deadline = self._event_completion_wait_deadline(event_obj or self._events.get(event_id))
        with self._targeted_core_drive_scope():
            while self._deadline_active(deadline):
                if event_obj is not None and event_obj.event_status == EventStatus.COMPLETED:
                    return (
                        await self._completed_event_after_optional_queue_drain(event_obj)
                        if drain_pending
                        else self._completed_snapshot_or_event(event_obj)
                    )
                await self._flush_related_pending_core_emits_async()
                buses = self._live_buses_for_event(
                    event_id,
                    event_type=event_obj.event_type if event_obj is not None else None,
                )
                completed_event = self._completed_live_event(event_id, buses)
                if completed_event is not None:
                    return (
                        await self._completed_event_after_optional_queue_drain(completed_event)
                        if drain_pending
                        else self._completed_snapshot_or_event(completed_event)
                    )
                if await self._await_active_untargeted_driver_for_event(event_id, buses):
                    continue
                progressed = False
                for bus in buses:
                    event_for_bus = bus._events.get(event_id)
                    if (
                        allow_queue_jump
                        and event_for_bus is not None
                        and any(event_id in candidate._queue_jump_event_ids for candidate in type(self)._instances)
                    ):
                        parent_invocation_id = next(
                            (
                                candidate._queue_jump_parent_invocation_by_event_id[event_id]
                                for candidate in type(self)._instances
                                if event_id in candidate._queue_jump_parent_invocation_by_event_id
                            ),
                            None,
                        ) or bus._find_parent_invocation_for_event(event_for_bus)
                        if parent_invocation_id is not None:
                            queue_jump_messages = bus.core.queue_jump_event(
                                event_id,
                                parent_invocation_id,
                                block_parent_completion=event_for_bus.event_emitted_by_result_id is not None,
                            )
                            if await bus._apply_messages_and_run_invocations_async(queue_jump_messages):
                                progressed = True
                            continue
                    if allow_queue_jump and bus._queue_jump_event_ids and event_id in bus._queue_jump_event_ids:
                        continue
                    messages = bus.core.process_next_route(
                        bus.bus_id,
                        limit=self._CORE_ROUTE_SLICE_LIMIT,
                        compact_response=True,
                    )
                    if messages:
                        progressed = True
                    if await bus._apply_messages_and_run_invocations_async(messages):
                        progressed = True
                completed_event = self._completed_live_event(
                    event_id,
                    event_type=event_obj.event_type if event_obj is not None else None,
                )
                if completed_event is not None:
                    return (
                        await self._completed_event_after_optional_queue_drain(completed_event)
                        if drain_pending
                        else self._completed_snapshot_or_event(completed_event)
                    )
                if not progressed:
                    snapshot = await self._get_event_snapshot_from_core_async(event_id)
                    if isinstance(snapshot, dict):
                        target_event = event_obj or self._events.get(event_id)
                        if target_event is None:
                            target_event = self._event_from_core_record(
                                self._event_types_by_pattern.get(str(snapshot.get('event_type')), '*'),
                                snapshot,
                            )
                        self._events[event_id] = target_event
                        await self._apply_core_snapshot_to_event_async(target_event, snapshot)
                        self.event_history[event_id] = target_event
                        self._trim_event_history_if_needed(force=target_event.event_status == EventStatus.COMPLETED)
                        self._forget_completed_event_if_unretained(target_event)
                        if target_event.event_status == EventStatus.COMPLETED:
                            return (
                                await self._completed_event_after_optional_queue_drain(target_event)
                                if drain_pending
                                else self._completed_snapshot_or_event(target_event)
                            )
                    await asyncio.sleep(0.001)
        raise RuntimeError(f'event did not complete within deadline: {event_id}')

    def _completed_live_event(
        self,
        event_id: str,
        buses: list[RustCoreEventBus] | None = None,
        *,
        event_type: str | None = None,
    ) -> BaseEvent[Any] | None:
        live_buses = buses if buses is not None else self._live_buses_for_event(event_id, event_type=event_type)
        if not live_buses:
            return None
        live_events = [bus._events.get(event_id) for bus in live_buses]
        if any(event is None or event.event_status != EventStatus.COMPLETED for event in live_events):
            return None
        return self._events.get(event_id) or next(event for event in live_events if event is not None)

    def _completed_explicit_await_event_id(self) -> str | None:
        for event_id in list(self._explicit_await_event_ids):
            if self._completed_live_event(event_id) is not None:
                return event_id
        return None

    async def _await_active_untargeted_driver_for_event(self, event_id: str, buses: list[RustCoreEventBus]) -> bool:
        for bus in buses:
            task = bus._background_drive_task
            if task is None or task.done() or task is asyncio.current_task():
                continue
            target_event_id = bus._background_drive_target_event_id
            if target_event_id is not None and target_event_id != event_id:
                continue
            event = bus._events.get(event_id)
            if event is None or event.event_completed_signal is None:
                continue
            try:
                await asyncio.wait_for(event.event_completed_signal.wait(), timeout=0.01)
            except asyncio.CancelledError:
                return False
            except TimeoutError:
                return False
            return event.event_status == EventStatus.COMPLETED
        return False

    async def _wait_for_event_with_shared_core_driver(
        self,
        event: BaseEvent[Any],
        timeout: float | None = None,
    ) -> bool:
        if event.event_status == EventStatus.COMPLETED:
            return True
        if event.event_id not in self.in_flight_event_ids:
            return False
        if event.event_completed_signal is None:
            return False
        if not self._can_schedule_async_processing() or self._suppress_auto_driver > 0:
            return False
        if event.event_emitted_by_result_id is not None or event.event_parent_id is not None:
            return False
        if len(self.in_flight_event_ids) < 8 and len(type(self)._active_explicit_await_event_ids()) < 8:
            return False

        self._ensure_background_driver(None)
        if timeout is None:
            await event.event_completed_signal.wait()
        else:
            await asyncio.wait_for(event.event_completed_signal.wait(), timeout=timeout)
        return event.event_status == EventStatus.COMPLETED

    def _ensure_passive_wait_driver(self) -> None:
        if not self._can_schedule_async_processing() or self._suppress_auto_driver > 0:
            return
        task = self._background_drive_task
        if task is not None and not task.done():
            if self._background_drive_target_event_id is not None:
                self._background_drive_target_event_id = None
            return
        self._ensure_background_driver(None)

    @contextmanager
    def _passive_handler_wait_scope(self) -> Any:
        self._passive_handler_wait_depth += 1
        try:
            yield
        finally:
            self._passive_handler_wait_depth -= 1

    def _can_return_locally_completed_event_without_snapshot(self, event: BaseEvent[Any]) -> bool:
        if self.middlewares:
            return False
        if event.event_id not in self._locally_completed_without_core_patch:
            return False
        if event.event_id in self._drain_pending_on_await_event_ids:
            return False
        if event.event_status != EventStatus.COMPLETED:
            return False
        self._remove_pending_event(event.event_id)
        self.in_flight_event_ids.discard(event.event_id)
        return True

    async def _completed_event_after_optional_queue_drain(self, event: BaseEvent[Any]) -> BaseEvent[Any]:
        completed = await self._completed_snapshot_or_event_async(event)
        if event.event_id not in self._drain_pending_on_await_event_ids:
            return completed
        active_event = self._active_event()
        if active_event is not None and active_event.event_id != event.event_id:
            self._drain_pending_on_await_event_ids.discard(event.event_id)
            return completed
        self._drain_pending_on_await_event_ids.discard(event.event_id)
        if not self._has_incomplete_pending_work(
            buses=self._live_buses_for_event(event.event_id, event_type=event.event_type),
            exclude_event_id=event.event_id,
        ):
            return completed
        background_task = self._background_drive_task
        if background_task is not None and background_task is not asyncio.current_task() and not background_task.done():
            if self._background_drive_target_event_id is not None:
                return completed
            await background_task
            completed = await self._completed_snapshot_or_event_async(event)
            if not self._has_incomplete_pending_work(
                buses=self._live_buses_for_event(event.event_id, event_type=event.event_type),
                exclude_event_id=event.event_id,
            ):
                return completed
        await self._await_core_outcome_commits_for_event(event.event_id, event_type=event.event_type)
        await self._drive_local_until_idle(None, allow_local_terminal_completion=True)
        await self._await_core_outcome_commits_for_event(event.event_id, event_type=event.event_type)
        return completed

    def _has_incomplete_pending_work(
        self,
        *,
        buses: list[RustCoreEventBus] | None = None,
        exclude_event_id: str | None = None,
    ) -> bool:
        for bus in buses or [self]:
            pending_ids = set(bus.in_flight_event_ids)
            pending_ids.update(bus._pending_event_ids)
            for queued_event in bus.pending_event_queue.iter_items():
                pending_ids.add(queued_event.event_id)
            for event_id in pending_ids:
                if event_id == exclude_event_id:
                    continue
                event = bus._events.get(event_id) or bus.event_history.get(event_id)
                if event is not None and event.event_status != EventStatus.COMPLETED:
                    return True
        return False

    async def _await_core_outcome_commits_for_event(self, event_id: str, *, event_type: str | None = None) -> None:
        current_task = asyncio.current_task()
        while True:
            tasks = {
                task
                for bus in self._live_buses_for_event(event_id, event_type=event_type)
                for task in bus._outcome_commit_tasks_by_event_id.get(event_id, set())
                if task is not current_task and not task.done()
            }
            if not tasks:
                return
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _completed_snapshot_or_event_async(self, event: BaseEvent[Any]) -> BaseEvent[Any]:
        self._normalize_error_result_statuses(event)
        expected_handler_ids = self._expected_live_handler_ids(event)
        if (
            event.event_status == EventStatus.COMPLETED
            and all(result.status in ('completed', 'error') for result in event.event_results.values())
            and expected_handler_ids.issubset(event.event_results.keys())
        ):
            self._merge_live_event_metadata(event)
            self.event_history[event.event_id] = event
            self._trim_event_history_if_needed(force=True)
            self._forget_completed_event_if_unretained(event)
            self._remove_pending_event(event.event_id)
            self.in_flight_event_ids.discard(event.event_id)
            await self._on_event_change_once(event, EventStatus.COMPLETED)
            return event
        if (
            event.event_status == EventStatus.COMPLETED
            and expected_handler_ids.issubset(event.event_results.keys())
            and all(event.event_results[handler_id].status in ('completed', 'error') for handler_id in expected_handler_ids)
        ):
            self._merge_live_event_metadata(event)
            self.event_history[event.event_id] = event
            self._trim_event_history_if_needed(force=True)
            self._forget_completed_event_if_unretained(event)
            self._remove_pending_event(event.event_id)
            self.in_flight_event_ids.discard(event.event_id)
            await self._on_event_change_once(event, EventStatus.COMPLETED)
            return event
        if event.event_status != EventStatus.COMPLETED and self._complete_event_if_local_results_terminal(event):
            await self._on_event_change_once(event, EventStatus.COMPLETED)
            return event
        snapshot = await self._get_event_snapshot_from_core_async(event.event_id)
        if isinstance(snapshot, dict):
            self._apply_core_snapshot_to_event(event, snapshot)
        self.event_history[event.event_id] = event
        self._trim_event_history_if_needed(force=event.event_status == EventStatus.COMPLETED)
        self._forget_completed_event_if_unretained(event)
        self._remove_pending_event(event.event_id)
        if event.event_status == EventStatus.COMPLETED:
            self.in_flight_event_ids.discard(event.event_id)
            await self._on_event_change_once(event, EventStatus.COMPLETED)
        return event

    def _merge_live_event_metadata(self, event: BaseEvent[Any]) -> None:
        merged_path = list(event.event_path)
        for bus in list(type(self)._instances):
            if bus._closed:
                continue
            other = bus._events.get(event.event_id)
            if other is None or other is event:
                continue
            for label in other.event_path:
                if label not in merged_path:
                    merged_path.append(label)
            if event.event_parent_id is None and other.event_parent_id is not None:
                event.event_parent_id = other.event_parent_id
            if event.event_emitted_by_handler_id is None and other.event_emitted_by_handler_id is not None:
                event.event_emitted_by_handler_id = other.event_emitted_by_handler_id
            if event.event_emitted_by_result_id is None and other.event_emitted_by_result_id is not None:
                event.event_emitted_by_result_id = other.event_emitted_by_result_id
            if event.event_started_at is None and other.event_started_at is not None:
                event.event_started_at = other.event_started_at
            if event.event_completed_at is None and other.event_completed_at is not None:
                event.event_completed_at = other.event_completed_at
        if merged_path != event.event_path:
            event.event_path = merged_path

    def _live_buses_for_event(self, event_id: str, *, event_type: str | None = None) -> list[RustCoreEventBus]:
        live_core_buses = [bus for bus in list(type(self)._instances) if not bus._closed and bus.core is self.core]
        owning_buses = [bus for bus in live_core_buses if event_id in bus._events]
        if event_type is None:
            event = self._events.get(event_id) or self.event_history.get(event_id)
            if event is not None:
                event_type = event.event_type
        for bus in live_core_buses:
            if event_type is not None:
                break
            event = bus._events.get(event_id) or bus.event_history.get(event_id)
            if event is not None:
                event_type = event.event_type
                break
        if event_type is None:
            snapshot = self._get_event_snapshot_from_core(event_id)
            if isinstance(snapshot, dict) and isinstance(snapshot.get('event_type'), str):
                event_type = cast(str, snapshot['event_type'])
        buses: list[RustCoreEventBus] = []
        seen_bus_ids: set[str] = set()
        for bus in owning_buses:
            buses.append(bus)
            seen_bus_ids.add(bus.bus_id)
        for bus in live_core_buses:
            if bus.bus_id in seen_bus_ids:
                continue
            if event_id in bus._events:
                buses.append(bus)
                seen_bus_ids.add(bus.bus_id)
                continue
            if '*' in bus.handlers_by_key:
                buses.append(bus)
                seen_bus_ids.add(bus.bus_id)
                continue
            if event_type is not None and event_type in bus.handlers_by_key:
                buses.append(bus)
                seen_bus_ids.add(bus.bus_id)
        return buses

    def _completed_snapshot_or_event(self, event: BaseEvent[Any]) -> BaseEvent[Any]:
        snapshot: dict[str, Any] | None = None
        self._normalize_error_result_statuses(event)
        expected_handler_ids = self._expected_live_handler_ids(event)
        if (
            event.event_status == EventStatus.COMPLETED
            and all(result.status in ('completed', 'error') for result in event.event_results.values())
            and expected_handler_ids.issubset(event.event_results.keys())
        ):
            self._merge_live_event_metadata(event)
            self.event_history[event.event_id] = event
            self._trim_event_history_if_needed(force=True)
            self._forget_completed_event_if_unretained(event)
            self._remove_pending_event(event.event_id)
            self.in_flight_event_ids.discard(event.event_id)
            return event
        if (
            event.event_status == EventStatus.COMPLETED
            and expected_handler_ids.issubset(event.event_results.keys())
            and all(event.event_results[handler_id].status in ('completed', 'error') for handler_id in expected_handler_ids)
        ):
            self._merge_live_event_metadata(event)
            self.event_history[event.event_id] = event
            self._trim_event_history_if_needed(force=True)
            self._forget_completed_event_if_unretained(event)
            self._remove_pending_event(event.event_id)
            self.in_flight_event_ids.discard(event.event_id)
            return event
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            snapshot = self._get_event_snapshot_from_core(event.event_id)
            if snapshot is None:
                time.sleep(0.001)
                continue
            raw_results = snapshot.get('event_results')
            result_ids = set(cast(dict[str, Any], raw_results).keys()) if isinstance(raw_results, dict) else set()
            if snapshot.get('event_status') == 'completed' and expected_handler_ids.issubset(result_ids):
                break
            time.sleep(0.001)
        if snapshot is None:
            return event
        self._apply_core_snapshot_to_event(event, snapshot)
        self.event_history[event.event_id] = event
        self._trim_event_history_if_needed(force=event.event_status == EventStatus.COMPLETED)
        self._forget_completed_event_if_unretained(event)
        self._remove_pending_event(event.event_id)
        if event.event_status == EventStatus.COMPLETED:
            self.in_flight_event_ids.discard(event.event_id)
        return event

    def _apply_core_snapshot_to_event(self, event: BaseEvent[Any], snapshot: dict[str, Any]) -> None:
        event_result_type = event.event_result_type
        local_was_complete = event.event_status == EventStatus.COMPLETED or bool(getattr(event, '_event_is_complete_flag', False))
        local_completed_at = event.event_completed_at
        local_event_path = list(event.event_path)
        snapshot = self._normalize_core_event_snapshot(snapshot)
        existing_results: dict[str, EventResult[Any]] = {}
        existing_children: dict[str, list[BaseEvent[Any]]] = {}
        existing_event_results: dict[str, BaseEvent[Any]] = {}
        existing_errors: dict[str, BaseException] = {}
        for result in event.event_results.values():
            existing_results[result.handler_id] = result
            existing_results[result.id] = result
            if result.error is not None:
                existing_errors[result.handler_id] = result.error
                existing_errors[result.id] = result.error
            if result.event_children:
                children = list(result.event_children)
                existing_children[result.handler_id] = children
                existing_children[result.id] = children
            if isinstance(result.result, BaseEvent):
                existing_event_results[result.handler_id] = result.result
                existing_event_results[result.id] = result.result
        raw_child_ids_by_result: dict[str, list[str]] = {}
        raw_results = snapshot.get('event_results')
        if isinstance(raw_results, dict):
            for handler_id, raw_result in cast(dict[str, Any], raw_results).items():
                if not isinstance(raw_result, dict):
                    continue
                child_ids = [
                    child_id for child_id in cast(list[Any], raw_result.get('event_children') or []) if isinstance(child_id, str)
                ]
                if child_ids:
                    raw_child_ids_by_result[str(handler_id)] = child_ids
                    result_id = raw_result.get('id') or raw_result.get('result_id')
                    if isinstance(result_id, str):
                        raw_child_ids_by_result[result_id] = child_ids
        if event_result_type is None:
            event_result_type = pydantic_model_from_json_schema(snapshot.get('event_result_type'))
            event.event_result_type = event_result_type
        event.event_results = self._order_event_results(
            event.event_type,
            self._trusted_event_results_from_core_snapshot(event, snapshot),
        )
        for existing_result in list(existing_results.values()):
            if existing_result.handler_id in event.event_results:
                continue
            event.event_results[existing_result.handler_id] = existing_result
        for result in event.event_results.values():
            existing_result = existing_results.get(result.handler_id) or existing_results.get(result.id)
            if (
                existing_result is not None
                and existing_result.status in ('completed', 'error')
                and (
                    result.status in ('pending', 'started')
                    or (
                        existing_result.status == 'completed'
                        and result.status == 'completed'
                        and result.result is None
                        and existing_result.result is not None
                    )
                )
            ):
                result.status = existing_result.status
                result.result = existing_result.result
                result.error = existing_result.error
                result.started_at = existing_result.started_at
                result.completed_at = existing_result.completed_at
                result.timeout = existing_result.timeout
                result.event_children = list(existing_result.event_children)
            self._event_id_by_result_id[result.id] = event.event_id
            result.result_type = event_result_type
            existing_error = existing_errors.get(result.handler_id) or existing_errors.get(result.id)
            if existing_error is not None:
                result.error = existing_error
            children = existing_children.get(result.handler_id) or existing_children.get(result.id)
            if children:
                result.event_children = children
            self._attach_result_children(
                event,
                result,
                raw_child_ids=raw_child_ids_by_result.get(result.id) or raw_child_ids_by_result.get(result.handler_id),
            )
        if isinstance(raw_results, dict):
            for handler_id, raw_result in cast(dict[str, Any], raw_results).items():
                if not isinstance(raw_result, dict):
                    continue
                result = event.event_results.get(handler_id)
                if result is None:
                    continue
                if handler_id in existing_errors or result.id in existing_errors:
                    continue
                normalized_error = self._normalize_core_error(raw_result.get('error'))
                if normalized_error is not None:
                    result.error = normalized_error
        event.event_path = [entry for entry in cast(list[Any], snapshot.get('event_path') or []) if isinstance(entry, str)]
        for label in local_event_path:
            if label not in event.event_path:
                event.event_path.append(label)
        event.event_parent_id = snapshot.get('event_parent_id') if isinstance(snapshot.get('event_parent_id'), str) else None
        event.event_emitted_by_handler_id = (
            snapshot.get('event_emitted_by_handler_id') if isinstance(snapshot.get('event_emitted_by_handler_id'), str) else None
        )
        event.event_emitted_by_result_id = (
            snapshot.get('event_emitted_by_result_id') if isinstance(snapshot.get('event_emitted_by_result_id'), str) else None
        )
        event.event_blocks_parent_completion = snapshot.get('event_blocks_parent_completion') is True
        event_started_at = snapshot.get('event_started_at')
        event_completed_at = snapshot.get('event_completed_at')
        event.event_started_at = event_started_at if isinstance(event_started_at, str) else None
        event.event_completed_at = event_completed_at if isinstance(event_completed_at, str) else None
        event_status = snapshot.get('event_status')
        if event_status in ('pending', 'started', 'completed'):
            event.event_status = EventStatus(event_status)
        if self._event_has_unfinished_results(event):
            event.event_status = (
                EventStatus.STARTED
                if any(result.status == 'started' for result in event.event_results.values())
                else EventStatus.PENDING
            )
            event._event_is_complete_flag = False  # pyright: ignore[reportPrivateUsage]
            event.event_completed_at = None
            self.in_flight_event_ids.add(event.event_id)
            on_idle = self._on_idle
            if on_idle is not None:
                on_idle.clear()
        if local_was_complete and event.event_status != EventStatus.COMPLETED:
            event.event_completed_at = local_completed_at or event.event_completed_at or monotonic_datetime()
            if event.event_started_at is None:
                event.event_started_at = event.event_completed_at
            event.event_status = EventStatus.COMPLETED
        if not event.event_results and event.event_status == EventStatus.COMPLETED and event.event_completed_at is not None:
            event.event_started_at = event.event_completed_at
        for result in event.event_results.values():
            result_value = result.result
            if (
                isinstance(result_value, dict)
                and isinstance(result_value.get('event_id'), str)
                and isinstance(result_value.get('event_type'), str)
            ):
                result.result = (
                    existing_event_results.get(result.handler_id)
                    or existing_event_results.get(result.id)
                    or BaseEvent[Any].model_validate(result_value)
                )
            elif result.status == 'completed' and result.error is None and result.result is not None:
                result.update(result=result.result)
            if result.status in ('completed', 'error'):
                completed_signal = result.handler_completed_signal
                if completed_signal is not None:
                    completed_signal.set()
        self._normalize_error_result_statuses(event)
        if event.event_status == EventStatus.COMPLETED:
            event._event_is_complete_flag = True  # pyright: ignore[reportPrivateUsage]
            self._cancel_event_slow_warning_timer(event.event_id)
            self._remove_pending_event(event.event_id)
            self.in_flight_event_ids.discard(event.event_id)
            self._trim_event_history_if_needed(force=True)
            self._forget_completed_event_if_unretained(event)
            completed_signal = event.event_completed_signal
            if completed_signal is not None:
                completed_signal.set()

    def _trusted_event_results_from_core_snapshot(
        self,
        event: BaseEvent[Any],
        snapshot: dict[str, Any],
    ) -> dict[str, EventResult[Any]]:
        raw_results = snapshot.get('event_results')
        if not isinstance(raw_results, dict):
            return {}
        results: dict[str, EventResult[Any]] = {}
        for handler_id, raw_result in cast(dict[str, Any], raw_results).items():
            if not isinstance(raw_result, dict):
                continue
            result = self._trusted_event_result_from_core_record(event, str(handler_id), raw_result)
            if result is not None:
                results[result.handler_id] = result
        return results

    def _trusted_event_result_from_core_record(
        self,
        event: BaseEvent[Any],
        handler_id: str,
        record: dict[str, Any],
    ) -> EventResult[Any] | None:
        result_id = record.get('id') or record.get('result_id')
        if not isinstance(result_id, str):
            return None
        handler = self._handlers.get(handler_id)
        if handler is None:
            raw_registered_at = record.get('handler_registered_at')
            handler = EventHandler.model_construct(
                id=handler_id,
                handler=None,
                handler_name=record.get('handler_name') if isinstance(record.get('handler_name'), str) else 'anonymous',
                handler_file_path=(record.get('handler_file_path') if isinstance(record.get('handler_file_path'), str) else None),
                handler_timeout=record.get('handler_timeout') if isinstance(record.get('handler_timeout'), int | float) else None,
                handler_slow_timeout=(
                    record.get('handler_slow_timeout') if isinstance(record.get('handler_slow_timeout'), int | float) else None
                ),
                handler_registered_at=raw_registered_at if isinstance(raw_registered_at, str) else event.event_created_at,
                event_pattern=record.get('handler_event_pattern')
                if isinstance(record.get('handler_event_pattern'), str)
                else event.event_type,
                eventbus_name=record.get('eventbus_name') if isinstance(record.get('eventbus_name'), str) else self.name,
                eventbus_id=record.get('eventbus_id') if isinstance(record.get('eventbus_id'), str) else self.bus_id,
            )
        status = record.get('status')
        if status == 'cancelled':
            status = 'error'
        if status not in ('pending', 'started', 'completed', 'error'):
            status = 'pending'
        started_at = record.get('started_at')
        completed_at = record.get('completed_at')
        return EventResult[Any].model_construct(
            id=result_id,
            status=status,
            event_id=event.event_id,
            handler=handler,
            result_type=event.event_result_type,
            timeout=record.get('timeout') if isinstance(record.get('timeout'), int | float) else None,
            started_at=started_at if isinstance(started_at, str) else None,
            result=record.get('result') if 'result' in record else None,
            error=None,
            completed_at=completed_at if isinstance(completed_at, str) else None,
            event_children=[],
        )

    async def _apply_core_snapshot_to_event_async(self, event: BaseEvent[Any], snapshot: dict[str, Any]) -> None:
        old_event_status = event.event_status
        old_result_statuses = {handler_id: result.status for handler_id, result in event.event_results.items()}
        self._apply_core_snapshot_to_event(event, snapshot)
        if not self.middlewares:
            return
        for handler_id, result in event.event_results.items():
            old_status = old_result_statuses.get(handler_id)
            if old_status == result.status:
                continue
            status = EventStatus.COMPLETED if result.status in ('completed', 'error') else EventStatus(result.status)
            await self._on_event_result_change_once(event, result, status)
        if old_event_status != event.event_status or event.event_status == EventStatus.COMPLETED:
            await self._on_event_change_once(event, event.event_status)

    def _expected_local_handler_ids(self, event_type: str) -> set[str]:
        expected: set[str] = set()
        for bus in list(type(self)._instances):
            if bus.bus_id != self.bus_id:
                continue
            expected.update(bus.handlers_by_key.get('*', []))
            expected.update(bus.handlers_by_key.get(event_type, []))
        return expected

    def _expected_live_handler_ids(self, event: BaseEvent[Any]) -> set[str]:
        expected: set[str] = set()
        live_buses = self._live_buses_for_event(event.event_id, event_type=event.event_type)
        if not live_buses:
            return self._expected_local_handler_ids(event.event_type)
        for bus in live_buses:
            expected.update(bus.handlers_by_key.get('*', []))
            expected.update(bus.handlers_by_key.get(event.event_type, []))
        return expected

    def _order_event_results(self, event_type: str, results: dict[str, EventResult[Any]]) -> dict[str, EventResult[Any]]:
        local_order: dict[str, int] = {}
        ordered_handler_ids = [
            *self.handlers_by_key.get(event_type, []),
            *self.handlers_by_key.get('*', []),
        ]
        for index, handler_id in enumerate(ordered_handler_ids):
            local_order.setdefault(handler_id, index)
        return dict(
            sorted(
                results.items(),
                key=lambda item: (local_order.get(item[0], len(local_order)), item[1].handler.handler_registered_at, item[0]),
            )
        )

    @overload
    async def find(
        self,
        event_type: type[T_Event],
        where: Callable[[T_Event], bool] | None = None,
        child_of: BaseEvent[Any] | None = None,
        past: bool | float | timedelta | None = None,
        future: bool | float | None = None,
        **event_fields: Any,
    ) -> T_Event | None: ...

    @overload
    async def find(
        self,
        event_type: str | Literal['*'],
        where: Callable[[BaseEvent[Any]], bool] | None = None,
        child_of: BaseEvent[Any] | None = None,
        past: bool | float | timedelta | None = None,
        future: bool | float | None = None,
        **event_fields: Any,
    ) -> BaseEvent[Any] | None: ...

    async def find(
        self,
        event_type: str | Literal['*'] | type[T_Event],
        where: Callable[[T_Event], bool] | None = None,
        child_of: BaseEvent[Any] | None = None,
        past: bool | float | timedelta | None = None,
        future: bool | float | None = None,
        **event_fields: Any,
    ) -> T_Event | BaseEvent[Any] | None:
        self._raise_if_destroyed()
        if 'limit' in event_fields:
            limit_field_value = event_fields.pop('limit')
            prior_where = where

            def where_with_limit_field(event: T_Event) -> bool:
                return getattr(event, 'limit', object()) == limit_field_value and (prior_where is None or prior_where(event))

            where = where_with_limit_field
        results = await self.filter(event_type, where, child_of=child_of, past=past, future=future, limit=1, **event_fields)
        return results[0] if results else None

    async def filter(
        self,
        event_type: str | Literal['*'] | type[T_Event],
        where: Callable[[T_Event], bool] | None = None,
        *,
        child_of: BaseEvent[Any] | None = None,
        past: bool | float | timedelta | None = None,
        future: bool | float | None = None,
        limit: int | None = None,
        **event_fields: Any,
    ) -> list[T_Event | BaseEvent[Any]]:
        self._raise_if_destroyed()
        if limit is not None and limit <= 0:
            return []
        if child_of is None and isinstance(event_fields.get('child_of'), BaseEvent):
            child_of = cast(BaseEvent[Any], event_fields.pop('child_of'))
        if 'past' in event_fields:
            past = cast(bool | float | timedelta | None, event_fields.pop('past'))
        if 'future' in event_fields:
            future = cast(bool | float | None, event_fields.pop('future'))
        resolved_past_input = True if past is None else past
        if isinstance(resolved_past_input, timedelta):
            resolved_past: bool | float = max(0.0, resolved_past_input.total_seconds())
        elif isinstance(resolved_past_input, bool):
            resolved_past = resolved_past_input
        else:
            resolved_past = max(0.0, float(resolved_past_input))
        resolved_future_input = False if future is None else future
        if isinstance(resolved_future_input, bool):
            resolved_future: bool | float = resolved_future_input
        else:
            resolved_future = max(0.0, float(resolved_future_input))
        if resolved_past is False and resolved_future is False:
            return []
        if self.event_history.max_history_size == 0:
            resolved_past = False
        event_pattern = EventHistory.normalize_event_pattern(event_type)
        initial_seen_ids: set[str] = set()
        if resolved_past is False:
            initial_seen_ids.update(self._events.keys())
            for record in self.core.list_events(event_pattern, bus_id=self.bus_id):
                event_id = record.get('event_id') if isinstance(record, dict) else None
                if isinstance(event_id, str):
                    initial_seen_ids.add(event_id)
        await self._flush_pending_core_emits_async()
        cutoff: str | None = None
        if resolved_past is not True and resolved_past is not False:
            cutoff_dt = datetime.fromisoformat(monotonic_datetime().replace('Z', '+00:00')) - timedelta(
                seconds=float(resolved_past)
            )
            cutoff = monotonic_datetime(cutoff_dt.isoformat().replace('+00:00', 'Z'))
        can_limit_core_history = limit is not None and cutoff is None and child_of is None and not event_fields and where is None

        def event_matches(event_obj: T_Event | BaseEvent[Any]) -> bool:
            return (
                (event_pattern == '*' or event_obj.event_type == event_pattern)
                and self._core_record_belongs_to_bus(event_obj)
                and (cutoff is None or event_obj.event_created_at >= cutoff)
                and (child_of is None or self._event_is_child_of(event_obj, child_of))
                and all(getattr(event_obj, field_name, object()) == expected for field_name, expected in event_fields.items())
                and (where is None or where(cast(T_Event, event_obj)))
            )

        async def ready_candidate(
            event_obj: T_Event | BaseEvent[Any],
            *,
            already_matched: bool = False,
        ) -> T_Event | BaseEvent[Any] | None:
            local_event = self._events.get(event_obj.event_id) or self.event_history.get(event_obj.event_id)
            if local_event is not None:
                event_obj = local_event
            if not already_matched and not event_matches(event_obj):
                return None
            if (
                resolved_future is False
                and event_obj.event_status != EventStatus.COMPLETED
                and not event_obj.event_results
                and self._active_event() is None
                and not any(bus._passive_handler_wait_depth > 0 for bus in list(type(self)._instances) if not bus._closed)
                and self._expected_live_handler_ids(event_obj)
            ):
                try:
                    event_obj = await self._drive_all_buses_until_event_completed(
                        event_obj.event_id,
                        drain_pending=False,
                        event_obj=event_obj,
                    )
                except RuntimeError:
                    return None
            if (
                event_obj.event_status == EventStatus.COMPLETED
                and event_obj.event_id in self._locally_completed_without_core_patch
            ):
                expected_handler_ids = self._expected_live_handler_ids(event_obj)
                if expected_handler_ids and not expected_handler_ids.issubset(event_obj.event_results.keys()):
                    await self._await_core_outcome_commits_for_event(event_obj.event_id, event_type=event_obj.event_type)
                    event_obj = await self._completed_snapshot_or_event_async(event_obj)
            return event_obj

        matches: list[T_Event | BaseEvent[Any]] = []
        seen_ids = set(initial_seen_ids)
        if resolved_past is not False:
            seen_ids.update(self.event_history.keys())
        if resolved_past is not False:
            for event_obj in reversed(list(self.event_history.values())):
                if event_obj.event_id in initial_seen_ids or not event_matches(event_obj):
                    continue
                ready_event = await ready_candidate(event_obj, already_matched=True)
                if ready_event is None:
                    continue
                matches.append(ready_event)
                if limit is not None and len(matches) >= limit:
                    return matches
        records = (
            []
            if resolved_past is False
            else self.core.list_events(event_pattern, limit if can_limit_core_history else None, self.bus_id)
        )
        for record in records:
            event_obj = self._event_from_core_record(event_type, record)
            if event_obj.event_id in seen_ids or not event_matches(event_obj):
                continue
            ready_event = await ready_candidate(event_obj, already_matched=True)
            if ready_event is None:
                continue
            matches.append(ready_event)
            seen_ids.add(event_obj.event_id)
            if limit is not None and len(matches) >= limit:
                return matches
        if resolved_future is False:
            return matches
        waiter: asyncio.Future[BaseEvent[Any] | None] | None = None
        waiter_matcher: Callable[[BaseEvent[Any]], bool] | None = None
        if limit == 1:
            waiter = asyncio.get_running_loop().create_future()

            def waiter_matcher(event_obj: BaseEvent[Any]) -> bool:
                if event_obj.event_id in seen_ids:
                    return False
                seen_ids.add(event_obj.event_id)
                return event_matches(event_obj)

            self._find_waiters.append((waiter_matcher, waiter))
        deadline = None if resolved_future is True else time.monotonic() + float(resolved_future)
        try:
            while deadline is None or time.monotonic() <= deadline:
                if waiter is not None and waiter.done():
                    event_obj = waiter.result()
                    if event_obj is None:
                        return matches
                    ready_event = await ready_candidate(event_obj, already_matched=True)
                    if ready_event is not None:
                        matches.append(ready_event)
                    return matches
                for event_obj in reversed(list(self._events.values())):
                    if event_obj.event_id in seen_ids:
                        continue
                    seen_ids.add(event_obj.event_id)
                    if not event_matches(event_obj):
                        continue
                    ready_event = await ready_candidate(event_obj, already_matched=True)
                    if ready_event is None:
                        continue
                    matches.append(ready_event)
                    if limit is not None and len(matches) >= limit:
                        return matches
                for record in self.core.list_events(event_pattern, bus_id=self.bus_id):
                    event_obj = self._event_from_core_record(event_type, record)
                    if event_obj.event_id in seen_ids:
                        continue
                    seen_ids.add(event_obj.event_id)
                    if not event_matches(event_obj):
                        continue
                    ready_event = await ready_candidate(event_obj, already_matched=True)
                    if ready_event is None:
                        continue
                    matches.append(ready_event)
                    if limit is not None and len(matches) >= limit:
                        return matches
                progressed = await self._process_available_routes_once()
                if progressed:
                    for event_obj in reversed(list(self._events.values())):
                        if event_obj.event_id in seen_ids:
                            continue
                        seen_ids.add(event_obj.event_id)
                        if not event_matches(event_obj):
                            continue
                        ready_event = await ready_candidate(event_obj, already_matched=True)
                        if ready_event is None:
                            continue
                        matches.append(ready_event)
                        if limit is not None and len(matches) >= limit:
                            return matches
                    for record in self.core.list_events(event_pattern, bus_id=self.bus_id):
                        event_obj = self._event_from_core_record(event_type, record)
                        if event_obj.event_id in seen_ids:
                            continue
                        seen_ids.add(event_obj.event_id)
                        if not event_matches(event_obj):
                            continue
                        ready_event = await ready_candidate(event_obj, already_matched=True)
                        if ready_event is None:
                            continue
                        matches.append(ready_event)
                        if limit is not None and len(matches) >= limit:
                            return matches
                if waiter is not None and not waiter.done():
                    wait_timeout = 0.05 if resolved_past is False else 0.005
                    if deadline is not None:
                        wait_timeout = max(0.0, min(wait_timeout, deadline - time.monotonic()))
                    if wait_timeout > 0:
                        try:
                            await asyncio.wait_for(asyncio.shield(waiter), timeout=wait_timeout)
                        except TimeoutError:
                            pass
                    if waiter.done():
                        event_obj = waiter.result()
                        if event_obj is None:
                            return matches
                        ready_event = await ready_candidate(cast(T_Event | BaseEvent[Any], event_obj), already_matched=True)
                        if ready_event is not None:
                            matches.append(ready_event)
                        return matches
                if waiter is not None and waiter.done():
                    event_obj = waiter.result()
                    if event_obj is None:
                        return matches
                    ready_event = await ready_candidate(cast(T_Event | BaseEvent[Any], event_obj), already_matched=True)
                    if ready_event is not None:
                        matches.append(ready_event)
                    return matches
                await asyncio.sleep(0.001)
            return matches
        finally:
            if waiter is not None:
                self._find_waiters = [(matcher, future) for matcher, future in self._find_waiters if future is not waiter]

    async def _process_available_routes_once(self) -> bool:
        progressed = False
        for bus in list(type(self)._instances):
            if bus._closed:
                continue
            if bus is not self and bus.core is not self.core:
                continue
            if await bus._flush_pending_core_emits_async():
                progressed = True
            messages = bus.core.process_next_route(
                bus.bus_id,
                limit=self._CORE_ROUTE_SLICE_LIMIT,
                compact_response=True,
            )
            if messages:
                progressed = True
            if await bus._apply_messages_and_run_invocations_async(messages):
                progressed = True
        return progressed

    def _resolve_find_waiters(self, event: BaseEvent[Any]) -> None:
        if not self._find_waiters:
            return
        remaining: list[tuple[Callable[[BaseEvent[Any]], bool], asyncio.Future[BaseEvent[Any] | None]]] = []
        for matcher, waiter in self._find_waiters:
            if waiter.done():
                continue
            if matcher(event):
                waiter.set_result(event)
            else:
                remaining.append((matcher, waiter))
        self._find_waiters = remaining

    async def _process_local_routes_once(self) -> bool:
        progressed = False
        if await self._flush_pending_core_emits_async():
            progressed = True
        messages = self.core.process_next_route(
            self.bus_id,
            limit=self._CORE_ROUTE_SLICE_LIMIT,
            compact_response=True,
        )
        if messages:
            progressed = True
        if await self._apply_messages_and_run_invocations_async(messages):
            progressed = True
        return progressed

    def _apply_messages_and_run_invocations(self, messages: list[dict[str, Any]]) -> bool:
        progressed = bool(messages)
        for message in messages:
            if not self._invocation_messages_from_core_message(message):
                self._apply_core_message(message)
        for invocation in self._invocation_messages_from_core_messages(messages):
            bus = self._bus_for_invocation(invocation)
            produced = bus._run_invocation(invocation)
            if produced:
                progressed = True
            for produced_message in produced:
                if not bus._invocation_messages_from_core_message(produced_message):
                    bus._apply_core_message(produced_message)
        return progressed

    def _bus_for_invocation(self, invocation: dict[str, Any]) -> RustCoreEventBus:
        handler_id = invocation.get('handler_id')
        if isinstance(handler_id, str) and handler_id in self._handlers:
            return self
        bus_id = invocation.get('bus_id')
        for bus in list(type(self)._instances):
            if bus._closed:
                continue
            if bus_id is not None and bus.bus_id != bus_id:
                continue
            if isinstance(handler_id, str) and handler_id in bus._handlers:
                return bus
        return self

    def _core_record_belongs_to_bus(self, event: BaseEvent[Any]) -> bool:
        if self.label in event.event_path:
            return True
        return any(
            result.eventbus_id == self.bus_id or result.eventbus_name == self.name for result in event.event_results.values()
        )

    def _event_is_child_of(self, event: BaseEvent[Any], ancestor: BaseEvent[Any]) -> bool:
        current_id = event.event_parent_id
        visited: set[str] = set()
        while current_id and current_id not in visited:
            if current_id == ancestor.event_id:
                return True
            visited.add(current_id)
            parent = self._events.get(current_id) or self.event_history.get(current_id)
            if parent is None:
                snapshot = self._get_event_snapshot_from_core(current_id)
                if not isinstance(snapshot, dict):
                    return False
                parent = self._event_from_core_record(
                    self._event_types_by_pattern.get(str(snapshot.get('event_type')), '*'),
                    snapshot,
                )
            current_id = parent.event_parent_id
        return False

    def event_is_child_of(self, event: BaseEvent[Any], ancestor: BaseEvent[Any]) -> bool:
        return self._event_is_child_of(event, ancestor)

    def event_is_parent_of(self, event: BaseEvent[Any], descendant: BaseEvent[Any]) -> bool:
        return self._event_is_child_of(descendant, event)

    def _collect_async_tasks(self) -> list[asyncio.Future[Any]]:
        current_task: asyncio.Task[Any] | None = None
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            pass
        tasks: list[asyncio.Future[Any]] = []
        seen: set[asyncio.Future[Any]] = set()

        def add_task(task: asyncio.Future[Any] | None) -> None:
            if task is None or task is current_task or task in seen:
                return
            seen.add(task)
            tasks.append(task)

        for task in self._processing_tasks | self._local_invocation_tasks | self._middleware_hook_tasks:
            add_task(task)
        for event_tasks in self._outcome_commit_tasks_by_event_id.values():
            for task in event_tasks:
                add_task(task)
        add_task(self._background_drive_task)
        return tasks

    def _cancel_async_tasks(self) -> list[asyncio.Future[Any]]:
        tasks = self._collect_async_tasks()
        for task in tasks:
            if not task.done():
                task.cancel()
        return tasks

    def _raise_if_destroyed(self) -> None:
        if self._destroyed:
            raise RuntimeError(f'{self} has been destroyed')

    def _resolve_find_waiters_on_destroy(self) -> None:
        waiters = self._find_waiters
        self._find_waiters = []
        for _matcher, waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)  # type: ignore[arg-type]

    async def destroy(
        self,
        timeout: float | dict[str, Any] | None = None,
        clear: bool = True,
    ) -> None:
        if isinstance(timeout, dict):
            options = timeout
            timeout = cast(float | None, options.get('timeout', 0))
            clear = bool(options.get('clear', clear))
        if clear:
            await self.stop(timeout=timeout, clear=True)
            self._destroyed = True
            self._closed = True
            self._is_running = False
            self._runloop_task = None
            self._on_idle = None  # type: ignore[assignment]
            self.pending_event_queue = None  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
            self._resolve_find_waiters_on_destroy()
            return

        if timeout is not None and timeout > 0:
            await self.wait_until_idle(timeout=timeout)
        self._cancel_async_tasks()
        self._closed = True
        self._destroyed = True
        self._is_running = False
        self._runloop_task = None
        if self._on_idle is None:  # type: ignore[comparison-overlap]
            self._on_idle = asyncio.Event()
        self._on_idle.set()
        self.pending_event_queue = None  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
        self._pending_event_ids.clear()
        self._processing_event_ids.clear()
        self.in_flight_event_ids = {
            event_id for event_id, event in self._events.items() if event.event_status != EventStatus.COMPLETED
        }
        self._resolve_find_waiters_on_destroy()
        type(self)._instances.discard(self)
        all_instances = getattr(type(self), 'all_instances', None)
        if all_instances is not None:
            all_instances.discard(self)

    def disconnect(self) -> None:
        self._cancel_async_tasks()
        release_core = self._core_finalizer.alive
        self._closed = True
        self._is_running = False
        self._runloop_task = None
        if self._on_idle is not None:  # type: ignore[comparison-overlap]
            self._on_idle.set()
        type(self)._instances.discard(self)
        all_instances = getattr(type(self), 'all_instances', None)
        if all_instances is not None:
            all_instances.discard(self)
        self._cancel_all_async_handler_slow_warnings()
        for event_id in list(self._event_slow_warning_timers):
            self._cancel_event_slow_warning_timer(event_id)
        if not release_core:
            self._registered_core_handler_ids.clear()
            return
        skip_unregister = False
        try:
            if not skip_unregister:
                for handler_id in list(self._registered_core_handler_ids):
                    try:
                        self.core.unregister_handler(handler_id)
                    except Exception:
                        pass
                try:
                    self.core.unregister_bus(self.bus_id)
                except Exception:
                    pass
            if self._uses_shared_core:
                RustCoreClient.release_named(self.core)
            elif self._owns_core_transport:
                try:
                    self.core.disconnect_host()
                except Exception:
                    pass
                self.core.disconnect()
        finally:
            self._registered_core_handler_ids.clear()
            self._core_finalizer.detach()

    def close(self) -> None:
        self.disconnect()

    async def wait_until_idle(self, timeout: float | None = None) -> None:
        await self._flush_pending_core_emits_async()
        if (
            not self.in_flight_event_ids
            and not self._pending_event_ids
            and self.pending_event_queue.qsize() == 0
            and not self._active_local_tasks()
        ):
            if await self._process_own_core_route_work_once_async():
                pass
            else:
                self._mark_idle()
                return
        live_core_buses = [bus for bus in list(type(self)._instances) if not bus._closed and bus.core is self.core]
        has_related_bus_pending = len(live_core_buses) > 1 and any(
            len(self._live_buses_for_event(event_id)) > 1 for event_id in self.in_flight_event_ids
        )
        background_task = self._background_drive_task
        if background_task is not None and not background_task.done() and not has_related_bus_pending:
            if timeout is None:
                await asyncio.gather(background_task, return_exceptions=True)
            else:
                await asyncio.wait({background_task}, timeout=max(0, timeout))
            if not background_task.done():
                return
            for event_id, event in list(self._events.items()):
                if event.event_status == EventStatus.COMPLETED:
                    self.in_flight_event_ids.discard(event_id)
                    continue
                snapshot = await self._get_event_snapshot_from_core_async(event_id)
                if isinstance(snapshot, dict):
                    await self._apply_core_snapshot_to_event_async(event, snapshot)
            if not any(event.event_status != EventStatus.COMPLETED for event in self._events.values()):
                self._mark_idle()
                return
        if self._background_worker:
            idle = await asyncio.to_thread(self.core.wait_bus_idle, self.bus_id, timeout)
            if idle:
                for event_id, event in list(self._events.items()):
                    if event.event_status == EventStatus.COMPLETED:
                        self.in_flight_event_ids.discard(event_id)
                        continue
                    snapshot = await self._get_event_snapshot_from_core_async(event_id)
                    if isinstance(snapshot, dict):
                        await self._apply_core_snapshot_to_event_async(event, snapshot)
                self._mark_idle()
            return
        deadline = None if timeout is None else time.monotonic() + max(0, timeout)
        while True:
            active_tasks = self._active_local_tasks()
            if any(len(self._live_buses_for_event(event_id)) > 1 for event_id in self.in_flight_event_ids):
                active_tasks.discard(self._background_drive_task)
            if active_tasks:
                remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
                if remaining == 0.0:
                    return
                done, _ = await asyncio.wait(active_tasks, timeout=remaining, return_when=asyncio.FIRST_COMPLETED)
                if not done:
                    return
                await asyncio.gather(*done, return_exceptions=True)
                continue
            for event_id in list(self.in_flight_event_ids):
                event = self._events.get(event_id)
                if event is None or event.event_status == EventStatus.COMPLETED:
                    self.in_flight_event_ids.discard(event_id)
            pending_event_ids = set(self.in_flight_event_ids)
            if not pending_event_ids:
                if await self._process_own_core_route_work_once_async():
                    continue
                self._mark_idle()
                return
            buses_by_id: dict[str, RustCoreEventBus] = {}
            for event_id in pending_event_ids:
                for bus in self._live_buses_for_event(event_id):
                    buses_by_id[bus.bus_id] = bus
            for bus in list(type(self)._instances):
                if not bus._closed and bus.core is self.core:
                    buses_by_id.setdefault(bus.bus_id, bus)
            buses = list(buses_by_id.values())
            if not buses:
                self._mark_idle()
                return
            progressed = False
            for bus in buses:
                messages = bus.core.process_next_route(
                    bus.bus_id,
                    limit=self._CORE_ROUTE_SLICE_LIMIT,
                    compact_response=True,
                )
                if messages:
                    progressed = True
                if await bus._apply_messages_and_run_invocations_async(messages):
                    progressed = True
            current_pending_event_ids = {
                event.event_id for event in self._events.values() if event.event_status != EventStatus.COMPLETED
            }
            if not current_pending_event_ids:
                if await self._process_own_core_route_work_once_async():
                    continue
                self._mark_idle()
                return
            if not current_pending_event_ids.issubset(pending_event_ids):
                progressed = True
                continue
            pending_event_ids = current_pending_event_ids
            all_complete = True
            for event_id in pending_event_ids:
                snapshot = await self._get_event_snapshot_from_core_async(event_id)
                if not isinstance(snapshot, dict):
                    all_complete = False
                    continue
                if snapshot.get('event_status') != 'completed':
                    all_complete = False
                for bus in self._live_buses_for_event(event_id):
                    event = bus._events.get(event_id)
                    if event is not None:
                        await bus._apply_core_snapshot_to_event_async(event, snapshot)
                    if event is not None and event.event_status == EventStatus.COMPLETED:
                        bus.in_flight_event_ids.discard(event_id)
                    else:
                        all_complete = False
                        if event is not None:
                            bus.in_flight_event_ids.add(event_id)
            if all_complete:
                self._mark_idle()
                return
            if deadline is not None and time.monotonic() >= deadline:
                return
            if not progressed:
                await asyncio.sleep(0.001)

    def is_idle(self) -> bool:
        self._flush_pending_core_emits_sync()
        return self.core.wait_bus_idle(self.bus_id, 0)

    def stop_sync(self) -> None:
        self._cancel_async_tasks()
        release_core = self._core_finalizer.alive
        self._closed = True
        self._is_running = False
        self._runloop_task = None
        if self._on_idle is not None:  # type: ignore[comparison-overlap]
            self._on_idle.set()
        self.name = f'_stopped_{self.id[-8:]}'
        type(self)._instances.discard(self)
        self._cancel_all_async_handler_slow_warnings()
        for event_id in list(self._event_slow_warning_timers):
            self._cancel_event_slow_warning_timer(event_id)
        all_instances = getattr(type(self), 'all_instances', None)
        if all_instances is not None:
            all_instances.discard(self)
        if not release_core:
            self._registered_core_handler_ids.clear()
            return
        skip_unregister = False
        try:
            if not skip_unregister:
                for handler_id in list(self._handlers):
                    try:
                        self.core.unregister_handler(handler_id)
                    except Exception:
                        pass
            self._registered_core_handler_ids.clear()
            if not skip_unregister:
                try:
                    self.core.unregister_bus(self.bus_id)
                except Exception:
                    pass
            try:
                if self._uses_shared_core:
                    RustCoreClient.release_named(self.core)
                elif self._owns_core_transport:
                    self.core.stop()
            except Exception:
                pass
        finally:
            self._core_finalizer.detach()

    def _clear_local_state_after_core_stop(self) -> None:
        dict.clear(self._handlers)
        self._handler_core_options.clear()
        self._registered_core_handler_ids.clear()
        self._registered_core_handler_records.clear()
        self.handlers_by_key.clear()
        self._handler_names_by_key.clear()
        self._events.clear()
        self._event_ids_without_results.clear()
        self.event_history.clear()
        self.pending_event_queue = CleanShutdownQueue(maxsize=0)
        self._pending_core_emits.clear()
        self._scheduled_core_messages.clear()
        self._scheduled_core_messages_task = None
        self._pending_event_ids.clear()
        self.in_flight_event_ids.clear()
        self._event_id_by_result_id.clear()
        self._child_event_ids_by_result_id.clear()
        self._child_event_ids_by_parent_handler.clear()
        self._cancel_all_async_handler_slow_warnings()
        for event_id in list(self._event_slow_warning_timers):
            self._cancel_event_slow_warning_timer(event_id)

    def stop(self, timeout: float | None = None, clear: bool = False) -> _CompletedAwaitable:
        tasks = self._cancel_async_tasks()
        if timeout is not None and timeout > 0 and self._core_finalizer.alive:
            try:
                self.core.wait_bus_idle(self.bus_id, timeout)
            except Exception:
                pass
        self.stop_sync()
        cleanup = self._clear_local_state_after_core_stop if clear else None
        return _CompletedAwaitable(tasks, cleanup=cleanup)

    def model_dump(self) -> dict[str, Any]:
        event_history: dict[str, dict[str, Any]] = {}
        for event_id, event in self.event_history.items():
            event_history[event_id] = event.model_dump(mode='json')
        for event in self.pending_event_queue.iter_items():
            event_history.setdefault(event.event_id, event.model_dump(mode='json'))
        return {
            'id': self.id,
            'name': self.name,
            'max_history_size': self.event_history.max_history_size,
            'max_history_drop': self.event_history.max_history_drop,
            'event_concurrency': self.event_concurrency,
            'event_timeout': self.event_timeout,
            'event_slow_timeout': self.event_slow_timeout,
            'event_handler_concurrency': self.event_handler_concurrency,
            'event_handler_completion': self.event_handler_completion,
            'event_handler_timeout': self.event_handler_timeout,
            'event_handler_slow_timeout': self.event_handler_slow_timeout,
            'event_handler_detect_file_paths': self.event_handler_detect_file_paths,
            'handlers': {
                handler_id: handler.model_dump(mode='json', exclude={'handler'}) for handler_id, handler in self._handlers.items()
            },
            'handlers_by_key': {key: list(ids) for key, ids in self.handlers_by_key.items()},
            'event_history': event_history,
            'pending_event_queue': [event.event_id for event in self.pending_event_queue.iter_items()],
        }

    def model_dump_json(self) -> str:
        return json.dumps(self.model_dump(), separators=(',', ':'))

    @classmethod
    def validate(cls, data: Any) -> RustCoreEventBus:
        if isinstance(data, str):
            parsed = json.loads(data)
            payload = parsed if isinstance(parsed, dict) else {}
        else:
            payload = data if isinstance(data, dict) else {}
        bus_name = str(payload.get('name') or 'EventBus')
        bus_id = cast(str | None, payload.get('id') if isinstance(payload.get('id'), str) else None)

        def timeout_value(key: str, default: float | None) -> float | None:
            value = payload.get(key)
            if isinstance(value, int | float):
                return cast(float, value)
            return default

        for existing_bus in list(getattr(cls, '_instances', [])):
            if existing_bus.name == bus_name and (bus_id is None or existing_bus.bus_id == bus_id):
                existing_bus.disconnect()
        bus = cls(
            name=bus_name,
            id=bus_id,
            event_concurrency=cast(EventConcurrency, payload.get('event_concurrency') or 'bus-serial'),
            event_handler_concurrency=cast(HandlerConcurrency, payload.get('event_handler_concurrency') or 'serial'),
            event_handler_completion=cast(HandlerCompletion, payload.get('event_handler_completion') or 'all'),
            max_history_size=cast(
                int | None,
                payload.get('max_history_size')
                if isinstance(payload.get('max_history_size'), int) or payload.get('max_history_size') is None
                else 100,
            ),
            max_history_drop=bool(payload.get('max_history_drop', False)),
            event_timeout=timeout_value('event_timeout', 60.0),
            event_slow_timeout=timeout_value('event_slow_timeout', 300.0),
            event_handler_timeout=timeout_value('event_handler_timeout', None),
            event_handler_slow_timeout=timeout_value('event_handler_slow_timeout', 30.0),
            event_handler_detect_file_paths=bool(payload.get('event_handler_detect_file_paths', True)),
            background_worker=False,
        )
        bus._handlers.clear()
        bus.handlers_by_key.clear()
        bus._handler_names_by_key.clear()
        raw_handlers = payload.get('handlers') if isinstance(payload.get('handlers'), dict) else {}
        for raw_handler_id, raw_handler in cast(dict[str, Any], raw_handlers).items():
            if not isinstance(raw_handler, dict):
                continue
            handler_payload = {**raw_handler, 'id': raw_handler.get('id') or raw_handler_id, 'handler': lambda _event: None}
            handler_entry = EventHandler.model_validate(handler_payload)
            bus._handlers[handler_entry.id] = handler_entry
        raw_handlers_by_key = payload.get('handlers_by_key') if isinstance(payload.get('handlers_by_key'), dict) else {}
        for key, ids in cast(dict[str, Any], raw_handlers_by_key).items():
            if isinstance(ids, list):
                bus.handlers_by_key[str(key)] = [str(handler_id) for handler_id in ids if isinstance(handler_id, str)]
        bus._rebuild_handler_name_index()
        raw_history = payload.get('event_history') if isinstance(payload.get('event_history'), dict) else {}
        for event_id, raw_event in cast(dict[str, Any], raw_history).items():
            if not isinstance(raw_event, dict):
                continue
            event = BaseEvent[Any].model_validate({**raw_event, 'event_id': raw_event.get('event_id') or event_id})
            for result in event.event_results.values():
                handler_id = result.handler_id
                if handler_id not in bus._handlers:
                    result.handler.handler = lambda _event: None
                    bus._handlers[handler_id] = result.handler
                    bus.handlers_by_key.setdefault(result.handler.event_pattern, []).append(handler_id)
                else:
                    result.handler = bus._handlers[handler_id]
            bus._events[event.event_id] = event
            bus.event_history[event.event_id] = event
            bus._index_event_relationships(event)
            bus._track_event_replayability_for_new_handlers(event)
        raw_pending = (
            cast(list[Any], payload.get('pending_event_queue')) if isinstance(payload.get('pending_event_queue'), list) else []
        )
        bus.pending_event_queue = CleanShutdownQueue(maxsize=0)
        bus._pending_event_ids.clear()
        for event_id in raw_pending:
            if isinstance(event_id, str) and event_id in bus.event_history:
                bus.pending_event_queue.put_nowait(bus.event_history[event_id])
                bus._pending_event_ids.add(event_id)
                bus.in_flight_event_ids.add(event_id)
        bus._sync_bus_history_policy()
        bus.core.import_bus_snapshot(
            bus=bus._bus_record(),
            handlers=[
                {
                    'handler_id': handler.id,
                    'bus_id': bus.bus_id,
                    'host_id': bus.core.session_id,
                    'event_pattern': handler.event_pattern,
                    'handler_name': handler.handler_name,
                    'handler_file_path': handler.handler_file_path,
                    'handler_registered_at': handler.handler_registered_at,
                    'handler_timeout': handler.handler_timeout,
                    'handler_slow_timeout': handler.handler_slow_timeout,
                    'handler_concurrency': None,
                    'handler_completion': None,
                }
                for handler in bus._handlers.values()
            ],
            events=[
                cast(dict[str, Any], event_payload)
                for event_payload in cast(dict[str, Any], raw_history).values()
                if isinstance(event_payload, dict)
            ],
            pending_event_ids=[event.event_id for event in bus.pending_event_queue.iter_items()],
        )
        return bus

    model_validate = validate

    def _start_worker(self) -> None:
        if self._worker_thread is not None:
            return
        self._worker_thread = threading.Thread(
            target=self._worker_loop, name=f'abxbus-core-worker-{self.bus_id[-4:]}', daemon=True
        )
        self._worker_thread.start()
        self._worker_ready.wait(timeout=5)

    def _worker_loop(self) -> None:
        worker_core = RustCoreClient(bus_name=self.name, session_id=self.core.session_id)
        try:
            worker_core.request_messages({'type': 'heartbeat'})
            self._worker_ready.set()
            while not self._closed:
                try:
                    messages = worker_core.wait_invocations(self.bus_id, 16)
                except Exception:
                    if self._closed:
                        return
                    raise
                for message in messages:
                    if not self._invocation_messages_from_core_message(message):
                        self._apply_core_message(message)
                for invocation in self._invocation_messages_from_core_messages(messages):
                    for produced in self._run_invocation(invocation, core=worker_core):
                        if not self._invocation_messages_from_core_message(produced):
                            self._apply_core_message(produced)
        finally:
            worker_core.close()

    def _event_from_core_record(
        self,
        event_type: str | Literal['*'] | type[T_Event],
        record: dict[str, Any],
    ) -> T_Event | BaseEvent[Any]:
        record = self._normalize_core_event_snapshot(record)
        if isinstance(event_type, type):
            typed_record = dict(record)
            typed_record.pop('event_results', None)
            event = event_type.model_validate(typed_record)
            self._apply_core_snapshot_to_event(event, record)
            return event
        event = BaseEvent[Any].model_validate({**record, 'event_results': {}})
        self._apply_core_snapshot_to_event(event, record)
        return event

    @staticmethod
    def _normalize_core_event_snapshot(record: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(record)
        raw_results = normalized.get('event_results')
        if not isinstance(raw_results, dict):
            return normalized
        event_results: dict[str, Any] = {}
        for handler_id, raw_result in cast(dict[str, Any], raw_results).items():
            if not isinstance(raw_result, dict):
                continue
            result = dict(raw_result)
            result.pop('handler_seq', None)
            result.pop('result_set', None)
            result.pop('result_is_undefined', None)
            result.pop('result_is_event_reference', None)
            result.pop('slow_warning_sent', None)
            result.pop('invocation', None)
            if result.get('status') == 'cancelled':
                result['status'] = 'error'
            event_results[str(handler_id)] = result
        normalized['event_results'] = event_results
        return normalized

    def _messages_should_race_first_completion(self, messages: list[dict[str, Any]]) -> bool:
        for invocation in self._invocation_messages_from_core_messages(messages):
            snapshot = invocation.get('event_snapshot')
            event_record = cast(dict[str, Any], snapshot) if isinstance(snapshot, dict) else {}
            completion = _option_value(event_record.get('event_handler_completion')) or self.event_handler_completion
            concurrency = _option_value(event_record.get('event_handler_concurrency')) or self.event_handler_concurrency
            if completion == 'first' and concurrency == 'parallel':
                return True
        return False

    @staticmethod
    def _messages_complete_event(messages: list[dict[str, Any]]) -> bool:
        for message in messages:
            if message.get('type') != 'patch':
                continue
            patch = message.get('patch')
            if isinstance(patch, dict) and patch.get('type') in ('event_completed', 'event_completed_compact'):
                return True
        return False

    @staticmethod
    def _messages_contain_first_result_resolution(messages: list[dict[str, Any]]) -> bool:
        for message in messages:
            if message.get('type') != 'patch':
                continue
            patch = message.get('patch')
            if not isinstance(patch, dict):
                continue
            if patch.get('type') in ('event_completed', 'event_completed_compact'):
                return True
            if patch.get('type') == 'result_cancelled' and patch.get('reason') == 'FirstResultWon':
                return True
        return False

    async def _apply_messages_and_run_invocations_async(self, messages: list[dict[str, Any]]) -> bool:
        if not messages:
            return False
        invocations: list[dict[str, Any]] = []
        for message in messages:
            nested_invocations = self._invocation_messages_from_core_message(message)
            if nested_invocations:
                invocations.extend(nested_invocations)
            else:
                await self._apply_core_message_async(message)
        race_first = self._messages_should_race_first_completion(messages)

        progressed = False
        while invocations:
            passive_queue_order = any(
                bus._passive_handler_wait_depth > 0 for bus in list(type(self)._instances) if not bus._closed
            )
            if (len(invocations) == 1 or passive_queue_order) and not race_first:
                invocation = invocations[0]
                remaining_invocations = invocations[1:] if passive_queue_order else []
                bus = self._bus_for_invocation(invocation)
                event_id = invocation.get('event_id')
                event = bus._events.get(event_id) if isinstance(event_id, str) else None
                event_concurrency = _option_value(event.event_concurrency) if event is not None else None
                if event_concurrency is None:
                    event_concurrency = bus.event_concurrency
                if event_concurrency == 'parallel' and bus._can_schedule_async_processing() and not passive_queue_order:
                    dispatch_context = event._get_dispatch_context() if event is not None else None  # pyright: ignore[reportPrivateUsage]
                    if dispatch_context is None:
                        task = asyncio.create_task(bus._run_single_invocation_outcome_async(invocation))
                    else:
                        task = asyncio.create_task(
                            bus._run_single_invocation_outcome_async(invocation),
                            context=dispatch_context.copy(),
                        )
                    bus._local_invocation_tasks.add(task)
                    task.add_done_callback(bus._local_invocation_tasks.discard)
                    invocation_id = invocation.get('invocation_id')
                    if isinstance(invocation_id, str):
                        task.add_done_callback(
                            lambda done_task, *, owner=bus, inv=invocation: owner._commit_single_invocation_task(done_task, inv)
                        )
                    invocations = remaining_invocations
                    progressed = True
                    continue
                else:
                    outcome = await bus._run_invocation_outcome_async_with_dispatch_context(invocation)
                    invocations = remaining_invocations
                    if not outcome:
                        continue
                    if not bus._apply_local_completed_outcome_without_patch_messages(invocation, outcome):
                        for message in bus._local_outcome_patch_messages(invocation, outcome):
                            await bus._apply_core_message_async(message)
                    invocation_id = outcome.get('invocation_id')
                    invocations_by_invocation = {invocation_id: invocation} if isinstance(invocation_id, str) else {}
                    produced = bus._complete_core_outcomes([outcome], invocations_by_invocation)
                    bus._release_queue_jump_children_for_invocation(invocation_id)
                    for message in produced:
                        nested_invocations = bus._invocation_messages_from_core_message(message)
                        if nested_invocations:
                            invocations.extend(nested_invocations)
                            continue
                        if message.get('type') != 'cancel_invocation':
                            await bus._apply_core_message_async(message)
                            continue
                        await bus._apply_core_message_async(message)
                    progressed = True
                continue
            inline_batch = await self._run_inline_sync_invocation_outcomes_async(invocations, race_first=race_first)
            if inline_batch is not None:
                invocations = []
                outcomes, buses_by_invocation, invocations_by_invocation = inline_batch
                if not outcomes:
                    continue
                produced_entries: list[tuple[RustCoreEventBus, dict[str, Any]]] = []
                all_completed = all(
                    isinstance(outcome.get('outcome'), dict)
                    and cast(dict[str, Any], outcome['outcome']).get('status') == 'completed'
                    for outcome in outcomes
                )
                if all_completed:
                    locally_applied_invocation_ids = self._apply_local_completed_outcomes_without_patch_messages(
                        outcomes,
                        buses_by_invocation,
                        invocations_by_invocation,
                    )
                    for outcome in outcomes:
                        invocation_id = outcome.get('invocation_id')
                        if not isinstance(invocation_id, str):
                            continue
                        if invocation_id in locally_applied_invocation_ids:
                            continue
                        outcome_bus = buses_by_invocation.get(invocation_id, self)
                        invocation = invocations_by_invocation.get(invocation_id)
                        if invocation is None:
                            continue
                        for message in outcome_bus._local_outcome_patch_messages(invocation, outcome):
                            await outcome_bus._apply_core_message_async(message)
                    self._schedule_core_outcome_commit(outcomes, buses_by_invocation, invocations_by_invocation)
                    progressed = True
                    continue
                outcome_bus = next(
                    (
                        buses_by_invocation[cast(str, outcome.get('invocation_id'))]
                        for outcome in outcomes
                        if isinstance(outcome.get('invocation_id'), str)
                        and cast(str, outcome.get('invocation_id')) in buses_by_invocation
                    ),
                    self,
                )
                produced = outcome_bus.core.complete_handler_outcomes(outcomes, compact_response=True)
                for outcome in outcomes:
                    outcome_bus._release_queue_jump_children_for_invocation(outcome.get('invocation_id'))
                produced_entries.extend((outcome_bus, message) for message in produced)
                if produced_entries:
                    progressed = True
                for produced_bus, message in produced_entries:
                    nested_invocations = produced_bus._invocation_messages_from_core_message(message)
                    if nested_invocations:
                        invocations.extend(nested_invocations)
                        continue
                    await produced_bus._apply_core_message_async(message)
                continue
            tasks_by_invocation: dict[str, asyncio.Task[dict[str, Any]]] = {}
            buses_by_invocation: dict[str, RustCoreEventBus] = {}
            invocations_by_invocation: dict[str, dict[str, Any]] = {}
            for message in invocations:
                bus = self._bus_for_invocation(message)
                event_id = message.get('event_id')
                event = bus._events.get(event_id) if isinstance(event_id, str) else None
                dispatch_context = event._get_dispatch_context() if event is not None else None  # pyright: ignore[reportPrivateUsage]
                if dispatch_context is None:
                    task = asyncio.create_task(bus._run_single_invocation_outcome_async(message))
                else:
                    task = asyncio.create_task(
                        bus._run_single_invocation_outcome_async(message),
                        context=dispatch_context.copy(),
                    )
                invocation_id = message.get('invocation_id')
                if isinstance(invocation_id, str):
                    tasks_by_invocation[invocation_id] = task
                    buses_by_invocation[invocation_id] = bus
                    invocations_by_invocation[invocation_id] = message
                bus._local_invocation_tasks.add(task)
                task.add_done_callback(bus._local_invocation_tasks.discard)
            pending = set(tasks_by_invocation.values())
            invocations = []
            try:
                while pending:
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    if pending:
                        await asyncio.sleep(0)
                        ready = {task for task in pending if task.done()}
                        if ready:
                            done.update(ready)
                            pending.difference_update(ready)
                    outcomes: list[dict[str, Any]] = []
                    for task in done:
                        try:
                            outcome = task.result()
                        except asyncio.CancelledError:
                            outcome = {}
                        if not outcome:
                            continue
                        outcomes.append(outcome)
                    if not outcomes:
                        continue
                    produced_entries: list[tuple[RustCoreEventBus, dict[str, Any]]] = []
                    all_completed = all(
                        isinstance(outcome.get('outcome'), dict)
                        and cast(dict[str, Any], outcome['outcome']).get('status') == 'completed'
                        for outcome in outcomes
                    )
                    if all_completed and not race_first:
                        locally_applied_invocation_ids = self._apply_local_completed_outcomes_without_patch_messages(
                            outcomes,
                            buses_by_invocation,
                            invocations_by_invocation,
                        )
                        for outcome in outcomes:
                            invocation_id = outcome.get('invocation_id')
                            if not isinstance(invocation_id, str):
                                continue
                            if invocation_id in locally_applied_invocation_ids:
                                continue
                            outcome_bus = buses_by_invocation.get(invocation_id, self)
                            invocation = invocations_by_invocation.get(invocation_id)
                            if invocation is None:
                                continue
                            for message in outcome_bus._local_outcome_patch_messages(invocation, outcome):
                                await outcome_bus._apply_core_message_async(message)
                        self._schedule_core_outcome_commit(outcomes, buses_by_invocation, invocations_by_invocation)
                        progressed = True
                        continue
                    else:
                        outcome_bus = next(
                            (
                                buses_by_invocation[cast(str, outcome.get('invocation_id'))]
                                for outcome in outcomes
                                if isinstance(outcome.get('invocation_id'), str)
                                and cast(str, outcome.get('invocation_id')) in buses_by_invocation
                            ),
                            self,
                        )
                        produced = outcome_bus.core.complete_handler_outcomes(outcomes, compact_response=True)
                        for outcome in outcomes:
                            outcome_bus._release_queue_jump_children_for_invocation(outcome.get('invocation_id'))
                        produced_entries.extend((outcome_bus, message) for message in produced)
                    if produced_entries:
                        progressed = True
                    produced_messages = [message for _bus, message in produced_entries]
                    for produced_bus, message in produced_entries:
                        nested_invocations = produced_bus._invocation_messages_from_core_message(message)
                        if nested_invocations:
                            invocations.extend(nested_invocations)
                            continue
                        if message.get('type') != 'cancel_invocation':
                            await produced_bus._apply_core_message_async(message)
                            continue
                        await produced_bus._apply_core_message_async(message)
                        invocation_id = message.get('invocation_id')
                        if not isinstance(invocation_id, str):
                            continue
                        pending_task = tasks_by_invocation.get(invocation_id)
                        if pending_task is not None and pending_task in pending:
                            pending_task.cancel()
                    first_result_resolved = race_first and self._messages_contain_first_result_resolution(produced_messages)
                    if produced_entries and (self._messages_complete_event(produced_messages) or first_result_resolved):
                        if race_first:
                            for pending_task in pending:
                                pending_task.add_done_callback(lambda task: task.exception() if not task.cancelled() else None)
                                pending_task.cancel()
                            pending = set()
                            if not invocations:
                                return progressed
                            race_first = self._messages_should_race_first_completion(produced_messages)
                            break
            finally:
                if pending:
                    for pending_task in pending:
                        pending_task.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)
        return progressed

    def _can_run_invocation_inline_without_task(self, invocation: dict[str, Any]) -> bool:
        bus = self._bus_for_invocation(invocation)
        if bus.middlewares:
            return False
        handler_id = invocation.get('handler_id')
        handler = bus._handlers.get(handler_id) if isinstance(handler_id, str) else None
        if handler is None:
            return bus._closed
        return bus._handler_can_run_local_immediate(handler) and bus._handler_return_annotation_allows_inline_sync(handler)

    @staticmethod
    def _handler_return_annotation_allows_inline_sync(handler: EventHandler) -> bool:
        callable_handler = handler.handler
        annotations = getattr(callable_handler, '__annotations__', {}) if callable_handler is not None else {}
        if not isinstance(annotations, dict):
            return False
        return_annotation = annotations.get('return', inspect.Signature.empty)
        if return_annotation is inspect.Signature.empty:
            return False
        if return_annotation is None or return_annotation is type(None):
            return True
        return isinstance(return_annotation, str) and return_annotation.replace(' ', '').lower() in {'none', 'nonetype'}

    async def _run_inline_sync_invocation_outcomes_async(
        self,
        invocations: list[dict[str, Any]],
        *,
        race_first: bool,
    ) -> tuple[list[dict[str, Any]], dict[str, RustCoreEventBus], dict[str, dict[str, Any]]] | None:
        if race_first or len(invocations) < self._INLINE_SYNC_INVOCATION_THRESHOLD:
            return None
        if not all(self._can_run_invocation_inline_without_task(invocation) for invocation in invocations):
            return None
        outcomes: list[dict[str, Any]] = []
        buses_by_invocation: dict[str, RustCoreEventBus] = {}
        invocations_by_invocation: dict[str, dict[str, Any]] = {}
        for index, invocation in enumerate(invocations, start=1):
            bus = self._bus_for_invocation(invocation)
            outcome = bus._run_invocation_outcome_inline_sync_with_dispatch_context(invocation)
            if outcome is None:
                return None
            if outcome:
                outcomes.append(outcome)
            invocation_id = invocation.get('invocation_id')
            if isinstance(invocation_id, str):
                buses_by_invocation[invocation_id] = bus
                invocations_by_invocation[invocation_id] = invocation
            if index % 4096 == 0:
                await asyncio.sleep(0)
        return outcomes, buses_by_invocation, invocations_by_invocation

    async def _run_invocation_async_with_dispatch_context(self, invocation: dict[str, Any]) -> list[dict[str, Any]]:
        event_id = invocation.get('event_id')
        event = self._events.get(event_id) if isinstance(event_id, str) else None
        dispatch_context = event._get_dispatch_context() if event is not None else None  # pyright: ignore[reportPrivateUsage]
        if dispatch_context is None:
            return await self._run_invocation_async(invocation)
        tokens: list[tuple[contextvars.ContextVar[Any], contextvars.Token[Any]]] = []
        for variable, value in dispatch_context.items():
            tokens.append((variable, variable.set(value)))
        try:
            return await self._run_invocation_async(invocation)
        finally:
            for variable, token in reversed(tokens):
                variable.reset(token)

    async def _run_invocation_outcome_async_with_dispatch_context(self, invocation: dict[str, Any]) -> dict[str, Any]:
        event_id = invocation.get('event_id')
        event = self._events.get(event_id) if isinstance(event_id, str) else None
        dispatch_context = event._get_dispatch_context() if event is not None else None  # pyright: ignore[reportPrivateUsage]
        if dispatch_context is None:
            return await self._run_single_invocation_outcome_async(invocation)
        tokens: list[tuple[contextvars.ContextVar[Any], contextvars.Token[Any]]] = []
        for variable, value in dispatch_context.items():
            tokens.append((variable, variable.set(value)))
        try:
            return await self._run_single_invocation_outcome_async(invocation)
        finally:
            for variable, token in reversed(tokens):
                variable.reset(token)

    def _run_invocation_outcome_inline_sync_with_dispatch_context(
        self,
        invocation: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not self._can_run_invocation_inline_without_task(invocation):
            return None
        event_id = invocation.get('event_id')
        event = self._events.get(event_id) if isinstance(event_id, str) else None
        dispatch_context = event._get_dispatch_context() if event is not None else None  # pyright: ignore[reportPrivateUsage]
        if dispatch_context is None:
            return self._run_single_invocation_outcome_inline_sync(invocation)
        tokens: list[tuple[contextvars.ContextVar[Any], contextvars.Token[Any]]] = []
        for variable, value in dispatch_context.items():
            tokens.append((variable, variable.set(value)))
        try:
            return self._run_single_invocation_outcome_inline_sync(invocation)
        finally:
            for variable, token in reversed(tokens):
                variable.reset(token)

    @staticmethod
    def _seconds_until_deadline(deadline_at: Any) -> float | None:
        if not isinstance(deadline_at, str):
            return None
        try:
            deadline = datetime.fromisoformat(deadline_at.replace('Z', '+00:00')).astimezone(UTC)
        except ValueError:
            return None
        return (deadline - datetime.now(UTC)).total_seconds()

    @staticmethod
    def _invocation_timeout_kind_and_remaining(invocation: dict[str, Any]) -> tuple[str, float] | None:
        deadlines: list[tuple[str, float]] = []
        handler_remaining = RustCoreEventBus._seconds_until_deadline(invocation.get('deadline_at'))
        if handler_remaining is not None:
            deadlines.append(('handler', handler_remaining))
        event_remaining = RustCoreEventBus._seconds_until_deadline(invocation.get('event_deadline_at'))
        if event_remaining is not None:
            deadlines.append(('event', event_remaining))
        if not deadlines:
            return None
        return min(deadlines, key=lambda item: item[1])

    def _timeout_seconds_for_invocation(self, invocation: dict[str, Any], kind: str) -> float | None:
        if kind == 'event':
            snapshot = invocation.get('event_snapshot')
            if isinstance(snapshot, dict):
                raw_timeout = snapshot.get('event_timeout')
                if isinstance(raw_timeout, int | float):
                    return float(raw_timeout)
            return self.event_timeout
        result_snapshot = invocation.get('result_snapshot')
        if isinstance(result_snapshot, dict):
            raw_timeout = result_snapshot.get('handler_timeout')
            if isinstance(raw_timeout, int | float):
                return float(raw_timeout)
        return None

    @staticmethod
    def _number_option(value: Any) -> float | None:
        value = _option_value(value)
        if isinstance(value, int | float):
            return float(value)
        return None

    @staticmethod
    def _positive_timeout_option(value: Any) -> float | None:
        value = _option_value(value)
        if not isinstance(value, int | float):
            return None
        timeout = float(value)
        if math.isfinite(timeout) and timeout > 0.0:
            return timeout
        return None

    def _effective_timeout_for_invocation(
        self,
        invocation: dict[str, Any],
        event: BaseEvent[Any] | None = None,
        handler: EventHandler | None = None,
    ) -> float | None:
        result_snapshot = invocation.get('result_snapshot')
        handler_timeout: float | None = None
        if isinstance(result_snapshot, dict):
            raw_timeout = result_snapshot.get('timeout')
            if isinstance(raw_timeout, int | float):
                handler_timeout = float(raw_timeout)
        if handler_timeout is None and handler is not None:
            handler_timeout = self._number_option(handler.handler_timeout)
        if handler_timeout is None and event is not None:
            handler_timeout = self._number_option(event.event_handler_timeout)
        if handler_timeout is None:
            handler_timeout = self._number_option(self.event_handler_timeout)
        snapshot = invocation.get('event_snapshot')
        raw_event_timeout: Any = None
        if isinstance(snapshot, dict):
            raw_event_timeout = snapshot.get('event_timeout')
        if event is not None:
            event_timeout = self._number_option(event.event_timeout)
        else:
            event_timeout = None
        if event_timeout is None:
            event_timeout = float(raw_event_timeout) if isinstance(raw_event_timeout, int | float) else self.event_timeout
        if event_timeout is not None and event_timeout <= 0:
            event_timeout = None
        if handler_timeout is None:
            return event_timeout
        if event_timeout is None:
            return handler_timeout
        return min(handler_timeout, event_timeout)

    def _apply_effective_timeout_to_result(
        self,
        invocation: dict[str, Any],
        result: EventResult[Any],
        event: BaseEvent[Any] | None = None,
        handler: EventHandler | None = None,
    ) -> None:
        if event is None:
            event = self._events.get(result.event_id)
        timeout = self._effective_timeout_for_invocation(invocation, event, handler or result.handler)
        result.timeout = timeout

    def _invocation_timeout_error(
        self,
        invocation: dict[str, Any],
        event: BaseEvent[Any],
        handler: EventHandler,
        kind: str,
    ) -> BaseException:
        timeout_seconds = self._timeout_seconds_for_invocation(invocation, kind)
        if kind == 'event':
            suffix = f' after {timeout_seconds}s' if timeout_seconds is not None else ''
            return EventHandlerAbortedError(f'Aborted running handler: event timeout{suffix}')
        suffix = f' after {timeout_seconds}s' if timeout_seconds is not None else ''
        return EventHandlerTimeoutError(f'Event handler {handler.label}({event}) timed out{suffix}')

    def _handler_slow_timeout_for_invocation(
        self,
        invocation: dict[str, Any],
        event: BaseEvent[Any],
        handler: EventHandler,
    ) -> float | None:
        for value in (
            handler.handler_slow_timeout,
            event.event_handler_slow_timeout,
            event.event_slow_timeout,
            self.event_handler_slow_timeout,
            self.event_slow_timeout,
        ):
            if _option_value(value) is not None:
                slow_timeout = self._positive_timeout_option(value)
                if slow_timeout is None:
                    return None
                effective_timeout = self._effective_timeout_for_invocation(invocation, event, handler)
                if effective_timeout is not None and effective_timeout <= slow_timeout:
                    return None
                return slow_timeout
        return None

    def _log_running_handler_slow_warning(
        self,
        event: BaseEvent[Any],
        handler: EventHandler,
    ) -> None:
        result = event.event_results.get(handler.id)
        if result is None or result.status != 'started':
            return
        started_at = result.started_at or event.event_started_at or event.event_created_at
        elapsed_seconds = self._elapsed_since_timestamp(started_at)
        logger.warning(
            '⚠️ Slow event handler: %s.on(%s#%s, %s) still running after %.1fs',
            result.eventbus_label,
            event.event_type,
            event.event_id[-4:],
            result.handler.label,
            elapsed_seconds,
        )

    def _start_async_handler_slow_warning_task(
        self,
        invocation: dict[str, Any],
        event: BaseEvent[Any],
        handler: EventHandler,
    ) -> tuple[str, str] | None:
        if (
            handler.handler_slow_timeout is None
            and event.event_handler_slow_timeout is None
            and event.event_slow_timeout is None
            and self.event_handler_slow_timeout is not None
        ):
            slow_timeout = self._positive_timeout_option(self.event_handler_slow_timeout)
            if slow_timeout is None:
                return None
            effective_timeout = self._effective_timeout_for_invocation(invocation, event, handler)
            if effective_timeout is not None and effective_timeout <= slow_timeout:
                return None
            self._async_default_handler_slow_warning_running += 1
            self._ensure_async_default_handler_slow_warning_timer(slow_timeout)
            return ('__default__', f'{event.event_id}:{handler.id}')
        slow_timeout = self._handler_slow_timeout_for_invocation(invocation, event, handler)
        if slow_timeout is None:
            return None
        key = (event.event_id, handler.id)
        deadline = time.monotonic() + slow_timeout
        self._async_handler_slow_warning_entries[key] = (deadline, event, handler)
        current_deadline = self._async_handler_slow_warning_deadline
        if self._async_handler_slow_warning_timer is None or current_deadline is None or deadline < current_deadline:
            self._schedule_next_async_handler_slow_warning()
        return key

    def _ensure_async_default_handler_slow_warning_timer(self, slow_timeout: float) -> None:
        if self._async_default_handler_slow_warning_timer is not None:
            return
        self._async_default_handler_slow_warning_timer = asyncio.get_running_loop().call_later(
            slow_timeout,
            self._fire_async_default_handler_slow_warnings,
        )

    def _fire_async_default_handler_slow_warnings(self) -> None:
        self._async_default_handler_slow_warning_timer = None
        if self._async_default_handler_slow_warning_running <= 0:
            self._async_default_handler_slow_warning_logged.clear()
            return
        slow_timeout = self._number_option(self.event_handler_slow_timeout)
        if slow_timeout is None:
            return
        next_delay: float | None = None
        for event in list(self._events.values()):
            if event.event_handler_slow_timeout is not None or event.event_slow_timeout is not None:
                continue
            for result in event.event_results.values():
                handler = result.handler
                if result.status != 'started' or handler.handler_slow_timeout is not None:
                    continue
                started_at = result.started_at or event.event_started_at or event.event_created_at
                elapsed = self._elapsed_since_timestamp(started_at)
                if elapsed >= slow_timeout:
                    key = (event.event_id, handler.id)
                    if key not in self._async_default_handler_slow_warning_logged:
                        self._async_default_handler_slow_warning_logged.add(key)
                        self._log_running_handler_slow_warning(event, handler)
                    continue
                remaining = slow_timeout - elapsed
                next_delay = remaining if next_delay is None else min(next_delay, remaining)
        if next_delay is not None:
            self._async_default_handler_slow_warning_timer = asyncio.get_running_loop().call_later(
                max(0.001, next_delay),
                self._fire_async_default_handler_slow_warnings,
            )

    def _schedule_next_async_handler_slow_warning(self) -> None:
        timer = self._async_handler_slow_warning_timer
        if timer is not None:
            timer.cancel()
            self._async_handler_slow_warning_timer = None
            self._async_handler_slow_warning_deadline = None
        if not self._async_handler_slow_warning_entries:
            return
        deadline = min(entry[0] for entry in self._async_handler_slow_warning_entries.values())
        self._async_handler_slow_warning_deadline = deadline
        self._async_handler_slow_warning_timer = asyncio.get_running_loop().call_later(
            max(0.0, deadline - time.monotonic()),
            self._fire_async_handler_slow_warnings,
        )

    def _fire_async_handler_slow_warnings(self) -> None:
        self._async_handler_slow_warning_timer = None
        self._async_handler_slow_warning_deadline = None
        now = time.monotonic()
        due: list[tuple[BaseEvent[Any], EventHandler]] = []
        for key, (deadline, event, handler) in list(self._async_handler_slow_warning_entries.items()):
            if deadline <= now:
                self._async_handler_slow_warning_entries.pop(key, None)
                due.append((event, handler))
        for event, handler in due:
            self._log_running_handler_slow_warning(event, handler)
        self._schedule_next_async_handler_slow_warning()

    def _cancel_async_handler_slow_warning_entry(self, key: tuple[str, str] | None) -> None:
        if key is None:
            return
        if key[0] == '__default__':
            self._async_default_handler_slow_warning_running = max(0, self._async_default_handler_slow_warning_running - 1)
            if self._async_default_handler_slow_warning_running == 0:
                default_timer = self._async_default_handler_slow_warning_timer
                if default_timer is not None:
                    default_timer.cancel()
                self._async_default_handler_slow_warning_timer = None
                self._async_default_handler_slow_warning_logged.clear()
            return
        entry = self._async_handler_slow_warning_entries.pop(key, None)
        if entry is None:
            return
        if not self._async_handler_slow_warning_entries:
            timer = self._async_handler_slow_warning_timer
            if timer is not None:
                timer.cancel()
            self._async_handler_slow_warning_timer = None
            self._async_handler_slow_warning_deadline = None
            return
        current_deadline = self._async_handler_slow_warning_deadline
        if current_deadline is not None and entry[0] <= current_deadline:
            self._schedule_next_async_handler_slow_warning()

    def _cancel_all_async_handler_slow_warnings(self) -> None:
        self._async_handler_slow_warning_entries.clear()
        timer = self._async_handler_slow_warning_timer
        if timer is not None:
            timer.cancel()
        self._async_handler_slow_warning_timer = None
        self._async_handler_slow_warning_deadline = None
        default_timer = self._async_default_handler_slow_warning_timer
        if default_timer is not None:
            default_timer.cancel()
        self._async_default_handler_slow_warning_timer = None
        self._async_default_handler_slow_warning_logged.clear()
        self._async_default_handler_slow_warning_running = 0

    def _start_sync_handler_slow_warning_timer(
        self,
        invocation: dict[str, Any],
        event: BaseEvent[Any],
        handler: EventHandler,
    ) -> threading.Timer | None:
        slow_timeout = self._handler_slow_timeout_for_invocation(invocation, event, handler)
        if slow_timeout is None:
            return None
        timer = threading.Timer(slow_timeout, self._log_running_handler_slow_warning, args=(event, handler))
        timer.daemon = True
        timer.start()
        return timer

    async def _invoke_handler_value_async(
        self,
        invocation: dict[str, Any],
        event: BaseEvent[Any],
        handler: EventHandler,
        abort_context: ActiveAbortContext,
    ) -> Any:
        slow_warning_task = self._start_async_handler_slow_warning_task(invocation, event, handler)

        async def call_handler() -> Any:
            value = handler(event)
            if inspect.isawaitable(value) and not isinstance(value, BaseEvent):
                return await cast(Coroutine[Any, Any, Any], value)
            return value

        try:
            timeout_state = self._invocation_timeout_kind_and_remaining(invocation)
            if timeout_state is None:
                return await call_handler()
            timeout_kind, remaining_seconds = timeout_state
            if remaining_seconds <= 0:
                error = self._invocation_timeout_error(invocation, event, handler, timeout_kind)
                abort_context.abort(error)
                event._cancel_pending_child_processing(error)  # pyright: ignore[reportPrivateUsage]
                raise error
            handler_task = asyncio.create_task(call_handler())
            done, _pending = await asyncio.wait({handler_task}, timeout=remaining_seconds)
            if handler_task in done:
                return handler_task.result()
            error = self._invocation_timeout_error(invocation, event, handler, timeout_kind)
            abort_context.abort(error)
            event._cancel_pending_child_processing(error)  # pyright: ignore[reportPrivateUsage]
            handler_task.cancel()
            await asyncio.gather(handler_task, return_exceptions=True)
            raise error
        finally:
            if slow_warning_task is not None:
                self._cancel_async_handler_slow_warning_entry(slow_warning_task)

    def _invoke_handler_value_sync(
        self,
        invocation: dict[str, Any],
        event: BaseEvent[Any],
        handler: EventHandler,
        abort_context: ActiveAbortContext,
    ) -> Any:
        slow_warning_timer = self._start_sync_handler_slow_warning_timer(invocation, event, handler)
        try:
            timeout_state = self._invocation_timeout_kind_and_remaining(invocation)
            if timeout_state is not None and timeout_state[1] <= 0:
                error = self._invocation_timeout_error(invocation, event, handler, timeout_state[0])
                abort_context.abort(error)
                event._cancel_pending_child_processing(error)  # pyright: ignore[reportPrivateUsage]
                raise error
            value = handler(event)
            if not inspect.isawaitable(value) or isinstance(value, BaseEvent):
                return value

            async def await_value() -> Any:
                timeout_state_inner = self._invocation_timeout_kind_and_remaining(invocation)
                if timeout_state_inner is None:
                    return await cast(Coroutine[Any, Any, Any], value)
                timeout_kind, remaining_seconds = timeout_state_inner
                if remaining_seconds <= 0:
                    error = self._invocation_timeout_error(invocation, event, handler, timeout_kind)
                    abort_context.abort(error)
                    event._cancel_pending_child_processing(error)  # pyright: ignore[reportPrivateUsage]
                    raise error
                handler_task = asyncio.create_task(cast(Coroutine[Any, Any, Any], value))
                done, _pending = await asyncio.wait({handler_task}, timeout=remaining_seconds)
                if handler_task in done:
                    return handler_task.result()
                error = self._invocation_timeout_error(invocation, event, handler, timeout_kind)
                abort_context.abort(error)
                event._cancel_pending_child_processing(error)  # pyright: ignore[reportPrivateUsage]
                handler_task.cancel()
                await asyncio.gather(handler_task, return_exceptions=True)
                raise error

            return asyncio.run(await_value())
        finally:
            if slow_warning_timer is not None:
                slow_warning_timer.cancel()

    def _run_invocation(self, invocation: dict[str, Any], *, core: RustCoreClient | None = None) -> list[dict[str, Any]]:
        core_client = core or self.core
        handler_id = invocation['handler_id']
        handler_entry = self._handlers.get(handler_id)
        if handler_entry is None:
            if self._closed:
                return []
            raise KeyError(handler_id)
        event_id = cast(str, invocation['event_id'])
        result_id = invocation.get('result_id')
        invocation_id = invocation.get('invocation_id')
        if isinstance(result_id, str) and isinstance(invocation_id, str):
            self._invocation_by_result_id[result_id] = invocation_id
        event_obj = self._events.get(event_id)
        if event_obj is None:
            snapshot = invocation.get('event_snapshot')
            if not isinstance(snapshot, dict):
                snapshot = core_client.get_event(event_id)
            if not isinstance(snapshot, dict):
                raise RuntimeError(f'missing event: {event_id}')
            event_obj = self._event_from_core_record(
                self._event_types_by_pattern.get(handler_entry.event_pattern, handler_entry.event_pattern),
                cast(dict[str, Any], snapshot),
            )
            self._events[event_id] = event_obj
        self._ensure_local_pending_results_for_event(event_obj)
        if isinstance(result_id, str):
            active_result = self._update_event_result_for_handler(event_obj, handler_entry, status='started')
            active_result.id = result_id
            self._apply_effective_timeout_to_result(invocation, active_result)
            self._event_id_by_result_id[result_id] = event_obj.event_id
        abort_context = ActiveAbortContext()
        active_batch = _core_outcome_batch_context.get()
        owned_batch: CoreOutcomeBatch | None = (
            {'outcomes': [], 'route_id': invocation.get('route_id') if isinstance(invocation.get('route_id'), str) else None}
            if active_batch is None
            else None
        )
        batch_token: contextvars.Token[CoreOutcomeBatch | None] | None = None
        if owned_batch is not None:
            batch_token = _core_outcome_batch_context.set(owned_batch)
        try:
            tokens = self._set_handler_context(event_obj, str(handler_id))
            abort_token = set_active_abort_context(abort_context)
            try:
                with self._handler_event_path_context(event_obj):
                    result = self._invoke_handler_value_sync(invocation, event_obj, handler_entry, abort_context)
            finally:
                reset_active_abort_context(abort_token)
                self._reset_handler_context(tokens)
        except EventHandlerAbortedError as exc:
            local_result = self._update_event_result_for_handler(event_obj, handler_entry, status='started')
            if isinstance(result_id, str):
                local_result.id = result_id
            self._apply_effective_timeout_to_result(invocation, local_result)
            local_result.update(error=exc)
            outcome = self._handler_error_outcome(exc)
            outcome_record = self._handler_outcome_record(
                invocation,
                outcome,
                process_available_after=self._process_available_after_handler_outcome(invocation),
            )
            if batch_token is not None:
                _core_outcome_batch_context.reset(batch_token)
                batch_token = None
            messages = self._complete_or_defer_outcome(
                core_client,
                invocation,
                outcome_record,
                active_batch=active_batch,
                owned_batch=owned_batch,
                direct_commit=lambda exc=exc: core_client.error_handler(
                    invocation,
                    exc,
                    process_route_after=self._process_route_after_handler_outcome(invocation),
                    process_available_after=self._process_available_after_handler_outcome(invocation),
                    compact_response=True,
                ),
            )
        except Exception as exc:
            local_result = self._update_event_result_for_handler(event_obj, handler_entry, status='started')
            if isinstance(result_id, str):
                local_result.id = result_id
            self._apply_effective_timeout_to_result(invocation, local_result)
            local_result.update(error=exc)
            outcome = self._handler_error_outcome(exc)
            outcome_record = self._handler_outcome_record(
                invocation,
                outcome,
                process_available_after=self._process_available_after_handler_outcome(invocation),
            )
            if batch_token is not None:
                _core_outcome_batch_context.reset(batch_token)
                batch_token = None
            messages = self._complete_or_defer_outcome(
                core_client,
                invocation,
                outcome_record,
                active_batch=active_batch,
                owned_batch=owned_batch,
                direct_commit=lambda exc=exc: core_client.error_handler(
                    invocation,
                    exc,
                    process_route_after=self._process_route_after_handler_outcome(invocation),
                    process_available_after=self._process_available_after_handler_outcome(invocation),
                    compact_response=True,
                ),
            )
        else:
            outcome = self._handler_completed_outcome(invocation, event_obj, handler_entry, result)
            outcome_record = self._handler_outcome_record(
                invocation,
                outcome,
                process_available_after=self._process_available_after_handler_outcome(invocation),
            )
            if batch_token is not None:
                _core_outcome_batch_context.reset(batch_token)
                batch_token = None
            messages = self._complete_or_defer_outcome(
                core_client,
                invocation,
                outcome_record,
                active_batch=active_batch,
                owned_batch=owned_batch,
                direct_commit=lambda: self._complete_invocation_with_typed_result(
                    core_client,
                    invocation,
                    event_obj,
                    handler_entry,
                    result,
                ),
            )
        finally:
            if batch_token is not None:
                _core_outcome_batch_context.reset(batch_token)
            if isinstance(result_id, str):
                self._invocation_by_result_id.pop(result_id, None)
        self._release_queue_jump_children_for_invocation(invocation.get('invocation_id'))
        produced = list(messages)
        for message in messages:
            for nested_invocation in self._invocation_messages_from_core_message(message):
                produced.extend(self._run_invocation(nested_invocation, core=core_client))
        return produced

    async def _run_invocation_async(self, invocation: dict[str, Any]) -> list[dict[str, Any]]:
        return await self._run_single_invocation_async(invocation)

    @staticmethod
    def _handler_error_outcome(error: BaseException | str) -> dict[str, Any]:
        if isinstance(error, EventHandlerAbortedError):
            error_kind = 'handler_aborted'
        elif isinstance(error, EventHandlerCancelledError | asyncio.CancelledError):
            error_kind = 'handler_cancelled'
        elif isinstance(error, TimeoutError):
            error_kind = 'handler_timeout'
        elif type(error).__name__ == 'EventHandlerResultSchemaError':
            error_kind = 'schema_error'
        else:
            error_kind = 'host_error'
        return {
            'status': 'errored',
            'error': {
                'kind': error_kind,
                'message': str(error),
            },
        }

    def _handler_completed_outcome(
        self,
        invocation: dict[str, Any],
        event: BaseEvent[Any],
        handler: EventHandler,
        handler_return_value: Any,
    ) -> dict[str, Any]:
        existing_result = event.event_results.get(handler.id)
        if existing_result is not None and existing_result.status == 'error':
            return self._handler_error_outcome(existing_result.error or 'handler failed')
        result = self._complete_untyped_result_fast(event, handler, handler_return_value)
        if result is None:
            result = self._update_event_result_for_handler(event, handler, result=handler_return_value)
            self._apply_effective_timeout_to_result(invocation, result)
        result_id = invocation.get('result_id')
        if isinstance(result_id, str):
            result.id = result_id
            self._event_id_by_result_id[result_id] = event.event_id
        if result.status == 'error':
            return self._handler_error_outcome(result.error or 'handler result failed event_result_type validation')
        if isinstance(result.result, BaseEvent):
            return {
                'status': 'completed',
                'value': {'event_id': result.result.event_id, 'event_type': result.result.event_type},
                'result_is_event_reference': True,
            }
        return {
            'status': 'completed',
            'value': result.result,
            'result_is_event_reference': False,
        }

    def _handler_outcome_record(
        self,
        invocation: dict[str, Any],
        outcome: dict[str, Any],
        *,
        process_available_after: bool = False,
    ) -> dict[str, Any]:
        return {
            'result_id': invocation['result_id'],
            'invocation_id': invocation['invocation_id'],
            'fence': invocation['fence'],
            'process_available_after': process_available_after,
            'outcome': outcome,
        }

    def _local_outcome_patch_messages(
        self,
        invocation: dict[str, Any],
        outcome_record: dict[str, Any],
    ) -> list[dict[str, Any]]:
        handler_id = invocation['handler_id']
        handler = self._handlers.get(handler_id)
        event_id = cast(str, invocation['event_id'])
        event = self._events.get(event_id)
        if handler is None or event is None:
            return []
        outcome = cast(dict[str, Any], outcome_record.get('outcome') or {})
        outcome_status = outcome.get('status')
        result_status = 'error' if outcome_status == 'errored' else 'completed'
        patch_type = 'result_cancelled' if outcome_status == 'errored' else 'result_completed'
        result = event.event_results.get(handler.id)
        if result is None:
            result = self._update_event_result_for_handler(event, handler, status='started')
        self._apply_effective_timeout_to_result(invocation, result)
        result_id = invocation.get('result_id')
        if isinstance(result_id, str):
            result.id = result_id
            self._event_id_by_result_id[result_id] = event.event_id
        result_record = {
            'result_id': result.id,
            'event_id': event.event_id,
            'handler_id': handler.id,
            'handler_name': handler.handler_name,
            'handler_file_path': handler.handler_file_path,
            'handler_timeout': handler.handler_timeout,
            'handler_slow_timeout': handler.handler_slow_timeout,
            'timeout': result.timeout,
            'handler_registered_at': handler.handler_registered_at,
            'handler_event_pattern': handler.event_pattern,
            'eventbus_name': handler.eventbus_name,
            'eventbus_id': handler.eventbus_id,
            'started_at': result.started_at,
            'completed_at': result.completed_at,
            'status': result_status,
            'result': outcome.get('value') if result_status == 'completed' else None,
            'error': outcome.get('error') if result_status == 'error' else None,
            'result_is_event_reference': outcome.get('result_is_event_reference') is True,
            'result_is_undefined': outcome.get('result_is_undefined') is True,
            'event_children': [child.event_id for child in result.event_children],
        }
        completed_at = result.completed_at or event.event_completed_at or monotonic_datetime()
        started_at = event.event_started_at or completed_at
        messages = [{'type': 'patch', 'patch': {'type': patch_type, 'result': result_record}}]
        completion = _option_value(event.event_handler_completion) or self.event_handler_completion
        snapshot = invocation.get('event_snapshot')
        if isinstance(snapshot, dict):
            completion = _option_value(snapshot.get('event_handler_completion')) or completion
        if completion != 'first':
            messages.append(
                {
                    'type': 'patch',
                    'patch': {
                        'type': 'event_completed_compact',
                        'event_id': event.event_id,
                        'completed_at': completed_at,
                        'event_started_at': started_at,
                    },
                }
            )
        return messages

    def _apply_local_completed_outcome_without_patch_messages(
        self,
        invocation: dict[str, Any],
        outcome_record: dict[str, Any],
    ) -> bool:
        event = self._apply_local_completed_outcome_fields_without_terminal_check(invocation, outcome_record)
        if event is None:
            return False
        if (
            event.event_id in self._locally_completed_without_core_patch
            and event.event_status == EventStatus.COMPLETED
            and event._event_is_complete_flag  # pyright: ignore[reportPrivateUsage]
        ):
            return True
        if not self._event_has_unfinished_results(event):
            if event.event_started_at is None:
                event.event_started_at = event.event_completed_at or monotonic_datetime()
            if event.event_completed_at is None:
                event.event_completed_at = event.event_started_at or monotonic_datetime()
            self._mark_local_event_completed_fast(event)
        return True

    def _apply_local_completed_outcomes_without_patch_messages(
        self,
        outcomes: list[dict[str, Any]],
        buses_by_invocation: dict[str, RustCoreEventBus],
        invocations_by_invocation: dict[str, dict[str, Any]],
    ) -> set[str]:
        applied_invocation_ids: set[str] = set()
        touched_events: dict[str, tuple[RustCoreEventBus, BaseEvent[Any]]] = {}
        for outcome in outcomes:
            invocation_id = outcome.get('invocation_id')
            if not isinstance(invocation_id, str):
                continue
            invocation = invocations_by_invocation.get(invocation_id)
            if invocation is None:
                continue
            outcome_bus = buses_by_invocation.get(invocation_id, self)
            event = outcome_bus._apply_local_completed_outcome_fields_without_terminal_check(invocation, outcome)
            if event is None:
                continue
            applied_invocation_ids.add(invocation_id)
            touched_events[event.event_id] = (outcome_bus, event)
        for event_bus, event in touched_events.values():
            if (
                event.event_id in event_bus._locally_completed_without_core_patch
                and event.event_status == EventStatus.COMPLETED
                and event._event_is_complete_flag  # pyright: ignore[reportPrivateUsage]
            ):
                continue
            if not event_bus._event_has_unfinished_results(event):
                if event.event_started_at is None:
                    event.event_started_at = event.event_completed_at or monotonic_datetime()
                if event.event_completed_at is None:
                    event.event_completed_at = event.event_started_at or monotonic_datetime()
                event_bus._mark_local_event_completed_fast(event)
        return applied_invocation_ids

    def _apply_local_completed_outcome_fields_without_terminal_check(
        self,
        invocation: dict[str, Any],
        outcome_record: dict[str, Any],
    ) -> BaseEvent[Any] | None:
        if self.middlewares:
            return None
        outcome = outcome_record.get('outcome')
        if not isinstance(outcome, dict) or outcome.get('status') != 'completed':
            return None
        event_id = invocation.get('event_id')
        handler_id = invocation.get('handler_id')
        if not isinstance(event_id, str) or not isinstance(handler_id, str):
            return None
        event = self._events.get(event_id)
        if event is None:
            return None
        if event.event_path != [self.label]:
            return None
        result = event.event_results.get(handler_id)
        if result is None or result.status != 'completed':
            return None
        completion = _option_value(event.event_handler_completion) or self.event_handler_completion
        snapshot = invocation.get('event_snapshot')
        if isinstance(snapshot, dict):
            completion = _option_value(snapshot.get('event_handler_completion')) or completion
        if completion == 'first':
            return None
        result_id = invocation.get('result_id')
        if isinstance(result_id, str):
            result.id = result_id
            self._event_id_by_result_id[result_id] = event.event_id
        return event

    def _mark_local_event_completed_fast(self, event: BaseEvent[Any]) -> None:
        self._remove_pending_event(event.event_id)
        self.in_flight_event_ids.discard(event.event_id)
        if event.event_status == EventStatus.PENDING and event.event_started_at is None:
            event._mark_started()  # pyright: ignore[reportPrivateUsage]
        event.event_status = EventStatus.COMPLETED
        event._event_is_complete_flag = True  # pyright: ignore[reportPrivateUsage]
        event._event_dispatch_context = None  # pyright: ignore[reportPrivateUsage]
        if event.event_completed_at is None:
            event.event_completed_at = monotonic_datetime()
        if event.event_started_at is None:
            event.event_started_at = event.event_completed_at
        self._locally_completed_without_core_patch.add(event.event_id)
        self._cancel_event_slow_warning_timer(event.event_id)
        self._trim_event_history_if_needed(force=True)
        self._forget_completed_event_if_unretained(event)
        completed_signal = event.event_completed_signal
        if completed_signal is not None:
            completed_signal.set()

    def _core_completion_already_applied_locally(self, event_id: str) -> bool:
        if self.middlewares or event_id not in self._locally_completed_without_core_patch:
            return False
        event = self._events.get(event_id)
        return event is None or (event.event_status == EventStatus.COMPLETED and not self._event_has_unfinished_results(event))

    def _schedule_core_outcome_commit(
        self,
        outcomes: list[dict[str, Any]],
        buses_by_invocation: dict[str, RustCoreEventBus],
        invocations_by_invocation: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        outcomes_by_bus: dict[RustCoreEventBus, list[dict[str, Any]]] = {}
        invocations_by_bus: dict[RustCoreEventBus, dict[str, dict[str, Any]]] = {}
        for outcome in outcomes:
            invocation_id = outcome.get('invocation_id')
            bus = buses_by_invocation.get(invocation_id, self) if isinstance(invocation_id, str) else self
            outcomes_by_bus.setdefault(bus, []).append(dict(outcome))
            if isinstance(invocation_id, str) and invocations_by_invocation is not None:
                invocation = invocations_by_invocation.get(invocation_id)
                if invocation is not None:
                    invocations_by_bus.setdefault(bus, {})[invocation_id] = invocation

        for bus, bus_outcomes in outcomes_by_bus.items():
            task = asyncio.create_task(
                bus._commit_core_outcomes_async(bus_outcomes, invocations_by_bus.get(bus, {})),
                context=bus._without_core_outcome_batch_context(),
            )
            bus._processing_tasks.add(task)
            task.add_done_callback(bus._processing_tasks.discard)
            event_ids = {
                str(invocation['event_id'])
                for invocation in invocations_by_bus.get(bus, {}).values()
                if isinstance(invocation.get('event_id'), str)
            }
            for event_id in event_ids:
                event_tasks = bus._outcome_commit_tasks_by_event_id.setdefault(event_id, set())
                event_tasks.add(task)

                def discard_commit_task(
                    done_task: asyncio.Task[None], *, event_id: str = event_id, owner: RustCoreEventBus = bus
                ) -> None:
                    tracked_tasks = owner._outcome_commit_tasks_by_event_id.get(event_id)
                    if tracked_tasks is None:
                        return
                    tracked_tasks.discard(done_task)
                    if not tracked_tasks:
                        owner._outcome_commit_tasks_by_event_id.pop(event_id, None)

                task.add_done_callback(discard_commit_task)

    async def _commit_core_outcomes_async(
        self,
        outcomes: list[dict[str, Any]],
        invocations_by_invocation: dict[str, dict[str, Any]],
    ) -> None:
        try:
            produced = self._complete_core_outcomes(outcomes, invocations_by_invocation)
            for outcome in outcomes:
                self._release_queue_jump_children_for_invocation(outcome.get('invocation_id'))
            if produced:
                await self._apply_messages_and_run_invocations_async(produced)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception('core outcome commit failed')

    def _commit_single_invocation_task(
        self,
        task: asyncio.Task[dict[str, Any]],
        invocation: dict[str, Any],
    ) -> None:
        try:
            outcome = task.result()
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception('core invocation task failed')
            return
        if not outcome:
            return

        async def commit() -> None:
            invocation_id = outcome.get('invocation_id')
            invocations_by_invocation = {invocation_id: invocation} if isinstance(invocation_id, str) else {}
            if not self._apply_local_completed_outcome_without_patch_messages(invocation, outcome):
                for message in self._local_outcome_patch_messages(invocation, outcome):
                    await self._apply_core_message_async(message)
            produced = self._complete_core_outcomes([outcome], invocations_by_invocation)
            self._release_queue_jump_children_for_invocation(invocation_id)
            if produced:
                await self._apply_messages_and_run_invocations_async(produced)

        try:
            commit_task = asyncio.create_task(commit(), context=self._without_core_outcome_batch_context())
        except RuntimeError:
            return
        self._processing_tasks.add(commit_task)
        commit_task.add_done_callback(self._processing_tasks.discard)
        event_id = invocation.get('event_id')
        if isinstance(event_id, str):
            event_tasks = self._outcome_commit_tasks_by_event_id.setdefault(event_id, set())
            event_tasks.add(commit_task)

            def discard_commit_task(
                done_task: asyncio.Task[None], *, event_id: str = event_id, owner: RustCoreEventBus = self
            ) -> None:
                tracked_tasks = owner._outcome_commit_tasks_by_event_id.get(event_id)
                if tracked_tasks is None:
                    return
                tracked_tasks.discard(done_task)
                if not tracked_tasks:
                    owner._outcome_commit_tasks_by_event_id.pop(event_id, None)

            commit_task.add_done_callback(discard_commit_task)

    def _complete_core_outcomes(
        self,
        outcomes: list[dict[str, Any]],
        invocations_by_invocation: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        commit_core = self.core
        produced: list[dict[str, Any]] = []
        if len(outcomes) > 1:
            return commit_core.complete_handler_outcomes_no_patches(outcomes, compact_response=True)
        if invocations_by_invocation and all(
            isinstance(outcome.get('invocation_id'), str)
            and cast(str, outcome['invocation_id']) in invocations_by_invocation
            and isinstance(outcome.get('outcome'), dict)
            and cast(dict[str, Any], outcome['outcome']).get('status') == 'completed'
            for outcome in outcomes
        ):
            for outcome in outcomes:
                invocation_id = cast(str, outcome['invocation_id'])
                invocation = invocations_by_invocation[invocation_id]
                outcome_payload = cast(dict[str, Any], outcome['outcome'])
                event_id = invocation.get('event_id')
                event = self._events.get(event_id) if isinstance(event_id, str) else None
                handler_id = invocation.get('handler_id')
                result = event.event_results.get(handler_id) if event is not None and isinstance(handler_id, str) else None
                process_route_after = not (
                    isinstance(event_id, str)
                    and any(event_id in bus._explicit_await_event_ids for bus in list(type(self)._instances) if not bus._closed)
                )
                complete = (
                    commit_core.complete_handler_no_patches
                    if event is not None
                    and result is not None
                    and self._can_use_patchless_completed_outcome(invocation, event, result)
                    else commit_core.complete_handler
                )
                produced.extend(
                    complete(
                        invocation,
                        outcome_payload.get('value'),
                        result_is_event_reference=outcome_payload.get('result_is_event_reference') is True,
                        process_route_after=process_route_after,
                        process_available_after=outcome.get('process_available_after') is True,
                        compact_response=True,
                    )
                )
        else:
            produced = commit_core.complete_handler_outcomes_no_patches(outcomes, compact_response=True)
        return produced

    def _invoke_handler_value_inline_sync(
        self,
        invocation: dict[str, Any],
        event: BaseEvent[Any],
        handler: EventHandler,
        abort_context: ActiveAbortContext,
    ) -> Any:
        slow_warning_timer = self._start_sync_handler_slow_warning_timer(invocation, event, handler)
        try:
            timeout_state = self._invocation_timeout_kind_and_remaining(invocation)
            if timeout_state is not None and timeout_state[1] <= 0:
                error = self._invocation_timeout_error(invocation, event, handler, timeout_state[0])
                abort_context.abort(error)
                event._cancel_pending_child_processing(error)  # pyright: ignore[reportPrivateUsage]
                raise error
            value = handler(event)
            if inspect.isawaitable(value) and not isinstance(value, BaseEvent):
                raise RuntimeError(
                    'inline synchronous handler execution requires a non-awaitable return value; '
                    f'{handler.handler_name} returned an awaitable'
                )
            return value
        finally:
            if slow_warning_timer is not None:
                slow_warning_timer.cancel()

    def _run_single_invocation_outcome_inline_sync(self, invocation: dict[str, Any]) -> dict[str, Any] | None:
        handler_id = invocation['handler_id']
        handler_entry = self._handlers.get(handler_id)
        if handler_entry is None:
            if self._closed:
                return {}
            raise KeyError(handler_id)
        event_id = cast(str, invocation['event_id'])
        result_id = invocation.get('result_id')
        invocation_id = invocation.get('invocation_id')
        if isinstance(result_id, str) and isinstance(invocation_id, str):
            self._invocation_by_result_id[result_id] = invocation_id
        event_obj = self._events.get(event_id)
        if event_obj is None:
            snapshot = invocation.get('event_snapshot')
            if not isinstance(snapshot, dict):
                snapshot = self.core.get_event(event_id)
            if not isinstance(snapshot, dict):
                raise RuntimeError(f'missing event: {event_id}')
            event_obj = self._event_from_core_record(
                self._event_types_by_pattern.get(handler_entry.event_pattern, handler_entry.event_pattern),
                cast(dict[str, Any], snapshot),
            )
            self._events[event_id] = event_obj
        _pending_result, notify_pending = self._pending_result_for_invocation(event_obj, handler_entry)
        if notify_pending:
            return None
        if isinstance(result_id, str):
            active_result = self._start_result_fast(event_obj, handler_entry)
            active_result.id = result_id
            self._apply_effective_timeout_to_result(invocation, active_result)
            self._event_id_by_result_id[result_id] = event_obj.event_id
        abort_context = ActiveAbortContext()
        try:
            tokens = self._set_handler_context(event_obj, str(handler_id))
            abort_token = set_active_abort_context(abort_context)
            try:
                with self._handler_event_path_context(event_obj):
                    result = self._invoke_handler_value_inline_sync(invocation, event_obj, handler_entry, abort_context)
            finally:
                reset_active_abort_context(abort_token)
                self._reset_handler_context(tokens)
        except EventHandlerAbortedError as exc:
            local_result = self._update_event_result_for_handler(event_obj, handler_entry, status='started')
            if isinstance(result_id, str):
                local_result.id = result_id
            self._apply_effective_timeout_to_result(invocation, local_result)
            local_result.update(error=exc)
            outcome = self._handler_error_outcome(exc)
        except Exception as exc:
            local_result = self._update_event_result_for_handler(event_obj, handler_entry, status='started')
            if isinstance(result_id, str):
                local_result.id = result_id
            self._apply_effective_timeout_to_result(invocation, local_result)
            local_result.update(error=exc)
            outcome = self._handler_error_outcome(exc)
        else:
            outcome = self._handler_completed_outcome(invocation, event_obj, handler_entry, result)
        finally:
            if isinstance(result_id, str):
                self._invocation_by_result_id.pop(result_id, None)
        return self._handler_outcome_record(
            invocation,
            outcome,
            process_available_after=self._process_available_after_handler_outcome(invocation),
        )

    def _complete_or_defer_outcome(
        self,
        core_client: RustCoreClient,
        invocation: dict[str, Any],
        outcome_record: dict[str, Any],
        *,
        active_batch: CoreOutcomeBatch | None,
        owned_batch: CoreOutcomeBatch | None,
        direct_commit: Callable[[], list[dict[str, Any]]],
    ) -> list[dict[str, Any]]:
        invocation_route_id = invocation.get('route_id') if isinstance(invocation.get('route_id'), str) else None
        if (
            active_batch is not None
            and active_batch['route_id'] is not None
            and active_batch['route_id'] == invocation_route_id
            and self._can_defer_outcome_to_active_batch(invocation)
        ):
            active_batch['outcomes'].append(outcome_record)
            return self._local_outcome_patch_messages(invocation, outcome_record)
        pending_outcomes = owned_batch['outcomes'] if owned_batch is not None else []
        if pending_outcomes:
            return core_client.complete_handler_outcomes([*pending_outcomes, outcome_record], compact_response=True)
        return direct_commit()

    def _can_defer_outcome_to_active_batch(self, invocation: dict[str, Any]) -> bool:
        if self._suppress_post_outcome_available_processing > 0:
            return False
        event_id = invocation.get('event_id')
        if not isinstance(event_id, str):
            return True
        return not any(
            event_id in bus._queue_jump_event_ids or event_id in bus._explicit_await_event_ids
            for bus in list(type(self)._instances)
            if not bus._closed
        )

    async def _run_single_invocation_outcome_async(self, invocation: dict[str, Any]) -> dict[str, Any]:
        handler_id = invocation['handler_id']
        handler_entry = self._handlers.get(handler_id)
        if handler_entry is None:
            if self._closed:
                return {}
            raise KeyError(handler_id)
        event_id = cast(str, invocation['event_id'])
        result_id = invocation.get('result_id')
        invocation_id = invocation.get('invocation_id')
        if isinstance(result_id, str) and isinstance(invocation_id, str):
            self._invocation_by_result_id[result_id] = invocation_id
        event_obj = self._events.get(event_id)
        if event_obj is None:
            snapshot = invocation.get('event_snapshot')
            if not isinstance(snapshot, dict):
                snapshot = await asyncio.to_thread(self.core.get_event, event_id)
            if not isinstance(snapshot, dict):
                raise RuntimeError(f'missing event: {event_id}')
            event_obj = self._event_from_core_record(
                self._event_types_by_pattern.get(handler_entry.event_pattern, handler_entry.event_pattern),
                cast(dict[str, Any], snapshot),
            )
            self._events[event_id] = event_obj
        pending_result, notify_pending = self._pending_result_for_invocation(event_obj, handler_entry)
        if notify_pending:
            await self._notify_pending_local_results_for_event(event_obj)
        if isinstance(result_id, str):
            previous_result_status = pending_result.status
            active_result = (
                self._update_event_result_for_handler(
                    event_obj,
                    handler_entry,
                    status='started',
                )
                if self.middlewares
                else self._start_result_fast(event_obj, handler_entry)
            )
            active_result.id = result_id
            self._apply_effective_timeout_to_result(invocation, active_result)
            self._event_id_by_result_id[result_id] = event_obj.event_id
            if self.middlewares and previous_result_status != 'started':
                await self._on_event_result_change_once(event_obj, active_result, EventStatus.STARTED)
                await self._on_event_change_once(event_obj, EventStatus.STARTED)
        abort_context = ActiveAbortContext()
        try:
            tokens = self._set_handler_context(event_obj, str(handler_id))
            abort_token = set_active_abort_context(abort_context)
            try:
                with self._handler_event_path_context(event_obj):
                    result = await self._invoke_handler_value_async(invocation, event_obj, handler_entry, abort_context)
            finally:
                reset_active_abort_context(abort_token)
                self._reset_handler_context(tokens)
        except EventHandlerAbortedError as exc:
            local_result = self._update_event_result_for_handler(event_obj, handler_entry, status='started')
            if isinstance(result_id, str):
                local_result.id = result_id
            self._apply_effective_timeout_to_result(invocation, local_result)
            local_result.update(error=exc)
            outcome = self._handler_error_outcome(exc)
        except Exception as exc:
            local_result = self._update_event_result_for_handler(event_obj, handler_entry, status='started')
            if isinstance(result_id, str):
                local_result.id = result_id
            self._apply_effective_timeout_to_result(invocation, local_result)
            local_result.update(error=exc)
            outcome = self._handler_error_outcome(exc)
        else:
            outcome = self._handler_completed_outcome(invocation, event_obj, handler_entry, result)
        finally:
            if isinstance(result_id, str):
                self._invocation_by_result_id.pop(result_id, None)
        return self._handler_outcome_record(
            invocation,
            outcome,
            process_available_after=self._process_available_after_handler_outcome(invocation),
        )

    async def _run_single_invocation_async(self, invocation: dict[str, Any]) -> list[dict[str, Any]]:
        handler_id = invocation['handler_id']
        handler_entry = self._handlers.get(handler_id)
        if handler_entry is None:
            if self._closed:
                return []
            raise KeyError(handler_id)
        event_id = cast(str, invocation['event_id'])
        result_id = invocation.get('result_id')
        invocation_id = invocation.get('invocation_id')
        if isinstance(result_id, str) and isinstance(invocation_id, str):
            self._invocation_by_result_id[result_id] = invocation_id
        event_obj = self._events.get(event_id)
        if event_obj is None:
            snapshot = invocation.get('event_snapshot')
            if not isinstance(snapshot, dict):
                snapshot = await asyncio.to_thread(self.core.get_event, event_id)
            if not isinstance(snapshot, dict):
                raise RuntimeError(f'missing event: {event_id}')
            event_obj = self._event_from_core_record(
                self._event_types_by_pattern.get(handler_entry.event_pattern, handler_entry.event_pattern),
                cast(dict[str, Any], snapshot),
            )
            self._events[event_id] = event_obj
        pending_result, notify_pending = self._pending_result_for_invocation(event_obj, handler_entry)
        if notify_pending:
            await self._notify_pending_local_results_for_event(event_obj)
        if isinstance(result_id, str):
            previous_result_status = pending_result.status
            active_result = (
                self._update_event_result_for_handler(
                    event_obj,
                    handler_entry,
                    status='started',
                )
                if self.middlewares
                else self._start_result_fast(event_obj, handler_entry)
            )
            active_result.id = result_id
            self._apply_effective_timeout_to_result(invocation, active_result)
            self._event_id_by_result_id[result_id] = event_obj.event_id
            if self.middlewares and previous_result_status != 'started':
                await self._on_event_result_change_once(event_obj, active_result, EventStatus.STARTED)
                await self._on_event_change_once(event_obj, EventStatus.STARTED)
        abort_context = ActiveAbortContext()
        active_batch = _core_outcome_batch_context.get()
        owned_batch: CoreOutcomeBatch | None = (
            {'outcomes': [], 'route_id': invocation.get('route_id') if isinstance(invocation.get('route_id'), str) else None}
            if active_batch is None
            else None
        )
        batch_token: contextvars.Token[CoreOutcomeBatch | None] | None = None
        if owned_batch is not None:
            batch_token = _core_outcome_batch_context.set(owned_batch)
        try:
            tokens = self._set_handler_context(event_obj, str(handler_id))
            abort_token = set_active_abort_context(abort_context)
            try:
                with self._handler_event_path_context(event_obj):
                    result = await self._invoke_handler_value_async(invocation, event_obj, handler_entry, abort_context)
            finally:
                reset_active_abort_context(abort_token)
                self._reset_handler_context(tokens)
        except EventHandlerAbortedError as exc:
            local_result = self._update_event_result_for_handler(event_obj, handler_entry, status='started')
            if isinstance(result_id, str):
                local_result.id = result_id
            self._apply_effective_timeout_to_result(invocation, local_result)
            local_result.update(error=exc)
            outcome = self._handler_error_outcome(exc)
            outcome_record = self._handler_outcome_record(
                invocation,
                outcome,
                process_available_after=self._process_available_after_handler_outcome(invocation),
            )
            if batch_token is not None:
                _core_outcome_batch_context.reset(batch_token)
                batch_token = None
            messages = self._complete_or_defer_outcome(
                self.core,
                invocation,
                outcome_record,
                active_batch=active_batch,
                owned_batch=owned_batch,
                direct_commit=lambda exc=exc: self.core.error_handler(
                    invocation,
                    exc,
                    process_route_after=self._process_route_after_handler_outcome(invocation),
                    process_available_after=self._process_available_after_handler_outcome(invocation),
                    compact_response=True,
                ),
            )
        except Exception as exc:
            local_result = self._update_event_result_for_handler(event_obj, handler_entry, status='started')
            if isinstance(result_id, str):
                local_result.id = result_id
            self._apply_effective_timeout_to_result(invocation, local_result)
            local_result.update(error=exc)
            outcome = self._handler_error_outcome(exc)
            outcome_record = self._handler_outcome_record(
                invocation,
                outcome,
                process_available_after=self._process_available_after_handler_outcome(invocation),
            )
            if batch_token is not None:
                _core_outcome_batch_context.reset(batch_token)
                batch_token = None
            messages = self._complete_or_defer_outcome(
                self.core,
                invocation,
                outcome_record,
                active_batch=active_batch,
                owned_batch=owned_batch,
                direct_commit=lambda exc=exc: self.core.error_handler(
                    invocation,
                    exc,
                    process_route_after=self._process_route_after_handler_outcome(invocation),
                    process_available_after=self._process_available_after_handler_outcome(invocation),
                    compact_response=True,
                ),
            )
        else:
            outcome = self._handler_completed_outcome(invocation, event_obj, handler_entry, result)
            outcome_record = self._handler_outcome_record(
                invocation,
                outcome,
                process_available_after=self._process_available_after_handler_outcome(invocation),
            )
            if batch_token is not None:
                _core_outcome_batch_context.reset(batch_token)
                batch_token = None
            messages = self._complete_or_defer_outcome(
                self.core,
                invocation,
                outcome_record,
                active_batch=active_batch,
                owned_batch=owned_batch,
                direct_commit=lambda: self._complete_invocation_with_typed_result(
                    self.core,
                    invocation,
                    event_obj,
                    handler_entry,
                    result,
                ),
            )
        finally:
            if batch_token is not None:
                _core_outcome_batch_context.reset(batch_token)
            if isinstance(result_id, str):
                self._invocation_by_result_id.pop(result_id, None)
        self._release_queue_jump_children_for_invocation(invocation.get('invocation_id'))
        return list(messages)

    def _complete_invocation_with_typed_result(
        self,
        core_client: RustCoreClient,
        invocation: dict[str, Any],
        event: BaseEvent[Any],
        handler: EventHandler,
        handler_return_value: Any,
    ) -> list[dict[str, Any]]:
        existing_result = event.event_results.get(handler.id)
        if existing_result is not None and existing_result.status == 'error':
            return core_client.error_handler(
                invocation,
                existing_result.error or 'handler failed',
                process_route_after=self._process_route_after_handler_outcome(invocation),
                compact_response=True,
            )
        result = self._update_event_result_for_handler(event, handler, result=handler_return_value)
        self._apply_effective_timeout_to_result(invocation, result)
        result_id = invocation.get('result_id')
        if isinstance(result_id, str):
            result.id = result_id
            self._event_id_by_result_id[result_id] = event.event_id
        if result.status == 'error':
            return core_client.error_handler(
                invocation,
                result.error or 'handler result failed event_result_type validation',
                process_route_after=self._process_route_after_handler_outcome(invocation),
                process_available_after=self._process_available_after_handler_outcome(invocation),
                compact_response=True,
            )
        if isinstance(result.result, BaseEvent):
            return core_client.complete_handler(
                invocation,
                {'event_id': result.result.event_id, 'event_type': result.result.event_type},
                result_is_event_reference=True,
                process_route_after=self._process_route_after_handler_outcome(invocation),
                process_available_after=self._process_available_after_handler_outcome(invocation),
                compact_response=True,
            )
        complete = (
            core_client.complete_handler_no_patches
            if self._can_use_patchless_completed_outcome(invocation, event, result)
            else core_client.complete_handler
        )
        return complete(
            invocation,
            result.result,
            process_route_after=self._process_route_after_handler_outcome(invocation),
            process_available_after=self._process_available_after_handler_outcome(invocation),
            compact_response=True,
        )

    def _can_use_patchless_completed_outcome(
        self,
        invocation: dict[str, Any],
        event: BaseEvent[Any],
        result: EventResult[Any],
    ) -> bool:
        if invocation.get('bus_id') != self.bus_id:
            return False
        if result.status != 'completed':
            return False
        if _option_value(event.event_handler_completion) == 'first':
            return False
        snapshot = invocation.get('event_snapshot')
        if isinstance(snapshot, dict) and _option_value(snapshot.get('event_handler_completion')) == 'first':
            return False
        return True

    @staticmethod
    def _can_schedule_async_processing() -> bool:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return False
        return loop.is_running()

    def _set_handler_context(self, event: BaseEvent[Any], handler_id: str) -> list[tuple[Any, Any]]:
        try:
            from abxbus.event_bus import EventBus
        except Exception:
            return []
        if not isinstance(self, EventBus):
            return []
        return [
            (type(self).current_event_context, type(self).current_event_context.set(event)),
            (type(self).current_handler_id_context, type(self).current_handler_id_context.set(handler_id)),
            (type(self).current_eventbus_context, type(self).current_eventbus_context.set(self)),
        ]

    @staticmethod
    def _reset_handler_context(tokens: list[tuple[Any, Any]]) -> None:
        for context_var, token in reversed(tokens):
            context_var.reset(token)

    @contextmanager
    def _run_with_handler_dispatch_context(self, event: BaseEvent[Any], handler_id: str) -> Any:
        tokens = self._set_handler_context(event, handler_id)
        try:
            yield
        finally:
            self._reset_handler_context(tokens)

    async def on_event_change(self, event: BaseEvent[Any], status: EventStatus) -> None:
        for middleware in self.middlewares:
            hook = getattr(middleware, 'on_event_change', None)
            if hook is None:
                continue
            result = hook(cast(Any, self), event, status)
            if inspect.isawaitable(result):
                task = asyncio.ensure_future(cast(Awaitable[Any], result))
                self._middleware_hook_tasks.add(task)
                try:
                    await task
                finally:
                    self._middleware_hook_tasks.discard(task)

    async def _on_event_change_once(self, event: BaseEvent[Any], status: EventStatus) -> None:
        key = (event.event_id, str(status))
        if key in self._middleware_event_statuses:
            return
        self._middleware_event_statuses.add(key)
        await self.on_event_change(event, status)

    async def _on_event_result_change_once(
        self,
        event: BaseEvent[Any],
        event_result: EventResult[Any],
        status: EventStatus,
    ) -> None:
        key = (event.event_id, event_result.handler_id, str(status))
        if key in self._middleware_result_statuses:
            return
        self._middleware_result_statuses.add(key)
        await self.on_event_result_change(event, event_result, status)

    async def _notify_pending_local_results_for_event(self, event: BaseEvent[Any]) -> None:
        if not self.middlewares:
            return
        await self._on_event_change_once(event, EventStatus.PENDING)
        for result in event.event_results.values():
            if result.status == 'pending':
                await self._on_event_result_change_once(event, result, EventStatus.PENDING)

    async def on_event_result_change(
        self,
        event: BaseEvent[Any],
        event_result: EventResult[Any],
        status: EventStatus,
    ) -> None:
        original_status = event_result.status
        phase_status = str(status)
        should_restore_status = status in (EventStatus.PENDING, EventStatus.STARTED) and original_status != phase_status
        if should_restore_status:
            event_result.status = cast(Any, phase_status)
        try:
            for middleware in self.middlewares:
                hook = getattr(middleware, 'on_event_result_change', None)
                if hook is None:
                    continue
                result = hook(cast(Any, self), event, event_result, status)
                if inspect.isawaitable(result):
                    task = asyncio.ensure_future(cast(Awaitable[Any], result))
                    self._middleware_hook_tasks.add(task)
                    try:
                        await task
                    finally:
                        self._middleware_hook_tasks.discard(task)
        finally:
            if should_restore_status:
                event_result.status = original_status

    async def on_bus_handlers_change(self, handler: EventHandler, registered: bool) -> None:
        for middleware in self.middlewares:
            hook = getattr(middleware, 'on_bus_handlers_change', None)
            if hook is None:
                continue
            result = hook(cast(Any, self), handler, registered)
            if inspect.isawaitable(result):
                task = asyncio.ensure_future(cast(Awaitable[Any], result))
                self._middleware_hook_tasks.add(task)
                try:
                    await task
                finally:
                    self._middleware_hook_tasks.discard(task)

    @contextmanager
    def _handler_event_path_context(self, event: BaseEvent[Any]) -> Any:
        original_path = list(event.event_path)
        if event.event_path[-1:] != [self.label]:
            event.event_path = [label for label in event.event_path if label != self.label]
            event.event_path.append(self.label)
        try:
            yield
        finally:
            final_path = list(event.event_path)
            merged_path = list(original_path)
            for label in final_path:
                if label not in merged_path:
                    merged_path.append(label)
            event.event_path = merged_path

    def _has_async_local_handlers(self, event: BaseEvent[Any]) -> bool:
        for handler_id in self.handlers_by_key.get('*', []):
            handler = self._handlers.get(handler_id)
            if handler is not None and inspect.iscoroutinefunction(handler.handler):
                return True
        for handler_id in self.handlers_by_key.get(event.event_type, []):
            handler = self._handlers.get(handler_id)
            if handler is not None and inspect.iscoroutinefunction(handler.handler):
                return True
        return False

    def _matching_local_handlers_for_event(self, event: BaseEvent[Any]) -> list[EventHandler]:
        handlers: list[EventHandler] = []
        seen: set[str] = set()
        for handler_id in [*self.handlers_by_key.get(event.event_type, []), *self.handlers_by_key.get('*', [])]:
            if handler_id in seen:
                continue
            seen.add(handler_id)
            handler = self._handlers.get(handler_id)
            if handler is not None:
                handlers.append(handler)
        return handlers

    def _ensure_local_pending_results_for_event(self, event: BaseEvent[Any]) -> None:
        if (_option_value(event.event_handler_completion) or self.event_handler_completion) == 'first':
            return
        for handler in self._matching_local_handlers_for_event(event):
            result = event.event_results.get(handler.id)
            if result is None:
                result = self._create_pending_result_fast(event, handler)
            elif result.handler.id != handler.id:
                result.handler = handler
            self._event_id_by_result_id[result.id] = event.event_id
        if event.event_results:
            self._event_ids_without_results.discard(event.event_id)

    @staticmethod
    def _create_pending_result_fast(event: BaseEvent[Any], handler: EventHandler) -> EventResult[Any]:
        result = EventResult[Any].construct_pending_handler_result(
            event_id=event.event_id,
            handler=handler,
            status='pending',
            timeout=event.event_timeout,
            result_type=event.event_result_type,
        )
        event.event_results[handler.id] = result
        return result

    def _pending_result_for_invocation(
        self,
        event: BaseEvent[Any],
        handler: EventHandler,
    ) -> tuple[EventResult[Any], bool]:
        if self.middlewares:
            self._ensure_local_pending_results_for_event(event)
            result = event.event_results.get(handler.id)
            if result is None:
                result = self._create_pending_result_fast(event, handler)
            return result, True
        result = event.event_results.get(handler.id)
        if result is None:
            self._ensure_local_pending_results_for_event(event)
            result = event.event_results.get(handler.id)
            if result is None:
                result = self._create_pending_result_fast(event, handler)
        self._event_id_by_result_id[result.id] = event.event_id
        return result, False

    def _complete_untyped_result_fast(
        self,
        event: BaseEvent[Any],
        handler: EventHandler,
        handler_return_value: Any,
    ) -> EventResult[Any] | None:
        if event.event_result_type is not None or isinstance(handler_return_value, BaseEvent):
            return None
        result = event.event_results.get(handler.id)
        if result is None:
            result = EventResult[Any].construct_pending_handler_result(
                event_id=event.event_id,
                handler=handler,
                status='completed',
                timeout=event.event_timeout,
                result_type=None,
            )
            event.event_results[handler.id] = result
        elif result.handler.id != handler.id:
            object.__setattr__(result, 'handler', handler)
        now = monotonic_datetime()
        object.__setattr__(result, 'status', 'completed')
        object.__setattr__(result, 'result', handler_return_value)
        object.__setattr__(result, 'error', None)
        if result.started_at is None:
            object.__setattr__(result, 'started_at', now)
        if result.completed_at is None:
            object.__setattr__(result, 'completed_at', now)
        completed_signal = result.handler_completed_signal
        if completed_signal is not None:
            completed_signal.set()
        if event.event_started_at is None:
            event.event_started_at = result.started_at
        if event.event_status == EventStatus.PENDING:
            event.event_status = EventStatus.STARTED
        return result

    def _start_result_fast(
        self,
        event: BaseEvent[Any],
        handler: EventHandler,
    ) -> EventResult[Any]:
        result = event.event_results.get(handler.id)
        if result is None:
            result = EventResult[Any].construct_pending_handler_result(
                event_id=event.event_id,
                handler=handler,
                status='started',
                timeout=event.event_timeout,
                result_type=event.event_result_type,
            )
            event.event_results[handler.id] = result
        elif result.handler.id != handler.id:
            object.__setattr__(result, 'handler', handler)
        now = result.started_at or monotonic_datetime()
        object.__setattr__(result, 'status', 'started')
        object.__setattr__(result, 'started_at', now)
        event.event_completed_at = None
        if event.event_started_at is None or now < event.event_started_at:
            event.event_started_at = now
        if event.event_status == EventStatus.PENDING:
            event.event_status = EventStatus.STARTED
        event._event_is_complete_flag = False  # pyright: ignore[reportPrivateUsage]
        return result

    def _update_event_result_for_handler(
        self,
        event: BaseEvent[Any],
        handler: EventHandler,
        **kwargs: Any,
    ) -> EventResult[Any]:
        result = event.event_results.get(handler.id)
        if result is None:
            result = EventResult[Any].construct_pending_handler_result(
                event_id=event.event_id,
                handler=handler,
                status=cast(Literal['pending', 'started', 'completed', 'error'], kwargs.get('status', 'pending')),
                timeout=event.event_timeout,
                result_type=event.event_result_type,
            )
            event.event_results[handler.id] = result
        elif result.handler.id != handler.id:
            result.handler = handler

        result.update(**kwargs)
        if result.status == 'started' and result.started_at is not None:
            event._mark_started(result.started_at)  # pyright: ignore[reportPrivateUsage]
        if 'timeout' in kwargs:
            result.timeout = kwargs['timeout']
        if kwargs.get('status') in ('pending', 'started'):
            event.event_completed_at = None
        return result

    @staticmethod
    def _event_has_unfinished_results(event: BaseEvent[Any]) -> bool:
        return any(result.status in ('pending', 'started') for result in event.event_results.values())

    def _handler_matches_event(self, handler: EventHandler, event: BaseEvent[Any]) -> bool:
        return handler.event_pattern == '*' or handler.event_pattern == event.event_type

    def _track_event_replayability_for_new_handlers(self, event: BaseEvent[Any]) -> None:
        if event.event_results:
            self._event_ids_without_results.discard(event.event_id)
        elif event.event_id in self._events:
            self._event_ids_without_results.add(event.event_id)

    def _has_events_replayable_for_new_handler(self) -> bool:
        if self.in_flight_event_ids:
            return True
        return bool(self._event_ids_without_results)

    def _restart_pending_events_for_handler(self, handler: EventHandler) -> None:
        for event in list(self._events.values()):
            if not self._handler_matches_event(handler, event):
                continue
            if handler.id in event.event_results:
                continue
            if event.event_status != EventStatus.PENDING and event.event_results:
                continue
            event.event_status = EventStatus.PENDING
            event.event_completed_at = None
            event._event_is_complete_flag = False  # pyright: ignore[reportPrivateUsage]
            on_idle = self._on_idle
            if on_idle is not None:
                on_idle.clear()
            self.in_flight_event_ids.add(event.event_id)
            if event.event_id not in self._pending_event_ids:
                self.pending_event_queue.put_nowait(event)
                self._pending_event_ids.add(event.event_id)
            self._ensure_local_pending_results_for_event(event)
            event_record = self._event_record_for_core(event)
            event_record.pop('event_pending_bus_count', None)
            event_record.pop('event_results', None)
            event_record['event_status'] = 'pending'
            event_record['event_started_at'] = None
            event_record['event_completed_at'] = None
            core_messages = self.core.emit_event(
                event_record,
                self.bus_id,
                defer_start=False,
                compact_response=True,
                parent_invocation_id=None,
                block_parent_completion=False,
                pause_parent_route=False,
            )
            if self._core_messages_start_work(core_messages):
                self._is_running = True
            if not core_messages:
                continue
            if self._can_schedule_async_processing():
                self._ensure_background_driver(event.event_id, initial_messages=core_messages)
            else:
                self._apply_non_invocation_messages(core_messages)

    def _has_local_handlers(self, event: BaseEvent[Any]) -> bool:
        return bool(self.handlers_by_key.get('*') or self.handlers_by_key.get(event.event_type))

    def _get_handlers_for_event(self, event: BaseEvent[Any]) -> dict[str, EventHandler]:
        """Compatibility helper for BaseEvent direct handler execution and perf instrumentation."""
        return {handler.id: handler for handler in self._matching_local_handlers_for_event(event)}

    async def _run_handler(
        self,
        event: BaseEvent[Any],
        handler: EventHandler,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Compatibility helper mirroring the pre-core EventBus handler hook."""
        result = event.event_results.get(handler.id)
        if result is None:
            result = self._update_event_result_for_handler(event, handler, status='pending')
        resolved_timeout = timeout
        if resolved_timeout is None:
            resolved_timeout = self._number_option(handler.handler_timeout)
        if resolved_timeout is None:
            resolved_timeout = self._number_option(event.event_handler_timeout)
        if resolved_timeout is None:
            resolved_timeout = self._number_option(self.event_handler_timeout)
        if resolved_timeout is None:
            resolved_timeout = self._number_option(event.event_timeout)
        if resolved_timeout is None:
            resolved_timeout = self._number_option(self.event_timeout)
        slow_timeout = self._number_option(handler.handler_slow_timeout)
        if slow_timeout is None:
            slow_timeout = self._number_option(event.event_handler_slow_timeout)
        if slow_timeout is None:
            slow_timeout = self._number_option(self.event_handler_slow_timeout)
        try:
            return await result.run_handler(
                event,
                eventbus=cast(Any, self),
                timeout=resolved_timeout,
                handler_slow_timeout=slow_timeout,
                notify_event_started=True,
            )
        finally:
            event._mark_completed(current_bus=cast(Any, self))  # pyright: ignore[reportPrivateUsage]

    async def _process_event(self, event: BaseEvent[Any]) -> BaseEvent[Any]:
        """Compatibility helper for old instrumentation hooks."""
        await event._run_handlers(eventbus=cast(Any, self))  # pyright: ignore[reportPrivateUsage]
        event._mark_completed(current_bus=cast(Any, self))  # pyright: ignore[reportPrivateUsage]
        return event

    def _missing_local_handler_results(self, event: BaseEvent[Any]) -> bool:
        for handler in self._handlers.values():
            if handler.event_pattern != '*' and handler.event_pattern != event.event_type:
                continue
            if handler.id not in event.event_results:
                return True
        return False

    def _hydrate_completed_event_if_missing_results(self, event: BaseEvent[Any]) -> None:
        if event.event_id in self._hydrating_completed_event_ids:
            return
        if not self._missing_local_handler_results(event):
            return
        self._hydrating_completed_event_ids.add(event.event_id)
        try:
            snapshot = self._get_event_snapshot_from_core(event.event_id)
            if isinstance(snapshot, dict):
                self._apply_core_snapshot_to_event(event, snapshot)
        finally:
            self._hydrating_completed_event_ids.discard(event.event_id)

    def _apply_core_message(self, message: dict[str, Any]) -> None:
        if message.get('type') == 'event_completed':
            event_id = message.get('event_id')
            if isinstance(event_id, str):
                if self._core_completion_already_applied_locally(event_id):
                    return
                self._mark_event_completed_from_core(event_id)
            return
        if message.get('type') != 'patch':
            return
        patch_obj = message.get('patch')
        patch = cast(dict[str, Any], patch_obj) if isinstance(patch_obj, dict) else None
        if patch is None:
            return
        self.core.ack_patch_messages([message])

        patch_type = patch.get('type')
        if patch_type == 'event_started':
            event_id = patch.get('event_id')
            if isinstance(event_id, str) and event_id in self._events:
                event = self._events[event_id]
                started_at = patch.get('started_at')
                if isinstance(started_at, str):
                    event.event_started_at = started_at
                    event.event_status = EventStatus.STARTED
                else:
                    event._mark_started()  # pyright: ignore[reportPrivateUsage]
                self._remove_pending_event(event_id)
            return
        if patch_type == 'result_pending':
            self._apply_result_record(cast(dict[str, Any], patch.get('result')), status_override='pending')
            return
        if patch_type == 'result_started':
            result_id = patch.get('result_id')
            invocation_id = patch.get('invocation_id')
            started_at = patch.get('started_at')
            if isinstance(result_id, str) and isinstance(invocation_id, str):
                self._invocation_by_result_id[result_id] = invocation_id
            event_id = self._event_id_by_result_id.get(result_id) if isinstance(result_id, str) else None
            event = self._events.get(event_id) if event_id is not None else None
            if event is None:
                return
            for result in event.event_results.values():
                if result.id == result_id:
                    if result.status in ('completed', 'error'):
                        return
                    result.update(status='started')
                    if isinstance(started_at, str):
                        result.started_at = started_at
                    event.event_status = EventStatus.STARTED
                    event.event_completed_at = None
                    event._event_is_complete_flag = False  # pyright: ignore[reportPrivateUsage]
                    self.in_flight_event_ids.add(event.event_id)
                    self._locally_completed_without_core_patch.discard(event.event_id)
                    return
            return
        if patch_type == 'result_slow_warning':
            self._log_result_slow_warning(patch)
            return
        if patch_type == 'event_slow_warning':
            self._log_event_slow_warning(patch)
            return
        if patch_type == 'result_completed':
            self._apply_result_record(cast(dict[str, Any], patch.get('result')))
            return
        if patch_type in ('result_cancelled', 'result_timed_out'):
            self._apply_result_record(cast(dict[str, Any], patch.get('result')))
            return
        if patch_type == 'event_completed':
            event_id = patch.get('event_id')
            if isinstance(event_id, str):
                if self._core_completion_already_applied_locally(event_id):
                    return
                completed_at = patch.get('completed_at')
                raw_results = patch.get('event_results')
                if not (isinstance(raw_results, dict) and raw_results) and any(
                    (event := bus._events.get(event_id)) is not None and bus._event_has_unfinished_results(event)
                    for bus in list(type(self)._instances)
                ):
                    return
                for bus in list(type(self)._instances):
                    bus._remove_pending_event(event_id)
                    bus.in_flight_event_ids.discard(event_id)
                for bus in list(type(self)._instances):
                    event = bus._events.get(event_id)
                    if event is None:
                        continue
                    if isinstance(raw_results, dict):
                        for raw_result in cast(dict[str, Any], raw_results).values():
                            bus._apply_result_record(cast(dict[str, Any], raw_result))
                    raw_path = patch.get('event_path')
                    if isinstance(raw_path, list) and all(isinstance(label, str) for label in raw_path):
                        event.event_path = raw_path
                    raw_parent_id = patch.get('event_parent_id')
                    event.event_parent_id = raw_parent_id if isinstance(raw_parent_id, str) else None
                    raw_handler_id = patch.get('event_emitted_by_handler_id')
                    event.event_emitted_by_handler_id = raw_handler_id if isinstance(raw_handler_id, str) else None
                    raw_result_id = patch.get('event_emitted_by_result_id')
                    event.event_emitted_by_result_id = raw_result_id if isinstance(raw_result_id, str) else None
                    raw_blocks_parent = patch.get('event_blocks_parent_completion')
                    if isinstance(raw_blocks_parent, bool):
                        event.event_blocks_parent_completion = raw_blocks_parent
                    raw_started_at = patch.get('event_started_at')
                    event.event_started_at = raw_started_at if isinstance(raw_started_at, str) else event.event_started_at
                    if isinstance(completed_at, str):
                        event.event_completed_at = completed_at
                    if event.event_completed_at is None:
                        event.event_completed_at = monotonic_datetime()
                    if event.event_started_at is None:
                        event.event_started_at = event.event_completed_at
                    event.event_status = EventStatus.COMPLETED
                    bus._cancel_event_slow_warning_timer(event.event_id)
                    bus._hydrate_completed_event_if_missing_results(event)
                    event._event_is_complete_flag = True  # pyright: ignore[reportPrivateUsage]
                    event._event_dispatch_context = None  # pyright: ignore[reportPrivateUsage]
                    bus._trim_event_history_if_needed(force=True)
                    bus._forget_completed_event_if_unretained(event)
                    completed_signal = event.event_completed_signal
                    if completed_signal is not None:
                        completed_signal.set()
            return
        if patch_type == 'event_completed_compact':
            event_id = patch.get('event_id')
            if isinstance(event_id, str):
                if self._core_completion_already_applied_locally(event_id):
                    return
                if any(
                    (event := bus._events.get(event_id)) is not None and bus._event_has_unfinished_results(event)
                    for bus in list(type(self)._instances)
                ):
                    return
                for bus in list(type(self)._instances):
                    bus._remove_pending_event(event_id)
                    bus.in_flight_event_ids.discard(event_id)
                for bus in list(type(self)._instances):
                    event = bus._events.get(event_id)
                    if event is None:
                        continue
                    completed_at = patch.get('completed_at')
                    started_at = patch.get('event_started_at')
                    if isinstance(started_at, str):
                        event.event_started_at = started_at
                    if event.event_status == EventStatus.PENDING and event.event_started_at is None:
                        event._mark_started()  # pyright: ignore[reportPrivateUsage]
                    event.event_status = EventStatus.COMPLETED
                    event._event_is_complete_flag = True  # pyright: ignore[reportPrivateUsage]
                    event._event_dispatch_context = None  # pyright: ignore[reportPrivateUsage]
                    if isinstance(completed_at, str):
                        event.event_completed_at = completed_at
                    if event.event_completed_at is None:
                        event.event_completed_at = monotonic_datetime()
                    if event.event_started_at is None:
                        event.event_started_at = event.event_completed_at
                    bus._cancel_event_slow_warning_timer(event.event_id)
                    bus._hydrate_completed_event_if_missing_results(event)
                    bus._merge_live_event_metadata(event)
                    bus._trim_event_history_if_needed(force=True)
                    bus._forget_completed_event_if_unretained(event)
                    completed_signal = event.event_completed_signal
                    if completed_signal is not None:
                        completed_signal.set()
            return

    def _mark_event_completed_from_core(self, event_id: str) -> None:
        if any(
            (event := bus._events.get(event_id)) is not None and bus._event_has_unfinished_results(event)
            for bus in list(type(self)._instances)
        ):
            return
        for bus in list(type(self)._instances):
            bus._remove_pending_event(event_id)
            bus.in_flight_event_ids.discard(event_id)
        for bus in list(type(self)._instances):
            event = bus._events.get(event_id)
            if event is None:
                continue
            if event.event_status == EventStatus.PENDING:
                event._mark_started()  # pyright: ignore[reportPrivateUsage]
            event.event_status = EventStatus.COMPLETED
            event._event_is_complete_flag = True  # pyright: ignore[reportPrivateUsage]
            event._event_dispatch_context = None  # pyright: ignore[reportPrivateUsage]
            if event.event_completed_at is None:
                event.event_completed_at = monotonic_datetime()
            if event.event_started_at is None:
                event.event_started_at = event.event_completed_at
            bus._cancel_event_slow_warning_timer(event.event_id)
            bus._hydrate_completed_event_if_missing_results(event)
            bus._merge_live_event_metadata(event)
            bus._trim_event_history_if_needed(force=True)
            bus._forget_completed_event_if_unretained(event)
            completed_signal = event.event_completed_signal
            if completed_signal is not None:
                completed_signal.set()

    async def _apply_core_message_async(self, message: dict[str, Any]) -> None:
        patch_obj = message.get('patch') if message.get('type') == 'patch' else None
        patch = cast(dict[str, Any], patch_obj) if isinstance(patch_obj, dict) else None
        patch_type = patch.get('type') if patch is not None else None
        pre_event_status: EventStatus | None = None
        pre_event_id = patch.get('event_id') if patch is not None else None
        if patch_type in ('event_started', 'event_completed', 'event_completed_compact') and isinstance(pre_event_id, str):
            pre_event = self._events.get(pre_event_id)
            if pre_event is not None:
                pre_event_status = pre_event.event_status
        self._apply_core_message(message)
        if not self.middlewares or patch is None:
            return
        if patch_type == 'event_emitted':
            event_record = patch.get('event')
            event_id = event_record.get('event_id') if isinstance(event_record, dict) else None
            event = self._events.get(event_id) if isinstance(event_id, str) else None
            if event is not None:
                await self._on_event_change_once(event, EventStatus.PENDING)
            return
        if patch_type == 'event_started':
            event_id = patch.get('event_id')
            event = self._events.get(event_id) if isinstance(event_id, str) else None
            if event is not None:
                await self._on_event_change_once(event, EventStatus.STARTED)
            return
        if patch_type in ('event_completed', 'event_completed_compact'):
            event_id = patch.get('event_id')
            event = self._events.get(event_id) if isinstance(event_id, str) else None
            if event is not None:
                await self._on_event_change_once(event, EventStatus.COMPLETED)
            return
        if patch_type in ('result_pending', 'result_completed', 'result_cancelled', 'result_timed_out'):
            record = patch.get('result')
            if not isinstance(record, dict):
                return
            event_id = record.get('event_id')
            handler_id = record.get('handler_id')
            event = self._events.get(event_id) if isinstance(event_id, str) else None
            result = event.event_results.get(handler_id) if event is not None and isinstance(handler_id, str) else None
            if event is not None and result is not None:
                status = EventStatus.PENDING if patch_type == 'result_pending' else EventStatus.COMPLETED
                await self._on_event_result_change_once(event, result, status)
            return
        if patch_type == 'result_started':
            result_id = patch.get('result_id')
            if not isinstance(result_id, str):
                return
            event_id = self._event_id_by_result_id.get(result_id)
            event = self._events.get(event_id) if event_id is not None else None
            if event is None:
                return
            for result in event.event_results.values():
                if result.id == result_id:
                    await self._on_event_result_change_once(event, result, EventStatus.STARTED)
                    return

    def _apply_result_record(self, record: dict[str, Any] | None, *, status_override: str | None = None) -> None:
        if not isinstance(record, dict):
            return
        event_id = record.get('event_id')
        handler_id = record.get('handler_id')
        if not isinstance(event_id, str) or not isinstance(handler_id, str):
            return
        event = self._events.get(event_id)
        handler = self._handlers.get(handler_id)
        if event is None or handler is None:
            return
        status = status_override or record.get('status')
        existing_result = event.event_results.get(handler_id)
        if status in ('pending', 'started') and existing_result is not None and existing_result.status in ('completed', 'error'):
            return
        if status == 'completed' and existing_result is not None and existing_result.status == 'error':
            return
        if (
            status in ('error', 'cancelled', 'timed_out')
            and existing_result is not None
            and existing_result.status == 'error'
            and existing_result.error is not None
        ):
            return
        update: dict[str, Any] = {}
        if status in ('pending', 'started'):
            update['status'] = status
        elif status == 'completed':
            result_value = record.get('result')
            if (
                isinstance(result_value, dict)
                and isinstance(result_value.get('event_id'), str)
                and isinstance(result_value.get('event_type'), str)
            ):
                if existing_result is not None and isinstance(existing_result.result, BaseEvent):
                    result_value = existing_result.result
                else:
                    result_value = BaseEvent[Any].model_validate(result_value)
            update['result'] = result_value
        elif status in ('error', 'cancelled', 'timed_out'):
            update['status'] = 'error'
            update['error'] = self._normalize_core_error(record.get('error')) or 'handler failed'
        result = self._update_event_result_for_handler(event, handler, **update)
        result.id = cast(str, record.get('result_id') or result.id)
        self._event_id_by_result_id[result.id] = event.event_id
        if status in ('pending', 'started'):
            event.event_status = EventStatus.STARTED if status == 'started' else EventStatus.PENDING
            event.event_completed_at = None
            event._event_is_complete_flag = False  # pyright: ignore[reportPrivateUsage]
            self.in_flight_event_ids.add(event.event_id)
            self._locally_completed_without_core_patch.discard(event.event_id)
        if status in ('completed', 'error', 'cancelled', 'timed_out'):
            self._invocation_by_result_id.pop(result.id, None)
        timeout = record.get('timeout')
        if isinstance(timeout, int | float):
            result.timeout = float(timeout)
        raw_child_ids = [
            child_id for child_id in cast(list[Any], record.get('event_children') or []) if isinstance(child_id, str)
        ]
        self._attach_result_children(event, result, raw_child_ids=raw_child_ids)
        self._normalize_error_result_statuses(event)

    @staticmethod
    def _elapsed_since_timestamp(started_at: str | None) -> float:
        if not started_at:
            return 0.0
        try:
            started_at_dt = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
        except ValueError:
            return 0.0
        return max(0.0, (datetime.now(UTC) - started_at_dt.astimezone(UTC)).total_seconds())

    def _log_result_slow_warning(self, patch: dict[str, Any]) -> None:
        result_id = patch.get('result_id')
        if not isinstance(result_id, str):
            return
        event_id = self._event_id_by_result_id.get(result_id)
        event = self._events.get(event_id) if event_id is not None else None
        if event is None:
            return
        for result in event.event_results.values():
            if result.id != result_id:
                continue
            started_at = result.started_at or event.event_started_at or event.event_created_at
            elapsed_seconds = self._elapsed_since_timestamp(started_at)
            logger.warning(
                '⚠️ Slow event handler: %s.on(%s#%s, %s) still running after %.1fs',
                result.eventbus_label,
                event.event_type,
                event.event_id[-4:],
                result.handler.label,
                elapsed_seconds,
            )
            return

    def _log_event_slow_warning(self, patch: dict[str, Any]) -> None:
        event_id = patch.get('event_id')
        if not isinstance(event_id, str):
            return
        event = self._events.get(event_id)
        if event is None:
            return
        running_handler_count = sum(1 for result in event.event_results.values() if result.status == 'started')
        started_at = event.event_started_at or event.event_created_at
        elapsed_seconds = self._elapsed_since_timestamp(started_at)
        logger.warning(
            '⚠️ Slow event processing: %s.on(%s#%s, %s handlers) still running after %.2fs',
            self.label,
            event.event_type,
            event.event_id[-4:],
            running_handler_count,
            elapsed_seconds,
        )

    @staticmethod
    def _normalize_error_result_statuses(event: BaseEvent[Any]) -> None:
        for result in event.event_results.values():
            if result.error is not None:
                result.status = 'error'

    def _attach_result_children(
        self,
        event: BaseEvent[Any],
        result: EventResult[Any],
        *,
        raw_child_ids: list[str] | None = None,
    ) -> None:
        child_ids = list(raw_child_ids or [])
        child_ids.extend(self._child_event_ids_by_result_id.get(result.id, ()))
        child_ids.extend(self._child_event_ids_by_parent_handler.get((event.event_id, result.handler_id), ()))
        if not child_ids:
            return
        changed = False
        for child_id in child_ids:
            if child_id == event.event_id:
                continue
            child = self._events.get(child_id)
            if child is None:
                child_snapshot = self._get_event_snapshot_from_core(child_id)
                if not isinstance(child_snapshot, dict):
                    continue
                child = self._event_from_core_record(
                    self._event_types_by_pattern.get(str(child_snapshot.get('event_type')), '*'),
                    child_snapshot,
                )
                self._events[child.event_id] = child
                self.event_history[child.event_id] = child
                self._index_event_relationships(child)
                self._trim_event_history_if_needed(force=child.event_status == EventStatus.COMPLETED)
                self._forget_completed_event_if_unretained(child)
            if self._attach_child_to_result(event, result, child):
                changed = True
        if changed:
            self._merge_referenced_event_instance(event)

    def _attach_child_to_result(
        self,
        _event: BaseEvent[Any],
        result: EventResult[Any],
        child: BaseEvent[Any],
    ) -> bool:
        for existing_child in result.event_children:
            if existing_child.event_id != child.event_id:
                continue
            if existing_child is not child:
                self._merge_event_instance(existing_child, child)
            return False
        result.event_children.append(child)
        return True

    def _merge_referenced_event_instance(self, event: BaseEvent[Any]) -> None:
        if event.event_parent_id is None:
            return
        parent = self._events.get(event.event_parent_id) or self.event_history.get(event.event_parent_id)
        if parent is None:
            return
        for result in parent.event_results.values():
            for child in result.event_children:
                if child.event_id == event.event_id and child is not event:
                    self._merge_event_instance(child, event)

    def _merge_event_instance(self, target: BaseEvent[Any], source: BaseEvent[Any]) -> None:
        if target is source:
            return
        for label in source.event_path:
            if label not in target.event_path:
                target.event_path.append(label)
        for attr in ('event_parent_id', 'event_emitted_by_handler_id', 'event_emitted_by_result_id'):
            if getattr(target, attr) is None and getattr(source, attr) is not None:
                setattr(target, attr, getattr(source, attr))
        if target.event_started_at is None and source.event_started_at is not None:
            target.event_started_at = source.event_started_at
        if source.event_status == EventStatus.COMPLETED:
            target.event_status = EventStatus.COMPLETED
            target.event_completed_at = target.event_completed_at or source.event_completed_at
        for handler_id, source_result in source.event_results.items():
            target_result = target.event_results.get(handler_id)
            if target_result is None:
                target.event_results[handler_id] = source_result
                continue
            if target_result.status in ('pending', 'started') and source_result.status in ('completed', 'error'):
                target_result.status = source_result.status
                target_result.result = source_result.result
                target_result.error = source_result.error
                target_result.completed_at = source_result.completed_at
            if target_result.started_at is None and source_result.started_at is not None:
                target_result.started_at = source_result.started_at
            for source_child in source_result.event_children:
                self._attach_child_to_result(target, target_result, source_child)

    @staticmethod
    def _normalize_core_error(error: Any) -> BaseException | None:
        if error is None:
            return None
        if isinstance(error, BaseException):
            return error
        if isinstance(error, dict):
            error_record = cast(dict[str, Any], error)
            message_value = error_record.get('message')
            message = message_value if isinstance(message_value, str) else str(error_record)
            kind = error_record.get('kind')
        else:
            message = str(error)
            kind = None
        if kind == 'handler_timeout':
            return EventHandlerTimeoutError(message)
        if kind == 'handler_aborted':
            return EventHandlerAbortedError(message)
        if kind == 'handler_cancelled':
            return EventHandlerCancelledError(message)
        if kind == 'schema_error':
            return EventHandlerResultSchemaError(message)
        lowered = message.lower()
        if 'first result resolved' in lowered:
            return EventHandlerCancelledError('Cancelled: first result resolved')
        if 'aborted' in lowered:
            return EventHandlerAbortedError(message)
        if 'cancelled' in lowered:
            return EventHandlerCancelledError(message)
        if 'timed out' in lowered:
            return EventHandlerTimeoutError(message)
        if 'event timed out' in lowered:
            return EventHandlerAbortedError(message)
        if 'expected event_result_type' in lowered:
            return EventHandlerResultSchemaError(message)
        return Exception(message)

    def _remove_pending_event(self, event_id: str) -> None:
        if event_id not in self._pending_event_ids:
            return
        self._pending_event_ids.discard(event_id)
        queue = self.pending_event_queue._queue  # pyright: ignore[reportPrivateUsage]
        if queue and queue[0].event_id == event_id:
            queue.popleft()
            return
        for event in tuple(queue):
            if event.event_id == event_id:
                self.pending_event_queue.remove_item(event)
                return

    def remove_event_from_pending_queue(self, event: BaseEvent[Any]) -> bool:
        removed = self.pending_event_queue.remove_item(event)
        if removed:
            self._pending_event_ids.discard(event.event_id)
        return removed

    def is_event_inflight_or_queued(self, event_id: str) -> bool:
        if event_id in self._pending_event_ids:
            return True
        event = self._events.get(event_id)
        return event is not None and event.event_status != EventStatus.COMPLETED

    def find_event_by_id(self, event_id: str) -> BaseEvent[Any] | None:
        local = self._events.get(event_id)
        if local is not None:
            return local
        snapshot = self._get_event_snapshot_from_core(event_id)
        if snapshot is None:
            return None
        event = BaseEvent[Any].model_validate(snapshot)
        self._events[event_id] = event
        self.event_history[event_id] = event
        self._trim_event_history_if_needed(force=event.event_status == EventStatus.COMPLETED)
        self._forget_completed_event_if_unretained(event)
        return event

    def log_tree(self) -> str:
        from abxbus.logging import log_eventbus_tree

        return log_eventbus_tree(cast(Any, self))

    def __enter__(self) -> RustCoreEventBus:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.disconnect()


def stable_core_bus_id(bus_name: str) -> str:
    return str(uuid5(NAMESPACE_DNS, f'abxbus-core-bus:{bus_name}'))
