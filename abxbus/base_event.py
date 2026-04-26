import asyncio
import contextvars
import inspect
import logging
import os
from collections.abc import AsyncIterator, Callable, Coroutine, Generator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from enum import StrEnum
from functools import partial
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Generic, Literal, Self, TypeAlias
from uuid import UUID

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    computed_field,
    field_serializer,
    field_validator,
    model_serializer,
    model_validator,
)
from typing_extensions import TypeVar  # needed to get TypeVar(default=...) above python 3.11
from uuid_extensions import uuid7str

from abxbus.event_handler import (
    ContravariantEventHandlerCallable,
    EventHandler,
    EventHandlerAbortedError,
    EventHandlerCancelledError,
    EventHandlerResultSchemaError,
    EventHandlerTimeoutError,
    NormalizedEventHandlerCallable,
)
from abxbus.helpers import (  # pyright: ignore[reportPrivateUsage]
    _run_with_slow_monitor,  # pyright: ignore[reportPrivateUsage]
    cancel_and_await,
    extract_basemodel_generic_arg,
    monotonic_datetime,
)
from abxbus.jsonschema import (
    pydantic_model_from_json_schema,
    pydantic_model_to_json_schema,
    result_type_identifier_from_schema,
    validate_result_against_type,
)

if TYPE_CHECKING:
    from abxbus.event_bus import EventBus
    from abxbus.lock_manager import ReentrantLock


logger = logging.getLogger('abxbus')

ABXBUS_LOGGING_LEVEL = os.getenv('ABXBUS_LOGGING_LEVEL', 'WARNING').upper()  # WARNING normally, otherwise DEBUG when testing
LIBRARY_VERSION = os.getenv('LIBRARY_VERSION', '0.0.1')

logger.setLevel(ABXBUS_LOGGING_LEVEL)


class EventStatus(StrEnum):
    """Lifecycle status used for events and middleware transition hooks.

    Event statuses are strictly ``pending`` -> ``started`` -> ``completed``.
    Handler-level failures are represented on ``EventResult.status == 'error'``
    and ``EventResult.error``, not as an event status.
    """

    PENDING = 'pending'
    STARTED = 'started'
    COMPLETED = 'completed'  # errored events are also considered completed


def validate_event_name(s: str) -> str:
    assert str(s).isidentifier() and not str(s).startswith('_'), f'Invalid event name: {s}'
    return str(s)


def validate_python_id_str(s: str) -> str:
    assert str(s).replace('.', '').isdigit(), f'Invalid Python ID: {s}'
    return str(s)


def validate_event_path_entry_str(s: str) -> str:
    entry = str(s)
    assert '#' in entry, f'Invalid event_path entry: {entry} (expected BusName#abcd)'
    bus_name, short_id = entry.rsplit('#', 1)
    assert bus_name.isidentifier() and short_id.isalnum() and len(short_id) == 4, (
        f'Invalid event_path entry: {entry} (expected BusName#abcd)'
    )
    return entry


def validate_uuid_str(s: str) -> str:
    uuid = UUID(str(s))
    return str(uuid)


UUIDStr: TypeAlias = Annotated[str, AfterValidator(validate_uuid_str)]
PythonIdStr: TypeAlias = Annotated[str, AfterValidator(validate_python_id_str)]
PythonIdentifierStr: TypeAlias = Annotated[str, AfterValidator(validate_event_name)]
EventPathEntryStr: TypeAlias = Annotated[str, AfterValidator(validate_event_path_entry_str)]


class EventHandlerConcurrencyMode(StrEnum):
    SERIAL = 'serial'
    PARALLEL = 'parallel'


class EventHandlerCompletionMode(StrEnum):
    ALL = 'all'
    FIRST = 'first'


class EventConcurrencyMode(StrEnum):
    GLOBAL_SERIAL = 'global-serial'
    BUS_SERIAL = 'bus-serial'
    PARALLEL = 'parallel'


T_EventResultType = TypeVar('T_EventResultType', bound=Any, default=None)
# TypeVar for BaseEvent and its subclasses
RESERVED_USER_EVENT_FIELDS = frozenset({'bus', 'emit', 'first', 'toString', 'toJSON', 'fromJSON'})
# We use contravariant=True because if a handler accepts BaseEvent,
# it can also handle any subclass of BaseEvent
T_Event = TypeVar('T_Event', bound='BaseEvent[Any]', contravariant=True, default='BaseEvent[Any]')
T_EmittedEvent = TypeVar('T_EmittedEvent', bound='BaseEvent[Any]', default='BaseEvent[Any]')
EventResultFilter = Callable[['EventResult[Any]'], bool]


def _default_format_exception_for_log(exc: BaseException) -> str:
    from traceback import TracebackException

    return ''.join(TracebackException.from_exception(exc, capture_locals=False).format())


def _as_any(value: Any) -> Any:
    return value


def _normalize_any_dict(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    value_any = _as_any(value)
    normalized: dict[str, Any] = {}
    for key, item in value_any.items():
        normalized[str(key)] = item
    return normalized


def _normalize_any_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        value_any = _as_any(value)
        return [item for item in value_any]
    return []


def _normalize_any_tuple(value: Any) -> tuple[Any, ...]:
    if isinstance(value, tuple):
        value_any = _as_any(value)
        return tuple(item for item in value_any)
    return ()


# Keep EventResult and BaseEvent co-located in this module.
# Cross-file generic forward refs between these two models caused fragile
# incomplete-model states and import-order dependent rebuild behavior in Pydantic.
# Context:
# - https://github.com/pydantic/pydantic/issues/1873
# - https://github.com/pydantic/pydantic/issues/707
# - https://stackoverflow.com/questions/77582955/how-can-i-separate-two-pydantic-models-into-different-files-when-these-models-ha
# - https://github.com/pydantic/pydantic/issues/11532
class EventResult(BaseModel, Generic[T_EventResultType]):
    """Individual result from a single handler."""

    model_config = ConfigDict(
        extra='forbid',
        arbitrary_types_allowed=True,
        validate_assignment=False,  # Validation handled in update() for flexible result types.
        validate_default=True,
        revalidate_instances='always',
    )

    # Automatically set fields, setup at Event init and updated by EventBus._run_handler()
    id: str = Field(default_factory=uuid7str)
    status: Literal['pending', 'started', 'completed', 'error'] = 'pending'
    event_id: str
    handler: EventHandler = Field(default_factory=EventHandler)
    result_type: Any = Field(default=None, exclude=True, repr=False)
    timeout: float | None = None
    started_at: str | None = None

    # Result fields, updated by EventBus._run_handler()
    result: T_EventResultType | 'BaseEvent[Any]' | None = None
    error: BaseException | None = None
    completed_at: str | None = None

    # Completion signal
    _handler_completed_signal: asyncio.Event | None = PrivateAttr(default=None)

    # Child events emitted during handler execution
    event_children: list['BaseEvent[Any]'] = Field(default_factory=lambda: [])

    @staticmethod
    def _serialize_datetime_json(value: str | datetime | None) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            value = value.astimezone(UTC).isoformat().replace('+00:00', 'Z')
        return monotonic_datetime(value)

    @staticmethod
    def _serialize_error_json(value: BaseException | None) -> dict[str, Any] | None:
        if value is None:
            return None
        return {'type': type(value).__name__, 'message': str(value)}

    @classmethod
    def _serialize_jsonable(cls, value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, datetime):
            return cls._serialize_datetime_json(value)
        if isinstance(value, BaseException):
            return cls._serialize_error_json(value)
        if isinstance(value, BaseEvent):
            return value.model_dump(mode='json')
        if isinstance(value, BaseModel):
            return value.model_dump(mode='json')
        if isinstance(value, list):
            return [cls._serialize_jsonable(item) for item in _normalize_any_list(value)]
        if isinstance(value, tuple):
            return [cls._serialize_jsonable(item) for item in _normalize_any_tuple(value)]
        if isinstance(value, dict):
            serialized_dict: dict[str, Any] = {}
            for key, item in _normalize_any_dict(value).items():
                serialized_dict[str(key)] = cls._serialize_jsonable(item)
            return serialized_dict
        return repr(value)

    @model_validator(mode='before')
    @classmethod
    def _hydrate_handler_from_flat_json(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        payload = _normalize_any_dict(data)
        if isinstance(payload.get('handler'), dict):
            raise ValueError('EventResult JSON no longer accepts nested handler payloads; use flat handler_* fields')

        if 'handler' not in payload:
            raw_handler_id = payload.pop('handler_id', None)
            raw_handler_name = payload.pop('handler_name', None)
            raw_handler_file_path = payload.pop('handler_file_path', None)
            raw_handler_timeout = payload.pop('handler_timeout', None)
            raw_handler_slow_timeout = payload.pop('handler_slow_timeout', None)
            raw_handler_registered_at = payload.pop('handler_registered_at', None)
            raw_handler_event_pattern = payload.pop('handler_event_pattern', None)
            raw_eventbus_name = payload.pop('eventbus_name', None)
            raw_eventbus_id = payload.pop('eventbus_id', None)

            has_flat_handler_fields = any(
                value is not None
                for value in (
                    raw_handler_id,
                    raw_handler_name,
                    raw_handler_file_path,
                    raw_handler_timeout,
                    raw_handler_slow_timeout,
                    raw_handler_registered_at,
                    raw_handler_event_pattern,
                    raw_eventbus_name,
                    raw_eventbus_id,
                )
            )
            if has_flat_handler_fields:
                handler_payload: dict[str, Any] = {
                    'handler_name': raw_handler_name or 'anonymous',
                    'event_pattern': raw_handler_event_pattern or '*',
                    'eventbus_name': raw_eventbus_name or 'EventBus',
                    'eventbus_id': raw_eventbus_id or '00000000-0000-0000-0000-000000000000',
                }
                if raw_handler_id is not None:
                    handler_payload['id'] = raw_handler_id
                if raw_handler_file_path is not None:
                    handler_payload['handler_file_path'] = raw_handler_file_path
                if raw_handler_timeout is not None:
                    handler_payload['handler_timeout'] = raw_handler_timeout
                if raw_handler_slow_timeout is not None:
                    handler_payload['handler_slow_timeout'] = raw_handler_slow_timeout
                if raw_handler_registered_at is not None:
                    handler_payload['handler_registered_at'] = raw_handler_registered_at
                payload['handler'] = handler_payload

        raw_children = payload.get('event_children')
        if isinstance(raw_children, list) and all(isinstance(item, str) for item in _normalize_any_list(raw_children)):
            payload['event_children'] = []

        raw_error = payload.get('error')
        if raw_error is not None and not isinstance(raw_error, BaseException):
            if isinstance(raw_error, str):
                payload['error'] = Exception(raw_error)
            elif isinstance(raw_error, dict):
                raw_error_json = _normalize_any_dict(raw_error)
                raw_message = raw_error_json.get('message')
                message = raw_message if isinstance(raw_message, str) else str(raw_error_json)
                payload['error'] = Exception(message)
            else:
                payload['error'] = Exception(str(raw_error))

        for at_key in ('started_at', 'completed_at'):
            if at_key not in payload:
                continue
            raw_at_value = payload.get(at_key)
            if raw_at_value is None:
                continue
            if isinstance(raw_at_value, datetime):
                normalized_value = (
                    raw_at_value.replace(tzinfo=UTC) if raw_at_value.tzinfo is None else raw_at_value.astimezone(UTC)
                )
                payload[at_key] = monotonic_datetime(normalized_value.isoformat().replace('+00:00', 'Z'))
            else:
                payload[at_key] = monotonic_datetime(str(raw_at_value))

        return payload

    @model_serializer(mode='plain', when_used='json')
    def _serialize_event_result_json(self) -> dict[str, Any]:
        handler = self.handler
        return {
            'id': self.id,
            'status': self.status,
            'event_id': self.event_id,
            'handler_id': self.handler_id,
            'handler_name': self.handler_name,
            'handler_file_path': handler.handler_file_path,
            'handler_timeout': handler.handler_timeout,
            'handler_slow_timeout': handler.handler_slow_timeout,
            'handler_registered_at': monotonic_datetime(handler.handler_registered_at),
            'handler_event_pattern': handler.event_pattern,
            'eventbus_id': self.eventbus_id,
            'eventbus_name': self.eventbus_name,
            'started_at': self._serialize_datetime_json(self.started_at),
            'completed_at': self._serialize_datetime_json(self.completed_at),
            'result': self._serialize_jsonable(self.result),
            'error': self._serialize_error_json(self.error),
            'event_children': [child.event_id for child in self.event_children],
        }

    @computed_field(return_type=str)
    @property
    def handler_id(self) -> str:
        return self.handler.id

    @computed_field(return_type=str)
    @property
    def handler_name(self) -> str:
        return self.handler.handler_name

    @computed_field(return_type=str)
    @property
    def eventbus_id(self) -> str:
        return self.handler.eventbus_id

    @computed_field(return_type=str)
    @property
    def eventbus_name(self) -> str:
        return self.handler.eventbus_name

    @property
    def eventbus_label(self) -> str:
        return self.handler.eventbus_label

    @property
    def handler_completed_signal(self) -> asyncio.Event | None:
        """Lazily create asyncio.Event when accessed."""
        if self._handler_completed_signal is None:
            try:
                asyncio.get_running_loop()
                self._handler_completed_signal = asyncio.Event()
            except RuntimeError:
                pass
        return self._handler_completed_signal

    def __str__(self) -> str:
        """Constant-time summary for hot-path logging."""
        handler_qualname = f'{self.eventbus_label}.{self.handler_name}'
        if self.status == 'pending':
            outcome = 'pending'
        elif self.status == 'started':
            outcome = 'started'
        elif self.error is not None:
            outcome = f'error:{type(self.error).__name__}'
        elif self.result is None:
            outcome = 'result:none'
        elif isinstance(self.result, BaseEvent):
            outcome = 'event'
        else:
            outcome = f'result:{type(self.result).__name__}'
        return f'{handler_qualname}() -> {outcome} ({self.status})'

    def __repr__(self) -> str:
        icon = '🏃' if self.status == 'pending' else '✅' if self.status == 'completed' else '❌'
        return f'{self.handler.label}() {icon}'

    def __await__(self) -> Generator[Self, Any, T_EventResultType | 'BaseEvent[Any]' | None]:
        """
        Wait for this result to complete and return the result or raise error.
        Does not execute the handler itself, only waits for completion.
        """

        async def wait_for_handler_to_complete_and_return_result() -> T_EventResultType | 'BaseEvent[Any]' | None:
            assert self.handler_completed_signal is not None, 'EventResult cannot be awaited outside of an async context'
            try:
                await asyncio.wait_for(self.handler_completed_signal.wait(), timeout=self.timeout)
            except TimeoutError:
                raise TimeoutError(
                    f'Event handler {self.eventbus_label}.{self.handler_name}(#{self.event_id[-4:]}) timed out after {self.timeout}s'
                )

            if self.status == 'error' and self.error is not None:
                raise self.error
            return self.result

        return wait_for_handler_to_complete_and_return_result().__await__()

    def update(self, **kwargs: Any) -> Self:
        """Update the EventResult with provided kwargs, called by EventBus during handler execution."""

        # Common mistake: returning an exception object instead of setting error.
        if 'result' in kwargs and isinstance(kwargs['result'], BaseException):
            logger.warning(
                f'ℹ Event handler {self.handler_name} returned an exception object, auto-converting to EventResult(result=None, status="error", error={kwargs["result"]})'
            )
            kwargs['error'] = kwargs['result']
            kwargs['status'] = 'error'
            kwargs['result'] = None

        if 'result' in kwargs:
            result: Any = kwargs['result']
            self.status = 'completed'
            if self.result_type is not None and result is not None:
                if isinstance(result, BaseEvent):
                    self.result = result
                else:
                    try:
                        validated_result = validate_result_against_type(self.result_type, result)
                        self.result = validated_result
                    except Exception as cast_error:
                        schema_id = result_type_identifier_from_schema(self.result_type) or 'unknown'
                        self.error = EventHandlerResultSchemaError(
                            f'Event handler returned a value that did not match expected event_result_type '
                            f'({schema_id}): {result} -> {type(cast_error).__name__}: {cast_error}'
                        )
                        self.result = None
                        self.status = 'error'
            else:
                self.result = result

        if 'error' in kwargs:
            assert isinstance(kwargs['error'], (BaseException, str)), (
                f'Invalid error type: {type(kwargs["error"]).__name__} {kwargs["error"]}'
            )
            self.error = kwargs['error'] if isinstance(kwargs['error'], BaseException) else Exception(kwargs['error'])
            self.status = 'error'

        if 'status' in kwargs:
            assert kwargs['status'] in ('pending', 'started', 'completed', 'error'), f'Invalid status: {kwargs["status"]}'
            self.status = kwargs['status']

        if self.status != 'pending' and not self.started_at:
            self.started_at = monotonic_datetime()
        if self.status in ('completed', 'error') and not self.completed_at:
            self.completed_at = monotonic_datetime()
            if self.handler_completed_signal:
                self.handler_completed_signal.set()
        return self

    def _create_slow_handler_warning_timer(
        self,
        event: 'BaseEvent[T_EventResultType]',
        eventbus: 'EventBus',
        handler_slow_timeout: float | None,
    ) -> Callable[[], Coroutine[Any, Any, None]] | None:
        should_warn_for_slow_handler = handler_slow_timeout is not None and (
            self.timeout is None or self.timeout > handler_slow_timeout
        )
        if not should_warn_for_slow_handler:
            return None
        return partial(self._slow_handler_monitor, event=event, eventbus=eventbus, handler_slow_timeout=handler_slow_timeout)

    async def _slow_handler_monitor(
        self,
        *,
        event: 'BaseEvent[T_EventResultType]',
        eventbus: 'EventBus',
        handler_slow_timeout: float | None,
    ) -> None:
        assert handler_slow_timeout is not None
        await asyncio.sleep(handler_slow_timeout)
        if self.status != 'started':
            return
        started_at = self.started_at or event.event_started_at or event.event_created_at
        started_at_dt = datetime.fromisoformat(started_at)
        elapsed_seconds = max(0.0, (datetime.now(UTC) - started_at_dt).total_seconds())
        logger.warning(
            '⚠️ Slow event handler: %s.on(%s#%s, %s) still running after %.1fs',
            eventbus.label,
            event.event_type,
            event.event_id[-4:],
            self.handler.label,
            elapsed_seconds,
        )

    async def _call_handler(
        self,
        event: 'BaseEvent[T_EventResultType]',
        handler: NormalizedEventHandlerCallable[T_EventResultType],
        dispatch_context: contextvars.Context | None,
    ) -> T_EventResultType | 'BaseEvent[Any]' | None:
        handler_task: asyncio.Task[Any] | None = None
        handler_return_value: T_EventResultType | BaseEvent[Any] | None = None
        try:
            if dispatch_context is None:
                handler_return_value = await handler(event)
            else:
                merged_context = dispatch_context.copy()
                for variable, value in contextvars.copy_context().items():
                    merged_context.run(variable.set, value)
                handler_task = asyncio.create_task(
                    handler(event),
                    context=merged_context,
                )
                handler_return_value = await handler_task
            return handler_return_value
        finally:
            await cancel_and_await(handler_task, timeout=0.1)

    @asynccontextmanager
    async def _run_with_timeout(self, event: 'BaseEvent[T_EventResultType]') -> AsyncIterator[None]:
        """Apply handler timeout and normalize timeout expiry to EventHandlerTimeoutError."""
        timeout_scope = asyncio.timeout(self.timeout)
        try:
            async with timeout_scope:
                yield
        except TimeoutError as exc:
            if not timeout_scope.expired():
                raise
            timeout_error = self._on_handler_timeout(event)
            raise timeout_error from exc

    def _on_handler_timeout(self, event: 'BaseEvent[T_EventResultType]') -> EventHandlerTimeoutError:
        children = f' and interrupted any processing of {len(event.event_children)} child events' if event.event_children else ''
        timeout_error = EventHandlerTimeoutError(
            f'Event handler {self.handler.label}({event}) timed out after {self.timeout}s{children}'
        )
        self.update(error=timeout_error)
        event._cancel_pending_child_processing(timeout_error)  # pyright: ignore[reportPrivateUsage]

        from abxbus.logging import log_timeout_tree

        log_timeout_tree(event, self)
        return timeout_error

    def _on_handler_error(
        self,
        event: 'BaseEvent[T_EventResultType]',
        eventbus: 'EventBus',
        exc: Exception,
        *,
        format_exception_for_log: Callable[[BaseException], str],
    ) -> None:
        normalized_error: Exception = exc
        if isinstance(exc, TimeoutError) and not isinstance(exc, EventHandlerTimeoutError):
            timeout_message = str(exc) or f'Event handler {self.handler.label}({event}) timed out'
            normalized_error = EventHandlerTimeoutError(timeout_message)

        self.update(error=normalized_error)
        red = '\033[91m'
        reset = '\033[0m'
        logger.error(
            f'❌ {eventbus} Error in event handler {self.handler_name}({event}) -> \n{red}{type(normalized_error).__name__}({normalized_error}){reset}\n{format_exception_for_log(normalized_error)}',
        )

    async def _on_handler_exit(self) -> None:
        return

    async def run_handler(
        self,
        event: 'BaseEvent[T_EventResultType]',
        *,
        eventbus: 'EventBus',
        timeout: float | None,
        handler_slow_timeout: float | None = None,
        notify_event_started: bool = False,
        format_exception_for_log: Callable[[BaseException], str] | None = None,
    ) -> T_EventResultType | 'BaseEvent[Any]' | None:
        """Execute one handler inside the unified runtime scope stack.

        Runtime layering for one handler execution:
        - per-event handler lock
        - handler execution context manager (ContextVars + dispatch lock mirror)
        - optional timeout wrapper (hard cap)
        - optional slow-handler background monitor
        - handler call + result/error normalization
        """
        _format_exception_for_log_callable = format_exception_for_log or _default_format_exception_for_log

        handler: NormalizedEventHandlerCallable[T_EventResultType] | None = self.handler._handler_async  # pyright: ignore[reportPrivateUsage]
        if handler is None:
            raise RuntimeError(f'EventResult {self.id} has no callable attached to handler {self.handler.id}')

        self.timeout = timeout
        self.result_type = event.event_result_type

        dispatch_context = event._get_dispatch_context()  # pyright: ignore[reportPrivateUsage]
        slow_handler_monitor: Callable[[], Coroutine[Any, Any, None]] | None = None
        try:
            async with eventbus.locks._run_with_handler_lock(eventbus, event, self):  # pyright: ignore[reportPrivateUsage]
                if self.status in ('error', 'completed'):
                    return self.result
                self.update(status='started')
                event._mark_started(self.started_at)  # pyright: ignore[reportPrivateUsage]
                await eventbus.on_event_result_change(event, self, EventStatus.STARTED)
                if notify_event_started:
                    await eventbus.on_event_change(event, EventStatus.STARTED)

                with eventbus._run_with_handler_dispatch_context(event, self.handler_id):  # pyright: ignore[reportPrivateUsage]
                    slow_handler_monitor = self._create_slow_handler_warning_timer(event, eventbus, handler_slow_timeout)
                    async with self._run_with_timeout(event):
                        async with _run_with_slow_monitor(
                            slow_handler_monitor,
                            task_name=f'{eventbus}.slow_handler_monitor({event}, {self.handler.label})',
                        ):
                            handler_return_value = await self._call_handler(
                                event,
                                handler,
                                dispatch_context,
                            )
                    self.update(result=handler_return_value)
                return self.result

        except asyncio.CancelledError:
            handler_interrupted_error = EventHandlerAbortedError(
                f'Event handler {self.handler.label}({event}) was interrupted because of a parent timeout'
            )
            self.update(error=handler_interrupted_error)
            raise

        except Exception as exc:
            self._on_handler_error(
                event,
                eventbus,
                exc,
                format_exception_for_log=_format_exception_for_log_callable,
            )
            raise
        finally:
            await self._on_handler_exit()

    def log_tree(
        self,
        indent: str = '',
        is_last: bool = True,
        event_children_by_parent: dict[str | None, list['BaseEvent[Any]']] | None = None,
    ) -> None:
        """Print this result and its child events with proper tree formatting."""
        from abxbus.logging import log_event_result_tree

        log_event_result_tree(self, indent, is_last, event_children_by_parent)


class BaseEvent(BaseModel, Generic[T_EventResultType]):
    """
    The base model used for all Events that flow through the EventBus system.
    """

    model_config = ConfigDict(
        extra='allow',
        arbitrary_types_allowed=True,
        # Allow ergonomic subclass defaults like `class MyEvent(BaseEvent): event_version = '1.2.3'`
        # without requiring repetitive type annotations on every override.
        ignored_types=(str,),
        # Runtime lifecycle updates mutate event fields very frequently; avoid
        # assignment-time model rebuilding and keep state updates stable/O(1).
        validate_assignment=False,
        validate_default=True,
        revalidate_instances='always',
    )

    # Class-level cache for auto-extracted event_result_type from BaseEvent[T]
    _event_result_type_cache: ClassVar[Any | None] = None

    event_type: PythonIdentifierStr = Field(default='UndefinedEvent', description='Event type name', max_length=64)
    event_version: str = Field(
        default=LIBRARY_VERSION,
        description='Event type version tag, defaults to LIBRARY_VERSION env var or "0.0.1" if not overridden',
    )
    event_timeout: float | None = Field(
        default=None, description='Timeout in seconds for event to finish processing (bus default applied at dispatch)'
    )
    event_slow_timeout: float | None = Field(
        default=None, description='Optional per-event slow processing warning threshold in seconds'
    )
    event_concurrency: EventConcurrencyMode | None = Field(
        default=None,
        description=(
            'Event scheduling strategy relative to other events: '
            "'global-serial' | 'bus-serial' | 'parallel'. "
            'None defers to the bus default.'
        ),
    )
    event_handler_timeout: float | None = Field(default=None, description='Optional per-event handler timeout cap in seconds')
    event_handler_slow_timeout: float | None = Field(
        default=None, description='Optional per-event slow handler warning threshold in seconds'
    )
    event_handler_concurrency: EventHandlerConcurrencyMode | None = Field(
        default=None,
        description=(
            "Handler scheduling strategy: 'serial' runs one handler at a time, 'parallel' runs handlers concurrently. "
            'None defers to the bus default.'
        ),
    )
    event_handler_completion: EventHandlerCompletionMode | None = Field(
        default=None,
        description=(
            "Handler completion strategy: 'all' waits for all handlers, 'first' resolves on first successful result. "
            'None defers to the bus default.'
        ),
    )
    event_blocks_parent_completion: bool = Field(
        default=False,
        description=(
            'Whether this event must finish before the emitting parent event can complete. '
            'Set False for background child events that should retain ancestry without blocking the parent phase.'
        ),
    )
    event_result_type: Any = Field(
        default=None, description='Schema/type for handler result validation (serialized as JSON Schema)'
    )

    @field_validator('event_result_type', mode='before')
    @classmethod
    def _deserialize_event_result_type(cls, value: Any) -> Any:
        return pydantic_model_from_json_schema(value)

    @field_serializer('event_result_type', when_used='json')
    def event_result_type_serializer(self, value: Any) -> dict[str, Any] | None:
        """Serialize event_result_type to JSON Schema for cross-language transport."""
        return pydantic_model_to_json_schema(value)

    # Runtime metadata
    event_id: UUIDStr = Field(default_factory=uuid7str, max_length=36)
    event_path: list[EventPathEntryStr] = Field(default_factory=list, description='Path tracking for event routing')
    event_parent_id: UUIDStr | None = Field(
        default=None, description='ID of the parent event that triggered this event', max_length=36
    )
    event_emitted_by_handler_id: str | None = Field(
        default=None,
        description='Handler id that emitted this event when dispatched from inside a handler',
    )
    event_pending_bus_count: int = Field(
        default=0,
        description='Number of buses that currently have this event pending/in-flight',
    )

    # Completion tracking fields
    event_created_at: str = Field(
        default_factory=monotonic_datetime,
        description='Timestamp when event was first dispatched to an EventBus aka marked pending',
    )
    event_status: EventStatus = Field(
        default=EventStatus.PENDING,
        description='Current event lifecycle status: pending, started, or completed',
    )
    event_started_at: str | None = Field(
        default=None,
        description='Timestamp when event processing first started',
    )
    event_completed_at: str | None = Field(
        default=None,
        description='Timestamp when event was completed by all handlers and child events',
    )

    event_results: dict[PythonIdStr, EventResult[T_EventResultType]] = Field(
        default_factory=dict, exclude=True
    )  # Results indexed by str(id(handler_func))

    # Completion signal
    _event_completed_signal: asyncio.Event | None = PrivateAttr(default=None)
    _event_is_complete_flag: bool = PrivateAttr(default=False)
    _lock_for_event_handler: 'ReentrantLock | None' = PrivateAttr(default=None)

    # Dispatch-time context for ContextVar propagation to handlers
    # Captured when emit() is called, used when executing handlers via ctx.run()
    _event_dispatch_context: contextvars.Context | None = PrivateAttr(default=None)

    def __hash__(self) -> int:
        """Make events hashable using their unique event_id"""
        return hash(self.event_id)

    def __str__(self) -> str:
        """Compact O(1) summary for hot-path logging."""
        completed_signal = self._event_completed_signal
        if (
            self.event_status == EventStatus.COMPLETED
            or self._event_is_complete_flag
            or (completed_signal is not None and completed_signal.is_set())
        ):
            icon = '✅'
        elif self.event_status == EventStatus.STARTED:
            icon = '🏃'
        else:
            icon = '⏳'

        bus_hint = self.event_path[-1] if self.event_path else '?'
        return f'{bus_hint}▶ {self.event_type}#{self.event_id[-4:]} {icon}'

    def _remove_self_from_queue(self, bus: 'EventBus') -> bool:
        """Remove this event from the bus's queue if present. Returns True if removed."""
        return bus.remove_event_from_pending_queue(self)

    @staticmethod
    def _iter_eventbuses_for_registry(registry_owner: 'EventBus | None' = None) -> list['EventBus']:
        from abxbus.event_bus import EventBus, get_current_eventbus

        if registry_owner is not None:
            return list(type(registry_owner).all_instances)
        current_bus = get_current_eventbus()
        if current_bus is not None:
            return list(type(current_bus).all_instances)
        return list(EventBus.iter_all_instances())

    def _is_queued_on_any_bus(self, ignore_bus: 'EventBus | None' = None) -> bool:
        """
        Check whether this event is currently queued on any live EventBus.

        This prevents premature completion when an event has been forwarded to
        another bus but that bus hasn't processed it yet.
        """
        for bus in self._iter_eventbuses_for_registry(ignore_bus):
            if not bus:
                continue
            if ignore_bus is not None and bus is ignore_bus:
                continue
            if bus.is_event_inflight_or_queued(self.event_id):
                return True
        return False

    async def _process_self_on_all_buses(self) -> None:
        """
        Process this specific event on all buses where it's queued.

        This handles the case where an event is forwarded to multiple buses -
        we need to process it on each bus, but we only process THIS event,
        not other events in the queues (to avoid overshoot).

        The loop continues until the event's completion signal is set, which
        happens after all handlers on all buses have completed.
        """
        max_iterations = 1000  # Prevent infinite loops
        iterations = 0

        # Cache the signal - in async context it will always be created
        completed_signal = self.event_completed_signal
        assert completed_signal is not None, 'event_completed_signal should exist in async context'
        claimed_processed_bus_ids: set[int] = set()
        try:
            while not completed_signal.is_set() and iterations < max_iterations:
                iterations += 1
                processed_any = False

                # Look for this specific event in all bus queues and process it
                for bus in self._iter_eventbuses_for_registry():
                    if not bus:
                        continue
                    processed_on_bus = False

                    if self._remove_self_from_queue(bus):
                        # Fast path: event is still in the queue, claim and process it via EventBus.step
                        # so completion/finalization uses the same logic as the runloop.
                        await bus.step(event=self)
                        bus.mark_pending_queue_task_done()
                        processed_on_bus = True
                    else:
                        # Slow path: another task already claimed queue.get() and set
                        # processing state, but may be blocked on an event-level lock held
                        # by the awaiting parent handler. Process once here to make progress.
                        bus_key = id(bus)
                        event_lock = bus.locks.get_lock_for_event(bus, self)
                        if (
                            event_lock is not None
                            and bus.is_event_processing(self.event_id)
                            and bus_key not in claimed_processed_bus_ids
                        ):
                            await bus.step(event=self)
                            claimed_processed_bus_ids.add(bus_key)
                            processed_on_bus = True

                    if processed_on_bus:
                        processed_any = True
                        if completed_signal.is_set():
                            break

                if completed_signal.is_set():
                    break

                if not processed_any:
                    # Event not in any queue, yield control and wait
                    await asyncio.sleep(0)

        except asyncio.CancelledError:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Polling loop cancelled for %s', self)
            raise

    async def _wait_for_completion_inside_handler(self) -> None:
        """
        Wait for this event to complete when called from inside a handler.

        Processes this specific event on all buses where it appears (handling
        the forwarding case), but doesn't process other events (avoiding overshoot).
        """
        await self._process_self_on_all_buses()

    def __await__(self) -> Generator[Self, Any, Any]:
        """Immediate await path (queue-jump when inside a handler), returns self."""

        async def wait_for_handlers_to_complete_then_return_event():
            self._mark_blocks_parent_completion_if_awaited_from_emitting_handler()
            if self._event_is_complete_flag:
                return self
            assert self.event_completed_signal is not None
            from abxbus.event_bus import in_handler_context

            is_inside_handler = in_handler_context()
            is_not_yet_complete = not self._event_is_complete_flag and not self.event_completed_signal.is_set()

            if is_not_yet_complete and is_inside_handler:
                await self._wait_for_completion_inside_handler()
            else:
                await self.event_completed()

            return self

        return wait_for_handlers_to_complete_then_return_event().__await__()

    async def event_completed(self) -> Self:
        """Queue-order await path (never queue-jumps), returns self."""
        self._mark_blocks_parent_completion_if_awaited_from_emitting_handler()
        if self._event_is_complete_flag:
            return self
        assert self.event_completed_signal is not None
        await self.event_completed_signal.wait()
        return self

    async def first(
        self,
        timeout: float | None = None,
        *,
        raise_if_any: bool = False,
        raise_if_none: bool = False,
    ) -> T_EventResultType | None:
        """
        Resolve with the first successful non-None handler result for this event.

        This switches the event to ``event_handler_completion='first'`` before awaiting completion.
        """
        self.event_handler_completion = EventHandlerCompletionMode.FIRST
        await self
        return await self.event_result(timeout=timeout, raise_if_any=raise_if_any, raise_if_none=raise_if_none)

    @model_validator(mode='before')
    @classmethod
    def _normalize_and_validate_event_input(cls, data: Any) -> Any:
        """Normalize event input once, enforce reserved namespaces, and apply built-in defaults."""
        if not isinstance(data, dict):
            return data
        params = _normalize_any_dict(data)

        for key in RESERVED_USER_EVENT_FIELDS:
            if key in params:
                raise ValueError(f'Field "{key}" is reserved for BaseEvent runtime APIs and cannot be set in event payload')

        for key in params:
            if key.startswith('event_') and key not in BaseEvent.model_fields:
                raise ValueError(f'Field "{key}" starts with "event_" but is not a recognized BaseEvent field')
            if key.startswith('model_'):
                raise ValueError(f'Field "{key}" starts with "model_" and is reserved for Pydantic model internals')

        for at_key in ('event_created_at', 'event_started_at', 'event_completed_at'):
            if at_key not in params:
                continue
            raw_value = params[at_key]
            if raw_value is None:
                continue
            if isinstance(raw_value, datetime):
                normalized_value = raw_value.replace(tzinfo=UTC) if raw_value.tzinfo is None else raw_value.astimezone(UTC)
                params[at_key] = monotonic_datetime(normalized_value.isoformat().replace('+00:00', 'Z'))
            else:
                params[at_key] = monotonic_datetime(str(raw_value))

        is_class_default_unchanged = cls.model_fields['event_type'].default == 'UndefinedEvent'
        is_event_type_not_provided = 'event_type' not in params or params['event_type'] == 'UndefinedEvent'
        if is_class_default_unchanged and is_event_type_not_provided:
            params['event_type'] = cls.__name__

        if 'event_result_type' not in params:
            if 'event_result_type' in cls.model_fields:
                field = cls.model_fields['event_result_type']
                if field.default is not None and field.default != BaseEvent.model_fields['event_result_type'].default:
                    params['event_result_type'] = field.default
                    return params

            if cls._event_result_type_cache is not None:
                params['event_result_type'] = cls._event_result_type_cache
                return params

            extracted_type = extract_basemodel_generic_arg(cls)
            cls._event_result_type_cache = extracted_type
            if extracted_type is not None:
                params['event_result_type'] = extracted_type

        return params

    @model_validator(mode='after')
    def _hydrate_event_result_types_from_event(self) -> Self:
        """Rehydrate per-handler result_type from the event-level event_result_type."""
        if self.event_results:
            first_result = next(iter(self.event_results.values()))
            if first_result.result_type != self.event_result_type:
                for event_result in self.event_results.values():
                    event_result.result_type = self.event_result_type
        return self

    @property
    def event_completed_signal(self) -> asyncio.Event | None:
        """Lazily create asyncio.Event when accessed"""
        if self._event_completed_signal is None:
            try:
                asyncio.get_running_loop()
                self._event_completed_signal = asyncio.Event()
            except RuntimeError:
                pass  # Keep it None if no event loop
        return self._event_completed_signal

    @property
    def event_children(self) -> list['BaseEvent[Any]']:
        """Get all child events dispatched from within this event's handlers"""
        children: list[BaseEvent[Any]] = []
        for event_result in self.event_results.values():
            children.extend(event_result.event_children)
        return children

    def _mark_started(self, started_at: str | datetime | None = None) -> None:
        """Mark event runtime state as started, preserving the earliest start timestamp."""
        if self.event_status == EventStatus.COMPLETED:
            return

        if isinstance(started_at, datetime):
            normalized_value = started_at.replace(tzinfo=UTC) if started_at.tzinfo is None else started_at.astimezone(UTC)
            resolved_started_at = monotonic_datetime(normalized_value.isoformat().replace('+00:00', 'Z'))
        elif started_at is None:
            resolved_started_at = monotonic_datetime()
        else:
            resolved_started_at = monotonic_datetime(started_at)
        if self.event_started_at is None or resolved_started_at < self.event_started_at:
            self.event_started_at = resolved_started_at
        if self.event_status == EventStatus.PENDING:
            self.event_status = EventStatus.STARTED
        self.event_completed_at = None
        self._event_is_complete_flag = False

    def _create_pending_handler_results(
        self,
        handlers: dict[PythonIdStr, EventHandler | ContravariantEventHandlerCallable[Any]],
        *,
        eventbus: 'EventBus | None' = None,
        timeout: float | None = None,
    ) -> 'dict[PythonIdStr, EventResult[T_EventResultType]]':
        """Ensure EventResult placeholders exist for provided handlers before execution.

        Any stale timing/error data from prior runs is cleared so consumers immediately see a fresh pending state.
        """
        pending_results: dict[PythonIdStr, EventResult[T_EventResultType]] = {}
        self._event_is_complete_flag = False
        self.event_completed_at = None
        if self.event_status == EventStatus.COMPLETED:
            self.event_status = EventStatus.PENDING
            self.event_started_at = None
        for handler_id, handler in handlers.items():
            event_result = self.event_result_update(
                handler=handler,
                eventbus=eventbus,
                status='pending',
            )
            # Reset runtime fields so we never reuse stale data
            event_result.result = None
            event_result.error = None
            event_result.started_at = None
            event_result.completed_at = None
            event_result.status = 'pending'
            event_result.timeout = timeout if timeout is not None else self.event_timeout
            event_result.result_type = self.event_result_type
            pending_results[handler_id] = event_result
        return pending_results

    @staticmethod
    def _is_first_mode_winning_result(event_result: 'EventResult[Any]') -> bool:
        return (
            event_result.status == 'completed'
            and event_result.error is None
            and event_result.result is not None
            and not isinstance(event_result.result, BaseEvent)
        )

    async def _mark_remaining_first_mode_result_cancelled(
        self,
        event_result: 'EventResult[Any]',
        *,
        eventbus: 'EventBus',
    ) -> None:
        if event_result.status in ('completed', 'error'):
            return
        event_result.update(error=EventHandlerCancelledError('Cancelled: first() resolved'))
        await eventbus.on_event_result_change(self, event_result, EventStatus.COMPLETED)

    async def _run_handlers(
        self: 'BaseEvent[T_EventResultType]',
        *,
        eventbus: 'EventBus',
        handlers: dict[PythonIdStr, EventHandler] | None = None,
        timeout: float | None = None,
    ) -> None:
        """Run all handlers for this event using the bus concurrency/completion configuration."""
        applicable_handlers = handlers if (handlers is not None) else eventbus._get_handlers_for_event(self)  # pyright: ignore[reportPrivateUsage]
        if not applicable_handlers:
            return

        pending_handler_map: dict[PythonIdStr, EventHandler | ContravariantEventHandlerCallable[Any]] = dict(applicable_handlers)
        pending_results = self._create_pending_handler_results(
            pending_handler_map,
            eventbus=eventbus,
            timeout=timeout if timeout is not None else self.event_timeout,
        )
        eventbus._resolve_find_waiters(self)  # pyright: ignore[reportPrivateUsage]
        if eventbus.middlewares:
            for pending_result in pending_results.values():
                await eventbus.on_event_result_change(self, pending_result, EventStatus.PENDING)

        completion_mode = self.event_handler_completion or eventbus.event_handler_completion
        concurrency_mode = self.event_handler_concurrency or eventbus.event_handler_concurrency
        handler_items = list(applicable_handlers.items())

        if concurrency_mode == EventHandlerConcurrencyMode.PARALLEL:
            if completion_mode == EventHandlerCompletionMode.FIRST:
                handler_tasks: dict[asyncio.Task[Any], PythonIdStr] = {}
                local_handler_ids: set[PythonIdStr] = set(applicable_handlers.keys())
                for handler_id, handler_entry in applicable_handlers.items():
                    handler_tasks[asyncio.create_task(eventbus._run_handler(self, handler_entry, timeout=timeout))] = handler_id  # pyright: ignore[reportPrivateUsage]

                pending_tasks: set[asyncio.Task[Any]] = set(handler_tasks.keys())
                winner_handler_id: PythonIdStr | None = None

                try:
                    while pending_tasks:
                        done_tasks, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
                        for done_task in done_tasks:
                            try:
                                await done_task
                            except Exception:
                                pass

                            done_handler_id = handler_tasks[done_task]
                            completed_result = self.event_results.get(done_handler_id)
                            if completed_result is not None and self._is_first_mode_winning_result(completed_result):
                                winner_handler_id = done_handler_id
                                break

                        if winner_handler_id is not None:
                            break

                    if winner_handler_id is not None:
                        for pending_task in pending_tasks:
                            pending_task.cancel()
                        if pending_tasks:
                            await asyncio.gather(*pending_tasks, return_exceptions=True)

                        for handler_id, event_result in self.event_results.items():
                            if handler_id not in local_handler_ids or handler_id == winner_handler_id:
                                continue
                            await self._mark_remaining_first_mode_result_cancelled(event_result, eventbus=eventbus)
                    elif pending_tasks:
                        await asyncio.gather(*pending_tasks, return_exceptions=True)
                except asyncio.CancelledError:
                    for pending_task in pending_tasks:
                        pending_task.cancel()
                    if pending_tasks:
                        await asyncio.gather(*pending_tasks, return_exceptions=True)
                    raise
                return

            parallel_tasks = [
                asyncio.create_task(eventbus._run_handler(self, handler_entry, timeout=timeout))  # pyright: ignore[reportPrivateUsage]
                for _, handler_entry in handler_items
            ]
            try:
                for task in parallel_tasks:
                    try:
                        await task
                    except Exception:
                        pass
            except asyncio.CancelledError:
                for task in parallel_tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*parallel_tasks, return_exceptions=True)
                raise
            return

        for index, (handler_id, handler_entry) in enumerate(handler_items):
            try:
                await eventbus._run_handler(self, handler_entry, timeout=timeout)  # pyright: ignore[reportPrivateUsage]
            except Exception as e:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        '❌ %s Handler %s#%s(%s) failed with %s: %s',
                        eventbus,
                        handler_entry.handler_name,
                        handler_entry.id[-4:],
                        self,
                        type(e).__name__,
                        e,
                    )

            if completion_mode != EventHandlerCompletionMode.FIRST:
                continue

            completed_result = self.event_results.get(handler_id)
            if completed_result is None or not self._is_first_mode_winning_result(completed_result):
                continue

            for remaining_handler_id, _ in handler_items[index + 1 :]:
                remaining_result = self.event_results.get(remaining_handler_id)
                if remaining_result is None:
                    continue
                await self._mark_remaining_first_mode_result_cancelled(remaining_result, eventbus=eventbus)
            break

    @staticmethod
    def _event_result_is_truthy(event_result: 'EventResult[T_EventResultType]') -> bool:
        # omit BaseEvent results, they are forwarded event refs not actual return values
        return (
            event_result.status == 'completed'
            and event_result.result is not None
            and not isinstance(event_result.result, BaseException)
            and not event_result.error
            and not isinstance(event_result.result, BaseEvent)
        )

    def _collect_handler_errors(self, include_cancelled: bool) -> list[Exception]:
        """Collect handler errors as Exception instances for aggregation."""
        collected_errors: list[Exception] = []
        for event_result in self.event_results.values():
            original_error = event_result.error
            if original_error is None and isinstance(event_result.result, BaseException):
                original_error = event_result.result

            if original_error is None:
                continue

            if isinstance(original_error, asyncio.CancelledError) and not include_cancelled:
                continue

            if isinstance(original_error, Exception):
                collected_errors.append(original_error)
                continue

            wrapped = RuntimeError(
                f'Non-Exception handler error from {event_result.eventbus_label}.{event_result.handler_name}: '
                f'{type(original_error).__name__}: {original_error}'
            )
            wrapped.__cause__ = original_error
            collected_errors.append(wrapped)
        return collected_errors

    async def event_result(
        self,
        timeout: float | None = None,
        include: EventResultFilter = _event_result_is_truthy,
        raise_if_any: bool = True,
        raise_if_none: bool = True,
    ) -> T_EventResultType | None:
        """Get the first non-None result from the event handlers"""
        self._mark_blocks_parent_completion_if_awaited_from_emitting_handler()
        if not self._event_is_complete_flag:
            completed_signal = self._event_completed_signal
            if completed_signal is None:
                completed_signal = self.event_completed_signal
            assert completed_signal is not None, 'EventResult cannot be awaited outside of an async context'
            await asyncio.wait_for(completed_signal.wait(), timeout=timeout or self.event_timeout)

        for event_result in self.event_results.values():
            try:
                await event_result
            except (Exception, asyncio.CancelledError):
                pass

        valid_results: dict[PythonIdStr, EventResult[T_EventResultType]] = {
            handler_key: event_result for handler_key, event_result in self.event_results.items() if include(event_result)
        }
        error_results = [
            event_result
            for event_result in self.event_results.values()
            if event_result.error or isinstance(event_result.result, BaseException)
        ]

        if raise_if_any and error_results:
            if len(error_results) == 1:
                single_result = error_results[0]
                single_error = single_result.error if single_result.error is not None else single_result.result
                if isinstance(single_error, BaseException):
                    raise single_error
                raise Exception(str(single_error))

            collected_errors = self._collect_handler_errors(include_cancelled=True)
            raise ExceptionGroup(
                f'Event {self.event_type}#{self.event_id[-4:]} had {len(collected_errors)} handler error(s)',
                collected_errors,
            )

        if raise_if_none and not valid_results:
            raise ValueError(
                f'Expected at least one handler to return a non-None result, but none did! {self} -> {self.event_results}'
            )

        for event_result in valid_results.values():
            assert event_result.result is not None, f'EventResult {event_result} has no result'

        results = list(valid_results.values())
        if not results:
            return None
        return self._coerce_typed_result_value(results[0].result)

    async def event_results_list(
        self,
        timeout: float | None = None,
        include: EventResultFilter = _event_result_is_truthy,
        raise_if_any: bool = True,
        raise_if_none: bool = True,
    ) -> list[T_EventResultType | None]:
        """Get all result values in a list [handler1_result, handler2_result, ...]"""
        self._mark_blocks_parent_completion_if_awaited_from_emitting_handler()
        if not self._event_is_complete_flag:
            completed_signal = self._event_completed_signal
            if completed_signal is None:
                completed_signal = self.event_completed_signal
            assert completed_signal is not None, 'EventResult cannot be awaited outside of an async context'
            await asyncio.wait_for(completed_signal.wait(), timeout=timeout or self.event_timeout)

        for event_result in self.event_results.values():
            try:
                await event_result
            except (Exception, asyncio.CancelledError):
                pass

        valid_results: dict[PythonIdStr, EventResult[T_EventResultType]] = {
            handler_key: event_result for handler_key, event_result in self.event_results.items() if include(event_result)
        }
        error_results = [
            event_result
            for event_result in self.event_results.values()
            if event_result.error or isinstance(event_result.result, BaseException)
        ]

        if raise_if_any and error_results:
            if len(error_results) == 1:
                single_result = error_results[0]
                single_error = single_result.error if single_result.error is not None else single_result.result
                if isinstance(single_error, BaseException):
                    raise single_error
                raise Exception(str(single_error))

            collected_errors = self._collect_handler_errors(include_cancelled=True)
            raise ExceptionGroup(
                f'Event {self.event_type}#{self.event_id[-4:]} had {len(collected_errors)} handler error(s)',
                collected_errors,
            )

        if raise_if_none and not valid_results:
            raise ValueError(
                f'Expected at least one handler to return a non-None result, but none did! {self} -> {self.event_results}'
            )

        for event_result in valid_results.values():
            assert event_result.result is not None, f'EventResult {event_result} has no result'

        results_list: list[T_EventResultType | None] = []
        for event_result in valid_results.values():
            results_list.append(self._coerce_typed_result_value(event_result.result))
        return results_list

    def event_result_update(
        self,
        handler: EventHandler | ContravariantEventHandlerCallable[Any],
        eventbus: 'EventBus | None' = None,
        **kwargs: Any,
    ) -> 'EventResult[T_EventResultType]':
        """Create or update an EventResult for a handler"""

        from abxbus.event_bus import EventBus

        assert eventbus is None or isinstance(eventbus, EventBus)
        if (
            eventbus is None
            and not isinstance(handler, EventHandler)
            and inspect.ismethod(handler)
            and isinstance(handler.__self__, EventBus)
        ):
            eventbus = handler.__self__

        if isinstance(handler, EventHandler):
            handler_entry = handler
            if eventbus is None and handler_entry.eventbus_id != '00000000-0000-0000-0000-000000000000':
                for bus in self._iter_eventbuses_for_registry(eventbus):
                    if bus and bus.id == handler_entry.eventbus_id:
                        eventbus = bus
                        break
                if (
                    eventbus is None
                    and handler_entry.eventbus_id
                    and handler_entry.eventbus_id != '00000000-0000-0000-0000-000000000000'
                ):
                    expected_label = handler_entry.eventbus_label
                    for bus in self._iter_eventbuses_for_registry(eventbus):
                        if bus and bus.label == expected_label:
                            eventbus = bus
                            break
        else:
            handler_entry = EventHandler.from_callable(
                handler=handler,
                event_pattern=self.event_type,
                eventbus_name=str(eventbus.name if eventbus is not None else 'EventBus'),
                eventbus_id=str(eventbus.id if eventbus is not None else '00000000-0000-0000-0000-000000000000'),
            )

        handler_id: PythonIdStr = handler_entry.id

        # Get or create EventResult
        if handler_id not in self.event_results:
            self.event_results[handler_id] = EventResult[T_EventResultType](
                event_id=self.event_id,
                handler=handler_entry,
                status=kwargs.get('status', 'pending'),
                timeout=self.event_timeout,
                result_type=self.event_result_type,
            )
            # logger.debug(f'Created EventResult for handler {handler_id}: {handler and EventHandler._get_callable_handler_name(handler)}')

        # Update the EventResult with provided kwargs
        existing_result = self.event_results[handler_id]
        if existing_result.handler.id != handler_entry.id:
            existing_result.handler = handler_entry

        existing_result.update(**kwargs)
        if existing_result.status == 'started' and existing_result.started_at is not None:
            self._mark_started(existing_result.started_at)
        if 'timeout' in kwargs:
            existing_result.timeout = kwargs['timeout']
        if kwargs.get('status') in ('pending', 'started'):
            self.event_completed_at = None
        # logger.debug(
        #     f'Updated EventResult for handler {handler_id}: status={self.event_results[handler_id].status}, total_results={len(self.event_results)}'
        # )
        # Don't mark complete here - let the EventBus do it after all handlers are done
        return existing_result

    @staticmethod
    def _coerce_typed_result_value(
        value: Any,
    ) -> T_EventResultType | None:
        # Handlers may return BaseEvent instances when forwarding via
        # bus.emit. Typed accessors expose only typed payload values,
        # so forwarded events are normalized to None.
        if isinstance(value, BaseEvent):
            return None
        return value

    def _mark_completed(self, current_bus: 'EventBus | None' = None) -> None:
        """Check if all handlers are done and signal completion"""
        completed_signal = self._event_completed_signal
        if completed_signal is not None and completed_signal.is_set():
            self._event_is_complete_flag = True
            if self.event_completed_at is None:
                self.event_completed_at = monotonic_datetime()
            if self.event_started_at is None:
                self.event_started_at = self.event_completed_at
            self.event_status = EventStatus.COMPLETED
            return

        # If there are no results at all, the event is complete.
        if not self.event_results:
            # Even with no local handlers, forwarded copies may still be queued elsewhere.
            if self._is_queued_on_any_bus(ignore_bus=current_bus):
                return
            if not self._are_all_children_complete():
                return
            self.event_completed_at = self.event_completed_at or monotonic_datetime()
            if self.event_started_at is None:
                self.event_started_at = self.event_completed_at
            self._event_is_complete_flag = True
            self.event_status = EventStatus.COMPLETED
            if completed_signal is not None:
                completed_signal.set()
            self._event_dispatch_context = None
            return

        # Check if all handler results are done.
        for result in self.event_results.values():
            if result.status not in ('completed', 'error'):
                return

        # Forwarded events may still be waiting in another bus queue.
        # Don't mark complete until all queue copies have been consumed.
        if self._is_queued_on_any_bus(ignore_bus=current_bus):
            return

        # Recursively check if all child events are also complete
        if not self._are_all_children_complete():
            return

        # All handlers and all child events are done.
        latest_completed: str | None = None
        for result in self.event_results.values():
            completed_at = result.completed_at
            if completed_at is None:
                continue
            if latest_completed is None or completed_at > latest_completed:
                latest_completed = completed_at
        self.event_completed_at = latest_completed or self.event_completed_at or monotonic_datetime()
        if self.event_started_at is None:
            self.event_started_at = self.event_completed_at
        self._event_is_complete_flag = True
        self.event_status = EventStatus.COMPLETED
        if completed_signal is not None:
            completed_signal.set()
        # Clear dispatch context to avoid memory leaks (it holds references to ContextVars)
        self._event_dispatch_context = None

    def _mark_pending(self) -> Self:
        """Reset mutable runtime state so this event can be dispatched again as pending."""
        self.event_status = EventStatus.PENDING
        self.event_started_at = None
        self._event_is_complete_flag = False
        self.event_completed_at = None
        self.event_results.clear()
        self._lock_for_event_handler = None
        self._event_dispatch_context = None
        try:
            asyncio.get_running_loop()
            self._event_completed_signal = asyncio.Event()
        except RuntimeError:
            self._event_completed_signal = None
        return self

    def event_reset(self) -> Self:
        """Return a fresh copy of this event with pending runtime state."""
        fresh_event = self.__class__.model_validate(self.model_dump(mode='python'))
        fresh_event.event_id = uuid7str()
        return fresh_event._mark_pending()

    def _get_handler_lock(self) -> 'ReentrantLock | None':
        return self._lock_for_event_handler

    def _set_handler_lock(self, lock: 'ReentrantLock | None') -> None:
        self._lock_for_event_handler = lock

    def _get_dispatch_context(self) -> contextvars.Context | None:
        return self._event_dispatch_context

    def _set_dispatch_context(self, dispatch_context: contextvars.Context | None) -> None:
        self._event_dispatch_context = dispatch_context

    def _mark_blocks_parent_completion_if_awaited_from_emitting_handler(self) -> None:
        """Upgrade same-handler awaited children into blocking children when they are explicitly linked."""
        if self.event_blocks_parent_completion:
            return
        from abxbus.event_bus import get_current_event, get_current_handler_id

        current_event = get_current_event()
        current_handler_id = get_current_handler_id()
        if current_event is None or current_handler_id is None:
            return
        if self.event_id == current_event.event_id:
            return
        if self.event_parent_id != current_event.event_id:
            return
        if self.event_emitted_by_handler_id != current_handler_id:
            return
        current_result = current_event.event_results.get(current_handler_id)
        if current_result is None:
            return
        if any(child.event_id == self.event_id for child in current_result.event_children):
            self.event_blocks_parent_completion = True

    def _are_all_children_complete(self, _visited: set[str] | None = None) -> bool:
        """Recursively check if all child events and their descendants are complete"""
        if _visited is None:
            _visited = set()

        # Prevent infinite recursion on circular references
        if self.event_id in _visited:
            return True
        _visited.add(self.event_id)

        for child_event in self.event_children:
            if not child_event.event_blocks_parent_completion:
                continue
            if child_event.event_status != 'completed':
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug('Event %s has incomplete child %s', self, child_event)
                return False
            # Recursively check child's children
            if not child_event._are_all_children_complete(_visited):
                return False
        return True

    def _cancel_pending_child_processing(self, error: BaseException) -> None:
        """Cancel any pending child events that were dispatched during handler execution"""
        if not isinstance(error, EventHandlerCancelledError):
            error = EventHandlerCancelledError(
                f'Cancelled pending handler as a result of parent error {error}'
            )  # keep the word "pending" in the error, checked by print_handler_line()
        for child_event in self.event_children:
            if not child_event.event_blocks_parent_completion:
                continue
            for result in child_event.event_results.values():
                if result.status == 'pending':
                    # print('CANCELLING CHILD HANDLER', result, 'due to', error)
                    result.update(error=error)
            child_event._cancel_pending_child_processing(error)

    def event_log_safe_summary(self) -> dict[str, Any]:
        """only event metadata without contents, avoid potentially sensitive event contents in logs"""
        return {k: v for k, v in self.model_dump(mode='json').items() if k.startswith('event_') and 'results' not in k}

    def event_log_tree(
        self,
        indent: str = '',
        is_last: bool = True,
        event_children_by_parent: 'dict[str | None, list[BaseEvent[Any]]] | None' = None,
    ) -> None:
        """Print this event and its results with proper tree formatting"""
        from abxbus.logging import log_event_tree

        log_event_tree(self, indent, is_last, event_children_by_parent)

    @property
    def bus(self) -> 'EventBus':
        """Get the EventBus that is currently processing this event"""
        from abxbus.event_bus import get_current_event, get_current_eventbus, in_handler_context

        if not in_handler_context():
            raise AttributeError('bus property can only be accessed from within an event handler')

        current_bus = get_current_eventbus()
        current_event = get_current_event()
        if current_bus is not None and current_event is not None and current_event.event_id == self.event_id:
            return current_bus

        # The event_path contains all buses this event has passed through
        # The last one in the path is the one currently processing
        if not self.event_path:
            raise RuntimeError('Event has no event_path - was it dispatched?')

        current_bus_label = self.event_path[-1]

        # Find the bus by label (BusName#abcd).
        # Create a list copy to avoid "Set changed size during iteration" error
        for bus in self._iter_eventbuses_for_registry(current_bus):
            if bus and bus.label == current_bus_label:
                return bus

        raise RuntimeError(f'Could not find active EventBus for path entry {current_bus_label}')

    @property
    def event_bus(self) -> 'EventBus':
        return self.bus

    def emit(self, event: T_EmittedEvent) -> T_EmittedEvent:
        """Emit an explicitly-owned child event that blocks this event's completion."""
        from abxbus.event_bus import get_current_event, get_current_handler_id

        event.event_blocks_parent_completion = True
        if event.event_parent_id is None and event.event_id != self.event_id:
            event.event_parent_id = self.event_id

        current_event = get_current_event()
        current_handler_id = get_current_handler_id()
        if (
            current_event is not None
            and current_handler_id is not None
            and current_event.event_id == self.event_id
            and event.event_id != self.event_id
            and current_handler_id in current_event.event_results
        ):
            if event.event_emitted_by_handler_id is None:
                event.event_emitted_by_handler_id = current_handler_id
            current_event.event_results[current_handler_id].event_children.append(event)

        return self.bus.emit(event)


def attr_name_allowed_on_event(key: str) -> bool:
    allowed_unprefixed_attrs = {'first', 'bus', 'emit'}
    return key in pydantic_builtin_attrs or key in event_builtin_attrs or key.startswith('_') or key in allowed_unprefixed_attrs


# PSA: All BaseEvent buil-in attrs and methods must be prefixed with "event_" in order to avoid clashing with data contents (which share a namespace with the metadata)
# This is the same approach Pydantic uses for their special `model_*` attrs (and BaseEvent is also a pydantic model, so model_ prefixes are reserved too)
# resist the urge to nest the event data in an inner object unless absolutely necessary, flat simplifies most of the code and makes it easier to read JSON logs with less nesting
pydantic_builtin_attrs = dir(BaseModel)
event_builtin_attrs = {key for key in dir(BaseEvent) if key.startswith('event_')}
illegal_attrs = {key for key in dir(BaseEvent) if not attr_name_allowed_on_event(key)}
assert not illegal_attrs, (
    'All BaseEvent attrs and methods must be prefixed with "event_" in order to avoid clashing '
    'with BaseEvent subclass fields used to store event contents (which share a namespace with the event_ metadata). '
    f'not allowed: {illegal_attrs}'
)

# Resolve forward refs after both core models are defined.
EventResult.model_rebuild()
BaseEvent.model_rebuild()
EventHandler.model_rebuild()
