import asyncio
import inspect
import os
from collections.abc import Awaitable, Callable, Coroutine
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias, cast, overload, runtime_checkable
from uuid import NAMESPACE_DNS, UUID, uuid5
from weakref import ref as weakref

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator, model_validator
from typing_extensions import TypeVar

from abxbus.helpers import monotonic_datetime

if TYPE_CHECKING:
    from abxbus.base_event import BaseEvent


# TypeVar for BaseEvent and its subclasses
# We use contravariant=True because if a handler accepts BaseEvent,
# it can also handle any subclass of BaseEvent
T_Event = TypeVar('T_Event', bound='BaseEvent[Any]', contravariant=True, default='BaseEvent[Any]')

# For protocols with __func__ attributes, we need an invariant TypeVar
T_EventInvariant = TypeVar('T_EventInvariant', bound='BaseEvent[Any]', default='BaseEvent[Any]')
T_EventResult = TypeVar('T_EventResult', default=Any)
T_HandlerEvent = TypeVar('T_HandlerEvent', bound='BaseEvent[Any]', default='BaseEvent[Any]')
T_HandlerReturn = TypeVar('T_HandlerReturn', default=Any)


class EventHandlerCancelledError(asyncio.CancelledError):
    """Handler was cancelled before starting or before producing a result."""


class EventHandlerTimeoutError(TimeoutError):
    """Handler exceeded its configured handler timeout."""


class EventHandlerAbortedError(asyncio.CancelledError):
    """Handler was interrupted while running (for example by event hard-timeout)."""


class EventHandlerResultSchemaError(ValueError):
    """Handler returned a value incompatible with the event_result_type schema."""


@runtime_checkable
class EventHandlerFunc(Protocol[T_Event]):
    """Protocol for sync event handler functions."""

    def __call__(self, event: T_Event, /) -> Any: ...


@runtime_checkable
class AsyncEventHandlerFunc(Protocol[T_Event]):
    """Protocol for async event handler functions."""

    async def __call__(self, event: T_Event, /) -> Any: ...


@runtime_checkable
class EventHandlerMethod(Protocol[T_Event]):
    """Protocol for instance method event handlers."""

    def __call__(self, self_: Any, event: T_Event, /) -> Any: ...

    __self__: Any
    __name__: str


@runtime_checkable
class AsyncEventHandlerMethod(Protocol[T_Event]):
    """Protocol for async instance method event handlers."""

    async def __call__(self, self_: Any, event: T_Event, /) -> Any: ...

    __self__: Any
    __name__: str


@runtime_checkable
class EventHandlerClassMethod(Protocol[T_EventInvariant]):
    """Protocol for class method event handlers."""

    def __call__(self, cls: type[Any], event: T_EventInvariant, /) -> Any: ...

    __self__: type[Any]
    __name__: str
    __func__: Callable[[type[Any], T_EventInvariant], Any]


@runtime_checkable
class AsyncEventHandlerClassMethod(Protocol[T_EventInvariant]):
    """Protocol for async class method event handlers."""

    async def __call__(self, cls: type[Any], event: T_EventInvariant, /) -> Any: ...

    __self__: type[Any]
    __name__: str
    __func__: Callable[[type[Any], T_EventInvariant], Awaitable[Any]]


# Event handlers are normalized to bound single-argument callables at registration time.
EventHandlerCallable: TypeAlias = EventHandlerFunc['BaseEvent[Any]'] | AsyncEventHandlerFunc['BaseEvent[Any]']

# Normalized async callable shape used at call sites that require a Coroutine.
NormalizedEventHandlerCallable: TypeAlias = Callable[
    ['BaseEvent[T_EventResult]'],
    Coroutine[Any, Any, T_EventResult | 'BaseEvent[Any]' | None],
]

# Internal normalized one-argument callable used for invocation at runtime.
_InvokableEventHandlerCallable: TypeAlias = Callable[['BaseEvent[Any]'], Any | Awaitable[Any]]

# ContravariantEventHandlerCallable allows subtype-specific handlers.
ContravariantEventHandlerCallable: TypeAlias = EventHandlerFunc[T_Event] | AsyncEventHandlerFunc[T_Event]

HANDLER_ID_NAMESPACE: UUID = uuid5(NAMESPACE_DNS, 'abxbus-handler')


def _format_handler_source_path(path: str, line_no: int | None = None) -> str:
    normalized = str(Path(path).expanduser().resolve())
    home = str(Path.home())
    if normalized == home:
        display = '~'
    elif normalized.startswith(home + os.sep):
        display = f'~{normalized[len(home) :]}'
    else:
        display = normalized
    return f'{display}:{line_no}' if line_no else display


def _defined_class_method_name(target: Callable[..., Any]) -> str | None:
    qualname = getattr(target, '__qualname__', None)
    if not isinstance(qualname, str):
        return None

    parts = qualname.split('.')
    if len(parts) < 2 or parts[-2] == '<locals>':
        return None

    class_name, method_name = parts[-2], parts[-1]
    if class_name.isidentifier() and method_name.isidentifier():
        return f'{class_name}.{method_name}'
    return None


class _HandlerCacheKey:
    __slots__ = ('handler_ref', 'handler_id', '_hash')

    def __init__(self, handler: Callable[..., Any]) -> None:
        # Some callables override __eq__ without __hash__ and become unhashable.
        # Use identity-based hashing for a stable cache key without retaining handlers.
        self.handler_ref = weakref(handler)
        self.handler_id = id(handler)
        self._hash = self.handler_id

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _HandlerCacheKey):
            return False
        if self.handler_id != other.handler_id:
            return False
        return self.handler_ref() is other.handler_ref()


@lru_cache(maxsize=100)
def _get_callable_handler_file_path(handler_key: _HandlerCacheKey) -> str | None:
    """Best-effort, low-overhead source location for a handler callable."""
    handler = handler_key.handler_ref()
    if handler is None:
        return None
    target: Any = handler.__func__ if inspect.ismethod(handler) else handler
    target = inspect.unwrap(target)

    code_obj = getattr(target, '__code__', None)
    if code_obj is not None:
        file_path = getattr(code_obj, 'co_filename', None)
        line_no = getattr(code_obj, 'co_firstlineno', None)
        if isinstance(file_path, str) and file_path.strip():
            return _format_handler_source_path(file_path, int(line_no) if isinstance(line_no, int) else None)

    try:
        source_file = inspect.getsourcefile(target) or inspect.getfile(target)
    except (OSError, TypeError):
        source_file = None

    line_no: int | None = None
    try:
        _, line_no = inspect.getsourcelines(target)
    except (OSError, TypeError):
        line_no = None

    if isinstance(source_file, str) and source_file.strip():
        return _format_handler_source_path(source_file, line_no)

    module = inspect.getmodule(target)
    module_file = getattr(module, '__file__', None) if module is not None else None
    if isinstance(module_file, str) and module_file.strip():
        return _format_handler_source_path(module_file, line_no)

    return None


@overload
def _normalize_handler_callable(
    handler: Callable[[T_HandlerEvent], Coroutine[Any, Any, T_HandlerReturn]],
) -> Callable[[T_HandlerEvent], Coroutine[Any, Any, T_HandlerReturn]]: ...


@overload
def _normalize_handler_callable(
    handler: Callable[[T_HandlerEvent], Awaitable[T_HandlerReturn]],
) -> Callable[[T_HandlerEvent], Coroutine[Any, Any, T_HandlerReturn]]: ...


@overload
def _normalize_handler_callable(
    handler: Callable[[T_HandlerEvent], T_HandlerReturn],
) -> Callable[[T_HandlerEvent], Coroutine[Any, Any, T_HandlerReturn]]: ...


def _normalize_handler_callable(
    handler: Callable[[T_HandlerEvent], object],
) -> Callable[[T_HandlerEvent], Coroutine[Any, Any, T_HandlerReturn]]:
    """Normalize one handler callable to a single async call signature."""
    if not callable(handler):
        raise ValueError(f'Handler {handler!r} must be callable, got: {type(handler)}')

    if inspect.iscoroutinefunction(handler):
        return cast(Callable[[T_HandlerEvent], Coroutine[Any, Any, T_HandlerReturn]], handler)

    async def normalized_handler(event: T_HandlerEvent) -> T_HandlerReturn:
        handler_result = handler(event)
        # BaseEvent implements __await__ for ergonomic `await event`, but handler
        # return values of BaseEvent must be treated as plain results (forwarded
        # child event refs), not awaited here.
        if inspect.isawaitable(handler_result):
            from abxbus.base_event import BaseEvent

            if isinstance(handler_result, BaseEvent):
                return cast(T_HandlerReturn, handler_result)
            return cast(T_HandlerReturn, await handler_result)
        return cast(T_HandlerReturn, handler_result)

    return normalized_handler


class EventHandler(BaseModel):
    """Serializable metadata wrapper around a registered event handler callable."""

    model_config = ConfigDict(
        extra='forbid',
        arbitrary_types_allowed=True,
        validate_assignment=False,
        validate_default=True,
        revalidate_instances='always',
    )

    id: str = ''
    handler: EventHandlerCallable | None = Field(default=None, exclude=True, repr=False)
    handler_name: str = 'anonymous'
    handler_file_path: str | None = None
    handler_timeout: float | None = None
    handler_slow_timeout: float | None = None
    handler_registered_at: str = Field(default_factory=monotonic_datetime)
    event_pattern: str = '*'
    eventbus_name: str = 'EventBus'
    eventbus_id: str = '00000000-0000-0000-0000-000000000000'
    _mutation_callback: Callable[['EventHandler'], None] | None = PrivateAttr(default=None)

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)
        if name in {'handler_timeout', 'handler_slow_timeout'}:
            callback = self._mutation_callback
            if callback is not None:
                callback(self)

    @field_validator('handler_name', mode='before')
    @classmethod
    def _validate_handler_name_field(cls, value: Any) -> str:
        if isinstance(value, str):
            normalized = value.strip()
            if normalized:
                return normalized
        return 'anonymous'

    @field_validator('eventbus_name')
    @classmethod
    def _validate_eventbus_name_field(cls, value: str) -> str:
        normalized = str(value)
        assert normalized.isidentifier() and not normalized.startswith('_'), f'Invalid event bus name: {value!r}'
        return normalized

    @field_validator('handler_registered_at', mode='before')
    @classmethod
    def _normalize_handler_registered_at(cls, value: Any) -> str:
        if isinstance(value, datetime):
            normalized_value = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
            normalized_input = normalized_value.isoformat().replace('+00:00', 'Z')
            return monotonic_datetime(normalized_input)
        if value is None:
            return monotonic_datetime()
        return monotonic_datetime(str(value))

    @property
    def eventbus_label(self) -> str:
        return f'{self.eventbus_name}#{self.eventbus_id[-4:]}'

    @staticmethod
    def get_callable_handler_name(handler: Callable[..., Any]) -> str:
        assert hasattr(handler, '__name__'), f'Handler {handler} has no __name__ attribute!'
        if inspect.ismethod(handler):
            defined_name = _defined_class_method_name(handler.__func__)
            if defined_name:
                return defined_name
            return f'{type(handler.__self__).__name__}.{handler.__name__}'
        if callable(handler):
            defined_name = _defined_class_method_name(handler)
            if defined_name:
                return defined_name
            handler_module = getattr(handler, '__module__', '<unknown>')
            handler_name = getattr(handler, '__name__', type(handler).__name__)
            return f'{handler_module}.{handler_name}'
        raise ValueError(f'Invalid handler: {handler} {type(handler)}, expected a function, coroutine, or method')

    @model_validator(mode='before')
    @classmethod
    def _populate_handler_name(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        params = cast(dict[str, Any], data)
        if params.get('id') is None:
            params.pop('id', None)
        handler = params.get('handler')
        if handler is not None and not params.get('handler_name'):
            try:
                derived_name = cls.get_callable_handler_name(cast(EventHandlerCallable, handler))
                params['handler_name'] = derived_name.strip() or 'function'
            except Exception:
                params['handler_name'] = 'function'
        return params

    def model_post_init(self, __context: Any) -> None:
        if not self.id:
            self.id = self.compute_handler_id()

    @property
    def _handler_async(self) -> NormalizedEventHandlerCallable[Any] | None:
        """Return the normalized async callable view of `handler`."""
        if self.handler is None:
            return None
        return cast(
            NormalizedEventHandlerCallable[Any],
            _normalize_handler_callable(cast(Callable[[Any], object], self.handler)),
        )

    def compute_handler_id(self) -> str:
        """Match TS handler-id algorithm: uuidv5(seed, HANDLER_ID_NAMESPACE)."""
        file_path = self.handler_file_path or 'unknown'
        seed = f'{self.eventbus_id}|{self.handler_name}|{file_path}|{self.handler_registered_at}|{self.event_pattern}'
        return str(uuid5(HANDLER_ID_NAMESPACE, seed))

    @property
    def label(self) -> str:
        assert self.id, 'EventHandler.id must be set'
        return f'{self.handler_name}#{self.id[-4:]}'

    def __str__(self) -> str:
        has_name = self.handler_name and self.handler_name != 'anonymous'
        assert self.id, 'EventHandler.id must be set'
        display = f'{self.handler_name}()' if has_name else f'function#{self.id[-4:]}()'
        return f'{display} @ {self.handler_file_path}' if self.handler_file_path else display

    def __call__(self, event: 'BaseEvent[Any]') -> Any:
        if self.handler is None:
            raise RuntimeError(f'EventHandler {self.id} has no callable attached')
        handler_callable = cast(Callable[[Any], Any], self.handler)
        return handler_callable(event)

    @classmethod
    def from_callable(
        cls,
        *,
        handler: ContravariantEventHandlerCallable[Any],
        event_pattern: str,
        eventbus_name: str,
        eventbus_id: str,
        detect_handler_file_path: bool = True,
        id: str | None = None,
        handler_file_path: str | None = None,
        handler_timeout: float | None = None,
        handler_slow_timeout: float | None = None,
        handler_registered_at: str | datetime | None = None,
    ) -> 'EventHandler':
        resolved_file_path = handler_file_path
        if resolved_file_path is None and detect_handler_file_path:
            resolved_file_path = _get_callable_handler_file_path(_HandlerCacheKey(handler))

        handler_params: dict[str, Any] = {
            'handler': handler,
            'handler_file_path': resolved_file_path,
            'handler_registered_at': handler_registered_at,
            'event_pattern': event_pattern,
            'eventbus_name': eventbus_name,
            'eventbus_id': eventbus_id,
        }
        try:
            derived_name = cls.get_callable_handler_name(handler)
            handler_params['handler_name'] = derived_name.strip() or 'function'
        except Exception:
            handler_params['handler_name'] = 'function'
        if id is not None:
            handler_params['id'] = id
        if handler_timeout is not None:
            handler_params['handler_timeout'] = handler_timeout
        if handler_slow_timeout is not None:
            handler_params['handler_slow_timeout'] = handler_slow_timeout

        entry = cls(**handler_params)
        if not entry.id:
            entry.id = entry.compute_handler_id()
        return entry


__all__ = [
    'AsyncEventHandlerClassMethod',
    'AsyncEventHandlerFunc',
    'AsyncEventHandlerMethod',
    'ContravariantEventHandlerCallable',
    'EventHandler',
    'EventHandlerCallable',
    'EventHandlerClassMethod',
    'EventHandlerFunc',
    'EventHandlerMethod',
    'NormalizedEventHandlerCallable',
]
