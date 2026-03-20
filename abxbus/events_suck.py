from __future__ import annotations

import inspect
import types
from collections.abc import Awaitable, Callable, Mapping
from types import SimpleNamespace
from typing import Any, Protocol, TypeVar, cast, get_args, get_origin

from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from abxbus.base_event import BaseEvent
from abxbus.event_bus import EventBus
from abxbus.helpers import extract_basemodel_generic_arg

EventClass = type[BaseEvent[Any]]
_BASE_EVENT_FIELD_NAMES = frozenset(BaseEvent.model_fields)
_EMPTY = inspect.Parameter.empty
T_Result = TypeVar('T_Result')


class _HasBus(Protocol):
    bus: EventBus


class GeneratedEvents(SimpleNamespace):
    by_name: dict[str, EventClass]


def _custom_event_fields(event_cls: EventClass) -> list[tuple[str, FieldInfo]]:
    return [
        (field_name, field) for field_name, field in event_cls.model_fields.items() if field_name not in _BASE_EVENT_FIELD_NAMES
    ]


def _event_field_default(field: FieldInfo) -> Any:
    default = field.get_default(call_default_factory=False)
    if default is PydanticUndefined:
        return None
    return default


def _event_result_annotation(event_cls: EventClass) -> Any:
    generic_result_type = extract_basemodel_generic_arg(event_cls)
    if generic_result_type is not None:
        return generic_result_type

    result_field = event_cls.model_fields.get('event_result_type')
    if result_field is not None and result_field.default not in (None, PydanticUndefined):
        return result_field.default

    return Any


def _callable_params(func: Callable[..., Any]) -> tuple[list[inspect.Parameter], bool, Any]:
    signature = inspect.signature(func)
    params = list(signature.parameters.values())
    has_var_kwargs = any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params)
    if params and params[0].name in {'self', 'cls'}:
        params = params[1:]

    filtered: list[inspect.Parameter] = []
    for param in params:
        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            raise TypeError(f'events_suck does not support *args in {func!r}')
        if param.kind == inspect.Parameter.POSITIONAL_ONLY:
            raise TypeError(f'events_suck does not support positional-only params in {func!r}')
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            continue
        filtered.append(param)

    return_annotation = signature.return_annotation if signature.return_annotation is not _EMPTY else Any
    return filtered, has_var_kwargs, return_annotation


def _event_payload(event: BaseEvent[Any]) -> dict[str, Any]:
    payload = {
        field_name: getattr(event, field_name)
        for field_name in event.__class__.model_fields
        if field_name not in _BASE_EVENT_FIELD_NAMES
    }
    extras = event.model_extra
    if isinstance(extras, dict):
        payload.update(extras)
    return payload


def _annotation_allows_none(annotation: Any) -> bool:
    if annotation is None or annotation is type(None):
        return True
    origin = get_origin(annotation)
    if origin is None:
        return False
    return any(arg is type(None) for arg in get_args(annotation))


def _make_event_class(event_name: str, func: Callable[..., Any]) -> EventClass:
    if not event_name.isidentifier() or event_name.startswith('_'):
        raise ValueError(f'Invalid event name: {event_name!r}')

    params, _, return_annotation = _callable_params(func)
    annotations: dict[str, Any] = {'event_result_type': Any}
    namespace: dict[str, Any] = {'__module__': __name__, 'event_type': event_name, 'event_result_type': return_annotation}
    for param in params:
        annotation = param.annotation if param.annotation is not _EMPTY else Any
        annotations[param.name] = annotation
        if param.default is not _EMPTY:
            namespace[param.name] = param.default
        elif _annotation_allows_none(annotation):
            namespace[param.name] = None
    namespace['__annotations__'] = annotations
    try:
        event_base = cast(type[Any], BaseEvent[return_annotation])
    except Exception:
        event_base = BaseEvent
    event_cls = types.new_class(event_name, (event_base,), exec_body=lambda ns: ns.update(namespace))
    return cast(EventClass, event_cls)


def make_events(events: Mapping[str, Callable[..., Any]]) -> GeneratedEvents:
    by_name = {event_name: _make_event_class(event_name, func) for event_name, func in events.items()}
    return GeneratedEvents(**by_name, by_name=by_name)


def make_handler(func: Callable[..., T_Result | Awaitable[T_Result]]) -> Callable[[BaseEvent[Any]], Awaitable[T_Result]]:
    params, has_var_kwargs, _ = _callable_params(func)

    async def _handler(event: BaseEvent[Any]) -> T_Result:
        payload = _event_payload(event)
        kwargs: dict[str, Any] = {}
        for param in params:
            if param.name in payload:
                kwargs[param.name] = payload.pop(param.name)
            elif param.default is _EMPTY:
                raise TypeError(f'Missing required event field {param.name!r} for handler {func!r}')
        if has_var_kwargs:
            kwargs.update(payload)
        result = func(**kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    return _handler


def _build_event_method(class_name: str, method_name: str, event_cls: EventClass):
    event_fields = _custom_event_fields(event_cls)
    event_field_names = tuple(field_name for field_name, _ in event_fields)

    parameters = [inspect.Parameter('self', inspect.Parameter.POSITIONAL_OR_KEYWORD)]
    for field_name, field in event_fields:
        field_annotation = field.annotation if field.annotation is not None else Any
        field_default = inspect.Parameter.empty if field.is_required() else _event_field_default(field)
        parameters.append(
            inspect.Parameter(
                field_name,
                inspect.Parameter.KEYWORD_ONLY,
                default=field_default,
                annotation=field_annotation,
            )
        )
    parameters.append(inspect.Parameter('extra', inspect.Parameter.VAR_KEYWORD, annotation=Any))
    signature = inspect.Signature(parameters=parameters, return_annotation=_event_result_annotation(event_cls))

    async def _method(self: _HasBus, *args: Any, **kwargs: Any) -> Any:
        bound = signature.bind(self, *args, **kwargs)
        payload: dict[str, Any] = {
            field_name: bound.arguments[field_name] for field_name in event_field_names if field_name in bound.arguments
        }
        payload.update(cast(dict[str, Any], bound.arguments.get('extra', {})))
        return await self.bus.emit(event_cls(**payload)).event_result()

    _method.__name__ = method_name
    _method.__qualname__ = f'{class_name}.{method_name}'
    _method.__annotations__ = {
        **{field_name: (field.annotation if field.annotation is not None else Any) for field_name, field in event_fields},
        'extra': Any,
        'return': signature.return_annotation,
    }
    cast(Any, _method).__signature__ = signature
    return _method


def wrap(class_name: str, methods: Mapping[str, EventClass]) -> type[Any]:
    if not class_name.isidentifier() or class_name.startswith('_'):
        raise ValueError(f'Invalid class name: {class_name!r}')

    def __init__(self: _HasBus, bus: EventBus | None = None) -> None:
        self.bus = bus or EventBus(f'{class_name}Bus')

    namespace: dict[str, Any] = {
        '__module__': __name__,
        '__annotations__': {'bus': EventBus},
        '__init__': __init__,
    }

    for method_name, event_cls in methods.items():
        if not method_name.isidentifier() or method_name.startswith('_'):
            raise ValueError(f'Invalid method name: {method_name!r}')
        namespace[method_name] = _build_event_method(class_name, method_name, event_cls)

    return cast(type[Any], type(class_name, (), namespace))


__all__ = ['GeneratedEvents', 'make_events', 'make_handler', 'wrap']
