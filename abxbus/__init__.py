"""Event bus library."""

from . import events_suck
from .base_event import (
    BaseEvent,
    EventConcurrencyMode,
    EventHandlerCompletionMode,
    EventHandlerConcurrencyMode,
    EventResult,
    EventStatus,
    PythonIdentifierStr,
    PythonIdStr,
    UUIDStr,
)
from .bridges import HTTPEventBridge, SocketEventBridge
from .event_bus import EventBus
from .event_handler import (
    EventHandler,
    EventHandlerAbortedError,
    EventHandlerCancelledError,
    EventHandlerResultSchemaError,
    EventHandlerTimeoutError,
)
from .event_history import EventHistory
from .helpers import monotonic_datetime
from .middlewares import (
    AutoErrorEventMiddleware,
    AutoHandlerChangeEventMiddleware,
    AutoReturnEventMiddleware,
    BusHandlerRegisteredEvent,
    BusHandlerUnregisteredEvent,
    EventBusMiddleware,
    LoggerEventBusMiddleware,
    OtelTracingMiddleware,
    SQLiteHistoryMirrorMiddleware,
    WALEventBusMiddleware,
)

__all__ = [
    'EventBus',
    'EventBusMiddleware',
    'BusHandlerRegisteredEvent',
    'BusHandlerUnregisteredEvent',
    'HTTPEventBridge',
    'SocketEventBridge',
    'LoggerEventBusMiddleware',
    'OtelTracingMiddleware',
    'SQLiteHistoryMirrorMiddleware',
    'AutoErrorEventMiddleware',
    'AutoHandlerChangeEventMiddleware',
    'AutoReturnEventMiddleware',
    'WALEventBusMiddleware',
    'EventHistory',
    'monotonic_datetime',
    'BaseEvent',
    'EventStatus',
    'EventResult',
    'EventHandler',
    'EventHandlerCancelledError',
    'EventHandlerResultSchemaError',
    'EventHandlerTimeoutError',
    'EventHandlerAbortedError',
    'EventHandlerConcurrencyMode',
    'EventHandlerCompletionMode',
    'EventConcurrencyMode',
    'UUIDStr',
    'PythonIdStr',
    'PythonIdentifierStr',
    'events_suck',
]
