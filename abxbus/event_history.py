from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any, Generic, Literal, TypeVar, overload

from .base_event import BaseEvent, UUIDStr
from .helpers import monotonic_datetime

BaseEventT = TypeVar('BaseEventT', bound=BaseEvent[Any])
TExpectedEvent = TypeVar('TExpectedEvent', bound=BaseEvent[Any])
EventPatternType = str | Literal['*'] | type[BaseEvent[Any]]

logger = logging.getLogger('abxbus')


class EventHistory(dict[UUIDStr, BaseEventT], Generic[BaseEventT]):
    """Ordered event history map with query and trim helpers."""

    __slots__ = ('max_history_size', 'max_history_drop', '_warned_about_dropping_uncompleted_events')

    def __init__(self, max_history_size: int | None = 100, max_history_drop: bool = False):
        super().__init__()
        self.max_history_size = max_history_size
        self.max_history_drop = max_history_drop
        self._warned_about_dropping_uncompleted_events = False

    def add_event(self, event: BaseEventT) -> None:
        self[event.event_id] = event

    def get_event(self, event_id: str) -> BaseEventT | None:
        return self.get(event_id)

    def remove_event(self, event_id: str) -> bool:
        if event_id not in self:
            return False
        del self[event_id]
        return True

    def has_event(self, event_id: str) -> bool:
        return event_id in self

    @staticmethod
    def normalize_event_pattern(event_pattern: EventPatternType) -> str:
        if event_pattern == '*':
            return '*'
        if isinstance(event_pattern, str):
            return event_pattern
        event_type_field = event_pattern.model_fields.get('event_type')
        event_type_default = event_type_field.default if event_type_field is not None else None
        if isinstance(event_type_default, str) and event_type_default not in ('', 'UndefinedEvent'):
            return event_type_default
        return event_pattern.__name__

    def event_is_child_of(self, event: BaseEvent[Any], ancestor: BaseEvent[Any]) -> bool:
        current_id = event.event_parent_id
        visited: set[str] = set()

        while current_id and current_id not in visited:
            if current_id == ancestor.event_id:
                return True
            visited.add(current_id)
            parent = self.get(current_id)
            if parent is None:
                return False
            current_id = parent.event_parent_id

        return False

    @overload
    async def find(
        self,
        event_type: type[TExpectedEvent],
        where: None = None,
        child_of: BaseEvent[Any] | None = None,
        past: bool | float | timedelta | None = None,
        future: bool | float | None = None,
        event_is_child_of: Callable[[BaseEvent[Any], BaseEvent[Any]], bool] | None = None,
        wait_for_future_match: Callable[
            [str, Callable[[BaseEvent[Any]], bool], bool | float],
            Awaitable[BaseEvent[Any] | None],
        ]
        | None = None,
        **event_fields: Any,
    ) -> TExpectedEvent | None: ...

    @overload
    async def find(
        self,
        event_type: type[TExpectedEvent],
        where: Callable[[TExpectedEvent], bool],
        child_of: BaseEvent[Any] | None = None,
        past: bool | float | timedelta | None = None,
        future: bool | float | None = None,
        event_is_child_of: Callable[[BaseEvent[Any], BaseEvent[Any]], bool] | None = None,
        wait_for_future_match: Callable[
            [str, Callable[[BaseEvent[Any]], bool], bool | float],
            Awaitable[BaseEvent[Any] | None],
        ]
        | None = None,
        **event_fields: Any,
    ) -> TExpectedEvent | None: ...

    @overload
    async def find(
        self,
        event_type: str | Literal['*'],
        where: Callable[[BaseEvent[Any]], bool] | None = None,
        child_of: BaseEvent[Any] | None = None,
        past: bool | float | timedelta | None = None,
        future: bool | float | None = None,
        event_is_child_of: Callable[[BaseEvent[Any], BaseEvent[Any]], bool] | None = None,
        wait_for_future_match: Callable[
            [str, Callable[[BaseEvent[Any]], bool], bool | float],
            Awaitable[BaseEvent[Any] | None],
        ]
        | None = None,
        **event_fields: Any,
    ) -> BaseEvent[Any] | None: ...

    async def find(
        self,
        event_type: EventPatternType,
        where: Callable[[Any], bool] | None = None,
        child_of: BaseEvent[Any] | None = None,
        past: bool | float | timedelta | None = None,
        future: bool | float | None = None,
        event_is_child_of: Callable[[BaseEvent[Any], BaseEvent[Any]], bool] | None = None,
        wait_for_future_match: Callable[
            [str, Callable[[BaseEvent[Any]], bool], bool | float],
            Awaitable[BaseEvent[Any] | None],
        ]
        | None = None,
        **event_fields: Any,
    ) -> BaseEvent[Any] | None:
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
            return None

        event_key = self.normalize_event_pattern(event_type)
        where_predicate: Callable[[BaseEvent[Any]], bool]
        if where is None:
            where_predicate = lambda _: True
        else:
            where_predicate = where

        child_check = event_is_child_of or self.event_is_child_of
        cutoff: str | None = None
        if resolved_past is not True:
            cutoff_dt = datetime.now(UTC) - timedelta(seconds=float(resolved_past))
            cutoff = monotonic_datetime(cutoff_dt.isoformat().replace('+00:00', 'Z'))
        missing = object()

        def matches(event: BaseEvent[Any]) -> bool:
            return (
                (event_key == '*' or event.event_type == event_key)
                and (child_of is None or child_check(event, child_of))
                and all(
                    getattr(event, field_name, missing) == expected_value for field_name, expected_value in event_fields.items()
                )
                and where_predicate(event)
            )

        if resolved_past is not False:
            events = list(self.values())
            for event in reversed(events):
                if cutoff is not None and event.event_created_at < cutoff:
                    continue
                if matches(event):
                    return event

        if resolved_future is False or wait_for_future_match is None:
            return None

        return await wait_for_future_match(event_key, matches, resolved_future)

    def trim_event_history(
        self,
        *,
        on_remove: Callable[[BaseEventT], None] | None = None,
        owner_label: str | None = None,
    ) -> int:
        if self.max_history_size is None:
            return 0

        if self.max_history_size == 0:
            completed_event_ids = [event_id for event_id, event in self.items() if event.event_status == 'completed']
            removed_count = 0
            for event_id in completed_event_ids:
                event = self.get(event_id)
                if event is None:
                    continue
                del self[event_id]
                if on_remove:
                    on_remove(event)
                removed_count += 1
            return removed_count

        if not self.max_history_drop or len(self) <= self.max_history_size:
            return 0

        remaining_overage = len(self) - self.max_history_size
        removed_count = 0

        def remove_event(event_id: str, event: BaseEventT) -> None:
            nonlocal removed_count
            del self[event_id]
            if on_remove:
                on_remove(event)
            removed_count += 1

        for event_id, event in list(self.items()):
            if remaining_overage <= 0:
                break
            if event.event_status != 'completed':
                continue
            remove_event(event_id, event)
            remaining_overage -= 1

        dropped_uncompleted = 0
        for event_id, event in list(self.items()):
            if remaining_overage <= 0:
                break
            if event.event_status != 'completed':
                dropped_uncompleted += 1
            remove_event(event_id, event)
            remaining_overage -= 1

        if dropped_uncompleted > 0 and not self._warned_about_dropping_uncompleted_events:
            self._warned_about_dropping_uncompleted_events = True
            owner = owner_label or 'EventBus'
            logger.warning(
                '[abxbus] ⚠️ Bus %s has exceeded max_history_size=%s and is dropping oldest history entries '
                '(even uncompleted events). Increase max_history_size or set max_history_drop=False to reject.',
                owner,
                self.max_history_size,
            )

        return removed_count
