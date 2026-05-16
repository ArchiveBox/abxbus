"""Shared helpers for bridge-side inbound dispatch."""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable
from typing import Any

from abxbus.base_event import BaseEvent
from abxbus.event_history import EventHistory

logger = logging.getLogger('abxbus.bridges')


def event_pattern_matches(event_pattern: Any, event: BaseEvent[Any]) -> bool:
    normalized_pattern = EventHistory.normalize_event_pattern(event_pattern)
    return normalized_pattern == '*' or normalized_pattern == event.event_type


async def dispatch_bridge_event(
    handlers: list[tuple[Any, Callable[[BaseEvent[Any]], Any]]],
    event: BaseEvent[Any],
) -> None:
    for event_pattern, handler in list(handlers):
        if not event_pattern_matches(event_pattern, event):
            continue
        try:
            result = handler(event)
            if inspect.isawaitable(result):
                await result
        except Exception:
            logger.exception('Error in bridge handler for %s', event.event_type)
