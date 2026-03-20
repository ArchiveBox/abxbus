"""Helper functions for logging event trees and formatting"""

import asyncio
import logging
import math
from collections import defaultdict
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from abxbus.base_event import BaseEvent, EventResult
    from abxbus.event_bus import EventBus


def format_timestamp(dt: str | datetime | None) -> str:
    """Format an ISO datetime string (or datetime) for display."""
    if dt is None:
        return 'N/A'
    if isinstance(dt, str):
        parsed = datetime.fromisoformat(dt)
    else:
        parsed = dt
    return parsed.strftime('%H:%M:%S.%f')[:-3]


def format_result_value(value: Any) -> str:
    """Format a result value for display"""
    if value is None:
        return 'None'
    if hasattr(value, 'event_type') and hasattr(value, 'event_id'):  # BaseEvent check without import
        return f'Event({value.event_type}#{value.event_id[-4:]})'
    if isinstance(value, (str, int, float, bool)):
        return repr(value)
    if isinstance(value, dict):
        value_dict = cast(dict[Any, Any], value)
        return f'dict({len(value_dict)} items)'
    if isinstance(value, list):
        value_list = cast(list[Any], value)
        return f'list({len(value_list)} items)'
    return f'{type(value).__name__}(...)'


def log_event_tree(
    event: 'BaseEvent[Any]',
    indent: str = '',
    is_last: bool = True,
    event_children_by_parent: dict[str | None, list['BaseEvent[Any]']] | None = None,
) -> str:
    from abxbus.base_event import logger

    """Print this event and its results with proper tree formatting"""
    # Determine the connector
    connector = '└── ' if is_last else '├── '

    # Print this event's line
    # Format timing info
    timing_str = f'[{format_timestamp(event.event_created_at)}'
    if event.event_completed_at and event.event_created_at:
        completed_dt = datetime.fromisoformat(event.event_completed_at)
        created_dt = datetime.fromisoformat(event.event_created_at)
        duration = (completed_dt - created_dt).total_seconds()
        timing_str += f' ({duration:.3f}s)'
    timing_str += ']'

    lines: list[str] = []

    event_line = f'{indent}{connector}{event.event_type}#{event.event_id[-4:]} {timing_str}'
    logger.warning(event_line)
    lines.append(event_line)

    # Calculate the new indent for children
    extension = '    ' if is_last else '│   '
    new_indent = indent + extension

    # Track which child events were printed via handlers to avoid duplicates
    printed_child_ids: set[str] = set()

    # Print each result
    if event.event_results:
        results_sorted = sorted(event.event_results.items(), key=lambda x: x[1].started_at or '')

        # Calculate which is the last item considering both results and unmapped children
        unmapped_children: list[BaseEvent[Any]] = []
        if event_children_by_parent:
            all_children = event_children_by_parent.get(event.event_id, [])
            for child in all_children:
                # Will be printed later if not already printed by a handler
                if child.event_id not in [c.event_id for r in event.event_results.values() for c in r.event_children]:
                    unmapped_children.append(child)

        total_items = len(results_sorted) + len(unmapped_children)

        for i, (_handler_id, result) in enumerate(results_sorted):
            is_last_item = i == total_items - 1
            lines.append(log_event_result_tree(result, new_indent, is_last_item, event_children_by_parent))
            # Track child events printed by this result
            for child in result.event_children:
                printed_child_ids.add(child.event_id)

    # Print unmapped children (those not printed by any handler)
    if event_children_by_parent:
        children = event_children_by_parent.get(event.event_id, [])
        for i, child in enumerate(children):
            if child.event_id not in printed_child_ids:
                is_last_child = i == len(children) - 1
                lines.append(log_event_tree(child, new_indent, is_last_child, event_children_by_parent))

    return '\n'.join(lines)


def log_event_result_tree(
    result: 'EventResult[Any]',
    indent: str = '',
    is_last: bool = True,
    event_children_by_parent: dict[str | None, list['BaseEvent[Any]']] | None = None,
) -> str:
    """Print this result and its child events with proper tree formatting"""

    from abxbus.base_event import logger

    # Determine the connector
    connector = '└── ' if is_last else '├── '

    # Status icon
    result_icon = (
        '✅'
        if result.status == 'completed'
        else '❌'
        if result.error is not None
        else '🏃'
        if result.status == 'started'
        else '⏳'
    )

    # Format handler name with bus info
    handler_display = f'{result.eventbus_label}.{result.handler.label}'

    # Format the result line
    result_line = f'{indent}{connector}{result_icon} {handler_display}'

    # Add timing info
    if result.started_at:
        result_line += f' [{format_timestamp(result.started_at)}'
        if result.completed_at:
            completed_dt = datetime.fromisoformat(result.completed_at)
            started_dt = datetime.fromisoformat(result.started_at)
            duration = (completed_dt - started_dt).total_seconds()
            result_line += f' ({duration:.3f}s)'
        result_line += ']'

    # Add result value or error
    if result.status == 'error' and result.error:
        result_line += f' ☠️ {type(result.error).__name__}: {str(result.error)}'
    elif result.status == 'completed':
        result_line += f' → {format_result_value(result.result)}'

    lines: list[str] = []
    logger.warning(result_line)
    lines.append(result_line)

    # Calculate the new indent for child events
    extension = '    ' if is_last else '│   '
    new_indent = indent + extension

    # Print child events dispatched by this handler

    if result.event_children:
        for i, child in enumerate(result.event_children):
            is_last_child = i == len(result.event_children) - 1
            lines.append(log_event_tree(child, new_indent, is_last_child, event_children_by_parent))

    return '\n'.join(lines)


def log_eventbus_tree(eventbus: 'EventBus') -> str:
    """Print a nice pretty formatted tree view of all events in the history including their results and child events recursively"""

    from abxbus.base_event import logger

    # Build a mapping of parent_id to child events
    parent_to_children: dict[str | None, list[BaseEvent[Any]]] = defaultdict(list)
    for event in eventbus.event_history.values():
        parent_to_children[event.event_parent_id].append(event)

    # Sort events by creation time
    for children in parent_to_children.values():
        children.sort(key=lambda e: e.event_created_at)

    # Find root events (those without parents or with self as parent)
    root_events = list(parent_to_children[None])

    # Also include events that have themselves as parent (edge case)
    for event in eventbus.event_history.values():
        if event.event_parent_id == event.event_id and event not in root_events:
            root_events.append(event)
            # Remove from its incorrect parent mapping to avoid double printing
            if event.event_id in parent_to_children:
                parent_to_children[event.event_id] = [
                    e for e in parent_to_children[event.event_id] if e.event_id != event.event_id
                ]

    logger.warning(f'📊 Event History Tree for {eventbus}')
    logger.warning('=' * 80)

    if not root_events:
        logger.warning('  (No events in history)')
        return '(No events in history)'

    # Print all root events using their log_tree helper method
    lines: list[str] = []
    for i, event in enumerate(root_events):
        is_last = i == len(root_events) - 1
        lines.append(log_event_tree(event, '', is_last, parent_to_children))

    logger.warning('=' * 80)

    return '\n'.join(lines)


def log_timeout_tree(event: 'BaseEvent[Any]', timed_out_result: 'EventResult[Any]') -> None:
    """Log detailed timeout information showing the event tree and which handler timed out"""

    from abxbus.base_event import logger
    from abxbus.event_bus import EventBus

    if not logger.isEnabledFor(logging.WARNING):
        return

    now = datetime.now(UTC)

    # Find the root event by walking up the parent chain
    root_event = event
    visited_parent_ids: set[str] = set()
    while root_event.event_parent_id and root_event.event_parent_id not in visited_parent_ids:
        visited_parent_ids.add(root_event.event_parent_id)
        parent_found = False
        # Search for parent in all EventBus instances
        for bus in list(EventBus.all_instances):
            if root_event.event_parent_id in bus.event_history:
                root_event = bus.event_history[root_event.event_parent_id]
                parent_found = True
                break
        if not parent_found:
            break

    red = '\033[91m'
    green = '\033[92m'
    yellow = '\033[93m'
    pink = '\033[95m'
    reset = '\033[0m'

    logger.warning('=' * 80)
    logger.warning(
        f'⏱️  TIMEOUT ERROR - Handling took more than {event.event_timeout}s for {timed_out_result.eventbus_label}.{timed_out_result.handler_name}({event})'
    )
    logger.warning('=' * 80)

    def print_handler_line(
        handler_indent: str,
        handler_name: str,
        event_id_suffix: str,
        status: str = 'pending',
        started_at: str | None = None,
        completed_at: str | None = None,
        timeout: float | None = None,
        is_expired: bool = False,
        is_interrupted: bool = False,
        is_pending: bool = False,
        error_type: str | None = None,
    ):
        """Print a formatted handler line with proper column alignment"""

        # Col 2: icon based on status
        if status == 'completed':
            col2_icon = '☑️'
        elif is_pending:
            col2_icon = '🔚'
        elif status == 'started' or is_interrupted:
            col2_icon = '➡️'
        elif is_expired:
            col2_icon = '⏰'
        elif status == 'error':
            col2_icon = '❌'
        else:
            col2_icon = '🔜'

        # Col 3: handler description
        col3_desc = f'{handler_name}(#{event_id_suffix})'

        # Col 4: padding to column 64
        left_part = f'{handler_indent}{col2_icon} {col3_desc}'
        col4_padding = ' ' * max(1, 64 - len(f'{handler_indent}   {col3_desc}'))  # assume icons are always 2 chars wide

        # Col 5-10: timing info
        max_time = timeout or 0
        if started_at:
            started_at_dt = datetime.fromisoformat(started_at)
            completed_at_dt = datetime.fromisoformat(completed_at) if completed_at else now
            elapsed_time = (completed_at_dt - started_at_dt).total_seconds()

            if is_expired or (elapsed_time >= max_time):
                col5_timing_icon = '⌛️'
                if is_expired:
                    col9_extra = f' ⬅️ {red}TIMEOUT HERE{reset} ⏰'
                else:
                    col9_extra = f' ☠️ {pink}{error_type or "FAILED"}{reset}'  # timed out before us, but unrelated to current timeout exception chain, not the direct cause of our current error
            elif is_interrupted and is_pending:
                col5_timing_icon = '  '
                col9_extra = f' ⛔️ {pink}{error_type or "AbortedError"}{reset}'
            elif is_interrupted:
                col5_timing_icon = '⏳'
                col9_extra = f' ⬅️ {yellow}INTERRUPTED{reset} ✂️'
            elif status == 'started':
                col5_timing_icon = '⏳'
                col9_extra = ''
            else:
                col5_timing_icon = '  '
                col9_extra = ' ✓' if status == 'completed' else '    ✗'

            if elapsed_time >= max_time and not is_pending:
                col6_elapsed = f'{red}{round(elapsed_time):2d}s{reset}'
            elif elapsed_time > 3:
                col6_elapsed = f'{yellow}{round(elapsed_time):2d}s{reset}'
            elif status == 'completed':
                col6_elapsed = f'{green}{round(elapsed_time):2d}s{reset}'
            elif is_pending:
                col6_elapsed = '   '
            elif is_interrupted or (status == 'error' and elapsed_time <= max_time):
                col6_elapsed = f'{yellow}{round(elapsed_time):2d}s{reset}'
            else:
                col6_elapsed = f'{round(elapsed_time):2d}s'

            col7_slash = '/'
            if is_expired or elapsed_time >= max_time:
                col8_max = f'{red}{math.ceil(timeout or 0):2d}s{reset}'
            else:
                col8_max = f'{math.ceil(timeout or 0):2d}s'
        else:
            # Never started - pending
            col5_timing_icon = '🔜'
            col6_elapsed = '   '
            col7_slash = '/'
            col8_max = f'{math.ceil(timeout or 0):2d}s'
            col9_extra = ''

        # Assemble and print
        logger.warning(f'{left_part}{col4_padding}{col5_timing_icon} {col6_elapsed}{col7_slash}{col8_max}  {col9_extra}')

    def print_event_tree(evt: 'BaseEvent[Any]', indent: str = ''):
        """Recursively print event and its handlers"""
        event_start_time = (
            min(
                (result.started_at for result in evt.event_results.values() if result.started_at is not None),
                default=evt.event_created_at,
            )
            or evt.event_created_at
        )
        now = datetime.now(UTC)
        event_start_time_dt = datetime.fromisoformat(event_start_time)
        elapsed = round((now - event_start_time_dt).total_seconds())

        # Event line formatted with proper columns
        # Col 1: indent, Col 2: icon (📣), Col 3: description
        col1_indent = indent
        col2_icon = '📣'
        col3_desc = f'{evt.event_type}#{evt.event_id[-4:]}'

        # Col 4: padding to column 70
        left_part = f'{col1_indent}{col2_icon} {col3_desc}'
        col4_padding = ' ' * max(1, 64 - len(f'{col1_indent}   {col3_desc}'))

        # Col 5-9: timing info
        col5_timing_icon = '   '  # No icon for event lines
        if elapsed >= ((evt.event_timeout or 1) * len(evt.event_results)):
            col6_elapsed = f'{red}{elapsed:2d}s{reset}'
        elif elapsed > 5 or evt.event_status == 'started':
            col6_elapsed = f'{yellow}{elapsed:2d}s{reset}'
        elif evt.event_status == 'completed' and all(result.error is None for result in evt.event_results.values()):
            col6_elapsed = f'{green}{elapsed:2d}s{reset}'
        else:
            col6_elapsed = f'{elapsed:2d}s'

        # col7_slash = '/'
        # col8_max = f'{round(evt.event_timeout or 1) * len(evt.event_results):2d}s'
        # if evt.event_status == 'completed':
        #     col8_max = f'{green}{col8_max}{reset}'
        # elif evt.event_status == 'error':
        #     col8_max = f'{red}{col8_max}{reset}'
        # elif evt.event_status == 'started':
        #     col8_max = f'{yellow}{col8_max}{reset}'
        # else:
        #     col8_max = f'{col8_max}{reset}'

        # Assemble and print
        logger.warning(f'{left_part}{col4_padding}{col5_timing_icon}    {col6_elapsed}')

        # Increase indent for handlers (3 spaces to align under event name)
        handler_indent = indent + '   '

        # Get all handlers for this event
        for result in evt.event_results.values():
            # Check if this is the exact handler that timed out
            is_expired = result.handler_id == timed_out_result.handler_id

            # Check if this handler was interrupted (started but not completed, in a child of the timed-out handler)
            is_interrupted = False
            if result.status == 'error' and isinstance(result.error, asyncio.CancelledError):
                # Check if this result is in a child event of the timed-out handler
                for timed_out_child in timed_out_result.event_children:
                    if evt.event_id == timed_out_child.event_id:
                        is_interrupted = True
                        break

            # Print the handler line using helper function
            print_handler_line(
                handler_indent=handler_indent,
                handler_name=result.handler_name,
                event_id_suffix=result.event_id[-4:],
                status=result.status,
                started_at=result.started_at,
                completed_at=result.completed_at,
                timeout=result.timeout or evt.event_timeout,
                is_expired=is_expired,
                is_interrupted=is_interrupted,
                is_pending='pending' in str(result.error),
                error_type=type(result.error).__name__ if result.error else None,
            )

            # Print child events dispatched by this handler
            for child_event in result.event_children:
                print_event_tree(child_event, handler_indent + '   ')

        # After showing all handlers that ran, show any registered handlers that never started
        # This is for handlers that were registered but didn't get to run due to timeouts
        # Find which EventBus contains this event
        event_bus = None
        for bus in list(EventBus.all_instances):
            if evt.event_id in bus.event_history:
                event_bus = bus
                break

        # Get all registered handlers that could match this event_type.
        if event_bus is not None:
            indexed_ids = list(event_bus.handlers_by_key.get(evt.event_type, [])) + list(event_bus.handlers_by_key.get('*', []))

            for handler_id in indexed_ids:
                entry = event_bus.handlers.get(handler_id)
                if entry is None:
                    continue
                # Check if this handler already ran (has an EventResult)
                if handler_id not in evt.event_results:
                    # This handler was registered but never started - use helper to format
                    print_handler_line(
                        handler_indent=handler_indent,
                        handler_name=entry.handler_name,
                        event_id_suffix=evt.event_id[-4:],
                        status='pending',  # Will show 🔲 icon
                        started_at=None,
                        completed_at=None,
                        timeout=evt.event_timeout,
                        is_expired=False,
                        is_interrupted=False,
                    )

    # Print the tree starting from root
    print_event_tree(root_event)

    logger.warning('\n' + '=' * 80 + '\n')
