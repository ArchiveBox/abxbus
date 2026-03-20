"""Test the EventBus.log_tree() method"""

from typing import Any, Literal

from abxbus import BaseEvent, EventBus, EventHandler, EventResult
from abxbus.helpers import monotonic_datetime


class RootEvent(BaseEvent[str]):
    data: str = 'root'


class ChildEvent(BaseEvent[list[int]]):
    value: int = 42


class GrandchildEvent(BaseEvent[str]):
    nested: dict[str, int] = {'level': 3}


def _result_with_handler(
    *,
    bus: EventBus,
    event_id: str,
    handler_id: str,
    handler_name: str,
    status: Literal['pending', 'started', 'completed', 'error'],
    started_at: str | None = None,
    completed_at: str | None = None,
    result: Any = None,
    error: BaseException | None = None,
) -> EventResult[Any]:
    handler = EventHandler(
        id=handler_id,
        handler_name=handler_name,
        eventbus_id=bus.id,
        eventbus_name=bus.name,
        event_pattern='*',
    )
    return EventResult[Any](
        event_id=event_id,
        handler=handler,
        status=status,
        started_at=started_at,
        completed_at=completed_at,
        result=result,
        error=error,
    )


def test_log_history_tree_single_event(capsys: Any) -> None:
    """Test tree output with a single event"""
    bus = EventBus(name='SingleBus')

    # Create and add event to history
    event = RootEvent(data='test')
    event.event_completed_at = monotonic_datetime()
    bus.event_history[event.event_id] = event

    captured_str = bus.log_tree()

    # captured = capsys.readouterr()
    # captured_str = captured.out + captured.err
    assert '└──' in captured_str and 'RootEvent' in captured_str
    # Should show start time and duration
    assert '[' in captured_str and ']' in captured_str


def test_log_history_tree_with_handlers(capsys: Any) -> None:
    """Test tree output with event handlers and results"""
    bus = EventBus(name='HandlerBus')

    # Create event with handler results
    event = RootEvent(data='test')
    event.event_completed_at = monotonic_datetime()

    # Add handler result
    handler_id = '018f8e40-1234-7000-8000-000000000101'
    event.event_results[handler_id] = _result_with_handler(
        bus=bus,
        event_id=event.event_id,
        handler_id=handler_id,
        handler_name='test_handler',
        status='completed',
        started_at=monotonic_datetime(),
        completed_at=monotonic_datetime(),
        result='status: success',
    )

    bus.event_history[event.event_id] = event
    captured_str = bus.log_tree()

    assert '└── RootEvent#' in captured_str
    assert f'└── ✅ {bus.label}.test_handler#' in captured_str
    assert "'status: success'" in captured_str


def test_log_history_tree_with_errors(capsys: Any) -> None:
    """Test tree output with handler errors"""
    bus = EventBus(name='ErrorBus')

    event = RootEvent()
    event.event_completed_at = monotonic_datetime()

    # Add error result
    handler_id = '018f8e40-1234-7000-8000-000000000102'
    event.event_results[handler_id] = _result_with_handler(
        bus=bus,
        event_id=event.event_id,
        handler_id=handler_id,
        handler_name='error_handler',
        status='error',
        started_at=monotonic_datetime(),
        completed_at=monotonic_datetime(),
        error=ValueError('Test error message'),
    )

    bus.event_history[event.event_id] = event
    captured_str = bus.log_tree()

    assert f'{bus.label}.error_handler#' in captured_str
    assert 'ValueError: Test error message' in captured_str


def test_log_history_tree_complex_nested() -> None:
    """Test tree output with complex nested events"""
    bus = EventBus(name='ComplexBus')

    # Create root event
    root = RootEvent(data='root_data')
    root.event_completed_at = monotonic_datetime()

    # Add root handler with child events
    root_handler_id = '018f8e40-1234-7000-8000-000000000103'
    root.event_results[root_handler_id] = _result_with_handler(
        bus=bus,
        event_id=root.event_id,
        handler_id=root_handler_id,
        handler_name='root_handler',
        status='completed',
        started_at=monotonic_datetime(),
        completed_at=monotonic_datetime(),
        result='Root processed',
    )

    # Create child event
    child = ChildEvent(value=100)
    child.event_parent_id = root.event_id
    child.event_completed_at = monotonic_datetime()

    # Add child to root handler's event_children
    root.event_results[root_handler_id].event_children.append(child)

    # Add child handler with grandchild
    child_handler_id = '018f8e40-1234-7000-8000-000000000104'
    child.event_results[child_handler_id] = _result_with_handler(
        bus=bus,
        event_id=child.event_id,
        handler_id=child_handler_id,
        handler_name='child_handler',
        status='completed',
        started_at=monotonic_datetime(),
        completed_at=monotonic_datetime(),
        result=[1, 2, 3],
    )

    # Create grandchild
    grandchild = GrandchildEvent()
    grandchild.event_parent_id = child.event_id
    grandchild.event_completed_at = monotonic_datetime()

    # Add grandchild to child handler's event_children
    child.event_results[child_handler_id].event_children.append(grandchild)

    # Add grandchild handler
    grandchild_handler_id = '018f8e40-1234-7000-8000-000000000105'
    grandchild.event_results[grandchild_handler_id] = _result_with_handler(
        bus=bus,
        event_id=grandchild.event_id,
        handler_id=grandchild_handler_id,
        handler_name='grandchild_handler',
        status='completed',
        started_at=monotonic_datetime(),
        completed_at=monotonic_datetime(),
        result=None,
    )

    # Add all to history
    bus.event_history[root.event_id] = root
    bus.event_history[child.event_id] = child
    bus.event_history[grandchild.event_id] = grandchild

    output = bus.log_tree()

    # Check structure - note that events may appear both as handler children and in parent mapping
    assert '└── RootEvent#' in output
    assert f'✅ {bus.label}.root_handler#' in output
    assert 'ChildEvent#' in output
    assert f'✅ {bus.label}.child_handler#' in output
    assert 'GrandchildEvent#' in output
    assert f'✅ {bus.label}.grandchild_handler#' in output

    # Check result formatting
    assert "'Root processed'" in output
    assert 'list(3 items)' in output
    assert 'None' in output


def test_log_history_tree_multiple_roots(capsys: Any) -> None:
    """Test tree output with multiple root events"""
    bus = EventBus(name='MultiBus')

    # Create multiple root events
    root1 = RootEvent(data='first')
    root1.event_completed_at = monotonic_datetime()

    root2 = RootEvent(data='second')
    root2.event_completed_at = monotonic_datetime()

    bus.event_history[root1.event_id] = root1
    bus.event_history[root2.event_id] = root2

    captured_str = bus.log_tree()

    # Both roots should be shown
    assert captured_str.count('├── RootEvent#') == 1  # First root
    assert captured_str.count('└── RootEvent#') == 1  # Last root


def test_log_history_tree_timing_info(capsys: Any) -> None:
    """Test that timing information is displayed correctly"""
    bus = EventBus(name='TimingBus')

    event = RootEvent()
    event.event_completed_at = monotonic_datetime()

    # Add handler with timing
    start_time = monotonic_datetime()
    end_time = monotonic_datetime()

    handler_id = '018f8e40-1234-7000-8000-000000000106'
    event.event_results[handler_id] = _result_with_handler(
        bus=bus,
        event_id=event.event_id,
        handler_id=handler_id,
        handler_name='timed_handler',
        status='completed',
        started_at=start_time,
        completed_at=end_time,
        result='done',
    )

    bus.event_history[event.event_id] = event
    captured_str = bus.log_tree()

    # Should show timing with duration
    assert '(' in captured_str  # Opening parenthesis for duration
    assert 's)' in captured_str  # Duration in seconds with closing parenthesis


def test_log_history_tree_running_handler(capsys: Any) -> None:
    """Test tree output with handlers still running"""
    bus = EventBus(name='RunningBus')

    event = RootEvent()

    # Add running handler (started but not completed)
    handler_id = '018f8e40-1234-7000-8000-000000000107'
    event.event_results[handler_id] = _result_with_handler(
        bus=bus,
        event_id=event.event_id,
        handler_id=handler_id,
        handler_name='running_handler',
        status='started',
        started_at=monotonic_datetime(),
        completed_at=None,
    )

    bus.event_history[event.event_id] = event
    captured_str = bus.log_tree()

    assert f'{bus.label}.running_handler#' in captured_str
    assert 'RootEvent#' in captured_str  # Event should also show as running
