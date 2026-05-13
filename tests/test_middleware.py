# pyright: basic
"""Consolidated middleware tests."""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import sqlite3
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from pydantic import Field

from abxbus import BaseEvent, EventBus, SQLiteHistoryMirrorMiddleware
from abxbus.middlewares import (
    AutoErrorEventMiddleware,
    AutoHandlerChangeEventMiddleware,
    AutoReturnEventMiddleware,
    BusHandlerRegisteredEvent,
    BusHandlerUnregisteredEvent,
    EventBusMiddleware,
    LoggerEventBusMiddleware,
    OtelTracingMiddleware,
    WALEventBusMiddleware,
)


class UserActionEvent(BaseEvent):
    """Test event model for user actions."""

    action: str
    user_id: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class TestWALPersistence:
    """Test automatic WAL persistence functionality"""

    async def test_wal_persistence_handler(self, tmp_path):
        """Test that events are automatically persisted to WAL file"""
        # Create event bus with WAL path
        wal_path = tmp_path / 'test_events.jsonl'
        bus = EventBus(name='TestBus', middlewares=[WALEventBusMiddleware(wal_path)])

        try:
            # Emit some events
            events = []
            for i in range(3):
                event = UserActionEvent(action=f'action_{i}', user_id=f'user_{i}')
                emitted_event = bus.emit(event)
                completed_event = await emitted_event
                events.append(completed_event)

            # Wait for processing
            await bus.wait_until_idle()

            # Check WAL file exists
            assert wal_path.exists()

            # Read and verify JSONL content
            lines = wal_path.read_text().strip().split('\n')
            assert len(lines) == 3

            # Parse each line as JSON
            for i, line in enumerate(lines):
                data = json.loads(line)
                assert data['action'] == f'action_{i}'
                assert data['user_id'] == f'user_{i}'
                assert data['event_type'] == 'UserActionEvent'
                assert isinstance(data['event_created_at'], str)
                datetime.fromisoformat(data['event_created_at'])

        finally:
            await bus.destroy()

    async def test_wal_persistence_creates_parent_dir(self, tmp_path):
        """Test that WAL persistence creates parent directories"""
        # Use a nested path that doesn't exist
        wal_path = tmp_path / 'nested' / 'dirs' / 'events.jsonl'
        assert not wal_path.parent.exists()

        # Create event bus
        bus = EventBus(name='TestBus', middlewares=[WALEventBusMiddleware(wal_path)])

        try:
            # Emit an event
            event = bus.emit(UserActionEvent(action='test', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
            await event

            # Wait for WAL persistence to complete
            await bus.wait_until_idle()

            # Parent directory should be created after event is processed
            assert wal_path.parent.exists()

            # Check file was created
            assert wal_path.exists()
        finally:
            await bus.destroy()

    async def test_wal_persistence_skips_incomplete_events(self, tmp_path):
        """Test that WAL persistence only writes completed events"""
        wal_path = tmp_path / 'incomplete_events.jsonl'
        bus = EventBus(name='TestBus', middlewares=[WALEventBusMiddleware(wal_path)])

        try:
            # Add a slow handler that will delay completion
            async def slow_handler(event: BaseEvent) -> str:
                await asyncio.sleep(0.1)
                return 'slow'

            bus.on('UserActionEvent', slow_handler)

            # Emit event without waiting
            event = bus.emit(UserActionEvent(action='test', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))

            # Check file doesn't exist yet (event not completed)
            assert not wal_path.exists()

            # Wait for completion
            event = await event
            await bus.wait_until_idle()

            # Now file should exist with completed event
            assert wal_path.exists()
            lines = wal_path.read_text().strip().split('\n')
            assert len(lines) == 1
            data = json.loads(lines[0])
            assert data['event_type'] == 'UserActionEvent'
            # The WAL should have been written after the event completed
            assert data['action'] == 'test'
            assert data['user_id'] == 'e692b6cb-ae63-773b-8557-3218f7ce5ced'

        finally:
            await bus.destroy()


class TestHandlerMiddleware:
    """Tests for the handler middleware pipeline."""

    async def test_middleware_constructor_auto_inits_classes_and_keeps_hook_order(self):
        calls: list[str] = []

        class ClassMiddleware(EventBusMiddleware):
            def __init__(self):
                calls.append('class:init')

            async def on_event_result_change(self, eventbus: EventBus, event: BaseEvent, event_result, status):
                if status == 'started':
                    calls.append('class:started')
                elif status == 'completed':
                    calls.append('class:completed')

        class InstanceMiddleware(EventBusMiddleware):
            async def on_event_result_change(self, eventbus: EventBus, event: BaseEvent, event_result, status):
                if status == 'started':
                    calls.append('instance:started')
                elif status == 'completed':
                    calls.append('instance:completed')

        instance_middleware = InstanceMiddleware()
        bus = EventBus(middlewares=[ClassMiddleware, instance_middleware])
        bus.on('UserActionEvent', lambda event: 'ok')

        try:
            completed = await bus.emit(UserActionEvent(action='test', user_id='d592b79f-4dd9-7d4d-88b1-0d0db7d84fcf'))
            await bus.wait_until_idle()

            assert isinstance(bus.middlewares[0], ClassMiddleware)
            assert bus.middlewares[1] is instance_middleware
            assert completed.event_results
            assert calls == [
                'class:init',
                'class:started',
                'instance:started',
                'class:completed',
                'instance:completed',
            ]
        finally:
            await bus.destroy()

    async def test_middleware_wraps_successful_handler(self):
        calls: list[tuple[str, str]] = []

        class TrackingMiddleware(EventBusMiddleware):
            def __init__(self, call_log: list[tuple[str, str]]):
                self.call_log = call_log

            async def on_event_result_change(self, eventbus: EventBus, event: BaseEvent, event_result, status):
                if status == 'started':
                    self.call_log.append(('before', event_result.status))
                elif status == 'completed':
                    self.call_log.append(('after', event_result.status))

        bus = EventBus(middlewares=[TrackingMiddleware(calls)])
        bus.on('UserActionEvent', lambda event: 'ok')

        try:
            completed = await bus.emit(UserActionEvent(action='test', user_id='d592b79f-4dd9-7d4d-88b1-0d0db7d84fcf'))
            await bus.wait_until_idle()

            assert completed.event_results
            result = next(iter(completed.event_results.values()))
            assert result.status == 'completed'
            assert result.result == 'ok'
            assert calls == [('before', 'started'), ('after', 'completed')]
        finally:
            await bus.destroy()

    async def test_middleware_observes_handler_errors(self):
        observations: list[tuple[str, str]] = []

        class ErrorMiddleware(EventBusMiddleware):
            def __init__(self, log: list[tuple[str, str]]):
                self.log = log

            async def on_event_result_change(self, eventbus: EventBus, event: BaseEvent, event_result, status):
                if status == 'started':
                    self.log.append(('before', event_result.status))
                elif status == 'completed' and event_result.error:
                    self.log.append(('error', type(event_result.error).__name__))

        async def failing_handler(event: BaseEvent) -> None:
            raise ValueError('boom')

        bus = EventBus(middlewares=[ErrorMiddleware(observations)])
        bus.on('UserActionEvent', failing_handler)

        try:
            event = await bus.emit(UserActionEvent(action='fail', user_id='16599da2-bf1d-7a5d-8e6e-ba01f216519a'))
            await bus.wait_until_idle()

            result = next(iter(event.event_results.values()))
            assert result.status == 'error'
            assert isinstance(result.error, ValueError)
            assert observations == [('before', 'started'), ('error', 'ValueError')]
        finally:
            await bus.destroy()

    async def test_middleware_hook_statuses_never_emit_error(self):
        observed_event_statuses: list[str] = []
        observed_result_hook_statuses: list[str] = []
        observed_result_runtime_statuses: list[str] = []

        class LifecycleMiddleware(EventBusMiddleware):
            async def on_event_change(self, eventbus: EventBus, event: BaseEvent, status):
                observed_event_statuses.append(str(status))

            async def on_event_result_change(self, eventbus: EventBus, event: BaseEvent, event_result, status):
                observed_result_hook_statuses.append(str(status))
                observed_result_runtime_statuses.append(event_result.status)

        async def failing_handler(event: BaseEvent) -> None:
            raise ValueError('boom')

        bus = EventBus(middlewares=[LifecycleMiddleware()], max_history_size=None)
        bus.on(UserActionEvent, failing_handler)

        try:
            event = await bus.emit(UserActionEvent(action='fail', user_id='2a312e4d-3035-7883-86b9-578ce47046b2'))
            await bus.wait_until_idle()

            result = next(iter(event.event_results.values()))
            assert result.status == 'error'
            assert isinstance(result.error, ValueError)

            assert observed_event_statuses == ['pending', 'started', 'completed']
            assert observed_result_hook_statuses == ['pending', 'started', 'completed']
            assert observed_result_runtime_statuses[-1] == 'error'
            assert 'error' not in observed_event_statuses
            assert 'error' not in observed_result_hook_statuses
        finally:
            await bus.destroy()

    async def test_middleware_event_status_order_is_deterministic_for_each_event(self):
        event_statuses_by_id: dict[str, list[str]] = {}

        class LifecycleMiddleware(EventBusMiddleware):
            async def on_event_change(self, eventbus: EventBus, event: BaseEvent, status):
                event_statuses_by_id.setdefault(event.event_id, []).append(str(status))

        async def handler(_event: UserActionEvent) -> str:
            await asyncio.sleep(0)
            return 'ok'

        bus = EventBus(middlewares=[LifecycleMiddleware()], max_history_size=None)
        bus.on(UserActionEvent, handler)

        batch_count = 5
        events_per_batch = 50
        try:
            for batch_index in range(batch_count):
                events = [
                    bus.emit(
                        UserActionEvent(
                            action='deterministic',
                            user_id=f'u-{batch_index}-{event_index}',
                        )
                    )
                    for event_index in range(events_per_batch)
                ]
                await asyncio.gather(*events)
                await bus.wait_until_idle()

                for event in events:
                    assert event_statuses_by_id[event.event_id] == ['pending', 'started', 'completed']

            assert len(event_statuses_by_id) == batch_count * events_per_batch
        finally:
            await bus.destroy()

    async def test_middleware_event_and_result_lifecycle_remains_monotonic_on_timeout(self):
        observed_event_statuses: list[str] = []
        observed_result_transitions: list[tuple[str, str, str]] = []

        class LifecycleMiddleware(EventBusMiddleware):
            async def on_event_change(self, eventbus: EventBus, event: BaseEvent, status):
                observed_event_statuses.append(str(status))

            async def on_event_result_change(self, eventbus: EventBus, event: BaseEvent, event_result, status):
                observed_result_transitions.append((event_result.handler_name, str(status), event_result.status))

        class TimeoutLifecycleEvent(BaseEvent[str]):
            event_timeout: float | None = 0.02

        async def slow_handler(_event: TimeoutLifecycleEvent) -> str:
            await asyncio.sleep(0.05)
            return 'slow'

        async def pending_handler(_event: TimeoutLifecycleEvent) -> str:
            return 'pending'

        bus = EventBus(middlewares=[LifecycleMiddleware()])
        bus.on(TimeoutLifecycleEvent, slow_handler)
        bus.on(TimeoutLifecycleEvent, pending_handler)

        try:
            await bus.emit(TimeoutLifecycleEvent())
            await bus.wait_until_idle()

            assert observed_event_statuses == ['pending', 'started', 'completed']

            slow_transitions = [entry for entry in observed_result_transitions if entry[0].endswith('slow_handler')]
            pending_transitions = [entry for entry in observed_result_transitions if entry[0].endswith('pending_handler')]

            assert [status for _, status, _ in slow_transitions] == ['pending', 'started', 'completed']
            assert [result_status for _, _, result_status in slow_transitions] == ['pending', 'started', 'error']

            assert [status for _, status, _ in pending_transitions] == ['pending', 'completed']
            assert [result_status for _, _, result_status in pending_transitions] == ['pending', 'error']
        finally:
            await bus.destroy()

    async def test_auto_error_event_middleware_emits_and_guards_recursion(self):
        seen: list[tuple[str, str]] = []
        bus = EventBus(middlewares=[AutoErrorEventMiddleware()])

        class UserActionEventErrorEvent(BaseEvent[None]):
            error_type: str

        async def fail_handler(event: BaseEvent) -> None:
            raise ValueError('boom')

        async def fail_auto(event: UserActionEventErrorEvent) -> None:
            raise RuntimeError('nested')

        async def on_auto_error_event(event: UserActionEventErrorEvent) -> None:
            seen.append((event.event_type, event.error_type))

        bus.on(UserActionEvent, fail_handler)
        bus.on(UserActionEventErrorEvent, on_auto_error_event)
        bus.on(UserActionEventErrorEvent, fail_auto)

        try:
            await bus.emit(UserActionEvent(action='fail', user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced'))
            await bus.wait_until_idle()
            assert seen == [('UserActionEventErrorEvent', 'ValueError')]
            assert await bus.find('UserActionEventErrorEventErrorEvent', past=True, future=False) is None
        finally:
            await bus.destroy()

    async def test_auto_return_event_middleware_emits_and_guards_recursion(self):
        seen: list[tuple[str, Any]] = []
        bus = EventBus(middlewares=[AutoReturnEventMiddleware()])

        class UserActionEventResultEvent(BaseEvent[None]):
            data: Any

        async def ok_handler(event: BaseEvent) -> int:
            return 123

        async def non_none_auto(event: UserActionEventResultEvent) -> str:
            return 'nested'

        async def on_auto_result_event(event: UserActionEventResultEvent) -> None:
            seen.append((event.event_type, event.data))

        bus.on(UserActionEvent, ok_handler)
        bus.on(UserActionEventResultEvent, on_auto_result_event)
        bus.on(UserActionEventResultEvent, non_none_auto)

        try:
            await bus.emit(UserActionEvent(action='ok', user_id='2a312e4d-3035-7883-86b9-578ce47046b2'))
            await bus.wait_until_idle()
            assert seen == [('UserActionEventResultEvent', 123)]
            assert await bus.find('UserActionEventResultEventResultEvent', past=True, future=False) is None
        finally:
            await bus.destroy()

    async def test_auto_return_event_middleware_skips_baseevent_returns(self):
        seen: list[tuple[str, Any]] = []
        bus = EventBus(middlewares=[AutoReturnEventMiddleware()])

        class UserActionEventResultEvent(BaseEvent[None]):
            data: Any

        class ReturnedEvent(BaseEvent):
            value: int

        async def returns_event(event: BaseEvent) -> ReturnedEvent:
            return ReturnedEvent(value=7)

        async def on_auto_result_event(event: UserActionEventResultEvent) -> None:
            seen.append((event.event_type, event.data))

        bus.on(UserActionEvent, returns_event)
        bus.on(UserActionEventResultEvent, on_auto_result_event)

        try:
            parent = await bus.emit(UserActionEvent(action='ok', user_id='6eb8a717-e19d-728b-8905-97f7e20c002e'))
            await bus.wait_until_idle()
            assert len(parent.event_results) == 1
            only_result = next(iter(parent.event_results.values()))
            assert isinstance(only_result.result, ReturnedEvent)
            assert seen == []
            assert await bus.find('UserActionEventResultEvent', past=True, future=False) is None
        finally:
            await bus.destroy()

    async def test_auto_handler_change_event_middleware_emits_registered_and_unregistered(self):
        registered: list[BusHandlerRegisteredEvent] = []
        unregistered: list[BusHandlerUnregisteredEvent] = []
        bus = EventBus(middlewares=[AutoHandlerChangeEventMiddleware()])

        def on_registered(event: BusHandlerRegisteredEvent) -> None:
            registered.append(event)

        def on_unregistered(event: BusHandlerUnregisteredEvent) -> None:
            unregistered.append(event)

        bus.on(BusHandlerRegisteredEvent, on_registered)
        bus.on(BusHandlerUnregisteredEvent, on_unregistered)

        async def target_handler(event: UserActionEvent) -> None:
            return None

        try:
            handler_entry = bus.on(UserActionEvent, target_handler)
            await bus.wait_until_idle()

            bus.off(UserActionEvent, handler_entry)
            await bus.wait_until_idle()

            matching_registered = [event for event in registered if event.handler.id == handler_entry.id]
            matching_unregistered = [event for event in unregistered if event.handler.id == handler_entry.id]
            assert matching_registered
            assert matching_unregistered
            assert matching_registered[-1].handler.eventbus_id == bus.id
            assert matching_registered[-1].handler.eventbus_name == bus.name
            assert matching_registered[-1].handler.event_pattern == 'UserActionEvent'
            assert matching_unregistered[-1].handler.event_pattern == 'UserActionEvent'
        finally:
            await bus.destroy()

    async def test_otel_tracing_middleware_tracks_parent_event_and_handler_spans(self):
        class RootEvent(BaseEvent):
            pass

        class ChildEvent(BaseEvent):
            pass

        class FakeSpan:
            def __init__(self, name: str, context: Any = None, start_time: int | None = None):
                self.name = name
                self.context = context
                self.start_time = start_time
                self.end_time: int | None = None
                self.attrs: dict[str, Any] = {}
                self.errors: list[str] = []
                self.ended = False

            def set_attribute(self, key: str, value: Any):
                self.attrs[key] = value

            def record_exception(self, error: BaseException):
                self.errors.append(type(error).__name__)

            def end(self, end_time: int | None = None):
                self.end_time = end_time
                self.ended = True

        class FakeTracer:
            def __init__(self):
                self.spans: list[FakeSpan] = []

            def start_span(self, name: str, context: Any = None, start_time: int | None = None):
                span = FakeSpan(name, context=context, start_time=start_time)
                self.spans.append(span)
                return span

        class FakeTraceAPI:
            @staticmethod
            def set_span_in_context(span: FakeSpan):
                return {'parent': span}

        tracer = FakeTracer()
        bus = EventBus(middlewares=[OtelTracingMiddleware(tracer=tracer, trace_api=FakeTraceAPI())], name='TraceBus')

        async def child_handler(event: ChildEvent) -> None:
            return None

        async def root_handler(event: RootEvent) -> None:
            child = event.emit(ChildEvent())
            await child

        bus.on(RootEvent, root_handler)
        bus.on(ChildEvent, child_handler)

        try:
            await bus.emit(RootEvent())
            await bus.wait_until_idle()

            root_event_span = next(span for span in tracer.spans if span.attrs.get('abxbus.event_type') == 'RootEvent')
            root_handler_span = next(
                span for span in tracer.spans if str(span.attrs.get('abxbus.handler_name', '')).endswith('root_handler')
            )
            child_event_span = next(span for span in tracer.spans if span.attrs.get('abxbus.event_type') == 'ChildEvent')
            child_handler_span = next(
                span for span in tracer.spans if str(span.attrs.get('abxbus.handler_name', '')).endswith('child_handler')
            )

            assert [span.name for span in tracer.spans] == [
                'TraceBus.emit(RootEvent)',
                f'{root_handler_span.attrs["abxbus.handler_name"]}(RootEvent)',
                'TraceBus.emit(ChildEvent)',
                f'{child_handler_span.attrs["abxbus.handler_name"]}(ChildEvent)',
            ]
            assert root_event_span.context is None
            assert root_event_span.attrs.get('abxbus.trace.root') is True
            assert root_handler_span.context['parent'] is root_event_span
            assert child_event_span.context['parent'] is root_handler_span
            assert child_handler_span.context['parent'] is child_event_span
            assert root_event_span.attrs.get('abxbus.event_bus.name') == bus.name
            assert root_handler_span.attrs.get('abxbus.event_bus.name') == bus.name
            assert child_event_span.attrs.get('abxbus.event_bus.name') == bus.name
            assert child_handler_span.attrs.get('abxbus.event_bus.name') == bus.name
            assert child_event_span.attrs.get('abxbus.event_parent_id') == root_event_span.attrs.get('abxbus.event_id')
            assert child_event_span.attrs.get('abxbus.event_emitted_by_handler_id') == root_handler_span.attrs.get(
                'abxbus.handler_id'
            )
            assert all(span.ended for span in tracer.spans)
            assert all(span.start_time is not None for span in tracer.spans)
            assert all(span.end_time is not None for span in tracer.spans)
            assert all(span.end_time > span.start_time for span in tracer.spans if span.end_time and span.start_time)
        finally:
            await bus.destroy()


class TestSQLiteHistoryMirror:
    async def test_sqlite_history_persists_events_and_results(self, tmp_path):
        db_path = tmp_path / 'events.sqlite'
        middleware = SQLiteHistoryMirrorMiddleware(db_path)
        bus = EventBus(middlewares=[middleware])

        async def handler(event: BaseEvent) -> str:
            return 'ok'

        bus.on('UserActionEvent', handler)

        try:
            await bus.emit(UserActionEvent(action='ping', user_id='b57fcb67-faeb-7a56-8907-116d8cbb1472'))
            await bus.wait_until_idle()

            conn = sqlite3.connect(db_path)
            events = conn.execute('SELECT phase, event_status FROM events_log ORDER BY id').fetchall()
            assert [phase for phase, _ in events] == ['pending', 'started', 'completed']
            assert [status for _, status in events] == ['pending', 'started', 'completed']

            result_rows = conn.execute(
                'SELECT phase, status, result_repr, error_repr FROM event_results_log ORDER BY id'
            ).fetchall()
            conn.close()

            assert [phase for phase, *_ in result_rows] == ['pending', 'started', 'completed']
            assert [status for _, status, *_ in result_rows] == ['pending', 'started', 'completed']
            assert result_rows[-1][2] == "'ok'"
            assert result_rows[-1][3] is None
        finally:
            await bus.destroy()

    def test_sqlite_history_close_is_idempotent(self, tmp_path):
        db_path = tmp_path / 'events.sqlite'
        middleware = SQLiteHistoryMirrorMiddleware(db_path)

        middleware.close()
        middleware.close()

        with pytest.raises(sqlite3.ProgrammingError):
            middleware._conn.execute('SELECT 1')


class TestLoggerMiddleware:
    async def test_logger_middleware_writes_file(self, tmp_path):
        log_path = tmp_path / 'events.log'
        bus = EventBus(middlewares=[LoggerEventBusMiddleware(log_path)])

        async def handler(event: BaseEvent) -> str:
            return 'logged'

        bus.on('UserActionEvent', handler)

        try:
            await bus.emit(UserActionEvent(action='log', user_id='1d4087d7-e791-702f-80b9-0fb09b726bc6'))
            await bus.wait_until_idle()

            assert log_path.exists()
            contents = log_path.read_text().strip().splitlines()
            assert contents
            assert 'UserActionEvent' in contents[-1]
        finally:
            await bus.destroy()

    async def test_logger_middleware_stdout_only(self, capsys):
        bus = EventBus(middlewares=[LoggerEventBusMiddleware()])

        async def handler(event: BaseEvent) -> str:
            return 'stdout'

        bus.on('UserActionEvent', handler)

        try:
            await bus.emit(UserActionEvent(action='log', user_id='1d4087d7-e791-702f-80b9-0fb09b726bc6'))
            await bus.wait_until_idle()

            captured = capsys.readouterr()
            assert 'UserActionEvent' in captured.out
            assert 'stdout' not in captured.err
        finally:
            await bus.destroy()

    async def test_sqlite_history_records_errors(self, tmp_path):
        db_path = tmp_path / 'events.sqlite'
        middleware = SQLiteHistoryMirrorMiddleware(db_path)
        bus = EventBus(middlewares=[middleware])

        async def failing_handler(event: BaseEvent) -> None:
            raise RuntimeError('handler boom')

        bus.on('UserActionEvent', failing_handler)

        try:
            await bus.emit(UserActionEvent(action='boom', user_id='28536f9b-4031-7f53-827f-98c24c1b3839'))
            await bus.wait_until_idle()

            conn = sqlite3.connect(db_path)
            result_rows = conn.execute('SELECT phase, status, error_repr FROM event_results_log ORDER BY id').fetchall()
            events = conn.execute('SELECT phase, event_status FROM events_log ORDER BY id').fetchall()
            conn.close()

            assert [phase for phase, *_ in result_rows] == ['pending', 'started', 'completed']
            assert [status for _, status, *_ in result_rows] == ['pending', 'started', 'error']
            assert 'RuntimeError' in result_rows[-1][2]
            assert [phase for phase, _ in events] == ['pending', 'started', 'completed']
            assert [status for _, status in events] == ['pending', 'started', 'completed']
        finally:
            await bus.destroy()


class MiddlewarePatternEvent(BaseEvent[str]):
    pass


async def _flush_hook_tasks(ticks: int = 6) -> None:
    for _ in range(ticks):
        await asyncio.sleep(0)


async def test_middleware_hooks_cover_class_string_and_wildcard_patterns() -> None:
    event_statuses_by_id: dict[str, list[str]] = {}
    result_hook_statuses_by_handler: dict[str, list[str]] = {}
    result_runtime_statuses_by_handler: dict[str, list[str]] = {}
    handler_change_records: list[dict[str, Any]] = []

    class RecordingMiddleware(EventBusMiddleware):
        async def on_event_change(self, eventbus: EventBus, event: BaseEvent[Any], status) -> None:
            event_statuses_by_id.setdefault(event.event_id, []).append(str(status))

        async def on_event_result_change(self, eventbus: EventBus, event: BaseEvent[Any], event_result, status) -> None:
            handler_id = event_result.handler_id
            result_hook_statuses_by_handler.setdefault(handler_id, []).append(str(status))
            result_runtime_statuses_by_handler.setdefault(handler_id, []).append(event_result.status)

        async def on_bus_handlers_change(self, eventbus: EventBus, handler, registered: bool) -> None:
            handler_change_records.append(
                {
                    'handler_id': handler.id,
                    'event_pattern': handler.event_pattern,
                    'registered': registered,
                    'eventbus_id': handler.eventbus_id,
                }
            )

    bus = EventBus(name='MiddlewareHookPatternParityBus', middlewares=[RecordingMiddleware()])

    async def class_handler(event: MiddlewarePatternEvent) -> str:
        return 'class-result'

    async def string_handler(event: BaseEvent[Any]) -> str:
        assert event.event_type == 'MiddlewarePatternEvent'
        return 'string-result'

    async def wildcard_handler(event: BaseEvent[Any]) -> str:
        return f'wildcard:{event.event_type}'

    class_entry = bus.on(MiddlewarePatternEvent, class_handler)
    string_entry = bus.on('MiddlewarePatternEvent', string_handler)
    wildcard_entry = bus.on('*', wildcard_handler)

    try:
        await _flush_hook_tasks()

        registered_records = [record for record in handler_change_records if record['registered'] is True]
        assert len(registered_records) == 3

        expected_patterns = {
            class_entry.id: 'MiddlewarePatternEvent',
            string_entry.id: 'MiddlewarePatternEvent',
            wildcard_entry.id: '*',
        }
        assert {record['handler_id'] for record in registered_records} == set(expected_patterns)
        for record in registered_records:
            assert record['event_pattern'] == expected_patterns[record['handler_id']]
            assert record['eventbus_id'] == bus.id

        event = await bus.emit(MiddlewarePatternEvent(event_timeout=0.2))
        await bus.wait_until_idle()

        assert str(event.event_status) == 'completed'
        assert event_statuses_by_id[event.event_id] == ['pending', 'started', 'completed']
        assert set(event.event_results) == set(expected_patterns)

        for handler_id in expected_patterns:
            assert result_hook_statuses_by_handler[handler_id] == ['pending', 'started', 'completed']
            assert result_runtime_statuses_by_handler[handler_id] == ['pending', 'started', 'completed']

        assert event.event_results[class_entry.id].result == 'class-result'
        assert event.event_results[string_entry.id].result == 'string-result'
        assert event.event_results[wildcard_entry.id].result == 'wildcard:MiddlewarePatternEvent'

        bus.off(MiddlewarePatternEvent, class_entry)
        bus.off('MiddlewarePatternEvent', string_entry)
        bus.off('*', wildcard_entry)
        await _flush_hook_tasks()

        unregistered_records = [record for record in handler_change_records if record['registered'] is False]
        assert len(unregistered_records) == 3
        assert {record['handler_id'] for record in unregistered_records} == set(expected_patterns)
        for record in unregistered_records:
            assert record['event_pattern'] == expected_patterns[record['handler_id']]
    finally:
        await bus.destroy()


async def test_middleware_hooks_cover_string_and_wildcard_patterns_for_ad_hoc_baseevent() -> None:
    event_statuses_by_id: dict[str, list[str]] = {}
    result_hook_statuses_by_handler: dict[str, list[str]] = {}
    result_runtime_statuses_by_handler: dict[str, list[str]] = {}
    handler_change_records: list[dict[str, Any]] = []

    class RecordingMiddleware(EventBusMiddleware):
        async def on_event_change(self, eventbus: EventBus, event: BaseEvent[Any], status) -> None:
            event_statuses_by_id.setdefault(event.event_id, []).append(str(status))

        async def on_event_result_change(self, eventbus: EventBus, event: BaseEvent[Any], event_result, status) -> None:
            handler_id = event_result.handler_id
            result_hook_statuses_by_handler.setdefault(handler_id, []).append(str(status))
            result_runtime_statuses_by_handler.setdefault(handler_id, []).append(event_result.status)

        async def on_bus_handlers_change(self, eventbus: EventBus, handler, registered: bool) -> None:
            handler_change_records.append(
                {
                    'handler_id': handler.id,
                    'event_pattern': handler.event_pattern,
                    'registered': registered,
                    'eventbus_id': handler.eventbus_id,
                }
            )

    bus = EventBus(name='MiddlewareHookStringPatternParityBus', middlewares=[RecordingMiddleware()])
    ad_hoc_event_type = 'AdHocPatternEvent'

    async def string_handler(event: BaseEvent[Any]) -> str:
        assert event.event_type == ad_hoc_event_type
        return f'string:{event.event_type}'

    async def wildcard_handler(event: BaseEvent[Any]) -> str:
        return f'wildcard:{event.event_type}'

    string_entry = bus.on(ad_hoc_event_type, string_handler)
    wildcard_entry = bus.on('*', wildcard_handler)

    try:
        await _flush_hook_tasks()

        registered_records = [record for record in handler_change_records if record['registered'] is True]
        assert len(registered_records) == 2

        expected_patterns = {
            string_entry.id: ad_hoc_event_type,
            wildcard_entry.id: '*',
        }
        assert {record['handler_id'] for record in registered_records} == set(expected_patterns)
        for record in registered_records:
            assert record['event_pattern'] == expected_patterns[record['handler_id']]
            assert record['eventbus_id'] == bus.id

        event = await bus.emit(BaseEvent(event_type=ad_hoc_event_type, event_timeout=0.2))
        await bus.wait_until_idle()

        assert str(event.event_status) == 'completed'
        assert event_statuses_by_id[event.event_id] == ['pending', 'started', 'completed']
        assert set(event.event_results) == set(expected_patterns)

        for handler_id in expected_patterns:
            assert result_hook_statuses_by_handler[handler_id] == ['pending', 'started', 'completed']
            assert result_runtime_statuses_by_handler[handler_id] == ['pending', 'started', 'completed']

        assert event.event_results[string_entry.id].result == f'string:{ad_hoc_event_type}'
        assert event.event_results[wildcard_entry.id].result == f'wildcard:{ad_hoc_event_type}'

        bus.off(ad_hoc_event_type, string_entry)
        bus.off('*', wildcard_entry)
        await _flush_hook_tasks()

        unregistered_records = [record for record in handler_change_records if record['registered'] is False]
        assert len(unregistered_records) == 2
        assert {record['handler_id'] for record in unregistered_records} == set(expected_patterns)
        for record in unregistered_records:
            assert record['event_pattern'] == expected_patterns[record['handler_id']]
    finally:
        await bus.destroy()


class HistoryTestEvent(BaseEvent):
    """Event for verifying middleware mirroring behaviour."""

    payload: str
    should_fail: bool = False


def _summarize_history(history: dict[str, BaseEvent[Any]]) -> list[dict[str, Any]]:
    """Collect comparable information about events stored in history."""
    summary: list[dict[str, Any]] = []
    for event in history.values():
        handler_results = [
            {
                'handler_name': result.handler_name.rsplit('.', 1)[-1],
                'status': result.status,
                'result': result.result,
                'error': repr(result.error) if result.error else None,
            }
            for result in sorted(event.event_results.values(), key=lambda r: r.handler_name)
        ]
        summary.append(
            {
                'event_type': event.event_type,
                'event_status': event.event_status,
                'event_path_length': len(event.event_path),
                'children': sorted(child.event_type for child in event.event_children),
                'handler_results': handler_results,
            }
        )
    return sorted(summary, key=lambda record: record['event_type'])


async def _run_scenario(
    *,
    middlewares: Sequence[Any] = (),
    should_fail: bool = False,
) -> list[dict[str, Any]]:
    """Execute a simple scenario and return the history summary."""
    bus = EventBus(middlewares=list(middlewares))

    async def ok_handler(event: HistoryTestEvent) -> str:
        return f'ok-{event.payload}'

    async def conditional_handler(event: HistoryTestEvent) -> str:
        if event.should_fail:
            raise RuntimeError('boom')
        return 'fine'

    bus.on('HistoryTestEvent', ok_handler)
    bus.on('HistoryTestEvent', conditional_handler)

    try:
        await bus.emit(HistoryTestEvent(payload='payload', should_fail=should_fail))
        await bus.wait_until_idle()
    finally:
        summary = _summarize_history(bus.event_history)
        await bus.destroy()

    return summary


@pytest.mark.asyncio
async def test_sqlite_mirror_matches_inmemory_success(tmp_path: Path) -> None:
    db_path = tmp_path / 'events_success.sqlite'
    in_memory_result = await _run_scenario()
    sqlite_result = await _run_scenario(middlewares=[SQLiteHistoryMirrorMiddleware(db_path)])
    assert sqlite_result == in_memory_result

    conn = sqlite3.connect(db_path)
    event_phases = conn.execute('SELECT phase FROM events_log ORDER BY id').fetchall()
    conn.close()
    assert {phase for (phase,) in event_phases} >= {'pending', 'started', 'completed'}


@pytest.mark.asyncio
async def test_sqlite_mirror_matches_inmemory_error(tmp_path: Path) -> None:
    db_path = tmp_path / 'events_error.sqlite'
    in_memory_result = await _run_scenario(should_fail=True)
    sqlite_result = await _run_scenario(
        middlewares=[SQLiteHistoryMirrorMiddleware(db_path)],
        should_fail=True,
    )
    assert sqlite_result == in_memory_result

    conn = sqlite3.connect(db_path)
    phases = conn.execute('SELECT DISTINCT phase FROM events_log').fetchall()
    conn.close()
    assert {phase for (phase,) in phases} >= {'pending', 'started', 'completed'}


def _worker_dispatch(db_path: str, worker_id: int) -> None:
    """Process entrypoint for exercising concurrent writes."""

    async def run() -> None:
        middleware = SQLiteHistoryMirrorMiddleware(Path(db_path))
        bus = EventBus(name=f'WorkerBus{worker_id}', middlewares=[middleware])

        async def handler(event: HistoryTestEvent) -> str:
            return f'worker-{worker_id}'

        bus.on('HistoryTestEvent', handler)
        try:
            await bus.emit(HistoryTestEvent(payload=f'worker-{worker_id}'))
            await bus.wait_until_idle()
        finally:
            await bus.destroy()

    asyncio.run(run())


def test_sqlite_mirror_supports_concurrent_processes(tmp_path: Path) -> None:
    db_path = tmp_path / 'shared_history.sqlite'
    ctx = multiprocessing.get_context('spawn')
    processes = [ctx.Process(target=_worker_dispatch, args=(str(db_path), idx)) for idx in range(3)]
    for proc in processes:
        proc.start()
    for proc in processes:
        proc.join(timeout=20)
        assert proc.exitcode == 0

    conn = sqlite3.connect(db_path)
    events = conn.execute('SELECT DISTINCT eventbus_name FROM events_log').fetchall()
    results_count = conn.execute('SELECT COUNT(*) FROM event_results_log').fetchone()
    conn.close()

    bus_labels = {name for (name,) in events}
    assert len(bus_labels) == 3
    for idx in range(3):
        assert any(label.startswith(f'WorkerBus{idx}#') and len(label.rsplit('#', 1)[-1]) == 4 for label in bus_labels)
    assert results_count is not None
    # Each worker records pending/started/completed for its single handler
    assert results_count[0] == 9
