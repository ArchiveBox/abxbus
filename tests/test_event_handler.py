import asyncio
from typing import Any

from abxbus import BaseEvent, EventBus, EventHandlerCompletionMode, EventHandlerConcurrencyMode, EventResult


class CompletionEvent(BaseEvent[str]):
    pass


class IntCompletionEvent(BaseEvent[int]):
    pass


class ChildCompletionEvent(BaseEvent[str]):
    pass


class BoolCompletionEvent(BaseEvent[bool]):
    pass


class StrCompletionEvent(BaseEvent[str]):
    pass


async def test_event_handler_completion_bus_default_first_serial() -> None:
    bus = EventBus(
        name='CompletionDefaultFirstBus',
        event_handler_concurrency=EventHandlerConcurrencyMode.SERIAL,
        event_handler_completion=EventHandlerCompletionMode.FIRST,
    )
    second_handler_called = False

    async def first_handler(_event: CompletionEvent) -> str:
        return 'first'

    async def second_handler(_event: CompletionEvent) -> str:
        nonlocal second_handler_called
        second_handler_called = True
        return 'second'

    bus.on(CompletionEvent, first_handler)
    bus.on(CompletionEvent, second_handler)

    try:
        event = bus.emit(CompletionEvent())
        assert event.event_handler_completion is None

        await event
        assert second_handler_called is False

        result = await event.event_result(raise_if_any=False, raise_if_none=False)
        assert result == 'first'

        first_result = next(result for result in event.event_results.values() if result.handler_name.endswith('first_handler'))
        second_result = next(result for result in event.event_results.values() if result.handler_name.endswith('second_handler'))
        assert first_result.status == 'completed'
        assert second_result.status == 'error'
        assert isinstance(second_result.error, asyncio.CancelledError)
    finally:
        await bus.stop()


async def test_event_handler_completion_explicit_override_beats_bus_default() -> None:
    bus = EventBus(
        name='CompletionOverrideBus',
        event_handler_concurrency=EventHandlerConcurrencyMode.SERIAL,
        event_handler_completion=EventHandlerCompletionMode.FIRST,
    )
    second_handler_called = False

    async def first_handler(_event: CompletionEvent) -> str:
        return 'first'

    async def second_handler(_event: CompletionEvent) -> str:
        nonlocal second_handler_called
        second_handler_called = True
        return 'second'

    bus.on(CompletionEvent, first_handler)
    bus.on(CompletionEvent, second_handler)

    try:
        event = bus.emit(CompletionEvent(event_handler_completion=EventHandlerCompletionMode.ALL))
        assert event.event_handler_completion == EventHandlerCompletionMode.ALL
        await event
        assert second_handler_called is True
    finally:
        await bus.stop()


async def test_event_parallel_first_races_and_cancels_non_winners() -> None:
    bus = EventBus(
        name='CompletionParallelFirstBus',
        event_handler_concurrency=EventHandlerConcurrencyMode.SERIAL,
        event_handler_completion=EventHandlerCompletionMode.ALL,
    )
    slow_started = False

    async def slow_handler_started(_event: CompletionEvent) -> str:
        nonlocal slow_started
        slow_started = True
        await asyncio.sleep(0.5)
        return 'slow-started'

    async def fast_winner(_event: CompletionEvent) -> str:
        await asyncio.sleep(0.01)
        return 'winner'

    async def slow_handler_pending_or_started(_event: CompletionEvent) -> str:
        await asyncio.sleep(0.5)
        return 'slow-other'

    bus.on(CompletionEvent, slow_handler_started)
    bus.on(CompletionEvent, fast_winner)
    bus.on(CompletionEvent, slow_handler_pending_or_started)

    try:
        event = bus.emit(
            CompletionEvent(
                event_handler_concurrency=EventHandlerConcurrencyMode.PARALLEL,
                event_handler_completion=EventHandlerCompletionMode.FIRST,
            )
        )
        assert event.event_handler_concurrency == EventHandlerConcurrencyMode.PARALLEL
        assert event.event_handler_completion == EventHandlerCompletionMode.FIRST

        started = asyncio.get_running_loop().time()
        await event
        elapsed = asyncio.get_running_loop().time() - started
        assert elapsed < 0.2
        assert slow_started is True

        winner_result = next(result for result in event.event_results.values() if result.handler_name.endswith('fast_winner'))
        assert winner_result.status == 'completed'
        assert winner_result.error is None
        assert winner_result.result == 'winner'

        loser_results = [result for result in event.event_results.values() if not result.handler_name.endswith('fast_winner')]
        assert len(loser_results) == 2
        assert all(result.status == 'error' for result in loser_results)
        assert all(isinstance(result.error, asyncio.CancelledError) for result in loser_results)

        resolved = await event.event_result(raise_if_any=False, raise_if_none=True)
        assert resolved == 'winner'
    finally:
        await bus.stop()


async def test_event_first_shortcut_sets_mode_and_cancels_parallel_losers() -> None:
    bus = EventBus(
        name='CompletionFirstShortcutBus',
        event_handler_concurrency=EventHandlerConcurrencyMode.PARALLEL,
        event_handler_completion=EventHandlerCompletionMode.ALL,
    )
    slow_handler_completed = False

    async def fast_handler(_event: CompletionEvent) -> str:
        await asyncio.sleep(0.01)
        return 'fast'

    async def slow_handler(_event: CompletionEvent) -> str:
        nonlocal slow_handler_completed
        await asyncio.sleep(0.5)
        slow_handler_completed = True
        return 'slow'

    bus.on(CompletionEvent, fast_handler)
    bus.on(CompletionEvent, slow_handler)

    try:
        event = bus.emit(CompletionEvent())
        assert event.event_handler_completion is None

        first_value = await event.first()

        assert first_value == 'fast'
        assert event.event_handler_completion == EventHandlerCompletionMode.FIRST
        assert slow_handler_completed is False

        error_results = [result for result in event.event_results.values() if result.status == 'error']
        assert error_results
        assert any(isinstance(result.error, asyncio.CancelledError) for result in error_results)
    finally:
        await bus.stop()


async def test_event_first_preserves_falsy_results() -> None:
    bus = EventBus(
        name='CompletionFalsyBus',
        event_handler_concurrency=EventHandlerConcurrencyMode.SERIAL,
        event_handler_completion=EventHandlerCompletionMode.ALL,
    )
    second_handler_called = False

    async def zero_handler(_event: IntCompletionEvent) -> int:
        return 0

    async def second_handler(_event: IntCompletionEvent) -> int:
        nonlocal second_handler_called
        second_handler_called = True
        return 99

    bus.on(IntCompletionEvent, zero_handler)
    bus.on(IntCompletionEvent, second_handler)

    try:
        event = bus.emit(IntCompletionEvent())
        result = await event.first()
        assert result == 0
        assert second_handler_called is False
    finally:
        await bus.stop()


async def test_event_first_preserves_false_and_empty_string_results() -> None:
    bool_bus = EventBus(
        name='CompletionFalsyFalseBus',
        event_handler_concurrency=EventHandlerConcurrencyMode.SERIAL,
        event_handler_completion=EventHandlerCompletionMode.ALL,
    )
    bool_second_handler_called = False

    async def bool_first_handler(_event: BoolCompletionEvent) -> bool:
        return False

    async def bool_second_handler(_event: BoolCompletionEvent) -> bool:
        nonlocal bool_second_handler_called
        bool_second_handler_called = True
        return True

    bool_bus.on(BoolCompletionEvent, bool_first_handler)
    bool_bus.on(BoolCompletionEvent, bool_second_handler)

    try:
        bool_event = bool_bus.emit(BoolCompletionEvent())
        bool_result = await bool_event.first()
        assert bool_result is False
        assert bool_second_handler_called is False
    finally:
        await bool_bus.stop()

    str_bus = EventBus(
        name='CompletionFalsyEmptyStringBus',
        event_handler_concurrency=EventHandlerConcurrencyMode.SERIAL,
        event_handler_completion=EventHandlerCompletionMode.ALL,
    )
    str_second_handler_called = False

    async def str_first_handler(_event: StrCompletionEvent) -> str:
        return ''

    async def str_second_handler(_event: StrCompletionEvent) -> str:
        nonlocal str_second_handler_called
        str_second_handler_called = True
        return 'second'

    str_bus.on(StrCompletionEvent, str_first_handler)
    str_bus.on(StrCompletionEvent, str_second_handler)

    try:
        str_event = str_bus.emit(StrCompletionEvent())
        str_result = await str_event.first()
        assert str_result == ''
        assert str_second_handler_called is False
    finally:
        await str_bus.stop()


async def test_event_first_skips_none_result_and_uses_next_winner() -> None:
    bus = EventBus(
        name='CompletionNoneSkipBus',
        event_handler_concurrency=EventHandlerConcurrencyMode.SERIAL,
        event_handler_completion=EventHandlerCompletionMode.ALL,
    )
    third_handler_called = False

    async def none_handler(_event: CompletionEvent) -> None:
        return None

    async def winner_handler(_event: CompletionEvent) -> str:
        return 'winner'

    async def third_handler(_event: CompletionEvent) -> str:
        nonlocal third_handler_called
        third_handler_called = True
        return 'third'

    bus.on(CompletionEvent, none_handler)
    bus.on(CompletionEvent, winner_handler)
    bus.on(CompletionEvent, third_handler)

    try:
        event = bus.emit(CompletionEvent())
        result = await event.first()
        assert result == 'winner'
        assert third_handler_called is False

        none_result = next(result for result in event.event_results.values() if result.handler_name.endswith('none_handler'))
        winner_result = next(result for result in event.event_results.values() if result.handler_name.endswith('winner_handler'))
        assert none_result.status == 'completed'
        assert none_result.result is None
        assert winner_result.status == 'completed'
        assert winner_result.result == 'winner'
    finally:
        await bus.stop()


async def test_event_first_skips_baseevent_result_and_uses_next_winner() -> None:
    bus = EventBus(
        name='CompletionBaseEventSkipBus',
        event_handler_concurrency=EventHandlerConcurrencyMode.SERIAL,
        event_handler_completion=EventHandlerCompletionMode.ALL,
    )
    third_handler_called = False

    async def baseevent_handler(_event: CompletionEvent) -> ChildCompletionEvent:
        return ChildCompletionEvent()

    async def winner_handler(_event: CompletionEvent) -> str:
        return 'winner'

    async def third_handler(_event: CompletionEvent) -> str:
        nonlocal third_handler_called
        third_handler_called = True
        return 'third'

    bus.on(CompletionEvent, baseevent_handler)
    bus.on(CompletionEvent, winner_handler)
    bus.on(CompletionEvent, third_handler)

    try:
        event = bus.emit(CompletionEvent())
        result = await event.first()
        assert result == 'winner'
        assert third_handler_called is False

        def include_completed_values(event_result: EventResult[Any]) -> bool:
            return event_result.status == 'completed' and event_result.error is None and event_result.result is not None

        first_completed_value = await event.event_result(
            include=include_completed_values,
            raise_if_any=False,
            raise_if_none=True,
        )
        # Typed accessors normalize BaseEvent results to None.
        assert first_completed_value is None

        await event.event_results_list(
            include=include_completed_values,
            raise_if_any=False,
            raise_if_none=True,
        )
        values_by_handler_id = {
            handler_id: result.result for handler_id, result in event.event_results.items() if include_completed_values(result)
        }
        assert any(value == 'winner' for value in values_by_handler_id.values())
        assert any(isinstance(value, ChildCompletionEvent) for value in values_by_handler_id.values())

        values_by_handler_name = {
            result.handler_name: result.result for result in event.event_results.values() if include_completed_values(result)
        }
        assert any(value == 'winner' for value in values_by_handler_name.values())
        assert any(isinstance(value, ChildCompletionEvent) for value in values_by_handler_name.values())

        values_list = await event.event_results_list(
            include=include_completed_values,
            raise_if_any=False,
            raise_if_none=True,
        )
        assert 'winner' in values_list
        assert None in values_list

        # Raw event_results keep the underlying BaseEvent result.
        baseevent_result = next(
            result for result in event.event_results.values() if result.handler_name.endswith('baseevent_handler')
        )
        assert isinstance(baseevent_result.result, ChildCompletionEvent)
    finally:
        await bus.stop()


async def test_event_first_returns_none_when_all_handlers_fail() -> None:
    bus = EventBus(name='CompletionAllFailBus', event_handler_concurrency='parallel')

    async def fail_fast(_event: CompletionEvent) -> str:
        raise RuntimeError('boom1')

    async def fail_slow(_event: CompletionEvent) -> str:
        await asyncio.sleep(0.01)
        raise RuntimeError('boom2')

    bus.on(CompletionEvent, fail_fast)
    bus.on(CompletionEvent, fail_slow)

    try:
        event = bus.emit(CompletionEvent())
        result = await event.first()
        assert result is None
    finally:
        await bus.stop()


# Consolidated from tests/test_event_handler_concurrency.py


from abxbus import BaseEvent


class ConcurrencyEvent(BaseEvent[str]):
    pass


async def test_event_handler_concurrency_bus_default_remains_unset_on_dispatch() -> None:
    bus = EventBus(name='ConcurrencyDefaultBus', event_handler_concurrency=EventHandlerConcurrencyMode.PARALLEL)

    async def one_handler(_event: ConcurrencyEvent) -> str:
        return 'ok'

    bus.on(ConcurrencyEvent, one_handler)

    try:
        event = bus.emit(ConcurrencyEvent())
        assert event.event_handler_concurrency is None
        await event
    finally:
        await bus.stop()


async def test_event_handler_concurrency_per_event_override_controls_execution_mode() -> None:
    bus = EventBus(name='ConcurrencyPerEventBus', event_handler_concurrency=EventHandlerConcurrencyMode.PARALLEL)
    inflight_by_event_id: dict[str, int] = {}
    max_inflight_by_event_id: dict[str, int] = {}
    counter_lock = asyncio.Lock()

    async def track_concurrency(event: ConcurrencyEvent) -> None:
        event_id = event.event_id
        async with counter_lock:
            current_inflight = inflight_by_event_id.get(event_id, 0) + 1
            inflight_by_event_id[event_id] = current_inflight
            max_inflight_by_event_id[event_id] = max(max_inflight_by_event_id.get(event_id, 0), current_inflight)
        await asyncio.sleep(0.02)
        async with counter_lock:
            inflight_by_event_id[event_id] = max(inflight_by_event_id.get(event_id, 1) - 1, 0)

    async def handler_a(event: ConcurrencyEvent) -> str:
        await track_concurrency(event)
        return 'a'

    async def handler_b(event: ConcurrencyEvent) -> str:
        await track_concurrency(event)
        return 'b'

    bus.on(ConcurrencyEvent, handler_a)
    bus.on(ConcurrencyEvent, handler_b)

    try:
        serial_event = bus.emit(ConcurrencyEvent(event_handler_concurrency=EventHandlerConcurrencyMode.SERIAL))
        parallel_event = bus.emit(ConcurrencyEvent(event_handler_concurrency=EventHandlerConcurrencyMode.PARALLEL))
        assert serial_event.event_handler_concurrency == EventHandlerConcurrencyMode.SERIAL
        assert parallel_event.event_handler_concurrency == EventHandlerConcurrencyMode.PARALLEL

        await serial_event
        await parallel_event

        assert max_inflight_by_event_id.get(serial_event.event_id) == 1
        assert max_inflight_by_event_id.get(parallel_event.event_id, 0) >= 2
    finally:
        await bus.stop()
