import asyncio
from typing import Any

import pytest

from abxbus import BaseEvent, EventBus, EventHandlerCompletionMode, EventHandlerConcurrencyMode, EventResult, EventStatus


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
        assert event.event_handler_completion is None
        assert second_handler_called is False

        result = await event.event_result(raise_if_any=False, raise_if_none=False)
        assert result == 'first'

        first_result = next(result for result in event.event_results.values() if result.handler_name.endswith('first_handler'))
        second_result = next(result for result in event.event_results.values() if result.handler_name.endswith('second_handler'))
        assert first_result.status == 'completed'
        assert second_result.status == 'error'
        assert isinstance(second_result.error, asyncio.CancelledError)
    finally:
        await bus.destroy()


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
        await bus.destroy()


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
        await bus.destroy()


async def test_event_handler_completion_explicit_first_cancels_parallel_losers() -> None:
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
        event = bus.emit(CompletionEvent(event_handler_completion=EventHandlerCompletionMode.FIRST))
        assert event.event_handler_completion == EventHandlerCompletionMode.FIRST

        completed_event = await event.now(first_result=True)
        first_value = await completed_event.event_result(raise_if_any=False)

        assert first_value == 'fast'
        assert event.event_handler_completion == EventHandlerCompletionMode.FIRST
        await event.wait(timeout=1.0)
        assert slow_handler_completed is False

        error_results = [result for result in event.event_results.values() if result.status == 'error']
        assert error_results
        assert any(isinstance(result.error, asyncio.CancelledError) for result in error_results)
    finally:
        await bus.destroy()


async def test_event_handler_completion_first_preserves_falsy_results() -> None:
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
        event = bus.emit(IntCompletionEvent(event_handler_completion=EventHandlerCompletionMode.FIRST))
        result = await (await event.now(first_result=True)).event_result(raise_if_any=False)
        assert result == 0
        assert second_handler_called is False
    finally:
        await bus.destroy()


async def test_event_handler_completion_first_preserves_false_and_empty_string_results() -> None:
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
        bool_event = bool_bus.emit(BoolCompletionEvent(event_handler_completion=EventHandlerCompletionMode.FIRST))
        bool_result = await (await bool_event.now(first_result=True)).event_result(raise_if_any=False)
        assert bool_result is False
        assert bool_second_handler_called is False
    finally:
        await bool_bus.destroy()

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
        str_event = str_bus.emit(StrCompletionEvent(event_handler_completion=EventHandlerCompletionMode.FIRST))
        str_result = await (await str_event.now(first_result=True)).event_result(raise_if_any=False)
        assert str_result == ''
        assert str_second_handler_called is False
    finally:
        await str_bus.destroy()


async def test_event_handler_completion_first_skips_none_result_and_uses_next_winner() -> None:
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
        event = bus.emit(CompletionEvent(event_handler_completion=EventHandlerCompletionMode.FIRST))
        result = await (await event.now(first_result=True)).event_result(raise_if_any=False)
        assert result == 'winner'
        assert third_handler_called is False

        none_result = next(result for result in event.event_results.values() if result.handler_name.endswith('none_handler'))
        winner_result = next(result for result in event.event_results.values() if result.handler_name.endswith('winner_handler'))
        assert none_result.status == 'completed'
        assert none_result.result is None
        assert winner_result.status == 'completed'
        assert winner_result.result == 'winner'
    finally:
        await bus.destroy()


async def test_event_handler_completion_first_skips_baseevent_result_and_uses_next_winner() -> None:
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
        event = bus.emit(CompletionEvent(event_handler_completion=EventHandlerCompletionMode.FIRST))
        result = await (await event.now(first_result=True)).event_result(raise_if_any=False)
        assert result == 'winner'
        assert third_handler_called is False

        def include_completed_values(result: Any, event_result: EventResult[Any]) -> bool:
            assert result == event_result.result or result is None
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
            handler_id: result.result
            for handler_id, result in event.event_results.items()
            if include_completed_values(result.result, result)
        }
        assert any(value == 'winner' for value in values_by_handler_id.values())
        assert any(isinstance(value, ChildCompletionEvent) for value in values_by_handler_id.values())

        values_by_handler_name = {
            result.handler_name: result.result
            for result in event.event_results.values()
            if include_completed_values(result.result, result)
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
        await bus.destroy()


async def test_now_runs_all_handlers_and_event_result_returns_first_valid_result() -> None:
    bus = EventBus(
        name='CompletionNowAllBus',
        event_handler_concurrency=EventHandlerConcurrencyMode.SERIAL,
        event_handler_completion=EventHandlerCompletionMode.FIRST,
    )
    late_handler_called = False

    async def baseevent_handler(_event: CompletionEvent) -> ChildCompletionEvent:
        return ChildCompletionEvent()

    async def none_handler(_event: CompletionEvent) -> None:
        return None

    async def winner_handler(_event: CompletionEvent) -> str:
        return 'winner'

    async def late_handler(_event: CompletionEvent) -> str:
        nonlocal late_handler_called
        late_handler_called = True
        return 'late'

    bus.on(CompletionEvent, baseevent_handler)
    bus.on(CompletionEvent, none_handler)
    bus.on(CompletionEvent, winner_handler)
    bus.on(CompletionEvent, late_handler)

    try:
        event = await bus.emit(CompletionEvent(event_handler_completion=EventHandlerCompletionMode.ALL)).now()
        result = await event.event_result(raise_if_any=False)

        assert result == 'winner'
        assert event.event_handler_completion == EventHandlerCompletionMode.ALL
        assert late_handler_called is True
    finally:
        await bus.destroy()


async def test_event_now_default_error_policy() -> None:
    no_handler_bus = EventBus(name='CompletionNowNoHandlerBus')
    try:
        event = await no_handler_bus.emit(CompletionEvent()).now()
        assert await event.event_result(raise_if_any=False, raise_if_none=False) is None
    finally:
        await no_handler_bus.destroy()

    none_bus = EventBus(name='CompletionNowNoneBus')

    async def none_handler(_event: CompletionEvent) -> None:
        return None

    none_bus.on(CompletionEvent, none_handler)
    try:
        event = await none_bus.emit(CompletionEvent()).now()
        assert await event.event_result(raise_if_any=False, raise_if_none=False) is None
    finally:
        await none_bus.destroy()

    all_error_bus = EventBus(name='CompletionNowAllErrorBus')

    async def fail_one(_event: CompletionEvent) -> str:
        raise RuntimeError('now boom 1')

    async def fail_two(_event: CompletionEvent) -> str:
        raise RuntimeError('now boom 2')

    all_error_bus.on(CompletionEvent, fail_one)
    all_error_bus.on(CompletionEvent, fail_two)
    try:
        with pytest.raises(ExceptionGroup, match='2 handler error'):
            event = await all_error_bus.emit(CompletionEvent()).now()
            await event.event_result()
    finally:
        await all_error_bus.destroy()

    mixed_valid_bus = EventBus(name='CompletionNowMixedValidBus')
    mixed_valid_bus.on(CompletionEvent, fail_one)

    async def winner(_event: CompletionEvent) -> str:
        return 'winner'

    mixed_valid_bus.on(CompletionEvent, winner)
    try:
        event = await mixed_valid_bus.emit(CompletionEvent()).now()
        assert await event.event_result(raise_if_any=False) == 'winner'
    finally:
        await mixed_valid_bus.destroy()

    mixed_none_bus = EventBus(name='CompletionNowMixedNoneBus')
    mixed_none_bus.on(CompletionEvent, fail_one)
    mixed_none_bus.on(CompletionEvent, none_handler)
    try:
        event = await mixed_none_bus.emit(CompletionEvent()).now()
        assert await event.event_result(raise_if_any=False, raise_if_none=False) is None
    finally:
        await mixed_none_bus.destroy()


async def test_event_result_options_match_event_results_shape() -> None:
    bus = EventBus(name='CompletionNowOptionsBus', event_handler_concurrency='serial')

    async def fail_handler(_event: CompletionEvent) -> str:
        raise RuntimeError('now option boom')

    async def first_valid(_event: CompletionEvent) -> str:
        return 'first'

    async def second_valid(_event: CompletionEvent) -> str:
        return 'second'

    bus.on(CompletionEvent, fail_handler)
    bus.on(CompletionEvent, first_valid)
    bus.on(CompletionEvent, second_valid)

    try:
        with pytest.raises(RuntimeError, match='now option boom'):
            event = await bus.emit(CompletionEvent()).now()
            await event.event_result(raise_if_any=True)

        def include_second(result: str | None, event_result: EventResult[Any]) -> bool:
            assert result == event_result.result
            return event_result.status == 'completed' and event_result.result == 'second'

        event = await bus.emit(CompletionEvent()).now()
        assert await event.event_result(include=include_second, raise_if_any=False) == 'second'
    finally:
        await bus.destroy()


async def test_event_result_returns_first_valid_result_by_registration_order_after_now() -> None:
    bus = EventBus(name='CompletionNowRegistrationOrderBus', event_handler_concurrency='parallel')

    async def slow_handler(_event: CompletionEvent) -> str:
        await asyncio.sleep(0.05)
        return 'slow'

    async def fast_handler(_event: CompletionEvent) -> str:
        await asyncio.sleep(0.001)
        return 'fast'

    bus.on(CompletionEvent, slow_handler)
    bus.on(CompletionEvent, fast_handler)

    try:
        event = await bus.emit(CompletionEvent()).now()
        assert await event.event_result() == 'slow'
    finally:
        await bus.destroy()


async def test_event_handler_completion_first_returns_none_when_all_handlers_fail() -> None:
    bus = EventBus(name='CompletionAllFailBus', event_handler_concurrency='parallel')

    async def fail_fast(_event: CompletionEvent) -> str:
        raise RuntimeError('boom1')

    async def fail_slow(_event: CompletionEvent) -> str:
        await asyncio.sleep(0.01)
        raise RuntimeError('boom2')

    bus.on(CompletionEvent, fail_fast)
    bus.on(CompletionEvent, fail_slow)

    try:
        event = await bus.emit(CompletionEvent(event_handler_completion=EventHandlerCompletionMode.FIRST)).now(first_result=True)
        result = await event.event_result(raise_if_any=False, raise_if_none=False)
        assert result is None
    finally:
        await bus.destroy()


async def test_event_handler_completion_first_result_options_match_event_result_options() -> None:
    bus = EventBus(name='CompletionFirstOptionsBus', event_handler_concurrency='parallel')

    async def fail_fast(_event: CompletionEvent) -> str:
        raise RuntimeError('first option boom')

    async def slow_winner(_event: CompletionEvent) -> str:
        await asyncio.sleep(0.01)
        return 'winner'

    bus.on(CompletionEvent, fail_fast)
    bus.on(CompletionEvent, slow_winner)

    try:
        event = await bus.emit(CompletionEvent(event_handler_completion=EventHandlerCompletionMode.FIRST)).now(first_result=True)
        assert await event.event_result(raise_if_any=False) == 'winner'
        with pytest.raises(RuntimeError, match='first option boom'):
            event = await bus.emit(CompletionEvent(event_handler_completion=EventHandlerCompletionMode.FIRST)).now(
                first_result=True
            )
            await event.event_result(raise_if_any=True)
    finally:
        await bus.destroy()

    none_bus = EventBus(name='CompletionFirstRaiseNoneBus')

    async def none_handler(_event: CompletionEvent) -> None:
        return None

    none_bus.on(CompletionEvent, none_handler)
    try:
        with pytest.raises(ValueError, match='Expected at least one handler'):
            event = await none_bus.emit(CompletionEvent(event_handler_completion=EventHandlerCompletionMode.FIRST)).now(
                first_result=True
            )
            await event.event_result(raise_if_none=True)
    finally:
        await none_bus.destroy()


async def test_now_first_result_timeout_limits_processing_wait() -> None:
    bus = EventBus(name='CompletionFirstTimeoutBus', event_handler_concurrency='serial')

    async def slow_handler(_event: CompletionEvent) -> str:
        await asyncio.sleep(0.5)
        return 'slow'

    bus.on(CompletionEvent, slow_handler)

    try:
        with pytest.raises(TimeoutError):
            await bus.emit(CompletionEvent(event_handler_completion=EventHandlerCompletionMode.FIRST)).now(
                timeout=0.01,
                first_result=True,
            )
    finally:
        await bus.destroy()


async def test_event_result_include_callback_receives_result_and_event_result() -> None:
    bus = EventBus(name='CompletionFirstIncludeBus', event_handler_concurrency='serial')
    seen_handler_names: list[str | None] = []
    seen_results: list[str | None] = []

    async def none_handler(_event: CompletionEvent) -> None:
        return None

    async def second_handler(_event: CompletionEvent) -> str:
        return 'second'

    bus.on(CompletionEvent, none_handler)
    bus.on(CompletionEvent, second_handler)

    def include_second(result: str | None, event_result: EventResult[Any]) -> bool:
        assert result == event_result.result
        seen_results.append(result)
        seen_handler_names.append(event_result.handler_name)
        return event_result.status == 'completed' and result == 'second'

    try:
        event = await bus.emit(CompletionEvent(event_handler_completion=EventHandlerCompletionMode.FIRST)).now(first_result=True)
        assert await event.event_result(include=include_second, raise_if_any=False) == 'second'
        assert seen_results[-2:] == [None, 'second']
        assert [name.rsplit('.', 1)[-1] if name else name for name in seen_handler_names[-2:]] == [
            'none_handler',
            'second_handler',
        ]
    finally:
        await bus.destroy()


async def test_event_results_include_callback_receives_result_and_event_result() -> None:
    bus = EventBus(name='CompletionResultsListIncludeBus', event_handler_concurrency='serial')
    seen_pairs: list[tuple[str | None, str | None]] = []

    async def keep_handler(_event: CompletionEvent) -> str:
        return 'keep'

    async def drop_handler(_event: CompletionEvent) -> str:
        return 'drop'

    bus.on(CompletionEvent, keep_handler)
    bus.on(CompletionEvent, drop_handler)

    def include_keep(result: str | None, event_result: EventResult[Any]) -> bool:
        assert result == event_result.result
        seen_pairs.append((result, event_result.handler_name.rsplit('.', 1)[-1] if event_result.handler_name else None))
        return result == 'keep'

    try:
        event = await bus.emit(CompletionEvent()).now()
        assert await event.event_results_list(
            include=include_keep,
            raise_if_any=False,
            raise_if_none=True,
        ) == ['keep']
        assert seen_pairs == [('keep', 'keep_handler'), ('drop', 'drop_handler')]
    finally:
        await bus.destroy()


async def test_event_result_returns_first_current_result_with_first_result_wait() -> None:
    bus = EventBus(name='CompletionFirstCurrentResultBus', event_handler_concurrency='parallel')

    async def slow_handler(_event: CompletionEvent) -> str:
        await asyncio.sleep(0.05)
        return 'slow'

    async def fast_handler(_event: CompletionEvent) -> str:
        await asyncio.sleep(0.001)
        return 'fast'

    bus.on(CompletionEvent, slow_handler)
    bus.on(CompletionEvent, fast_handler)

    try:
        event = await bus.emit(CompletionEvent()).now(first_result=True)
        assert await event.event_result() == 'fast'
    finally:
        await bus.destroy()


async def test_event_result_raise_if_any_includes_first_mode_control_errors() -> None:
    bus = EventBus(
        name='CompletionFirstControlErrorBus',
        event_handler_concurrency=EventHandlerConcurrencyMode.PARALLEL,
        event_handler_completion=EventHandlerCompletionMode.ALL,
    )

    async def fast_handler(_event: CompletionEvent) -> str:
        await asyncio.sleep(0.01)
        return 'fast'

    async def slow_handler(_event: CompletionEvent) -> str:
        await asyncio.sleep(0.5)
        return 'slow'

    bus.on(CompletionEvent, fast_handler)
    bus.on(CompletionEvent, slow_handler)

    try:
        event = await bus.emit(CompletionEvent(event_handler_completion=EventHandlerCompletionMode.FIRST)).now(first_result=True)
        assert await event.event_result(raise_if_any=True) == 'fast'
        await event.wait(timeout=1.0)
        with pytest.raises(asyncio.CancelledError, match='first result resolved'):
            await event.event_result(raise_if_any=True)
        assert await event.event_result(raise_if_any=False, raise_if_none=True) == 'fast'
    finally:
        await bus.destroy()


async def test_result_shortcuts_reject_unattached_pending_event() -> None:
    wait_event = CompletionEvent()
    with pytest.raises(RuntimeError, match='no bus attached'):
        await wait_event.wait()
    assert wait_event.event_handler_completion is None

    result_event = CompletionEvent()
    with pytest.raises(RuntimeError, match='no bus attached'):
        await result_event.event_result()
    assert result_event.event_handler_completion is None

    now_event = CompletionEvent()
    with pytest.raises(RuntimeError, match='no bus attached'):
        await now_event.now()
    assert now_event.event_handler_completion is None


async def test_result_shortcuts_allow_completed_unattached_event() -> None:
    wait_event = CompletionEvent(event_status=EventStatus.COMPLETED)
    assert await wait_event.wait() is wait_event

    result_event = CompletionEvent(event_status=EventStatus.COMPLETED)
    assert await result_event.event_result(raise_if_none=False) is None

    now_event = CompletionEvent(event_status=EventStatus.COMPLETED)
    assert await now_event.now() is now_event
