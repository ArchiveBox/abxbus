import asyncio

from abxbus import (
    BaseEvent,
    EventBus,
    EventHandlerAbortedError,
    EventHandlerCancelledError,
    EventHandlerCompletionMode,
    EventHandlerConcurrencyMode,
    EventHandlerResultSchemaError,
    EventHandlerTimeoutError,
)


class TaxonomyEvent(BaseEvent[str]):
    pass


class IntTaxonomyEvent(BaseEvent[int]):
    pass


async def test_result_schema_mismatch_uses_event_handler_result_schema_error() -> None:
    bus = EventBus(name='TaxonomySchemaBus')

    def wrong_type(_event: IntTaxonomyEvent) -> str:
        return 'not-an-int'

    bus.on(IntTaxonomyEvent, wrong_type)

    try:
        event = await bus.emit(IntTaxonomyEvent())
        await bus.wait_until_idle()
        result = next(iter(event.event_results.values()))
        assert result.status == 'error'
        assert isinstance(result.error, EventHandlerResultSchemaError)
    finally:
        await bus.destroy()


async def test_handler_timeout_uses_event_handler_timeout_error() -> None:
    bus = EventBus(name='TaxonomyTimeoutBus')

    class TimeoutEvent(BaseEvent[str]):
        event_timeout: float | None = 0.2
        event_handler_timeout: float | None = 0.01

    async def slow_handler(_event: TimeoutEvent) -> str:
        await asyncio.sleep(0.05)
        return 'slow'

    bus.on(TimeoutEvent, slow_handler)

    try:
        event = await bus.emit(TimeoutEvent())
        await bus.wait_until_idle()
        result = next(iter(event.event_results.values()))
        assert result.status == 'error'
        assert isinstance(result.error, EventHandlerTimeoutError)
        assert isinstance(result.error, TimeoutError)
    finally:
        await bus.destroy()


async def test_first_mode_pending_non_winner_uses_cancelled_error_class() -> None:
    bus = EventBus(
        name='TaxonomyFirstPendingBus',
        event_handler_concurrency=EventHandlerConcurrencyMode.SERIAL,
        event_handler_completion=EventHandlerCompletionMode.FIRST,
    )

    async def winner(_event: TaxonomyEvent) -> str:
        return 'winner'

    async def never_runs(_event: TaxonomyEvent) -> str:
        await asyncio.sleep(0.1)
        return 'loser'

    bus.on(TaxonomyEvent, winner)
    bus.on(TaxonomyEvent, never_runs)

    try:
        event = await bus.emit(TaxonomyEvent())
        await bus.wait_until_idle()

        loser_result = next(result for result in event.event_results.values() if result.handler_name.endswith('never_runs'))
        assert loser_result.status == 'error'
        assert isinstance(loser_result.error, EventHandlerCancelledError)
        assert isinstance(loser_result.error, asyncio.CancelledError)
    finally:
        await bus.destroy()


async def test_parallel_first_started_loser_uses_aborted_error_class() -> None:
    bus = EventBus(
        name='TaxonomyFirstParallelBus',
        event_handler_concurrency=EventHandlerConcurrencyMode.PARALLEL,
        event_handler_completion=EventHandlerCompletionMode.FIRST,
    )
    slow_started = asyncio.Event()

    async def slow_loser(_event: TaxonomyEvent) -> str:
        slow_started.set()
        await asyncio.sleep(5)
        return 'slow'

    async def fast_winner(_event: TaxonomyEvent) -> str:
        await slow_started.wait()
        return 'winner'

    bus.on(TaxonomyEvent, slow_loser)
    bus.on(TaxonomyEvent, fast_winner)

    try:
        event = await bus.emit(TaxonomyEvent())
        await bus.wait_until_idle()

        slow_result = next(result for result in event.event_results.values() if result.handler_name.endswith('slow_loser'))
        assert slow_result.status == 'error'
        assert isinstance(slow_result.error, EventHandlerAbortedError)
        assert isinstance(slow_result.error, asyncio.CancelledError)
    finally:
        await bus.destroy()
