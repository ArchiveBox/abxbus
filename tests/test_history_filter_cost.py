import asyncio
import cProfile

from abxbus import BaseEvent, EventBus


class CaptureOutputEvent(BaseEvent[int]):
    sequence: int


class CaptureFinishedEvent(BaseEvent[str]):
    filename: str


async def test_typed_history_queries_do_not_pay_full_matcher_cost_for_unrelated_output():
    """Unrelated retained output must not add a Python call per queried entry."""
    bus = EventBus(max_history_size=10_001, middlewares=[])
    received: list[int] = []

    async def consume_output(event: CaptureOutputEvent) -> int:
        received.append(event.sequence)
        return event.sequence

    async def finish_capture(event: CaptureFinishedEvent) -> str:
        return event.filename

    bus.on(CaptureOutputEvent, consume_output)
    bus.on(CaptureFinishedEvent, finish_capture)
    try:
        finished = await bus.emit(CaptureFinishedEvent(filename='capture.txt')).now()
        for batch in range(100):
            events = [bus.emit(CaptureOutputEvent(sequence=batch * 100 + i)) for i in range(100)]
            await asyncio.gather(*(event.now() for event in events))
        assert received == list(range(10_000))
        assert await finished.event_result() == 'capture.txt'
        assert len(bus.event_history) == 10_001

        # Count Python calls instead of imposing machine-dependent wall-clock
        # limits. Coverage traces the library but not this test, so comparing
        # its runtime to a test-local loop would measure instrumentation cost.
        profiler = cProfile.Profile()
        with profiler:
            assert await bus.filter(CaptureFinishedEvent) == [finished]
        python_calls = sum(entry.callcount for entry in profiler.getstats() if not isinstance(entry.code, str))
        assert python_calls < 100, python_calls
    finally:
        await bus.destroy(clear=True)


async def test_typed_history_filter_observes_mutations_during_predicate():
    """Type/field selection stays live even though membership is snapshotted."""
    bus = EventBus()
    try:
        older = await bus.emit(CaptureFinishedEvent(filename='old.txt')).now()
        newer = await bus.emit(CaptureFinishedEvent(filename='new.txt')).now()
        older.event_type = 'CaptureOutputEvent'

        def select(event: CaptureFinishedEvent) -> bool:
            if event is newer:
                older.event_type = 'CaptureFinishedEvent'
                older.filename = 'updated.txt'
                bus.event_history.remove_event(older.event_id)
            return event.filename.endswith('.txt')

        assert await bus.filter(CaptureFinishedEvent, where=select) == [newer, older]
        assert older.filename == 'updated.txt'
        assert await bus.filter(CaptureFinishedEvent) == [newer]
    finally:
        await bus.destroy(clear=True)
