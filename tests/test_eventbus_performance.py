import asyncio
import functools
import gc
import inspect
import logging
import math
import os
import time
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any, Literal

import psutil
import pytest

import abxbus.base_event as base_event_module
import abxbus.event_bus as event_bus_module
from abxbus import BaseEvent, EventBus, EventHandlerAbortedError, EventHandlerCancelledError, EventHandlerTimeoutError

pytestmark = pytest.mark.timeout(120, method='thread')


@contextmanager
def suppress_abxbus_warning_logs() -> Any:
    """Reduce intentional timeout warning spam during stress scenarios."""

    abxbus_logger = logging.getLogger('abxbus')
    previous_level = abxbus_logger.level
    abxbus_logger.setLevel(logging.ERROR)
    try:
        yield
    finally:
        abxbus_logger.setLevel(previous_level)


def get_memory_usage_mb():
    """Get current process memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def percentile(values: list[float], q: float) -> float:
    """Simple percentile helper without numpy dependency."""
    if not values:
        return 0.0
    sorted_values = sorted(values)
    pos = (len(sorted_values) - 1) * q
    low = math.floor(pos)
    high = math.ceil(pos)
    if low == high:
        return sorted_values[int(pos)]
    return sorted_values[low] + (sorted_values[high] - sorted_values[low]) * (pos - low)


async def dispatch_and_measure(
    bus: EventBus,
    event_factory: Callable[[], BaseEvent[Any]],
    total_events: int,
    batch_size: int = 40,
) -> tuple[float, float, float, float, float]:
    """
    Dispatch many events and return:
    (throughput_events_per_sec, dispatch_p50_ms, dispatch_p95_ms, done_p50_ms, done_p95_ms)
    """
    dispatch_latencies_ms: list[float] = []
    done_latencies_ms: list[float] = []
    pending: list[tuple[BaseEvent[Any], float]] = []

    async def wait_one(item: tuple[BaseEvent[Any], float]) -> None:
        event, t_dispatch_done = item
        await event
        done_latencies_ms.append((time.perf_counter() - t_dispatch_done) * 1000)

    start = time.perf_counter()
    for _ in range(total_events):
        t0 = time.perf_counter()
        event = bus.emit(event_factory())
        dispatch_latencies_ms.append((time.perf_counter() - t0) * 1000)
        pending.append((event, time.perf_counter()))
        if len(pending) >= batch_size:
            await asyncio.gather(*(wait_one(item) for item in pending))
            pending.clear()

    if pending:
        await asyncio.gather(*(wait_one(item) for item in pending))
    await bus.wait_until_idle()

    elapsed = time.perf_counter() - start
    throughput = total_events / max(elapsed, 1e-9)
    return (
        throughput,
        percentile(dispatch_latencies_ms, 0.50),
        percentile(dispatch_latencies_ms, 0.95),
        percentile(done_latencies_ms, 0.50),
        percentile(done_latencies_ms, 0.95),
    )


async def run_mode_throughput_benchmark(
    *,
    event_handler_concurrency: Literal['serial', 'parallel'],
    total_events: int = 5_000,
    batch_size: int = 50,
) -> tuple[int, float]:
    """Run a basic no-op throughput benchmark for one handler mode."""
    bus = EventBus(
        name=f'ThroughputFloor_{event_handler_concurrency}',
        event_handler_concurrency=event_handler_concurrency,
        middlewares=[],
        max_history_drop=True,
    )

    processed = 0

    async def handler(event: SimpleEvent) -> None:
        nonlocal processed
        processed += 1

    bus.on(SimpleEvent, handler)

    pending: list[BaseEvent[Any]] = []
    start = time.time()
    try:
        for _ in range(total_events):
            pending.append(bus.emit(SimpleEvent()))
            if len(pending) >= batch_size:
                await asyncio.gather(*pending)
                pending.clear()

        if pending:
            await asyncio.gather(*pending)

        await bus.wait_until_idle()
    finally:
        await bus.stop(timeout=0, clear=True)

    duration = time.time() - start
    throughput = total_events / max(duration, 1e-9)
    return processed, throughput


async def run_io_fanout_benchmark(
    *,
    event_handler_concurrency: Literal['serial', 'parallel'],
    total_events: int = 800,
    handlers_per_event: int = 4,
    sleep_seconds: float = 0.0015,
    batch_size: int = 40,
) -> tuple[int, float]:
    """Benchmark I/O-bound fanout to compare serial vs parallel handler mode."""
    bus = EventBus(
        name=f'Fanout_{event_handler_concurrency}',
        event_handler_concurrency=event_handler_concurrency,
        middlewares=[],
        max_history_drop=True,
    )

    handled = 0

    for index in range(handlers_per_event):

        async def handler(event: SimpleEvent) -> None:
            nonlocal handled
            await asyncio.sleep(sleep_seconds)
            handled += 1

        handler.__name__ = f'fanout_handler_{index}'
        bus.on(SimpleEvent, handler)

    pending: list[BaseEvent[Any]] = []
    start = time.time()
    try:
        for _ in range(total_events):
            pending.append(bus.emit(SimpleEvent()))
            if len(pending) >= batch_size:
                await asyncio.gather(*pending)
                pending.clear()

        if pending:
            await asyncio.gather(*pending)

        await bus.wait_until_idle()
    finally:
        await bus.stop(timeout=0, clear=True)

    duration = time.time() - start
    return handled, duration


def throughput_floor_for_mode(event_handler_concurrency: Literal['serial', 'parallel']) -> int:
    """
    Conservative per-mode floor to catch severe regressions while avoiding CI flakiness.
    """
    if event_handler_concurrency == 'parallel':
        return 500
    return 600


def throughput_regression_floor(
    first_run_throughput: float,
    *,
    min_fraction: float,
    hard_floor: float,
) -> float:
    """
    Scenario+mode regression threshold using same-run baseline + absolute safety floor.
    """
    return max(hard_floor, first_run_throughput * min_fraction)


class MethodProfiler:
    """Lightweight monkeypatch profiler for selected class methods."""

    def __init__(self) -> None:
        self.stats: dict[str, dict[str, float]] = {}
        self._restore: list[tuple[type[Any], str, Any]] = []

    def instrument(
        self,
        owner: type[Any],
        method_name_or_ref: str | Callable[..., Any],
        label: str | None = None,
    ) -> None:
        if isinstance(method_name_or_ref, str):
            method_name = method_name_or_ref
        else:
            method_name = getattr(method_name_or_ref, '__name__', '')
            if not method_name:
                raise ValueError('method_name_or_ref callable must define __name__')
        original = getattr(owner, method_name)
        metric_name = label or f'{owner.__name__}.{method_name}'
        wrapped_method: Any

        if inspect.iscoroutinefunction(original):

            @functools.wraps(original)
            async def wrapped_async(*args: Any, **kwargs: Any) -> Any:
                started = time.perf_counter()
                try:
                    return await original(*args, **kwargs)
                finally:
                    elapsed = time.perf_counter() - started
                    metric = self.stats.setdefault(metric_name, {'calls': 0.0, 'total_s': 0.0})
                    metric['calls'] += 1.0
                    metric['total_s'] += elapsed

            wrapped_method = wrapped_async
        else:

            @functools.wraps(original)
            def wrapped_sync(*args: Any, **kwargs: Any) -> Any:
                started = time.perf_counter()
                try:
                    return original(*args, **kwargs)
                finally:
                    elapsed = time.perf_counter() - started
                    metric = self.stats.setdefault(metric_name, {'calls': 0.0, 'total_s': 0.0})
                    metric['calls'] += 1.0
                    metric['total_s'] += elapsed

            wrapped_method = wrapped_sync

        self._restore.append((owner, method_name, original))
        setattr(owner, method_name, wrapped_method)

    def restore(self) -> None:
        for owner, method_name, original in reversed(self._restore):
            setattr(owner, method_name, original)
        self._restore.clear()

    def top_lines(self, limit: int = 12) -> list[str]:
        ranked = sorted(self.stats.items(), key=lambda item: item[1]['total_s'], reverse=True)
        lines: list[str] = []
        for name, metric in ranked[:limit]:
            calls = int(metric['calls'])
            total_s = metric['total_s']
            avg_us = (total_s * 1_000_000.0) / max(calls, 1)
            lines.append(f'{name}: calls={calls:,} total={total_s:.3f}s avg={avg_us:.1f}us')
        return lines


async def run_contention_round(
    *,
    event_handler_concurrency: Literal['serial', 'parallel'],
    bus_count: int = 10,
    events_per_bus: int = 120,
    batch_size: int = 20,
) -> dict[str, float]:
    """
    Concurrently dispatch on many buses to stress global lock contention.
    """
    buses = [
        EventBus(
            name=f'LockContention_{i}_{event_handler_concurrency}',
            event_handler_concurrency=event_handler_concurrency,
            middlewares=[],
            max_history_drop=True,
        )
        for i in range(bus_count)
    ]
    counters = [0 for _ in range(bus_count)]
    dispatch_latencies_ms: list[float] = []
    done_latencies_ms: list[float] = []

    for index, bus in enumerate(buses):

        def make_handler(handler_index: int):
            async def handler(event: SimpleEvent) -> None:
                counters[handler_index] += 1

            handler.__name__ = f'contention_handler_{handler_index}'
            return handler

        bus.on(SimpleEvent, make_handler(index))

    async def wait_batch(batch: list[tuple[BaseEvent[Any], float]]) -> None:
        async def wait_one(item: tuple[BaseEvent[Any], float]) -> None:
            event, dispatch_done_at = item
            await event
            done_latencies_ms.append((time.perf_counter() - dispatch_done_at) * 1000)

        await asyncio.gather(*(wait_one(item) for item in batch))

    async def producer(bus: EventBus) -> None:
        pending: list[tuple[BaseEvent[Any], float]] = []
        for _ in range(events_per_bus):
            t0 = time.perf_counter()
            event = bus.emit(SimpleEvent())
            dispatch_latencies_ms.append((time.perf_counter() - t0) * 1000)
            pending.append((event, time.perf_counter()))
            if len(pending) >= batch_size:
                await wait_batch(pending)
                pending.clear()

        if pending:
            await wait_batch(pending)
        await bus.wait_until_idle()

    total_events = bus_count * events_per_bus
    start = time.perf_counter()
    try:
        await asyncio.gather(*(producer(bus) for bus in buses))
    finally:
        await asyncio.gather(*(bus.stop(timeout=0, clear=True) for bus in buses))

    duration = time.perf_counter() - start
    return {
        'throughput': total_events / max(duration, 1e-9),
        'dispatch_p50_ms': percentile(dispatch_latencies_ms, 0.50),
        'dispatch_p95_ms': percentile(dispatch_latencies_ms, 0.95),
        'done_p50_ms': percentile(done_latencies_ms, 0.50),
        'done_p95_ms': percentile(done_latencies_ms, 0.95),
        'fairness_min': float(min(counters)),
        'fairness_max': float(max(counters)),
    }


class SimpleEvent(BaseEvent):
    """Simple event without Generic for performance testing"""

    pass


@pytest.mark.asyncio
async def test_20k_events_with_memory_control():
    """Test processing 20k events with no memory leaks"""

    # Record initial memory
    gc.collect()
    initial_memory = get_memory_usage_mb()
    print(f'\nInitial memory: {initial_memory:.1f} MB')

    # Use bounded history with drop enabled to allow sustained flooding.
    bus = EventBus(name='ManyEvents', middlewares=[], max_history_drop=True)

    print('EventBus settings:')
    print(f'  max_history_size: {bus.event_history.max_history_size}')
    print(f'  queue maxsize: {bus.pending_event_queue.maxsize if bus.pending_event_queue else "not created"}')
    print('Starting event dispatch...')

    processed_count = 0

    async def handler(event: SimpleEvent) -> None:
        nonlocal processed_count
        processed_count += 1

    bus.on('SimpleEvent', handler)

    total_events = 20_000  # Reduced for faster tests

    start_time = time.time()
    memory_samples: list[float] = []
    max_memory = initial_memory

    # Dispatch all events as fast as possible (naive flood).
    dispatched = 0
    pending_events: list[BaseEvent[Any]] = []

    while dispatched < total_events:
        event = bus.emit(SimpleEvent())
        pending_events.append(event)
        dispatched += 1
        if dispatched <= 5:
            print(f'Dispatched event {dispatched}')

        # Sample memory every 10k events
        if dispatched % 10_000 == 0 and dispatched > 0:
            gc.collect()
            current_memory = get_memory_usage_mb()
            memory_samples.append(current_memory)
            max_memory = max(max_memory, current_memory)
            elapsed = time.time() - start_time
            rate = dispatched / elapsed
            print(
                f'Progress: {dispatched:,} events, '
                f'Memory: {current_memory:.1f} MB (+{current_memory - initial_memory:.1f} MB), '
                f'History: {len(bus.event_history)}, '
                f'Rate: {rate:.0f} events/sec'
            )

    # Wait for all remaining events to complete
    if pending_events:
        await asyncio.gather(*pending_events)

    # Final wait
    await bus.wait_until_idle()

    duration = time.time() - start_time

    # Final memory check
    gc.collect()
    final_memory = get_memory_usage_mb()
    memory_growth = final_memory - initial_memory
    peak_growth = max_memory - initial_memory

    print('\nFinal Results:')
    print(f'Processed: {processed_count:,} events')
    print(f'Duration: {duration:.2f} seconds')
    print(f'Rate: {processed_count / duration:,.0f} events/sec')
    print(f'Initial memory: {initial_memory:.1f} MB')
    print(f'Peak memory: {max_memory:.1f} MB (+{peak_growth:.1f} MB)')
    print(f'Final memory: {final_memory:.1f} MB (+{memory_growth:.1f} MB)')

    # Debug: Check if dispatch pipeline still has work
    queue_size = bus.pending_event_queue.qsize() if bus.pending_event_queue else 0
    print(f'DEBUG: Queue size: {queue_size}')
    print(f'DEBUG: In-flight event ids: {len(bus.in_flight_event_ids)}')

    # Safely get event history size without iterating
    try:
        history_size = len(bus.event_history)
        print(f'Event history size: {history_size} (capped at {bus.event_history.max_history_size})')
    except Exception as e:
        print(f'ERROR getting event history size: {type(e).__name__}: {e}')

    # Verify results
    print('DEBUG: About to check processed_count assertion...')
    assert processed_count == total_events, f'Only processed {processed_count} of {total_events}'
    print('DEBUG: About to check duration assertion...')
    assert duration < 360.0, f'Took {duration:.2f}s, should be < 360s'

    # Check memory usage stayed reasonable
    print('DEBUG: About to check memory assertion...')
    assert peak_growth < 300.0, f'Memory grew by {peak_growth:.1f} MB at peak, indicates memory leak'

    # Check event history is properly limited
    print('DEBUG: About to check history size assertions...')
    assert bus.event_history.max_history_size is not None
    assert len(bus.event_history) <= bus.event_history.max_history_size, (
        f'Event history has {len(bus.event_history)} events, should be <= {bus.event_history.max_history_size}'
    )

    # Explicitly clean up the bus to prevent hanging
    print('\nCleaning up EventBus...')
    queue_size_before_stop = bus.pending_event_queue.qsize() if bus.pending_event_queue else 0
    print(f'Before stop - Queue size: {queue_size_before_stop}')
    print(f'Before stop - In-flight event ids: {len(bus.in_flight_event_ids)}')

    await bus.stop(timeout=0, clear=True)
    print('EventBus stopped successfully')


@pytest.mark.asyncio
async def test_hard_limit_enforcement():
    """Test that max_history_drop=False rejects dispatches at max_history_size."""
    bus = EventBus(
        name='HardLimitTest',
        max_history_size=100,
        max_history_drop=False,
        middlewares=[],
    )

    try:
        # Create a slow handler to keep events pending
        async def slow_handler(event: SimpleEvent) -> None:
            await asyncio.sleep(0.5)  # Reduced from 10s to 0.5s

        bus.on('SimpleEvent', slow_handler)

        # Try to dispatch more than the configured history limit
        events_dispatched = 0
        errors = 0

        for _ in range(200):
            try:
                bus.emit(SimpleEvent())
                events_dispatched += 1
            except RuntimeError as e:
                if 'history limit reached' in str(e):
                    errors += 1
                else:
                    raise

        print(f'\nDispatched {events_dispatched} events')
        print(f'Hit history-limit error {errors} times')

        # Should reject once limit is reached
        assert bus.event_history.max_history_size is not None
        assert events_dispatched <= bus.event_history.max_history_size
        assert errors > 0

    finally:
        # Properly stop the bus to clean up pending tasks
        await bus.stop(timeout=0, clear=True)  # Don't wait, just force cleanup


@pytest.mark.asyncio
async def test_cleanup_prioritizes_pending():
    """Test that cleanup keeps pending events and removes completed ones"""
    bus = EventBus(name='CleanupTest', max_history_size=10, max_history_drop=True, middlewares=[])

    try:
        # Process some events to completion
        completed_events: list[BaseEvent[Any]] = []
        for _ in range(5):
            event = bus.emit(BaseEvent(event_type='QuickEvent'))
            completed_events.append(event)

        await asyncio.gather(*completed_events)

        # Add pending events with slow handler (reduced sleep time)
        async def slow_handler(event: BaseEvent) -> None:
            if event.event_type == 'SlowEvent':
                await asyncio.sleep(0.5)  # Reduced from 10s to 0.5s

        bus.on('*', slow_handler)

        pending_events: list[BaseEvent[Any]] = []
        for _ in range(10):
            event = bus.emit(BaseEvent(event_type='SlowEvent'))
            pending_events.append(event)

        # Give them time to start
        await asyncio.sleep(0.1)

        # Check history - should prioritize keeping pending events
        history_types: dict[str, int] = {}
        for event in bus.event_history.values():
            status = event.event_status
            history_types[status] = history_types.get(status, 0) + 1

        print('\nHistory after cleanup:')
        print(f'  Total: {len(bus.event_history)} (max: {bus.event_history.max_history_size})')
        print(f'  By status: {history_types}')

        # Should have removed completed events to make room for pending
        assert bus.event_history.max_history_size is not None
        assert (
            len(bus.event_history) <= bus.event_history.max_history_size * 1.2
        )  # allow for some overhead to avoid frequent gc pausing
        assert history_types.get('pending', 0) + history_types.get('started', 0) >= 5

    finally:
        # Properly stop the bus to clean up pending tasks
        await bus.stop(timeout=0, clear=True)  # Don't wait, just force cleanup


@pytest.mark.asyncio
async def test_ephemeral_buses_with_forwarding_churn():
    """
    Closest Python equivalent to request-scoped bus churn:
    create short-lived buses, forward between them, process events, then clear.
    """
    total_bus_pairs = 60
    events_per_pair = 20
    total_events = total_bus_pairs * events_per_pair
    initial_instances = len(EventBus.all_instances)

    handled_a = 0
    handled_b = 0

    start = time.time()

    for idx in range(total_bus_pairs):
        bus_a = EventBus(name=f'EphemeralA_{idx}_{os.getpid()}', middlewares=[], max_history_drop=True)
        bus_b = EventBus(name=f'EphemeralB_{idx}_{os.getpid()}', middlewares=[], max_history_drop=True)

        async def handler_a(event: SimpleEvent) -> None:
            nonlocal handled_a
            handled_a += 1

        async def handler_b(event: SimpleEvent) -> None:
            nonlocal handled_b
            handled_b += 1

        bus_a.on(SimpleEvent, handler_a)
        bus_b.on(SimpleEvent, handler_b)
        bus_a.on('*', bus_b.emit)

        try:
            pending = [bus_a.emit(SimpleEvent()) for _ in range(events_per_pair)]
            await asyncio.gather(*pending)
            await bus_a.wait_until_idle()
            await bus_b.wait_until_idle()

            assert (
                bus_a.event_history.max_history_size is None or len(bus_a.event_history) <= bus_a.event_history.max_history_size
            )
            assert (
                bus_b.event_history.max_history_size is None or len(bus_b.event_history) <= bus_b.event_history.max_history_size
            )
        finally:
            await bus_a.stop(timeout=0, clear=True)
            await bus_b.stop(timeout=0, clear=True)

    duration = time.time() - start
    gc.collect()

    assert handled_a == total_events
    assert handled_b == total_events
    assert len(EventBus.all_instances) <= initial_instances
    assert duration < 180.0, f'Ephemeral bus churn took too long: {duration:.2f}s'


@pytest.mark.asyncio
async def test_forwarding_queue_jump_timeout_mix_stays_stable():
    """
    Stress a mixed path in Python:
    parent handler awaits forwarded child events, with intermittent child timeouts.
    """

    class MixedParentEvent(BaseEvent):
        iteration: int = 0
        event_timeout: float | None = 0.2

    class MixedChildEvent(BaseEvent):
        iteration: int = 0
        event_timeout: float | None = 0.05

    history_limit = 500
    total_iterations = 300

    bus_a = EventBus(name='MixedPathA', max_history_size=history_limit, max_history_drop=True, middlewares=[])
    bus_b = EventBus(name='MixedPathB', max_history_size=history_limit, max_history_drop=True, middlewares=[])

    parent_handled = 0
    child_handled = 0
    child_events: list[MixedChildEvent] = []

    async def child_handler(event: MixedChildEvent) -> str:
        nonlocal child_handled
        child_handled += 1
        if event.iteration % 7 == 0:
            await asyncio.sleep(0.01)
        else:
            await asyncio.sleep(0.0005)
        return 'child_done'

    async def parent_handler(event: MixedParentEvent) -> str:
        nonlocal parent_handled
        parent_handled += 1

        child_timeout = 0.001 if event.iteration % 7 == 0 else 0.05
        child = bus_a.emit(MixedChildEvent(iteration=event.iteration, event_timeout=child_timeout))
        bus_b.emit(child)
        child_events.append(child)
        await child
        return 'parent_done'

    bus_a.on(MixedParentEvent, parent_handler)
    bus_b.on(MixedChildEvent, child_handler)

    start = time.time()
    try:
        with suppress_abxbus_warning_logs():
            for i in range(total_iterations):
                await bus_a.emit(MixedParentEvent(iteration=i))
            await bus_a.wait_until_idle()
            await bus_b.wait_until_idle()
    finally:
        await bus_a.stop(timeout=0, clear=True)
        await bus_b.stop(timeout=0, clear=True)

    duration = time.time() - start

    assert parent_handled == total_iterations
    assert child_handled == total_iterations
    timeout_count = sum(
        1
        for child in child_events
        if any(
            isinstance(
                result.error,
                (TimeoutError, EventHandlerTimeoutError, EventHandlerAbortedError, EventHandlerCancelledError),
            )
            for result in child.event_results.values()
        )
    )
    assert timeout_count > 0
    assert len(bus_a.event_history) <= history_limit
    assert len(bus_b.event_history) <= history_limit
    assert duration < 180.0, f'Mixed forwarding/queue-jump/timeout path took too long: {duration:.2f}s'


@pytest.mark.asyncio
async def test_history_bound_is_strict_after_idle():
    """After steady-state processing, history should stay within max_history_size."""
    bus = EventBus(name='StrictHistoryBound', max_history_size=25, max_history_drop=True, middlewares=[])

    async def handler(event: SimpleEvent) -> None:
        return None

    bus.on(SimpleEvent, handler)

    try:
        for _ in range(200):
            await bus.emit(SimpleEvent())

        await bus.wait_until_idle()
        assert len(bus.event_history) <= 25
    finally:
        await bus.stop(timeout=0, clear=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_handler_concurrency',
    ['serial', 'parallel'],
    ids=['serial_handler_concurrency', 'parallel_handler_concurrency'],
)
async def test_basic_throughput_floor_regression_guard(event_handler_concurrency: Literal['serial', 'parallel']):
    """
    Throughput regression guard across Python's handler concurrency modes.
    Keeps threshold conservative to avoid CI flakiness while still catching
    severe slowdowns.
    """
    processed, rate = await run_mode_throughput_benchmark(event_handler_concurrency=event_handler_concurrency)

    assert processed == 5_000
    minimum_rate = throughput_floor_for_mode(event_handler_concurrency)
    mode = event_handler_concurrency
    assert rate >= minimum_rate, f'{mode} throughput regression: {rate:.0f} events/sec (expected >= {minimum_rate} events/sec)'


@pytest.mark.asyncio
async def test_event_handler_concurrency_mode_improves_io_bound_fanout():
    """
    For I/O-bound workloads with multiple handlers per event, parallel mode should
    provide a meaningful speedup versus serial mode.
    """
    serial_handled, serial_duration = await run_io_fanout_benchmark(event_handler_concurrency='serial')
    parallel_handled, parallel_duration = await run_io_fanout_benchmark(event_handler_concurrency='parallel')

    expected_total = 800 * 4
    assert serial_handled == expected_total
    assert parallel_handled == expected_total
    assert parallel_duration < serial_duration * 0.8, (
        f'Expected parallel handler mode to be faster for I/O fanout; '
        f'serial={serial_duration:.2f}s parallel={parallel_duration:.2f}s'
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_handler_concurrency',
    ['serial', 'parallel'],
    ids=['serial_handler_concurrency', 'parallel_handler_concurrency'],
)
async def test_forwarding_throughput_floor_across_modes(event_handler_concurrency: Literal['serial', 'parallel']):
    """
    Regression guard for forwarding path in both handler execution modes.
    """
    source_bus = EventBus(
        name=f'ForwardSource_{event_handler_concurrency}',
        event_handler_concurrency=event_handler_concurrency,
        middlewares=[],
        max_history_drop=True,
    )
    target_bus = EventBus(
        name=f'ForwardTarget_{event_handler_concurrency}',
        event_handler_concurrency=event_handler_concurrency,
        middlewares=[],
        max_history_drop=True,
    )

    handled = 0

    async def sink_handler(event: SimpleEvent) -> None:
        nonlocal handled
        handled += 1

    source_bus.on('*', target_bus.emit)
    target_bus.on(SimpleEvent, sink_handler)

    total_events = 3_000
    pending: list[BaseEvent[Any]] = []
    batch_size = 40
    start = time.time()
    try:
        for _ in range(total_events):
            pending.append(source_bus.emit(SimpleEvent()))
            if len(pending) >= batch_size:
                await asyncio.gather(*pending)
                pending.clear()

        if pending:
            await asyncio.gather(*pending)
        await source_bus.wait_until_idle()
        await target_bus.wait_until_idle()
    finally:
        await source_bus.stop(timeout=0, clear=True)
        await target_bus.stop(timeout=0, clear=True)

    duration = time.time() - start
    throughput = total_events / max(duration, 1e-9)
    floor = 200

    assert handled == total_events
    mode = event_handler_concurrency
    assert throughput >= floor, (
        f'{mode} forwarding throughput regression: {throughput:.0f} events/sec (expected >= {floor} events/sec)'
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_handler_concurrency',
    ['serial', 'parallel'],
    ids=['serial_handler_concurrency', 'parallel_handler_concurrency'],
)
async def test_global_lock_contention_multi_bus_matrix(event_handler_concurrency: Literal['serial', 'parallel']):
    """
    High-contention benchmark: many buses dispatching concurrently under global lock.
    """
    phase1 = await run_contention_round(event_handler_concurrency=event_handler_concurrency)
    phase2 = await run_contention_round(event_handler_concurrency=event_handler_concurrency)

    expected_per_bus = 120.0
    hard_floor = 120.0
    regression_floor = throughput_regression_floor(
        phase1['throughput'],
        min_fraction=0.55,
        hard_floor=90.0,
    )

    assert phase1['fairness_min'] == expected_per_bus
    assert phase1['fairness_max'] == expected_per_bus
    assert phase2['fairness_min'] == expected_per_bus
    assert phase2['fairness_max'] == expected_per_bus
    assert phase1['throughput'] >= hard_floor, (
        f'lock-contention throughput too low: {phase1["throughput"]:.0f} events/sec (expected >= {hard_floor:.0f})'
    )
    assert phase2['throughput'] >= regression_floor, (
        f'lock-contention regression: phase1={phase1["throughput"]:.0f} '
        f'phase2={phase2["throughput"]:.0f} '
        f'(required >= {regression_floor:.0f})'
    )
    assert phase2['dispatch_p95_ms'] < 75.0
    assert phase2['done_p95_ms'] < 750.0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'handlers_per_event',
    [10, 30],
    ids=['fanout_10_handlers', 'fanout_30_handlers'],
)
async def test_event_handler_concurrency_mode_scales_with_high_fanout(handlers_per_event: int):
    """
    High fanout benchmark to catch regressions in parallel handler scheduling.
    """
    serial_handled, serial_duration = await run_io_fanout_benchmark(
        event_handler_concurrency='serial',
        total_events=400,
        handlers_per_event=handlers_per_event,
        sleep_seconds=0.001,
        batch_size=25,
    )
    parallel_handled, parallel_duration = await run_io_fanout_benchmark(
        event_handler_concurrency='parallel',
        total_events=400,
        handlers_per_event=handlers_per_event,
        sleep_seconds=0.001,
        batch_size=25,
    )

    expected_total = 400 * handlers_per_event
    speedup = serial_duration / max(parallel_duration, 1e-9)
    minimum_speedup = 1.2 if handlers_per_event == 10 else 1.5

    assert serial_handled == expected_total
    assert parallel_handled == expected_total
    assert speedup >= minimum_speedup, (
        f'Parallel fanout speedup too small for {handlers_per_event} handlers/event: '
        f'{speedup:.2f}x (expected >= {minimum_speedup:.2f}x)'
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_handler_concurrency',
    ['serial', 'parallel'],
    ids=['serial_handler_concurrency', 'parallel_handler_concurrency'],
)
async def test_queue_jump_perf_matrix_by_mode(event_handler_concurrency: Literal['serial', 'parallel']):
    """
    Queue-jump throughput/latency matrix (parent awaits child on same bus) by mode.
    """

    class QueueJumpParentEvent(BaseEvent):
        iteration: int = 0
        event_timeout: float | None = 0.2

    class QueueJumpChildEvent(BaseEvent):
        iteration: int = 0
        event_timeout: float | None = 0.2

    bus = EventBus(
        name=f'QueueJump_{event_handler_concurrency}',
        event_handler_concurrency=event_handler_concurrency,
        middlewares=[],
        max_history_drop=True,
    )

    parent_count = 0
    child_count = 0
    phase_counter = 0

    async def child_handler(event: QueueJumpChildEvent) -> None:
        nonlocal child_count
        child_count += 1
        await asyncio.sleep(0.0005)

    async def parent_handler(event: QueueJumpParentEvent) -> None:
        nonlocal parent_count
        parent_count += 1
        child = bus.emit(QueueJumpChildEvent(iteration=event.iteration))
        await child

    bus.on(QueueJumpParentEvent, parent_handler)
    bus.on(QueueJumpChildEvent, child_handler)

    def parent_factory() -> QueueJumpParentEvent:
        nonlocal phase_counter
        event = QueueJumpParentEvent(iteration=phase_counter)
        phase_counter += 1
        return event

    try:
        phase1 = await dispatch_and_measure(bus, parent_factory, total_events=500, batch_size=20)
        phase2 = await dispatch_and_measure(bus, parent_factory, total_events=500, batch_size=20)
    finally:
        await bus.stop(timeout=0, clear=True)

    hard_floor = 60.0
    regression_floor = throughput_regression_floor(phase1[0], min_fraction=0.50, hard_floor=50.0)

    assert parent_count == 1_000
    assert child_count == 1_000
    assert phase1[0] >= hard_floor, f'queue-jump throughput too low: {phase1[0]:.0f} events/sec (expected >= {hard_floor:.0f})'
    assert phase2[0] >= regression_floor, (
        f'queue-jump regression: phase1={phase1[0]:.0f} phase2={phase2[0]:.0f} (required >= {regression_floor:.0f})'
    )
    assert phase2[2] < 45.0
    assert phase2[4] < 360.0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_handler_concurrency',
    ['serial', 'parallel'],
    ids=['serial_handler_concurrency', 'parallel_handler_concurrency'],
)
async def test_forwarding_chain_perf_matrix_by_mode(event_handler_concurrency: Literal['serial', 'parallel']):
    """
    Forwarding chain A -> B -> C throughput/latency matrix by mode.
    """
    source_bus = EventBus(
        name=f'ChainSource_{event_handler_concurrency}',
        event_handler_concurrency=event_handler_concurrency,
        max_history_size=120,
        middlewares=[],
        max_history_drop=True,
    )
    middle_bus = EventBus(
        name=f'ChainMiddle_{event_handler_concurrency}',
        event_handler_concurrency=event_handler_concurrency,
        max_history_size=120,
        middlewares=[],
        max_history_drop=True,
    )
    sink_bus = EventBus(
        name=f'ChainSink_{event_handler_concurrency}',
        event_handler_concurrency=event_handler_concurrency,
        max_history_size=120,
        middlewares=[],
        max_history_drop=True,
    )

    sink_count = 0

    async def sink_handler(event: SimpleEvent) -> None:
        nonlocal sink_count
        sink_count += 1

    async def forward_to_middle(event: BaseEvent[Any]) -> None:
        while True:
            try:
                middle_bus.emit(event)
                return
            except asyncio.QueueFull:
                await asyncio.sleep(0)
            except RuntimeError as exc:
                if 'history limit reached' not in str(exc):
                    raise
                await asyncio.sleep(0)

    async def forward_to_sink(event: BaseEvent[Any]) -> None:
        while True:
            try:
                sink_bus.emit(event)
                return
            except asyncio.QueueFull:
                await asyncio.sleep(0)
            except RuntimeError as exc:
                if 'history limit reached' not in str(exc):
                    raise
                await asyncio.sleep(0)

    source_bus.on('*', forward_to_middle)
    middle_bus.on('*', forward_to_sink)
    sink_bus.on(SimpleEvent, sink_handler)

    try:
        phase1 = await dispatch_and_measure(source_bus, SimpleEvent, total_events=500, batch_size=5)
        phase2 = await dispatch_and_measure(source_bus, SimpleEvent, total_events=500, batch_size=5)
        await source_bus.wait_until_idle()
        await middle_bus.wait_until_idle()
        await sink_bus.wait_until_idle()
    finally:
        await source_bus.stop(timeout=0, clear=True)
        await middle_bus.stop(timeout=0, clear=True)
        await sink_bus.stop(timeout=0, clear=True)

    hard_floor = 35.0
    regression_floor = throughput_regression_floor(phase1[0], min_fraction=0.45, hard_floor=20.0)

    assert sink_count == 1_000
    assert phase1[0] >= hard_floor
    assert phase2[0] >= regression_floor
    assert phase2[2] < 120.0
    assert phase2[4] < 1050.0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_handler_concurrency',
    ['serial', 'parallel'],
    ids=['serial_handler_concurrency', 'parallel_handler_concurrency'],
)
async def test_timeout_churn_perf_matrix_by_mode(event_handler_concurrency: Literal['serial', 'parallel']):
    """
    Timeout-heavy phase followed by healthy phase should keep throughput healthy.
    """

    class TimeoutChurnEvent(BaseEvent):
        mode: str = 'slow'
        iteration: int = 0
        event_timeout: float | None = 0.01

    bus = EventBus(
        name=f'TimeoutChurn_{event_handler_concurrency}',
        event_handler_concurrency=event_handler_concurrency,
        middlewares=[],
        max_history_drop=True,
    )

    timeout_phase_events: list[TimeoutChurnEvent] = []
    recovery_phase_events: list[TimeoutChurnEvent] = []
    timeout_counter = 0
    recovery_counter = 0

    async def handler(event: TimeoutChurnEvent) -> None:
        if event.mode == 'slow':
            await asyncio.sleep(0.006)
        else:
            await asyncio.sleep(0)

    bus.on(TimeoutChurnEvent, handler)

    def timeout_factory() -> TimeoutChurnEvent:
        nonlocal timeout_counter
        is_slow = (timeout_counter % 3) != 0
        event = TimeoutChurnEvent(
            mode='slow' if is_slow else 'fast',
            iteration=timeout_counter,
            event_timeout=0.001 if is_slow else 0.02,
        )
        timeout_phase_events.append(event)
        timeout_counter += 1
        return event

    def recovery_factory() -> TimeoutChurnEvent:
        nonlocal recovery_counter
        event = TimeoutChurnEvent(
            mode='fast',
            iteration=10_000 + recovery_counter,
            # Keep recovery timeout comfortably above scheduler jitter so any
            # recovery error signals a real lock/cancellation bug, not timing noise.
            event_timeout=0.05,
        )
        recovery_phase_events.append(event)
        recovery_counter += 1
        return event

    try:
        with suppress_abxbus_warning_logs():
            timeout_phase = await dispatch_and_measure(bus, timeout_factory, total_events=180, batch_size=20)
            recovery_phase = await dispatch_and_measure(bus, recovery_factory, total_events=500, batch_size=25)
    finally:
        await bus.stop(timeout=0, clear=True)

    timeout_count = sum(
        1
        for event in timeout_phase_events
        if event.mode == 'slow'
        and any(
            isinstance(
                result.error,
                (TimeoutError, EventHandlerTimeoutError, EventHandlerAbortedError, EventHandlerCancelledError),
            )
            for result in event.event_results.values()
        )
    )
    recovery_errors = sum(
        1 for event in recovery_phase_events if any(result.error is not None for result in event.event_results.values())
    )
    hard_floor = 120.0
    regression_floor = throughput_regression_floor(
        timeout_phase[0],
        min_fraction=0.45,
        hard_floor=100.0,
    )

    assert timeout_count > 0
    assert recovery_errors == 0
    assert recovery_phase[0] >= hard_floor
    assert recovery_phase[0] >= regression_floor
    assert recovery_phase[2] < 36.0
    assert recovery_phase[4] < 70.0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_handler_concurrency',
    ['serial', 'parallel'],
    ids=['serial_handler_concurrency', 'parallel_handler_concurrency'],
)
async def test_memory_envelope_by_mode_for_capped_history(event_handler_concurrency: Literal['serial', 'parallel']):
    """
    Mode-specific memory slope/envelope check with capped history.
    """
    bus = EventBus(
        name=f'MemoryEnvelope_{event_handler_concurrency}',
        event_handler_concurrency=event_handler_concurrency,
        max_history_size=60,
        middlewares=[],
        max_history_drop=True,
    )

    async def handler(event: SimpleEvent) -> None:
        return None

    bus.on(SimpleEvent, handler)

    gc.collect()
    before_mb = get_memory_usage_mb()

    try:
        metrics = await dispatch_and_measure(bus, SimpleEvent, total_events=6_000, batch_size=40)
        done_mb = get_memory_usage_mb()
        gc.collect()
        gc_mb = get_memory_usage_mb()
        retained = len(bus.event_history)
    finally:
        await bus.stop(timeout=0, clear=True)

    done_delta = done_mb - before_mb
    gc_delta = gc_mb - before_mb
    per_dispatched_kb = (max(done_delta, 0.0) * 1024.0) / 6_000
    per_retained_mb = max(gc_delta, 0.0) / max(retained, 1)
    done_budget = 130.0 if event_handler_concurrency == 'parallel' else 110.0
    gc_budget = 70.0 if event_handler_concurrency == 'parallel' else 60.0

    assert retained <= 60
    assert metrics[0] >= 450.0
    assert metrics[2] < 30.0
    assert metrics[4] < 60.0
    assert done_delta < done_budget
    assert gc_delta < gc_budget
    assert per_dispatched_kb < 32.0
    assert per_retained_mb < 1.5


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_handler_concurrency',
    ['serial', 'parallel'],
    ids=['serial_handler_concurrency', 'parallel_handler_concurrency'],
)
async def test_max_history_none_single_bus_stress_matrix(event_handler_concurrency: Literal['serial', 'parallel']):
    """
    Unlimited-history mode stress for single bus: throughput + memory envelope.
    """
    bus = EventBus(
        name=f'UnlimitedSingle_{event_handler_concurrency}',
        event_handler_concurrency=event_handler_concurrency,
        max_history_size=None,
        middlewares=[],
        max_history_drop=True,
    )
    processed = 0

    async def handler(event: SimpleEvent) -> None:
        nonlocal processed
        processed += 1

    bus.on(SimpleEvent, handler)

    gc.collect()
    before_mb = get_memory_usage_mb()
    try:
        phase1 = await dispatch_and_measure(bus, SimpleEvent, total_events=1_500, batch_size=120)
        phase2 = await dispatch_and_measure(bus, SimpleEvent, total_events=1_500, batch_size=120)
        done_mb = get_memory_usage_mb()
        gc.collect()
        gc_mb = get_memory_usage_mb()
        history_size = len(bus.event_history)
    finally:
        await bus.stop(timeout=0, clear=True)

    done_delta = done_mb - before_mb
    gc_delta = gc_mb - before_mb
    per_event_mb = max(gc_delta, 0.0) / 3_000
    hard_floor = 220.0
    regression_floor = throughput_regression_floor(phase1[0], min_fraction=0.55, hard_floor=170.0)

    assert processed == 3_000
    assert history_size == 3_000
    assert phase1[0] >= hard_floor
    assert phase2[0] >= regression_floor
    assert phase2[2] < 36.0
    assert phase2[4] < 240.0
    assert done_delta < 260.0
    assert gc_delta < 220.0
    assert per_event_mb < 0.08


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_handler_concurrency',
    ['serial', 'parallel'],
    ids=['serial_handler_concurrency', 'parallel_handler_concurrency'],
)
async def test_max_history_none_forwarding_chain_stress_matrix(event_handler_concurrency: Literal['serial', 'parallel']):
    """
    Unlimited-history forwarding chain (A -> B -> C) stress by mode.
    """
    source_bus = EventBus(
        name=f'UnlimitedChainSource_{event_handler_concurrency}',
        event_handler_concurrency=event_handler_concurrency,
        max_history_size=None,
        middlewares=[],
        max_history_drop=True,
    )
    middle_bus = EventBus(
        name=f'UnlimitedChainMiddle_{event_handler_concurrency}',
        event_handler_concurrency=event_handler_concurrency,
        max_history_size=None,
        middlewares=[],
        max_history_drop=True,
    )
    sink_bus = EventBus(
        name=f'UnlimitedChainSink_{event_handler_concurrency}',
        event_handler_concurrency=event_handler_concurrency,
        max_history_size=None,
        middlewares=[],
        max_history_drop=True,
    )

    sink_count = 0

    async def sink_handler(event: SimpleEvent) -> None:
        nonlocal sink_count
        sink_count += 1

    source_bus.on('*', middle_bus.emit)
    middle_bus.on('*', sink_bus.emit)
    sink_bus.on(SimpleEvent, sink_handler)

    gc.collect()
    before_mb = get_memory_usage_mb()
    try:
        phase1 = await dispatch_and_measure(source_bus, SimpleEvent, total_events=900, batch_size=100)
        phase2 = await dispatch_and_measure(source_bus, SimpleEvent, total_events=900, batch_size=100)
        done_mb = get_memory_usage_mb()
        gc.collect()
        gc_mb = get_memory_usage_mb()
        source_hist = len(source_bus.event_history)
        middle_hist = len(middle_bus.event_history)
        sink_hist = len(sink_bus.event_history)
    finally:
        await source_bus.stop(timeout=0, clear=True)
        await middle_bus.stop(timeout=0, clear=True)
        await sink_bus.stop(timeout=0, clear=True)

    gc_delta = gc_mb - before_mb
    done_delta = done_mb - before_mb
    hard_floor = 170.0
    regression_floor = throughput_regression_floor(phase1[0], min_fraction=0.55, hard_floor=130.0)

    assert sink_count == 1_800
    assert source_hist == 1_800
    assert middle_hist == 1_800
    assert sink_hist == 1_800
    assert phase1[0] >= hard_floor
    assert phase2[0] >= regression_floor
    assert phase2[2] < 45.0
    assert phase2[4] < 300.0
    assert done_delta < 320.0
    assert gc_delta < 280.0


@pytest.mark.asyncio
async def test_perf_debug_hot_path_breakdown() -> None:
    """
    Debug-only perf test:
    profiles key hot-path methods to confirm where time is spent before optimizing.
    """
    profiler = MethodProfiler()
    instrumented: list[tuple[type[Any], str]] = [
        (event_bus_module.ReentrantLock, '__aenter__'),
        (event_bus_module.ReentrantLock, '__aexit__'),
        (event_bus_module.EventBus, '_get_handlers_for_event'),
        (event_bus_module.EventBus, '_process_event'),
        (event_bus_module.EventBus, '_run_handler'),
        (event_bus_module.EventHistory, 'trim_event_history'),
        (base_event_module.BaseEvent, '_create_pending_handler_results'),
    ]
    for owner, method_ref in instrumented:
        profiler.instrument(owner, method_ref)

    class DebugParentEvent(BaseEvent):
        idx: int = 0
        event_timeout: float | None = 0.2

    class DebugChildEvent(BaseEvent):
        idx: int = 0
        event_timeout: float | None = 0.2

    bus_a = EventBus(name='PerfDebugA', middlewares=[], max_history_drop=True)
    bus_b = EventBus(name='PerfDebugB', middlewares=[], max_history_drop=True)

    forwarded_simple_count = 0
    child_count = 0
    parent_counter = 0

    async def forwarded_simple_handler(event: SimpleEvent) -> None:
        nonlocal forwarded_simple_count
        forwarded_simple_count += 1

    async def child_handler(event: DebugChildEvent) -> None:
        nonlocal child_count
        child_count += 1
        await asyncio.sleep(0)

    async def parent_handler(event: DebugParentEvent) -> None:
        child = bus_a.emit(DebugChildEvent(idx=event.idx))
        bus_b.emit(child)
        await child

    bus_a.on('*', bus_b.emit)
    bus_b.on(SimpleEvent, forwarded_simple_handler)
    bus_a.on(DebugParentEvent, parent_handler)
    bus_b.on(DebugChildEvent, child_handler)

    def parent_factory() -> DebugParentEvent:
        nonlocal parent_counter
        event = DebugParentEvent(idx=parent_counter)
        parent_counter += 1
        return event

    gc.collect()
    before_mb = get_memory_usage_mb()
    start = time.perf_counter()
    try:
        simple_metrics = await dispatch_and_measure(bus_a, SimpleEvent, total_events=2_000, batch_size=50)
        parent_metrics = await dispatch_and_measure(bus_a, parent_factory, total_events=600, batch_size=20)
        await bus_a.wait_until_idle()
        await bus_b.wait_until_idle()
    finally:
        await bus_a.stop(timeout=0, clear=True)
        await bus_b.stop(timeout=0, clear=True)
        profiler.restore()
    elapsed = time.perf_counter() - start
    done_mb = get_memory_usage_mb()
    gc.collect()
    gc_mb = get_memory_usage_mb()

    print('\n[perf-debug] scenario=global_fifo_forwarding_queue_jump')
    print(f'[perf-debug] elapsed_s={elapsed:.3f}')
    print(
        f'[perf-debug] simple throughput={simple_metrics[0]:.0f}/s dispatch_p95={simple_metrics[2]:.3f}ms done_p95={simple_metrics[4]:.3f}ms'
    )
    print(
        f'[perf-debug] queue_jump throughput={parent_metrics[0]:.0f}/s dispatch_p95={parent_metrics[2]:.3f}ms done_p95={parent_metrics[4]:.3f}ms'
    )
    print(f'[perf-debug] memory_mb before={before_mb:.1f} done={done_mb:.1f} gc={gc_mb:.1f}')
    print(f'[perf-debug] forwarded_simple_count={forwarded_simple_count:,} child_count={child_count:,}')
    print('[perf-debug] hot_path_top_total_time:')
    for line in profiler.top_lines(limit=14):
        print(f'[perf-debug]   {line}')

    assert forwarded_simple_count == 2_000
    assert child_count == 600
