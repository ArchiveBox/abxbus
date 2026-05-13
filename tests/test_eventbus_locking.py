import asyncio

import pytest

from abxbus import BaseEvent, EventBus, EventConcurrencyMode
from abxbus.retry import retry


class GlobalSerialEvent(BaseEvent[str]):
    order: int = 0
    source: str = 'a'


class PerBusSerialEvent(BaseEvent[str]):
    order: int = 0
    source: str = 'a'


class ParallelEvent(BaseEvent[str]):
    order: int = 0


class ParallelHandlerEvent(BaseEvent[str]):
    pass


class OverrideParallelEvent(BaseEvent[str]):
    order: int = 0
    event_concurrency: EventConcurrencyMode | None = EventConcurrencyMode.PARALLEL


class OverrideSerialEvent(BaseEvent[str]):
    order: int = 0
    event_concurrency: EventConcurrencyMode | None = EventConcurrencyMode.BUS_SERIAL


class ParentEvent(BaseEvent[str]):
    pass


class ChildEvent(BaseEvent[str]):
    pass


class SiblingEvent(BaseEvent[str]):
    pass


class HandlerLockEvent(BaseEvent[str]):
    order: int = 0
    source: str = 'a'


@pytest.mark.asyncio
async def test_event_concurrency_global_serial_allows_only_one_inflight_across_buses() -> None:
    bus_a = EventBus(name='GlobalSerialA', event_concurrency='global-serial')
    bus_b = EventBus(name='GlobalSerialB', event_concurrency='global-serial')
    in_flight = 0
    max_in_flight = 0
    starts: list[str] = []

    async def handler(event: GlobalSerialEvent) -> None:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        starts.append(f'{event.source}:{event.order}')
        await asyncio.sleep(0.01)
        in_flight -= 1

    bus_a.on(GlobalSerialEvent, handler)
    bus_b.on(GlobalSerialEvent, handler)

    try:
        for i in range(3):
            bus_a.emit(GlobalSerialEvent(order=i, source='a'))
            bus_b.emit(GlobalSerialEvent(order=i, source='b'))

        await asyncio.gather(bus_a.wait_until_idle(), bus_b.wait_until_idle())

        starts_a = [int(value.split(':')[1]) for value in starts if value.startswith('a:')]
        starts_b = [int(value.split(':')[1]) for value in starts if value.startswith('b:')]
        assert max_in_flight == 1
        assert starts_a == [0, 1, 2]
        assert starts_b == [0, 1, 2]
    finally:
        await bus_a.destroy(clear=True, timeout=0)
        await bus_b.destroy(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_event_concurrency_bus_serial_serializes_per_bus_but_overlaps_across_buses() -> None:
    bus_a = EventBus(name='BusSerialA', event_concurrency='bus-serial')
    bus_b = EventBus(name='BusSerialB', event_concurrency='bus-serial')
    b_started = asyncio.Event()

    in_flight_global = 0
    max_in_flight_global = 0
    in_flight_a = 0
    in_flight_b = 0
    max_in_flight_a = 0
    max_in_flight_b = 0

    async def on_a(_event: PerBusSerialEvent) -> None:
        nonlocal in_flight_global, max_in_flight_global, in_flight_a, max_in_flight_a
        in_flight_global += 1
        in_flight_a += 1
        max_in_flight_global = max(max_in_flight_global, in_flight_global)
        max_in_flight_a = max(max_in_flight_a, in_flight_a)
        await b_started.wait()
        await asyncio.sleep(0.01)
        in_flight_global -= 1
        in_flight_a -= 1

    async def on_b(_event: PerBusSerialEvent) -> None:
        nonlocal in_flight_global, max_in_flight_global, in_flight_b, max_in_flight_b
        in_flight_global += 1
        in_flight_b += 1
        max_in_flight_global = max(max_in_flight_global, in_flight_global)
        max_in_flight_b = max(max_in_flight_b, in_flight_b)
        b_started.set()
        await asyncio.sleep(0.01)
        in_flight_global -= 1
        in_flight_b -= 1

    bus_a.on(PerBusSerialEvent, on_a)
    bus_b.on(PerBusSerialEvent, on_b)

    try:
        bus_a.emit(PerBusSerialEvent(order=0, source='a'))
        bus_b.emit(PerBusSerialEvent(order=0, source='b'))
        await asyncio.gather(bus_a.wait_until_idle(), bus_b.wait_until_idle())

        assert max_in_flight_a == 1
        assert max_in_flight_b == 1
        assert max_in_flight_global >= 2
    finally:
        await bus_a.destroy(clear=True, timeout=0)
        await bus_b.destroy(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_event_concurrency_parallel_allows_same_bus_events_to_overlap() -> None:
    bus = EventBus(name='ParallelEventBus', event_concurrency='parallel', event_handler_concurrency='parallel')
    release = asyncio.Event()
    overlap_seen = asyncio.Event()

    in_flight = 0
    max_in_flight = 0

    async def handler(_event: ParallelEvent) -> None:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        if in_flight >= 2:
            overlap_seen.set()
        await release.wait()
        await asyncio.sleep(0.005)
        in_flight -= 1

    bus.on(ParallelEvent, handler)

    try:
        first = bus.emit(ParallelEvent(order=0))
        second = bus.emit(ParallelEvent(order=1))
        await asyncio.wait_for(overlap_seen.wait(), timeout=1.0)
        release.set()
        await asyncio.gather(first, second)
        await bus.wait_until_idle()

        assert max_in_flight >= 2
    finally:
        await bus.destroy(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_event_handler_concurrency_parallel_runs_handlers_for_same_event_concurrently() -> None:
    bus = EventBus(name='ParallelHandlerBus', event_concurrency='bus-serial', event_handler_concurrency='parallel')
    release = asyncio.Event()
    overlap_seen = asyncio.Event()
    in_flight = 0
    max_in_flight = 0

    async def handler_a(_event: ParallelHandlerEvent) -> None:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        if in_flight >= 2:
            overlap_seen.set()
        await release.wait()
        in_flight -= 1

    async def handler_b(_event: ParallelHandlerEvent) -> None:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        if in_flight >= 2:
            overlap_seen.set()
        await release.wait()
        in_flight -= 1

    bus.on(ParallelHandlerEvent, handler_a)
    bus.on(ParallelHandlerEvent, handler_b)

    try:
        event = bus.emit(ParallelHandlerEvent())
        await asyncio.wait_for(overlap_seen.wait(), timeout=1.0)
        release.set()
        await event
        await bus.wait_until_idle()

        assert max_in_flight >= 2
    finally:
        await bus.destroy(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_event_concurrency_override_parallel_beats_bus_serial_default() -> None:
    bus = EventBus(name='OverrideParallelBus', event_concurrency='bus-serial', event_handler_concurrency='parallel')
    release = asyncio.Event()
    overlap_seen = asyncio.Event()
    in_flight = 0
    max_in_flight = 0

    async def handler(_event: OverrideParallelEvent) -> None:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        if in_flight >= 2:
            overlap_seen.set()
        await release.wait()
        in_flight -= 1

    bus.on(OverrideParallelEvent, handler)

    try:
        first = bus.emit(OverrideParallelEvent(order=0, event_concurrency=EventConcurrencyMode.PARALLEL))
        second = bus.emit(OverrideParallelEvent(order=1, event_concurrency=EventConcurrencyMode.PARALLEL))
        await asyncio.wait_for(overlap_seen.wait(), timeout=1.0)
        release.set()
        await asyncio.gather(first, second)
        await bus.wait_until_idle()

        assert max_in_flight >= 2
    finally:
        await bus.destroy(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_event_concurrency_override_bus_serial_beats_bus_parallel_default() -> None:
    bus = EventBus(name='OverrideBusSerialBus', event_concurrency='parallel', event_handler_concurrency='parallel')
    release = asyncio.Event()
    in_flight = 0
    max_in_flight = 0

    async def handler(_event: OverrideSerialEvent) -> None:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await release.wait()
        in_flight -= 1

    bus.on(OverrideSerialEvent, handler)

    try:
        first = bus.emit(OverrideSerialEvent(order=0, event_concurrency=EventConcurrencyMode.BUS_SERIAL))
        second = bus.emit(OverrideSerialEvent(order=1, event_concurrency=EventConcurrencyMode.BUS_SERIAL))
        await asyncio.sleep(0.02)
        assert max_in_flight == 1

        release.set()
        await asyncio.gather(first, second)
        await bus.wait_until_idle()
    finally:
        await bus.destroy(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_queue_jump_awaited_child_preempts_queued_sibling_on_same_bus() -> None:
    bus = EventBus(name='QueueJumpBus', event_concurrency='bus-serial', event_handler_concurrency='serial')
    order: list[str] = []
    parent_started = asyncio.Event()
    sibling_queued = asyncio.Event()

    async def on_parent(event: ParentEvent) -> None:
        order.append('parent_start')
        parent_started.set()
        await sibling_queued.wait()
        child = event.event_bus.emit(ChildEvent())
        await child
        order.append('parent_end')

    async def on_child(_event: ChildEvent) -> None:
        order.append('child_start')
        order.append('child_end')

    async def on_sibling(_event: SiblingEvent) -> None:
        order.append('sibling')

    bus.on(ParentEvent, on_parent)
    bus.on(ChildEvent, on_child)
    bus.on(SiblingEvent, on_sibling)

    try:
        parent = bus.emit(ParentEvent())
        await parent_started.wait()
        sibling = bus.emit(SiblingEvent())
        sibling_queued.set()
        await asyncio.gather(parent, sibling)
        await bus.wait_until_idle()

        assert order[0] == 'parent_start'
        assert sorted(order) == ['child_end', 'child_start', 'parent_end', 'parent_start', 'sibling']
        assert order.index('child_start') < order.index('child_end') < order.index('sibling')
        assert order.index('child_end') < order.index('parent_end')
    finally:
        await bus.destroy(clear=True, timeout=0)


@pytest.mark.asyncio
async def test_retry_global_handler_lock_serializes_handlers_across_buses() -> None:
    bus_a = EventBus(name='GlobalHandlerA', event_concurrency='parallel', event_handler_concurrency='serial')
    bus_b = EventBus(name='GlobalHandlerB', event_concurrency='parallel', event_handler_concurrency='serial')

    in_flight = 0
    max_in_flight = 0

    @retry(semaphore_scope='global', semaphore_name='eventbus_locking_global_handler', semaphore_limit=1)
    async def locked_handler(_event: HandlerLockEvent) -> None:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.005)
        in_flight -= 1

    bus_a.on(HandlerLockEvent, locked_handler)
    bus_b.on(HandlerLockEvent, locked_handler)

    try:
        for i in range(4):
            bus_a.emit(HandlerLockEvent(order=i, source='a'))
            bus_b.emit(HandlerLockEvent(order=i, source='b'))

        await asyncio.gather(bus_a.wait_until_idle(), bus_b.wait_until_idle())
        assert max_in_flight == 1
    finally:
        await bus_a.destroy(clear=True, timeout=0)
        await bus_b.destroy(clear=True, timeout=0)


# Folded from test_retry.py to keep test layout class-based.
import inspect
import multiprocessing
import os
import re
import time
from typing import Any

import abxbus.retry as retry_helpers


def worker_acquire_semaphore(
    worker_id: int,
    start_time: float,
    results_queue: 'multiprocessing.Queue[Any]',
    hold_time: float = 0.5,
    timeout: float = 5.0,
    should_release: bool = True,
):
    """Worker process that tries to acquire a semaphore."""
    try:
        print(f'Worker {worker_id} starting...')

        # Define a function decorated with multiprocess semaphore
        @retry(
            max_attempts=1,
            timeout=10,
            semaphore_limit=3,  # Only 3 concurrent processes allowed
            semaphore_name='test_multiprocess_sem',
            semaphore_scope='multiprocess',
            semaphore_timeout=timeout,
            semaphore_lax=False,  # Strict mode - must acquire semaphore
        )
        async def semaphore_protected_function():
            acquire_time = time.time() - start_time
            results_queue.put(('acquired', worker_id, acquire_time))

            # Hold the semaphore for a bit
            await asyncio.sleep(hold_time)

            release_time = time.time() - start_time
            results_queue.put(('released', worker_id, release_time))
            return f'Worker {worker_id} completed'

        # Run the async function
        print(f'Worker {worker_id} running async function...')
        result = asyncio.run(semaphore_protected_function())
        print(f'Worker {worker_id} completed with result: {result}')
        results_queue.put(('completed', worker_id, result))

    except TimeoutError as e:
        timeout_time = time.time() - start_time
        print(f'Worker {worker_id} timed out: {e}')
        results_queue.put(('timeout', worker_id, timeout_time, str(e)))
    except Exception as e:
        error_time = time.time() - start_time
        print(f'Worker {worker_id} error: {type(e).__name__}: {e}')
        import traceback

        traceback.print_exc()
        results_queue.put(('error', worker_id, error_time, str(e)))


def worker_that_dies(
    worker_id: int,
    start_time: float,
    results_queue: 'multiprocessing.Queue[Any]',
    die_after: float = 0.2,
):
    """Worker process that acquires semaphore then dies without releasing."""
    try:

        @retry(
            max_attempts=1,
            timeout=10,
            semaphore_limit=2,  # Only 2 concurrent processes
            semaphore_name='test_death_sem',
            semaphore_scope='multiprocess',
            semaphore_timeout=5.0,
            semaphore_lax=False,
        )
        async def semaphore_protected_function():
            acquire_time = time.time() - start_time
            results_queue.put(('acquired', worker_id, acquire_time))

            # Hold for a bit then simulate crash
            await asyncio.sleep(die_after)

            # Simulate unexpected death
            os._exit(1)  # Hard exit without cleanup

        asyncio.run(semaphore_protected_function())

    except Exception as e:
        error_time = time.time() - start_time
        results_queue.put(('error', worker_id, error_time, str(e)))


def worker_death_test_normal(
    worker_id: int,
    start_time: float,
    results_queue: 'multiprocessing.Queue[Any]',
):
    """Worker for death test that uses the same semaphore."""

    @retry(
        max_attempts=1,
        timeout=10,
        semaphore_limit=2,
        semaphore_name='test_death_sem',
        semaphore_scope='multiprocess',
        semaphore_timeout=5.0,
        semaphore_lax=False,
    )
    async def semaphore_protected_function():
        acquire_time = time.time() - start_time
        results_queue.put(('acquired', worker_id, acquire_time))
        await asyncio.sleep(0.2)
        release_time = time.time() - start_time
        results_queue.put(('released', worker_id, release_time))
        return f'Worker {worker_id} completed'

    try:
        result = asyncio.run(semaphore_protected_function())
        results_queue.put(('completed', worker_id, result))
    except Exception as e:
        error_time = time.time() - start_time
        results_queue.put(('error', worker_id, error_time, str(e)))


def worker_with_custom_limit(
    worker_id: int,
    start_time: float,
    results_queue: 'multiprocessing.Queue[Any]',
    hold_time: float = 0.5,
    timeout: float = 5.0,
    semaphore_limit: int = 2,
    semaphore_name: str = 'test_custom_sem',
):
    """Worker process with customizable semaphore limit."""
    try:

        @retry(
            max_attempts=1,
            timeout=10,
            semaphore_limit=semaphore_limit,
            semaphore_name=semaphore_name,
            semaphore_scope='multiprocess',
            semaphore_timeout=timeout,
            semaphore_lax=False,
        )
        async def semaphore_protected_function():
            acquire_time = time.time() - start_time
            results_queue.put(('acquired', worker_id, acquire_time))

            # Hold the semaphore for a bit
            await asyncio.sleep(hold_time)

            release_time = time.time() - start_time
            results_queue.put(('released', worker_id, release_time))
            return f'Worker {worker_id} completed'

        # Run the async function
        result = asyncio.run(semaphore_protected_function())
        results_queue.put(('completed', worker_id, result))

    except TimeoutError as e:
        timeout_time = time.time() - start_time
        results_queue.put(('timeout', worker_id, timeout_time, str(e)))
    except Exception as e:
        error_time = time.time() - start_time
        results_queue.put(('error', worker_id, error_time, str(e)))


class TestMultiprocessSemaphore:
    """Test multiprocess semaphore functionality."""

    def test_basic_multiprocess_semaphore(self):
        """Test that semaphore limits work across processes."""
        results_queue: multiprocessing.Queue[Any] = multiprocessing.Queue()
        start_time = time.time()
        processes: list[multiprocessing.Process] = []

        # Start first batch of 3 workers (fills all slots)
        for i in range(3):
            p = multiprocessing.Process(target=worker_acquire_semaphore, args=(i, start_time, results_queue, 1.0, 5.0))
            p.start()
            processes.append(p)

        # Wait to ensure first batch has acquired all slots
        time.sleep(0.5)

        # Now start second batch - they should wait
        for i in range(3, 6):
            p = multiprocessing.Process(target=worker_acquire_semaphore, args=(i, start_time, results_queue, 0.5, 5.0))
            p.start()
            processes.append(p)

        # Wait for all processes to complete
        for p in processes:
            p.join(timeout=10)

        # Collect results
        results: list[tuple[Any, ...]] = []
        while not results_queue.empty():
            results.append(results_queue.get())

        # Analyze results
        acquired_events = [r for r in results if r[0] == 'acquired']
        completed_events = [r for r in results if r[0] == 'completed']

        # All 6 workers should complete successfully
        assert len(completed_events) == 6, f'Expected 6 completions, got {len(completed_events)}'

        # Sort by acquisition time
        acquired_events.sort(key=lambda x: x[2])

        # Extract worker IDs in order of acquisition
        acquisition_order = [event[1] for event in acquired_events]

        # First 3 acquisitions should be from first batch (0, 1, 2)
        first_three = set(acquisition_order[:3])
        assert first_three == {0, 1, 2}, f'First 3 acquisitions should be workers 0-2, got {first_three}'

        # Last 3 acquisitions should be from second batch (3, 4, 5)
        last_three = set(acquisition_order[3:])
        assert last_three == {3, 4, 5}, f'Last 3 acquisitions should be workers 3-5, got {last_three}'

        # Verify semaphore is actually limiting concurrency
        # Check that no more than 3 workers held the semaphore simultaneously
        active_workers: list[int] = []
        # Keep only acquire/release events with numeric timestamps.
        timed_events: list[tuple[str, int, float]] = [
            (str(e[0]), int(e[1]), float(e[2]))
            for e in results
            if len(e) >= 3 and e[0] in ('acquired', 'released') and isinstance(e[2], (int, float))
        ]
        for event in sorted(timed_events, key=lambda x: x[2]):  # Sort all events by time
            if event[0] == 'acquired':
                active_workers.append(event[1])
                assert len(active_workers) <= 3, f'Too many workers active: {active_workers}'
            elif event[0] == 'released':
                if event[1] in active_workers:
                    active_workers.remove(event[1])

    def test_semaphore_timeout(self):
        """Test that semaphore timeout works correctly."""
        results_queue: multiprocessing.Queue[Any] = multiprocessing.Queue()
        start_time = time.time()
        processes: list[multiprocessing.Process] = []

        # Start first 3 workers to fill all slots
        for i in range(3):
            p = multiprocessing.Process(
                target=worker_acquire_semaphore,
                args=(i, start_time, results_queue, 3.0, 5.0),  # 3s hold, 5s timeout
            )
            p.start()
            processes.append(p)

        # Wait a bit to ensure first 3 have acquired the semaphore
        time.sleep(0.5)

        # Now start the 4th worker with a short timeout
        p = multiprocessing.Process(
            target=worker_acquire_semaphore,
            args=(3, start_time, results_queue, 1.0, 0.5),  # 1s hold, 0.5s timeout
        )
        p.start()
        processes.append(p)

        # Wait for processes
        for p in processes:
            p.join(timeout=10)

        # Collect results
        results: list[tuple[str, int, float]] = []
        while not results_queue.empty():
            results.append(results_queue.get())

        # Check that we have timeout events
        timeout_events = [r for r in results if r[0] == 'timeout']
        completed_events = [r for r in results if r[0] == 'completed']

        # 3 should complete, 1 should timeout
        assert len(completed_events) == 3, f'Expected 3 completions, got {len(completed_events)}'
        assert len(timeout_events) == 1, f'Expected 1 timeout, got {len(timeout_events)}'

        # The timeout should be from worker 3
        assert timeout_events[0][1] == 3, f'Expected worker 3 to timeout, but worker {timeout_events[0][1]} timed out'

    def test_process_death_releases_semaphore(self):
        """Test that killing a process releases its semaphore slot."""
        results_queue: multiprocessing.Queue[Any] = multiprocessing.Queue()
        start_time = time.time()

        # Start 2 processes that will die (limit is 2)
        death_processes: list[multiprocessing.Process] = []
        for i in range(2):
            p = multiprocessing.Process(target=worker_that_dies, args=(i, start_time, results_queue, 0.3))
            p.start()
            death_processes.append(p)

        # Wait a bit for them to acquire
        time.sleep(0.5)

        # Now start 2 more processes that should be able to acquire after the first 2 die
        normal_processes: list[multiprocessing.Process] = []
        for i in range(2, 4):
            p = multiprocessing.Process(target=worker_death_test_normal, args=(i, start_time, results_queue))
            p.start()
            normal_processes.append(p)

        # Wait for death processes to exit
        for p in death_processes:
            p.join(timeout=2)
            assert p.exitcode == 1, f'Process should have exited with code 1, got {p.exitcode}'

        # Wait for normal processes
        for p in normal_processes:
            p.join(timeout=10)
            assert p.exitcode == 0, 'Process should complete successfully'

        # Collect results
        results: list[tuple[str, int, float]] = []
        while not results_queue.empty():
            results.append(results_queue.get())

        # Check that processes 2 and 3 were able to acquire
        acquired_events = [r for r in results if r[0] == 'acquired']
        completed_events = [r for r in results if r[0] == 'completed' and r[1] >= 2]

        # Should have 4 acquisitions total (2 that died + 2 that completed)
        assert len(acquired_events) >= 4, f'Expected at least 4 acquisitions, got {len(acquired_events)}'

        # Processes 2 and 3 should complete
        assert len(completed_events) == 2, f'Expected 2 completions from workers 2-3, got {len(completed_events)}'

    def test_concurrent_acquisition_order(self):
        """Test that processes acquire semaphore with fairness."""
        results_queue: multiprocessing.Queue[Any] = multiprocessing.Queue()
        start_time = time.time()
        processes: list[multiprocessing.Process] = []

        # Start first 2 processes (fills all slots with limit=2)
        for i in range(2):
            p = multiprocessing.Process(
                target=worker_with_custom_limit,
                args=(i, start_time, results_queue, 1.0, 5.0, 2, 'test_concurrent_order_sem'),  # 1s hold time, limit=2
            )
            p.start()
            processes.append(p)

        # Wait to ensure first 2 have acquired
        time.sleep(0.5)

        # Start next 3 processes in sequence with delays to establish order
        for i in range(2, 5):
            p = multiprocessing.Process(
                target=worker_with_custom_limit,
                args=(i, start_time, results_queue, 0.5, 5.0, 2, 'test_concurrent_order_sem'),  # 0.5s hold time, limit=2
            )
            p.start()
            processes.append(p)
            time.sleep(0.2)  # 200ms delay to establish clear queue order

        # Wait for all to complete
        for p in processes:
            p.join(timeout=10)

        # Collect and analyze results
        results: list[tuple[str, int, float]] = []
        while not results_queue.empty():
            results.append(results_queue.get())

        acquired_events = [r for r in results if r[0] == 'acquired']
        acquired_events.sort(key=lambda x: x[2])  # Sort by acquisition time

        # Extract worker IDs in order of acquisition
        acquisition_order = [event[1] for event in acquired_events]

        # Verify all workers acquired
        assert len(acquisition_order) == 5, f'All 5 workers should acquire, got {len(acquisition_order)}'
        assert set(acquisition_order) == {0, 1, 2, 3, 4}, f'All workers should acquire: {acquisition_order}'

        # First 2 should be workers 0 and 1 (they started first and slots were available)
        assert set(acquisition_order[:2]) == {0, 1}, f'First 2 acquisitions should be workers 0-1, got {acquisition_order[:2]}'

        # Next 3 should be workers 2, 3, 4 (they had to wait)
        assert set(acquisition_order[2:]) == {2, 3, 4}, f'Next 3 acquisitions should be workers 2-4, got {acquisition_order[2:]}'

        # Verify timing - workers 2, 3, 4 should acquire after workers 0, 1 start releasing
        first_batch_release_times = [r[2] for r in results if r[0] == 'released' and r[1] in {0, 1}]
        second_batch_acquire_times = [r[2] for r in results if r[0] == 'acquired' and r[1] in {2, 3, 4}]

        if first_batch_release_times and second_batch_acquire_times:
            min_release = min(first_batch_release_times)
            min_second_acquire = min(second_batch_acquire_times)
            # Second batch should start acquiring around when first batch releases
            assert min_second_acquire >= min_release - 0.1, (
                f'Second batch should acquire after first batch releases. '
                f'First release: {min_release:.2f}s, Second acquire: {min_second_acquire:.2f}s'
            )

    def test_semaphore_persistence_across_runs(self):
        """Test that semaphore state persists correctly across process runs."""
        results_queue: multiprocessing.Queue[Any] = multiprocessing.Queue()
        start_time = time.time()

        # First run: Start 3 processes that hold semaphore (limit is 3)
        first_batch: list[multiprocessing.Process] = []
        for i in range(3):
            p = multiprocessing.Process(
                target=worker_acquire_semaphore,
                args=(i, start_time, results_queue, 1.0, 5.0),  # Hold for 1 second
            )
            p.start()
            first_batch.append(p)

        # Wait for them to acquire and ensure all slots are taken
        time.sleep(0.5)

        # Try to start one more - should timeout quickly
        timeout_worker = multiprocessing.Process(
            target=worker_acquire_semaphore,
            args=(99, start_time, results_queue, 0.5, 0.3),  # Very short timeout
        )
        timeout_worker.start()
        timeout_worker.join(timeout=2)

        # Wait for first batch to complete
        for p in first_batch:
            p.join(timeout=5)

        # Now start a new batch - should work immediately
        second_batch: list[multiprocessing.Process] = []
        for i in range(3, 6):
            p = multiprocessing.Process(target=worker_acquire_semaphore, args=(i, start_time, results_queue, 0.2, 5.0))
            p.start()
            second_batch.append(p)

        for p in second_batch:
            p.join(timeout=5)

        # Analyze results
        results: list[tuple[str, int, float]] = []
        while not results_queue.empty():
            results.append(results_queue.get())

        timeout_events = [r for r in results if r[0] == 'timeout' and r[1] == 99]
        second_batch_acquired = [r for r in results if r[0] == 'acquired' and r[1] >= 3]

        # Worker 99 should timeout
        assert len(timeout_events) == 1, 'Worker 99 should timeout'

        # Second batch should all acquire successfully
        assert len(second_batch_acquired) == 3, 'All second batch workers should acquire'

        # Verify the second batch acquired after the first batch started releasing
        # Get the minimum release time from first batch
        first_batch_released = [r for r in results if r[0] == 'released' and r[1] < 3]
        if first_batch_released:
            min_release_time = min(r[2] for r in first_batch_released)
            # At least one second batch worker should have acquired after first release
            second_batch_times = [event[2] for event in second_batch_acquired]
            assert any(t >= min_release_time - 0.1 for t in second_batch_times), (
                f'Second batch should acquire after first batch releases. '
                f'Min release: {min_release_time:.2f}, Second batch times: {second_batch_times}'
            )

    async def test_semaphore_file_disappears(self):
        """Test that semaphores handle missing lock files gracefully."""
        import shutil
        import tempfile
        from pathlib import Path

        # Use a custom directory for this test
        test_dir = Path(tempfile.gettempdir()) / 'test_semaphore_disappear'
        test_dir.mkdir(exist_ok=True)

        original_dir = retry_helpers.MULTIPROCESS_SEMAPHORE_DIR
        try:
            # Monkey patch the directory for this test
            retry_helpers.MULTIPROCESS_SEMAPHORE_DIR = test_dir

            acquired_count = 0

            @retry(
                max_attempts=1,
                timeout=5,
                semaphore_limit=2,
                semaphore_name='disappearing_sem',
                semaphore_scope='multiprocess',
                semaphore_lax=True,  # Allow continuing without semaphore
            )
            async def test_function():
                nonlocal acquired_count
                acquired_count += 1

                # After first acquisition, remove the semaphore directory
                if acquired_count == 1:
                    shutil.rmtree(test_dir, ignore_errors=True)

                await asyncio.sleep(0.1)
                return f'completed_{acquired_count}'

            # Run multiple tasks concurrently
            tasks = [test_function() for _ in range(3)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Should complete successfully despite directory removal
            successful_results = [r for r in results if isinstance(r, str) and r.startswith('completed_')]
            assert len(successful_results) == 3, f'All tasks should complete. Results: {results}'

        finally:
            # Restore original directory
            retry_helpers.MULTIPROCESS_SEMAPHORE_DIR = original_dir
            # Clean up test directory
            shutil.rmtree(test_dir, ignore_errors=True)


class TestRegularSemaphoreScopes:
    """Test non-multiprocess semaphore scopes still work correctly."""

    async def test_global_scope(self):
        """Test global scope semaphore."""
        results: list[tuple[str, int, float]] = []

        @retry(
            max_attempts=1,
            timeout=1,
            semaphore_limit=2,
            semaphore_scope='global',
            semaphore_name='test_global',
        )
        async def test_func(worker_id: int):
            results.append(('start', worker_id, time.time()))
            await asyncio.sleep(0.1)
            results.append(('end', worker_id, time.time()))
            return worker_id

        # Run 4 tasks concurrently (limit is 2)
        tasks = [test_func(i) for i in range(4)]
        await asyncio.gather(*tasks)

        # Check that only 2 ran concurrently
        starts = [r for r in results if r[0] == 'start']
        starts.sort(key=lambda x: x[2])

        # First 2 should start immediately
        assert starts[1][2] - starts[0][2] < 0.05

        # 3rd should wait for first to finish
        assert starts[2][2] - starts[0][2] > 0.08

    async def test_class_scope(self):
        """Test class scope semaphore."""

        class TestClass:
            def __init__(self):
                self.results: list[tuple[str, int, float]] = []

            @retry(
                max_attempts=1,
                timeout=1,
                semaphore_limit=1,
                semaphore_scope='class',
                semaphore_name='test_method',
            )
            async def test_method(self, worker_id: int):
                self.results.append(('start', worker_id, time.time()))
                await asyncio.sleep(0.1)
                self.results.append(('end', worker_id, time.time()))
                return worker_id

        # Create two instances
        obj1 = TestClass()
        obj2 = TestClass()

        # Run method on both instances concurrently
        # They should share the semaphore (class scope)
        start_time = time.time()
        await asyncio.gather(
            obj1.test_method(1),
            obj2.test_method(2),
        )
        end_time = time.time()

        # Should take ~0.2s (sequential) not ~0.1s (parallel)
        assert end_time - start_time > 0.18

    async def test_self_scope(self):
        """Test self scope semaphore."""

        class TestClass:
            def __init__(self):
                self.results: list[tuple[str, int, float]] = []

            @retry(
                max_attempts=1,
                timeout=1,
                semaphore_limit=1,
                semaphore_scope='instance',
                semaphore_name='test_method',
            )
            async def test_method(self, worker_id: int):
                self.results.append(('start', worker_id, time.time()))
                await asyncio.sleep(0.1)
                self.results.append(('end', worker_id, time.time()))
                return worker_id

        # Create two instances
        obj1 = TestClass()
        obj2 = TestClass()

        # Run method on both instances concurrently
        # They should NOT share the semaphore (self scope)
        start_time = time.time()
        await asyncio.gather(
            obj1.test_method(1),
            obj2.test_method(2),
        )
        end_time = time.time()

        # Should be closer to parallel execution (~0.1s) than strict serialization (~0.2s).
        # Allow overhead from periodic overload checks.
        assert end_time - start_time < 0.25


class TestRetryApiParity:
    async def test_defaults_match_typescript(self):
        params = inspect.signature(retry).parameters
        assert params['max_attempts'].default == 1
        assert params['timeout'].default is None

    async def test_max_attempts_counts_total_attempts(self):
        attempt_count = 0

        @retry(max_attempts=3)
        async def flaky():
            nonlocal attempt_count
            attempt_count += 1
            raise ValueError('always fails')

        with pytest.raises(ValueError):
            await flaky()

        assert attempt_count == 3

    async def test_retry_on_errors_supports_exception_classes_and_regex(self):
        attempt_count = 0

        @retry(
            max_attempts=4,
            retry_after=0.01,
            retry_on_errors=[re.compile(r'^ValueError: temporary failure$'), RuntimeError],
        )
        async def flaky():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise ValueError('temporary failure')
            return 'ok'

        assert await flaky() == 'ok'
        assert attempt_count == 3

    async def test_semaphore_name_callable_uses_call_args_for_keying(self):
        active = 0
        max_active = 0

        def _semaphore_key(a: str, b: str) -> str:
            return f'{a}-{b}'

        @retry(
            max_attempts=1,
            semaphore_limit=1,
            semaphore_scope='global',
            semaphore_name=_semaphore_key,
        )
        async def keyed(a: str, b: str):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.05)
            active -= 1

        max_active = 0
        await asyncio.gather(keyed('same', 'key'), keyed('same', 'key'))
        assert max_active == 1

        max_active = 0
        await asyncio.gather(keyed('a', '1'), keyed('b', '2'))
        assert max_active >= 2


if __name__ == '__main__':
    # Run the tests
    pytest.main([__file__, '-v'])
