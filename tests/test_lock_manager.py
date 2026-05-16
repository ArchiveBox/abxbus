import asyncio
from typing import Any

# pyright: reportPrivateUsage=false
import pytest

from abxbus import BaseEvent, EventBus, EventConcurrencyMode, EventHandlerConcurrencyMode, EventResult
from abxbus.lock_manager import ReentrantLock


async def test_wait_until_idle_behaves_correctly() -> None:
    class LockManagerIdleEvent(BaseEvent[str]):
        pass

    bus = EventBus(name='IdleBus')
    calls = 0

    async def handler(_event: LockManagerIdleEvent) -> str:
        nonlocal calls
        calls += 1
        return 'ok'

    bus.on(LockManagerIdleEvent, handler)

    event = bus.emit(LockManagerIdleEvent())
    await bus.wait_until_idle(timeout=1)

    assert calls == 1
    assert event.event_status == 'completed'
    assert len(event.event_results) == 1
    for result in event.event_results.values():
        assert result.status == 'completed'
        assert result.result == 'ok'

    await bus.destroy()


async def test_reentrant_lock_nested_context_reuses_single_permit() -> None:
    lock = ReentrantLock()

    assert lock.locked() is False
    async with lock:
        assert lock.locked() is True
        async with lock:
            assert lock.locked() is True
        assert lock.locked() is True
    assert lock.locked() is False


async def test_reentrant_lock_serializes_across_tasks() -> None:
    lock = ReentrantLock()
    active = 0
    max_active = 0

    async def worker() -> None:
        nonlocal active, max_active
        async with lock:
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            active -= 1

    await asyncio.gather(*(worker() for _ in range(4)))
    assert max_active == 1


async def test_lock_manager_get_lock_for_event_modes() -> None:
    bus = EventBus(name='LockManagerEventModesBus', event_concurrency='bus-serial')
    event = BaseEvent(event_type='LockModesEvent')

    assert bus.locks.get_lock_for_event(bus, event) is bus.event_bus_serial_lock

    event.event_concurrency = EventConcurrencyMode.GLOBAL_SERIAL
    assert bus.locks.get_lock_for_event(bus, event) is bus.event_global_serial_lock

    event.event_concurrency = EventConcurrencyMode.PARALLEL
    assert bus.locks.get_lock_for_event(bus, event) is None

    await bus.destroy()


async def test_lock_manager_get_lock_for_event_handler_modes() -> None:
    bus = EventBus(name='LockManagerHandlerModesBus', event_handler_concurrency='serial')
    event = BaseEvent(event_type='LockHandlerModesEvent')

    assert event._get_handler_lock() is None  # pyright: ignore[reportPrivateUsage]
    event_result = event.event_result_update(handler=lambda _event: None)
    handler_lock = bus.locks.get_lock_for_event_handler(bus, event, event_result)
    assert handler_lock is not None
    assert event._get_handler_lock() is handler_lock  # pyright: ignore[reportPrivateUsage]

    event.event_handler_concurrency = EventHandlerConcurrencyMode.PARALLEL
    handler_result = event.event_result_update(handler=lambda _event: None)
    assert bus.locks.get_lock_for_event_handler(bus, event, handler_result) is None

    await bus.destroy()


async def test_run_with_event_lock_and_handler_lock_respect_parallel_bypass() -> None:
    bus = EventBus(
        name='LockManagerBypassBus',
        event_concurrency='bus-serial',
        event_handler_concurrency='serial',
    )

    parallel_event: BaseEvent[Any] = BaseEvent(
        event_type='ParallelBypassEvent',
        event_concurrency=EventConcurrencyMode.PARALLEL,
        event_handler_concurrency=EventHandlerConcurrencyMode.PARALLEL,
    )

    async with bus.locks._run_with_event_lock(bus, parallel_event):
        assert bus.event_bus_serial_lock.locked() is False

    parallel_result: EventResult[Any] = parallel_event.event_result_update(handler=lambda _event: None)
    async with bus.locks._run_with_handler_lock(bus, parallel_event, parallel_result):
        assert parallel_event._get_handler_lock() is None  # pyright: ignore[reportPrivateUsage]

    serial_event: BaseEvent[Any] = BaseEvent(event_type='SerialAcquireEvent')
    async with bus.locks._run_with_event_lock(bus, serial_event):
        assert bus.event_bus_serial_lock.locked() is True

    serial_result: EventResult[Any] = serial_event.event_result_update(handler=lambda _event: None)
    async with bus.locks._run_with_handler_lock(bus, serial_event, serial_result):
        lock = serial_event._get_handler_lock()  # pyright: ignore[reportPrivateUsage]
        assert lock is not None
        assert lock.locked() is True

    lock = serial_event._get_handler_lock()  # pyright: ignore[reportPrivateUsage]
    assert lock is not None
    assert lock.locked() is False

    await bus.destroy()


async def test_handler_dispatch_context_marks_and_restores_lock_depth() -> None:
    bus = EventBus(name='LockDispatchContextBus', event_concurrency='bus-serial')
    event = BaseEvent(event_type='DispatchContextEvent')

    lock = bus.locks.get_lock_for_event(bus, event)
    assert lock is not None
    assert lock._depth() == 0  # pyright: ignore[reportPrivateUsage]

    with bus.locks._run_with_handler_dispatch_context(bus, event):
        assert lock._depth() == 1  # pyright: ignore[reportPrivateUsage]
        async with lock:
            assert lock._depth() == 2  # pyright: ignore[reportPrivateUsage]
        assert lock._depth() == 1  # pyright: ignore[reportPrivateUsage]

    assert lock._depth() == 0  # pyright: ignore[reportPrivateUsage]
    await bus.destroy()


async def test_reentrant_lock_releases_and_reraises_on_exception() -> None:
    lock = ReentrantLock()

    with pytest.raises(RuntimeError, match='reentrant-lock-error'):
        async with lock:
            assert lock.locked() is True
            raise RuntimeError('reentrant-lock-error')

    assert lock.locked() is False


async def test_run_with_event_lock_releases_and_reraises_on_exception() -> None:
    bus = EventBus(name='LockManagerEventErrorBus', event_concurrency='bus-serial')
    event: BaseEvent[Any] = BaseEvent(event_type='EventLockErrorEvent')

    lock = bus.locks.get_lock_for_event(bus, event)
    assert lock is not None
    assert lock.locked() is False

    with pytest.raises(RuntimeError, match='event-lock-error'):
        async with bus.locks._run_with_event_lock(bus, event):
            assert lock.locked() is True
            raise RuntimeError('event-lock-error')

    assert lock.locked() is False
    await bus.destroy()


async def test_run_with_handler_lock_releases_and_reraises_on_exception() -> None:
    bus = EventBus(name='LockManagerHandlerErrorBus', event_handler_concurrency='serial')
    event: BaseEvent[Any] = BaseEvent(event_type='HandlerLockErrorEvent')
    event_result: EventResult[Any] = event.event_result_update(handler=lambda _event: None)

    with pytest.raises(RuntimeError, match='handler-lock-error'):
        async with bus.locks._run_with_handler_lock(bus, event, event_result):
            lock = event._get_handler_lock()  # pyright: ignore[reportPrivateUsage]
            assert lock is not None
            assert lock.locked() is True
            raise RuntimeError('handler-lock-error')

    lock = event._get_handler_lock()  # pyright: ignore[reportPrivateUsage]
    assert lock is not None
    assert lock.locked() is False
    await bus.destroy()


async def test_handler_dispatch_context_restores_depth_and_reraises_on_exception() -> None:
    bus = EventBus(name='LockDispatchContextErrorBus', event_concurrency='bus-serial')
    event = BaseEvent(event_type='DispatchContextErrorEvent')

    lock = bus.locks.get_lock_for_event(bus, event)
    assert lock is not None
    assert lock._depth() == 0  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(RuntimeError, match='dispatch-context-error'):
        with bus.locks._run_with_handler_dispatch_context(bus, event):
            assert lock._depth() == 1  # pyright: ignore[reportPrivateUsage]
            raise RuntimeError('dispatch-context-error')

    assert lock._depth() == 0  # pyright: ignore[reportPrivateUsage]
    await bus.destroy()
