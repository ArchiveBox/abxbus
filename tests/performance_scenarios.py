from __future__ import annotations

import asyncio
import gc
import math
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import psutil

from abxbus import BaseEvent, EventBus

TRIM_TARGET = 1
HISTORY_LIMIT_STREAM = 512
HISTORY_LIMIT_ON_OFF = 128
HISTORY_LIMIT_EPHEMERAL_BUS = 128
HISTORY_LIMIT_FIXED_HANDLERS = 128
HISTORY_LIMIT_WORST_CASE = 128
RUNTIME_PERF_FIXED_HANDLER_COUNT = 5_000
WORST_CASE_IMMEDIATE_TIMEOUT_MS = 0.0001
WORST_CASE_IMMEDIATE_TIMEOUT_SECONDS = WORST_CASE_IMMEDIATE_TIMEOUT_MS / 1000.0


@dataclass(slots=True)
class PerfLimits:
    single_run_ms: float = 120_000.0
    worst_case_ms: float = 180_000.0


@dataclass(slots=True)
class PerfInput:
    runtime_name: str = 'python'
    log: Callable[[str], None] = print
    now: Callable[[], float] = lambda: time.perf_counter() * 1000.0
    limits: PerfLimits = field(default_factory=PerfLimits)

    async def sleep(self, ms: float) -> None:
        await asyncio.sleep(ms / 1000.0)

    def force_gc(self) -> None:
        gc.collect()

    def get_memory_usage(self) -> dict[str, int]:
        process = psutil.Process(os.getpid())
        return {'rss': int(process.memory_info().rss)}

    def get_cpu_time_ms(self) -> float:
        process = psutil.Process(os.getpid())
        cpu = process.cpu_times()
        return float((cpu.user + cpu.system) * 1000.0)


@dataclass(slots=True)
class MemoryTracker:
    hooks: PerfInput
    baseline_rss: int = 0
    peak_rss: int = 0

    def __post_init__(self) -> None:
        snapshot = self.hooks.get_memory_usage()
        self.baseline_rss = snapshot['rss']
        self.peak_rss = snapshot['rss']

    def sample(self) -> None:
        snapshot = self.hooks.get_memory_usage()
        if snapshot['rss'] > self.peak_rss:
            self.peak_rss = snapshot['rss']

    def peak_rss_kb_per_event(self, events: int) -> float | None:
        if events <= 0:
            return None
        delta = float(max(0, self.peak_rss - self.baseline_rss))
        return (delta / 1024.0) / float(events)


class PerfSimpleEvent(BaseEvent[int]):
    batch_id: int = 0
    value: int = 0


class PerfTrimEvent(BaseEvent[None]):
    pass


class PerfTrimEphemeralEvent(BaseEvent[None]):
    pass


class PerfFixedHandlersEvent(BaseEvent[int]):
    base_value: int = 0


class PerfTrimFixedHandlersEvent(BaseEvent[None]):
    pass


class PerfRequestEvent(BaseEvent[int]):
    value: int = 0


class PerfTrimOnOffEvent(BaseEvent[None]):
    pass


class WCParent(BaseEvent[int]):
    iteration: int = 0
    value: int = 0


class WCChild(BaseEvent[int]):
    iteration: int = 0
    value: int = 0


class WCGrandchild(BaseEvent[int]):
    iteration: int = 0
    value: int = 0


class WCTrimEvent(BaseEvent[None]):
    pass


def _format_ms_per_event(value: float, unit: str = 'event') -> str:
    return f'{value:.3f}ms/{unit}'


def _format_kb_per_unit(value: float, unit: str = 'event') -> str:
    return f'{value:.3f}kb/{unit}'


def _format_ms(value: float) -> str:
    return f'{value:.3f}ms'


async def _wait_for_runtime_settle(hooks: PerfInput) -> None:
    await hooks.sleep(50)


async def _trim_bus_history_to_one_event(bus: EventBus, trim_event_type: type[BaseEvent[Any]]) -> None:
    prev_size = bus.event_history.max_history_size
    prev_drop = bus.event_history.max_history_drop
    bus.event_history.max_history_size = TRIM_TARGET
    bus.event_history.max_history_drop = True
    ev = bus.emit(trim_event_type())
    await ev
    await bus.wait_until_idle()
    assert len(bus.event_history) <= TRIM_TARGET, f'trim-to-1 failed for {bus}: {len(bus.event_history)}/{TRIM_TARGET}'
    bus.event_history.max_history_size = prev_size
    bus.event_history.max_history_drop = prev_drop


async def _dispatch_naive(
    bus: EventBus,
    events: list[BaseEvent[Any]],
    on_dispatched: Callable[[BaseEvent[Any]], None] | None = None,
) -> tuple[list[BaseEvent[Any]], str | None]:
    queued: list[BaseEvent[Any]] = []
    error: str | None = None

    for event in events:
        try:
            queued_event = bus.emit(event)
            queued.append(queued_event)
            if on_dispatched is not None:
                on_dispatched(queued_event)
        except Exception as exc:
            error = f'{type(exc).__name__}: {exc}'
            break

    if queued:
        await asyncio.gather(*queued, return_exceptions=True)
        await bus.wait_until_idle()

    return queued, error


def _scenario_result(
    *,
    scenario: str,
    total_events: int,
    total_ms: float,
    ms_per_event: float,
    ms_per_event_unit: str,
    peak_rss_kb_per_event: float | None,
    peak_rss_unit: str = 'event',
    throughput: int,
    ok: bool,
    error: str | None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        'scenario': scenario,
        'ok': ok,
        'error': error,
        'total_events': total_events,
        'total_ms': total_ms,
        'ms_per_event': ms_per_event,
        'ms_per_event_unit': ms_per_event_unit,
        'ms_per_event_label': _format_ms_per_event(ms_per_event, ms_per_event_unit),
        'peak_rss_kb_per_event': peak_rss_kb_per_event,
        'peak_rss_unit': peak_rss_unit,
        'peak_rss_kb_per_event_label': (
            None if peak_rss_kb_per_event is None else _format_kb_per_unit(peak_rss_kb_per_event, peak_rss_unit)
        ),
        'throughput': throughput,
    }
    if extra:
        result.update(extra)
    return result


def _record(hooks: PerfInput, metrics: dict[str, Any]) -> None:
    parts = [
        f'events={metrics.get("total_events", "n/a")}',
        f'total={_format_ms(float(metrics.get("total_ms", 0.0)))}',
        f'latency={_format_ms_per_event(float(metrics.get("ms_per_event", 0.0)), str(metrics.get("ms_per_event_unit", "event")))}',
    ]
    peak_rss = metrics.get('peak_rss_kb_per_event')
    if isinstance(peak_rss, (int, float)):
        parts.append(f'peak_rss={_format_kb_per_unit(float(peak_rss), str(metrics.get("peak_rss_unit", "event")))}')
    parts.append(f'throughput={int(metrics.get("throughput", 0))}/s')
    parts.append(f'ok={"yes" if metrics.get("ok", False) else "no"}')
    if metrics.get('error'):
        parts.append(f'error={metrics["error"]}')
    if isinstance(metrics.get('cpu_ms'), (int, float)):
        parts.append(f'cpu={_format_ms(float(metrics["cpu_ms"]))}')
    if isinstance(metrics.get('cpu_ms_per_event'), (int, float)):
        parts.append(
            f'cpu_per_unit={_format_ms_per_event(float(metrics["cpu_ms_per_event"]), str(metrics.get("ms_per_event_unit", "event")))}'
        )
    hooks.log(f'[{hooks.runtime_name}] {metrics["scenario"]}: ' + ' '.join(parts))


async def run_perf_50k_events(input: PerfInput) -> dict[str, Any]:
    hooks = input
    scenario = '50k events'
    total_events = int(50_000)
    bus = EventBus(
        name='Perf50kBus',
        max_history_size=HISTORY_LIMIT_STREAM,
        max_history_drop=True,
        middlewares=[],
    )

    processed_count: int = 0
    checksum: int = 0
    expected_checksum: int = 0
    sampled_early_event_ids: list[str] = []

    def simple_handler(event: PerfSimpleEvent) -> None:
        nonlocal processed_count, checksum
        processed_count += 1
        checksum += event.value + event.batch_id

    bus.on(PerfSimpleEvent, simple_handler)

    memory = MemoryTracker(hooks)
    t0 = hooks.now()
    cpu_t0 = hooks.get_cpu_time_ms()

    batch_size = 512
    dispatched_events: int = 0
    dispatch_error: str | None = None
    while dispatched_events < total_events:
        queued_batch: list[BaseEvent[Any]] = []
        this_batch = min(batch_size, total_events - dispatched_events)
        for _ in range(this_batch):
            i = dispatched_events
            batch_id = i // batch_size
            value = (i % 97) + 1
            expected_checksum += value + batch_id
            try:
                queued_event = bus.emit(PerfSimpleEvent(batch_id=batch_id, value=value))
                queued_batch.append(queued_event)
                if len(sampled_early_event_ids) < 64:
                    sampled_early_event_ids.append(queued_event.event_id)
                dispatched_events += 1
            except Exception as exc:
                dispatch_error = f'{type(exc).__name__}: {exc}'
                break
        if queued_batch:
            await asyncio.gather(*queued_batch, return_exceptions=True)
            await bus.wait_until_idle()
        if dispatched_events % 2048 == 0:
            memory.sample()
        if dispatch_error is not None:
            break

    memory.sample()

    await _trim_bus_history_to_one_event(bus, PerfTrimEvent)
    t1 = hooks.now()
    cpu_t1 = hooks.get_cpu_time_ms()
    await _wait_for_runtime_settle(hooks)
    memory.sample()

    total_ms = t1 - t0
    ms_denominator = max(dispatched_events, 1)
    ms_per_event = total_ms / float(ms_denominator)
    throughput = int(round(dispatched_events / max(total_ms / 1000.0, 1e-9)))
    peak_rss_kb_per_event = memory.peak_rss_kb_per_event(ms_denominator)
    cpu_ms = max(0.0, cpu_t1 - cpu_t0)
    cpu_ms_per_event = cpu_ms / float(ms_denominator)

    expected_for_dispatched = 0
    for i in range(dispatched_events):
        batch_id = i // 512
        value = (i % 97) + 1
        expected_for_dispatched += value + batch_id

    sampled_evicted_count = sum(1 for event_id in sampled_early_event_ids if event_id not in bus.event_history)
    ok = (
        dispatch_error is None
        and dispatched_events == total_events
        and processed_count == dispatched_events
        and checksum == expected_for_dispatched
    )

    result = _scenario_result(
        scenario=scenario,
        total_events=dispatched_events,
        total_ms=total_ms,
        ms_per_event=ms_per_event,
        ms_per_event_unit='event',
        peak_rss_kb_per_event=peak_rss_kb_per_event,
        throughput=throughput,
        ok=ok,
        error=dispatch_error,
        extra={
            'attempted_events': total_events,
            'processed_count': processed_count,
            'checksum': checksum,
            'expected_checksum': expected_for_dispatched,
            'sampled_evicted_count': sampled_evicted_count,
            'cpu_ms': cpu_ms,
            'cpu_ms_per_event': cpu_ms_per_event,
        },
    )

    await bus.destroy(clear=True)
    _record(hooks, result)
    return result


async def run_perf_ephemeral_buses(input: PerfInput) -> dict[str, Any]:
    hooks = input
    scenario = '500 buses x 100 events'
    total_buses = int(500)
    events_per_bus = int(100)
    attempted_events = int(total_buses * events_per_bus)

    processed_count: int = 0
    checksum: int = 0
    expected_checksum: int = 0
    dispatched_events: int = 0
    first_error: str | None = None

    memory = MemoryTracker(hooks)
    t0 = hooks.now()
    cpu_t0 = hooks.get_cpu_time_ms()

    for bus_index in range(total_buses):
        bus = EventBus(
            name=f'PerfEphemeralBus_{bus_index}',
            max_history_size=HISTORY_LIMIT_EPHEMERAL_BUS,
            max_history_drop=True,
            middlewares=[],
        )

        def bus_handler(event: PerfSimpleEvent) -> None:
            nonlocal processed_count, checksum
            processed_count += 1
            checksum += (event.batch_id * 7) + event.value

        bus.on(PerfSimpleEvent, bus_handler)

        events: list[BaseEvent[Any]] = []
        for i in range(events_per_bus):
            value = ((bus_index * events_per_bus + i) % 89) + 1
            events.append(PerfSimpleEvent(batch_id=bus_index, value=value))

        queued, err = await _dispatch_naive(bus, events)
        dispatched_events += len(queued)
        for i in range(len(queued)):
            value = ((bus_index * events_per_bus + i) % 89) + 1
            expected_checksum += (bus_index * 7) + value

        if err and first_error is None:
            first_error = err

        memory.sample()
        await _trim_bus_history_to_one_event(bus, PerfTrimEphemeralEvent)
        await bus.destroy(clear=True)

        if bus_index % 10 == 0:
            memory.sample()

    total_ms = hooks.now() - t0
    cpu_t1 = hooks.get_cpu_time_ms()
    await _wait_for_runtime_settle(hooks)
    memory.sample()

    ms_denominator = max(dispatched_events, 1)
    ms_per_event = total_ms / float(ms_denominator)
    peak_rss_kb_per_event = memory.peak_rss_kb_per_event(ms_denominator)
    throughput = int(round(dispatched_events / max(total_ms / 1000.0, 1e-9)))
    cpu_ms = max(0.0, cpu_t1 - cpu_t0)
    cpu_ms_per_event = cpu_ms / float(ms_denominator)

    ok = (
        first_error is None
        and dispatched_events == attempted_events
        and processed_count == dispatched_events
        and checksum == expected_checksum
    )

    result = _scenario_result(
        scenario=scenario,
        total_events=dispatched_events,
        total_ms=total_ms,
        ms_per_event=ms_per_event,
        ms_per_event_unit='event',
        peak_rss_kb_per_event=peak_rss_kb_per_event,
        throughput=throughput,
        ok=ok,
        error=first_error,
        extra={
            'attempted_events': attempted_events,
            'processed_count': processed_count,
            'checksum': checksum,
            'expected_checksum': expected_checksum,
            'cpu_ms': cpu_ms,
            'cpu_ms_per_event': cpu_ms_per_event,
        },
    )

    _record(hooks, result)
    return result


async def run_perf_single_event_many_fixed_handlers(input: PerfInput) -> dict[str, Any]:
    hooks = input
    scenario = '1 event x 5k parallel handlers'
    total_events = int(1)
    total_handlers = int(RUNTIME_PERF_FIXED_HANDLER_COUNT)
    bus = EventBus(
        name='PerfFixedHandlersBus',
        max_history_size=HISTORY_LIMIT_FIXED_HANDLERS,
        max_history_drop=True,
        event_handler_concurrency='parallel',
        middlewares=[],
    )

    processed_count: int = 0
    checksum: int = 0
    base_value = 11
    expected_checksum: int = 0

    for i in range(total_handlers):
        weight = (i % 29) + 1
        expected_checksum += base_value + weight

        def make_handler(local_weight: int, index: int) -> Callable[[PerfFixedHandlersEvent], None]:
            def fixed_handler(event: PerfFixedHandlersEvent) -> None:
                nonlocal processed_count, checksum
                processed_count += 1
                checksum += event.base_value + local_weight

            fixed_handler.__name__ = f'fixed_handler_{index}'
            return fixed_handler

        bus.on(PerfFixedHandlersEvent, make_handler(weight, i))

    memory = MemoryTracker(hooks)
    t0 = hooks.now()
    cpu_t0 = hooks.get_cpu_time_ms()

    error: str | None = None
    try:
        event = bus.emit(PerfFixedHandlersEvent(base_value=base_value))
        await event
        await bus.wait_until_idle()
    except Exception as exc:
        error = f'{type(exc).__name__}: {exc}'

    total_ms = hooks.now() - t0
    cpu_t1 = hooks.get_cpu_time_ms()
    await _wait_for_runtime_settle(hooks)
    memory.sample()

    ms_per_event = total_ms / float(max(total_handlers, 1))
    peak_rss_kb_per_event = memory.peak_rss_kb_per_event(total_handlers)
    throughput = int(round(total_events / max(total_ms / 1000.0, 1e-9)))
    cpu_ms = max(0.0, cpu_t1 - cpu_t0)
    cpu_ms_per_event = cpu_ms / float(max(total_handlers, 1))

    ok = error is None and processed_count == total_handlers and checksum == expected_checksum

    result = _scenario_result(
        scenario=scenario,
        total_events=total_events,
        total_ms=total_ms,
        ms_per_event=ms_per_event,
        ms_per_event_unit='handler',
        peak_rss_kb_per_event=peak_rss_kb_per_event,
        peak_rss_unit='handler',
        throughput=throughput,
        ok=ok,
        error=error,
        extra={
            'processed_count': processed_count,
            'checksum': checksum,
            'expected_checksum': expected_checksum,
            'total_handlers': total_handlers,
            'cpu_ms': cpu_ms,
            'cpu_ms_per_event': cpu_ms_per_event,
        },
    )

    await _trim_bus_history_to_one_event(bus, PerfTrimFixedHandlersEvent)
    await bus.destroy(clear=True)
    _record(hooks, result)
    return result


async def run_perf_on_off_churn(input: PerfInput) -> dict[str, Any]:
    hooks = input
    scenario = '50k one-off handlers over 50k events'
    total_events = int(50_000)
    bus = EventBus(
        name='PerfOnOffBus',
        max_history_size=HISTORY_LIMIT_ON_OFF,
        max_history_drop=True,
        middlewares=[],
    )

    processed_count: int = 0
    checksum: int = 0
    expected_checksum: int = 0
    error: str | None = None
    event_key = PerfRequestEvent.__name__

    memory = MemoryTracker(hooks)
    t0 = hooks.now()
    cpu_t0 = hooks.get_cpu_time_ms()

    for i in range(total_events):
        weight = (i % 13) + 1
        value = (i % 101) + 1
        expected_checksum += value + weight

        def one_off_handler(event: PerfRequestEvent) -> None:
            nonlocal processed_count, checksum
            processed_count += 1
            checksum += event.value + weight

        handler_entry = bus.on(PerfRequestEvent, one_off_handler)

        try:
            ev = bus.emit(PerfRequestEvent(value=value))
            await ev
        except Exception as exc:
            error = f'{type(exc).__name__}: {exc}'
            break

        bus.off(event_key, handler_entry.id)

        if i % 1000 == 0:
            memory.sample()

    await bus.wait_until_idle()
    total_ms = hooks.now() - t0
    cpu_t1 = hooks.get_cpu_time_ms()
    await _wait_for_runtime_settle(hooks)
    memory.sample()

    ms_denominator = max(processed_count, 1)
    ms_per_event = total_ms / float(ms_denominator)
    peak_rss_kb_per_event = memory.peak_rss_kb_per_event(ms_denominator)
    throughput = int(round(processed_count / max(total_ms / 1000.0, 1e-9)))
    cpu_ms = max(0.0, cpu_t1 - cpu_t0)
    cpu_ms_per_event = cpu_ms / float(ms_denominator)

    ok = (
        error is None
        and processed_count == total_events
        and checksum == expected_checksum
        and len(bus.handlers_by_key.get(event_key, [])) == 0
    )

    result = _scenario_result(
        scenario=scenario,
        total_events=processed_count,
        total_ms=total_ms,
        ms_per_event=ms_per_event,
        ms_per_event_unit='event',
        peak_rss_kb_per_event=peak_rss_kb_per_event,
        throughput=throughput,
        ok=ok,
        error=error,
        extra={
            'attempted_events': total_events,
            'processed_count': processed_count,
            'checksum': checksum,
            'expected_checksum': expected_checksum,
            'cpu_ms': cpu_ms,
            'cpu_ms_per_event': cpu_ms_per_event,
        },
    )

    await _trim_bus_history_to_one_event(bus, PerfTrimOnOffEvent)
    await bus.destroy(clear=True)
    _record(hooks, result)
    return result


async def run_perf_worst_case(input: PerfInput) -> dict[str, Any]:
    hooks = input
    scenario = 'worst-case forwarding + timeouts'
    total_iterations = int(500)
    history_limit = HISTORY_LIMIT_WORST_CASE
    bus_a = EventBus(name='PerfWorstCaseA', max_history_size=history_limit, max_history_drop=True, middlewares=[])
    bus_b = EventBus(name='PerfWorstCaseB', max_history_size=history_limit, max_history_drop=True, middlewares=[])
    bus_c = EventBus(name='PerfWorstCaseC', max_history_size=history_limit, max_history_drop=True, middlewares=[])

    parent_handled_a: int = 0
    parent_handled_b: int = 0
    child_handled: int = 0
    grandchild_handled: int = 0
    timeout_count: int = 0
    cancel_count: int = 0
    checksum: int = 0
    error: str | None = None

    def parent_b_handler(event: WCParent) -> None:
        nonlocal parent_handled_b, checksum
        parent_handled_b += 1
        checksum += event.value + 3

    async def child_handler(event: WCChild) -> None:
        nonlocal child_handled, checksum
        child_handled += 1
        checksum += (event.value * 2) + event.iteration
        gc_event = event.emit(WCGrandchild(iteration=event.iteration, value=event.value + 1))
        if event.event_timeout is not None:
            await hooks.sleep(0)
        await gc_event

    def grandchild_handler(event: WCGrandchild) -> None:
        nonlocal grandchild_handled, checksum
        grandchild_handled += 1
        checksum += (event.value * 3) + event.iteration

    bus_b.on(WCParent, parent_b_handler)
    bus_c.on(WCChild, child_handler)
    bus_c.on(WCGrandchild, grandchild_handler)

    memory = MemoryTracker(hooks)
    t0 = hooks.now()
    cpu_t0 = hooks.get_cpu_time_ms()

    try:
        for iteration in range(total_iterations):
            should_timeout = iteration % 5 == 0
            value = (iteration % 37) + 1

            async def ephemeral_handler(event: WCParent) -> None:
                nonlocal parent_handled_a, checksum
                parent_handled_a += 1
                checksum += event.value + 11
                child = event.emit(
                    WCChild(
                        iteration=event.iteration,
                        value=event.value,
                        # event_timeout is in seconds; mirror TS near-zero timeout value.
                        event_timeout=WORST_CASE_IMMEDIATE_TIMEOUT_SECONDS if should_timeout else None,
                    )
                )
                bus_c.emit(child)
                try:
                    await child
                except Exception:
                    pass

            ephemeral_entry = bus_a.on(WCParent, ephemeral_handler)
            parent = WCParent(iteration=iteration, value=value)
            ev_a = bus_a.emit(parent)
            bus_b.emit(parent)
            await ev_a
            bus_a.off(WCParent, ephemeral_entry.id)

            if iteration % 10 == 0:
                await bus_a.find(WCParent, future=0.001)
            if iteration % 5 == 0:
                memory.sample()

        await bus_a.wait_until_idle()
        await bus_b.wait_until_idle()
        await bus_c.wait_until_idle()
    except Exception as exc:
        error = f'{type(exc).__name__}: {exc}'

    for event in bus_c.event_history.values():
        for event_result in event.event_results.values():
            if isinstance(event_result.error, TimeoutError):
                timeout_count += 1
            if isinstance(event_result.error, asyncio.CancelledError):
                cancel_count += 1

    total_ms = hooks.now() - t0
    cpu_t1 = hooks.get_cpu_time_ms()
    estimated_events = total_iterations * 3
    ms_per_event = total_ms / float(max(estimated_events, 1))
    peak_rss_kb_per_event = memory.peak_rss_kb_per_event(max(estimated_events, 1))
    cpu_ms = max(0.0, cpu_t1 - cpu_t0)
    cpu_ms_per_event = cpu_ms / float(max(estimated_events, 1))

    ok = (
        error is None
        and parent_handled_a == total_iterations
        and parent_handled_b == total_iterations
        and len(bus_a.handlers_by_key.get(WCParent.__name__, [])) == 0
    )

    result = _scenario_result(
        scenario=scenario,
        total_events=estimated_events,
        total_ms=total_ms,
        ms_per_event=ms_per_event,
        ms_per_event_unit='event',
        peak_rss_kb_per_event=peak_rss_kb_per_event,
        throughput=int(round(estimated_events / max(total_ms / 1000.0, 1e-9))),
        ok=ok,
        error=error,
        extra={
            'parent_handled_a': parent_handled_a,
            'parent_handled_b': parent_handled_b,
            'child_handled': child_handled,
            'grandchild_handled': grandchild_handled,
            'timeout_count': timeout_count,
            'cancel_count': cancel_count,
            'checksum': checksum,
            'cpu_ms': cpu_ms,
            'cpu_ms_per_event': cpu_ms_per_event,
        },
    )

    await _trim_bus_history_to_one_event(bus_a, WCTrimEvent)
    await _trim_bus_history_to_one_event(bus_b, WCTrimEvent)
    await _trim_bus_history_to_one_event(bus_c, WCTrimEvent)
    await _wait_for_runtime_settle(hooks)

    await bus_a.destroy(clear=True)
    await bus_b.destroy(clear=True)
    await bus_c.destroy(clear=True)

    _record(hooks, result)
    return result


PERF_SCENARIO_RUNNERS: dict[str, Callable[[PerfInput], Any]] = {
    '50k-events': run_perf_50k_events,
    '500-buses-x-100-events': run_perf_ephemeral_buses,
    '1-event-x-50k-parallel-handlers': run_perf_single_event_many_fixed_handlers,
    '50k-one-off-handlers': run_perf_on_off_churn,
    'worst-case-forwarding-timeouts': run_perf_worst_case,
}

PERF_SCENARIO_IDS = tuple(PERF_SCENARIO_RUNNERS.keys())


async def run_perf_scenario_by_id(input: PerfInput, scenario_id: str) -> dict[str, Any]:
    scenario = PERF_SCENARIO_RUNNERS.get(scenario_id)
    if scenario is None:
        raise ValueError(f'unknown perf scenario {scenario_id!r}, expected one of: {", ".join(PERF_SCENARIO_IDS)}')

    try:
        result = await scenario(input)
    except Exception as exc:
        result = {
            'scenario': scenario_id,
            'ok': False,
            'error': f'{type(exc).__name__}: {exc}',
            'total_events': 0,
            'total_ms': 0.0,
            'ms_per_event': 0.0,
            'ms_per_event_unit': 'event',
            'throughput': 0,
            'peak_rss_kb_per_event': None,
            'peak_rss_kb_per_event_label': None,
        }

    heap_delta_after_gc_mb = await _measure_heap_delta_after_gc(input)
    if heap_delta_after_gc_mb is not None:
        result['heap_delta_after_gc_mb'] = round(heap_delta_after_gc_mb, 3)
        input.log(f'[{input.runtime_name}] {result["scenario"]}: heap_delta_after_gc={result["heap_delta_after_gc_mb"]:.3f}mb')

    return result


async def run_all_perf_scenarios(input: PerfInput) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for scenario_id in PERF_SCENARIO_IDS:
        results.append(await run_perf_scenario_by_id(input, scenario_id))
    return results


async def _measure_heap_delta_after_gc(input: PerfInput) -> float | None:
    process = psutil.Process(os.getpid())
    before = float(process.memory_info().rss)

    for _ in range(4):
        input.force_gc()
        await input.sleep(15)

    after = float(process.memory_info().rss)
    delta_mb = max(0.0, (after - before) / (1024.0 * 1024.0))
    if math.isnan(delta_mb):
        return None
    return delta_mb
