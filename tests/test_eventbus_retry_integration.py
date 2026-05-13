import asyncio
import time

from abxbus import BaseEvent, EventBus
from abxbus.retry import retry


class TestRetryWithEventBus:
    """Test @retry decorator with EventBus handlers."""

    async def test_retry_decorator_on_eventbus_handler(self):
        """Test that @retry decorator works correctly when applied to EventBus handlers."""
        handler_calls: list[tuple[str, float]] = []

        class TestEvent(BaseEvent[str]):
            """Simple test event."""

            message: str

        bus = EventBus(name='test_retry_bus')

        @retry(
            max_attempts=3,
            retry_after=0.1,
            timeout=1.0,
            semaphore_limit=1,
            semaphore_scope='global',
        )
        async def retrying_handler(event: TestEvent) -> str:
            call_time = time.time()
            handler_calls.append(('called', call_time))

            if len(handler_calls) < 3:
                raise ValueError(f'Attempt {len(handler_calls)} failed')

            return f'Success: {event.message}'

        bus.on('TestEvent', retrying_handler)

        event = TestEvent(message='Hello retry!')
        completed_event = await bus.emit(event)
        await bus.wait_until_idle(timeout=5)

        assert len(handler_calls) == 3, f'Expected 3 attempts, got {len(handler_calls)}'
        for i in range(1, len(handler_calls)):
            delay = handler_calls[i][1] - handler_calls[i - 1][1]
            assert delay >= 0.08, f'Retry delay {i} was {delay:.3f}s, expected >= 0.08s'

        assert completed_event.event_status == 'completed'
        handler_result = await completed_event.event_result()
        assert handler_result == 'Success: Hello retry!'

        await bus.destroy()

    async def test_retry_with_semaphore_on_multiple_handlers(self):
        """Test @retry decorator with semaphore limiting concurrent handler executions."""
        active_handlers: list[int] = []
        max_concurrent = 0
        handler_results: dict[int, list[tuple[str, float]]] = {1: [], 2: [], 3: [], 4: []}

        class WorkEvent(BaseEvent[str]):
            """Event that triggers work."""

            work_id: int

        bus = EventBus(name='test_concurrent_bus', event_handler_concurrency='parallel')

        def create_handler(handler_id: int):
            @retry(
                max_attempts=1,
                timeout=5.0,
                semaphore_limit=2,
                semaphore_name='test_handler_sem',
                semaphore_scope='global',
            )
            async def limited_handler(event: WorkEvent) -> str:
                nonlocal max_concurrent
                active_handlers.append(handler_id)
                handler_results[handler_id].append(('started', time.time()))

                current_concurrent = len(active_handlers)
                max_concurrent = max(max_concurrent, current_concurrent)
                await asyncio.sleep(0.2)

                active_handlers.remove(handler_id)
                handler_results[handler_id].append(('completed', time.time()))
                return f'Handler {handler_id} processed work {event.work_id}'

            limited_handler.__name__ = f'limited_handler_{handler_id}'
            return limited_handler

        for i in range(1, 5):
            handler = create_handler(i)
            bus.on('WorkEvent', handler)

        event = WorkEvent(work_id=1)
        await bus.emit(event)
        await bus.wait_until_idle(timeout=3)

        assert max_concurrent == 2, f'Max concurrent was {max_concurrent}, expected exactly 2 with semaphore_limit=2'
        for handler_id in range(1, 5):
            assert len(handler_results[handler_id]) == 2, f'Handler {handler_id} should have started and completed'

        await bus.destroy()

    async def test_retry_timeout_with_eventbus_handler(self):
        """Test that retry timeout works correctly with EventBus handlers."""

        class TimeoutEvent(BaseEvent[str]):
            """Event for timeout testing."""

            test_id: str
            event_timeout: float | None = 1

        bus = EventBus(name='test_timeout_bus')
        handler_started = False

        @retry(
            max_attempts=1,
            timeout=0.2,
        )
        async def wrapped_handler(event: TimeoutEvent) -> str:
            nonlocal handler_started
            handler_started = True
            await asyncio.sleep(5)
            return 'Should not reach here'

        bus.on(TimeoutEvent, wrapped_handler)

        event = TimeoutEvent(test_id='7ebbd9f4-755a-7f13-828a-183dfe2d4302')
        await bus.emit(event)
        await bus.wait_until_idle(timeout=2)

        assert handler_started, 'Handler should have started'
        assert len(event.event_results) == 1
        result = next(iter(event.event_results.values()))
        assert result.status == 'error'
        assert result.error is not None
        assert isinstance(result.error, TimeoutError)

        await bus.destroy()

    async def test_retry_with_event_type_filter(self):
        """Test retry decorator with specific exception types."""

        class RetryTestEvent(BaseEvent[str]):
            """Event for testing retry on specific exceptions."""

            attempt_limit: int

        bus = EventBus(name='test_exception_filter_bus')
        attempt_count = 0

        @retry(
            max_attempts=4,
            retry_after=0.05,
            timeout=1.0,
            retry_on_errors=[ValueError, RuntimeError],
        )
        async def selective_retry_handler(event: RetryTestEvent) -> str:
            nonlocal attempt_count
            attempt_count += 1

            if attempt_count == 1:
                raise ValueError('This should be retried')
            if attempt_count == 2:
                raise RuntimeError('This should also be retried')
            if attempt_count == 3:
                raise TypeError('This should NOT be retried')

            return 'Success'

        bus.on('RetryTestEvent', selective_retry_handler)

        event = RetryTestEvent(attempt_limit=3)
        await bus.emit(event)
        await bus.wait_until_idle(timeout=2)

        assert attempt_count == 3, f'Expected 3 attempts, got {attempt_count}'
        handler_id = list(event.event_results.keys())[0]
        result = event.event_results[handler_id]
        assert result.status == 'error'
        assert isinstance(result.error, TypeError)
        assert 'This should NOT be retried' in str(result.error)

        await bus.destroy()

    async def test_retry_decorated_method_class_scope_serializes_across_instances(self):
        """Class scope semaphore should serialize bound method handlers across instances."""

        class ScopeClassEvent(BaseEvent[str]):
            pass

        bus = EventBus(name='test_scope_class_bus', event_handler_concurrency='parallel')
        active = 0
        max_active = 0

        class SomeService:
            @retry(
                max_attempts=1,
                semaphore_scope='class',
                semaphore_limit=1,
                semaphore_name='on_scope_class_event',
            )
            async def on_scope_class_event(self, _event: ScopeClassEvent) -> str:
                nonlocal active, max_active
                active += 1
                max_active = max(max_active, active)
                await asyncio.sleep(0.05)
                active -= 1
                return 'ok'

        service_a = SomeService()
        service_b = SomeService()
        bus.on(ScopeClassEvent, service_a.on_scope_class_event)
        bus.on(ScopeClassEvent, service_b.on_scope_class_event)

        event = await bus.emit(ScopeClassEvent())
        await event.wait()

        assert max_active == 1, f'class scope should serialize across instances, got max_active={max_active}'
        await bus.destroy()

    async def test_retry_decorated_method_instance_scope_allows_parallel_across_instances(self):
        """Instance scope semaphore should allow bound handlers from different instances to overlap."""

        class ScopeInstanceEvent(BaseEvent[str]):
            pass

        bus = EventBus(name='test_scope_instance_bus', event_handler_concurrency='parallel')
        active = 0
        max_active = 0
        calls = 0

        class SomeService:
            @retry(
                max_attempts=1,
                semaphore_scope='instance',
                semaphore_limit=1,
                semaphore_name='on_scope_instance_event',
            )
            async def on_scope_instance_event(self, _event: ScopeInstanceEvent) -> str:
                nonlocal active, max_active, calls
                active += 1
                max_active = max(max_active, active)
                calls += 1
                await asyncio.sleep(0.05)
                active -= 1
                return 'ok'

        service_a = SomeService()
        service_b = SomeService()
        bus.on(ScopeInstanceEvent, service_a.on_scope_instance_event)
        bus.on(ScopeInstanceEvent, service_b.on_scope_instance_event)

        event = await bus.emit(ScopeInstanceEvent())
        await event.wait()

        assert calls == 2, f'expected both handlers to run, got calls={calls}'
        assert max_active == 2, f'instance scope should allow overlap across instances, got max_active={max_active}'
        await bus.destroy()

    async def test_retry_decorated_method_global_scope_serializes_all_bound_handlers(self):
        """Global scope semaphore should serialize bound method handlers across all instances."""

        class ScopeGlobalEvent(BaseEvent[str]):
            pass

        bus = EventBus(name='test_scope_global_bus', event_handler_concurrency='parallel')
        active = 0
        max_active = 0

        class SomeService:
            @retry(
                max_attempts=1,
                semaphore_scope='global',
                semaphore_limit=1,
                semaphore_name='on_scope_global_event',
            )
            async def on_scope_global_event(self, _event: ScopeGlobalEvent) -> str:
                nonlocal active, max_active
                active += 1
                max_active = max(max_active, active)
                await asyncio.sleep(0.05)
                active -= 1
                return 'ok'

        service_a = SomeService()
        service_b = SomeService()
        bus.on(ScopeGlobalEvent, service_a.on_scope_global_event)
        bus.on(ScopeGlobalEvent, service_b.on_scope_global_event)

        event = await bus.emit(ScopeGlobalEvent())
        await event.wait()

        assert max_active == 1, f'global scope should serialize all handlers, got max_active={max_active}'
        await bus.destroy()

    async def test_retry_hof_bind_after_wrap_instance_scope_preserves_instance_isolation(self):
        """HOF pattern retry(...)(fn) then bind to instances should keep instance-scope isolation."""

        class HofBindEvent(BaseEvent[str]):
            pass

        bus = EventBus(name='test_hof_bind_bus', event_handler_concurrency='parallel')
        active = 0
        max_active = 0

        @retry(
            max_attempts=1,
            semaphore_scope='instance',
            semaphore_limit=1,
            semaphore_name='hof_bind_handler',
        )
        async def handler(self: object, _event: HofBindEvent) -> str:
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.05)
            active -= 1
            return 'ok'

        class Holder:
            pass

        holder_a = Holder()
        holder_b = Holder()
        bus.on(HofBindEvent, handler.__get__(holder_a, Holder))
        bus.on(HofBindEvent, handler.__get__(holder_b, Holder))

        event = await bus.emit(HofBindEvent())
        await event.wait()

        assert max_active == 2, f'bind-after-wrap instance scope should allow overlap, got max_active={max_active}'
        await bus.destroy()

    async def test_retry_wrapping_emit_retries_full_dispatch_cycle(self):
        """Retry wrapper around emit+wait should retry full event dispatch when handler errors."""

        class TabsEvent(BaseEvent[str]):
            pass

        class DOMEvent(BaseEvent[str]):
            pass

        class ScreenshotEvent(BaseEvent[str]):
            pass

        bus = EventBus(name='test_retry_emit_bus', event_handler_concurrency='parallel')
        tabs_attempts = 0
        dom_calls = 0
        screenshot_calls = 0

        async def tabs_handler(_event: TabsEvent) -> str:
            nonlocal tabs_attempts
            tabs_attempts += 1
            if tabs_attempts < 3:
                raise RuntimeError(f'tabs fail attempt {tabs_attempts}')
            return 'tabs ok'

        async def dom_handler(_event: DOMEvent) -> str:
            nonlocal dom_calls
            dom_calls += 1
            return 'dom ok'

        async def screenshot_handler(_event: ScreenshotEvent) -> str:
            nonlocal screenshot_calls
            screenshot_calls += 1
            return 'screenshot ok'

        bus.on(TabsEvent, tabs_handler)
        bus.on(DOMEvent, dom_handler)
        bus.on(ScreenshotEvent, screenshot_handler)

        @retry(max_attempts=4)
        async def emit_tabs_with_retry() -> TabsEvent:
            tabs_event = await bus.emit(TabsEvent())
            await tabs_event.wait()
            failed_results = [result for result in tabs_event.event_results.values() if result.status == 'error']
            if failed_results:
                first_error = failed_results[0].error
                if isinstance(first_error, Exception):
                    raise first_error
                raise RuntimeError(f'tabs emit failed with non-exception error payload: {first_error!r}')
            return tabs_event

        async def emit_and_wait(event: BaseEvent[str]):
            emitted = await bus.emit(event)
            await emitted.wait()
            return emitted

        tabs_event, dom_event, screenshot_event = await asyncio.gather(
            emit_tabs_with_retry(),
            emit_and_wait(DOMEvent()),
            emit_and_wait(ScreenshotEvent()),
        )

        assert tabs_attempts == 3, f'expected 3 attempts for tabs flow, got {tabs_attempts}'
        assert tabs_event.event_status == 'completed'
        assert dom_calls == 1
        assert screenshot_calls == 1
        assert dom_event.event_status == 'completed'
        assert screenshot_event.event_status == 'completed'
        await bus.destroy()
