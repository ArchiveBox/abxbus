"""
Tests for ContextVar propagation through event dispatch and handler execution.

This addresses GitHub issue #20: ContextVar values set before dispatch should
be accessible inside event handlers.

The key insight is that context must be captured at DISPATCH time (when the
user calls bus.emit()), not at PROCESSING time (when the event is pulled
from the queue and handlers are executed).
"""

# pyright: reportUnusedVariable=false
# pyright: reportUnusedFunction=false

import asyncio
from contextvars import ContextVar
from typing import Any

import pytest

from abxbus import BaseEvent, EventBus

# Test context variables (simulating user-defined context like request_id)
request_id_var: ContextVar[str] = ContextVar('request_id', default='<unset>')
user_id_var: ContextVar[str] = ContextVar('user_id', default='<unset>')
trace_id_var: ContextVar[str] = ContextVar('trace_id', default='<unset>')


class SimpleEvent(BaseEvent[str]):
    """Simple event for context propagation tests."""

    pass


class ChildEvent(BaseEvent[str]):
    """Child event for nested context tests."""

    pass


class TestContextPropagation:
    """Test that ContextVar values propagate from dispatch site to handlers."""

    async def test_contextvar_propagates_to_handler(self):
        """
        Basic test: ContextVar set before dispatch should be accessible in handler.

        This is the core issue from GitHub #20.
        """
        bus = EventBus(name='ContextTestBus')
        captured_values: dict[str, str] = {}

        async def handler(event: SimpleEvent) -> str:
            # These should have the values set BEFORE dispatch, not defaults
            captured_values['request_id'] = request_id_var.get()
            captured_values['user_id'] = user_id_var.get()
            return 'handled'

        bus.on(SimpleEvent, handler)

        try:
            # Set context values (simulating FastAPI request context)
            request_id_var.set('req-12345')
            user_id_var.set('user-abc')

            # Dispatch and await
            event = await bus.emit(SimpleEvent())

            # Handler should have seen the context values
            assert captured_values['request_id'] == 'req-12345', f"Expected 'req-12345', got '{captured_values['request_id']}'"
            assert captured_values['user_id'] == 'user-abc', f"Expected 'user-abc', got '{captured_values['user_id']}'"

        finally:
            await bus.stop(clear=True)

    async def test_contextvar_propagates_through_nested_handlers(self):
        """
        Nested dispatch: Context should propagate through parent -> child handlers.

        When a handler dispatches and awaits a child event, the child handler
        should also have access to the original context.
        """
        bus = EventBus(name='NestedContextBus')
        captured_parent: dict[str, str] = {}
        captured_child: dict[str, str] = {}

        async def parent_handler(event: SimpleEvent) -> str:
            captured_parent['request_id'] = request_id_var.get()
            captured_parent['trace_id'] = trace_id_var.get()

            # Dispatch child event
            child = await event.emit(ChildEvent())
            return 'parent_done'

        async def child_handler(event: ChildEvent) -> str:
            captured_child['request_id'] = request_id_var.get()
            captured_child['trace_id'] = trace_id_var.get()
            return 'child_done'

        bus.on(SimpleEvent, parent_handler)
        bus.on(ChildEvent, child_handler)

        try:
            # Set context
            request_id_var.set('req-nested-123')
            trace_id_var.set('trace-xyz')

            await bus.emit(SimpleEvent())

            # Both handlers should see the context
            assert captured_parent['request_id'] == 'req-nested-123'
            assert captured_parent['trace_id'] == 'trace-xyz'
            assert captured_child['request_id'] == 'req-nested-123'
            assert captured_child['trace_id'] == 'trace-xyz'

        finally:
            await bus.stop(clear=True)

    async def test_context_isolation_between_dispatches(self):
        """
        Different dispatches should have isolated contexts.

        If dispatch A sets request_id='req-A' and dispatch B sets request_id='req-B',
        handler A should see 'req-A' and handler B should see 'req-B'.
        """
        bus = EventBus(name='IsolationTestBus')
        captured_values: list[str] = []

        async def handler(event: SimpleEvent) -> str:
            # Small delay to ensure both handlers run
            await asyncio.sleep(0.01)
            captured_values.append(request_id_var.get())
            return 'handled'

        bus.on(SimpleEvent, handler)

        try:
            # Dispatch two events with different contexts
            async def dispatch_with_context(req_id: str):
                request_id_var.set(req_id)
                await bus.emit(SimpleEvent())

            # Run both dispatches
            request_id_var.set('req-A')
            event_a = bus.emit(SimpleEvent())

            request_id_var.set('req-B')
            event_b = bus.emit(SimpleEvent())

            await event_a
            await event_b

            # Each handler should have seen its own context
            # Note: order might vary, so just check both values are present
            assert 'req-A' in captured_values, f"Expected 'req-A' in {captured_values}"
            assert 'req-B' in captured_values, f"Expected 'req-B' in {captured_values}"

        finally:
            await bus.stop(clear=True)

    async def test_context_propagates_to_parallel_handler_concurrency(self):
        """
        When event_handler_concurrency='parallel', all handlers should see the dispatch context.
        """
        bus = EventBus(name='ParallelContextBus', event_handler_concurrency='parallel')
        captured_values: list[str] = []
        lock = asyncio.Lock()

        async def handler1(event: SimpleEvent) -> str:
            async with lock:
                captured_values.append(f'h1:{request_id_var.get()}')
            return 'h1_done'

        async def handler2(event: SimpleEvent) -> str:
            async with lock:
                captured_values.append(f'h2:{request_id_var.get()}')
            return 'h2_done'

        bus.on(SimpleEvent, handler1)
        bus.on(SimpleEvent, handler2)

        try:
            request_id_var.set('req-parallel')
            await bus.emit(SimpleEvent())

            assert 'h1:req-parallel' in captured_values, f"Handler1 didn't see context: {captured_values}"
            assert 'h2:req-parallel' in captured_values, f"Handler2 didn't see context: {captured_values}"

        finally:
            await bus.stop(clear=True)

    async def test_context_propagates_through_event_forwarding(self):
        """
        When events are forwarded between buses, context should propagate.
        """
        bus1 = EventBus(name='Bus1')
        bus2 = EventBus(name='Bus2')
        captured_bus1: dict[str, str] = {}
        captured_bus2: dict[str, str] = {}

        async def bus1_handler(event: SimpleEvent) -> str:
            captured_bus1['request_id'] = request_id_var.get()
            return 'bus1_done'

        async def bus2_handler(event: SimpleEvent) -> str:
            captured_bus2['request_id'] = request_id_var.get()
            return 'bus2_done'

        bus1.on(SimpleEvent, bus1_handler)
        bus1.on('*', bus2.emit)  # Forward all events to bus2
        bus2.on(SimpleEvent, bus2_handler)

        try:
            request_id_var.set('req-forwarded')
            await bus1.emit(SimpleEvent())
            await bus2.wait_until_idle()

            assert captured_bus1['request_id'] == 'req-forwarded', f"Bus1 handler didn't see context: {captured_bus1}"
            assert captured_bus2['request_id'] == 'req-forwarded', f"Bus2 handler didn't see context: {captured_bus2}"

        finally:
            await bus1.stop(clear=True)
            await bus2.stop(clear=True)

    async def test_handler_can_modify_context_without_affecting_parent(self):
        """
        Handler modifications to ContextVar should not affect the parent context.

        This ensures context is properly copied, not shared.
        """
        bus = EventBus(name='ModifyContextBus')
        parent_value_after_child: str = ''

        async def parent_handler(event: SimpleEvent) -> str:
            nonlocal parent_value_after_child
            # Set a value in parent
            request_id_var.set('parent-value')

            # Dispatch child which will modify the context
            await event.emit(ChildEvent())

            # Parent's context should be unchanged
            parent_value_after_child = request_id_var.get()
            return 'parent_done'

        async def child_handler(event: ChildEvent) -> str:
            # Modify context in child
            request_id_var.set('child-modified')
            return 'child_done'

        bus.on(SimpleEvent, parent_handler)
        bus.on(ChildEvent, child_handler)

        try:
            await bus.emit(SimpleEvent())

            # Parent should still see its own value, not child's modification
            assert parent_value_after_child == 'parent-value', (
                f"Parent context was modified by child: got '{parent_value_after_child}'"
            )

        finally:
            await bus.stop(clear=True)

    async def test_event_parent_id_tracking_still_works(self):
        """
        Critical: Internal context vars (event_parent_id tracking) must still work
        when we propagate dispatch-time context.

        This ensures our context merging doesn't break the abxbus internals.
        """
        bus = EventBus(name='ParentIdTrackingBus')
        parent_event_id: str | None = None
        child_event_parent_id: str | None = None

        async def parent_handler(event: SimpleEvent) -> str:
            nonlocal parent_event_id
            parent_event_id = event.event_id

            # Child event should get parent_id set through the explicit child API.
            child = await event.emit(ChildEvent())
            return 'parent_done'

        async def child_handler(event: ChildEvent) -> str:
            nonlocal child_event_parent_id
            child_event_parent_id = event.event_parent_id
            return 'child_done'

        bus.on(SimpleEvent, parent_handler)
        bus.on(ChildEvent, child_handler)

        try:
            # Set user context (to ensure we're testing the merge scenario)
            request_id_var.set('req-parent-tracking')

            await bus.emit(SimpleEvent())

            # Verify parent ID tracking works
            assert parent_event_id is not None, 'Parent event ID was not captured'
            assert child_event_parent_id is not None, 'Child event parent ID was not set'
            assert child_event_parent_id == parent_event_id, (
                f"Child's parent_id ({child_event_parent_id}) doesn't match parent's id ({parent_event_id})"
            )

        finally:
            await bus.stop(clear=True)

    async def test_dispatch_context_and_parent_id_both_work(self):
        """
        Both user-defined ContextVars AND internal event tracking must work together.

        This is the key test for context stacking/merging.
        """
        bus = EventBus(name='CombinedContextBus')
        results: dict[str, Any] = {}

        async def parent_handler(event: SimpleEvent) -> str:
            results['parent_request_id'] = request_id_var.get()
            results['parent_event_id'] = event.event_id

            # Dispatch child - should get both user context AND parent tracking
            child = await event.emit(ChildEvent())
            return 'parent_done'

        async def child_handler(event: ChildEvent) -> str:
            results['child_request_id'] = request_id_var.get()
            results['child_event_parent_id'] = event.event_parent_id
            return 'child_done'

        bus.on(SimpleEvent, parent_handler)
        bus.on(ChildEvent, child_handler)

        try:
            # Set user context
            request_id_var.set('req-combined-test')

            await bus.emit(SimpleEvent())

            # User context should propagate
            assert results['parent_request_id'] == 'req-combined-test', (
                f"Parent didn't see user context: {results['parent_request_id']}"
            )
            assert results['child_request_id'] == 'req-combined-test', (
                f"Child didn't see user context: {results['child_request_id']}"
            )

            # Internal parent tracking should also work
            assert results['child_event_parent_id'] == results['parent_event_id'], (
                f'Parent ID tracking broken: child.parent_id={results["child_event_parent_id"]}, parent.id={results["parent_event_id"]}'
            )

        finally:
            await bus.stop(clear=True)

    async def test_deeply_nested_context_and_parent_tracking(self):
        """
        Test that both user context and parent tracking work through multiple levels.
        """
        bus = EventBus(name='DeepNestingBus')
        results: list[dict[str, Any]] = []

        class Level2Event(BaseEvent[str]):
            pass

        class Level3Event(BaseEvent[str]):
            pass

        async def level1_handler(event: SimpleEvent) -> str:
            results.append(
                {
                    'level': 1,
                    'request_id': request_id_var.get(),
                    'event_id': event.event_id,
                    'parent_id': event.event_parent_id,
                }
            )
            await event.emit(Level2Event())
            return 'level1_done'

        async def level2_handler(event: Level2Event) -> str:
            results.append(
                {
                    'level': 2,
                    'request_id': request_id_var.get(),
                    'event_id': event.event_id,
                    'parent_id': event.event_parent_id,
                }
            )
            await event.emit(Level3Event())
            return 'level2_done'

        async def level3_handler(event: Level3Event) -> str:
            results.append(
                {
                    'level': 3,
                    'request_id': request_id_var.get(),
                    'event_id': event.event_id,
                    'parent_id': event.event_parent_id,
                }
            )
            return 'level3_done'

        bus.on(SimpleEvent, level1_handler)
        bus.on(Level2Event, level2_handler)
        bus.on(Level3Event, level3_handler)

        try:
            request_id_var.set('req-deep-nesting')

            await bus.emit(SimpleEvent())

            # All levels should see the user context
            assert len(results) == 3, f'Expected 3 levels, got {len(results)}'
            for r in results:
                assert r['request_id'] == 'req-deep-nesting', f"Level {r['level']} didn't see user context: {r['request_id']}"

            # Parent chain should be correct
            assert results[0]['parent_id'] is None, 'Level 1 should have no parent'
            assert results[1]['parent_id'] == results[0]['event_id'], (
                f'Level 2 parent mismatch: {results[1]["parent_id"]} != {results[0]["event_id"]}'
            )
            assert results[2]['parent_id'] == results[1]['event_id'], (
                f'Level 3 parent mismatch: {results[2]["parent_id"]} != {results[1]["event_id"]}'
            )

        finally:
            await bus.stop(clear=True)


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
