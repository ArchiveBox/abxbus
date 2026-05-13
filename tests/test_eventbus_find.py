"""
Tests for the unified find() method and tree traversal helpers.

Addresses GitHub Issues #10 (debouncing) and #15 (past + child_of lookup).
"""

# pyright: reportUnknownMemberType=false
# pyright: reportUnknownLambdaType=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnusedVariable=false

import asyncio
from datetime import UTC, datetime

import pytest

from abxbus import BaseEvent, EventBus


# Test event types
class ParentEvent(BaseEvent[str]):
    pass


class ChildEvent(BaseEvent[str]):
    pass


class GrandchildEvent(BaseEvent[str]):
    pass


class UnrelatedEvent(BaseEvent[str]):
    pass


class ScreenshotEvent(BaseEvent[str]):
    """Example event for debouncing tests."""

    target_id: str = ''
    full_page: bool = False


class NavigateEvent(BaseEvent[str]):
    """Example event for race condition tests."""

    url: str = ''


class TabCreatedEvent(BaseEvent[str]):
    """Example event that fires as result of navigation."""

    tab_id: str = ''


class SystemEvent(BaseEvent[str]):
    pass


class UserActionEvent(BaseEvent[str]):
    action: str = ''
    user_id: str = ''


class NumberedEvent(BaseEvent[str]):
    value: int = 0


TARGET_ID_1 = '9b447756-908c-7b75-8a51-4a2c2b4d9b14'
TARGET_ID_2 = '194870e1-fa02-70a4-8101-d10d57c3449c'
TARGET_ID_3 = '7d787f06-07fd-7406-8be7-0255fb41f459'
TARGET_ID_4 = 'a2c7f40b-a8a7-78b2-84ef-9f8c60c40a24'
TARGET_ID_CHILD = '12f38f3d-d8a7-7ae2-8778-bc27e285ea34'


# =============================================================================
# Tree Traversal Helper Tests
# =============================================================================


class TestEventIsChildOf:
    """Tests for event_is_child_of() method."""

    async def test_direct_child_returns_true(self):
        """event_is_child_of returns True for direct parent-child relationship."""
        bus = EventBus()

        try:
            # Create parent-child relationship via dispatch inside handler
            child_event_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                child = await event.emit(ChildEvent())
                child_event_ref.append(child)
                return 'parent_done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, lambda e: 'child_done')

            parent = await bus.emit(ParentEvent())
            await bus.wait_until_idle()

            child = child_event_ref[0]

            # Verify the relationship
            assert bus.event_is_child_of(child, parent) is True

        finally:
            await bus.destroy(clear=True)

    async def test_grandchild_returns_true(self):
        """event_is_child_of returns True for grandparent relationship."""
        bus = EventBus()

        try:
            grandchild_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                await event.emit(ChildEvent())
                return 'parent_done'

            async def child_handler(event: ChildEvent) -> str:
                grandchild = await event.emit(GrandchildEvent())
                grandchild_ref.append(grandchild)
                return 'child_done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, child_handler)
            bus.on(GrandchildEvent, lambda e: 'grandchild_done')

            parent = await bus.emit(ParentEvent())
            await bus.wait_until_idle()

            grandchild = grandchild_ref[0]

            # Grandchild should be descendant of parent
            assert bus.event_is_child_of(grandchild, parent) is True

        finally:
            await bus.destroy(clear=True)

    async def test_unrelated_events_returns_false(self):
        """event_is_child_of returns False for unrelated events."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'parent_done')
            bus.on(UnrelatedEvent, lambda e: 'unrelated_done')

            parent = await bus.emit(ParentEvent())
            unrelated = await bus.emit(UnrelatedEvent())

            assert bus.event_is_child_of(unrelated, parent) is False

        finally:
            await bus.destroy(clear=True)

    async def test_same_event_returns_false(self):
        """event_is_child_of returns False when checking event against itself."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            event = await bus.emit(ParentEvent())

            assert bus.event_is_child_of(event, event) is False

        finally:
            await bus.destroy(clear=True)

    async def test_reversed_relationship_returns_false(self):
        """event_is_child_of returns False when parent/child are reversed."""
        bus = EventBus()

        try:
            child_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                child = await event.emit(ChildEvent())
                child_ref.append(child)
                return 'parent_done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, lambda e: 'child_done')

            parent = await bus.emit(ParentEvent())
            await bus.wait_until_idle()

            child = child_ref[0]

            # Parent is NOT a child of child
            assert bus.event_is_child_of(parent, child) is False

        finally:
            await bus.destroy(clear=True)


class TestEventIsParentOf:
    """Tests for event_is_parent_of() method."""

    async def test_direct_parent_returns_true(self):
        """event_is_parent_of returns True for direct parent-child relationship."""
        bus = EventBus()

        try:
            child_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                child = await event.emit(ChildEvent())
                child_ref.append(child)
                return 'parent_done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, lambda e: 'child_done')

            parent = await bus.emit(ParentEvent())
            await bus.wait_until_idle()

            child = child_ref[0]

            # Parent IS parent of child
            assert bus.event_is_parent_of(parent, child) is True

        finally:
            await bus.destroy(clear=True)

    async def test_grandparent_returns_true(self):
        """event_is_parent_of returns True for grandparent relationship."""
        bus = EventBus()

        try:
            grandchild_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                await event.emit(ChildEvent())
                return 'parent_done'

            async def child_handler(event: ChildEvent) -> str:
                grandchild = await event.emit(GrandchildEvent())
                grandchild_ref.append(grandchild)
                return 'child_done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, child_handler)
            bus.on(GrandchildEvent, lambda e: 'grandchild_done')

            parent = await bus.emit(ParentEvent())
            await bus.wait_until_idle()

            grandchild = grandchild_ref[0]

            # Parent IS ancestor of grandchild
            assert bus.event_is_parent_of(parent, grandchild) is True

        finally:
            await bus.destroy(clear=True)


# =============================================================================
# find() Basic Functionality Tests
# =============================================================================


class TestFindPastOnly:
    """Tests for find(past=True, future=False) history lookup behavior."""

    async def test_max_history_zero_disables_past_but_future_still_works(self):
        """With max_history_size=0, future find resolves on dispatch but completed events are not searchable in past."""
        bus = EventBus(max_history_size=0)

        try:
            bus.on(ParentEvent, lambda e: 'done')

            find_future_task = asyncio.create_task(bus.find(ParentEvent, past=False, future=1))
            await asyncio.sleep(0)

            dispatched = bus.emit(ParentEvent())
            found_future = await find_future_task
            assert found_future is not None
            assert found_future.event_id == dispatched.event_id

            await dispatched
            assert dispatched.event_id not in bus.event_history

            found_past = await bus.find(ParentEvent, past=True, future=False)
            assert found_past is None
        finally:
            await bus.destroy(clear=True)

    async def test_returns_matching_event_from_history(self):
        """find(past=True, future=False) returns event from history."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch event first
            dispatched = await bus.emit(ParentEvent())

            # Find it in history (past=True = search all history)
            found = await bus.find(ParentEvent, past=True, future=False)

            assert found is not None
            assert found.event_id == dispatched.event_id

        finally:
            await bus.destroy(clear=True)

    async def test_history_lookup_is_bus_scoped(self):
        """find(past=True, future=False) only searches this bus history."""
        bus_a = EventBus(name='FindScopeA')
        bus_b = EventBus(name='FindScopeB')

        try:
            bus_b.on(NumberedEvent, lambda e: 'done')
            await bus_b.emit(NumberedEvent(value=10))

            found_on_a = await bus_a.find(NumberedEvent, past=True, future=False)
            found_on_b = await bus_b.find(NumberedEvent, past=True, future=False)

            assert found_on_a is None
            assert found_on_b is not None
            assert found_on_b.value == 10
        finally:
            await bus_a.destroy(clear=True)
            await bus_b.destroy(clear=True)

    async def test_found_event_retains_origin_bus_label(self):
        """Events returned by find() keep the bus label in event_path."""
        bus = EventBus(name='FindBusRef')

        try:
            bus.on(NumberedEvent, lambda e: 'done')
            await bus.emit(NumberedEvent(value=7))

            found = await bus.find(NumberedEvent, past=True, future=False)
            assert found is not None
            assert found.event_path
            assert found.event_path[-1] == bus.label
        finally:
            await bus.destroy(clear=True)

    async def test_past_float_filters_by_time_window(self):
        """find(past=0.1) only returns events from last 0.1 seconds."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch an event
            _old_event = await bus.emit(ParentEvent())

            # Wait a bit
            await asyncio.sleep(0.15)

            # Dispatch another event
            new_event = await bus.emit(ParentEvent())

            # With a very short past window, should only find the new event
            found = await bus.find(ParentEvent, past=0.1, future=False)
            assert found is not None
            assert found.event_id == new_event.event_id

            # With a longer past window, should still find new event (most recent first)
            found = await bus.find(ParentEvent, past=1.0, future=False)
            assert found is not None
            assert found.event_id == new_event.event_id

        finally:
            await bus.destroy(clear=True)

    async def test_past_float_returns_none_when_all_events_too_old(self):
        """find(past=0.05) returns None if all events are older than 0.05 seconds."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch an event
            await bus.emit(ParentEvent())

            # Wait longer than our window
            await asyncio.sleep(0.15)

            # With very short past window, should find nothing
            found = await bus.find(ParentEvent, past=0.05, future=False)
            assert found is None

        finally:
            await bus.destroy(clear=True)

    async def test_returns_none_when_no_match(self):
        """find(past=True, future=False) returns None when no matching event."""
        bus = EventBus()

        try:
            # No events dispatched
            found = await bus.find(ParentEvent, past=True, future=False)

            assert found is None

        finally:
            await bus.destroy(clear=True)

    async def test_respects_where_filter(self):
        """find() applies where filter correctly."""
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            # Dispatch two events with different target_ids
            await bus.emit(ScreenshotEvent(target_id=TARGET_ID_1))
            event2 = await bus.emit(ScreenshotEvent(target_id=TARGET_ID_2))

            # Find only the one with target_id='194870e1-fa02-70a4-8101-d10d57c3449c'
            found = await bus.find(
                ScreenshotEvent,
                where=lambda e: e.target_id == TARGET_ID_2,
                past=True,
                future=False,
            )

            assert found is not None
            assert found.event_id == event2.event_id

        finally:
            await bus.destroy(clear=True)

    async def test_returns_most_recent_match(self):
        """find() returns most recent matching event from history."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch multiple events
            await bus.emit(ParentEvent())
            await asyncio.sleep(0.01)  # Ensure different timestamps
            event2 = await bus.emit(ParentEvent())

            # Should return the most recent
            found = await bus.find(ParentEvent, past=True, future=False)

            assert found is not None
            assert found.event_id == event2.event_id

        finally:
            await bus.destroy(clear=True)

    async def test_past_includes_in_progress_events(self):
        """History search should include pending/started events, matching TS semantics."""
        bus = EventBus()

        try:
            release_handler = asyncio.Event()

            async def slow_handler(event: ParentEvent) -> str:
                await release_handler.wait()
                return 'done'

            bus.on(ParentEvent, slow_handler)

            dispatched = bus.emit(ParentEvent())
            await asyncio.sleep(0.02)  # Let handler start.

            found_while_running = await bus.find(ParentEvent, past=True, future=False)
            assert found_while_running is not None
            assert found_while_running.event_id == dispatched.event_id
            assert found_while_running.event_status in ('pending', 'started')

            release_handler.set()
            await dispatched
            await bus.wait_until_idle()

            found_after_completion = await bus.find(ParentEvent, past=True, future=False)
            assert found_after_completion is not None
            assert found_after_completion.event_id == dispatched.event_id
        finally:
            await bus.destroy(clear=True)

    async def test_find_default_is_past_only_no_future_wait(self):
        """find() with no windows defaults to past=True, future=False."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')
            start = datetime.now(UTC)
            found = await bus.find(ParentEvent)
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert found is None
            assert elapsed < 0.05
        finally:
            await bus.destroy(clear=True)

    async def test_find_supports_event_field_keyword_filters(self):
        """find(..., **kwargs) applies metadata equality filters."""
        bus = EventBus()

        try:
            release = asyncio.Event()

            async def slow_handler(event: ParentEvent) -> str:
                await release.wait()
                return 'done'

            bus.on(ParentEvent, slow_handler)

            in_flight = bus.emit(ParentEvent())
            await asyncio.sleep(0.02)

            pending_or_started = await bus.find(ParentEvent, past=True, future=False, event_status='started')
            if pending_or_started is None:
                pending_or_started = await bus.find(ParentEvent, past=True, future=False, event_status='pending')

            assert pending_or_started is not None
            assert pending_or_started.event_id == in_flight.event_id

            release.set()
            await in_flight
            completed = await bus.find(ParentEvent, past=True, future=False, event_status='completed')
            assert completed is not None
            assert completed.event_id == in_flight.event_id
        finally:
            await bus.destroy(clear=True)

    async def test_find_supports_event_id_and_event_timeout_filters(self):
        """find(..., **kwargs) supports exact-match metadata equality filters."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            event_a = await bus.emit(ParentEvent(event_timeout=11))
            await bus.emit(ParentEvent(event_timeout=22))

            found = await bus.find(
                ParentEvent,
                past=True,
                future=False,
                event_id=event_a.event_id,
                event_timeout=11,
            )
            assert found is not None
            assert found.event_id == event_a.event_id

            mismatch = await bus.find(
                ParentEvent,
                past=True,
                future=False,
                event_id=event_a.event_id,
                event_timeout=22,
            )
            assert mismatch is None
        finally:
            await bus.destroy(clear=True)

    async def test_find_supports_non_event_data_field_filters(self):
        """find(..., **kwargs) supports exact-match filters for non event_* fields too."""
        bus = EventBus()

        try:
            bus.on(UserActionEvent, lambda e: 'done')

            await bus.emit(UserActionEvent(action='logout', user_id='28536f9b-4031-7f53-827f-98c24c1b3839'))
            expected = await bus.emit(UserActionEvent(action='login', user_id='b57fcb67-faeb-7a56-8907-116d8cbb1472'))

            found = await bus.find(
                UserActionEvent, past=True, future=False, action='login', user_id='b57fcb67-faeb-7a56-8907-116d8cbb1472'
            )
            assert found is not None
            assert found.event_id == expected.event_id

            not_found = await bus.find(UserActionEvent, past=True, future=False, action='signup')
            assert not_found is None
        finally:
            await bus.destroy(clear=True)

    async def test_find_wildcard_with_where_filter_matches_history(self):
        """find('*', where=..., past=True) matches across event types in history."""
        bus = EventBus()

        try:
            bus.on(UserActionEvent, lambda e: 'done')
            bus.on(SystemEvent, lambda e: 'done')

            expected = await bus.emit(UserActionEvent(action='login', user_id='b57fcb67-faeb-7a56-8907-116d8cbb1472'))
            await bus.emit(SystemEvent())

            found = await bus.find(
                '*',
                where=lambda event: (
                    isinstance(event, UserActionEvent) and event.user_id == 'b57fcb67-faeb-7a56-8907-116d8cbb1472'
                ),
                past=True,
                future=False,
            )

            assert found is not None
            assert found.event_id == expected.event_id
            assert found.event_type == 'UserActionEvent'
        finally:
            await bus.destroy(clear=True)


class TestFindFutureOnly:
    """Tests for find(past=False, future=...) future wait behavior."""

    async def test_waits_for_future_event(self):
        """find(past=False, future=1) waits for event to be dispatched."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Start waiting for event
            async def dispatch_after_delay():
                await asyncio.sleep(0.05)
                return await bus.emit(ParentEvent())

            find_task = asyncio.create_task(bus.find(ParentEvent, past=False, future=1))
            dispatch_task = asyncio.create_task(dispatch_after_delay())

            found, dispatched = await asyncio.gather(find_task, dispatch_task)

            assert found is not None
            assert found.event_id == dispatched.event_id

        finally:
            await bus.destroy(clear=True)

    async def test_future_float_timeout(self):
        """find(future=0.01) times out quickly when no event."""
        bus = EventBus()

        try:
            start = datetime.now(UTC)
            found = await bus.find(ParentEvent, past=False, future=0.01)
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert found is None
            assert elapsed < 0.1  # Should timeout quickly

        finally:
            await bus.destroy(clear=True)

    async def test_ignores_past_events(self):
        """find(past=False, future=...) ignores events already in history."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch event first
            await bus.emit(ParentEvent())

            # Should NOT find it (past=False), and timeout quickly
            found = await bus.find(ParentEvent, past=False, future=0.01)

            assert found is None

        finally:
            await bus.destroy(clear=True)

    async def test_ignores_inflight_events_dispatched_before_find(self):
        """find(past=False, future=...) ignores already-dispatched in-flight events."""
        bus = EventBus()

        try:
            release = asyncio.Event()

            async def slow_handler(event: ParentEvent) -> str:
                await release.wait()
                return 'done'

            bus.on(ParentEvent, slow_handler)

            in_flight = bus.emit(ParentEvent())
            await asyncio.sleep(0.01)

            found = await bus.find(ParentEvent, past=False, future=0.05)
            assert found is None

            release.set()
            await in_flight
        finally:
            await bus.destroy(clear=True)

    async def test_future_works_with_string_event_type(self):
        """find('EventName', ...) resolves using string keys, not just model classes."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            async def dispatch_after_delay():
                await asyncio.sleep(0.05)
                return await bus.emit(ParentEvent())

            find_task = asyncio.create_task(bus.find('ParentEvent', past=False, future=1))
            dispatch_task = asyncio.create_task(dispatch_after_delay())

            found, dispatched = await asyncio.gather(find_task, dispatch_task)

            assert found is not None
            assert found.event_id == dispatched.event_id
            assert found.event_type == 'ParentEvent'
        finally:
            await bus.destroy(clear=True)

    async def test_find_wildcard_with_where_filter_waits_for_future_match(self):
        """find('*', where=..., past=False) waits for matching future event only."""
        bus = EventBus()

        try:
            bus.on(SystemEvent, lambda e: 'done')
            bus.on(UserActionEvent, lambda e: 'done')

            find_task = asyncio.create_task(
                bus.find(
                    '*',
                    where=lambda event: event.event_type == 'UserActionEvent' and getattr(event, 'action', None) == 'special',
                    past=False,
                    future=0.3,
                )
            )

            await asyncio.sleep(0.02)
            await bus.emit(SystemEvent())
            await bus.emit(UserActionEvent(action='normal', user_id='16ced2b3-de40-7d9b-85c8-c02241a00354'))
            expected = await bus.emit(UserActionEvent(action='special', user_id='391ce6ed-aa72-73d6-87c4-5e20f3c6fc63'))

            found = await find_task
            assert found is not None
            assert found.event_id == expected.event_id
            assert found.event_type == 'UserActionEvent'
        finally:
            await bus.destroy(clear=True)

    async def test_future_class_pattern_matches_generic_base_event_by_event_type(self):
        """find(SomeEventClass) should match BaseEvent(event_type='SomeEventClass')."""
        bus = EventBus()

        try:

            class DifferentNameFromClass(BaseEvent[str]):
                pass

            bus.on('DifferentNameFromClass', lambda e: 'done')

            async def dispatch_after_delay():
                await asyncio.sleep(0.05)
                return await bus.emit(BaseEvent(event_type='DifferentNameFromClass'))

            find_task = asyncio.create_task(bus.find(DifferentNameFromClass, past=False, future=1))
            dispatch_task = asyncio.create_task(dispatch_after_delay())

            found, dispatched = await asyncio.gather(find_task, dispatch_task)

            assert found is not None
            assert found.event_id == dispatched.event_id
            assert found.event_type == 'DifferentNameFromClass'
        finally:
            await bus.destroy(clear=True)

    async def test_multiple_concurrent_find_waiters_resolve_correct_events(self):
        """Concurrent find() waiters should each resolve to the correct event."""
        bus = EventBus()

        try:
            # Keep one permanent handler so we can assert temporary find handlers are cleaned up.
            bus.on(ScreenshotEvent, lambda e: 'done')
            baseline_handler_count = len(bus.handlers_by_key.get('ScreenshotEvent', []))

            wait_for_a = asyncio.create_task(
                bus.find(
                    ScreenshotEvent,
                    where=lambda e: e.target_id == TARGET_ID_3,
                    past=False,
                    future=1,
                )
            )
            wait_for_b = asyncio.create_task(
                bus.find(
                    ScreenshotEvent,
                    where=lambda e: e.target_id == TARGET_ID_4,
                    past=False,
                    future=1,
                )
            )

            await asyncio.sleep(0.02)
            event_a = await bus.emit(ScreenshotEvent(target_id=TARGET_ID_3))
            event_b = await bus.emit(ScreenshotEvent(target_id=TARGET_ID_4))

            found_a, found_b = await asyncio.gather(wait_for_a, wait_for_b)

            assert found_a is not None
            assert found_b is not None
            assert found_a.event_id == event_a.event_id
            assert found_b.event_id == event_b.event_id

            # All temporary find handlers should be removed.
            assert len(bus.handlers_by_key.get('ScreenshotEvent', [])) == baseline_handler_count
        finally:
            await bus.destroy(clear=True)

    async def test_find_future_resolves_before_handlers_complete(self):
        """find(future=...) resolves on dispatch, before slow handlers complete."""
        bus = EventBus()

        try:
            processing_complete = False

            async def slow_handler(event: ParentEvent) -> str:
                nonlocal processing_complete
                await asyncio.sleep(0.1)
                processing_complete = True
                return 'done'

            bus.on(ParentEvent, slow_handler)

            find_task = asyncio.create_task(bus.find(ParentEvent, past=False, future=1))
            await asyncio.sleep(0.01)

            dispatched = bus.emit(ParentEvent())
            found = await find_task

            assert found is not None
            assert found.event_id == dispatched.event_id
            assert processing_complete is False
            assert found.event_status in ('pending', 'started')

            await bus.wait_until_idle()
            assert processing_complete is True
        finally:
            await bus.destroy(clear=True)

    async def test_find_returns_coroutine_that_can_be_awaited_later(self):
        """A started find(...) coroutine should resolve later after dispatch."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            find_task = asyncio.create_task(
                bus.find(
                    ParentEvent,
                    where=lambda e: e.event_type == 'ParentEvent',
                    past=False,
                    future=1,
                )
            )

            await asyncio.sleep(0.05)
            dispatched = await bus.emit(ParentEvent())

            found = await find_task
            assert found is not None
            assert found.event_id == dispatched.event_id
        finally:
            await bus.destroy(clear=True)


class TestFindNeitherPastNorFuture:
    """Tests for find(past=False, future=False) - should return None."""

    async def test_returns_none_immediately(self):
        """find(past=False, future=False) returns None immediately."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch event
            await bus.emit(ParentEvent())

            # With both past and future disabled, should return None
            start = datetime.now(UTC)
            found = await bus.find(ParentEvent, past=False, future=False)
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert found is None
            assert elapsed < 0.1  # Should be instant

        finally:
            await bus.destroy(clear=True)


class TestFindPastAndFuture:
    """Tests for find(past=..., future=...) - combined search."""

    async def test_returns_past_event_immediately(self):
        """find(past=True, future=5) returns past event without waiting."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch event first
            dispatched = await bus.emit(ParentEvent())

            # Should find it immediately from history
            start = datetime.now(UTC)
            found = await bus.find(ParentEvent, past=True, future=5)
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert found is not None
            assert found.event_id == dispatched.event_id
            assert elapsed < 0.1  # Should be nearly instant

        finally:
            await bus.destroy(clear=True)

    async def test_waits_for_future_when_no_past_match(self):
        """find(past=True, future=1) waits for future if no past match."""
        bus = EventBus()

        try:
            bus.on(ChildEvent, lambda e: 'done')

            # Different event type in history
            bus.on(ParentEvent, lambda e: 'done')
            await bus.emit(ParentEvent())

            # Start waiting for ChildEvent (not in history)
            async def dispatch_after_delay():
                await asyncio.sleep(0.05)
                return await bus.emit(ChildEvent())

            find_task = asyncio.create_task(bus.find(ChildEvent, past=True, future=1))
            dispatch_task = asyncio.create_task(dispatch_after_delay())

            found, dispatched = await asyncio.gather(find_task, dispatch_task)

            assert found is not None
            assert found.event_id == dispatched.event_id

        finally:
            await bus.destroy(clear=True)

    async def test_past_and_future_independent_control(self):
        """past=0.05, future=0.05 uses different windows for each."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch an old event
            await bus.emit(ParentEvent())
            await asyncio.sleep(0.15)

            # With short past window (0.05s), old event won't be found
            # With short future window (0.05s), will timeout
            start = datetime.now(UTC)
            found = await bus.find(ParentEvent, past=0.05, future=0.05)
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert found is None
            # Should have waited ~0.05s for future
            assert 0.04 < elapsed < 0.15

        finally:
            await bus.destroy(clear=True)

    async def test_past_true_future_float(self):
        """past=True searches all history, future=0.1 waits up to 0.1s."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch an old event
            dispatched = await bus.emit(ParentEvent())
            await asyncio.sleep(0.15)

            # past=True should find the old event (no time window)
            found = await bus.find(ParentEvent, past=True, future=0.1)

            assert found is not None
            assert found.event_id == dispatched.event_id

        finally:
            await bus.destroy(clear=True)

    async def test_past_float_future_true_would_wait_forever(self):
        """past=0.05 with old events + future=True - verify past window works."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch an old event
            await bus.emit(ParentEvent())
            await asyncio.sleep(0.15)

            # past=0.05 won't find old event, but we dispatch a new one
            async def dispatch_after_delay():
                await asyncio.sleep(0.05)
                return await bus.emit(ParentEvent())

            find_task = asyncio.create_task(bus.find(ParentEvent, past=0.05, future=1))
            dispatch_task = asyncio.create_task(dispatch_after_delay())

            found, dispatched = await asyncio.gather(find_task, dispatch_task)

            # Should find the new event from future wait
            assert found is not None
            assert found.event_id == dispatched.event_id

        finally:
            await bus.destroy(clear=True)

    async def test_most_recent_wins_across_completed_and_inflight(self):
        """find(past=True, future=True) returns newest event even when it is in-flight."""
        bus = EventBus()

        try:
            release = asyncio.Event()

            async def numbered_handler(event: NumberedEvent) -> str:
                if event.value == 2:
                    await release.wait()
                return f'handled-{event.value}'

            bus.on(NumberedEvent, numbered_handler)

            await bus.emit(NumberedEvent(value=1))
            in_flight = bus.emit(NumberedEvent(value=2))
            await asyncio.sleep(0.01)

            found = await bus.find(NumberedEvent, past=True, future=True)
            assert found is not None
            assert found.event_id == in_flight.event_id
            assert found.event_status in ('pending', 'started')

            release.set()
            await in_flight
        finally:
            await bus.destroy(clear=True)


# =============================================================================
# find() with child_of Tests
# =============================================================================


class TestFindWithChildOf:
    """Tests for find() with child_of parameter."""

    async def test_returns_child_of_specified_parent(self):
        """find(child_of=parent) returns event that is child of parent."""
        bus = EventBus()

        try:
            child_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                child = await event.emit(ChildEvent())
                child_ref.append(child)
                return 'parent_done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, lambda e: 'child_done')

            parent = await bus.emit(ParentEvent())
            await bus.wait_until_idle()

            # Find child of parent
            found = await bus.find(ChildEvent, child_of=parent, past=True, future=False)

            assert found is not None
            assert found.event_id == child_ref[0].event_id

        finally:
            await bus.destroy(clear=True)

    async def test_returns_none_for_non_child(self):
        """find(child_of=parent) returns None if event is not a child."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'parent_done')
            bus.on(UnrelatedEvent, lambda e: 'unrelated_done')

            parent = await bus.emit(ParentEvent())
            await bus.emit(UnrelatedEvent())

            # Should not find UnrelatedEvent as child of parent
            found = await bus.find(UnrelatedEvent, child_of=parent, past=True, future=False)

            assert found is None

        finally:
            await bus.destroy(clear=True)

    async def test_finds_grandchild(self):
        """find(child_of=grandparent) returns grandchild event."""
        bus = EventBus()

        try:
            grandchild_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                await event.emit(ChildEvent())
                return 'parent_done'

            async def child_handler(event: ChildEvent) -> str:
                grandchild = await event.emit(GrandchildEvent())
                grandchild_ref.append(grandchild)
                return 'child_done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, child_handler)
            bus.on(GrandchildEvent, lambda e: 'grandchild_done')

            parent = await bus.emit(ParentEvent())
            await bus.wait_until_idle()

            # Find grandchild of parent
            found = await bus.find(GrandchildEvent, child_of=parent, past=True, future=False)

            assert found is not None
            assert found.event_id == grandchild_ref[0].event_id

        finally:
            await bus.destroy(clear=True)

    async def test_child_of_works_across_forwarded_buses(self):
        """find(child_of=parent) works when events are forwarded across buses."""
        main_bus = EventBus(name='MainBus')
        auth_bus = EventBus(name='AuthBus')

        try:
            child_ref: list[BaseEvent] = []

            # Forward ParentEvent from main_bus to auth_bus
            main_bus.on(ParentEvent, auth_bus.emit)

            # auth_bus handles ParentEvent and dispatches a ChildEvent
            async def auth_handler(event: ParentEvent) -> str:
                child = await event.emit(ChildEvent())
                child_ref.append(child)
                return 'auth_done'

            auth_bus.on(ParentEvent, auth_handler)
            auth_bus.on(ChildEvent, lambda e: 'child_done')

            # Dispatch on main_bus, which forwards to auth_bus
            parent = await main_bus.emit(ParentEvent())
            await main_bus.wait_until_idle()
            await auth_bus.wait_until_idle()

            # Find child event on auth_bus using parent from main_bus
            found = await auth_bus.find(ChildEvent, child_of=parent, past=5, future=5)

            assert found is not None
            assert found.event_id == child_ref[0].event_id

        finally:
            await main_bus.destroy(clear=True)
            await auth_bus.destroy(clear=True)

    async def test_future_wait_with_child_of(self):
        """find(child_of=..., past=False, future=...) waits for future matching child."""
        bus = EventBus()

        try:

            async def parent_handler(event: ParentEvent) -> str:
                await asyncio.sleep(0.03)
                await event.emit(ChildEvent())
                return 'parent_done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, lambda e: 'child_done')

            parent = bus.emit(ParentEvent())

            found = await bus.find(
                ChildEvent,
                child_of=parent,
                past=False,
                future=0.3,
            )
            assert found is not None
            assert found.event_parent_id == parent.event_id

            await parent
        finally:
            await bus.destroy(clear=True)


# =============================================================================
# find() coverage for historical lookup/wait patterns
# =============================================================================


class TestFindLegacyPatternCoverage:
    """Tests that find() covers all historical lookup/wait patterns."""

    async def test_find_waits_for_future_event(self):
        """find(past=False, future=...) waits for future events."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            async def dispatch_after_delay():
                await asyncio.sleep(0.05)
                return await bus.emit(ParentEvent())

            find_task = asyncio.create_task(bus.find(ParentEvent, past=False, future=1))
            dispatch_task = asyncio.create_task(dispatch_after_delay())

            found, dispatched = await asyncio.gather(find_task, dispatch_task)

            assert found is not None
            assert found.event_id == dispatched.event_id

        finally:
            await bus.destroy(clear=True)

    async def test_find_with_include_style_filter(self):
        """find(where=...) supports include-style filters."""
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            async def dispatch_events():
                await asyncio.sleep(0.02)
                await bus.emit(ScreenshotEvent(target_id='32b90140-a7ee-7ae7-830c-71a099e93cb3'))
                await asyncio.sleep(0.02)
                return await bus.emit(ScreenshotEvent(target_id='519664bf-c9fa-7654-896b-fb0cc5b6adab'))

            find_task = asyncio.create_task(
                bus.find(
                    ScreenshotEvent,
                    where=lambda e: e.target_id == '519664bf-c9fa-7654-896b-fb0cc5b6adab',
                    past=False,
                    future=1,
                )
            )
            dispatch_task = asyncio.create_task(dispatch_events())

            found, dispatched = await asyncio.gather(find_task, dispatch_task)

            assert found is not None
            assert found.target_id == '519664bf-c9fa-7654-896b-fb0cc5b6adab'

        finally:
            await bus.destroy(clear=True)

    async def test_find_with_exclude_style_filter(self):
        """find(where=...) supports exclude-style filters."""
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            async def dispatch_events():
                await asyncio.sleep(0.02)
                await bus.emit(ScreenshotEvent(target_id='1556eff9-dea5-78ae-8219-7bb92f787370'))
                await asyncio.sleep(0.02)
                return await bus.emit(ScreenshotEvent(target_id='45c2761f-3475-72aa-8dd8-b3cf4a4923e2'))

            find_task = asyncio.create_task(
                bus.find(
                    ScreenshotEvent,
                    where=lambda e: e.target_id != '1556eff9-dea5-78ae-8219-7bb92f787370',
                    past=False,
                    future=1,
                )
            )
            dispatch_task = asyncio.create_task(dispatch_events())

            found, dispatched = await asyncio.gather(find_task, dispatch_task)

            assert found is not None
            assert found.target_id == '45c2761f-3475-72aa-8dd8-b3cf4a4923e2'

        finally:
            await bus.destroy(clear=True)

    async def test_find_with_past_true_and_future_timeout(self):
        """find(past=True, future=...) finds already-dispatched events."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch event first
            dispatched = await bus.emit(ParentEvent())

            found = await bus.find(ParentEvent, past=True, future=5)

            assert found is not None
            assert found.event_id == dispatched.event_id

        finally:
            await bus.destroy(clear=True)

    async def test_find_with_past_float_and_future_timeout(self):
        """find(past=5.0, future=...) searches recent history first."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch event first
            dispatched = await bus.emit(ParentEvent())

            found = await bus.find(ParentEvent, past=5.0, future=1)

            assert found is not None
            assert found.event_id == dispatched.event_id

        finally:
            await bus.destroy(clear=True)

    async def test_find_with_child_of_and_future_timeout(self):
        """find(child_of=parent) filters by parent relationship."""
        bus = EventBus()

        try:
            child_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                child = await event.emit(ChildEvent())
                child_ref.append(child)
                return 'parent_done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, lambda e: 'child_done')

            parent = await bus.emit(ParentEvent())
            await bus.wait_until_idle()

            found = await bus.find(ChildEvent, child_of=parent, past=True, future=5)

            assert found is not None
            assert found.event_id == child_ref[0].event_id

        finally:
            await bus.destroy(clear=True)


# =============================================================================
# Race Condition Fix Tests (Issue #15)
# =============================================================================


class TestRaceConditionFix:
    """Tests for race conditions where events fire before lookup starts."""

    async def test_find_catches_already_fired_event(self):
        """find(past=True) catches event that fired before the call."""
        bus = EventBus()

        try:
            tab_ref: list[BaseEvent] = []

            async def navigate_handler(event: NavigateEvent) -> str:
                # This synchronously creates the tab event
                tab = await event.emit(TabCreatedEvent(tab_id='06bee4cf-9f51-7e5d-82d3-65f35169329c'))
                tab_ref.append(tab)
                return 'navigate_done'

            bus.on(NavigateEvent, navigate_handler)
            bus.on(TabCreatedEvent, lambda e: 'tab_created')

            # Dispatch navigation - tab event fires during handler
            nav_event = await bus.emit(NavigateEvent(url='https://example.com'))

            # By now TabCreatedEvent has already fired
            # Using find(past=True) should catch it
            found = await bus.find(TabCreatedEvent, child_of=nav_event, past=True, future=False)

            assert found is not None
            assert found.event_id == tab_ref[0].event_id

        finally:
            await bus.destroy(clear=True)

    async def test_child_of_filters_to_correct_parent(self):
        """child_of correctly filters to events from the right parent."""
        bus = EventBus()

        try:

            async def navigate_handler(event: NavigateEvent) -> str:
                await event.emit(TabCreatedEvent(tab_id=f'tab_for_{event.url}'))
                return 'navigate_done'

            bus.on(NavigateEvent, navigate_handler)
            bus.on(TabCreatedEvent, lambda e: 'tab_created')

            # Two navigations, each creates a tab
            nav1 = await bus.emit(NavigateEvent(url='site1'))
            nav2 = await bus.emit(NavigateEvent(url='site2'))

            # Find tab created by nav1 specifically
            tab1 = await bus.find(TabCreatedEvent, child_of=nav1, past=True, future=False)

            # Find tab created by nav2 specifically
            tab2 = await bus.find(TabCreatedEvent, child_of=nav2, past=True, future=False)

            assert tab1 is not None
            assert tab2 is not None
            assert tab1.tab_id == 'tab_for_site1'
            assert tab2.tab_id == 'tab_for_site2'

        finally:
            await bus.destroy(clear=True)


# =============================================================================
# New Parameter Combination Tests
# =============================================================================


class TestNewParameterCombinations:
    """Tests for the new bool | float parameter combinations."""

    async def test_past_true_future_false_searches_all_history(self):
        """past=True, future=False searches all history instantly."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch event and wait
            dispatched = await bus.emit(ParentEvent())
            await asyncio.sleep(0.1)

            # Should find old event with past=True
            found = await bus.find(ParentEvent, past=True, future=False)
            assert found is not None
            assert found.event_id == dispatched.event_id

        finally:
            await bus.destroy(clear=True)

    async def test_past_float_future_false_filters_by_age(self):
        """past=0.05, future=False only searches last 0.05 seconds."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch event
            await bus.emit(ParentEvent())
            await asyncio.sleep(0.1)  # Make it old

            # past=0.05 means "events in last 0.05 seconds" = nothing old
            found = await bus.find(ParentEvent, past=0.05, future=False)
            assert found is None

        finally:
            await bus.destroy(clear=True)

    async def test_past_false_future_float_waits_for_timeout(self):
        """past=False, future=0.05 waits up to 0.05 seconds."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            start = datetime.now(UTC)
            found = await bus.find(ParentEvent, past=False, future=0.05)
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert found is None
            assert 0.04 < elapsed < 0.15  # Should wait ~0.05s

        finally:
            await bus.destroy(clear=True)

    async def test_past_true_future_true_searches_all_and_waits_forever(self):
        """past=True, future=True searches all history, would wait forever."""
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            # Dispatch an old event
            dispatched = await bus.emit(ParentEvent())
            await asyncio.sleep(0.1)

            # past=True should find the old event immediately
            start = datetime.now(UTC)
            found = await bus.find(ParentEvent, past=True, future=True)
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert found is not None
            assert found.event_id == dispatched.event_id
            assert elapsed < 0.1  # Should be instant (found in past)

        finally:
            await bus.destroy(clear=True)

    async def test_find_with_where_and_past_float(self):
        """where filter combined with past=float works correctly."""
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            # Dispatch events with different target_ids
            await bus.emit(ScreenshotEvent(target_id=TARGET_ID_1))
            await asyncio.sleep(0.15)
            event2 = await bus.emit(ScreenshotEvent(target_id=TARGET_ID_2))

            # Find with both where filter and past window
            found = await bus.find(
                ScreenshotEvent,
                where=lambda e: e.target_id == TARGET_ID_2,
                past=0.1,  # Only search last 0.1 seconds
                future=False,
            )
            assert found is not None
            assert found.event_id == event2.event_id

            # tab1 is too old for the past window
            found = await bus.find(
                ScreenshotEvent,
                where=lambda e: e.target_id == TARGET_ID_1,
                past=0.1,
                future=False,
            )
            assert found is None

        finally:
            await bus.destroy(clear=True)

    async def test_find_with_child_of_and_past_float(self):
        """child_of filter combined with past=float works correctly."""
        bus = EventBus()

        try:
            child_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                child = await event.emit(ChildEvent())
                child_ref.append(child)
                return 'done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ChildEvent, lambda e: 'done')

            parent = await bus.emit(ParentEvent())
            await bus.wait_until_idle()

            # Find child with past window - should work since event is fresh
            found = await bus.find(
                ChildEvent,
                child_of=parent,
                past=5,  # 5 second window
                future=False,
            )
            assert found is not None
            assert found.event_id == child_ref[0].event_id

        finally:
            await bus.destroy(clear=True)

    async def test_find_with_all_parameters(self):
        """All parameters combined work correctly."""
        bus = EventBus()

        try:
            child_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                child = await event.emit(ScreenshotEvent(target_id=TARGET_ID_CHILD))
                child_ref.append(child)
                return 'done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ScreenshotEvent, lambda e: 'done')

            parent = await bus.emit(ParentEvent())
            await bus.wait_until_idle()

            # Find with all parameters
            found = await bus.find(
                ScreenshotEvent,
                where=lambda e: e.target_id == TARGET_ID_CHILD,
                child_of=parent,
                past=5,
                future=False,
            )
            assert found is not None
            assert found.event_id == child_ref[0].event_id
            assert found.target_id == TARGET_ID_CHILD

        finally:
            await bus.destroy(clear=True)


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
