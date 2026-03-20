import asyncio
from datetime import UTC, datetime

from abxbus import BaseEvent, EventBus


class ParentEvent(BaseEvent[str]):
    pass


class ScreenshotEvent(BaseEvent[str]):
    target_id: str = ''
    full_page: bool = False


class SyncEvent(BaseEvent[str]):
    pass


TARGET_ID_1 = '9b447756-908c-7b75-8a51-4a2c2b4d9b14'
TARGET_ID_2 = '194870e1-fa02-70a4-8101-d10d57c3449c'


class TestDebouncingPattern:
    """Tests for the debouncing pattern: find() or emit()."""

    async def test_simple_debounce_with_child_of_reuses_recent_event(self):
        bus = EventBus()

        try:
            child_ref: list[BaseEvent] = []

            async def parent_handler(event: ParentEvent) -> str:
                child = await bus.emit(ScreenshotEvent(target_id='0c1ccf21-65c0-7390-8b64-9182e985740e'))
                child_ref.append(child)
                return 'parent_done'

            bus.on(ParentEvent, parent_handler)
            bus.on(ScreenshotEvent, lambda e: 'screenshot_done')

            parent = await bus.emit(ParentEvent())
            await bus.wait_until_idle()

            reused = await (
                await bus.find(
                    ScreenshotEvent,
                    child_of=parent,
                    past=10,
                    future=False,
                )
                or bus.emit(ScreenshotEvent(target_id='d41abf01-392b-7f28-8992-3105a258867d'))
            )

            assert reused.event_id == child_ref[0].event_id
            assert reused.event_parent_id == parent.event_id
        finally:
            await bus.stop(clear=True)

    async def test_returns_existing_fresh_event(self):
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            original = await bus.emit(ScreenshotEvent(target_id=TARGET_ID_1))

            def is_fresh(event: ScreenshotEvent) -> bool:
                if event.event_completed_at is None:
                    return False
                completed_at = datetime.fromisoformat(event.event_completed_at)
                return (datetime.now(UTC) - completed_at).total_seconds() < 5

            def matches_fresh_tab(event: ScreenshotEvent) -> bool:
                return event.target_id == TARGET_ID_1 and is_fresh(event)

            result = await (
                await bus.find(
                    ScreenshotEvent,
                    where=matches_fresh_tab,
                    past=True,
                    future=False,
                )
                or bus.emit(ScreenshotEvent(target_id=TARGET_ID_1))
            )

            assert result.event_id == original.event_id

        finally:
            await bus.stop(clear=True)

    async def test_advanced_debounce_prefers_history_then_waits_future_then_dispatches(self):
        bus = EventBus()

        try:
            pending_event = asyncio.create_task(bus.find(SyncEvent, past=False, future=0.5))

            async def dispatch_later() -> None:
                await asyncio.sleep(0.05)
                await bus.emit(SyncEvent())

            dispatch_task = asyncio.create_task(dispatch_later())

            resolved_event = await (
                (await bus.find(SyncEvent, past=True, future=False)) or (await pending_event) or bus.emit(SyncEvent())
            )

            await dispatch_task
            assert resolved_event is not None
            assert resolved_event.event_type == 'SyncEvent'
        finally:
            await bus.stop(clear=True)

    async def test_dispatches_new_when_no_match(self):
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            result = await (
                await bus.find(
                    ScreenshotEvent,
                    where=lambda e: e.target_id == TARGET_ID_1,
                    past=True,
                    future=False,
                )
                or bus.emit(ScreenshotEvent(target_id=TARGET_ID_1))
            )

            assert result is not None
            assert result.target_id == TARGET_ID_1
            assert result.event_status == 'completed'

        finally:
            await bus.stop(clear=True)

    async def test_dispatches_new_when_stale(self):
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            await bus.emit(ScreenshotEvent(target_id=TARGET_ID_1))

            result = await (
                await bus.find(
                    ScreenshotEvent,
                    where=lambda e: e.target_id == TARGET_ID_1 and False,
                    past=True,
                    future=False,
                )
                or bus.emit(ScreenshotEvent(target_id=TARGET_ID_1))
            )

            assert result is not None
            screenshots = [e for e in bus.event_history.values() if isinstance(e, ScreenshotEvent)]
            assert len(screenshots) == 2

        finally:
            await bus.stop(clear=True)

    async def test_find_past_only_returns_immediately_without_waiting(self):
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            start = datetime.now(UTC)
            result = await bus.find(ParentEvent, past=True, future=False)
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert result is None
            assert elapsed < 0.05

        finally:
            await bus.stop(clear=True)

    async def test_find_past_float_returns_immediately_without_waiting(self):
        bus = EventBus()

        try:
            bus.on(ParentEvent, lambda e: 'done')

            start = datetime.now(UTC)
            result = await bus.find(ParentEvent, past=5, future=False)
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert result is None
            assert elapsed < 0.05

        finally:
            await bus.stop(clear=True)

    async def test_or_chain_without_waiting_finds_existing(self):
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            original = await bus.emit(ScreenshotEvent(target_id=TARGET_ID_1))

            start = datetime.now(UTC)
            result = await (
                await bus.find(
                    ScreenshotEvent,
                    where=lambda e: e.target_id == TARGET_ID_1,
                    past=True,
                    future=False,
                )
                or bus.emit(ScreenshotEvent(target_id=TARGET_ID_1))
            )
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert result.event_id == original.event_id
            assert elapsed < 0.1

        finally:
            await bus.stop(clear=True)

    async def test_or_chain_without_waiting_dispatches_when_no_match(self):
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            start = datetime.now(UTC)
            result = await (
                await bus.find(
                    ScreenshotEvent,
                    where=lambda e: e.target_id == TARGET_ID_1,
                    past=True,
                    future=False,
                )
                or bus.emit(ScreenshotEvent(target_id=TARGET_ID_1))
            )
            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert result is not None
            assert result.target_id == TARGET_ID_1
            assert elapsed < 0.1

        finally:
            await bus.stop(clear=True)

    async def test_or_chain_multiple_sequential_lookups(self):
        bus = EventBus()

        try:
            bus.on(ScreenshotEvent, lambda e: 'done')

            start = datetime.now(UTC)

            result1 = await (
                await bus.find(
                    ScreenshotEvent,
                    where=lambda e: e.target_id == TARGET_ID_1,
                    past=True,
                    future=False,
                )
                or bus.emit(ScreenshotEvent(target_id=TARGET_ID_1))
            )

            result2 = await (
                await bus.find(
                    ScreenshotEvent,
                    where=lambda e: e.target_id == TARGET_ID_1,
                    past=True,
                    future=False,
                )
                or bus.emit(ScreenshotEvent(target_id=TARGET_ID_1))
            )

            result3 = await (
                await bus.find(
                    ScreenshotEvent,
                    where=lambda e: e.target_id == TARGET_ID_2,
                    past=True,
                    future=False,
                )
                or bus.emit(ScreenshotEvent(target_id=TARGET_ID_2))
            )

            elapsed = (datetime.now(UTC) - start).total_seconds()

            assert result1.event_id == result2.event_id
            assert result3.event_id != result1.event_id
            assert result3.target_id == TARGET_ID_2
            assert elapsed < 0.2

        finally:
            await bus.stop(clear=True)
