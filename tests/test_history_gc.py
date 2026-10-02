"""Expired child events must be collectible while their parent stays alive."""

import asyncio
import gc
import weakref
from pathlib import Path

import pytest

from abxbus import BaseEvent, EventBus


class SavedFileEvent(BaseEvent):
    path: str


@pytest.mark.asyncio
async def test_expired_children_are_collectible_under_retained_parent(tmp_path: Path):
    bus = EventBus(max_history_size=None, event_ttl=0)
    children: list[weakref.ReferenceType[SavedFileEvent]] = []

    def save_file(event: SavedFileEvent) -> None:
        Path(event.path).write_text('saved')
        children.append(weakref.ref(event))

    async def save_files(event: BaseEvent) -> None:
        for index in range(10):
            await event.emit(SavedFileEvent(path=str(tmp_path / str(index)))).now()

    bus.on(SavedFileEvent, save_file)
    bus.on('SaveFilesEvent', save_files)
    parent = BaseEvent(event_type='SaveFilesEvent', event_ttl=-1)
    try:
        await bus.emit(parent).now()
        await parent.event_result(raise_if_any=True)
        await bus.wait_until_idle()
        await bus.emit(BaseEvent(event_type='TrimEvent')).now()
        await bus.wait_until_idle()
        await asyncio.sleep(0)
        gc.collect()
        assert bus.event_history[parent.event_id] is parent
        assert len(children) == 10
        assert all(reference() is None for reference in children)
        assert [path.read_text() for path in sorted(tmp_path.iterdir())] == ['saved'] * 10
    finally:
        await bus.destroy()
