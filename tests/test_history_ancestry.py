"""History pressure must not destroy the ancestry needed by running handlers."""

from pathlib import Path

import pytest

from abxbus import BaseEvent, EventBus


class WriteFileEvent(BaseEvent):
    path: str
    contents: str


class WriteFilesEvent(BaseEvent):
    directory: str


@pytest.mark.asyncio
async def test_completed_resource_event_is_retained_until_its_scope_closes(tmp_path: Path):
    bus = EventBus(max_history_size=2, max_history_drop=True)

    def write_file(event: WriteFileEvent) -> None:
        Path(event.path).write_text(event.contents)

    bus.on(WriteFileEvent, write_file)
    parent = await bus.emit(WriteFilesEvent(directory=str(tmp_path))).now()
    resource = WriteFileEvent(path=str(tmp_path / 'resource'), contents='open', event_parent_id=parent.event_id)
    try:
        with bus.event_history.retain(resource):
            await bus.emit(resource).now()
            with bus.event_history.retain(resource):
                for index in range(8):
                    await bus.emit(WriteFileEvent(path=str(tmp_path / str(index)), contents=str(index))).now()
                    assert bus.event_history[resource.event_id] is resource
                    assert bus.event_history[parent.event_id] is parent
            await bus.emit(WriteFileEvent(path=str(tmp_path / 'still-open'), contents='open')).now()
            assert bus.event_history[resource.event_id] is resource
        for index in range(3):
            await bus.emit(WriteFileEvent(path=str(tmp_path / 'after'), contents=str(index))).now()
        await bus.wait_until_idle()
        assert resource.event_id not in bus.event_history
        assert parent.event_id not in bus.event_history
        assert len(bus.event_history) <= 2
        assert (tmp_path / 'resource').read_text() == 'open'
        assert (tmp_path / 'after').read_text() == '2'
    finally:
        await bus.destroy()


@pytest.mark.asyncio
async def test_history_pressure_preserves_pending_children_and_parent(tmp_path: Path):
    bus = EventBus(max_history_size=3, max_history_drop=True)
    parent = WriteFilesEvent(directory=str(tmp_path))

    async def write_file(event: WriteFileEvent) -> None:
        assert bus.event_history.get(parent.event_id) is parent
        assert bus.event_history.get(event.event_id) is event
        assert bus.event_is_child_of(event, parent)
        Path(event.path).write_text(event.contents)

    async def write_files(event: WriteFilesEvent) -> None:
        children = [
            event.emit(WriteFileEvent(path=str(Path(event.directory) / str(index)), contents=str(index))) for index in range(12)
        ]
        for child in children:
            await child.now()
            await child.event_result(raise_if_any=True)

    bus.on(WriteFileEvent, write_file)
    bus.on(WriteFilesEvent, write_files)
    try:
        await bus.emit(parent).now()
        await parent.event_result(raise_if_any=True)
        await bus.wait_until_idle()
        assert [Path(tmp_path / str(index)).read_text() for index in range(12)] == [str(index) for index in range(12)]
        assert len(bus.event_history) <= 3
        for event in bus.event_history.values():
            if event.event_parent_id:
                assert event.event_parent_id in bus.event_history
    finally:
        await bus.destroy()


@pytest.mark.asyncio
async def test_history_pressure_preserves_completed_parent_of_pending_child():
    import asyncio

    bus = EventBus(max_history_size=2, max_history_drop=True)
    release = asyncio.Event()

    async def hold(event: BaseEvent) -> str:
        await release.wait()
        return 'ok'

    bus.on('HeldChild', hold)
    try:
        parent = await bus.emit(BaseEvent(event_type='Parent')).now()
        await bus.emit(BaseEvent(event_type='Extra')).now()
        child = bus.emit(BaseEvent(event_type='HeldChild', event_parent_id=parent.event_id))
        extras = [bus.emit(BaseEvent(event_type='Extra')) for _ in range(8)]
        assert bus.event_history.get(parent.event_id) is parent
        assert bus.event_history.get(child.event_id) is child
        release.set()
        await asyncio.gather(child, *extras)
        await bus.wait_until_idle()
        assert await child.event_result(raise_if_any=True) == 'ok'
        assert len(bus.event_history) <= 2
    finally:
        release.set()
        await bus.destroy()
