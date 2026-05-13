#!/usr/bin/env -S uv run python
"""Run: uv run python examples/parent_child_tracking.py"""

import asyncio

from abxbus import BaseEvent, EventBus


class ParentEvent(BaseEvent[str]):
    workflow: str


class ChildEvent(BaseEvent[str]):
    stage: str


class GrandchildEvent(BaseEvent[str]):
    note: str


def short_id(value: str | None) -> str:
    if value is None:
        return 'none'
    return value[-8:]


async def main() -> None:
    bus = EventBus(name='ParentChildTrackingBus')

    try:

        async def on_child(event: ChildEvent) -> str:
            print(f'child handler start: {event.event_type}#{short_id(event.event_id)}')

            grandchild = event.emit(GrandchildEvent(note=f'spawned by {event.stage}'))
            print(
                '  child dispatched grandchild: '
                f'{grandchild.event_type}#{short_id(grandchild.event_id)} '
                f'parent_id={short_id(grandchild.event_parent_id)}'
            )

            await grandchild.now()
            print(f'  child resumed after awaiting grandchild: {short_id(grandchild.event_id)}')
            return f'child_completed:{event.stage}'

        async def on_grandchild(event: GrandchildEvent) -> str:
            print(f'grandchild handler: {event.event_type}#{short_id(event.event_id)} note="{event.note}"')
            return f'grandchild_completed:{event.note}'

        async def on_parent(event: ParentEvent) -> str:
            print(f'parent handler start: {event.event_type}#{short_id(event.event_id)} workflow="{event.workflow}"')

            awaited_child = event.emit(ChildEvent(stage='awaited-child'))
            print(
                '  parent emitted child: '
                f'{awaited_child.event_type}#{short_id(awaited_child.event_id)} '
                f'parent_id={short_id(awaited_child.event_parent_id)}'
            )
            await awaited_child.now()
            print(f'  parent resumed after awaited child: {short_id(awaited_child.event_id)}')

            background_child = event.emit(ChildEvent(stage='background-child'))
            print(
                '  parent dispatched linked background child: '
                f'{background_child.event_type}#{short_id(background_child.event_id)} '
                f'parent_id={short_id(background_child.event_parent_id)}'
            )

            direct_grandchild = event.emit(GrandchildEvent(note='directly from parent'))
            print(
                '  parent dispatched grandchild type directly: '
                f'{direct_grandchild.event_type}#{short_id(direct_grandchild.event_id)} '
                f'parent_id={short_id(direct_grandchild.event_parent_id)}'
            )
            await direct_grandchild.now()

            return 'parent_completed'

        bus.on(ChildEvent, on_child)
        bus.on(GrandchildEvent, on_grandchild)
        bus.on(ParentEvent, on_parent)

        parent = bus.emit(ParentEvent(workflow='demo-parent-child-tracking'))
        await parent.now()
        await bus.wait_until_idle()

        print('\n=== Event History Relationships ===')
        history = sorted(bus.event_history.values(), key=lambda event: event.event_created_at)

        for item in history:
            parent_event = bus.event_history.get(item.event_parent_id) if item.event_parent_id else None
            print(
                ' | '.join(
                    [
                        f'{item.event_type}#{short_id(item.event_id)}',
                        (
                            f'parent={parent_event.event_type}#{short_id(parent_event.event_id)}'
                            if parent_event is not None
                            else 'parent=none'
                        ),
                        f'isChildOfRoot={bus.event_is_child_of(item, parent)}',
                        f'rootIsParentOf={bus.event_is_parent_of(parent, item)}',
                    ]
                )
            )

        first_child = next((event for event in history if event.event_type == 'ChildEvent'), None)
        nested_grandchild = next(
            (
                event
                for event in history
                if event.event_type == 'GrandchildEvent'
                and first_child is not None
                and event.event_parent_id == first_child.event_id
            ),
            None,
        )
        if first_child is not None and nested_grandchild is not None:
            print(
                'grandchild->child relationship check: '
                f'{nested_grandchild.event_type}#{short_id(nested_grandchild.event_id)} '
                f'is child of {first_child.event_type}#{short_id(first_child.event_id)} = '
                f'{bus.event_is_child_of(nested_grandchild, first_child)}'
            )

        print('\n=== bus.log_tree() ===')
        print(bus.log_tree())
    finally:
        await bus.stop(clear=True, timeout=0)


if __name__ == '__main__':
    asyncio.run(main())
