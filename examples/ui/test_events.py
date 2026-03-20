"""Utility script to generate auto events for the monitor app."""

from __future__ import annotations

import argparse
import asyncio
import random
import string
import sys
from collections.abc import Sequence
from pathlib import Path

from abxbus import BaseEvent, EventBus, SQLiteHistoryMirrorMiddleware

if __package__ in (None, ''):
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from examples.ui.config import resolve_db_path
else:
    from .config import resolve_db_path


class RandomTestEvent(BaseEvent):
    abc_payload_field: str
    xyz_category_field: str
    route_hint: str | None = None


class FollowUpEvent(BaseEvent):
    abc_parent_payload_field: str
    xyz_detail_field: str
    depth: int


class AuditTrailEvent(BaseEvent):
    source_event_id: str
    handler_name: str
    message: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Generate random events for the abxbus monitor.')
    parser.add_argument('--min-delay', type=float, default=0.2, help='Minimum delay between root events (seconds).')
    parser.add_argument('--max-delay', type=float, default=1.0, help='Maximum delay between root events (seconds).')
    parser.add_argument('--error-rate', type=float, default=0.2, help='Fraction of handlers that should raise an error.')
    parser.add_argument('--child-rate', type=float, default=0.4, help='Probability of dispatching follow-up events.')
    parser.add_argument('--audit-rate', type=float, default=0.5, help='Probability of emitting audit trail events.')
    parser.add_argument('--max-depth', type=int, default=2, help='Maximum nested follow-up depth.')
    parser.add_argument('--burst-size', type=int, default=4, help='Number of root events per burst.')
    parser.add_argument('--categories', nargs='*', default=['alpha', 'beta', 'gamma'], help='Event categories to sample.')
    parser.add_argument('--concurrent', type=int, default=3, help='Number of concurrent root event producers.')
    parser.add_argument('--events', type=int, default=0, help='Optional count. 0 = run forever.')
    return parser.parse_args()


def _random_text(length: int = 8) -> str:
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))


async def run_generator(args: argparse.Namespace) -> None:
    db_path = resolve_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    middleware = SQLiteHistoryMirrorMiddleware(db_path)
    bus = EventBus(
        name='MonitorGenerator',
        middlewares=[middleware],
        event_handler_concurrency='parallel',
        max_history_size=0,
    )

    categories: Sequence[str] = args.categories or ['default']

    async def random_handler(event: RandomTestEvent) -> str:
        await asyncio.sleep(random.uniform(0.35, 0.7))
        if random.random() < args.child_rate:
            depth = random.randint(1, max(1, args.max_depth))
            await emit_followups(event, depth)
        if random.random() < args.audit_rate:
            bus.emit(
                AuditTrailEvent(
                    source_event_id=event.event_id,
                    handler_name='random_handler',
                    message=f'Processed payload {event.abc_payload_field}',
                )
            )
        if random.random() < args.error_rate:
            raise RuntimeError(f'Flaky handler failed for payload={event.abc_payload_field}')
        return event.abc_payload_field[::-1]

    async def analytics_handler(event: RandomTestEvent) -> None:
        await asyncio.sleep(random.uniform(0.2, 0.5))
        if random.random() < args.audit_rate:
            bus.emit(
                AuditTrailEvent(
                    source_event_id=event.event_id,
                    handler_name='analytics_handler',
                    message=f'Category {event.xyz_category_field}',
                )
            )

    async def auditing_handler(event: RandomTestEvent) -> str:
        await asyncio.sleep(random.uniform(0.25, 0.6))
        return f'route:{event.route_hint or "default"}|category:{event.xyz_category_field}'

    async def followup_handler(event: FollowUpEvent) -> str:
        await asyncio.sleep(random.uniform(0.3, 0.65))
        if random.random() < 0.3 and event.depth < args.max_depth:
            await emit_followups(event, args.max_depth - event.depth)
        return f'followup:{event.xyz_detail_field}'

    async def audit_handler(event: AuditTrailEvent) -> None:
        await asyncio.sleep(random.uniform(0.2, 0.4))

    bus.on('RandomTestEvent', random_handler)
    bus.on('RandomTestEvent', analytics_handler)
    bus.on('RandomTestEvent', auditing_handler)
    bus.on('FollowUpEvent', followup_handler)
    bus.on('AuditTrailEvent', audit_handler)

    print(f'🟢 Streaming events to {db_path}')

    async def producer_task(task_id: int) -> None:
        emitted = 0
        while args.events == 0 or emitted < args.events:
            burst = random.randint(1, max(1, args.burst_size))
            for _ in range(burst):
                payload = _random_text(10)
                event = RandomTestEvent(
                    abc_payload_field=payload,
                    xyz_category_field=random.choice(list(categories)),
                    route_hint=f'route-{task_id}-{random.randint(1, 3)}',
                    event_result_type=str,
                )
                bus.emit(event)
                emitted += 1
                if args.events and emitted >= args.events:
                    break
                await asyncio.sleep(random.uniform(args.min_delay, args.max_delay))
            await asyncio.sleep(random.uniform(args.min_delay, args.max_delay))

    async def emit_followups(parent_event: BaseEvent, remaining_depth: int) -> None:
        depth = getattr(parent_event, 'depth', 0) + 1
        followup_count = random.randint(1, 2)
        for _ in range(followup_count):
            follow_up = FollowUpEvent(
                abc_parent_payload_field=getattr(parent_event, 'abc_payload_field', parent_event.event_id),
                xyz_detail_field=_random_text(6),
                depth=depth,
                event_result_type=str,
            )
            bus.emit(follow_up)
        if remaining_depth > 1 and random.random() < 0.6:
            await asyncio.sleep(random.uniform(0.2, 0.4))
            await emit_followups(parent_event, remaining_depth - 1)

    try:
        producers = [asyncio.create_task(producer_task(idx)) for idx in range(max(1, args.concurrent))]
        await asyncio.gather(*producers)
        await bus.wait_until_idle()
    finally:
        await bus.stop()


def main() -> None:
    args = parse_args()
    asyncio.run(run_generator(args))


if __name__ == '__main__':
    main()
