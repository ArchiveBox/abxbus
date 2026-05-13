"""Test comprehensive event patterns including forwarding, async/sync dispatch, and parent-child tracking."""

# pyright: reportUnusedVariable=false

import asyncio
from typing import Any

from abxbus import BaseEvent, EventBus


class ParentEvent(BaseEvent[str]):
    pass


class ChildEvent(BaseEvent[str]):
    pass


class ImmediateChildEvent(BaseEvent[str]):
    pass


class QueuedChildEvent(BaseEvent[str]):
    pass


async def test_comprehensive_patterns():
    """Test all event patterns work correctly without race conditions."""
    print('\n=== Test Comprehensive Patterns ===')

    bus1 = EventBus(name='bus1')
    bus2 = EventBus(name='bus2')  # Fixed typo from 'bus1' to 'bus2'

    results: list[tuple[int, str]] = []
    execution_counter = {'count': 0}  # Use a dict to track execution order

    def child_bus2_event_handler(event: BaseEvent[str]) -> str:
        """This gets triggered when the event is forwarded to the second bus."""
        execution_counter['count'] += 1
        seq = execution_counter['count']
        event_type_short = event.__class__.__name__.replace('Event', '')
        print(f'[{seq}] child_bus2_event_handler: processing {event.__class__.__name__} on bus2')
        results.append((seq, f'bus2_handler_{event_type_short}'))
        return 'forwarded bus result'

    bus2.on('*', child_bus2_event_handler)  # register a handler on bus2
    bus1.on('*', bus2.emit)  # forward all events from bus1 -> bus2

    async def parent_bus1_handler(event: ParentEvent) -> str:
        # Only process the parent ParentEvent

        execution_counter['count'] += 1
        seq = execution_counter['count']
        print(f'\n[{seq}] parent_bus1_handler: START processing {event}')
        results.append((seq, 'parent_start'))

        # Pattern 1: Explicit child dispatch - handlers can run without awaiting here
        print('\n1. Testing async dispatch...')
        child_event_async = event.emit(QueuedChildEvent())
        print(f'   child_event_async.event_status = {child_event_async.event_status}')
        assert child_event_async.event_status != 'completed'

        # Pattern 2: Sync dispatch with await - handlers run immediately
        print('\n2. Testing sync dispatch (await)...')
        child_event_sync = await event.emit(ImmediateChildEvent())
        print(f'   child_event_sync.event_status = {child_event_sync.event_status}')
        assert child_event_sync.event_status == 'completed'

        # Check that forwarded handler result is available
        print('\n3. Checking forwarded handler results...')
        print(f'   child_event_sync.event_results: {child_event_sync.event_results}')
        event_results = await child_event_sync.event_results_list(raise_if_none=False)
        print(f'   Results: {event_results}')
        # The forwarding handler (bus.emit) returns the event object itself
        # We need to check if the child event was processed on bus2
        # Check that the event was forwarded by looking at:
        # 1. The event path includes bus2
        assert bus2.label in child_event_sync.event_path
        # 2. Debug what handlers processed this event
        print('   Handlers that processed this event:')
        for result in child_event_sync.event_results.values():
            print(f'     - {result.handler_name} (bus: {result.eventbus_name})')
        # The event was forwarded from bus1 and processed by bus2.
        assert any(
            result.eventbus_name == 'bus1' and 'emit' in result.handler_name for result in child_event_sync.event_results.values()
        )
        assert any(
            result.eventbus_name == 'bus2' and 'child_bus2_event_handler' in result.handler_name
            for result in child_event_sync.event_results.values()
        )
        print('   Event was successfully forwarded to bus2')

        # Check parent-child relationships
        print('\n4. Checking parent-child relationships...')
        print(f'   child_event_async.event_parent_id = {child_event_async.event_parent_id}')
        print(f'   child_event_sync.event_parent_id = {child_event_sync.event_parent_id}')
        print(f'   event.event_id = {event.event_id}')
        assert child_event_async.event_parent_id == event.event_id
        assert child_event_sync.event_parent_id == event.event_id

        execution_counter['count'] += 1
        seq = execution_counter['count']
        print(f'[{seq}] parent_bus1_handler: END')
        results.append((seq, 'parent_end'))
        return 'parent_done'

    bus1.on(ParentEvent, parent_bus1_handler)

    # Dispatch parent event and wait for completion
    print('\nDispatching parent event...')
    parent_event = await bus1.emit(ParentEvent())

    # Wait for all buses to finish processing
    await bus1.wait_until_idle()
    await bus2.wait_until_idle()

    # This is a happy-path test: no handler should have errored.
    assert all(result.error is None for result in parent_event.event_results.values()), parent_event.event_results

    # Verify all child events have correct parent
    print('\n5. Verifying all events have correct parent...')
    all_events = list(bus1.event_history.values())
    print(f'   Total events in history: {len(all_events)}')
    for i, event in enumerate(all_events):
        print(
            f'   Event {i}: {event.__class__.__name__}, id: {event.event_id[-4:]}, parent_id: {event.event_parent_id[-4:] if event.event_parent_id else "None"}'
        )

    # Child events should have parent's ID
    event_children = [e for e in all_events if isinstance(e, (ImmediateChildEvent, QueuedChildEvent))]
    assert all(event.event_parent_id == parent_event.event_id for event in event_children)

    # Sort results by sequence number to see actual execution order
    sorted_results = sorted(results, key=lambda x: x[0])
    execution_order = [item[1] for item in sorted_results]

    print('\nExecution order:')
    for seq, action in sorted_results:
        print(f'  [{seq}] {action}')

    # Verify the execution order
    # The actual order depends on handler registration and event processing
    print(f'\nActual execution order: {execution_order}')

    # 1. Parent handler starts
    assert execution_order[0] == 'parent_start'

    # 2. ImmediateChild is processed immediately (during await)
    assert 'bus2_handler_ImmediateChild' in execution_order

    # 3. Parent handler should finish (if no error)
    if 'parent_end' in execution_order:
        parent_end_idx = execution_order.index('parent_end')
        assert parent_end_idx > 1

    # 4. Count events: 1 ImmediateChild, 1 QueuedChild, 1 Parent
    assert execution_order.count('bus2_handler_ImmediateChild') == 1
    assert execution_order.count('bus2_handler_QueuedChild') == 1
    assert execution_order.count('bus2_handler_Parent') == 1

    print('\n✅ All comprehensive patterns work correctly!')

    # Print event history tree before destroyping buses
    print('\nEvent History Trees:')
    print(f'bus1 has {len(bus1.event_history)} events in history')
    print(f'bus2 has {len(bus2.event_history)} events in history')

    # Debug: show which events have None parent
    print('\nEvents with no parent (roots):')
    for event in bus1.event_history.values():
        if event.event_parent_id is None:
            print(f'  - {event}')

    from abxbus.logging import log_eventbus_tree

    log_eventbus_tree(bus1)
    log_eventbus_tree(bus2)

    await bus1.destroy(clear=True)
    await bus2.destroy(clear=True)


async def test_await_forwarded_event_waits_for_target_bus_handlers():
    """
    Awaiting a dispatched event on source bus must wait for forwarded target-bus
    handlers too, not only the source forwarding handler.
    """
    bus_src = EventBus(name='ForwardWaitSrc')
    bus_dst = EventBus(name='ForwardWaitDst')

    class ForwardedEvent(BaseEvent[str]):
        pass

    target_started = asyncio.Event()
    target_finished = asyncio.Event()

    async def target_handler(event: ForwardedEvent) -> str:
        target_started.set()
        await asyncio.sleep(0.05)
        target_finished.set()
        return 'target_done'

    bus_src.on('*', bus_dst.emit)
    bus_dst.on(ForwardedEvent, target_handler)

    try:
        t0 = asyncio.get_running_loop().time()
        event = await bus_src.emit(ForwardedEvent())
        elapsed = asyncio.get_running_loop().time() - t0

        assert target_started.is_set()
        assert target_finished.is_set()
        assert elapsed >= 0.04
        assert any(
            result.eventbus_name == 'ForwardWaitDst' and result.handler_name.endswith('target_handler')
            for result in event.event_results.values()
        ), event.event_results
        assert all(result.status in ('completed', 'error') for result in event.event_results.values())
    finally:
        await bus_src.destroy(clear=True)
        await bus_dst.destroy(clear=True)


async def test_race_condition_stress():
    """Stress test to ensure no race conditions."""
    print('\n=== Test Race Condition Stress ===')

    bus1 = EventBus(name='bus1')
    bus2 = EventBus(name='bus2')

    results: list[str] = []

    async def child_handler(event: BaseEvent[str]) -> str:
        bus_name = event.event_path[-1] if event.event_path else 'unknown'
        results.append(f'child_{bus_name}')
        # Add small delay to simulate work
        await asyncio.sleep(0.001)
        return f'child_done_{bus_name}'

    async def parent_handler(event: BaseEvent[Any]) -> str:
        # Dispatch multiple children in different ways
        children: list[BaseEvent[Any]] = []

        # Async dispatches
        for _ in range(3):
            children.append(bus1.emit(QueuedChildEvent()))

        # Sync dispatches
        for _ in range(3):
            child = await bus1.emit(ImmediateChildEvent())
            assert child.event_status == 'completed'
            children.append(child)

        # Verify all have correct parent
        assert all(c.event_parent_id == event.event_id for c in children)
        return 'parent_done'

    def bad_handler(bad: BaseEvent[Any]) -> None:
        pass

    # Setup forwarding
    bus1.on('*', bus2.emit)
    bus1.on(QueuedChildEvent, child_handler)
    bus1.on(ImmediateChildEvent, child_handler)
    bus2.on(QueuedChildEvent, child_handler)
    bus2.on(ImmediateChildEvent, child_handler)
    bus1.on(BaseEvent, parent_handler)
    bus1.on(BaseEvent, bad_handler)

    # Run multiple times to check for race conditions
    for run in range(5):
        results.clear()

        await bus1.emit(BaseEvent())
        await bus1.wait_until_idle()
        await bus2.wait_until_idle()

        # Should have 6 child events processed on each bus
        bus1_results = [entry for entry in results if entry.startswith(f'child_{bus1.label}')]
        bus2_results = [entry for entry in results if entry.startswith(f'child_{bus2.label}')]
        assert len(bus1_results) == 6, f'Run {run}: Expected 6 child_{bus1.label}, got {len(bus1_results)}'
        assert len(bus2_results) == 6, f'Run {run}: Expected 6 child_{bus2.label}, got {len(bus2_results)}'

    print('✅ No race conditions detected!')

    # Print event history tree for the last run
    print('\nEvent history for the last test run:')
    from abxbus.logging import log_eventbus_tree

    log_eventbus_tree(bus1)
    log_eventbus_tree(bus2)

    await bus1.destroy(clear=True)
    await bus2.destroy(clear=True)


async def test_awaited_child_jumps_queue_no_overshoot():
    """
    Test the edge case in BaseEvent.__await__() (models.py):
    - When a handler dispatches and awaits a child event, that child should
      execute immediately (jumping the FIFO queue)
    - Other queued events (Event2, Event3) should NOT be processed (no overshoot)
    - FIFO order should be maintained for remaining events after completion
    """
    print('\n=== Test Awaited Child Jumps Queue (No Overshoot) ===')

    bus = EventBus(name='TestBus', max_history_size=100)
    execution_order: list[str] = []

    class Event1(BaseEvent[str]):
        pass

    class Event2(BaseEvent[str]):
        pass

    class Event3(BaseEvent[str]):
        pass

    class ChildEvent(BaseEvent[str]):
        pass

    async def event1_handler(event: Event1) -> str:
        execution_order.append('Event1_start')
        # Dispatch and await child - this should jump the queue
        child = bus.emit(ChildEvent())
        execution_order.append('Child_dispatched')
        await child
        execution_order.append('Child_await_returned')
        execution_order.append('Event1_end')
        return 'event1_done'

    async def event2_handler(event: Event2) -> str:
        execution_order.append('Event2_start')
        execution_order.append('Event2_end')
        return 'event2_done'

    async def event3_handler(event: Event3) -> str:
        execution_order.append('Event3_start')
        execution_order.append('Event3_end')
        return 'event3_done'

    async def child_handler(event: ChildEvent) -> str:
        execution_order.append('Child_start')
        execution_order.append('Child_end')
        return 'child_done'

    bus.on(Event1, event1_handler)
    bus.on(Event2, event2_handler)
    bus.on(Event3, event3_handler)
    bus.on(ChildEvent, child_handler)

    try:
        # Dispatch all three events (they go into the queue)
        event1 = bus.emit(Event1())
        event2 = bus.emit(Event2())
        event3 = bus.emit(Event3())

        # Verify events are queued
        await asyncio.sleep(0)  # Let dispatch settle
        print(f'After dispatch: E1={event1.event_status}, E2={event2.event_status}, E3={event3.event_status}')

        # Await Event1 - this triggers processing and the child should jump queue
        await event1

        print(f'After await event1: {execution_order}')
        print(f'Statuses: E1={event1.event_status}, E2={event2.event_status}, E3={event3.event_status}')

        # KEY ASSERTION 1: Child executed during Event1's handler (jumped queue)
        assert 'Child_start' in execution_order, 'Child should have executed'
        assert 'Child_end' in execution_order, 'Child should have completed'
        child_start_idx = execution_order.index('Child_start')
        child_end_idx = execution_order.index('Child_end')
        event1_end_idx = execution_order.index('Event1_end')
        assert child_start_idx < event1_end_idx, 'Child should execute before Event1 ends'
        assert child_end_idx < event1_end_idx, 'Child should complete before Event1 ends'

        # KEY ASSERTION 2: Event2 and Event3 did NOT execute yet (no overshoot)
        assert 'Event2_start' not in execution_order, f'Event2 should NOT have started (no overshoot). Order: {execution_order}'
        assert 'Event3_start' not in execution_order, f'Event3 should NOT have started (no overshoot). Order: {execution_order}'

        # KEY ASSERTION 3: Event2 and Event3 have not completed yet.
        # They may be marked "started" once dequeued, even if their handlers haven't run.
        assert event2.event_status in ('pending', 'started'), f'Event2 should be pending/started, got {event2.event_status}'
        assert event3.event_status in ('pending', 'started'), f'Event3 should be pending/started, got {event3.event_status}'

        # Now let the remaining events process
        await bus.wait_until_idle()

        print(f'Final execution order: {execution_order}')

        # KEY ASSERTION 4: FIFO order maintained - Event2 before Event3
        event2_start_idx = execution_order.index('Event2_start')
        event3_start_idx = execution_order.index('Event3_start')
        assert event2_start_idx < event3_start_idx, 'FIFO: Event2 should start before Event3'

        # Verify all completed
        assert event2.event_status == 'completed'
        assert event3.event_status == 'completed'

        # KEY ASSERTION 5: event_history reflects dispatch order, but started_at/completed_at
        # timestamps reflect actual execution order (post-reordering)
        history_list = list(bus.event_history.values())
        history_types = [e.__class__.__name__ for e in history_list]
        print(f'Event history (dispatch order): {history_types}')

        # Find the child event and E2/E3
        child_event = next(e for e in history_list if isinstance(e, ChildEvent))
        event2_from_history = next(e for e in history_list if isinstance(e, Event2))
        event3_from_history = next(e for e in history_list if isinstance(e, Event3))

        # Verify execution order via timestamps: Child should have started before E2 and E3
        assert child_event.event_started_at is not None, 'Child should have started_at timestamp'
        assert event2_from_history.event_started_at is not None, 'Event2 should have started_at timestamp'
        assert event3_from_history.event_started_at is not None, 'Event3 should have started_at timestamp'

        assert child_event.event_started_at < event2_from_history.event_started_at, (
            f'Child should have started before Event2. Child: {child_event.event_started_at}, E2: {event2_from_history.event_started_at}'
        )
        assert child_event.event_started_at < event3_from_history.event_started_at, (
            f'Child should have started before Event3. Child: {child_event.event_started_at}, E3: {event3_from_history.event_started_at}'
        )

        print(f'Child started_at: {child_event.event_started_at}')
        print(f'Event2 started_at: {event2_from_history.event_started_at}')
        print(f'Event3 started_at: {event3_from_history.event_started_at}')

        print('✅ Awaited child jumps queue, no overshoot, FIFO maintained!')

    finally:
        await bus.destroy(clear=True)


async def test_dispatch_multiple_await_one_skips_others():
    """
    Test that when a handler dispatches multiple events and awaits only one,
    the awaited event jumps the queue while the non-awaited ones stay in place.

    Scenario:
    - Queue: [E1, E2, E3]
    - E1 handler dispatches ChildA, ChildB, ChildC (queue becomes [E2, E3, ChildA, ChildB, ChildC])
    - E1 handler awaits only ChildB
    - ChildB should jump to front and execute immediately
    - ChildA and ChildC should NOT execute (they stay behind E2, E3 in queue)
    - E2 and E3 should NOT execute during E1's handler
    """
    print('\n=== Test Dispatch Multiple, Await One ===')

    bus = EventBus(name='MultiDispatchBus', max_history_size=100)
    execution_order: list[str] = []

    class Event1(BaseEvent[str]):
        pass

    class Event2(BaseEvent[str]):
        pass

    class Event3(BaseEvent[str]):
        pass

    class ChildA(BaseEvent[str]):
        pass

    class ChildB(BaseEvent[str]):
        pass

    class ChildC(BaseEvent[str]):
        pass

    async def event1_handler(event: Event1) -> str:
        execution_order.append('Event1_start')

        # Dispatch three children but only await the middle one
        child_a = bus.emit(ChildA())
        execution_order.append('ChildA_dispatched')

        child_b = bus.emit(ChildB())
        execution_order.append('ChildB_dispatched')

        child_c = bus.emit(ChildC())
        execution_order.append('ChildC_dispatched')

        # Only await ChildB - it should jump the queue
        await child_b
        execution_order.append('ChildB_await_returned')

        execution_order.append('Event1_end')
        return 'event1_done'

    async def event2_handler(event: Event2) -> str:
        execution_order.append('Event2_start')
        execution_order.append('Event2_end')
        return 'event2_done'

    async def event3_handler(event: Event3) -> str:
        execution_order.append('Event3_start')
        execution_order.append('Event3_end')
        return 'event3_done'

    async def child_a_handler(event: ChildA) -> str:
        execution_order.append('ChildA_start')
        execution_order.append('ChildA_end')
        return 'child_a_done'

    async def child_b_handler(event: ChildB) -> str:
        execution_order.append('ChildB_start')
        execution_order.append('ChildB_end')
        return 'child_b_done'

    async def child_c_handler(event: ChildC) -> str:
        execution_order.append('ChildC_start')
        execution_order.append('ChildC_end')
        return 'child_c_done'

    bus.on(Event1, event1_handler)
    bus.on(Event2, event2_handler)
    bus.on(Event3, event3_handler)
    bus.on(ChildA, child_a_handler)
    bus.on(ChildB, child_b_handler)
    bus.on(ChildC, child_c_handler)

    try:
        # Dispatch E1, E2, E3
        event1 = bus.emit(Event1())
        event2 = bus.emit(Event2())
        event3 = bus.emit(Event3())

        # Await E1
        await event1

        print(f'After await event1: {execution_order}')

        # ChildB should have executed (it was awaited)
        assert 'ChildB_start' in execution_order, 'ChildB should have executed'
        assert 'ChildB_end' in execution_order, 'ChildB should have completed'

        # ChildB should have executed before Event1 ended (queue jump worked)
        child_b_end_idx = execution_order.index('ChildB_end')
        event1_end_idx = execution_order.index('Event1_end')
        assert child_b_end_idx < event1_end_idx, 'ChildB should complete before Event1 ends'

        # ChildA and ChildC should NOT have executed BEFORE Event1 ended (no overshoot)
        # They may have executed after Event1 completed (via background task), which is fine
        if 'ChildA_start' in execution_order:
            child_a_start_idx = execution_order.index('ChildA_start')
            assert child_a_start_idx > event1_end_idx, f'ChildA should NOT start before Event1 ends. Order: {execution_order}'
        if 'ChildC_start' in execution_order:
            child_c_start_idx = execution_order.index('ChildC_start')
            assert child_c_start_idx > event1_end_idx, f'ChildC should NOT start before Event1 ends. Order: {execution_order}'

        # E2 and E3 should NOT have executed BEFORE Event1 ended (no overshoot)
        if 'Event2_start' in execution_order:
            event2_start_idx = execution_order.index('Event2_start')
            assert event2_start_idx > event1_end_idx, f'Event2 should NOT start before Event1 ends. Order: {execution_order}'
        if 'Event3_start' in execution_order:
            event3_start_idx = execution_order.index('Event3_start')
            assert event3_start_idx > event1_end_idx, f'Event3 should NOT start before Event1 ends. Order: {execution_order}'

        # Now process remaining events
        await bus.wait_until_idle()

        print(f'Final execution order: {execution_order}')

        # Verify FIFO order for remaining: E2, E3, ChildA, ChildC
        # (ChildA and ChildC were dispatched after E2/E3 were already queued)
        event2_start_idx = execution_order.index('Event2_start')
        event3_start_idx = execution_order.index('Event3_start')
        child_a_start_idx = execution_order.index('ChildA_start')
        child_c_start_idx = execution_order.index('ChildC_start')

        assert event2_start_idx < event3_start_idx, 'FIFO: E2 before E3'
        assert event3_start_idx < child_a_start_idx, 'FIFO: E3 before ChildA'
        assert child_a_start_idx < child_c_start_idx, 'FIFO: ChildA before ChildC'

        print('✅ Dispatch multiple, await one works correctly!')

    finally:
        await bus.destroy(clear=True)


async def test_multi_bus_forwarding_with_queued_events():
    """
    Test queue jumping with multiple buses that have forwarding set up,
    where both buses already have events queued.

    Scenario:
    - Bus1 has [E1, E2] queued
    - Bus2 has [E3, E4] queued
    - E1's handler dispatches Child to Bus1 and awaits it
    - Child should jump Bus1's queue (ahead of E2)
    - Bus2 should continue processing independently (bus-serial is per-bus)
    """
    print('\n=== Test Multi-Bus Forwarding With Queued Events ===')

    bus1 = EventBus(name='Bus1', max_history_size=100)
    bus2 = EventBus(name='Bus2', max_history_size=100)
    execution_order: list[str] = []

    class Event1(BaseEvent[str]):
        pass

    class Event2(BaseEvent[str]):
        pass

    class Event3(BaseEvent[str]):
        pass

    class Event4(BaseEvent[str]):
        pass

    class ChildEvent(BaseEvent[str]):
        pass

    async def event1_handler(event: Event1) -> str:
        execution_order.append('Bus1_Event1_start')
        # Dispatch child to bus1 and await
        child = bus1.emit(ChildEvent())
        execution_order.append('Child_dispatched_to_Bus1')
        await child
        execution_order.append('Child_await_returned')
        execution_order.append('Bus1_Event1_end')
        return 'event1_done'

    async def event2_handler(event: Event2) -> str:
        execution_order.append('Bus1_Event2_start')
        execution_order.append('Bus1_Event2_end')
        return 'event2_done'

    async def event3_handler(event: Event3) -> str:
        execution_order.append('Bus2_Event3_start')
        execution_order.append('Bus2_Event3_end')
        return 'event3_done'

    async def event4_handler(event: Event4) -> str:
        execution_order.append('Bus2_Event4_start')
        execution_order.append('Bus2_Event4_end')
        return 'event4_done'

    async def child_handler(event: ChildEvent) -> str:
        execution_order.append('Child_start')
        execution_order.append('Child_end')
        return 'child_done'

    # Register handlers on respective buses
    bus1.on(Event1, event1_handler)
    bus1.on(Event2, event2_handler)
    bus1.on(ChildEvent, child_handler)

    bus2.on(Event3, event3_handler)
    bus2.on(Event4, event4_handler)

    try:
        # Queue events on both buses
        event1 = bus1.emit(Event1())
        event2 = bus1.emit(Event2())
        event3 = bus2.emit(Event3())
        event4 = bus2.emit(Event4())

        await asyncio.sleep(0)  # Let dispatch settle

        print(f'Bus1 queue size: {bus1.pending_event_queue.qsize() if bus1.pending_event_queue else 0}')
        print(f'Bus2 queue size: {bus2.pending_event_queue.qsize() if bus2.pending_event_queue else 0}')

        # Await E1 - child should jump Bus1's queue
        await event1

        print(f'After await event1: {execution_order}')

        # Child should have executed
        assert 'Child_start' in execution_order, 'Child should have executed'
        assert 'Child_end' in execution_order, 'Child should have completed'

        # Child should have executed before Event1 ended
        child_end_idx = execution_order.index('Child_end')
        event1_end_idx = execution_order.index('Bus1_Event1_end')
        assert child_end_idx < event1_end_idx, 'Child should complete before Event1 ends'

        # E2 on Bus1 should NOT have executed yet
        assert 'Bus1_Event2_start' not in execution_order, f'E2 on Bus1 should NOT have started. Order: {execution_order}'

        # Bus2 runs independently under bus-serial event concurrency.
        # Its queued events may already be running while Bus1 awaits the child.
        assert 'Bus2_Event3_start' in execution_order, f'E3 on Bus2 should have started. Order: {execution_order}'

        # Now process remaining events on both buses
        await bus1.wait_until_idle()
        await bus2.wait_until_idle()

        print(f'Final execution order: {execution_order}')

        # Verify all events eventually executed
        assert 'Bus1_Event2_start' in execution_order
        assert 'Bus2_Event3_start' in execution_order
        assert 'Bus2_Event4_start' in execution_order

        print('✅ Multi-bus forwarding with queued events works correctly!')

    finally:
        await bus1.destroy(clear=True)
        await bus2.destroy(clear=True)


async def test_await_already_completed_event():
    """
    Test that awaiting an event that's already completed is a no-op.
    The event isn't in the queue anymore, so there's nothing to reorder.
    """
    print('\n=== Test Await Already Completed Event ===')

    bus = EventBus(name='AlreadyCompletedBus', max_history_size=100)
    execution_order: list[str] = []

    class Event1(BaseEvent[str]):
        pass

    class Event2(BaseEvent[str]):
        pass

    async def event1_handler(event: Event1) -> str:
        execution_order.append('Event1_start')
        execution_order.append('Event1_end')
        return 'event1_done'

    async def event2_handler(event: Event2) -> str:
        execution_order.append('Event2_start')
        execution_order.append('Event2_end')
        return 'event2_done'

    bus.on(Event1, event1_handler)
    bus.on(Event2, event2_handler)

    try:
        # Dispatch and await E1 first
        event1 = await bus.emit(Event1())
        assert event1.event_status == 'completed'

        # Now dispatch E2
        event2 = bus.emit(Event2())

        # Await E1 again - should be a no-op since it's already completed
        await event1  # Should return immediately

        print(f'After second await event1: {execution_order}')

        # E2 should NOT have executed yet (we didn't trigger processing)
        # The second await on completed E1 should just return without processing queue
        assert event2.event_status == 'pending', f'E2 should still be pending, got {event2.event_status}'

        # Complete E2
        await bus.wait_until_idle()

        print(f'Final execution order: {execution_order}')
        print('✅ Await already completed event works correctly!')

    finally:
        await bus.destroy(clear=True)


async def test_multiple_awaits_same_event():
    """
    Test that multiple concurrent awaits on the same event work correctly.
    Only the first await should trigger queue reordering; subsequent awaits
    should just wait on the completion signal.
    """
    print('\n=== Test Multiple Awaits Same Event ===')

    bus = EventBus(name='MultiAwaitBus', max_history_size=100)
    execution_order: list[str] = []
    await_results: list[str] = []
    child_ref: BaseEvent[str] | None = None

    class Event1(BaseEvent[str]):
        pass

    class Event2(BaseEvent[str]):
        pass

    class ChildEvent(BaseEvent[str]):
        pass

    async def event1_handler(event: Event1) -> str:
        nonlocal child_ref
        execution_order.append('Event1_start')

        # Dispatch child
        child = bus.emit(ChildEvent())
        child_ref = child

        # Create multiple concurrent awaits on the same child
        async def await_child(name: str):
            await child
            await_results.append(f'{name}_completed')

        # Start two concurrent awaits
        task1 = asyncio.create_task(await_child('await1'))
        task2 = asyncio.create_task(await_child('await2'))

        # Wait for both
        await asyncio.gather(task1, task2)
        execution_order.append('Both_awaits_completed')

        execution_order.append('Event1_end')
        return 'event1_done'

    async def event2_handler(event: Event2) -> str:
        execution_order.append('Event2_start')
        execution_order.append('Event2_end')
        return 'event2_done'

    async def child_handler(event: ChildEvent) -> str:
        execution_order.append('Child_start')
        await asyncio.sleep(0.01)  # Small delay to ensure both awaits are waiting
        execution_order.append('Child_end')
        return 'child_done'

    bus.on(Event1, event1_handler)
    bus.on(Event2, event2_handler)
    bus.on(ChildEvent, child_handler)

    try:
        event1 = bus.emit(Event1())
        event2 = bus.emit(Event2())

        await event1

        print(f'After await event1: {execution_order}')
        print(f'Await results: {await_results}')

        # Both awaits should have completed
        assert len(await_results) == 2, f'Both awaits should complete, got {await_results}'
        assert 'await1_completed' in await_results
        assert 'await2_completed' in await_results

        # Child should have executed exactly once and before Event1 ended
        assert execution_order.count('Child_start') == 1
        assert execution_order.count('Child_end') == 1
        assert 'Child_start' in execution_order
        assert 'Child_end' in execution_order
        child_end_idx = execution_order.index('Child_end')
        event1_end_idx = execution_order.index('Event1_end')
        assert child_end_idx < event1_end_idx

        # Child event should have exactly one handler result (no double-run).
        assert child_ref is not None
        assert len(child_ref.event_results) == 1

        # E2 should NOT have executed yet
        assert 'Event2_start' not in execution_order, f'E2 should NOT have started. Order: {execution_order}'

        await bus.wait_until_idle()

        print(f'Final execution order: {execution_order}')
        print('✅ Multiple awaits same event works correctly!')

    finally:
        await bus.destroy(clear=True)


async def test_deeply_nested_awaited_children():
    """
    Test deeply nested awaited children: Event1 awaits Child1, which awaits Child2.
    All should complete before Event2 starts (no overshoot at any level).
    """
    print('\n=== Test Deeply Nested Awaited Children ===')

    bus = EventBus(name='DeepNestedBus', max_history_size=100)
    execution_order: list[str] = []

    class Event1(BaseEvent[str]):
        pass

    class Event2(BaseEvent[str]):
        pass

    class Child1(BaseEvent[str]):
        pass

    class Child2(BaseEvent[str]):
        pass

    async def event1_handler(event: Event1) -> str:
        execution_order.append('Event1_start')
        child1 = bus.emit(Child1())
        await child1
        execution_order.append('Event1_end')
        return 'event1_done'

    async def child1_handler(event: Child1) -> str:
        execution_order.append('Child1_start')
        child2 = bus.emit(Child2())
        await child2
        execution_order.append('Child1_end')
        return 'child1_done'

    async def child2_handler(event: Child2) -> str:
        execution_order.append('Child2_start')
        execution_order.append('Child2_end')
        return 'child2_done'

    async def event2_handler(event: Event2) -> str:
        execution_order.append('Event2_start')
        execution_order.append('Event2_end')
        return 'event2_done'

    bus.on(Event1, event1_handler)
    bus.on(Child1, child1_handler)
    bus.on(Child2, child2_handler)
    bus.on(Event2, event2_handler)

    try:
        event1 = bus.emit(Event1())
        event2 = bus.emit(Event2())

        await event1

        print(f'After await event1: {execution_order}')

        # All nested children should have completed
        assert 'Child1_start' in execution_order
        assert 'Child1_end' in execution_order
        assert 'Child2_start' in execution_order
        assert 'Child2_end' in execution_order

        # Verify nesting order: Child2 completes before Child1
        child2_end_idx = execution_order.index('Child2_end')
        child1_end_idx = execution_order.index('Child1_end')
        event1_end_idx = execution_order.index('Event1_end')
        assert child2_end_idx < child1_end_idx < event1_end_idx

        # E2 should NOT have started
        assert 'Event2_start' not in execution_order, f'E2 should NOT have started. Order: {execution_order}'

        await bus.wait_until_idle()

        print(f'Final execution order: {execution_order}')

        # E2 should start after E1 ends
        event2_start_idx = execution_order.index('Event2_start')
        assert event2_start_idx > event1_end_idx

        print('✅ Deeply nested awaited children works correctly!')

    finally:
        await bus.destroy(clear=True)


async def main():
    """Run all tests."""
    await test_comprehensive_patterns()
    await test_race_condition_stress()
    await test_awaited_child_jumps_queue_no_overshoot()
    await test_dispatch_multiple_await_one_skips_others()
    await test_multi_bus_forwarding_with_queued_events()
    await test_await_already_completed_event()
    await test_multiple_awaits_same_event()
    await test_deeply_nested_awaited_children()


if __name__ == '__main__':
    asyncio.run(main())
