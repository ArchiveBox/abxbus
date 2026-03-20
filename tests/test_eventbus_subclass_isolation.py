from __future__ import annotations

from abxbus import BaseEvent, EventBus


def test_eventbus_subclasses_isolate_registries_and_global_serial_locks() -> None:
    class IsolatedBusA(EventBus):
        pass

    class IsolatedBusB(EventBus):
        pass

    bus_a1 = IsolatedBusA('IsolatedBusA1', event_concurrency='global-serial')
    bus_a2 = IsolatedBusA('IsolatedBusA2', event_concurrency='global-serial')
    bus_b1 = IsolatedBusB('IsolatedBusB1', event_concurrency='global-serial')

    assert bus_a1 in IsolatedBusA.all_instances
    assert bus_a2 in IsolatedBusA.all_instances
    assert bus_b1 not in IsolatedBusA.all_instances
    assert bus_b1 in IsolatedBusB.all_instances
    assert bus_a1 not in IsolatedBusB.all_instances
    assert bus_a1 not in EventBus.all_instances
    assert bus_b1 not in EventBus.all_instances

    lock_a1 = bus_a1.locks.get_lock_for_event(bus_a1, BaseEvent())
    lock_a2 = bus_a2.locks.get_lock_for_event(bus_a2, BaseEvent())
    lock_b1 = bus_b1.locks.get_lock_for_event(bus_b1, BaseEvent())

    assert lock_a1 is not None
    assert lock_a2 is not None
    assert lock_b1 is not None
    assert lock_a1 is lock_a2
    assert lock_a1 is not lock_b1
