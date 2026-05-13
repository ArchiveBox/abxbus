# pyright: basic
"""
Tests for EventBus name conflict resolution with garbage collection.

Tests that EventBus instances that would be garbage collected don't cause
name conflicts when creating new instances with the same name.
"""

import asyncio
import gc
import weakref

import pytest

from abxbus import BaseEvent, EventBus


class TestNameConflictGC:
    """Test EventBus name conflict resolution with garbage collection"""

    def test_name_conflict_with_live_reference(self):
        """Test that name conflict generates a warning and auto-generates a unique name"""
        # Create an EventBus with a specific name
        bus1 = EventBus(name='GCTestConflict')

        # Try to create another with the same name - should warn and auto-generate unique name
        with pytest.warns(UserWarning, match='EventBus with name "GCTestConflict" already exists'):
            bus2 = EventBus(name='GCTestConflict')

        # The second bus should have a unique name
        assert bus2.name.startswith('GCTestConflict_')
        assert bus2.name != 'GCTestConflict'
        assert len(bus2.name) == len('GCTestConflict_') + 8  # Original name + underscore + 8 char suffix

    def test_name_no_conflict_after_deletion(self):
        """Test that name conflict is NOT raised after the existing bus is deleted and GC runs"""
        import gc

        # Create an EventBus with a specific name
        bus1 = EventBus(name='GCTestBus1')

        # Delete the reference and force GC
        del bus1
        gc.collect()  # Force garbage collection to release the WeakSet reference

        # Creating another with the same name should work since the first one was collected
        bus2 = EventBus(name='GCTestBus1')
        assert bus2.name == 'GCTestBus1'

    def test_name_no_conflict_with_no_reference(self):
        """Test that name conflict is NOT raised when the existing bus was never assigned"""
        import gc

        # Create an EventBus with a specific name but don't keep a reference
        EventBus(name='GCTestBus2')  # No assignment, will be garbage collected
        gc.collect()  # Force garbage collection

        # Creating another with the same name should work since the first one is gone
        bus2 = EventBus(name='GCTestBus2')
        assert bus2.name == 'GCTestBus2'

    def test_name_conflict_with_weak_reference_only(self):
        """Test that name conflict is NOT raised when only weak references exist"""
        import gc

        # Create an EventBus and keep only a weak reference
        bus1 = EventBus(name='GCTestBus3')
        weak_ref = weakref.ref(bus1)

        # Verify the weak reference works
        assert weak_ref() is bus1

        # Delete the strong reference and force GC
        del bus1
        gc.collect()  # Force garbage collection

        # At this point, only the weak reference exists (and the WeakSet reference)
        # Creating another with the same name should work
        bus2 = EventBus(name='GCTestBus3')
        assert bus2.name == 'GCTestBus3'

        # The weak reference should now return None
        assert weak_ref() is None

    def test_multiple_buses_with_gc(self):
        """Test multiple EventBus instances with some being garbage collected"""
        import gc

        # Create multiple buses, some with strong refs, some without
        bus1 = EventBus(name='GCMulti1')
        EventBus(name='GCMulti2')  # Will be GC'd
        bus3 = EventBus(name='GCMulti3')
        EventBus(name='GCMulti4')  # Will be GC'd

        gc.collect()  # Force garbage collection

        # Should be able to create new buses with the names of GC'd buses
        bus2_new = EventBus(name='GCMulti2')
        bus4_new = EventBus(name='GCMulti4')

        # But not with names of buses that still exist - they get auto-generated names
        with pytest.warns(UserWarning, match='EventBus with name "GCMulti1" already exists'):
            bus1_conflict = EventBus(name='GCMulti1')
        assert bus1_conflict.name.startswith('GCMulti1_')

        with pytest.warns(UserWarning, match='EventBus with name "GCMulti3" already exists'):
            bus3_conflict = EventBus(name='GCMulti3')
        assert bus3_conflict.name.startswith('GCMulti3_')

    @pytest.mark.asyncio
    async def test_name_conflict_after_destroy_and_clear(self):
        """Test that clearing an EventBus allows reusing its name"""
        import gc

        # Create an EventBus
        bus1 = EventBus(name='GCDestroyClear')

        # Destroy and clear it (this renames the bus to _destroyed_* and removes from all_instances)
        await bus1.destroy(clear=True)

        # Delete the reference and force GC
        del bus1
        gc.collect()

        # Now we should be able to create a new one with the same name
        bus2 = EventBus(name='GCDestroyClear')
        assert bus2.name == 'GCDestroyClear'

    def test_weakset_behavior(self):
        """Test that the WeakSet properly tracks EventBus instances"""
        initial_count = len(EventBus.all_instances)

        # Create some buses
        bus1 = EventBus(name='WeakTest1')
        bus2 = EventBus(name='WeakTest2')
        bus3 = EventBus(name='WeakTest3')

        # Check they're tracked
        assert len(EventBus.all_instances) == initial_count + 3

        # Delete one
        del bus2

        # The WeakSet should automatically remove it (no gc.collect needed)
        # But we need to check the actual buses in the set, not just the count
        names = {bus.name for bus in EventBus.all_instances if hasattr(bus, 'name') and bus.name.startswith('WeakTest')}
        assert 'WeakTest1' in names
        assert 'WeakTest3' in names
        # WeakTest2 might still be there until the next iteration

    def test_eventbus_removed_from_weakset(self):
        """Test that dead EventBus instances are removed from WeakSet after GC"""
        import gc

        # Create a bus that will be "dead" (no strong references)
        EventBus(name='GCDeadBus')
        gc.collect()  # Force garbage collection

        # When we try to create a new bus with the same name, it should work
        bus = EventBus(name='GCDeadBus')
        assert bus.name == 'GCDeadBus'

        # The dead bus should have been removed from all_instances
        names = [b.name for b in EventBus.all_instances if hasattr(b, 'name') and b.name == 'GCDeadBus']
        assert len(names) == 1  # Only the new one

    def test_concurrent_name_creation(self):
        """Test that concurrent creation with same name generates warning and unique name"""
        # This tests the edge case where two buses might be created nearly simultaneously
        bus1 = EventBus(name='ConcurrentTest')

        # Even if we're in the middle of checking, the second one should get a unique name
        with pytest.warns(UserWarning, match='EventBus with name "ConcurrentTest" already exists'):
            bus2 = EventBus(name='ConcurrentTest')

        assert bus1.name == 'ConcurrentTest'
        assert bus2.name.startswith('ConcurrentTest_')
        assert bus2.name != bus1.name

    @pytest.mark.asyncio
    async def test_unreferenced_buses_with_history_can_be_cleaned_without_instance_leak(self):
        """
        Buses with populated history may outlive local scope while runloops are still active,
        but they must be releasable via explicit cleanup without leaking all_instances.
        """
        import gc

        class GcHistoryEvent(BaseEvent[str]):
            pass

        baseline_instances = len(EventBus.all_instances)
        refs: list[weakref.ReferenceType[EventBus]] = []

        async def create_and_fill_bus(index: int) -> weakref.ReferenceType[EventBus]:
            bus = EventBus(name=f'GCNoDestroyBus_{index}')
            bus.on(GcHistoryEvent, lambda e: 'ok')
            for _ in range(40):
                await bus.emit(GcHistoryEvent())
            await bus.wait_until_idle()
            return weakref.ref(bus)

        for i in range(30):
            refs.append(await create_and_fill_bus(i))

        # Encourage GC/finalization first (best effort without explicit destroy()).
        for _ in range(20):
            gc.collect()
            await asyncio.sleep(0.02)

        alive_buses = [ref() for ref in refs if ref() is not None]
        still_live = [bus for bus in alive_buses if bus is not None]

        # Deterministically clean up anything still alive.
        for bus in still_live:
            await bus.destroy(clear=True, timeout=0)
        # Loop variable keeps a strong ref to the last bus in CPython.
        if still_live:
            del bus
        del still_live
        del alive_buses

        # Final GC and WeakSet purge.
        for _ in range(10):
            gc.collect()
            await asyncio.sleep(0.01)
        _ = list(EventBus.all_instances)

        assert all(ref() is None for ref in refs), 'all buses should be collectable after cleanup'
        assert len(EventBus.all_instances) <= baseline_instances

    @pytest.mark.asyncio
    async def test_unreferenced_buses_with_history_are_collected_without_destroy(self):
        """
        Unreferenced buses should be collectable without explicit destroy(clear=True),
        even after processing events and populating history.
        """
        import gc

        class GcImplicitEvent(BaseEvent[str]):
            pass

        baseline_instances = len(EventBus.all_instances)
        refs: list[weakref.ReferenceType[EventBus]] = []

        async def create_and_fill_bus(index: int) -> weakref.ReferenceType[EventBus]:
            bus = EventBus(name=f'GCImplicitNoDestroy_{index}')
            bus.on(GcImplicitEvent, lambda e: 'ok')
            for _ in range(30):
                await bus.emit(GcImplicitEvent())
            await bus.wait_until_idle()
            return weakref.ref(bus)

        for i in range(20):
            refs.append(await create_and_fill_bus(i))

        for _ in range(80):
            gc.collect()
            await asyncio.sleep(0.02)
            if all(ref() is None for ref in refs):
                break

        # Force WeakSet iteration to purge any dead refs.
        _ = list(EventBus.all_instances)

        assert all(ref() is None for ref in refs), 'all unreferenced buses should be collected without destroy()'
        assert len(EventBus.all_instances) <= baseline_instances

    def test_subclass_registry_and_global_lock_are_collected_with_subclass(self):
        """
        When a temporary EventBus subclass goes out of scope, its class-scoped
        all_instances registry and global-serial lock should be collectable too.
        """
        subclass_ref = None
        registry_ref = None
        lock_ref = None
        bus_ref = None

        def create_scoped_subclass() -> None:
            class ScopedSubclassBus(EventBus):
                pass

            bus = ScopedSubclassBus(name='ScopedSubclassBus', event_concurrency='global-serial')
            nonlocal subclass_ref, registry_ref, lock_ref, bus_ref
            subclass_ref = weakref.ref(ScopedSubclassBus)
            registry_ref = weakref.ref(ScopedSubclassBus.all_instances)
            lock_ref = weakref.ref(bus.event_global_serial_lock)
            bus_ref = weakref.ref(bus)

        create_scoped_subclass()
        assert subclass_ref is not None
        assert registry_ref is not None
        assert lock_ref is not None
        assert bus_ref is not None

        for _ in range(500):
            gc.collect()
            if subclass_ref() is None and registry_ref() is None and lock_ref() is None and bus_ref() is None:
                break

        assert bus_ref() is None, 'subclass bus instance should be collectable'
        assert subclass_ref() is None, 'subclass type should be collectable'
        assert registry_ref() is None, 'subclass all_instances registry should be collectable'
        assert lock_ref() is None, 'subclass global-serial lock should be collectable'
