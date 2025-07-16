# pyright: basic
"""
Tests for EventBus name conflict resolution with garbage collection.

Tests that EventBus instances that would be garbage collected don't cause
name conflicts when creating new instances with the same name.
"""

import weakref

import pytest

from bubus import EventBus


class TestNameConflictGC:
    """Test EventBus name conflict resolution with garbage collection"""

    def test_name_conflict_with_live_reference(self):
        """Test that name conflict generates a warning and auto-generates a unique name"""
        # Create an EventBus with a specific name
        bus1 = EventBus(name='TestBus')

        # Try to create another with the same name - should warn and auto-generate unique name
        with pytest.warns(UserWarning, match='EventBus with name "TestBus" already exists'):
            bus2 = EventBus(name='TestBus')

        # The second bus should have a unique name
        assert bus2.name.startswith('TestBus_')
        assert bus2.name != 'TestBus'
        assert len(bus2.name) == len('TestBus_') + 8  # Original name + underscore + 8 char suffix

    def test_name_no_conflict_after_deletion(self):
        """Test that name conflict is NOT raised after the existing bus is deleted"""
        # Create an EventBus with a specific name
        bus1 = EventBus(name='TestBus')

        # Delete the reference
        del bus1

        # Creating another with the same name should work since the first one has no references
        bus2 = EventBus(name='TestBus')
        assert bus2.name == 'TestBus'

    def test_name_no_conflict_with_no_reference(self):
        """Test that name conflict is NOT raised when the existing bus was never assigned"""
        # Create an EventBus with a specific name but don't keep a reference
        EventBus(name='TestBus')  # No assignment, will be garbage collected

        # Creating another with the same name should work since the first one is gone
        bus2 = EventBus(name='TestBus')
        assert bus2.name == 'TestBus'

    def test_name_conflict_with_weak_reference_only(self):
        """Test that name conflict is NOT raised when only weak references exist"""
        # Create an EventBus and keep only a weak reference
        bus1 = EventBus(name='TestBus')
        weak_ref = weakref.ref(bus1)

        # Verify the weak reference works
        assert weak_ref() is bus1

        # Delete the strong reference
        del bus1

        # At this point, only the weak reference exists (and the WeakSet reference)
        # Creating another with the same name should work
        bus2 = EventBus(name='TestBus')
        assert bus2.name == 'TestBus'

        # The weak reference should now return None
        assert weak_ref() is None

    def test_multiple_buses_with_gc(self):
        """Test multiple EventBus instances with some being garbage collected"""
        # Create multiple buses, some with strong refs, some without
        bus1 = EventBus(name='Bus1')
        EventBus(name='Bus2')  # Will be GC'd
        bus3 = EventBus(name='Bus3')
        EventBus(name='Bus4')  # Will be GC'd

        # Should be able to create new buses with the names of GC'd buses
        bus2_new = EventBus(name='Bus2')
        bus4_new = EventBus(name='Bus4')

        # But not with names of buses that still exist - they get auto-generated names
        with pytest.warns(UserWarning, match='EventBus with name "Bus1" already exists'):
            bus1_conflict = EventBus(name='Bus1')
        assert bus1_conflict.name.startswith('Bus1_')

        with pytest.warns(UserWarning, match='EventBus with name "Bus3" already exists'):
            bus3_conflict = EventBus(name='Bus3')
        assert bus3_conflict.name.startswith('Bus3_')

    @pytest.mark.asyncio
    async def test_name_conflict_after_stop_and_clear(self):
        """Test that clearing an EventBus allows reusing its name"""
        # Create an EventBus
        bus1 = EventBus(name='TestBus')

        # Stop and clear it
        await bus1.stop(clear=True)

        # Delete the reference to allow garbage collection
        del bus1

        # Now we should be able to create a new one with the same name
        bus2 = EventBus(name='TestBus')
        assert bus2.name == 'TestBus'

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
        """Test that our implementation removes dead EventBus from WeakSet during conflict check"""
        # Create a bus that will be "dead" (no strong references)
        EventBus(name='DeadBus')

        # When we try to create a new bus with the same name, the conflict check
        # should detect the dead bus and remove it from the WeakSet
        bus = EventBus(name='DeadBus')
        assert bus.name == 'DeadBus'

        # The dead bus should have been removed from all_instances
        names = [b.name for b in EventBus.all_instances if hasattr(b, 'name') and b.name == 'DeadBus']
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
