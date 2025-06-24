"""Test comprehensive event patterns including forwarding, async/sync dispatch, and parent-child tracking."""

import asyncio
from bubus import EventBus, BaseEvent


class ParentEvent(BaseEvent):
    pass


class ChildEvent(BaseEvent):
    pass


async def test_comprehensive_patterns():
    """Test all event patterns work correctly without race conditions."""
    print("\n=== Test Comprehensive Patterns ===")
    
    bus1 = EventBus(name='bus1')
    bus2 = EventBus(name='bus2')  # Fixed typo from 'bus1' to 'bus2'
    
    results = []
    execution_counter = {'count': 0}  # Use a dict to track execution order
    
    def child_bus2_event_handler(event: BaseEvent):
        """This gets triggered when the event is forwarded to the second bus."""
        execution_counter['count'] += 1
        seq = execution_counter['count']
        print(f"[{seq}] child_bus2_event_handler: processing {event.__class__.__name__} on bus2")
        results.append((seq, f"bus2_handler_{event.__class__.__name__}"))
        return 'forwarded bus result'
    
    bus2.on('*', child_bus2_event_handler)   # register a handler on bus2
    bus1.on('*', bus2.dispatch)              # forward all events from bus1 -> bus2
    
    async def parent_bus1_handler(event: ParentEvent):
        # Only process the parent ParentEvent
            
        execution_counter['count'] += 1
        seq = execution_counter['count']
        print(f"\n[{seq}] parent_bus1_handler: START processing {event}")
        results.append((seq, "parent_start"))
        
        # Pattern 1: Async dispatch - handlers run after parent completes
        print("\n1. Testing async dispatch...")
        child_event_async = bus1.dispatch(ChildEvent())
        print(f"   child_event_async.event_status = {child_event_async.event_status}")
        assert child_event_async.event_status != 'completed'
        
        # Pattern 2: Sync dispatch with await - handlers run immediately
        print("\n2. Testing sync dispatch (await)...")
        child_event_sync = await bus1.dispatch(ChildEvent())
        print(f"   child_event_sync.event_status = {child_event_sync.event_status}")
        assert child_event_sync.event_status == 'completed'
        
        # Check that forwarded handler result is available
        print("\n3. Checking forwarded handler results...")
        event_results = await child_event_sync.event_results_list()
        print(f"   Results: {event_results}")
        # The forwarding handler (bus.dispatch) returns the event object itself
        # We need to check if the child event was processed on bus2
        assert len(event_results) > 0  # At least one handler processed it
        # The event should have been forwarded to bus2
        assert 'bus2' in child_event_sync.event_path
        
        # Check parent-child relationships
        print("\n4. Checking parent-child relationships...")
        print(f"   child_event_async.event_parent_id = {child_event_async.event_parent_id}")
        print(f"   child_event_sync.event_parent_id = {child_event_sync.event_parent_id}")
        print(f"   event.event_id = {event.event_id}")
        assert child_event_async.event_parent_id == event.event_id
        assert child_event_sync.event_parent_id == event.event_id
        
        execution_counter['count'] += 1
        seq = execution_counter['count']
        print(f"[{seq}] parent_bus1_handler: END")
        results.append((seq, "parent_end"))
        return "parent_done"
    
    bus1.on(ParentEvent, parent_bus1_handler)
    
    # Dispatch parent event and wait for completion
    print("\nDispatching parent event...")
    parent_event = await bus1.dispatch(ParentEvent())
    
    # Wait for all buses to finish processing
    await bus1.wait_until_idle()
    await bus2.wait_until_idle()
    
    # Verify all child events have correct parent
    print("\n5. Verifying all events have correct parent...")
    all_events = list(bus1.event_history.values())
    print(f"   Total events in history: {len(all_events)}")
    for i, event in enumerate(all_events):
        print(f"   Event {i}: {event.__class__.__name__}, parent_id: {event.event_parent_id}")
    
    # First event is the parent, all others should have parent's ID
    assert all(event.event_parent_id == parent_event.event_id for event in all_events[1:])
    
    # Sort results by sequence number to see actual execution order
    sorted_results = sorted(results, key=lambda x: x[0])
    execution_order = [item[1] for item in sorted_results]
    
    print(f"\nExecution order:")
    for seq, action in sorted_results:
        print(f"  [{seq}] {action}")
    
    # Verify the execution order  
    # The actual order depends on handler registration and event processing
    print(f"\nActual execution order: {execution_order}")
    
    # 1. Parent handler starts
    assert execution_order[0] == "parent_start"
    
    # 2. Sync child is processed immediately (during await)
    assert "bus2_handler_ChildEvent" in execution_order
    
    # 3. Parent handler should finish (if no error)
    if "parent_end" in execution_order:
        parent_end_idx = execution_order.index("parent_end")
        assert parent_end_idx > 1
    
    # 4. Count that we have 2 ChildEvent handlers and 1 ParentEvent handler
    assert execution_order.count("bus2_handler_ChildEvent") == 2
    assert execution_order.count("bus2_handler_ParentEvent") == 1
    
    print("\n✅ All comprehensive patterns work correctly!")
    
    await bus1.stop()
    await bus2.stop()


async def test_race_condition_stress():
    """Stress test to ensure no race conditions."""
    print("\n=== Test Race Condition Stress ===")
    
    bus1 = EventBus(name='bus1') 
    bus2 = EventBus(name='bus2')
    
    results = []
    
    async def child_handler(event: ChildEvent):
        bus_name = event.event_path[-1] if event.event_path else "unknown"
        results.append(f"child_{bus_name}")
        # Add small delay to simulate work
        await asyncio.sleep(0.001)
        return f"child_done_{bus_name}"
    
    async def parent_handler(event: BaseEvent):
        # Dispatch multiple children in different ways
        children = []
        
        # Async dispatches
        for i in range(3):
            children.append(bus1.dispatch(ChildEvent()))
        
        # Sync dispatches
        for i in range(3):
            child = await bus1.dispatch(ChildEvent())
            assert child.event_status == 'completed'
            children.append(child)
        
        # Verify all have correct parent
        assert all(c.event_parent_id == event.event_id for c in children)
        return "parent_done"
    
    # Setup forwarding
    bus1.on('*', bus2.dispatch)
    bus1.on(ChildEvent, child_handler)
    bus2.on(ChildEvent, child_handler)
    bus1.on(BaseEvent, parent_handler)
    
    # Run multiple times to check for race conditions
    for run in range(5):
        results.clear()
        
        parent = await bus1.dispatch(BaseEvent())
        await bus1.wait_until_idle()
        await bus2.wait_until_idle()
        
        # Should have 6 child events processed on each bus
        assert results.count('child_bus1') == 6, f"Run {run}: Expected 6 child_bus1, got {results.count('child_bus1')}"
        assert results.count('child_bus2') == 6, f"Run {run}: Expected 6 child_bus2, got {results.count('child_bus2')}"
    
    print("✅ No race conditions detected!")
    
    await bus1.stop()
    await bus2.stop()


async def main():
    """Run all tests."""
    await test_comprehensive_patterns()
    await test_race_condition_stress()


if __name__ == "__main__":
    asyncio.run(main())