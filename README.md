# `bubus`: Pydantic-based event bus for async Python

Bubus is an advanced Pydantic-powered event bus with async support, designed for building reactive, event-driven applications with Python. It provides a powerful yet simple API for implementing publish-subscribe patterns with type safety, async handlers, and advanced features like event forwarding between buses.

## Quickstart

Install bubus and get started with a simple event-driven application:

```bash
pip install bubus
```

```python
import asyncio
from bubus import EventBus, BaseEvent

class UserLoginEvent(BaseEvent):
    username: str
    timestamp: float

async def handle_login(event: UserLoginEvent):
    print(f"User {event.username} logged in at {event.timestamp}")
    return {"status": "success", "user": event.username}

bus = EventBus()
bus.on('UserLoginEvent', handle_login)

event = bus.dispatch(UserLoginEvent(username="alice", timestamp=1234567890))
result = await event
print(f"Login handled: {result.event_results}")
```

<br/>

---

<br/>

## Features

### Type-Safe Events with Pydantic

Define events as Pydantic models with full type checking and validation:

```python
from typing import Any
from bubus import BaseEvent

class OrderCreatedEvent(BaseEvent):
    order_id: str
    customer_id: str
    total_amount: float
    items: list[dict[str, Any]]

# Events are automatically validated
event = OrderCreatedEvent(
    order_id="ORD-123",
    customer_id="CUST-456", 
    total_amount=99.99,
    items=[{"sku": "ITEM-1", "quantity": 2}]
)
```

### Async and Sync Handler Support

Register both synchronous and asynchronous handlers for maximum flexibility:

```python
# Async handler
async def async_handler(event: BaseEvent):
    await asyncio.sleep(0.1)  # Simulate async work
    return "async result"

# Sync handler
def sync_handler(event: BaseEvent):
    return "sync result"

bus.on('MyEvent', async_handler)
bus.on('MyEvent', sync_handler)
```

### Event Pattern Matching

Subscribe to events using multiple patterns:

```python
# By event type string
bus.on('UserActionEvent', handler)

# By event model class
bus.on(UserActionEvent, handler)

# Wildcard - handle all events
bus.on('*', universal_handler)
```

### Hierarchical Event Bus Networks

Create networks of event buses that forward events between each other:

```python
# Create a hierarchy of buses
main_bus = EventBus(name='MainBus')
auth_bus = EventBus(name='AuthBus')
data_bus = EventBus(name='DataBus')

# Forward events between buses
main_bus.on('*', auth_bus.dispatch)
auth_bus.on('*', data_bus.dispatch)

# Events flow through the hierarchy with tracking
event = main_bus.dispatch(MyEvent())
await event
print(event.event_path)  # ['MainBus', 'AuthBus', 'DataBus']
```

### Event Results Aggregation

Collect and aggregate results from multiple handlers:

```python
async def config_handler_1(event):
    return {"debug": True, "port": 8080}

async def config_handler_2(event):
    return {"debug": False, "timeout": 30}

bus.on('GetConfig', config_handler_1)
bus.on('GetConfig', config_handler_2)

event = await bus.dispatch(BaseEvent(event_type='GetConfig'))

# Merge all dict results
config = await event.event_results_flat_dict()
# {'debug': False, 'port': 8080, 'timeout': 30}

# Or get individual results
results = await event.event_results_by_handler_id()
```

### FIFO Event Processing

Events are processed in strict FIFO order, maintaining consistency:

```python
# Events are processed in the order they were dispatched
for i in range(10):
    bus.dispatch(ProcessTaskEvent(task_id=i))

# Even with async handlers, order is preserved
await bus.wait_until_idle()
```

### Parallel Handler Execution

Enable parallel processing of handlers for better performance:

```python
# Create bus with parallel handler execution
bus = EventBus(parallel_handlers=True)

# Multiple handlers run concurrently for each event
bus.on('DataEvent', slow_handler_1)  # Takes 1 second
bus.on('DataEvent', slow_handler_2)  # Takes 1 second

start = time.time()
await bus.dispatch(DataEvent())
# Total time: ~1 second (not 2)
```

### Event Expectation (Async Waiting)

Wait for specific events with optional filtering:

```python
# Wait for any response event
response = await bus.expect('ResponseEvent', timeout=30)

# Wait with predicate filtering
response = await bus.expect(
    'ResponseEvent',
    predicate=lambda e: e.request_id == my_request_id,
    timeout=30
)
```

### Write-Ahead Logging

Persist events automatically for durability and debugging:

```python
# Enable WAL persistence
bus = EventBus(name='MyBus', wal_path='./events.jsonl')

# All completed events are automatically persisted
bus.dispatch(ImportantEvent(data="critical"))

# Events are saved as JSONL for easy processing
# {"event_type": "ImportantEvent", "data": "critical", ...}
```

### Event Context and Parent Tracking

Automatically track event relationships and causality:

```python
async def parent_handler(event: BaseEvent):
    # Child events automatically get parent_id set
    child_event = bus.dispatch(ChildEvent())
    return await child_event

# Parent-child relationships are tracked
parent = bus.dispatch(ParentEvent())
await parent
# Child events will have event_parent_id = parent.event_id
```

---

## Development

Set up the development environment using `uv`:

```bash
git clone https://github.com/browser-use/bubus && cd bubus

# Create virtual environment with Python 3.12
uv venv --python 3.12

# Activate virtual environment (varies by OS)
source .venv/bin/activate  # On Unix/macOS
# or
.venv\Scripts\activate  # On Windows

# Install dependencies
uv sync --dev --all-extras
```

```bash
# Run all tests
pytest tests -v x --full-trace

# Run specific test file
pytest tests/test_eventbus.py
```

## Inspiration

- https://www.cosmicpython.com/book/chapter_08_events_and_message_bus.html#message_bus_diagram ⭐️
- https://developer.mozilla.org/en-US/docs/Web/API/EventTarget ⭐️
- https://github.com/pytest-dev/pluggy ⭐️
- https://github.com/teamhide/fastapi-event ⭐️
- https://github.com/ethereum/lahja ⭐️
- https://github.com/enricostara/eventure ⭐️
- https://github.com/akhundMurad/diator ⭐️
- https://github.com/n89nanda/pyeventbus
- https://github.com/iunary/aioemit
- https://github.com/dboslee/evently
- https://github.com/ArcletProject/Letoderea
- https://github.com/seanpar203/event-bus
- https://github.com/n89nanda/pyeventbus
- https://github.com/nicolaszein/py-async-bus
- https://github.com/AngusWG/simple-event-bus
- https://www.joeltok.com/posts/2021-03-building-an-event-bus-in-python/

## License

This project is licensed under the MIT License. For more information, see the main browser-use repository: https://github.com/browser-use/browser-use
