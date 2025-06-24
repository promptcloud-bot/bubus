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

Create networks of event buses that forward events between each other (with automatic loop prevention):

```python
# Create a hierarchy of buses
main_bus = EventBus(name='MainBus')
auth_bus = EventBus(name='AuthBus')
data_bus = EventBus(name='DataBus')

# Forward events between buses (infinite loops are automatically prevented)
main_bus.on('*', auth_bus.dispatch)
auth_bus.on('*', data_bus.dispatch)

# Events flow through the hierarchy with tracking
event = main_bus.dispatch(MyEvent())
await event
print(event.event_path)  # ['MainBus', 'AuthBus', 'DataBus']  # list of busses that have already procssed the event
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

Enable parallel processing of handlers for better performance.  
The tradeoff is slightly less deterministic ordering as handler execution order will not be guaranteed when run in parallel.

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
# Block until a specific event is seen (with optional timeout)
request = await bus.dispatch(RequestEvent(...))
response = await bus.expect('ResponseEvent', timeout=30)

# Block until a specific event is seen (with optional predicate filtering)
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

Automatically track event relationships and causality tree:

```python
async def parent_handler(event: BaseEvent):
    # handlers can emit more events to be processed asynchronously after this handler completes
    child_event_async = bus.dispatch(ChildEvent())
    # ChildEvent handlers will run after parent_handler exits

    # you can also use expect to await asynchronously dispatched nested event completion
    await bus.expect(ChildEvent)

    # or you can dispatch an event and block until it finishes processing by awaiting the event
    # (recursively waits for all handlers, including if event is forwarded to other busses)
    child_event_sync = await bus.dispatch(ChildEvent())
    # ChildEvent handlers run immediately

    # in all cases, parent-child relationships are automagically tracked
    assert child_event_async.event_parent_id == parent_event.event_id
    assert child_event_sync.event_parent_id == parent_event.event_id

bus.dispatch(ParentEvent())
```

<br/>

---
---

<br/>

## API Documentation

### `EventBus`

The main event bus class that manages event processing and handler execution.

```python
EventBus(
    name: str | None = None,
    wal_path: Path | str | None = None,
    parallel_handlers: bool = False
)
```

**Parameters:**

- `name`: Optional unique name for the bus (auto-generated if not provided)
- `wal_path`: Path for write-ahead logging of events to a `jsonl` file (optional)
- `parallel_handlers`: If `True`, handlers run concurrently for each event, otherwise serially if `False` (the default)

#### `EventBus` Properties

- `name`: The bus identifier
- `id`: Unique UUID7 for this bus instance
- `event_history`: Dict of all events the bus has seen by event_id
- `events_pending`: List of events waiting to be processed
- `events_started`: List of events currently being processed
- `events_completed`: List of completed events


#### `EventBus` Methods

##### `on(event_type: str | Type[BaseEvent], handler: Callable)`

Subscribe a handler to events matching a specific event type or `'*'` for all events.

```python
bus.on('UserEvent', handler_func)  # By event type string
bus.on(UserEvent, handler_func)    # By event class
bus.on('*', handler_func)          # Wildcard - all events
```

##### `dispatch(event: BaseEvent) -> BaseEvent`

Enqueue an event for processing and return the pending `Event` immediately (synchronous).

```python
event = bus.dispatch(MyEvent(data="test"))
result = await event  # await the pending Event to get the completed Event
```

##### `expect(event_type: str | Type[BaseEvent], timeout: float | None=None, predicate: Callable[[BaseEvent], bool]=None) -> BaseEvent`

Wait for a specific event to occur.

```python
# Wait for any UserEvent
event = await bus.expect('UserEvent', timeout=30)

# Wait with custom filter
event = await bus.expect(
    'UserEvent',
    predicate=lambda e: e.user_id == 'specific_user'
)
```

##### `wait_until_idle(timeout: float | None=None)`

Wait until all events are processed and the bus is idle.

```python
await bus.wait_until_idle()             # wait indefinitely until EventBus has finished processing all events

await bus.wait_until_idle(timeout=5.0)  # wait up to 5 seconds
```

##### `stop(timeout: float | None=None)`

Stop the event bus, optionally waiting for pending events.

```python
await bus.stop(timeout=1.0)  # Graceful stop, wait up to 1sec for pending and active events to finish processing
await bus.stop()             # Immediate shutdown, aborts all pending and actively processing events
```

---

### `BaseEvent`

Base class for all events. Subclass `BaseEvent` to define your own events.

Make sure none of your own event data fields start with `event_` or `model_` to avoid clashing with `BaseEvent` or `pydantic` builtin attrs.

#### `BaseEvent` Fields

```python
class BaseEvent(BaseModel):
    # Framework-managed fields
    event_type: str              # Defaults to class name
    event_id: str                # Unique UUID7 identifier, auto-generated if not provided
    event_timeout: float = 60.0  # Maximum execution in seconds for each handler
    event_schema: str            # Module.Class@version (auto-set based on class & LIBRARY_VERSION env var)
    event_parent_id: str         # Parent event ID (auto-set)
    event_path: list[str]        # List of bus names traversed (auto-set)
    event_created_at: datetime   # When event was created, auto-generated
    event_results: dict[str, EventResult]   # Handler results
    
    # Data fields
    # ... subclass BaseEvent to add your own event data fields here ...
    # some_key: str
    # some_other_key: dict[str, int]
    # ...
```

`event.event_results` contains a dict of pending `EventResult` objects that will be completed once handlers finish executing.


#### `BaseEvent` Properties

- `event_status`: `Literal['pending', 'started', 'complete']` Event status
- `event_started_at`: `datetime` When first handler started processing
- `event_completed_at`: `datetime` When all handlers completed processing

#### `BaseEvent` Methods

##### `await event`

Await the `Event` object directly to get the completed `Event` object once all handlers have finished executing.

```python
event = bus.dispatch(MyEvent())
completed_event = await event

raw_result_values = [(await event_result) for event_result in completed_event.event_results.values()]
# equivalent to: completed_event.event_results_list()  (see below)
```

##### `event_result(timeout: float | None=None) -> Any`

Utility method helper to execute all the handlers and return the first handler's raw result value.

```python
result = await event.event_result()
```

##### `event_results_by_handler_id(timeout: float | None=None) -> dict`

Utility method helper to get all raw result values organized by `{handler_id: result_value}`.

```python
results = await event.event_results_by_handler_id()
# {'handler_id_1': result1, 'handler_id_2': result2}
```

##### `event_results_list(timeout: float | None=None) -> list[Any]`

Utility method helper to get all raw result values in a list.

```python
results = await event.event_results_list()
# [result1, result2]
```

##### `event_results_flat_dict(timeout: float | None=None) -> dict`

Utility method helper to merge all raw result values that are `dict`s into a single flat `dict`.

```python
results = await event.event_results_flat_dict()
# {'key1': 'value1', 'key2': 'value2'}
```

##### `event_results_flat_list(timeout: float | None=None) -> list`

Utility method helper to merge all raw result values that are `list`s into a single flat `list`.

```python
results = await event.event_results_flat_list()
# ['item1', 'item2', 'item3']
```


---

### `EventResult`

The placeholder object that represents the pending result from a single handler executing an event.  
`Event.event_results` contains a `dict[PythonIdStr, EventResult]` in the shape of `{handler_id: EventResult()}`.

You shouldn't need to ever directly use this class, it's an internal wrapper to track pending and completed results from each handler within `BaseEvent.event_results`.

#### `EventResult` Fields

```python
class EventResult(BaseModel):
    id: str                    # Unique identifier
    handler_id: str           # Handler function ID
    handler_name: str         # Handler function name
    eventbus_id: str          # Bus that executed this handler
    eventbus_name: str        # Bus name
    
    status: str               # 'pending', 'started', 'completed', 'error'
    result: Any               # Handler return value
    error: str | None         # Error message if failed
    
    started_at: datetime      # When handler started
    completed_at: datetime    # When handler completed
    timeout: float            # Handler timeout in seconds
```

#### `EventResult` Methods

##### `await result`

Await the `EventResult` object directly to get the raw result value.

```python
handler_result = event.event_results['handler_id']
value = await handler_result  # Returns result or raises an exception if handler hits an error
```

<br/>

---
---

<br/>

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
