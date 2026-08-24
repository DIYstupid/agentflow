import asyncio

import pytest

from events import Event, EventBus, EventStreamOverflow, EventType
from graph import Edge, FunctionNode, Graph
from runtime import TaskManager, TaskStatus
from runtime.errors import NonRetryableToolError, RetryableToolError
from storage import CheckpointRepository, Database, EventRepository, TaskRepository
from tools.base import Tool
from tools.executor import ToolExecutor
from tools.registry import ToolRegistry
from tools.retry import RetryPolicy


def two_node_graph(first, second):
    return Graph(
        graph_id="event-graph",
        nodes={"a": FunctionNode("a", first), "b": FunctionNode("b", second)},
        edges=[Edge("a", "b")],
        start_node="a",
    )


async def test_task_and_node_events_are_ordered_and_persisted(tmp_path):
    database = Database(tmp_path / "agentflow.db")
    await database.initialize()
    tasks = TaskRepository(database)
    checkpoints = CheckpointRepository(database)
    events = EventRepository(database)
    bus = EventBus()
    bus.add_subscriber(events.save)

    async def first(context):
        return "A"

    async def second(context):
        return "B"

    manager = TaskManager(
        task_repository=tasks,
        checkpoint_repository=checkpoints,
        event_bus=bus,
    )
    try:
        task = await manager.submit(two_node_graph(first, second), {})
        assert (await manager.wait(task.task_id)).status is TaskStatus.COMPLETED
        persisted = await events.list_events(task.task_id)

        assert [event.event_type for event in persisted] == [
            EventType.TASK_CREATED,
            EventType.TASK_STARTED,
            EventType.NODE_STARTED,
            EventType.NODE_COMPLETED,
            EventType.NODE_STARTED,
            EventType.NODE_COMPLETED,
            EventType.TASK_COMPLETED,
        ]
        assert [event.node_id for event in persisted[2:6]] == ["a", "a", "b", "b"]
        assert persisted[3].data == {"output": "A"}
        assert persisted[-1].data == {"status": "completed", "output": "B"}
    finally:
        await manager.shutdown()
        await database.close()


async def test_node_and_task_failure_events():
    captured = []
    bus = EventBus()

    async def capture(event):
        captured.append(event)

    bus.add_subscriber(capture)

    async def fail(context):
        raise RuntimeError("node broke")

    graph = Graph(
        graph_id="failure",
        nodes={"bad": FunctionNode("bad", fail)},
        start_node="bad",
    )
    manager = TaskManager(event_bus=bus)
    try:
        task = await manager.submit(graph, {})
        assert (await manager.wait(task.task_id)).status is TaskStatus.FAILED
        assert [event.event_type for event in captured] == [
            EventType.TASK_CREATED,
            EventType.TASK_STARTED,
            EventType.NODE_STARTED,
            EventType.NODE_FAILED,
            EventType.TASK_FAILED,
        ]
        assert captured[-2].data["error"] == "RuntimeError: node broke"
    finally:
        await manager.shutdown()


async def test_running_cancellation_emits_task_cancelled():
    captured = []
    started = asyncio.Event()
    bus = EventBus()

    async def capture(event):
        captured.append(event)

    bus.add_subscriber(capture)

    async def work(context):
        started.set()
        await asyncio.sleep(30)

    graph = Graph(
        graph_id="cancel",
        nodes={"work": FunctionNode("work", work)},
        start_node="work",
    )
    manager = TaskManager(event_bus=bus)
    try:
        task = await manager.submit(graph, {})
        await started.wait()
        manager.cancel(task.task_id)
        assert (await manager.wait(task.task_id)).status is TaskStatus.CANCELLED
        assert captured[-1].event_type is EventType.TASK_CANCELLED
        assert EventType.NODE_FAILED not in [event.event_type for event in captured]
    finally:
        await manager.shutdown()


class RetryThenSucceedTool(Tool):
    name = "retry_then_succeed"
    timeout = 1
    max_retries = 2
    max_concurrency = 1

    def __init__(self):
        self.calls = 0

    async def execute(self, arguments):
        self.calls += 1
        if self.calls < 3:
            raise RetryableToolError("temporary")
        return "ok"


class NonRetryableFailureTool(Tool):
    name = "non_retryable"
    timeout = 1
    max_retries = 3
    max_concurrency = 1

    async def execute(self, arguments):
        raise NonRetryableToolError("invalid")


async def test_tool_started_retry_and_completed_events():
    captured = []
    bus = EventBus()

    async def capture(event):
        captured.append(event)

    bus.add_subscriber(capture)
    registry = ToolRegistry()
    registry.register(RetryThenSucceedTool())
    executor = ToolExecutor(
        registry,
        retry_policy=RetryPolicy(base_delay=0, max_delay=0),
        event_bus=bus,
    )

    assert await executor.execute(
        "retry_then_succeed", {}, task_id="task-1", node_id="tool-node"
    ) == "ok"
    assert [event.event_type for event in captured] == [
        EventType.TOOL_STARTED,
        EventType.TOOL_RETRY,
        EventType.TOOL_RETRY,
        EventType.TOOL_COMPLETED,
    ]
    assert [event.data["attempt"] for event in captured[1:3]] == [1, 2]
    assert all(event.node_id == "tool-node" for event in captured)


async def test_tool_failure_event():
    captured = []
    bus = EventBus()

    async def capture(event):
        captured.append(event)

    bus.add_subscriber(capture)
    registry = ToolRegistry()
    registry.register(NonRetryableFailureTool())
    executor = ToolExecutor(registry, event_bus=bus)

    with pytest.raises(NonRetryableToolError):
        await executor.execute("non_retryable", {}, task_id="task-1")
    assert [event.event_type for event in captured] == [
        EventType.TOOL_STARTED,
        EventType.TOOL_FAILED,
    ]


async def test_event_streams_are_task_isolated():
    bus = EventBus()
    first = bus.subscribe("task-1")
    second = bus.subscribe("task-2")
    try:
        event = Event.create("task-1", EventType.TASK_STARTED)
        await bus.publish(event)
        assert await asyncio.wait_for(anext(first), 1) == event
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(second), 0.01)
    finally:
        first.close()
        second.close()


async def test_slow_event_consumer_is_disconnected_on_overflow():
    bus = EventBus()
    subscription = bus.subscribe("task-1", max_queue_size=1)
    await bus.publish(Event.create("task-1", EventType.TASK_STARTED))
    await bus.publish(Event.create("task-1", EventType.NODE_STARTED))

    with pytest.raises(EventStreamOverflow):
        await anext(subscription)
