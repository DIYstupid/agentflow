import json

import httpx

from api.stream import stream_task_events
from app.main import create_app
from events import Event, EventBus, EventType
from runtime import TaskManager
from runtime.task import AgentTask
from storage import Database, EventRepository, TaskRepository


def event_name(frame: str) -> str:
    return next(line.removeprefix("event: ") for line in frame.splitlines() if line.startswith("event: "))


async def test_sse_replays_history_then_switches_to_live_events(tmp_path):
    database = Database(tmp_path / "agentflow.db")
    await database.initialize()
    tasks = TaskRepository(database)
    events = EventRepository(database)
    task = AgentTask.create("graph", {})
    await tasks.create(task)
    bus = EventBus()
    bus.add_subscriber(events.save)
    await bus.publish(Event.create(task.task_id, EventType.TASK_CREATED))

    stream = stream_task_events(task.task_id, bus, events)
    first = await anext(stream)
    assert event_name(first) == "task_created"

    await bus.publish(Event.create(task.task_id, EventType.NODE_STARTED, node_id="a"))
    await bus.publish(Event.create(task.task_id, EventType.TASK_COMPLETED))
    second = await anext(stream)
    third = await anext(stream)
    assert [event_name(second), event_name(third)] == [
        "node_started",
        "task_completed",
    ]
    try:
        await anext(stream)
        assert False, "terminal event must close SSE stream"
    except StopAsyncIteration:
        pass
    await database.close()


async def test_sse_http_endpoint_and_not_found(tmp_path):
    database = Database(tmp_path / "agentflow.db")
    await database.initialize()
    tasks = TaskRepository(database)
    events = EventRepository(database)
    task = AgentTask.create("graph", {})
    await tasks.create(task)
    bus = EventBus()
    bus.add_subscriber(events.save)
    await bus.publish(Event.create(task.task_id, EventType.TASK_STARTED))
    await bus.publish(
        Event.create(
            task.task_id,
            EventType.TASK_COMPLETED,
            data={"answer": "你好"},
        )
    )
    manager = TaskManager(task_repository=tasks)
    app = create_app(manager, bus, events)
    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            response = await client.get(f"/tasks/{task.task_id}/events")
            missing = await client.get("/tasks/missing/events")

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        assert "event: task_started" in response.text
        assert "event: task_completed" in response.text
        data_line = next(
            line for line in response.text.splitlines() if '"answer"' in line
        )
        assert json.loads(data_line.removeprefix("data: "))["data"] == {
            "answer": "你好"
        }
        assert missing.status_code == 404
    finally:
        await database.close()
