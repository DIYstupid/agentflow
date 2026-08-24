from datetime import UTC, datetime, timedelta

from runtime.context import ExecutionContext
from runtime.task import AgentTask, TaskStatus
from storage import (
    CheckpointRepository,
    Database,
    EventRepository,
    TaskRepository,
)


async def test_task_repository_round_trip_and_status_query(tmp_path):
    database = Database(tmp_path / "agentflow.db")
    await database.initialize()
    repository = TaskRepository(database)
    try:
        task = AgentTask.create("graph-1", {"question": "你好"}, timeout_seconds=3)
        await repository.create(task)

        task.status = TaskStatus.RUNNING
        task.current_node = "planner"
        task.started_at = datetime.now(UTC)
        await repository.update(task)

        loaded = await repository.get(task.task_id)
        assert loaded is not None
        assert loaded.task_id == task.task_id
        assert loaded.status is TaskStatus.RUNNING
        assert loaded.input == {"question": "你好"}
        assert loaded.current_node == "planner"
        assert loaded.started_at == task.started_at
        assert loaded.timeout_seconds == 3
        assert [item.task_id for item in await repository.list_by_status(
            TaskStatus.RUNNING
        )] == [task.task_id]
        assert await repository.list_by_status(TaskStatus.COMPLETED) == []
    finally:
        await database.close()


async def test_checkpoint_save_and_restore_is_snapshot(tmp_path):
    database = Database(tmp_path / "agentflow.db")
    await database.initialize()
    tasks = TaskRepository(database)
    checkpoints = CheckpointRepository(database)
    try:
        task = AgentTask.create("graph-1", {"value": 1})
        await tasks.create(task)
        context = ExecutionContext(
            task_id=task.task_id,
            variables={"value": 1},
            node_outputs={"a": {"answer": 2}},
            metadata={"trace_id": "trace-1"},
        )
        saved = await checkpoints.save(task.task_id, "a", context)
        context.variables["value"] = 99
        context.node_outputs["b"] = "later"

        restored = await checkpoints.latest(task.task_id)
        assert restored is not None
        assert restored.id == saved.id
        assert restored.node_id == "a"
        assert restored.context.task_id == task.task_id
        assert restored.context.variables == {"value": 1}
        assert restored.context.node_outputs == {"a": {"answer": 2}}
        assert restored.context.metadata == {"trace_id": "trace-1"}
    finally:
        await database.close()


async def test_event_repository_preserves_order_and_payload(tmp_path):
    database = Database(tmp_path / "agentflow.db")
    await database.initialize()
    tasks = TaskRepository(database)
    events = EventRepository(database)
    try:
        task = AgentTask.create("graph-1", {})
        await tasks.create(task)
        now = datetime.now(UTC)
        first = await events.append(
            task.task_id,
            "task_started",
            {"attempt": 1},
            created_at=now,
        )
        second = await events.append(
            task.task_id,
            "node_started",
            {"input": "你好"},
            node_id="planner",
            created_at=now + timedelta(microseconds=1),
        )

        loaded = await events.list_for_task(task.task_id)
        assert [event.event_id for event in loaded] == [
            first.event_id,
            second.event_id,
        ]
        assert loaded[1].node_id == "planner"
        assert loaded[1].data == {"input": "你好"}
    finally:
        await database.close()


async def test_database_context_manager_and_cascade_delete(tmp_path):
    path = tmp_path / "nested" / "agentflow.db"
    async with Database(path) as database:
        tasks = TaskRepository(database)
        checkpoints = CheckpointRepository(database)
        task = AgentTask.create("graph-1", {})
        await tasks.create(task)
        await checkpoints.save(task.task_id, "a", ExecutionContext(task.task_id))
        await tasks.delete(task.task_id)
        assert await checkpoints.latest(task.task_id) is None

    assert path.exists()
