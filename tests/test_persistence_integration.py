import asyncio

from graph import Edge, FunctionNode, Graph
from runtime import Scheduler, TaskManager, TaskStatus
from storage import CheckpointRepository, Database, TaskRepository


def two_node_graph(first, second):
    return Graph(
        graph_id="persistent-graph",
        nodes={
            "a": FunctionNode("a", first),
            "b": FunctionNode("b", second),
        },
        edges=[Edge("a", "b")],
        start_node="a",
    )


async def test_task_and_each_node_checkpoint_are_durable(tmp_path):
    path = tmp_path / "agentflow.db"
    database = Database(path)
    await database.initialize()
    tasks = TaskRepository(database)
    checkpoints = CheckpointRepository(database)

    async def first(context):
        return {"step": 1}

    async def second(context):
        return {"step": 2}

    manager = TaskManager(
        Scheduler(max_queue_size=4, max_concurrency=1),
        task_repository=tasks,
        checkpoint_repository=checkpoints,
    )
    task = await manager.submit(two_node_graph(first, second), {"input": "value"})
    completed = await manager.wait(task.task_id)
    await manager.shutdown()
    await database.close()

    assert completed.status is TaskStatus.COMPLETED

    reopened = Database(path)
    await reopened.initialize()
    try:
        persisted = await TaskRepository(reopened).get(task.task_id)
        saved = await CheckpointRepository(reopened).list_for_task(task.task_id)

        assert persisted is not None
        assert persisted.status is TaskStatus.COMPLETED
        assert persisted.output == {"step": 2}
        assert persisted.current_node == "b"
        assert [checkpoint.node_id for checkpoint in saved] == ["a", "b"]
        assert saved[0].context.node_outputs == {"a": {"step": 1}}
        assert saved[1].context.node_outputs == {
            "a": {"step": 1},
            "b": {"step": 2},
        }
    finally:
        await reopened.close()


async def test_next_node_waits_until_checkpoint_succeeds():
    checkpoint_started = asyncio.Event()
    allow_checkpoint = asyncio.Event()
    second_started = asyncio.Event()

    class BlockingCheckpointRepository:
        async def save(self, task_id, node_id, context, next_node=None):
            if node_id == "a":
                checkpoint_started.set()
                await allow_checkpoint.wait()

    async def first(context):
        return "a"

    async def second(context):
        second_started.set()
        return "b"

    manager = TaskManager(checkpoint_repository=BlockingCheckpointRepository())
    try:
        task = await manager.submit(two_node_graph(first, second), {})
        await checkpoint_started.wait()
        await asyncio.sleep(0)
        assert second_started.is_set() is False

        allow_checkpoint.set()
        assert (await manager.wait(task.task_id)).status is TaskStatus.COMPLETED
        assert second_started.is_set()
    finally:
        allow_checkpoint.set()
        await manager.shutdown()


async def test_checkpoint_failure_stops_graph():
    second_executed = False

    class FailingCheckpointRepository:
        async def save(self, task_id, node_id, context, next_node=None):
            raise RuntimeError("checkpoint unavailable")

    async def first(context):
        return "a"

    async def second(context):
        nonlocal second_executed
        second_executed = True
        return "b"

    manager = TaskManager(checkpoint_repository=FailingCheckpointRepository())
    try:
        task = await manager.submit(two_node_graph(first, second), {})
        completed = await manager.wait(task.task_id)

        assert completed.status is TaskStatus.FAILED
        assert completed.error == "RuntimeError: checkpoint unavailable"
        assert second_executed is False
        assert manager.get_context(task.task_id).node_outputs == {"a": "a"}
    finally:
        await manager.shutdown()


async def test_running_node_is_persisted_before_handler_executes(tmp_path):
    database = Database(tmp_path / "agentflow.db")
    await database.initialize()
    tasks = TaskRepository(database)
    entered_handler = asyncio.Event()
    release = asyncio.Event()

    async def work(context):
        entered_handler.set()
        await release.wait()
        return "done"

    graph = Graph(
        graph_id="current-node-graph",
        nodes={"long-running": FunctionNode("long-running", work)},
        start_node="long-running",
    )
    manager = TaskManager(task_repository=tasks)
    try:
        task = await manager.submit(graph, {})
        await entered_handler.wait()
        persisted = await tasks.get(task.task_id)

        assert persisted is not None
        assert persisted.status is TaskStatus.RUNNING
        assert persisted.current_node == "long-running"

        release.set()
        await manager.wait(task.task_id)
    finally:
        release.set()
        await manager.shutdown()
        await database.close()


async def test_pending_cancellation_is_persisted_before_wait_returns(tmp_path):
    database = Database(tmp_path / "agentflow.db")
    await database.initialize()
    tasks = TaskRepository(database)
    first_started = asyncio.Event()
    release = asyncio.Event()

    async def blocker(context):
        first_started.set()
        await release.wait()
        return "done"

    manager = TaskManager(
        Scheduler(max_queue_size=2, max_concurrency=1), task_repository=tasks
    )
    try:
        graph = Graph(
            graph_id="cancel-graph",
            nodes={"work": FunctionNode("work", blocker)},
            start_node="work",
        )
        running = await manager.submit(graph, {})
        await first_started.wait()
        pending = await manager.submit(graph, {})

        assert manager.cancel(pending.task_id)
        await manager.wait(pending.task_id)
        persisted = await tasks.get(pending.task_id)
        assert persisted is not None
        assert persisted.status is TaskStatus.CANCELLED
        assert persisted.completed_at is not None

        release.set()
        await manager.wait(running.task_id)
    finally:
        release.set()
        await manager.shutdown()
        await database.close()


async def test_concurrent_tasks_persist_without_sqlite_conflicts(tmp_path):
    database = Database(tmp_path / "agentflow.db")
    await database.initialize()
    tasks = TaskRepository(database)
    checkpoints = CheckpointRepository(database)

    async def work(context):
        await asyncio.sleep(0)
        return {"index": context.variables["index"]}

    graph = Graph(
        graph_id="concurrent-persistence",
        nodes={"work": FunctionNode("work", work)},
        start_node="work",
    )
    manager = TaskManager(
        Scheduler(max_queue_size=40, max_concurrency=8),
        task_repository=tasks,
        checkpoint_repository=checkpoints,
    )
    try:
        submitted = [
            await manager.submit(graph, {"index": index}) for index in range(40)
        ]
        completed = await asyncio.gather(
            *(manager.wait(task.task_id) for task in submitted)
        )

        assert all(task.status is TaskStatus.COMPLETED for task in completed)
        persisted = await tasks.list_by_status(TaskStatus.COMPLETED)
        assert len(persisted) == 40
        for task in submitted:
            checkpoint = await checkpoints.latest(task.task_id)
            assert checkpoint is not None
            assert checkpoint.node_id == "work"
    finally:
        await manager.shutdown()
        await database.close()
