import asyncio
from datetime import UTC, datetime, timedelta

from graph import ConditionNode, Edge, FunctionNode, Graph
from runtime import RecoveryManager, Scheduler, TaskManager, TaskStatus
from runtime.context import ExecutionContext
from runtime.task import AgentTask
from storage import CheckpointRepository, Database, TaskRepository


def sequential_graph(log):
    def handler(name):
        async def run(context):
            log.append(name)
            return name.upper()

        return run

    return Graph(
        graph_id="sequential",
        nodes={
            "a": FunctionNode("a", handler("a")),
            "b": FunctionNode("b", handler("b")),
            "c": FunctionNode("c", handler("c")),
        },
        edges=[Edge("a", "b"), Edge("b", "c")],
        start_node="a",
    )


async def persist_running_task(
    tasks,
    graph_id,
    input=None,
    timeout_seconds=None,
    started_at=None,
):
    task = AgentTask.create(graph_id, input or {}, timeout_seconds)
    task.status = TaskStatus.RUNNING
    task.started_at = started_at or datetime.now(UTC)
    await tasks.create(task)
    return task


async def test_crash_recovery_skips_checkpointed_a_and_b(tmp_path):
    path = tmp_path / "agentflow.db"
    before_crash = Database(path)
    await before_crash.initialize()
    tasks = TaskRepository(before_crash)
    checkpoints = CheckpointRepository(before_crash)
    task = await persist_running_task(tasks, "sequential", {"question": "q"})
    context = ExecutionContext(
        task_id=task.task_id,
        variables={"question": "q"},
        node_outputs={"a": "A", "b": "B"},
    )
    await checkpoints.save(task.task_id, "b", context, next_node="c")
    await before_crash.close()

    execution_log = []
    graph = sequential_graph(execution_log)
    after_restart = Database(path)
    await after_restart.initialize()
    restored_tasks = TaskRepository(after_restart)
    restored_checkpoints = CheckpointRepository(after_restart)
    manager = TaskManager(
        task_repository=restored_tasks,
        checkpoint_repository=restored_checkpoints,
    )
    recovery = RecoveryManager(
        restored_tasks,
        restored_checkpoints,
        manager,
        {graph.graph_id: graph},
    )
    try:
        restored = await recovery.restore()
        assert [item.task_id for item in restored] == [task.task_id]
        completed = await manager.wait(task.task_id)

        assert completed.status is TaskStatus.COMPLETED
        assert completed.output == "C"
        assert execution_log == ["c"]
        saved = await restored_checkpoints.list_for_task(task.task_id)
        assert [checkpoint.node_id for checkpoint in saved] == ["b", "c"]
        assert saved[-1].context.node_outputs == {"a": "A", "b": "B", "c": "C"}
    finally:
        await manager.shutdown()
        await after_restart.close()


async def test_running_task_without_checkpoint_restarts_from_graph_start(tmp_path):
    database = Database(tmp_path / "agentflow.db")
    await database.initialize()
    tasks = TaskRepository(database)
    checkpoints = CheckpointRepository(database)
    task = await persist_running_task(tasks, "sequential", {"value": 1})
    log = []
    graph = sequential_graph(log)
    manager = TaskManager(
        task_repository=tasks, checkpoint_repository=checkpoints
    )
    try:
        await RecoveryManager(
            tasks, checkpoints, manager, {graph.graph_id: graph}
        ).restore()
        completed = await manager.wait(task.task_id)

        assert completed.status is TaskStatus.COMPLETED
        assert log == ["a", "b", "c"]
    finally:
        await manager.shutdown()
        await database.close()


async def test_condition_checkpoint_restores_selected_branch(tmp_path):
    database = Database(tmp_path / "agentflow.db")
    await database.initialize()
    tasks = TaskRepository(database)
    checkpoints = CheckpointRepository(database)
    task = await persist_running_task(tasks, "condition")
    await checkpoints.save(
        task.task_id,
        "condition",
        ExecutionContext(
            task_id=task.task_id,
            node_outputs={"condition": "yes"},
        ),
    )
    log = []

    async def choose(context):
        log.append("condition")
        return "no"

    def branch(name):
        async def run(context):
            log.append(name)
            return name

        return run

    graph = Graph(
        graph_id="condition",
        nodes={
            "condition": ConditionNode(
                "condition", choose, {"yes": "yes", "no": "no"}
            ),
            "yes": FunctionNode("yes", branch("yes")),
            "no": FunctionNode("no", branch("no")),
        },
        start_node="condition",
    )
    manager = TaskManager(
        task_repository=tasks, checkpoint_repository=checkpoints
    )
    try:
        await RecoveryManager(
            tasks, checkpoints, manager, {graph.graph_id: graph}
        ).restore()
        completed = await manager.wait(task.task_id)

        assert completed.status is TaskStatus.COMPLETED
        assert log == ["yes"]
    finally:
        await manager.shutdown()
        await database.close()


async def test_terminal_checkpoint_is_finalized_without_reexecution(tmp_path):
    database = Database(tmp_path / "agentflow.db")
    await database.initialize()
    tasks = TaskRepository(database)
    checkpoints = CheckpointRepository(database)
    task = await persist_running_task(tasks, "terminal")
    await checkpoints.save(
        task.task_id,
        "only",
        ExecutionContext(task_id=task.task_id, node_outputs={"only": "saved"}),
    )
    executed = False

    async def only(context):
        nonlocal executed
        executed = True
        return "new"

    graph = Graph(
        graph_id="terminal",
        nodes={"only": FunctionNode("only", only)},
        start_node="only",
    )
    manager = TaskManager(
        task_repository=tasks, checkpoint_repository=checkpoints
    )
    try:
        restored = await RecoveryManager(
            tasks, checkpoints, manager, {graph.graph_id: graph}
        ).restore()
        completed = await manager.wait(task.task_id)

        assert restored == [completed]
        assert completed.status is TaskStatus.COMPLETED
        assert completed.output == "saved"
        assert executed is False
    finally:
        await manager.shutdown()
        await database.close()


async def test_missing_graph_marks_task_failed(tmp_path):
    database = Database(tmp_path / "agentflow.db")
    await database.initialize()
    tasks = TaskRepository(database)
    checkpoints = CheckpointRepository(database)
    task = await persist_running_task(tasks, "missing")
    manager = TaskManager(
        task_repository=tasks, checkpoint_repository=checkpoints
    )
    try:
        restored = await RecoveryManager(tasks, checkpoints, manager, {}).restore()
        failed = await manager.wait(task.task_id)

        assert restored == [failed]
        assert failed.status is TaskStatus.FAILED
        assert failed.error.startswith("RecoveryError:")
        assert (await tasks.get(task.task_id)).status is TaskStatus.FAILED
    finally:
        await manager.shutdown()
        await database.close()


async def test_expired_task_timeout_is_not_reexecuted(tmp_path):
    database = Database(tmp_path / "agentflow.db")
    await database.initialize()
    tasks = TaskRepository(database)
    checkpoints = CheckpointRepository(database)
    task = await persist_running_task(
        tasks,
        "expired",
        timeout_seconds=1,
        started_at=datetime.now(UTC) - timedelta(seconds=10),
    )
    executed = False

    async def work(context):
        nonlocal executed
        executed = True

    graph = Graph(
        graph_id="expired",
        nodes={"work": FunctionNode("work", work)},
        start_node="work",
    )
    manager = TaskManager(
        task_repository=tasks, checkpoint_repository=checkpoints
    )
    try:
        await RecoveryManager(
            tasks, checkpoints, manager, {graph.graph_id: graph}
        ).restore()
        failed = await manager.wait(task.task_id)

        assert failed.status is TaskStatus.FAILED
        assert failed.error.startswith("TaskTimeoutError:")
        assert executed is False
    finally:
        await manager.shutdown()
        await database.close()


async def test_recovery_backpressures_instead_of_rejecting_queue(tmp_path):
    database = Database(tmp_path / "agentflow.db")
    await database.initialize()
    tasks = TaskRepository(database)
    checkpoints = CheckpointRepository(database)
    persisted = [await persist_running_task(tasks, "many") for _ in range(6)]

    async def work(context):
        await asyncio.sleep(0.001)
        return "done"

    graph = Graph(
        graph_id="many",
        nodes={"work": FunctionNode("work", work)},
        start_node="work",
    )
    manager = TaskManager(
        Scheduler(max_queue_size=1, max_concurrency=1),
        task_repository=tasks,
        checkpoint_repository=checkpoints,
    )
    try:
        restored = await RecoveryManager(
            tasks, checkpoints, manager, {graph.graph_id: graph}
        ).restore()
        completed = await asyncio.gather(
            *(manager.wait(task.task_id) for task in restored)
        )

        assert {task.task_id for task in completed} == {
            task.task_id for task in persisted
        }
        assert all(task.status is TaskStatus.COMPLETED for task in completed)
    finally:
        await manager.shutdown()
        await database.close()
