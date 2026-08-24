import asyncio
from uuid import UUID

import pytest

from graph import FunctionNode, Graph
from runtime import Scheduler, TaskManager, TaskStatus
from runtime.errors import TaskRejected


def single_node_graph(handler, graph_id="test-graph"):
    return Graph(
        graph_id=graph_id,
        nodes={"work": FunctionNode(id="work", handler=handler)},
        start_node="work",
    )


async def test_task_success_and_current_node():
    async def work(context):
        return {"answer": context.variables["value"] + 1}

    manager = TaskManager(Scheduler(max_queue_size=4, max_concurrency=2))
    try:
        task = await manager.submit(single_node_graph(work), {"value": 41})
        completed = await manager.wait(task.task_id)

        assert UUID(task.task_id).version == 4
        assert completed.status is TaskStatus.COMPLETED
        assert completed.output == {"answer": 42}
        assert completed.current_node == "work"
        assert completed.started_at is not None
        assert completed.completed_at is not None
        assert completed.error is None
        assert manager.get_context(task.task_id).node_outputs == {
            "work": {"answer": 42}
        }
    finally:
        await manager.shutdown()


async def test_task_failure():
    async def fail(context):
        raise RuntimeError("broken node")

    manager = TaskManager()
    try:
        task = await manager.submit(single_node_graph(fail), {})
        completed = await manager.wait(task.task_id)

        assert completed.status is TaskStatus.FAILED
        assert completed.output is None
        assert completed.error == "RuntimeError: broken node"
    finally:
        await manager.shutdown()


async def test_task_timeout_cancels_execution():
    cancelled = asyncio.Event()

    async def slow(context):
        try:
            await asyncio.sleep(30)
        finally:
            cancelled.set()

    manager = TaskManager()
    try:
        task = await manager.submit(
            single_node_graph(slow), {}, timeout_seconds=0.01
        )
        completed = await manager.wait(task.task_id)

        assert completed.status is TaskStatus.FAILED
        assert completed.error.startswith("TaskTimeoutError:")
        assert cancelled.is_set()
    finally:
        await manager.shutdown()


async def test_runner_timeout_error_is_regular_failure():
    async def downstream_timeout(context):
        raise TimeoutError("dependency timed out")

    manager = TaskManager()
    try:
        task = await manager.submit(
            single_node_graph(downstream_timeout), {}, timeout_seconds=5
        )
        completed = await manager.wait(task.task_id)

        assert completed.status is TaskStatus.FAILED
        assert completed.error == "TimeoutError: dependency timed out"
    finally:
        await manager.shutdown()


async def test_cancel_running_task_waits_for_cleanup():
    started = asyncio.Event()
    cleaned_up = asyncio.Event()

    async def work(context):
        started.set()
        try:
            await asyncio.sleep(30)
        finally:
            cleaned_up.set()

    manager = TaskManager()
    try:
        task = await manager.submit(single_node_graph(work), {})
        await started.wait()
        assert manager.cancel(task.task_id) is True

        completed = await manager.wait(task.task_id)
        assert completed.status is TaskStatus.CANCELLED
        assert completed.error.startswith("TaskCancelledError:")
        assert cleaned_up.is_set()
        assert manager.cancel(task.task_id) is False
    finally:
        await manager.shutdown()


async def test_cancel_pending_task_never_executes():
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    pending_executed = False

    async def blocker(context):
        first_started.set()
        await release_first.wait()
        return "first"

    async def pending(context):
        nonlocal pending_executed
        pending_executed = True
        return "second"

    manager = TaskManager(Scheduler(max_queue_size=2, max_concurrency=1))
    try:
        first = await manager.submit(single_node_graph(blocker, "first"), {})
        await first_started.wait()
        second = await manager.submit(single_node_graph(pending, "second"), {})

        assert second.status is TaskStatus.PENDING
        assert manager.cancel(second.task_id) is True
        assert (await manager.wait(second.task_id)).status is TaskStatus.CANCELLED

        release_first.set()
        assert (await manager.wait(first.task_id)).status is TaskStatus.COMPLETED
        assert pending_executed is False
    finally:
        await manager.shutdown()


async def test_bounded_queue_rejects_when_full():
    started = asyncio.Event()
    release = asyncio.Event()

    async def blocker(context):
        started.set()
        await release.wait()
        return "ok"

    graph = single_node_graph(blocker)
    manager = TaskManager(Scheduler(max_queue_size=1, max_concurrency=1))
    try:
        running = await manager.submit(graph, {})
        await started.wait()
        queued = await manager.submit(graph, {})
        assert manager.scheduler.queue_depth == 1

        with pytest.raises(TaskRejected, match="queue is full"):
            await manager.submit(graph, {})

        release.set()
        await asyncio.gather(
            manager.wait(running.task_id), manager.wait(queued.task_id)
        )
    finally:
        release.set()
        await manager.shutdown()


async def test_global_concurrency_with_120_tasks():
    active = 0
    peak = 0

    async def work(context):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.005)
            return {"index": context.variables["index"]}
        finally:
            active -= 1

    manager = TaskManager(Scheduler(max_queue_size=120, max_concurrency=7))
    try:
        tasks = [
            await manager.submit(single_node_graph(work), {"index": index})
            for index in range(120)
        ]
        completed = await asyncio.gather(
            *(manager.wait(task.task_id) for task in tasks)
        )

        assert all(task.status is TaskStatus.COMPLETED for task in completed)
        assert peak == 7
        assert manager.scheduler.running_count == 0
    finally:
        await manager.shutdown()


def test_scheduler_and_timeout_configuration_validation():
    with pytest.raises(ValueError, match="max_queue_size"):
        Scheduler(max_queue_size=0)
    with pytest.raises(ValueError, match="max_concurrency"):
        Scheduler(max_concurrency=0)

    async def work(context):
        return None

    task_manager = TaskManager()

    async def submit_invalid_timeout():
        with pytest.raises(ValueError, match="timeout_seconds"):
            await task_manager.submit(single_node_graph(work), {}, timeout_seconds=0)

    asyncio.run(submit_invalid_timeout())
