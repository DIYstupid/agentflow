import asyncio

import pytest

from graph.node import ConditionNode, FunctionNode, NodeResult, ParallelNode
from runtime.context import ExecutionContext
from runtime.errors import GraphError



async def test_function_node_executes_handler():
    calls = []

    async def handler(context):
        calls.append(context)
        return {"ok": True}

    node = FunctionNode(id="parse", handler=handler)
    context = ExecutionContext(task_id="t")
    result = await node.execute(context)

    assert calls == [context]
    assert isinstance(result, NodeResult)
    assert result.output == {"ok": True}
    assert result.next_node is None


async def test_condition_node_routes_branch():
    async def condition(context):
        return "yes"

    node = ConditionNode(
        id="check", condition=condition, branches={"yes": "search", "no": "finish"}
    )
    result = await node.execute(ExecutionContext(task_id="t"))
    assert result.output == "yes"
    assert result.next_node == "search"


async def test_condition_node_unknown_branch_raises():
    async def condition(context):
        return "maybe"

    node = ConditionNode(
        id="check", condition=condition, branches={"yes": "search", "no": "finish"}
    )
    with pytest.raises(GraphError, match="maybe"):
        await node.execute(ExecutionContext(task_id="t"))


async def test_parallel_node_runs_children_concurrently():
    # 并发性的确定性证明：第二个 child 释放第一个 child。
    # 若串行执行，first 将永远等不到 release → wait_for 超时 → 失败。
    started = asyncio.Event()
    release = asyncio.Event()

    async def first(context):
        started.set()
        await release.wait()
        return "first"

    async def second(context):
        await started.wait()
        release.set()
        return "second"

    node = ParallelNode(
        id="p",
        children=[
            FunctionNode(id="f", handler=first),
            FunctionNode(id="s", handler=second),
        ],
    )
    context = ExecutionContext(task_id="t")
    task = asyncio.create_task(node.execute(context))
    await asyncio.wait_for(started.wait(), 2)
    release.set()
    result = await asyncio.wait_for(task, 2)

    assert result.output == {"f": "first", "s": "second"}
    assert context.node_outputs["f"] == "first"
    assert context.node_outputs["s"] == "second"


async def test_parallel_node_fail_fast_cancels_siblings():
    sibling_cancelled = asyncio.Event()
    sibling_started = asyncio.Event()

    async def bad(context):
        await sibling_started.wait()
        raise RuntimeError("boom")

    async def slow(context):
        sibling_started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            sibling_cancelled.set()
            raise

    node = ParallelNode(
        id="p",
        children=[
            FunctionNode(id="bad", handler=bad),
            FunctionNode(id="slow", handler=slow),
        ],
    )
    with pytest.raises(RuntimeError, match="boom"):
        await node.execute(ExecutionContext(task_id="t"))
    assert sibling_cancelled.is_set()


def test_parallel_node_rejects_duplicate_child_ids():
    with pytest.raises(GraphError, match="duplicate"):
        ParallelNode(
            id="p",
            children=[
                FunctionNode(id="dup", handler=lambda c: None),
                FunctionNode(id="dup", handler=lambda c: None),
            ],
        )
