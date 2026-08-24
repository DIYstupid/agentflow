import asyncio

import pytest

from graph.edge import Edge
from graph.executor import GraphExecutor
from graph.graph import Graph, GraphValidationError
from graph.node import ConditionNode, FunctionNode, ParallelNode
from runtime.context import ExecutionContext


async def noop(context):
    return None


def make_node(node_id):
    return FunctionNode(id=node_id, handler=noop)


def make_recorder(name, log):
    async def handler(context):
        log.append(name)
        return name

    return FunctionNode(id=name, handler=handler)


def make_graph(nodes, edges, start):
    return Graph(graph_id="g", nodes=nodes, edges=edges, start_node=start)


def test_graph_validation():
    # 合法 DAG 通过校验
    graph = make_graph(
        nodes={"a": make_node("a"), "b": make_node("b"), "c": make_node("c")},
        edges=[Edge("a", "b"), Edge("b", "c")],
        start="a",
    )
    graph.validate()  # 不抛异常

    # start node 不存在
    with pytest.raises(GraphValidationError, match="start"):
        make_graph(nodes={"a": make_node("a")}, edges=[], start="missing").validate()


def test_invalid_edge():
    # edge target 不存在
    with pytest.raises(GraphValidationError, match="nope"):
        make_graph(
            nodes={"a": make_node("a")},
            edges=[Edge("a", "nope")],
            start="a",
        ).validate()

    # edge source 不存在
    with pytest.raises(GraphValidationError, match="nope"):
        make_graph(
            nodes={"a": make_node("a")},
            edges=[Edge("nope", "a")],
            start="a",
        ).validate()


def test_duplicate_node():
    nodes = {
        "a": FunctionNode(id="dup", handler=noop),
        "b": FunctionNode(id="dup", handler=noop),
    }
    with pytest.raises(GraphValidationError, match="dup"):
        make_graph(nodes=nodes, edges=[], start="a").validate()


def test_unreachable_node():
    with pytest.raises(GraphValidationError, match="reachable"):
        make_graph(
            nodes={"a": make_node("a"), "x": make_node("x")},
            edges=[],
            start="a",
        ).validate()


def test_cycle_detected():
    with pytest.raises(GraphValidationError, match="cycle"):
        make_graph(
            nodes={"a": make_node("a"), "b": make_node("b")},
            edges=[Edge("a", "b"), Edge("b", "a")],
            start="a",
        ).validate()


def test_condition_node_rejects_ordinary_outgoing_edge():
    async def pick(context):
        return "yes"

    graph = make_graph(
        nodes={
            "condition": ConditionNode(
                id="condition", condition=pick, branches={"yes": "selected"}
            ),
            "selected": make_node("selected"),
            "phantom": make_node("phantom"),
        },
        edges=[Edge("condition", "phantom")],
        start="condition",
    )

    with pytest.raises(GraphValidationError, match="branches only"):
        graph.validate()


async def test_condition_branch():
    log = []

    async def pick(context):
        log.append("cond")
        return context.variables["route"]

    def build():
        return make_graph(
            nodes={
                "start": make_recorder("start", log),
                "cond": ConditionNode(
                    id="cond", condition=pick, branches={"yes": "b", "no": "c"}
                ),
                "b": make_recorder("b", log),
                "c": make_recorder("c", log),
            },
            edges=[Edge("start", "cond")],
            start="start",
        )

    ctx_yes = ExecutionContext(task_id="t", variables={"route": "yes"})
    assert await GraphExecutor(build()).execute(ctx_yes) == "b"
    assert log == ["start", "cond", "b"]

    ctx_no = ExecutionContext(task_id="t", variables={"route": "no"})
    assert await GraphExecutor(build()).execute(ctx_no) == "c"
    assert log == ["start", "cond", "b", "start", "cond", "c"]


async def test_parallel_node():
    # ParallelNode 嵌入 Graph 中并发执行：串行会死锁，wait_for 超时即失败
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

    graph = make_graph(
        nodes={
            "start": make_node("start"),
            "p": ParallelNode(
                id="p",
                children=[
                    FunctionNode(id="f", handler=first),
                    FunctionNode(id="s", handler=second),
                ],
            ),
        },
        edges=[Edge("start", "p")],
        start="start",
    )
    task = asyncio.create_task(GraphExecutor(graph).execute(ExecutionContext(task_id="t")))
    await asyncio.wait_for(started.wait(), 2)
    release.set()
    result = await asyncio.wait_for(task, 2)

    assert result == {"f": "first", "s": "second"}
