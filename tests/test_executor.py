import pytest

from graph.edge import Edge
from graph.executor import GraphExecutor
from graph.graph import Graph, GraphValidationError
from graph.node import FunctionNode
from runtime.context import ExecutionContext


def make_recorder(name, log):
    async def handler(context):
        log.append(name)
        return name

    return FunctionNode(id=name, handler=handler)


async def test_sequential_execution_records_outputs():
    log = []
    graph = Graph(
        graph_id="g",
        nodes={
            "a": make_recorder("a", log),
            "b": make_recorder("b", log),
            "c": make_recorder("c", log),
        },
        edges=[Edge("a", "b"), Edge("b", "c")],
        start_node="a",
    )
    ctx = ExecutionContext(task_id="t", variables={"input": 1})
    result = await GraphExecutor(graph).execute(ctx)

    assert result == "c"
    assert log == ["a", "b", "c"]
    assert ctx.node_outputs == {"a": "a", "b": "b", "c": "c"}
    assert ctx.variables == {"input": 1}


async def test_exception_propagates():
    log = []

    async def boom(context):
        raise RuntimeError("node failed")

    graph = Graph(
        graph_id="g",
        nodes={
            "a": FunctionNode(id="a", handler=boom),
            "b": make_recorder("b", log),
        },
        edges=[Edge("a", "b")],
        start_node="a",
    )
    with pytest.raises(RuntimeError, match="node failed"):
        await GraphExecutor(graph).execute(ExecutionContext(task_id="t"))
    assert log == []  # b 未被执行


async def test_executor_validates_before_running():
    log = []
    graph = Graph(
        graph_id="g",
        nodes={"a": make_recorder("a", log)},
        edges=[Edge("a", "ghost")],
        start_node="a",
    )
    with pytest.raises(GraphValidationError):
        await GraphExecutor(graph).execute(ExecutionContext(task_id="t"))
    assert log == []  # 校验失败时不执行任何 Node
