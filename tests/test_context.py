from runtime.context import ExecutionContext


def test_context_defaults():
    ctx = ExecutionContext(task_id="t-1")
    assert ctx.task_id == "t-1"
    assert ctx.variables == {}
    assert ctx.node_outputs == {}
    assert ctx.metadata == {}


def test_context_initial_values():
    ctx = ExecutionContext(
        task_id="t-1",
        variables={"q": 1},
        node_outputs={"a": 2},
        metadata={"trace_id": "x"},
    )
    assert ctx.variables["q"] == 1
    assert ctx.node_outputs["a"] == 2
    assert ctx.metadata["trace_id"] == "x"
