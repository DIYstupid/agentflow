from collections.abc import Awaitable, Callable
from typing import Any

from graph.graph import Graph, GraphValidationError
from runtime.context import ExecutionContext


class GraphExecutor:
    """执行 Graph（§47）。

    路由规则（按优先级）：
    1. NodeResult.next_node 非 None → 路由到该 Node（ConditionNode 分支）
    2. 否则沿该 Node 的唯一出边前进
    3. 无出边 → 执行结束，返回最后输出

    每个 Node 的输出同时写入 context.node_outputs[node.id]（§7），
    ParallelNode 的子 Node 输出也一并写入。
    """

    def __init__(self, graph: Graph) -> None:
        self.graph = graph

    async def execute(
        self,
        context: ExecutionContext,
        on_node_start: Callable[[str], None] | None = None,
        on_node_started: Callable[[str], Awaitable[None]] | None = None,
        on_node_completed: Callable[[str, ExecutionContext], Awaitable[None]]
        | None = None,
        on_checkpoint: Callable[[str, str | None, ExecutionContext], Awaitable[None]]
        | None = None,
        start_node: str | None = None,
    ) -> Any:
        self.graph.validate()

        current: str | None = start_node or self.graph.start_node
        if current not in self.graph.nodes:
            raise GraphValidationError(f"resume node {current!r} does not exist")
        last_output: Any = None
        while current is not None:
            node = self.graph.nodes[current]
            if on_node_start is not None:
                on_node_start(node.id)
            if on_node_started is not None:
                await on_node_started(node.id)
            result = await node.execute(context)
            context.node_outputs[node.id] = result.output
            last_output = result.output
            next_node = self._resolve_next(node.id, result.next_node)
            if on_node_completed is not None:
                await on_node_completed(node.id, context)
            if on_checkpoint is not None:
                await on_checkpoint(node.id, next_node, context)
            current = next_node

        return last_output

    def _resolve_next(self, node_id: str, dynamic_next: str | None) -> str | None:
        if dynamic_next is not None:
            if dynamic_next not in self.graph.nodes:
                raise GraphValidationError(
                    f"node {node_id!r} routed to missing node {dynamic_next!r}"
                )
            return dynamic_next
        outgoing = [e.target for e in self.graph.edges if e.source == node_id]
        return outgoing[0] if outgoing else None
