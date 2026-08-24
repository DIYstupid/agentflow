from collections.abc import Callable
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
    ) -> Any:
        self.graph.validate()

        current: str | None = self.graph.start_node
        last_output: Any = None
        while current is not None:
            node = self.graph.nodes[current]
            if on_node_start is not None:
                on_node_start(node.id)
            result = await node.execute(context)
            context.node_outputs[node.id] = result.output
            last_output = result.output

            if result.next_node is not None:
                if result.next_node not in self.graph.nodes:
                    raise GraphValidationError(
                        f"node {node.id!r} routed to missing node {result.next_node!r}"
                    )
                current = result.next_node
            else:
                outgoing = [e.target for e in self.graph.edges if e.source == node.id]
                current = outgoing[0] if outgoing else None

        return last_output
