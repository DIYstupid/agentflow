from dataclasses import dataclass, field

from graph.edge import Edge
from graph.node import ConditionNode, Node
from runtime.errors import GraphError


class GraphValidationError(GraphError):
    """Graph 校验失败（§8 / §17）。"""


@dataclass
class Graph:
    """DAG 描述（§8）：nodes 为 id -> Node，edges 为无条件边，start_node 为入口。"""

    graph_id: str
    nodes: dict[str, Node]
    edges: list[Edge] = field(default_factory=list)
    start_node: str = ""

    def validate(self) -> None:
        """执行前校验（§8）：

        1. start node 是否存在
        2. Node ID 与 dict key 一致（重复 ID 在此暴露）
        3. edge 的 source / target 是否存在
        4. ConditionNode branch 目标是否存在
        5. 非 Condition Node 至多一条出边（分支必须走 ConditionNode）
        6. 从 start node 可达性：所有 Node 必须可达
        7. 不存在 cycle（V1 为 DAG）
        """
        # 1. start node
        if self.start_node not in self.nodes:
            raise GraphValidationError(
                f"start node {self.start_node!r} does not exist in nodes: {sorted(self.nodes)}"
            )

        # 2. Node ID 与 key 一致
        for key, node in self.nodes.items():
            if node.id != key:
                raise GraphValidationError(
                    f"node id {node.id!r} does not match its key {key!r}"
                )

        # 3. edge 端点
        for edge in self.edges:
            if edge.source not in self.nodes:
                raise GraphValidationError(
                    f"edge {edge.source!r} -> {edge.target!r}: source node does not exist"
                )
            if edge.target not in self.nodes:
                raise GraphValidationError(
                    f"edge {edge.source!r} -> {edge.target!r}: target node does not exist"
                )

        # 4. branch 目标
        for node_id, node in self.nodes.items():
            if isinstance(node, ConditionNode):
                for key, target in node.branches.items():
                    if target not in self.nodes:
                        raise GraphValidationError(
                            f"condition node {node_id!r} branch {key!r} "
                            f"targets missing node {target!r}"
                        )

        # 5. 非 Condition Node 至多一条出边
        for node_id, node in self.nodes.items():
            out_edges = [e for e in self.edges if e.source == node_id]
            if not isinstance(node, ConditionNode) and len(out_edges) > 1:
                raise GraphValidationError(
                    f"node {node_id!r} has {len(out_edges)} outgoing edges; "
                    f"use ConditionNode for branching"
                )

        # 6 + 7. 可达性 + cycle：从 start node DFS
        UNVISITED, VISITING, DONE = 0, 1, 2
        state: dict[str, int] = {}

        def successors(node_id: str) -> list[str]:
            node = self.nodes[node_id]
            targets: list[str] = []
            if isinstance(node, ConditionNode):
                targets.extend(node.branches.values())
            targets.extend(e.target for e in self.edges if e.source == node_id)
            return targets

        def dfs(node_id: str, path: list[str]) -> None:
            state[node_id] = VISITING
            for nxt in successors(node_id):
                if state.get(nxt) == VISITING:
                    cycle = path[path.index(nxt):] + [nxt]
                    raise GraphValidationError(f"cycle detected: {' -> '.join(cycle)}")
                if state.get(nxt) is None:
                    dfs(nxt, path + [nxt])
            state[node_id] = DONE

        dfs(self.start_node, [self.start_node])

        unreachable = sorted(n for n in self.nodes if n not in state)
        if unreachable:
            raise GraphValidationError(f"unreachable nodes: {unreachable}")
