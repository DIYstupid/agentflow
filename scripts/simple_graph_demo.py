"""Milestone 1 Demo（DESIGN.md §47）：

    START
      ↓
      A
      ↓
    Condition
     ↙     ↘
    B       C

用法：.venv/bin/python scripts/simple_graph_demo.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from graph import ConditionNode, Edge, FunctionNode, Graph, GraphExecutor
from runtime.context import ExecutionContext


def build_graph() -> Graph:
    async def start(context: ExecutionContext):
        return dict(context.variables)  # 透传输入

    async def step_a(context: ExecutionContext):
        value = context.variables.get("value", 0)
        return {"stepped": value + 1}

    async def condition(context: ExecutionContext):
        return context.variables["route"]

    async def branch_b(context: ExecutionContext):
        return {"branch": "B"}

    async def branch_c(context: ExecutionContext):
        return {"branch": "C"}

    return Graph(
        graph_id="simple-demo",
        nodes={
            "START": FunctionNode(id="START", handler=start),
            "A": FunctionNode(id="A", handler=step_a),
            "Condition": ConditionNode(
                id="Condition", condition=condition, branches={"yes": "B", "no": "C"}
            ),
            "B": FunctionNode(id="B", handler=branch_b),
            "C": FunctionNode(id="C", handler=branch_c),
        },
        edges=[Edge("START", "A"), Edge("A", "Condition")],
        start_node="START",
    )


async def main() -> None:
    graph = build_graph()
    graph.validate()
    print(f"Graph {graph.graph_id!r} validated OK\n")

    for route in ("yes", "no"):
        context = ExecutionContext(
            task_id=f"demo-{route}", variables={"route": route, "value": 41}
        )
        output = await GraphExecutor(graph).execute(context)
        print(f"route={route!r}")
        print(f"  final output   : {output!r}")
        print(f"  context outputs: {context.node_outputs}")
        print()


if __name__ == "__main__":
    asyncio.run(main())
