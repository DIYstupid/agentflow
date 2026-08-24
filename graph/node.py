import asyncio
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from runtime.context import ExecutionContext
from runtime.errors import GraphError


@dataclass
class NodeResult:
    """Node 执行结果（§9）。

    next_node 非 None 时由 GraphExecutor 直接路由（ConditionNode 使用）。
    """

    output: Any
    next_node: str | None = None


class Node(ABC):
    """Node 基类（§9）。所有 Node 通过 execute() 消费 ExecutionContext 并产出 NodeResult。"""

    def __init__(self, id: str) -> None:
        self.id = id

    @abstractmethod
    async def execute(self, context: ExecutionContext) -> NodeResult:
        """执行节点逻辑。"""


class FunctionNode(Node):
    """执行普通 Python 异步逻辑（§10）。"""

    def __init__(
        self,
        id: str,
        handler: Callable[[ExecutionContext], Awaitable[Any]],
    ) -> None:
        super().__init__(id)
        self.handler = handler

    async def execute(self, context: ExecutionContext) -> NodeResult:
        return NodeResult(output=await self.handler(context))


class ConditionNode(Node):
    """条件路由（§11）。condition 返回 branch key，路由到 branches[key]。"""

    def __init__(
        self,
        id: str,
        condition: Callable[[ExecutionContext], Awaitable[str]],
        branches: dict[str, str],
    ) -> None:
        super().__init__(id)
        self.condition = condition
        self.branches = branches

    async def execute(self, context: ExecutionContext) -> NodeResult:
        key = await self.condition(context)
        if key not in self.branches:
            raise GraphError(
                f"condition node {self.id!r} returned unknown branch {key!r}; "
                f"expected one of {sorted(self.branches)}"
            )
        return NodeResult(output=key, next_node=self.branches[key])


class ParallelNode(Node):
    """并发执行多个子 Node（§12），使用 asyncio.TaskGroup；任一失败即 Fail Fast。"""

    def __init__(self, id: str, children: list[Node]) -> None:
        super().__init__(id)
        child_ids = [child.id for child in children]
        if len(child_ids) != len(set(child_ids)):
            raise GraphError(f"parallel node {id!r} has duplicate child ids: {child_ids}")
        self.children = children

    async def execute(self, context: ExecutionContext) -> NodeResult:
        async def run(child: Node) -> NodeResult:
            result = await child.execute(context)
            context.node_outputs[child.id] = result.output
            return result

        try:
            async with asyncio.TaskGroup() as tg:
                tasks = {child.id: tg.create_task(run(child)) for child in self.children}
        except* Exception as eg:
            # Fail Fast（§12）：单个子任务失败时直接向上抛出原始异常；
            # 多个子任务失败时保留 ExceptionGroup 供上层处理。
            if len(eg.exceptions) == 1:
                raise eg.exceptions[0]
            raise
        outputs = {child_id: task.result().output for child_id, task in tasks.items()}
        return NodeResult(output=outputs)
