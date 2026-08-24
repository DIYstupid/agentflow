from typing import Any

from graph.executor import GraphExecutor
from graph.graph import Graph
from runtime.context import ExecutionContext
from runtime.scheduler import Scheduler
from runtime.task import AgentTask


class TaskManager:
    """创建、查询、等待和取消内存中的 Agent Task。"""

    def __init__(self, scheduler: Scheduler | None = None) -> None:
        self.scheduler = scheduler or Scheduler()
        self._tasks: dict[str, AgentTask] = {}
        self._contexts: dict[str, ExecutionContext] = {}

    async def start(self) -> None:
        await self.scheduler.start()

    async def submit(
        self,
        graph: Graph,
        input: dict[str, Any],
        timeout_seconds: float | None = None,
    ) -> AgentTask:
        graph.validate()
        task = AgentTask.create(
            graph_id=graph.graph_id,
            input=input,
            timeout_seconds=timeout_seconds,
        )
        context = ExecutionContext(task_id=task.task_id, variables=dict(input))
        executor = GraphExecutor(graph)

        async def run(execution_context: ExecutionContext) -> Any:
            return await executor.execute(
                execution_context,
                on_node_start=lambda node_id: setattr(task, "current_node", node_id),
            )

        await self.scheduler.submit(task, context, run)
        self._tasks[task.task_id] = task
        self._contexts[task.task_id] = context
        return task

    def get(self, task_id: str) -> AgentTask | None:
        return self._tasks.get(task_id)

    def get_context(self, task_id: str) -> ExecutionContext | None:
        return self._contexts.get(task_id)

    async def wait(self, task_id: str) -> AgentTask:
        task = self._require(task_id)
        await task._done.wait()
        return task

    def cancel(self, task_id: str) -> bool:
        self._require(task_id)
        return self.scheduler.cancel(task_id)

    async def shutdown(self, cancel_tasks: bool = True) -> None:
        await self.scheduler.shutdown(cancel_tasks=cancel_tasks)

    def _require(self, task_id: str) -> AgentTask:
        try:
            return self._tasks[task_id]
        except KeyError:
            raise KeyError(f"task {task_id!r} does not exist") from None
