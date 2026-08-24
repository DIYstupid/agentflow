from typing import Any

from events import Event, EventBus, EventType
from graph.executor import GraphExecutor
from graph.graph import Graph
from runtime.context import ExecutionContext
from runtime.scheduler import Scheduler
from runtime.task import AgentTask, TaskStatus
from storage.checkpoint_repository import CheckpointRepository
from storage.task_repository import TaskRepository


class TaskManager:
    """创建、查询、等待和取消内存中的 Agent Task。"""

    def __init__(
        self,
        scheduler: Scheduler | None = None,
        task_repository: TaskRepository | None = None,
        checkpoint_repository: CheckpointRepository | None = None,
        event_bus: EventBus | None = None,
    ) -> None:
        self.scheduler = scheduler or Scheduler()
        self.task_repository = task_repository
        self.checkpoint_repository = checkpoint_repository
        self.event_bus = event_bus
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

        if self.task_repository is not None:
            await self.task_repository.create(task)

        async def mark_node_started(node_id: str) -> None:
            task.current_node = node_id
            if self.task_repository is not None:
                await self.task_repository.update(task)
            await self._publish(
                Event.create(task.task_id, EventType.NODE_STARTED, node_id=node_id)
            )

        async def save_checkpoint(
            node_id: str,
            next_node: str | None,
            execution_context: ExecutionContext,
        ) -> None:
            if self.checkpoint_repository is not None:
                await self.checkpoint_repository.save(
                    task.task_id, node_id, execution_context, next_node
                )
            await self._publish(
                Event.create(
                    task.task_id,
                    EventType.NODE_COMPLETED,
                    node_id=node_id,
                    data={"output": execution_context.node_outputs[node_id]},
                )
            )

        async def node_failed(node_id: str, error: Exception) -> None:
            await self._publish(
                Event.create(
                    task.task_id,
                    EventType.NODE_FAILED,
                    node_id=node_id,
                    data={"error": f"{type(error).__name__}: {error}"},
                )
            )

        async def run(execution_context: ExecutionContext) -> Any:
            return await executor.execute(
                execution_context,
                on_node_started=mark_node_started,
                on_checkpoint=save_checkpoint,
                on_node_failed=node_failed,
            )

        self._tasks[task.task_id] = task
        self._contexts[task.task_id] = context
        try:
            await self._publish(
                Event.create(
                    task.task_id,
                    EventType.TASK_CREATED,
                    data={"graph_id": task.graph_id},
                )
            )
            await self.scheduler.submit(
                task,
                context,
                run,
                state_callback=self._task_state_changed,
            )
        except Exception:
            self._tasks.pop(task.task_id, None)
            self._contexts.pop(task.task_id, None)
            if self.task_repository is not None:
                await self.task_repository.delete(task.task_id)
            raise
        return task

    async def resume(
        self,
        task: AgentTask,
        graph: Graph,
        context: ExecutionContext,
        start_node: str,
        execution_timeout: float | None = None,
    ) -> AgentTask:
        """Re-admit a persisted RUNNING task without creating a new database row."""
        graph.validate()
        if task.graph_id != graph.graph_id:
            raise ValueError(
                f"task graph {task.graph_id!r} does not match {graph.graph_id!r}"
            )
        if context.task_id != task.task_id:
            raise ValueError("checkpoint context belongs to a different task")
        if start_node not in graph.nodes:
            raise ValueError(f"resume node {start_node!r} does not exist")
        if task.task_id in self._tasks:
            raise ValueError(f"task {task.task_id!r} is already managed")

        executor = GraphExecutor(graph)

        async def mark_node_started(node_id: str) -> None:
            task.current_node = node_id
            if self.task_repository is not None:
                await self.task_repository.update(task)
            await self._publish(
                Event.create(task.task_id, EventType.NODE_STARTED, node_id=node_id)
            )

        async def save_checkpoint(
            node_id: str,
            next_node: str | None,
            execution_context: ExecutionContext,
        ) -> None:
            if self.checkpoint_repository is not None:
                await self.checkpoint_repository.save(
                    task.task_id, node_id, execution_context, next_node
                )
            await self._publish(
                Event.create(
                    task.task_id,
                    EventType.NODE_COMPLETED,
                    node_id=node_id,
                    data={"output": execution_context.node_outputs[node_id]},
                )
            )

        async def node_failed(node_id: str, error: Exception) -> None:
            await self._publish(
                Event.create(
                    task.task_id,
                    EventType.NODE_FAILED,
                    node_id=node_id,
                    data={"error": f"{type(error).__name__}: {error}"},
                )
            )

        async def run(execution_context: ExecutionContext) -> Any:
            return await executor.execute(
                execution_context,
                on_node_started=mark_node_started,
                on_checkpoint=save_checkpoint,
                on_node_failed=node_failed,
                start_node=start_node,
            )

        task.status = TaskStatus.RUNNING
        task.completed_at = None
        task.error = None
        self._tasks[task.task_id] = task
        self._contexts[task.task_id] = context
        try:
            await self.scheduler.submit(
                task,
                context,
                run,
                state_callback=self._task_state_changed,
                wait_for_capacity=True,
                execution_timeout=execution_timeout,
            )
        except BaseException:
            self._tasks.pop(task.task_id, None)
            self._contexts.pop(task.task_id, None)
            raise
        return task

    async def attach_completed(
        self, task: AgentTask, context: ExecutionContext
    ) -> AgentTask:
        if task.task_id in self._tasks:
            raise ValueError(f"task {task.task_id!r} is already managed")
        self._tasks[task.task_id] = task
        self._contexts[task.task_id] = context
        await self._publish_task_status(task)
        task._done.set()
        return task

    async def _task_state_changed(self, task: AgentTask) -> None:
        if self.task_repository is not None:
            await self.task_repository.update(task)
        await self._publish_task_status(task)

    async def _publish_task_status(self, task: AgentTask) -> None:
        event_type = {
            TaskStatus.RUNNING: EventType.TASK_STARTED,
            TaskStatus.COMPLETED: EventType.TASK_COMPLETED,
            TaskStatus.FAILED: EventType.TASK_FAILED,
            TaskStatus.CANCELLED: EventType.TASK_CANCELLED,
        }.get(task.status)
        if event_type is None:
            return
        data: dict[str, Any] = {"status": task.status.value}
        if task.output is not None:
            data["output"] = task.output
        if task.error is not None:
            data["error"] = task.error
        await self._publish(Event.create(task.task_id, event_type, data=data))

    async def _publish(self, event: Event) -> None:
        if self.event_bus is not None:
            await self.event_bus.publish(event)

    def get(self, task_id: str) -> AgentTask | None:
        return self._tasks.get(task_id)

    def get_context(self, task_id: str) -> ExecutionContext | None:
        return self._contexts.get(task_id)

    async def load(self, task_id: str) -> AgentTask | None:
        task = self.get(task_id)
        if task is not None or self.task_repository is None:
            return task
        return await self.task_repository.get(task_id)

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
