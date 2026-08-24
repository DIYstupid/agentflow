from collections.abc import Callable, Mapping
from datetime import UTC, datetime

from graph.graph import Graph
from graph.node import ConditionNode
from runtime.context import ExecutionContext
from runtime.errors import RecoveryError, TaskTimeoutError
from runtime.manager import TaskManager
from runtime.task import AgentTask, TaskStatus
from storage.checkpoint_repository import Checkpoint, CheckpointRepository
from storage.task_repository import TaskRepository


GraphResolver = Mapping[str, Graph] | Callable[[str], Graph | None]


class RecoveryManager:
    """恢复异常退出时遗留的 RUNNING Task（at-least-once node execution）。"""

    def __init__(
        self,
        task_repository: TaskRepository,
        checkpoint_repository: CheckpointRepository,
        task_manager: TaskManager,
        graphs: GraphResolver,
    ) -> None:
        self.task_repository = task_repository
        self.checkpoint_repository = checkpoint_repository
        self.task_manager = task_manager
        self.graphs = graphs

    async def restore(self) -> list[AgentTask]:
        tasks = await self.task_repository.list_by_status(TaskStatus.RUNNING)
        restored: list[AgentTask] = []
        for task in tasks:
            restored.append(await self._restore_task(task))
        return restored

    async def _restore_task(self, task: AgentTask) -> AgentTask:
        graph = self._resolve_graph(task.graph_id)
        if graph is None:
            return await self._fail(
                task, f"graph {task.graph_id!r} is not registered for recovery"
            )
        try:
            graph.validate()
        except Exception as error:
            return await self._fail(task, f"graph validation failed: {error}")

        checkpoint = await self.checkpoint_repository.latest(task.task_id)
        if checkpoint is None:
            context = ExecutionContext(
                task_id=task.task_id,
                variables=dict(task.input),
            )
            next_node = graph.start_node
        else:
            if checkpoint.context.task_id != task.task_id:
                return await self._fail(task, "checkpoint belongs to a different task")
            if checkpoint.node_id not in graph.nodes:
                return await self._fail(
                    task,
                    f"checkpoint node {checkpoint.node_id!r} is missing from graph",
                )
            context = checkpoint.context
            try:
                next_node = self._next_node(graph, checkpoint)
            except RecoveryError as error:
                return await self._fail(task, str(error))

            if next_node is None:
                task.status = TaskStatus.COMPLETED
                task.current_node = checkpoint.node_id
                task.output = context.node_outputs.get(checkpoint.node_id)
                task.error = None
                task.completed_at = datetime.now(UTC)
                await self.task_repository.update(task)
                return self.task_manager.attach_completed(task, context)

        remaining_timeout = self._remaining_timeout(task)
        if remaining_timeout is not None and remaining_timeout <= 0:
            return await self._fail(
                task,
                "task timeout elapsed before recovery",
                error_type=TaskTimeoutError.__name__,
                context=context,
            )

        await self.task_manager.resume(
            task,
            graph,
            context,
            start_node=next_node,
            execution_timeout=remaining_timeout,
        )
        return task

    def _resolve_graph(self, graph_id: str) -> Graph | None:
        if callable(self.graphs):
            return self.graphs(graph_id)
        return self.graphs.get(graph_id)

    @staticmethod
    def _next_node(graph: Graph, checkpoint: Checkpoint) -> str | None:
        if checkpoint.next_node is not None:
            if checkpoint.next_node not in graph.nodes:
                raise RecoveryError(
                    f"checkpoint targets missing node {checkpoint.next_node!r}"
                )
            return checkpoint.next_node

        node = graph.nodes[checkpoint.node_id]
        if isinstance(node, ConditionNode):
            branch = checkpoint.context.node_outputs.get(checkpoint.node_id)
            if branch not in node.branches:
                raise RecoveryError(
                    f"checkpoint has invalid branch {branch!r} "
                    f"for condition node {checkpoint.node_id!r}"
                )
            return node.branches[branch]

        outgoing = [
            edge.target for edge in graph.edges if edge.source == checkpoint.node_id
        ]
        return outgoing[0] if outgoing else None

    @staticmethod
    def _remaining_timeout(task: AgentTask) -> float | None:
        if task.timeout_seconds is None:
            return None
        if task.started_at is None:
            return task.timeout_seconds
        started_at = task.started_at
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=UTC)
        elapsed = (datetime.now(UTC) - started_at).total_seconds()
        return task.timeout_seconds - elapsed

    async def _fail(
        self,
        task: AgentTask,
        message: str,
        error_type: str = RecoveryError.__name__,
        context: ExecutionContext | None = None,
    ) -> AgentTask:
        task.status = TaskStatus.FAILED
        task.error = f"{error_type}: {message}"
        task.completed_at = datetime.now(UTC)
        await self.task_repository.update(task)
        return self.task_manager.attach_completed(
            task,
            context
            or ExecutionContext(task_id=task.task_id, variables=dict(task.input)),
        )
