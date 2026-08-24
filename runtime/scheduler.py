import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from runtime.context import ExecutionContext
from runtime.errors import TaskCancelledError, TaskRejected, TaskTimeoutError
from runtime.task import AgentTask, TERMINAL_TASK_STATUSES, TaskStatus


TaskRunner = Callable[[ExecutionContext], Awaitable[Any]]


@dataclass(frozen=True)
class _WorkItem:
    task: AgentTask
    context: ExecutionContext
    runner: TaskRunner


class Scheduler:
    """有界、可取消的 asyncio Task Scheduler（DESIGN.md §19 / §49）。"""

    def __init__(self, max_queue_size: int = 100, max_concurrency: int = 10) -> None:
        if max_queue_size < 1:
            raise ValueError("max_queue_size must be at least 1")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")

        self._queue: asyncio.Queue[_WorkItem | None] = asyncio.Queue(
            maxsize=max_queue_size
        )
        self._max_concurrency = max_concurrency
        self._global_limit = asyncio.Semaphore(max_concurrency)
        self._workers: list[asyncio.Task[None]] = []
        self._running: dict[str, asyncio.Task[Any]] = {}
        self._known: dict[str, AgentTask] = {}
        self._started = False
        self._accepting = False

    @property
    def queue_depth(self) -> int:
        return self._queue.qsize()

    @property
    def running_count(self) -> int:
        return len(self._running)

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._accepting = True
        self._workers = [
            asyncio.create_task(self._worker(index), name=f"agentflow-worker-{index}")
            for index in range(self._max_concurrency)
        ]

    async def submit(
        self,
        task: AgentTask,
        context: ExecutionContext,
        runner: TaskRunner,
    ) -> None:
        if not self._started:
            await self.start()
        if not self._accepting:
            raise TaskRejected("scheduler is not accepting tasks")
        try:
            self._queue.put_nowait(_WorkItem(task=task, context=context, runner=runner))
        except asyncio.QueueFull:
            raise TaskRejected("scheduler queue is full") from None
        self._known[task.task_id] = task

    def cancel(self, task_id: str) -> bool:
        task = self._known.get(task_id)
        if task is None or task.status in TERMINAL_TASK_STATUSES:
            return False

        task.status = TaskStatus.CANCELLED
        task.error = f"{TaskCancelledError.__name__}: task was cancelled"

        execution = self._running.get(task_id)
        if execution is not None:
            execution.cancel()
        else:
            task.completed_at = datetime.now(UTC)
            task._done.set()
        return True

    async def shutdown(self, cancel_tasks: bool = True) -> None:
        if not self._started:
            return
        self._accepting = False

        if cancel_tasks:
            for task_id in list(self._known):
                self.cancel(task_id)

        await self._queue.join()
        for _ in self._workers:
            await self._queue.put(None)
        await asyncio.gather(*self._workers)

        self._workers.clear()
        self._running.clear()
        self._started = False

    async def _worker(self, index: int) -> None:
        del index
        while True:
            item = await self._queue.get()
            try:
                if item is None:
                    return
                if item.task.status is TaskStatus.CANCELLED:
                    continue
                async with self._global_limit:
                    await self._execute(item)
            finally:
                self._queue.task_done()

    async def _execute(self, item: _WorkItem) -> None:
        task = item.task
        if task.status is TaskStatus.CANCELLED:
            return

        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now(UTC)
        execution = asyncio.create_task(
            item.runner(item.context), name=f"agentflow-task-{task.task_id}"
        )
        self._running[task.task_id] = execution

        try:
            if task.timeout_seconds is None:
                task.output = await execution
            else:
                try:
                    async with asyncio.timeout(task.timeout_seconds) as timeout_scope:
                        task.output = await execution
                except TimeoutError:
                    if not timeout_scope.expired():
                        raise
                    raise TaskTimeoutError(
                        f"task exceeded {task.timeout_seconds}s timeout"
                    ) from None
        except TaskTimeoutError as error:
            task.status = TaskStatus.FAILED
            task.error = f"{TaskTimeoutError.__name__}: {error}"
        except asyncio.CancelledError:
            task.status = TaskStatus.CANCELLED
            task.error = f"{TaskCancelledError.__name__}: task was cancelled"
        except Exception as error:
            task.status = TaskStatus.FAILED
            task.error = f"{type(error).__name__}: {error}"
        else:
            # cancel() may win the narrow race after execution has produced a
            # result but before this worker has finalized the task.
            if task.status is TaskStatus.CANCELLED:
                task.output = None
            else:
                task.status = TaskStatus.COMPLETED
                task.error = None
        finally:
            self._running.pop(task.task_id, None)
            if task.completed_at is None:
                task.completed_at = datetime.now(UTC)
            task._done.set()
