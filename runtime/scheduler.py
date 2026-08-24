import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from runtime.context import ExecutionContext
from runtime.errors import TaskCancelledError, TaskRejected, TaskTimeoutError
from runtime.task import AgentTask, TERMINAL_TASK_STATUSES, TaskStatus


TaskRunner = Callable[[ExecutionContext], Awaitable[Any]]
TaskStateCallback = Callable[[AgentTask], Awaitable[None]]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _WorkItem:
    task: AgentTask
    context: ExecutionContext
    runner: TaskRunner
    state_callback: TaskStateCallback | None
    execution_timeout: float | None


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
        self._state_callbacks: dict[str, TaskStateCallback] = {}
        self._finalizers: set[asyncio.Task[None]] = set()
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
        state_callback: TaskStateCallback | None = None,
        wait_for_capacity: bool = False,
        execution_timeout: float | None = None,
    ) -> None:
        if not self._started:
            await self.start()
        if not self._accepting:
            raise TaskRejected("scheduler is not accepting tasks")
        item = _WorkItem(
            task=task,
            context=context,
            runner=runner,
            state_callback=state_callback,
            execution_timeout=(
                task.timeout_seconds
                if execution_timeout is None
                else execution_timeout
            )
        )
        if wait_for_capacity:
            await self._queue.put(item)
        else:
            try:
                self._queue.put_nowait(item)
            except asyncio.QueueFull:
                raise TaskRejected("scheduler queue is full") from None
        self._known[task.task_id] = task
        if state_callback is not None:
            self._state_callbacks[task.task_id] = state_callback

    def cancel(self, task_id: str) -> bool:
        task = self._known.get(task_id)
        if task is None or task.status in TERMINAL_TASK_STATUSES:
            return False

        was_running = task.status is TaskStatus.RUNNING
        task.status = TaskStatus.CANCELLED
        task.error = f"{TaskCancelledError.__name__}: task was cancelled"

        execution = self._running.get(task_id)
        if execution is not None:
            execution.cancel()
        elif not was_running:
            task.completed_at = datetime.now(UTC)
            callback = self._state_callbacks.get(task_id)
            if callback is None:
                task._done.set()
            else:
                finalizer = asyncio.create_task(
                    self._persist_pending_cancellation(task, callback),
                    name=f"agentflow-cancel-{task.task_id}",
                )
                self._finalizers.add(finalizer)
                finalizer.add_done_callback(self._finalizers.discard)
        return True

    async def shutdown(self, cancel_tasks: bool = True) -> None:
        if not self._started:
            return
        self._accepting = False

        if cancel_tasks:
            for task_id in list(self._known):
                self.cancel(task_id)

        await self._queue.join()
        if self._finalizers:
            await asyncio.gather(*self._finalizers)
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
        if task.started_at is None:
            task.started_at = datetime.now(UTC)
        execution: asyncio.Task[Any] | None = None

        try:
            if item.state_callback is not None:
                await item.state_callback(task)
            if task.status is TaskStatus.CANCELLED:
                raise asyncio.CancelledError
            execution = asyncio.create_task(
                item.runner(item.context), name=f"agentflow-task-{task.task_id}"
            )
            self._running[task.task_id] = execution
            if item.execution_timeout is None:
                task.output = await execution
            else:
                try:
                    async with asyncio.timeout(item.execution_timeout) as timeout_scope:
                        task.output = await execution
                except TimeoutError:
                    if not timeout_scope.expired():
                        raise
                    raise TaskTimeoutError(
                        f"task exceeded {item.execution_timeout}s execution timeout"
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
            try:
                if item.state_callback is not None:
                    await item.state_callback(task)
            except Exception:
                logger.exception(
                    "failed to persist terminal task state",
                    extra={"task_id": task.task_id, "status": task.status.value},
                )
            finally:
                task._done.set()

    async def _persist_pending_cancellation(
        self,
        task: AgentTask,
        callback: TaskStateCallback,
    ) -> None:
        try:
            await callback(task)
        except Exception:
            logger.exception(
                "failed to persist cancelled task state",
                extra={"task_id": task.task_id},
            )
        finally:
            task._done.set()
