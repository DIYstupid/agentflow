import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import uuid4


class TaskStatus(str, Enum):
    """一次 Agent 执行的生命周期状态（DESIGN.md §6 / §49）。"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_TASK_STATUSES = {
    TaskStatus.COMPLETED,
    TaskStatus.FAILED,
    TaskStatus.CANCELLED,
}


@dataclass
class AgentTask:
    task_id: str
    graph_id: str
    status: TaskStatus
    input: dict[str, Any]
    output: Any | None
    current_node: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    error: str | None
    timeout_seconds: float | None = None
    _done: asyncio.Event = field(default_factory=lambda: asyncio.Event(), repr=False)

    @classmethod
    def create(
        cls,
        graph_id: str,
        input: dict[str, Any],
        timeout_seconds: float | None = None,
    ) -> "AgentTask":
        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than 0")
        return cls(
            task_id=str(uuid4()),
            graph_id=graph_id,
            status=TaskStatus.PENDING,
            input=dict(input),
            output=None,
            current_node=None,
            created_at=datetime.now(UTC),
            started_at=None,
            completed_at=None,
            error=None,
            timeout_seconds=timeout_seconds,
        )

