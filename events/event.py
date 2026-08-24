from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import uuid4


class EventType(str, Enum):
    TASK_CREATED = "task_created"
    TASK_STARTED = "task_started"
    NODE_STARTED = "node_started"
    NODE_COMPLETED = "node_completed"
    NODE_FAILED = "node_failed"
    TOOL_STARTED = "tool_started"
    TOOL_RETRY = "tool_retry"
    TOOL_COMPLETED = "tool_completed"
    TOOL_FAILED = "tool_failed"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    TASK_CANCELLED = "task_cancelled"


TERMINAL_EVENT_TYPES = {
    EventType.TASK_COMPLETED,
    EventType.TASK_FAILED,
    EventType.TASK_CANCELLED,
}


@dataclass(frozen=True)
class Event:
    event_id: str
    task_id: str
    event_type: EventType
    node_id: str | None
    timestamp: datetime
    data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        task_id: str,
        event_type: EventType,
        node_id: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> "Event":
        return cls(
            event_id=str(uuid4()),
            task_id=task_id,
            event_type=event_type,
            node_id=node_id,
            timestamp=datetime.now(UTC),
            data=dict(data or {}),
        )
