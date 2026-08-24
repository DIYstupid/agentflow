import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from storage.database import Database


@dataclass(frozen=True)
class StoredEvent:
    event_id: str
    task_id: str
    event_type: str
    node_id: str | None
    data: dict[str, Any]
    created_at: datetime


class EventRepository:
    """M4 的事件存储接口；EventBus 与运行时事件发布在 M6 接入。"""

    def __init__(self, database: Database) -> None:
        self.database = database

    async def append(
        self,
        task_id: str,
        event_type: str,
        data: dict[str, Any] | None = None,
        node_id: str | None = None,
        event_id: str | None = None,
        created_at: datetime | None = None,
    ) -> StoredEvent:
        event = StoredEvent(
            event_id=event_id or str(uuid4()),
            task_id=task_id,
            event_type=event_type,
            node_id=node_id,
            data=dict(data or {}),
            created_at=created_at or datetime.now(UTC),
        )
        await self.database.execute(
            """
            INSERT INTO events (
                event_id, task_id, event_type, node_id, data_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                event.task_id,
                event.event_type,
                event.node_id,
                json.dumps(event.data, ensure_ascii=False),
                event.created_at.isoformat(),
            ),
        )
        return event

    async def list_for_task(self, task_id: str) -> list[StoredEvent]:
        rows = await self.database.fetch_all(
            "SELECT * FROM events WHERE task_id = ? ORDER BY created_at, event_id",
            (task_id,),
        )
        return [
            StoredEvent(
                event_id=row["event_id"],
                task_id=row["task_id"],
                event_type=row["event_type"],
                node_id=row["node_id"],
                data=json.loads(row["data_json"]),
                created_at=datetime.fromisoformat(row["created_at"]),
            )
            for row in rows
        ]
