import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from runtime.context import ExecutionContext
from storage.database import Database


@dataclass(frozen=True)
class Checkpoint:
    id: int
    task_id: str
    node_id: str
    context: ExecutionContext
    created_at: datetime


class CheckpointRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def save(
        self,
        task_id: str,
        node_id: str,
        context: ExecutionContext,
    ) -> Checkpoint:
        created_at = datetime.now(UTC)
        context_json = json.dumps(
            {
                "task_id": context.task_id,
                "variables": context.variables,
                "node_outputs": context.node_outputs,
                "metadata": context.metadata,
            },
            ensure_ascii=False,
        )
        checkpoint_id = await self.database.execute(
            """
            INSERT INTO checkpoints (task_id, node_id, context_json, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (task_id, node_id, context_json, created_at.isoformat()),
        )
        return Checkpoint(
            id=checkpoint_id,
            task_id=task_id,
            node_id=node_id,
            context=self._copy_context(context),
            created_at=created_at,
        )

    async def latest(self, task_id: str) -> Checkpoint | None:
        row = await self.database.fetch_one(
            """
            SELECT * FROM checkpoints
            WHERE task_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (task_id,),
        )
        return self._from_row(row) if row is not None else None

    async def list_for_task(self, task_id: str) -> list[Checkpoint]:
        rows = await self.database.fetch_all(
            "SELECT * FROM checkpoints WHERE task_id = ? ORDER BY id",
            (task_id,),
        )
        return [self._from_row(row) for row in rows]

    @staticmethod
    def _copy_context(context: ExecutionContext) -> ExecutionContext:
        data = json.loads(
            json.dumps(
                {
                    "variables": context.variables,
                    "node_outputs": context.node_outputs,
                    "metadata": context.metadata,
                }
            )
        )
        return ExecutionContext(task_id=context.task_id, **data)

    @staticmethod
    def _from_row(row: Any) -> Checkpoint:
        data = json.loads(row["context_json"])
        context = ExecutionContext(
            task_id=data["task_id"],
            variables=data["variables"],
            node_outputs=data["node_outputs"],
            metadata=data["metadata"],
        )
        return Checkpoint(
            id=row["id"],
            task_id=row["task_id"],
            node_id=row["node_id"],
            context=context,
            created_at=datetime.fromisoformat(row["created_at"]),
        )
