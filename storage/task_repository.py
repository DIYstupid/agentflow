import json
from datetime import datetime
from typing import Any

from runtime.task import AgentTask, TaskStatus
from storage.database import Database


class TaskRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def create(self, task: AgentTask) -> None:
        await self.database.execute(
            """
            INSERT INTO tasks (
                task_id, graph_id, status, input_json, output_json,
                current_node, created_at, started_at, completed_at, error,
                timeout_seconds
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            self._values(task),
        )

    async def update(self, task: AgentTask) -> None:
        await self.database.execute(
            """
            UPDATE tasks SET
                graph_id = ?, status = ?, input_json = ?, output_json = ?,
                current_node = ?, created_at = ?, started_at = ?,
                completed_at = ?, error = ?, timeout_seconds = ?
            WHERE task_id = ?
            """,
            self._values(task)[1:] + (task.task_id,),
        )

    async def delete(self, task_id: str) -> None:
        await self.database.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))

    async def get(self, task_id: str) -> AgentTask | None:
        row = await self.database.fetch_one(
            "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
        )
        return self._from_row(row) if row is not None else None

    async def list_by_status(self, *statuses: TaskStatus) -> list[AgentTask]:
        if not statuses:
            return []
        placeholders = ", ".join("?" for _ in statuses)
        rows = await self.database.fetch_all(
            f"SELECT * FROM tasks WHERE status IN ({placeholders}) "
            "ORDER BY created_at, task_id",
            tuple(status.value for status in statuses),
        )
        return [self._from_row(row) for row in rows]

    @staticmethod
    def _values(task: AgentTask) -> tuple[Any, ...]:
        return (
            task.task_id,
            task.graph_id,
            task.status.value,
            json.dumps(task.input, ensure_ascii=False),
            json.dumps(task.output, ensure_ascii=False)
            if task.output is not None
            else None,
            task.current_node,
            task.created_at.isoformat(),
            task.started_at.isoformat() if task.started_at else None,
            task.completed_at.isoformat() if task.completed_at else None,
            task.error,
            task.timeout_seconds,
        )

    @staticmethod
    def _from_row(row: Any) -> AgentTask:
        return AgentTask(
            task_id=row["task_id"],
            graph_id=row["graph_id"],
            status=TaskStatus(row["status"]),
            input=json.loads(row["input_json"]),
            output=json.loads(row["output_json"])
            if row["output_json"] is not None
            else None,
            current_node=row["current_node"],
            created_at=datetime.fromisoformat(row["created_at"]),
            started_at=datetime.fromisoformat(row["started_at"])
            if row["started_at"]
            else None,
            completed_at=datetime.fromisoformat(row["completed_at"])
            if row["completed_at"]
            else None,
            error=row["error"],
            timeout_seconds=row["timeout_seconds"],
        )
