import asyncio
from pathlib import Path
from typing import Any

import aiosqlite


SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    graph_id TEXT NOT NULL,
    status TEXT NOT NULL,
    input_json TEXT NOT NULL,
    output_json TEXT,
    current_node TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    error TEXT,
    timeout_seconds REAL
);

CREATE TABLE IF NOT EXISTS checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    context_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_checkpoints_task_created
    ON checkpoints(task_id, created_at);

CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    node_id TEXT,
    data_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_events_task_created
    ON events(task_id, created_at);
"""


class Database:
    """共享的异步 SQLite 连接和最小查询接口。SQL 只由 Repository 使用。"""

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        self._connection: aiosqlite.Connection | None = None
        self._operation_lock = asyncio.Lock()

    async def initialize(self) -> None:
        if self._connection is not None:
            return
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._connection = await aiosqlite.connect(self.path)
        self._connection.row_factory = aiosqlite.Row
        await self._connection.execute("PRAGMA foreign_keys = ON")
        if self.path != ":memory:":
            await self._connection.execute("PRAGMA journal_mode = WAL")
        await self._connection.executescript(SCHEMA)
        await self._connection.commit()

    async def close(self) -> None:
        if self._connection is None:
            return
        await self._connection.close()
        self._connection = None

    async def execute(self, sql: str, parameters: tuple[Any, ...] = ()) -> int:
        connection = self._require_connection()
        cursor: aiosqlite.Cursor | None = None
        async with self._operation_lock:
            try:
                cursor = await connection.execute(sql, parameters)
                await connection.commit()
                lastrowid = cursor.lastrowid
            except BaseException:
                await connection.rollback()
                raise
            finally:
                if cursor is not None:
                    await cursor.close()
        return int(lastrowid or 0)

    async def fetch_one(
        self, sql: str, parameters: tuple[Any, ...] = ()
    ) -> aiosqlite.Row | None:
        connection = self._require_connection()
        async with self._operation_lock:
            cursor = await connection.execute(sql, parameters)
            row = await cursor.fetchone()
            await cursor.close()
        return row

    async def fetch_all(
        self, sql: str, parameters: tuple[Any, ...] = ()
    ) -> list[aiosqlite.Row]:
        connection = self._require_connection()
        async with self._operation_lock:
            cursor = await connection.execute(sql, parameters)
            rows = await cursor.fetchall()
            await cursor.close()
        return rows

    def _require_connection(self) -> aiosqlite.Connection:
        if self._connection is None:
            raise RuntimeError("database is not initialized")
        return self._connection

    async def __aenter__(self) -> "Database":
        await self.initialize()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()
