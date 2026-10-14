"""
database/repository.py
Owner: Parth Gauniyal (Member 2)

PostgreSQL repository layer for ConcurrentHub tasks.

Key DBMS guarantees:
1. All SQL statements use parameterized queries (%(param)s) — never raw string formatting.
2. Version-checked updates use a single atomic UPDATE ... WHERE id = %(task_id)s AND version = %(expected_version)s RETURNING ... statement.
3. The atomic conditional UPDATE is the true correctness guarantee against lost updates.
   When 0 rows are returned, a follow-up SELECT is executed solely to distinguish
   NOT_FOUND (missing record) from VERSION_CONFLICT (stale expected version)
   for accurate error reporting.
4. Pooled connections and transactions are managed via context managers so locks and
   connections are always released even if an exception occurs.
"""

from _future_ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from database.postgres_db import DatabaseUnavailableError, PostgresPoolManager
from scheduler.scheduler import UpdateRequest, UpdateResult, UpdateStatus

SCHEMA_FILE_PATH = Path(_file_).resolve().parent / "schema.sql"

CREATE_TASK_SQL = """
INSERT INTO tasks (title, version)
VALUES (%(title)s, 1)
RETURNING id, title, version, created_at, updated_at;
"""

GET_TASK_BY_ID_SQL = """
SELECT id, title, version, created_at, updated_at
FROM tasks
WHERE id = %(task_id)s;
"""

LIST_TASKS_SQL = """
SELECT id, title, version, created_at, updated_at
FROM tasks
ORDER BY id ASC;
"""

CONDITIONAL_UPDATE_TASK_SQL = """
UPDATE tasks
SET title = %(title)s,
    version = version + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE id = %(task_id)s
  AND version = %(expected_version)s
RETURNING id, title, version, created_at, updated_at;
"""

CHECK_CURRENT_VERSION_SQL = """
SELECT id, version
FROM tasks
WHERE id = %(task_id)s;
"""
def _serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a database row dictionary so timestamps are ISO-8601 strings."""
    serialized = dict(row)
    for key in ("created_at", "updated_at"):
        val = serialized.get(key)
        if isinstance(val, datetime):
            serialized[key] = val.isoformat()
    return serialized


class PostgresTaskRepository:
    """
    Repository providing CRUD and optimistic version-checked updates backed by
    a PostgresPoolManager.
    """

    def _init_(self, pool_manager: PostgresPoolManager) -> None:
        self._pool_manager = pool_manager

    @property
    def pool_manager(self) -> PostgresPoolManager:
        return self._pool_manager

    def init_schema(self, schema_sql: Optional[str] = None) -> None:
        """Create the tasks table and index using database/schema.sql."""
        sql_text = schema_sql or SCHEMA_FILE_PATH.read_text(encoding="utf-8")
        with self._pool_manager.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_text)

    def create_task(self, title: str, user_id: str = "anonymous") -> Dict[str, Any]:
        """Insert a new task at version 1 and return the created record."""
        clean_title = title.strip()
        if not clean_title:
            raise ValueError("Task title must not be empty.")

        with self._pool_manager.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_TASK_SQL, {"title": clean_title})
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError("INSERT INTO tasks did not return a created row.")
                record = _serialize_row(row)
                record["user_id"] = user_id
                return record