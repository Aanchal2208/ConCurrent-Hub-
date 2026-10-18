from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from database.postgres_db import DatabaseUnavailableError, PostgresPoolManager
from scheduler.scheduler import UpdateRequest, UpdateResult, UpdateStatus

SCHEMA_FILE_PATH = Path(__file__).resolve().parent / "schema.sql"

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

    def __init__(self, pool_manager: PostgresPoolManager) -> None:
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

    def get_task(self, task_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve a task by primary key id, or None if it does not exist."""
        with self._pool_manager.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(GET_TASK_BY_ID_SQL, {"task_id": task_id})
                row = cur.fetchone()
                return _serialize_row(row) if row is not None else None

    def list_tasks(self) -> List[Dict[str, Any]]:
        """Return all tasks ordered by ascending id."""
        with self._pool_manager.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(LIST_TASKS_SQL)
                rows = cur.fetchall()
                return [_serialize_row(r) for r in rows]

    def update_task(
        self,
        task_id: int,
        title: str,
        expected_version: int,
        user_id: str = "anonymous",
    ) -> UpdateResult:
        """
        Atomically update a task's title and increment its version ONLY if its
        current version in PostgreSQL matches expected_version.

        Correctness note:
        The UPDATE ... WHERE id = %(task_id)s AND version = %(expected_version)s
        statement is executed atomically by PostgreSQL. Even if two connections
        bypass the application scheduler and execute this statement concurrently,
        PostgreSQL's row-level locking and predicate re-evaluation under READ COMMITTED
        guarantee that at most one UPDATE matches expected_version.

        If cur.fetchone() returns None, no modification occurred. We then run a
        diagnostic SELECT to report whether the task_id does not exist (NOT_FOUND)
        or exists with a newer version (VERSION_CONFLICT).
        """
        clean_title = title.strip()
        if not clean_title:
            return UpdateResult(
                status=UpdateStatus.ERROR,
                task_id=task_id,
                message="Task title must not be empty.",
            )

        if expected_version < 1:
            return UpdateResult(
                status=UpdateStatus.ERROR,
                task_id=task_id,
                message="expected_version must be >= 1.",
            )

        try:
            with self._pool_manager.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        CONDITIONAL_UPDATE_TASK_SQL,
                        {
                            "title": clean_title,
                            "task_id": task_id,
                            "expected_version": expected_version,
                        },
                    )
                    updated_row = cur.fetchone()

                    if updated_row is not None:
                        record = _serialize_row(updated_row)
                        record["user_id"] = user_id
                        return UpdateResult(
                            status=UpdateStatus.SUCCESS,
                            task_id= int(record["id"]),
                            new_version=int(record["version"]),
                            message="Task updated successfully",
                            task=record,
                        )
                     # The atomic conditional UPDATE matched 0 rows (no data was changed).
                    # Perform a follow-up read to distinguish missing record vs stale version.
                    cur.execute(CHECK_CURRENT_VERSION_SQL, {"task_id": task_id})
                    existing_row = cur.fetchone()

                    if existing_row is None:
                        return UpdateResult(
                            status=UpdateStatus.NOT_FOUND,
                            task_id=task_id,
                            message=f"Task {task_id} not found",
                        )

                    return UpdateResult(
                        status=UpdateStatus.VERSION_CONFLICT,
                        task_id=task_id,
                        current_version=int(existing_row["version"]),
                        message="The task was modified by another request",
                    )
        except DatabaseUnavailableError as exc:
            return UpdateResult(
                status=UpdateStatus.ERROR,
                task_id=task_id,
                message=f"Database unavailable: {exc}",
            )
        except Exception as exc:
            return UpdateResult(
                status=UpdateStatus.ERROR,
                task_id=task_id,
                message=f"Database error during update: {exc}",
            )

    def handle_scheduled_update(self, request: UpdateRequest) -> UpdateResult:
        """
        Callback adapter allowing PerRecordScheduler to invoke this PostgreSQL
        repository directly.
        """
        return self.update_task(
            task_id=request.task_id,
            title=request.title,
            expected_version=request.expected_version,
            user_id=request.user_id,
        )
