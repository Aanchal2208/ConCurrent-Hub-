"""
PostgreSQL repository for tasks — identical logic to Phase 2 but imports
the Phase 3 schema (which includes the users table).

Also adds UserRepository methods via the UserRepository class in auth.py.
Team Nexus — OSDBMS-V-2026-T185
"""

from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from database.postgres_db import DatabaseUnavailableError, PostgresPoolManager
from scheduler.scheduler import UpdateResult, UpdateStatus

SCHEMA_FILE_PATH = Path(__file__).resolve().parent / "schema.sql"

CREATE_TASK_SQL = """
INSERT INTO tasks (title, description, status, priority, assignee, user_id, version)
VALUES (%(title)s, %(description)s, %(status)s, %(priority)s, %(assignee)s, %(user_id)s, 1)
RETURNING id, title, description, status, priority, assignee, user_id, version, created_at, updated_at;
"""

GET_TASK_BY_ID_SQL = """
SELECT id, title, description, status, priority, assignee, user_id, version, created_at, updated_at
FROM tasks WHERE id = %(task_id)s;
"""

CONDITIONAL_UPDATE_TASK_SQL = """
UPDATE tasks
SET title        = %(title)s,
    description  = %(description)s,
    status       = %(status)s,
    priority     = %(priority)s,
    assignee     = %(assignee)s,
    user_id      = %(user_id)s,
    version      = version + 1,
    updated_at   = CURRENT_TIMESTAMP
WHERE id = %(task_id)s
  AND version = %(expected_version)s
RETURNING id, title, description, status, priority, assignee, user_id, version, created_at, updated_at;
"""

CONDITIONAL_DELETE_TASK_SQL = """
DELETE FROM tasks WHERE id = %(task_id)s AND version = %(expected_version)s
RETURNING id, title, version;
"""

UNCONDITIONAL_DELETE_TASK_SQL = """
DELETE FROM tasks WHERE id = %(task_id)s RETURNING id, title, version;
"""

CHECK_CURRENT_VERSION_SQL = """
SELECT id, version, title, description, status, priority, assignee, user_id, created_at, updated_at
FROM tasks WHERE id = %(task_id)s;
"""

INSERT_AUDIT_LOG_SQL = """
INSERT INTO audit_logs (actor, task_id, operation, old_version, new_version, outcome, details)
VALUES (%(actor)s, %(task_id)s, %(operation)s, %(old_version)s, %(new_version)s, %(outcome)s, %(details)s)
RETURNING id, timestamp, actor, task_id, operation, old_version, new_version, outcome, details;
"""

LIST_AUDIT_LOGS_SQL = """
SELECT id, timestamp, actor, task_id, operation, old_version, new_version, outcome, details
FROM audit_logs ORDER BY id DESC LIMIT %(limit)s;
"""


def _serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert datetime fields to ISO strings."""
    out = dict(row)
    for key in ("created_at", "updated_at", "timestamp"):
        val = out.get(key)
        if isinstance(val, datetime):
            out[key] = val.isoformat()
    return out


class PostgresTaskRepository:
    """Full CRUD + optimistic concurrency control backed by PostgreSQL."""

    def __init__(self, pool_manager: PostgresPoolManager) -> None:
        self._pm = pool_manager

    def _execute_timed(
        self,
        cursor: Any,
        query: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> None:
        started = time.perf_counter()
        try:
            if params is None:
                cursor.execute(query)
            else:
                cursor.execute(query, params)
        finally:
            self._pm.record_timing(
                "query_execution_ms", (time.perf_counter() - started) * 1000
            )

    @property
    def pool_manager(self) -> PostgresPoolManager:
        return self._pm

    def init_schema(self, schema_sql: Optional[str] = None) -> None:
        sql_text = schema_sql or SCHEMA_FILE_PATH.read_text(encoding="utf-8")
        with self._pm.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_text)

    # -----------------------------------------------------------------------
    # CRUD
    # -----------------------------------------------------------------------

    def create_task(
        self,
        title: str,
        description: str = "",
        status: str = "TODO",
        priority: str = "MEDIUM",
        assignee: str = "unassigned",
        user_id: str = "system",
    ) -> Dict[str, Any]:
        title = title.strip()
        if not title:
            raise ValueError("Task title must not be empty.")
        status = (status or "TODO").upper().strip()
        if status not in ("TODO", "IN_PROGRESS", "REVIEW", "DONE"):
            status = "TODO"
        priority = (priority or "MEDIUM").upper().strip()
        if priority not in ("LOW", "MEDIUM", "HIGH", "CRITICAL"):
            priority = "MEDIUM"
        with self._pm.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_TASK_SQL, {
                    "title": title,
                    "description": (description or "").strip(),
                    "status": status,
                    "priority": priority,
                    "assignee": (assignee or "unassigned").strip(),
                    "user_id": user_id,
                })
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError("INSERT returned no row.")
                return _serialize_row(row)

    def get_task(self, task_id: int) -> Optional[Dict[str, Any]]:
        with self._pm.connection() as conn:
            with conn.cursor() as cur:
                self._execute_timed(cur, GET_TASK_BY_ID_SQL, {"task_id": task_id})
                row = cur.fetchone()
                return _serialize_row(row) if row else None

    def list_tasks(
        self,
        status: Optional[str] = None,
        priority: Optional[str] = None,
        assignee: Optional[str] = None,
        search: Optional[str] = None,
        sort_by: str = "id",
        sort_dir: str = "asc",
    ) -> List[Dict[str, Any]]:
        query = [
            "SELECT id, title, description, status, priority, assignee, user_id, version, "
            "created_at, updated_at FROM tasks WHERE 1=1"
        ]
        params: Dict[str, Any] = {}
        if status:
            query.append("AND status = %(status)s")
            params["status"] = status.upper()
        if priority:
            query.append("AND priority = %(priority)s")
            params["priority"] = priority.upper()
        if assignee:
            query.append("AND assignee = %(assignee)s")
            params["assignee"] = assignee
        if search:
            query.append("AND (title ILIKE %(search)s OR description ILIKE %(search)s)")
            params["search"] = f"%{search}%"
        valid_cols = {"id": "id", "version": "version", "title": "title",
                      "updated_at": "updated_at", "priority": "priority", "status": "status"}
        col = valid_cols.get(sort_by.lower(), "id")
        direction = "DESC" if sort_dir.lower() == "desc" else "ASC"
        query.append(f"ORDER BY {col} {direction};")
        sql = " ".join(query)
        with self._pm.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return [_serialize_row(r) for r in cur.fetchall()]

    def update_task(
        self,
        task_id: int,
        expected_version: int,
        title: Optional[str] = None,
        description: Optional[str] = None,
        status: Optional[str] = None,
        priority: Optional[str] = None,
        assignee: Optional[str] = None,
        user_id: str = "anonymous",
    ) -> UpdateResult:
        """
        Atomically update a task ONLY when version matches expected_version.
        Returns an UpdateResult with SUCCESS, VERSION_CONFLICT, NOT_FOUND, or ERROR.
        """
        if expected_version < 1:
            return UpdateResult(status=UpdateStatus.ERROR, task_id=task_id,
                                message="expected_version must be >= 1.")

        if title is not None:
            new_title = title.strip()
            if not new_title:
                return UpdateResult(status=UpdateStatus.ERROR, task_id=task_id,
                                    message="Title must not be blank.")
        else:
            new_title = None

        if status is not None:
            new_status = (status or "TODO").upper().strip()
            if new_status not in ("TODO", "IN_PROGRESS", "REVIEW", "DONE"):
                return UpdateResult(status=UpdateStatus.ERROR, task_id=task_id,
                                    message=f"Invalid task status: {status}")
        else:
            new_status = None

        if priority is not None:
            new_priority = (priority or "MEDIUM").upper().strip()
            if new_priority not in ("LOW", "MEDIUM", "HIGH", "CRITICAL"):
                return UpdateResult(status=UpdateStatus.ERROR, task_id=task_id,
                                    message=f"Invalid task priority: {priority}")
        else:
            new_priority = None

        new_description = None if description is None else description.strip()
        new_assignee = None if assignee is None else assignee.strip()

        update_sql = """
            UPDATE tasks
            SET title = COALESCE(%(title)s, title),
                description = COALESCE(%(description)s, description),
                status = COALESCE(%(status)s, status),
                priority = COALESCE(%(priority)s, priority),
                assignee = COALESCE(%(assignee)s, assignee),
                user_id = COALESCE(%(user_id)s, user_id),
                version = version + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %(task_id)s AND version = %(expected_version)s
            RETURNING id, title, description, status, priority, assignee, user_id, version, created_at, updated_at;
        """

        try:
            with self._pm.connection() as conn:
                with conn.cursor() as cur:
                    self._execute_timed(cur, update_sql, {
                        "title": new_title,
                        "description": new_description,
                        "status": new_status,
                        "priority": new_priority,
                        "assignee": new_assignee,
                        "user_id": user_id,
                        "task_id": task_id,
                        "expected_version": expected_version,
                    })
                    updated_row = cur.fetchone()
                    if updated_row is not None:
                        record = _serialize_row(updated_row)
                        return UpdateResult(
                            status=UpdateStatus.SUCCESS,
                            task_id=int(record["id"]),
                            new_version=int(record["version"]),
                            message="Task updated successfully",
                            task=record,
                        )

                    self._execute_timed(cur, CHECK_CURRENT_VERSION_SQL, {"task_id": task_id})
                    stale_row = cur.fetchone()
                    if stale_row is None:
                        return UpdateResult(status=UpdateStatus.NOT_FOUND, task_id=task_id,
                                            message=f"Task {task_id} not found")
                    current_ver = int(stale_row["version"])
                    return UpdateResult(
                        status=UpdateStatus.VERSION_CONFLICT,
                        task_id=task_id,
                        current_version=current_ver,
                        message=(f"Conflict: expected version {expected_version}, "
                                 f"but task is now at version {current_ver}."),
                        task=_serialize_row(stale_row),
                    )
        except DatabaseUnavailableError as exc:
            return UpdateResult(status=UpdateStatus.ERROR, task_id=task_id,
                                message=f"Database unavailable: {exc}")
        except Exception as exc:
            return UpdateResult(status=UpdateStatus.ERROR, task_id=task_id,
                                message=f"Database error: {exc}")

    def delete_task(
        self,
        task_id: int,
        expected_version: Optional[int] = None,
        user_id: str = "anonymous",
    ) -> Dict[str, Any]:
        try:
            with self._pm.connection() as conn:
                with conn.cursor() as cur:
                    if expected_version is not None:
                        cur.execute(CONDITIONAL_DELETE_TASK_SQL,
                                    {"task_id": task_id, "expected_version": expected_version})
                    else:
                        cur.execute(UNCONDITIONAL_DELETE_TASK_SQL, {"task_id": task_id})
                    row = cur.fetchone()
                    if row is not None:
                        return {"status": "SUCCESS", "task_id": task_id,
                                "message": "Task deleted successfully"}

                    cur.execute(CHECK_CURRENT_VERSION_SQL, {"task_id": task_id})
                    current_row = cur.fetchone()
                    if current_row is None:
                        return {"status": "NOT_FOUND", "task_id": task_id,
                                "message": f"Task {task_id} not found"}
                    current_version = int(current_row["version"])
                    return {
                        "status": "VERSION_CONFLICT",
                        "task_id": task_id,
                        "current_version": current_version,
                        "message": (f"Cannot delete task {task_id}: expected version "
                                    f"{expected_version}, current is {current_version}."),
                    }
        except DatabaseUnavailableError:
            raise
        except Exception as exc:
            return {"status": "ERROR", "task_id": task_id,
                    "message": f"Database error: {exc}"}

    # -----------------------------------------------------------------------
    # Audit log
    # -----------------------------------------------------------------------

    def record_audit_log(
        self,
        actor: str,
        task_id: Optional[int],
        operation: str,
        old_version: Optional[int],
        new_version: Optional[int],
        outcome: str,
        details: str = "",
    ) -> Optional[Dict[str, Any]]:
        try:
            with self._pm.connection() as conn:
                with conn.cursor() as cur:
                    self._execute_timed(cur, INSERT_AUDIT_LOG_SQL, {
                        "actor": actor, "task_id": task_id, "operation": operation,
                        "old_version": old_version, "new_version": new_version,
                        "outcome": outcome, "details": details,
                    })
                    row = cur.fetchone()
                    return _serialize_row(row) if row else None
        except Exception:
            return None

    def list_audit_logs(self, limit: int = 100) -> List[Dict[str, Any]]:
        try:
            with self._pm.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(LIST_AUDIT_LOGS_SQL, {"limit": limit})
                    return [_serialize_row(r) for r in cur.fetchall()]
        except Exception:
            return []
