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
