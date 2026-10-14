"""
database/test_database.py
Owner: Parth Gauniyal (Member 2)

Automated tests for the PostgreSQL repository and connection pool.

Important:
- Live PostgreSQL integration tests explicitly check whether PostgreSQL is reachable.
- When PostgreSQL is unavailable (e.g., Docker is not running), those tests are
  reported as `SKIPPED` (never falsely passed).
- Unit tests at the top of this file verify parameterized SQL generation,
  conditional update conflict vs missing-record handling, and connection-release
  guarantees without requiring a live PostgreSQL server.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional
import pytest

from database.postgres_db import DatabaseUnavailableError, PostgresPoolManager
from database.repository import (
    CHECK_CURRENT_VERSION_SQL,
    CONDITIONAL_UPDATE_TASK_SQL,
    PostgresTaskRepository,
)
from scheduler.scheduler import UpdateStatus


# ---------------------------------------------------------------------------
# 1. Offline Repository Unit Tests (verify SQL parameterization & release guarantees)
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, state: Dict[str, Any]) -> None:
        self._state = state
        self._next_row: Optional[Dict[str, Any]] = None

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
        self._state["executed_queries"].append((sql.strip(), params))
        if self._state.get("raise_on_execute"):
            raise RuntimeError("Simulated query execution error")

        if "UPDATE tasks" in sql:
            self._next_row = self._state.get("update_return_row")
        elif "SELECT id, version" in sql:
            self._next_row = self._state.get("select_version_row")
        elif "INSERT INTO tasks" in sql:
            assert params is not None
            self._next_row = {
                "id": 1,
                "title": params["title"],
                "version": 1,
                "created_at": "2026-10-10T10:00:00+00:00",
                "updated_at": "2026-10-10T10:00:00+00:00",
            }

    def fetchone(self) -> Optional[Dict[str, Any]]:
        return self._next_row


class _FakeConnection:
    def __init__(self, state: Dict[str, Any]) -> None:
        self._state = state

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._state)


class _FakePoolManager:
    def __init__(self, state: Dict[str, Any]) -> None:
        self.state = state
        self.checked_out = 0
        self.released = 0

    @contextmanager
    def connection(self) -> Iterator[_FakeConnection]:
        self.checked_out += 1
        try:
            yield _FakeConnection(self.state)
        finally:
            self.released += 1


def test_repository_uses_parameterized_sql_and_Distinguishes_conflict_vs_not_found() -> None:
    """
    Verify that `PostgresTaskRepository.update_task` uses parameterized SQL
    and distinguishes `VERSION_CONFLICT` from `NOT_FOUND` when the conditional
    UPDATE returns no rows.
    """
    state: Dict[str, Any] = {
        "executed_queries": [],
        "update_return_row": None,
        "select_version_row": {"id": 42, "version": 5},
    }
    fake_pool = _FakePoolManager(state)
    repo = PostgresTaskRepository(fake_pool)  # type: ignore[arg-type]

    conflict_res = repo.update_task(task_id=42, title="Attempted edit", expected_version=3)
    assert conflict_res.status == UpdateStatus.VERSION_CONFLICT
    assert conflict_res.current_version == 5
    assert fake_pool.checked_out == 1
    assert fake_pool.released == 1

    # Verify parameterized placeholders were passed separately from SQL text
    first_sql, first_params = state["executed_queries"][0]
    assert first_sql == CONDITIONAL_UPDATE_TASK_SQL.strip()
    assert first_params == {"title": "Attempted edit", "task_id": 42, "expected_version": 3}

    second_sql, second_params = state["executed_queries"][1]
    assert second_sql == CHECK_CURRENT_VERSION_SQL.strip()
    assert second_params == {"task_id": 42}

    # Now test missing record (SELECT returns None)
    state["select_version_row"] = None
    missing_res = repo.update_task(task_id=999, title="Missing task", expected_version=1)
    assert missing_res.status == UpdateStatus.NOT_FOUND
    assert fake_pool.checked_out == 2
    assert fake_pool.released == 2


def test_repository_releases_connection_when_exception_occurs() -> None:
    """Pooled connection context must always release the connection even on exception."""
    state: Dict[str, Any] = {
        "executed_queries": [],
        "raise_on_execute": True,
    }
    fake_pool = _FakePoolManager(state)
    repo = PostgresTaskRepository(fake_pool)  # type: ignore[arg-type]

    err_res = repo.update_task(task_id=1, title="Trigger exception", expected_version=1)
    assert err_res.status == UpdateStatus.ERROR
    assert fake_pool.checked_out == 1
    assert fake_pool.released == 1


# ---------------------------------------------------------------------------
# 2. Live PostgreSQL Integration Tests (skipped clearly when DB is offline)
# ---------------------------------------------------------------------------


@pytest.fixture()
def live_postgres_repo() -> Iterator[PostgresTaskRepository]:
    """
    Fixture that connects to a live PostgreSQL instance, initializes the schema,
    truncates the `tasks` table for a clean test run, and closes the pool afterward.
    """
    probe = PostgresPoolManager(min_size=2, max_size=6, open_immediately=False)
    if not probe.check_connectivity():
        probe.close()
        pytest.skip(
            "PostgreSQL is unavailable at DATABASE_URL. Skipping live PostgreSQL "
            "integration test. Start PostgreSQL with `docker compose up -d postgres` "
            "and run `pytest database/test_database.py -v`."
        )

    probe.open()
    repo = PostgresTaskRepository(probe)
    repo.init_schema()
    with probe.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE tasks RESTART IDENTITY;")

    try:
        yield repo
    finally:
        probe.close()


def test_postgres_schema_creation_and_create_read_task(
    live_postgres_repo: PostgresTaskRepository,
) -> None:
    """Verify schema creation, task insertion, and task retrieval in PostgreSQL."""
    created = live_postgres_repo.create_task("Test PostgreSQL task", user_id="parth")
    assert created["id"] == 1
    assert created["title"] == "Test PostgreSQL task"
    assert created["version"] == 1

    fetched = live_postgres_repo.get_task(created["id"])
    assert fetched is not None
    assert fetched["id"] == 1
    assert fetched["title"] == "Test PostgreSQL task"
    assert fetched["version"] == 1


def test_postgres_successful_version_checked_update(
    live_postgres_repo: PostgresTaskRepository,
) -> None:
    """Verify atomic conditional update increments version from 1 to 2."""
    created = live_postgres_repo.create_task("Initial DB Title", user_id="parth")
    result = live_postgres_repo.update_task(
        task_id=created["id"],
        title="Updated DB Title",
        expected_version=1,
        user_id="parth",
    )
    assert result.status == UpdateStatus.SUCCESS
    assert result.new_version == 2

    fetched = live_postgres_repo.get_task(created["id"])
    assert fetched is not None
    assert fetched["title"] == "Updated DB Title"
    assert fetched["version"] == 2


def test_postgres_reject_stale_version(
    live_postgres_repo: PostgresTaskRepository,
) -> None:
    """Verify stale expected_version is rejected with VERSION_CONFLICT and does not modify row."""
    created = live_postgres_repo.create_task("Version 1", user_id="u1")
    live_postgres_repo.update_task(
        task_id=created["id"],
        title="Version 2",
        expected_version=1,
        user_id="u1",
    )

    stale = live_postgres_repo.update_task(
        task_id=created["id"],
        title="Stale overwrite attempt",
        expected_version=1,
        user_id="u2",
    )
    assert stale.status == UpdateStatus.VERSION_CONFLICT
    assert stale.current_version == 2

    fetched = live_postgres_repo.get_task(created["id"])
    assert fetched is not None
    assert fetched["title"] == "Version 2"
    assert fetched["version"] == 2


def test_postgres_two_concurrent_updates_same_expected_version(
    live_postgres_repo: PostgresTaskRepository,
) -> None:
    """
    When two threads concurrently execute `update_task` against PostgreSQL with the
    same expected_version=1, at most one succeeds and the other gets VERSION_CONFLICT.
    """
    created = live_postgres_repo.create_task("Concurrent target", user_id="seed")
    barrier = threading.Barrier(2, timeout=5.0)
    outcomes: List[str] = []
    lock = threading.Lock()

    def contender(idx: int) -> None:
        barrier.wait()
        res = live_postgres_repo.update_task(
            task_id=created["id"],
            title=f"Writer {idx}",
            expected_version=1,
            user_id=f"user_{idx}",
        )
        with lock:
            outcomes.append(res.status)

    t1 = threading.Thread(target=contender, args=(1,))
    t2 = threading.Thread(target=contender, args=(2,))
    t1.start()
    t2.start()
    t1.join(timeout=5.0)
    t2.join(timeout=5.0)

    assert sorted(outcomes) == sorted([UpdateStatus.SUCCESS, UpdateStatus.VERSION_CONFLICT])
    final_row = live_postgres_repo.get_task(created["id"])
    assert final_row is not None
    assert final_row["version"] == 2


def test_postgres_connection_pool_cleanup() -> None:
    """Verify connection pool closes cleanly and rejects further checkout attempts."""
    pool_mgr = PostgresPoolManager(min_size=1, max_size=2, open_immediately=False)
    pool_mgr.close()
    assert pool_mgr.get_pool_stats()["closed"] is True

    with pytest.raises(DatabaseUnavailableError):
        with pool_mgr.connection():
            pass