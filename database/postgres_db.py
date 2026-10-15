"""
database/postgres_db.py
Owner: Parth Gauniyal (Member 2)

PostgreSQL connection pool manager built on Psycopg 3 (psycopg and psycopg_pool).

Important architectural distinction:
- This module manages a pool of persistent PostgreSQL network connections
  (psycopg_pool.ConnectionPool) to avoid TCP/authentication handshake overhead
  on every query.
- It is completely separate from the application-level BoundedObjectPool in
  resource_pool/object_pool.py, which reuses in-memory Python buffers.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolTimeout


class DatabaseUnavailableError(Exception):
    """Raised when the PostgreSQL server cannot be reached or the pool fails to initialize."""


def build_dsn_from_env() -> str:
    """Construct a PostgreSQL connection string from environment variables."""
    explicit_url = os.getenv("DATABASE_URL")
    if explicit_url:
        # Normalize SQLAlchemy-style URLs if present in environment
        return explicit_url.replace("postgresql+asyncpg://", "postgresql://").replace(
            "postgresql+psycopg://", "postgresql://"
        )

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    dbname = os.getenv("POSTGRES_DB", "concurrenthub")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
class PostgresPoolManager:
    """
    Wraps psycopg_pool.ConnectionPool with explicit lifecycle management,
    safe connection checkout/return semantics, and clear error handling when
    PostgreSQL is unavailable.
    """

    def __init__(
        self,
        conninfo: Optional[str] = None,
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
        timeout: Optional[float] = None,
        open_immediately: bool = False,
    ) -> None:
        self.conninfo = conninfo or build_dsn_from_env()
        self.min_size = min_size if min_size is not None else int(os.getenv("DB_POOL_MIN_SIZE", "2"))
        self.max_size = max_size if max_size is not None else int(os.getenv("DB_POOL_MAX_SIZE", "10"))
        self.timeout = timeout if timeout is not None else float(os.getenv("DB_POOL_TIMEOUT", "5.0"))

        if self.min_size < 1 or self.max_size < self.min_size:
            raise ValueError(
                f"Invalid pool bounds: min_size={self.min_size}, max_size={self.max_size}"
            )

        self._pool: Optional[ConnectionPool] = None
        self._closed = False

        if open_immediately:
            self.open()

    def open(self) -> None:
        """Initialize and open the underlying Psycopg 3 connection pool."""
        if self._closed:
            raise DatabaseUnavailableError("Cannot reopen a closed PostgresPoolManager.")
        if self._pool is not None:
            return

        try:
            pool = ConnectionPool(
                conninfo=self.conninfo,
                min_size=self.min_size,
                max_size=self.max_size,
                timeout=self.timeout,
                kwargs={"row_factory": dict_row, "connect_timeout": 3},
                open=False,
            )
            pool.open(wait=True, timeout=self.timeout)
            self._pool = pool
        except Exception as exc:
            if self._pool is not None:
                try:
                    self._pool.close()
                except Exception:
                    pass
                self._pool = None
            raise DatabaseUnavailableError(
                f"Unable to initialize PostgreSQL connection pool ({self.conninfo}): {exc}"
            ) from exc

    def check_connectivity(self) -> bool:
        """
        Verify whether PostgreSQL is reachable and can execute a simple SELECT 1.
        Returns False instead of crashing when PostgreSQL is offline.
        """
        try:
            if self._pool is not None and not self._closed:
                with self.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1 AS ok;")
                        row = cur.fetchone()
                        return bool(row and row.get("ok") == 1)

            with psycopg.connect(self.conninfo, connect_timeout=2, row_factory=dict_row) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 AS ok;")
                    row = cur.fetchone()
                    return bool(row and row.get("ok") == 1)
        except Exception:
            return False
    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection[Any]]:
        """
        Borrow a pooled PostgreSQL connection within a transaction context.

         Psycopg 3's pool.connection() context manager automatically:
        - Checks out an idle connection from the pool.
        - Commits the transaction on normal exit.
        - Rolls back the transaction if an exception is raised inside the block.
        - Always returns the connection to the pool in finally.
        """
        if self._closed:
            raise DatabaseUnavailableError("PostgreSQL connection pool is closed.")
        if self._pool is None:
            self.open()
        assert self._pool is not None

        try:
            with self._pool.connection(timeout=self.timeout) as conn:
                yield conn
        except PoolTimeout as exc:
            raise DatabaseUnavailableError(
                f"Timed out waiting for a PostgreSQL connection from pool (max_size={self.max_size})."
            ) from exc
        except psycopg.Error as exc:
            raise DatabaseUnavailableError(f"PostgreSQL operational error: {exc}") from exc

    def get_pool_stats(self) -> Dict[str, Any]:
        """Return connection pool configuration and live Psycopg pool metrics."""
        raw_stats: Dict[str, Any] = {}
        if self._pool is not None and not self._closed:
            try:
                raw_stats = dict(self._pool.get_stats())
            except Exception:
                raw_stats = {}

        return {
            "min_size": self.min_size,
            "max_size": self.max_size,
            "timeout_seconds": self.timeout,
            "is_open": self._pool is not None and not self._closed,
            "closed": self._closed,
            "driver": "psycopg3 + psycopg_pool",
            "pool_metrics": raw_stats,
        }

    def close(self) -> None:
        """Gracefully close all pooled PostgreSQL connections."""
        self._closed = True
        if self._pool is not None:
            try:
                self._pool.close()
            finally:
                self._pool = None