"""
PostgreSQL connection pool manager built on Psycopg 3 (`psycopg` and `psycopg_pool`).
OS & DBMS Phase 2 (Team Nexus - OSDBMS-V-2026-T185)
"""

from __future__ import annotations

import os
import threading
import time
from collections import deque
from contextlib import contextmanager
from urllib.parse import urlsplit, urlunsplit
from typing import Any, Dict, Iterator, Optional, cast

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool, PoolTimeout
    PsycopgError = psycopg.Error
    PSYCOPG_AVAILABLE = True
except ImportError:
    psycopg = None  # type: ignore
    dict_row = None  # type: ignore
    ConnectionPool = None  # type: ignore
    PoolTimeout = Exception  # type: ignore
    PsycopgError = Exception
    PSYCOPG_AVAILABLE = False


class DatabaseUnavailableError(Exception):
    """Raised when the PostgreSQL server cannot be reached or the pool fails to initialize."""


def _redact_password(conninfo: str) -> str:
    """Hide password information while preserving the connection target."""
    try:
        parsed = urlsplit(conninfo)
        if not parsed.hostname:
            return conninfo
        username = parsed.username or "postgres"
        password = parsed.password
        if password is None:
            return conninfo
        netloc = f"{username}:***@{parsed.hostname}:{parsed.port or 5432}"
        if parsed.hostname and ":" in parsed.hostname:
            netloc = f"{username}:***@[{parsed.hostname}]:{parsed.port or 5432}"
        safe = parsed._replace(netloc=netloc)
        return urlunsplit(safe)
    except Exception:
        return "postgresql://***:***@***"


def build_dsn_from_env() -> str:
    """Construct a PostgreSQL connection string from environment variables."""
    explicit_url = os.getenv("DATABASE_URL")
    if explicit_url:
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
    Wraps `psycopg_pool.ConnectionPool` with explicit lifecycle management,
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

        self._pool: Optional[Any] = None
        self._closed = False
        self._timing_lock = threading.Lock()
        self._timing_samples: Dict[str, deque[float]] = {}
        self._timing_totals: Dict[str, tuple[int, float, float]] = {}

        if open_immediately:
            self.open()

    def open(self) -> None:
        """Initialize and open the underlying Psycopg 3 connection pool."""
        if not PSYCOPG_AVAILABLE or ConnectionPool is None:
            raise DatabaseUnavailableError("The psycopg package is not installed.")
        if self._closed:
            raise DatabaseUnavailableError("Cannot reopen a closed PostgresPoolManager.")
        if self._pool is not None:
            return

        pool = None
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
            if pool is not None:
                try:
                    pool.close()
                except Exception:
                    pass
            self._pool = None
            raise DatabaseUnavailableError(
                f"Unable to initialize PostgreSQL connection pool ({_redact_password(self.conninfo)}): {exc}"
            ) from exc

    def check_connectivity(self) -> bool:
        """
        Verify whether PostgreSQL is reachable and can execute a simple `SELECT 1`.
        Returns False instead of crashing when PostgreSQL is offline.
        """
        if not PSYCOPG_AVAILABLE or psycopg is None:
            return False
        try:
            if self._pool is not None and not self._closed:
                with self.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1 AS ok;")
                        row = cur.fetchone()
                        return bool(row and cast(Dict[str, Any], row).get("ok") == 1)

            with psycopg.connect(
                self.conninfo,
                connect_timeout=2,
                row_factory=cast(Any, dict_row),
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 AS ok;")
                    row = cur.fetchone()
                    return bool(row and cast(Dict[str, Any], row).get("ok") == 1)
        except Exception:
            return False

    @contextmanager
    def connection(self) -> Iterator[Any]:
        """
        Borrow a pooled PostgreSQL connection within a transaction context.
        Rolls back on exception and returns the connection to the pool in finally.
        """
        if self._closed:
            raise DatabaseUnavailableError("PostgreSQL connection pool is closed.")
        if self._pool is None:
            self.open()
        assert self._pool is not None

        connection_context = self._pool.connection(timeout=self.timeout)
        acquisition_started = time.perf_counter()
        try:
            conn = connection_context.__enter__()
        except PoolTimeout as exc:
            self.record_timing(
                "connection_acquisition_ms",
                (time.perf_counter() - acquisition_started) * 1000,
            )
            raise DatabaseUnavailableError(
                f"Timed out waiting for a PostgreSQL connection from pool (max_size={self.max_size})."
            ) from exc
        except PsycopgError as exc:
            self.record_timing(
                "connection_acquisition_ms",
                (time.perf_counter() - acquisition_started) * 1000,
            )
            raise DatabaseUnavailableError(f"PostgreSQL operational error: {exc}") from exc

        self.record_timing(
            "connection_acquisition_ms",
            (time.perf_counter() - acquisition_started) * 1000,
        )
        transaction_started = time.perf_counter()
        try:
            yield conn
        except BaseException as exc:
            try:
                connection_context.__exit__(type(exc), exc, exc.__traceback__)
            finally:
                self.record_timing(
                    "transaction_ms",
                    (time.perf_counter() - transaction_started) * 1000,
                )
            raise
        else:
            try:
                connection_context.__exit__(None, None, None)
            except PsycopgError as exc:
                raise DatabaseUnavailableError(f"PostgreSQL transaction failed: {exc}") from exc
            finally:
                self.record_timing(
                    "transaction_ms",
                    (time.perf_counter() - transaction_started) * 1000,
                )

    def record_timing(self, name: str, duration_ms: float) -> None:
        """Record one bounded timing sample and its lifetime aggregates."""
        with self._timing_lock:
            samples = self._timing_samples.setdefault(name, deque(maxlen=4096))
            samples.append(duration_ms)
            count, total, maximum = self._timing_totals.get(name, (0, 0.0, 0.0))
            self._timing_totals[name] = (
                count + 1,
                total + duration_ms,
                max(maximum, duration_ms),
            )

    def _timing_snapshot(self) -> Dict[str, Dict[str, Any]]:
        with self._timing_lock:
            snapshot: Dict[str, Dict[str, Any]] = {}
            for name, (count, total, maximum) in self._timing_totals.items():
                samples = sorted(self._timing_samples[name])
                snapshot[name] = {
                    "count": count,
                    "total_ms": total,
                    "average_ms": total / count if count else 0.0,
                    "max_ms": maximum,
                    "p95_ms": samples[min(int((len(samples) - 1) * 0.95), len(samples) - 1)]
                    if samples
                    else None,
                }
            return snapshot

    def get_pool_stats(self) -> Dict[str, Any]:
        """Return connection pool configuration and live Psycopg pool metrics."""
        raw_stats: Dict[str, Any] = {}
        if self._pool is not None and not self._closed:
            try:
                raw_stats = dict(self._pool.get_stats())
            except Exception:
                raw_stats = {}

        total_connections = int(raw_stats.get("pool_size", 0))
        available_connections = int(raw_stats.get("pool_available", 0))
        in_use_connections = max(0, total_connections - available_connections)

        return {
            "min_size": self.min_size,
            "max_size": self.max_size,
            "timeout_seconds": self.timeout,
            "is_open": self._pool is not None and not self._closed,
            "closed": self._closed,
            "driver": "psycopg3 + psycopg_pool",
            "total_connections": total_connections,
            "available_connections": available_connections,
            "in_use_connections": in_use_connections,
            "pool_wait_time_ms": int(raw_stats.get("requests_wait_ms", 0)),
            "pool_timeout_count": int(raw_stats.get("requests_errors", 0)),
            "pool_metrics": raw_stats,
            "timings": self._timing_snapshot(),
        }

    def close(self) -> None:
        """Gracefully close all pooled PostgreSQL connections."""
        self._closed = True
        if self._pool is not None:
            try:
                self._pool.close()
            finally:
                self._pool = None
