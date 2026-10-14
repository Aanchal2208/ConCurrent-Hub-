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

from _future_ import annotations

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
