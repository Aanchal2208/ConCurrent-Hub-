"""
database/init_db.py
Owner: Parth Gauniyal (Member 2)

Database initialization script for ConcurrentHub Phase 2.
Applies database/schema.sql to PostgreSQL and optionally seeds sample tasks.

Run command:
    python -m database.init_db
"""

import sys

from database.postgres_db import DatabaseUnavailableError, PostgresPoolManager
from database.repository import PostgresTaskRepository


def initialize_database(seed_initial_tasks: bool = True) -> bool:
    print("=" * 72)
    print("ConcurrentHub Phase 2 — PostgreSQL Schema Initialization")
    print("Owner: Parth Gauniyal (Member 2)")
    print("=" * 72)

    pool_manager = PostgresPoolManager(open_immediately=False)
    print(f"Target DSN: {pool_manager.conninfo}")