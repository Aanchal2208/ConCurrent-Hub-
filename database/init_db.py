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

    if not pool_manager.check_connectivity():
        print("\n[WARNING] PostgreSQL is not reachable at the configured DSN.")
        print("To start PostgreSQL locally with Docker Compose, run:")
        print("    docker compose up -d postgres")
        print("Then re-run:")
        print("    python -m database.init_db")
        print("=" * 72)
        return False

    try:
        pool_manager.open()
        repo = PostgresTaskRepository(pool_manager)
        repo.init_schema()
        print("[OK] Schema database/schema.sql applied successfully (tasks table ready).")

        existing = repo.list_tasks()
        if seed_initial_tasks and not existing:
            t1 = repo.create_task("Prepare OS & DBMS Phase 2 report", user_id="aanchal")
            t2 = repo.create_task("Verify PostgreSQL connection pooling", user_id="parth")
            t3 = repo.create_task("Test FastAPI & WebSocket integration", user_id="gargi")
            print(f"[OK] Seeded 3 initial tasks (IDs: {t1['id']}, {t2['id']}, {t3['id']}).")
        else:
            print(f"[OK] Database currently contains {len(existing)} task(s).")

        print(f"[OK] Pool stats: {pool_manager.get_pool_stats()}")
        return True
    except DatabaseUnavailableError as exc:
        print(f"[ERROR] Failed to initialize database: {exc}")
        return False
    finally:
        pool_manager.close()
        print("[Done] Connection pool closed cleanly.")
        print("=" * 72)


if __name__ == "__main__":
    success = initialize_database()
    sys.exit(0 if success else 1)