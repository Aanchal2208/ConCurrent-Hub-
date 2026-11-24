

from __future__ import annotations

import logging
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

from fastapi import FastAPI, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from backend.auth import UserRepository
from backend.routes import router as api_router
from backend.services import TaskService
from database.postgres_db import DatabaseUnavailableError, PostgresPoolManager
from database.repository import PostgresTaskRepository
from realtime.connection_manager import ConnectionManager
from resource_pool.object_pool import BoundedObjectPool, ReusableUpdateBuffer
from scheduler.scheduler import PerRecordScheduler

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("concurrenthub")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("Starting ConcurrentHub...")

    pg_pool = getattr(app.state, "pg_pool", None)
    if pg_pool is None:
        pg_pool = PostgresPoolManager()
        app.state.pg_pool = pg_pool

    task_repo = getattr(app.state, "task_repo", None)
    if task_repo is None:
        task_repo = PostgresTaskRepository(pg_pool)
        app.state.task_repo = task_repo

    user_repo = getattr(app.state, "user_repo", None)
    if user_repo is None:
        user_repo = UserRepository(pg_pool)
        app.state.user_repo = user_repo

    try:
        if not pg_pool.check_connectivity():
            logger.warning(
                "PostgreSQL is not reachable at startup; starting in degraded mode. "
                "API requests will return 503 until the database becomes available."
            )
    except DatabaseUnavailableError as exc:
        logger.warning("PostgreSQL unavailable during startup: %s", exc)
    except Exception as exc:
        logger.warning("Unexpected PostgreSQL startup check error: %s", exc)

    try:
        task_repo.init_schema()
        logger.info("Task schema initialized.")
    except Exception as exc:
        logger.warning("Task schema initialization skipped because PostgreSQL is unavailable: %s", exc)

    try:
        user_repo.init_schema()
        logger.info("User schema initialized.")
    except Exception as exc:
        logger.warning("User schema initialization skipped because PostgreSQL is unavailable: %s", exc)

    connection_manager = getattr(app.state, "connection_manager", None)
    if connection_manager is None:
        connection_manager = ConnectionManager()
        app.state.connection_manager = connection_manager

    object_pool = getattr(app.state, "object_pool", None)
    if object_pool is None:
        object_pool = BoundedObjectPool(capacity=32, preallocate=True)
        app.state.object_pool = object_pool

    task_service = getattr(app.state, "task_service", None)
    if task_service is None:
        task_service = TaskService(
            repository=task_repo,
            object_pool=object_pool,
            connection_manager=connection_manager,
            storage_backend="postgres",
            persisted_in_postgresql=True,
            db_pool_stats_fn=pg_pool.get_pool_stats,
            scheduler_workers=pg_pool.max_size,
        )
        app.state.task_service = task_service

    logger.info(
        "ConcurrentHub ready. Storage=postgres, PoolCapacity=%d, DB_available=%s",
        object_pool.capacity,
        pg_pool.check_connectivity(),
    )

    yield

    logger.info("Shutting down ConcurrentHub...")
    task_service.close()
    pg_pool.close()
    logger.info("Shutdown complete.")


def create_app() -> FastAPI:
    application = FastAPI(
        title="ConcurrentHub",
        description=(
            "Multi-user collaborative task management with workload-aware per-record "
            "scheduling, optimistic concurrency control, object pooling, PostgreSQL "
            "persistence, and real-time WebSocket broadcasts. "
            "OS & DBMS Project — Team Nexus (OSDBMS-V-2026-T185)"
        ),
        version="3.0.0",
        lifespan=lifespan,
        docs_url="/api/docs",
        redoc_url="/api/redoc",
    )

    pg_pool = PostgresPoolManager()
    task_repo = PostgresTaskRepository(pg_pool)
    user_repo = UserRepository(pg_pool)
    connection_manager = ConnectionManager()
    object_pool = BoundedObjectPool(capacity=32, preallocate=True)
    task_service = TaskService(
        repository=task_repo,
        object_pool=object_pool,
        connection_manager=connection_manager,
        storage_backend="postgres",
        persisted_in_postgresql=True,
        db_pool_stats_fn=pg_pool.get_pool_stats,
        scheduler_workers=pg_pool.max_size,
    )
    application.state.pg_pool = pg_pool
    application.state.task_repo = task_repo
    application.state.user_repo = user_repo
    application.state.connection_manager = connection_manager
    application.state.object_pool = object_pool
    application.state.task_service = task_service

    # CORS — configure allowed origins from environment variable
    raw_origins = os.getenv(
        "ALLOWED_ORIGINS",
        "http://localhost:8000,http://127.0.0.1:8000,"
        "http://localhost:5500,http://127.0.0.1:5500",
    )
    allowed_origins = [o.strip() for o in raw_origins.split(",") if o.strip()]
    if not allowed_origins:
        allowed_origins = ["http://localhost:8000"]

    application.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # REST API routes
    application.include_router(api_router, prefix="")

    # WebSocket endpoint
    @application.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket) -> None:
        manager: ConnectionManager = getattr(application.state, "connection_manager", None)
        if manager is None:
            await websocket.close(code=1011)
            return
        await manager.connect(websocket)
        try:
            while True:
                msg = await websocket.receive_text()
                if msg.strip().lower() == "ping":
                    await websocket.send_json({"event": "pong"})
        except Exception:
            await manager.disconnect(websocket)

    # Static frontend — mount AFTER all API routes so /ws and /api/* win
    base_dir = PROJECT_ROOT
    frontend_dir = base_dir / "frontend"
    if frontend_dir.exists():
        application.mount(
            "/",
            StaticFiles(directory=str(frontend_dir), html=True),
            name="frontend",
        )
    else:
        logger.warning("frontend/ directory not found — no static files served.")

    return application


app = create_app()
