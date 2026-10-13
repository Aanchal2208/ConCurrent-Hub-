
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from backend.routes import router as api_router
from backend.services import TaskRepositoryProtocol, TaskService
from database.postgres_db import PostgresPoolManager
from database.repository import PostgresTaskRepository
from realtime.connection_manager import ConnectionManager
from realtime.websocket_server import create_websocket_router
from resource_pool.object_pool import BoundedObjectPool, ReusableUpdateBuffer
from scheduler.scheduler import InMemoryTaskStore, PerRecordScheduler

logger = logging.getLogger("concurrenthub")


def create_app(
    repository: Optional[TaskRepositoryProtocol] = None,
    scheduler: Optional[PerRecordScheduler] = None,
    use_in_memory_db: Optional[bool] = None,
) -> FastAPI:
    connection_manager = ConnectionManager()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool_manager: Optional[PostgresPoolManager] = None
        env_mode = os.getenv("DB_MODE", "auto").strip().lower()
        force_memory = (
            use_in_memory_db
            if use_in_memory_db is not None
            else env_mode in ("memory", "mock", "demo")
        )

        active_repo: TaskRepositoryProtocol
        storage_backend: str
        persisted_in_pg: bool
        db_pool_stats_fn = None

        if repository is not None:
            active_repo = repository
            storage_backend = "injected_custom_repository"
            persisted_in_pg = isinstance(repository, PostgresTaskRepository)

        elif force_memory:
            active_repo = InMemoryTaskStore()
            storage_backend = "in_memory_demo"
            persisted_in_pg = False
            logger.warning(
                "Starting ConcurrentHub in EXPLICIT IN-MEMORY DEMO MODE. "
                "Writes are stored in RAM only and are NOT persisted in PostgreSQL."
            )

        else:
            candidate_pool = PostgresPoolManager(open_immediately=False)

            if candidate_pool.check_connectivity():
                candidate_pool.open()
                pg_repo = PostgresTaskRepository(candidate_pool)
                pg_repo.init_schema()

                pool_manager = candidate_pool
                active_repo = pg_repo
                storage_backend = "postgres"
                persisted_in_pg = True
                db_pool_stats_fn = candidate_pool.get_pool_stats

                logger.info(
                    "Connected to PostgreSQL (%s). Schema verified and connection pool active.",
                    candidate_pool.conninfo,
                )

            elif env_mode == "postgres_strict":
                candidate_pool.close()
                raise RuntimeError(
                    f"DB_MODE=postgres_strict and PostgreSQL is unreachable at {candidate_pool.conninfo}"
                )

            else:
                candidate_pool.close()
                active_repo = InMemoryTaskStore()
                storage_backend = "in_memory_demo"
                persisted_in_pg = False

                logger.warning(
                    "PostgreSQL is unreachable at %s. Falling back to IN-MEMORY DEMO MODE "
                    "(persisted_in_postgresql=False). Start PostgreSQL via `docker compose up -d postgres` "
                    "for persistent ACID storage.",
                    candidate_pool.conninfo,
                )

        obj_pool: BoundedObjectPool[ReusableUpdateBuffer] = BoundedObjectPool(
            capacity=int(os.getenv("OBJECT_POOL_CAPACITY", "64")),
            preallocate=True,
        )

        task_service = TaskService(
            repository=active_repo,
            scheduler=scheduler,
            object_pool=obj_pool,
            connection_manager=connection_manager,
            storage_backend=storage_backend,
            persisted_in_postgresql=persisted_in_pg,
            db_pool_stats_fn=db_pool_stats_fn,
        )

        app.state.task_service = task_service
        app.state.connection_manager = connection_manager

        try:
            yield
        finally:
            task_service.close()
            if pool_manager is not None:
                pool_manager.close()

    app = FastAPI(
        title="ConcurrentHub Phase 2 — Collaborative Task Backend",
        description=(
            "OS & DBMS Project (OSDBMS-V-2026-T185, Team Nexus). "
            "Coordinates concurrent record updates through an application-level "
            "per-record FIFO scheduler, a BareHeap-inspired object pool, "
            "PostgreSQL optimistic concurrency control, and real-time WebSockets."
        ),
        version="2.0.0",
        lifespan=lifespan,
    )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "status": "VALIDATION_ERROR",
                "message": "Invalid request payload",
                "details": jsonable_encoder(exc.errors()),
            },
        )

    app.include_router(api_router)
    app.include_router(create_websocket_router(connection_manager))

    return app


app = create_app()
