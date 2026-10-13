
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Protocol

from realtime.connection_manager import ConnectionManager
from realtime.events import (
    build_server_error_event,
    build_task_created_event,
    build_task_updated_event,
    build_version_conflict_event,
)
from resource_pool.object_pool import BoundedObjectPool, ReusableUpdateBuffer
from scheduler.scheduler import (
    PerRecordScheduler,
    UpdateRequest,
    UpdateResult,
    UpdateStatus,
)


class TaskRepositoryProtocol(Protocol):
    def create_task(
        self, title: str, user_id: str = "anonymous"
    ) -> Dict[str, Any]:
        ...

    def get_task(self, task_id: int) -> Optional[Dict[str, Any]]:
        ...

    def list_tasks(self) -> List[Dict[str, Any]]:
        ...

    def update_task(
        self,
        task_id: int,
        title: str,
        expected_version: int,
        user_id: str = "anonymous",
    ) -> UpdateResult:
        ...


class TaskService:
    def __init__(
        self,
        repository: TaskRepositoryProtocol,
        scheduler: Optional[PerRecordScheduler] = None,
        object_pool: Optional[BoundedObjectPool[ReusableUpdateBuffer]] = None,
        connection_manager: Optional[ConnectionManager] = None,
        storage_backend: str = "postgres",
        persisted_in_postgresql: bool = True,
        db_pool_stats_fn: Optional[Any] = None,
    ) -> None:
        self.repository = repository
        self.object_pool: BoundedObjectPool[ReusableUpdateBuffer] = (
            object_pool or BoundedObjectPool(capacity=32, preallocate=True)
        )
        self.connection_manager = connection_manager or ConnectionManager()
        self.storage_backend = storage_backend
        self.persisted_in_postgresql = persisted_in_postgresql
        self._db_pool_stats_fn = db_pool_stats_fn

        self.scheduler = scheduler or PerRecordScheduler(
            update_handler=self._execute_pooled_repo_update,
            max_workers=8,
        )

    def _execute_pooled_repo_update(
        self, request: UpdateRequest
    ) -> UpdateResult:
        with self.object_pool.lease(timeout=5.0) as buf:
            buf.write_update(
                task_id=request.task_id,
                expected_version=request.expected_version,
                title=request.title,
                user_id=request.user_id,
            )
            return self.repository.update_task(
                task_id=buf.metadata["task_id"],
                title=buf.metadata["title"],
                expected_version=buf.metadata["expected_version"],
                user_id=buf.metadata["user_id"],
            )

    async def create_task(
        self, title: str, user_id: str = "user_1"
    ) -> Dict[str, Any]:
        created = await asyncio.to_thread(
            self.repository.create_task, title, user_id
        )
        event = build_task_created_event(
            task_id=int(created["id"]),
            version=int(created["version"]),
            title=str(created["title"]),
            user_id=user_id,
        )
        await self.connection_manager.broadcast(event)
        return created

    async def get_task(
        self, task_id: int
    ) -> Optional[Dict[str, Any]]:
        return await asyncio.to_thread(
            self.repository.get_task, task_id
        )

    async def list_tasks(self) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(
            self.repository.list_tasks
        )

    async def update_task_via_scheduler(
        self,
        task_id: int,
        expected_version: int,
        title: str,
        user_id: str = "user_1",
    ) -> UpdateResult:
        update_req = UpdateRequest(
            task_id=task_id,
            expected_version=expected_version,
            title=title,
            user_id=user_id,
        )

        result = await asyncio.to_thread(
            self.scheduler.submit_update, update_req
        )

        if (
            result.status == UpdateStatus.SUCCESS
            and result.new_version is not None
        ):
            await self.connection_manager.broadcast(
                build_task_updated_event(
                    task_id=result.task_id,
                    version=result.new_version,
                    title=title,
                    user_id=user_id,
                )
            )

        elif (
            result.status == UpdateStatus.VERSION_CONFLICT
            and result.current_version is not None
        ):
            await self.connection_manager.broadcast(
                build_version_conflict_event(
                    task_id=result.task_id,
                    expected_version=expected_version,
                    current_version=result.current_version,
                    message=result.message,
                )
            )

        elif result.status == UpdateStatus.ERROR:
            await self.connection_manager.broadcast(
                build_server_error_event(
                    message=result.message,
                    task_id=result.task_id,
                )
            )

        return result

    def get_health_snapshot(self) -> Dict[str, Any]:
        sched_snap = self.scheduler.get_queue_snapshot()
        db_stats = (
            self._db_pool_stats_fn()
            if callable(self._db_pool_stats_fn)
            else None
        )

        return {
            "status": "ok",
            "storage_backend": self.storage_backend,
            "persisted_in_postgresql": self.persisted_in_postgresql,
            "active_websocket_clients": self.connection_manager.active_count,
            "scheduler": {
                "closed": sched_snap["closed"],
                "max_workers": sched_snap["max_workers"],
                "active_queue_count": sched_snap["active_queue_count"],
                "contention_summary": sched_snap["contention_summary"],
            },
            "object_pool": self.object_pool.get_stats(),
            "database_pool": db_stats,
        }

    def close(self) -> None:
        self.scheduler.shutdown(wait=True)
        self.object_pool.close()
