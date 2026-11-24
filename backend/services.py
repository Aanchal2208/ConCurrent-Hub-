

from __future__ import annotations

import asyncio
import logging
import threading
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Protocol

from realtime.connection_manager import ConnectionManager
from realtime.events import (
    build_audit_event,
    build_server_error_event,
    build_task_created_event,
    build_task_deleted_event,
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

logger = logging.getLogger(__name__)


class TaskRepositoryProtocol(Protocol):
    def create_task(self, title: str, description: str, status: str, priority: str,
                    assignee: str, user_id: str) -> Dict[str, Any]: ...
    def get_task(self, task_id: int) -> Optional[Dict[str, Any]]: ...
    def list_tasks(self, status: Optional[str], priority: Optional[str],
                   assignee: Optional[str], search: Optional[str],
                   sort_by: str, sort_dir: str) -> List[Dict[str, Any]]: ...
    def update_task(self, task_id: int, expected_version: int, title: Optional[str],
                    description: Optional[str], status: Optional[str],
                    priority: Optional[str], assignee: Optional[str],
                    user_id: str) -> UpdateResult: ...
    def delete_task(self, task_id: int, expected_version: Optional[int],
                    user_id: str) -> Dict[str, Any]: ...


class TaskService:
    """
    Coordinates task lifecycle, optimistic updates via the per-record scheduler,
    object-pool buffer leasing, audit logging, and real-time WebSocket notifications.
    """

    def __init__(
        self,
        repository: TaskRepositoryProtocol,
        scheduler: Optional[PerRecordScheduler] = None,
        object_pool: Optional[BoundedObjectPool[ReusableUpdateBuffer]] = None,
        connection_manager: Optional[ConnectionManager] = None,
        storage_backend: str = "postgres",
        persisted_in_postgresql: bool = True,
        db_pool_stats_fn: Optional[Any] = None,
        scheduler_workers: Optional[int] = None,
    ) -> None:
        self.repository = repository
        self.object_pool: BoundedObjectPool[ReusableUpdateBuffer] = (
            object_pool or BoundedObjectPool(capacity=32, preallocate=True)
        )
        self.connection_manager = connection_manager or ConnectionManager()
        self.storage_backend = storage_backend
        self.persisted_in_postgresql = persisted_in_postgresql
        self._db_pool_stats_fn = db_pool_stats_fn

        self._activity_lock = threading.Lock()
        self._activity_events: deque[Dict[str, Any]] = deque(maxlen=200)

        self.scheduler = scheduler or PerRecordScheduler(
            update_handler=self._execute_pooled_repo_update,
            max_workers=scheduler_workers or 8,
        )

    def _execute_pooled_repo_update(self, request: UpdateRequest) -> UpdateResult:
        """
        Called sequentially per task_id by PerRecordScheduler worker threads.
        Leases a pre-allocated ReusableUpdateBuffer to stage the payload.
        """
        with self.object_pool.lease(timeout=5.0) as buf:
            buf.write_update(
                task_id=request.task_id,
                expected_version=request.expected_version,
                title=request.title,
                description=request.description,
                status=request.status,
                priority=request.priority,
                assignee=request.assignee,
                user_id=request.user_id,
            )
            return self.repository.update_task(
                task_id=buf.metadata["task_id"],
                expected_version=buf.metadata["expected_version"],
                title=buf.metadata["title"],
                description=buf.metadata["description"],
                status=buf.metadata["status"],
                priority=buf.metadata["priority"],
                assignee=buf.metadata["assignee"],
                user_id=buf.metadata["user_id"],
            )

    # -----------------------------------------------------------------------
    # Task operations
    # -----------------------------------------------------------------------

    async def create_task(
        self,
        title: str,
        description: str = "",
        status: str = "TODO",
        priority: str = "MEDIUM",
        assignee: str = "unassigned",
        user_id: str = "system",
    ) -> Dict[str, Any]:
        created = await asyncio.to_thread(
            self.repository.create_task,
            title=title, description=description, status=status,
            priority=priority, assignee=assignee, user_id=user_id,
        )
        await self._record_activity(
            actor=user_id, task_id=int(created["id"]), operation="CREATE",
            old_version=None, new_version=int(created["version"]),
            outcome="SUCCESS", details=f"Created task '{created['title']}'",
        )
        await self.connection_manager.broadcast(build_task_created_event(
            task_id=int(created["id"]), version=int(created["version"]),
            title=str(created["title"]), description=str(created.get("description", "")),
            status=str(created.get("status", "TODO")), priority=str(created.get("priority", "MEDIUM")),
            assignee=str(created.get("assignee", "unassigned")), user_id=user_id,
        ))
        return created

    async def get_task(self, task_id: int) -> Optional[Dict[str, Any]]:
        return await asyncio.to_thread(self.repository.get_task, task_id)

    async def list_tasks(
        self,
        status: Optional[str] = None,
        priority: Optional[str] = None,
        assignee: Optional[str] = None,
        search: Optional[str] = None,
        sort_by: str = "id",
        sort_dir: str = "asc",
    ) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(
            self.repository.list_tasks,
            status=status, priority=priority, assignee=assignee,
            search=search, sort_by=sort_by, sort_dir=sort_dir,
        )

    async def update_task_via_scheduler(
        self,
        task_id: int,
        expected_version: int,
        title: Optional[str] = None,
        description: Optional[str] = None,
        status: Optional[str] = None,
        priority: Optional[str] = None,
        assignee: Optional[str] = None,
        user_id: str = "anonymous",
    ) -> UpdateResult:
        """Route an update through the PerRecordScheduler for serialization."""
        req = UpdateRequest(
            task_id=task_id, expected_version=expected_version, title=title,
            description=description, status=status, priority=priority,
            assignee=assignee, user_id=user_id,
        )
        scheduled = self.scheduler.submit_update_async(req)
        try:
            result = await asyncio.wait_for(
                asyncio.shield(asyncio.wrap_future(scheduled)),
                timeout=10.0,
            )
        except asyncio.TimeoutError:
            result = UpdateResult(
                status=UpdateStatus.ERROR,
                task_id=task_id,
                message=f"Scheduler timed out waiting for record {task_id} after 10.0s.",
            )

        outcome_str = {
            UpdateStatus.SUCCESS: "SUCCESS",
            UpdateStatus.VERSION_CONFLICT: "CONFLICT",
            UpdateStatus.NOT_FOUND: "FAILED",
        }.get(result.status, "ERROR")

        await self._record_activity(
            actor=user_id, task_id=result.task_id, operation="UPDATE",
            old_version=expected_version, new_version=result.new_version,
            outcome=outcome_str, details=result.message,
        )

        if result.status == UpdateStatus.SUCCESS and result.new_version is not None:
            await self.connection_manager.broadcast(build_task_updated_event(
                task_id=result.task_id, version=result.new_version, title=title,
                description=description, status=status, priority=priority,
                assignee=assignee, user_id=user_id,
            ))
        elif result.status == UpdateStatus.VERSION_CONFLICT and result.current_version is not None:
            await self.connection_manager.broadcast(build_version_conflict_event(
                task_id=result.task_id, expected_version=expected_version,
                current_version=result.current_version, message=result.message,
            ))
        elif result.status == UpdateStatus.ERROR:
            await self.connection_manager.broadcast(build_server_error_event(
                message=result.message, task_id=result.task_id,
            ))

        return result

    async def delete_task(
        self,
        task_id: int,
        expected_version: Optional[int] = None,
        user_id: str = "anonymous",
    ) -> Dict[str, Any]:
        res = await asyncio.to_thread(
            self.repository.delete_task,
            task_id=task_id, expected_version=expected_version, user_id=user_id,
        )
        if res.get("status") == "SUCCESS":
            await self._record_activity(
                actor=user_id, task_id=task_id, operation="DELETE",
                old_version=expected_version, new_version=None,
                outcome="SUCCESS", details=f"Deleted task {task_id}",
            )
            await self.connection_manager.broadcast(
                build_task_deleted_event(task_id=task_id, user_id=user_id)
            )
        return res

    # -----------------------------------------------------------------------
    # Health / telemetry
    # -----------------------------------------------------------------------

    def get_health_snapshot(self) -> Dict[str, Any]:
        db_stats = self._db_pool_stats_fn() if callable(self._db_pool_stats_fn) else None
        return {
            "status": "ok",
            "storage_backend": self.storage_backend,
            "persisted_in_postgresql": self.persisted_in_postgresql,
            "active_websocket_clients": self.connection_manager.active_count,
            "scheduler": self.scheduler.get_queue_snapshot(),
            "object_pool": self.object_pool.get_stats(),
            "database_pool": db_stats,
            "auth_status": {"enabled": True, "provider": "pbkdf2_hmac_sha256_postgres"},
        }

    def get_activity_snapshot(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._activity_lock:
            return list(self._activity_events)[:limit]

    async def _record_activity(
        self,
        actor: str,
        task_id: Optional[int],
        operation: str,
        old_version: Optional[int],
        new_version: Optional[int],
        outcome: str,
        details: str = "",
    ) -> None:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "actor": actor, "task_id": task_id, "operation": operation,
            "old_version": old_version, "new_version": new_version,
            "outcome": outcome, "details": details,
        }
        with self._activity_lock:
            self._activity_events.appendleft(entry)
        record_audit_log = getattr(self.repository, "record_audit_log", None)
        if callable(record_audit_log):
            try:
                await asyncio.to_thread(record_audit_log, **entry)
            except Exception:
                logger.exception("Failed to persist task activity audit event.")
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.connection_manager.broadcast(build_audit_event(**entry)))
        except RuntimeError:
            pass

    def close(self) -> None:
        self.scheduler.shutdown(wait=True)
        self.object_pool.close()
