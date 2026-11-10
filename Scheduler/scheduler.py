"""
Application-level, MxTasks-inspired per-record FIFO update scheduler.
OS & DBMS Phase 2 (Team Nexus - OSDBMS-V-2026-T185)

Key properties:
1. Maintains a dedicated FIFO queue (`queue.Queue`) per active `task_id`.
2. Serializes conflicting updates targeting the same `task_id` in strict arrival order.
3. Dispatches independent `task_id` queues concurrently across worker threads.
4. Validates optimistic versions in memory or via PostgreSQL repository callback.
5. In-Memory fallback store supports full CRUD (Create, List with filters/search/sort, Get, OCC Update, Delete).
"""

from __future__ import annotations

import copy
import queue
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from scheduler.conflict_detector import ConflictDetector
from scheduler.version_check import validate_version


class UpdateStatus:
    """Standard status codes shared across scheduler, repository, and backend API."""

    SUCCESS = "SUCCESS"
    VERSION_CONFLICT = "VERSION_CONFLICT"
    NOT_FOUND = "NOT_FOUND"
    ERROR = "ERROR"


@dataclass(frozen=True)
class UpdateRequest:
    """Represents a single task update operation submitted to the scheduler."""

    task_id: int
    expected_version: int
    title: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    priority: Optional[str] = None
    assignee: Optional[str] = None
    user_id: str = "anonymous"


@dataclass(frozen=True)
class UpdateResult:
    """Standardized outcome returned by the scheduler and repository layers."""

    status: str
    task_id: int
    message: str
    new_version: Optional[int] = None
    current_version: Optional[int] = None
    task: Optional[Dict[str, Any]] = None
    sequence_order: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary formatted according to the shared API contract."""
        payload: Dict[str, Any] = {
            "status": self.status,
            "task_id": self.task_id,
            "message": self.message,
        }
        if self.new_version is not None:
            payload["new_version"] = self.new_version
        if self.current_version is not None:
            payload["current_version"] = self.current_version
        if self.task is not None:
            payload["task"] = copy.deepcopy(self.task)
        if self.sequence_order is not None:
            payload["sequence_order"] = self.sequence_order
        return payload


class InMemoryTaskStore:
    """
    Thread-safe in-memory task repository.
    Allows complete CRUD, search, filtering, sorting, and optimistic concurrency
    validation without requiring PostgreSQL to be running.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tasks: Dict[int, Dict[str, Any]] = {}
        self._next_id: int = 1

    def create_task(
        self,
        title: str,
        description: str = "",
        status: str = "TODO",
        priority: str = "MEDIUM",
        assignee: str = "unassigned",
        user_id: str = "anonymous",
    ) -> Dict[str, Any]:
        """Create a new task at version 1."""
        clean_title = title.strip()
        if not clean_title:
            raise ValueError("Task title must not be empty.")

        clean_status = status.upper().strip() if status else "TODO"
        if clean_status not in ("TODO", "IN_PROGRESS", "REVIEW", "DONE"):
            clean_status = "TODO"

        clean_priority = priority.upper().strip() if priority else "MEDIUM"
        if clean_priority not in ("LOW", "MEDIUM", "HIGH", "CRITICAL"):
            clean_priority = "MEDIUM"

        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock:
            task_id = self._next_id
            self._next_id += 1
            record = {
                "id": task_id,
                "title": clean_title,
                "description": (description or "").strip(),
                "status": clean_status,
                "priority": clean_priority,
                "assignee": (assignee or "unassigned").strip(),
                "version": 1,
                "user_id": user_id,
                "created_at": now_iso,
                "updated_at": now_iso,
            }
            self._tasks[task_id] = record
            return copy.deepcopy(record)

    def get_task(self, task_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve a copy of a task by ID, or None if not found."""
        with self._lock:
            record = self._tasks.get(task_id)
            return copy.deepcopy(record) if record is not None else None

    def list_tasks(
        self,
        status: Optional[str] = None,
        priority: Optional[str] = None,
        assignee: Optional[str] = None,
        search: Optional[str] = None,
        sort_by: str = "id",
        sort_dir: str = "asc",
    ) -> List[Dict[str, Any]]:
        """Return copies of all tasks matching optional filters and sorting."""
        with self._lock:
            tasks = [copy.deepcopy(t) for t in self._tasks.values()]

        # Filter
        if status:
            tasks = [t for t in tasks if t.get("status", "").upper() == status.upper()]
        if priority:
            tasks = [t for t in tasks if t.get("priority", "").upper() == priority.upper()]
        if assignee:
            tasks = [t for t in tasks if t.get("assignee", "").lower() == assignee.lower()]
        if search:
            q = search.lower()
            tasks = [t for t in tasks if q in t.get("title", "").lower() or q in t.get("description", "").lower()]

        # Sort
        reverse = (sort_dir.lower() == "desc")
        valid_keys = {"id", "title", "version", "updated_at", "priority", "status"}
        sort_key = sort_by.lower() if sort_by.lower() in valid_keys else "id"
        tasks.sort(key=lambda t: t.get(sort_key, ""), reverse=reverse)
        return tasks

    def update_task(
        self,
        task_id: int,
        expected_version: int,
        title: str,
        description: Optional[str] = None,
        status: Optional[str] = None,
        priority: Optional[str] = None,
        assignee: Optional[str] = None,
        user_id: str = "anonymous",
    ) -> UpdateResult:
        """
        Perform an atomic version-checked update in memory.
        A failed version check never modifies the stored record.
        """
        clean_title = title.strip()
        if not clean_title:
            return UpdateResult(
                status=UpdateStatus.ERROR,
                task_id=task_id,
                message="Task title must not be empty.",
            )

        with self._lock:
            record = self._tasks.get(task_id)
            if record is None:
                return UpdateResult(
                    status=UpdateStatus.NOT_FOUND,
                    task_id=task_id,
                    message=f"Task {task_id} not found",
                )

            current_ver = record["version"]
            check = validate_version(current_ver, expected_version)
            if not check.is_valid:
                return UpdateResult(
                    status=UpdateStatus.VERSION_CONFLICT,
                    task_id=task_id,
                    current_version=current_ver,
                    message=f"Conflict detected. Expected version {expected_version}, but task is at version {current_ver}.",
                    task=copy.deepcopy(record),
                )

            # Atomic commit
            new_version = current_ver + 1
            now_iso = datetime.now(timezone.utc).isoformat()
            record["title"] = clean_title
            if description is not None:
                record["description"] = description.strip()
            if status is not None:
                record["status"] = status.upper().strip()
            if priority is not None:
                record["priority"] = priority.upper().strip()
            if assignee is not None:
                record["assignee"] = assignee.strip()
            record["version"] = new_version
            record["user_id"] = user_id
            record["updated_at"] = now_iso

            return UpdateResult(
                status=UpdateStatus.SUCCESS,
                task_id=task_id,
                new_version=new_version,
                message="Task updated successfully",
                task=copy.deepcopy(record),
            )

    def delete_task(
        self,
        task_id: int,
        expected_version: Optional[int] = None,
        user_id: str = "anonymous",
    ) -> Dict[str, Any]:
        """Delete task with optional version checking."""
        with self._lock:
            record = self._tasks.get(task_id)
            if record is None:
                return {"status": "NOT_FOUND", "task_id": task_id, "message": f"Task {task_id} not found"}

            if expected_version is not None and record["version"] != expected_version:
                return {
                    "status": "VERSION_CONFLICT",
                    "task_id": task_id,
                    "current_version": record["version"],
                    "message": f"Cannot delete task {task_id}: expected version {expected_version}, current is {record['version']}.",
                }

            del self._tasks[task_id]
            return {"status": "SUCCESS", "task_id": task_id, "message": "Task deleted successfully"}


class PerRecordScheduler:
    """
    Per-record FIFO update scheduler inspired by the MxTasks approach.

    - Every active `task_id` has its own FIFO `queue.Queue`.
    - Updates targeting the SAME `task_id` are strictly serialized in arrival order.
    - Updates targeting DIFFERENT `task_id`s run concurrently in a thread pool.
    - Tracks queue depth, contention, and execution outcomes via `ConflictDetector`.
    """

    def __init__(
        self,
        update_handler: Optional[Callable[[UpdateRequest], UpdateResult]] = None,
        max_workers: int = 8,
    ) -> None:
        self.update_handler = update_handler
        self.max_workers = max(1, max_workers)
        self._lock = threading.Lock()
        self._queues: Dict[int, queue.Queue] = {}
        self._queue_active: Dict[int, bool] = {}
        self._pool = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="sched-worker",
        )
        self.detector = ConflictDetector()
        self._sequence_counter = 0
        self._closed = False
        self._in_flight_tasks: Dict[int, float] = {}

    def submit_update(self, request: UpdateRequest, timeout: float = 10.0) -> UpdateResult:
        """
        Synchronously submit an update request to its per-record queue and wait
        for the result Future up to `timeout` seconds.
        """
        fut = self.submit_update_async(request)
        try:
            return fut.result(timeout=timeout)
        except TimeoutError:
            return UpdateResult(
                status=UpdateStatus.ERROR,
                task_id=request.task_id,
                message=f"Scheduler timed out waiting for record {request.task_id} after {timeout}s.",
            )
        except Exception as exc:
            return UpdateResult(
                status=UpdateStatus.ERROR,
                task_id=request.task_id,
                message=f"Scheduler error processing record {request.task_id}: {exc}",
            )

    def submit_update_async(self, request: UpdateRequest) -> Future[UpdateResult]:
        """
        Enqueue an update request into the record's dedicated queue.
        Returns a Future that resolves to an UpdateResult.
        """
        fut: Future[UpdateResult] = Future()

        with self._lock:
            if self._closed:
                fut.set_result(
                    UpdateResult(
                        status=UpdateStatus.ERROR,
                        task_id=request.task_id,
                        message="Scheduler is closed. Request rejected.",
                    )
                )
                return fut

            self._sequence_counter += 1
            seq = self._sequence_counter

            tid = request.task_id
            if tid not in self._queues:
                self._queues[tid] = queue.Queue()
                self._queue_active[tid] = False

            q = self._queues[tid]
            q.put((request, fut, seq))
            depth = q.qsize()

            self.detector.record_enqueue(tid, depth)

            if not self._queue_active[tid]:
                self._queue_active[tid] = True
                self._pool.submit(self._drain_queue, tid)

        return fut

    def _drain_queue(self, task_id: int) -> None:
        """Worker loop processing all pending requests for a specific record sequentially."""
        while True:
            with self._lock:
                q = self._queues.get(task_id)
                if q is None or q.empty():
                    self._queue_active[task_id] = False
                    if q is not None and q.empty():
                        del self._queues[task_id]
                        del self._queue_active[task_id]
                    break

            try:
                request, fut, seq = q.get_nowait()
            except queue.Empty:
                with self._lock:
                    self._queue_active[task_id] = False
                    if task_id in self._queues and self._queues[task_id].empty():
                        del self._queues[task_id]
                        del self._queue_active[task_id]
                break

            start_t = time.perf_counter()
            with self._lock:
                self._in_flight_tasks[task_id] = start_t

            try:
                if self.update_handler is not None:
                    res = self.update_handler(request)
                else:
                    res = UpdateResult(
                        status=UpdateStatus.SUCCESS,
                        task_id=request.task_id,
                        new_version=request.expected_version + 1,
                        message="Processed by default scheduler handler.",
                    )

                is_conflict = (res.status == UpdateStatus.VERSION_CONFLICT)
                is_success = (res.status == UpdateStatus.SUCCESS)
                self.detector.record_outcome(task_id, is_conflict, is_success)

                if not fut.done():
                    fut.set_result(res)
            except Exception as exc:
                self.detector.record_outcome(task_id, is_conflict=False, is_success=False)
                if not fut.done():
                    fut.set_result(
                        UpdateResult(
                            status=UpdateStatus.ERROR,
                            task_id=task_id,
                            message=f"Handler exception: {exc}",
                        )
                    )
            finally:
                with self._lock:
                    self._in_flight_tasks.pop(task_id, None)
                q.task_done()

    def get_metrics(self) -> Dict[str, Any]:
        """Return live telemetry metrics."""
        summary = self.detector.get_global_summary()
        with self._lock:
            active_q_count = sum(1 for active in self._queue_active.values() if active)
            active_records = list(self._queue_active.keys())
            pending = sum(q.qsize() for q in self._queues.values())
            in_flight = len(self._in_flight_tasks)

        return {
            "closed": self._closed,
            "max_workers": self.max_workers,
            "active_workers": min(in_flight, self.max_workers),
            "active_queue_count": active_q_count,
            "active_record_queues": active_records,
            "pending_tasks": pending,
            "in_flight_tasks": in_flight,
            "total_enqueued": summary.get("total_enqueued", 0),
            "contended_enqueues": summary.get("total_contended_enqueues", 0),
            "successful_executions": summary.get("total_successful_updates", 0),
            "version_conflicts": summary.get("total_version_conflicts", 0),
            "contention_summary": summary,
        }

    def get_queue_snapshot(self) -> Dict[str, Any]:
        return self.get_metrics()

    def shutdown(self, wait: bool = True) -> None:
        """Shut down thread pool."""
        with self._lock:
            self._closed = True
        self._pool.shutdown(wait=wait)
