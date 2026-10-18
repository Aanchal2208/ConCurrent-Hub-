

from __future__ import annotations

import copy
import queue
import threading
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
    title: str
    user_id: str = "anonymous"


@dataclass(frozen=True)
class UpdateResult:
    """
    Standardized outcome returned by the scheduler and repository layers.
    Matches the shared API contract in docs/api-contract.md.
    """

    status: str
    task_id: int
    message: str
    new_version: Optional[int] = None
    current_version: Optional[int] = None
    task: Optional[Dict[str, Any]] = None
    sequence_order: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary formatted according to the shared API contract."""
        if self.status == UpdateStatus.SUCCESS:
            payload: Dict[str, Any] = {
                "status": self.status,
                "task_id": self.task_id,
                "new_version": self.new_version,
                "message": self.message,
            }
            if self.task is not None:
                payload["task"] = copy.deepcopy(self.task)
            return payload

        if self.status == UpdateStatus.VERSION_CONFLICT:
            return {
                "status": self.status,
                "task_id": self.task_id,
                "current_version": self.current_version,
                "message": self.message,
            }

        return {
            "status": self.status,
            "task_id": self.task_id,
            "message": self.message,
        }


class InMemoryTaskStore:
    """
    Standalone thread-safe in-memory task repository.
    Allows Aanchal's scheduler demo and unit tests (as well as API mock/test mode)
    to run without requiring a live PostgreSQL instance.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tasks: Dict[int, Dict[str, Any]] = {}
        self._next_id: int = 1

    def create_task(self, title: str, user_id: str = "anonymous") -> Dict[str, Any]:
        """Create a new task at version 1."""
        clean_title = title.strip()
        if not clean_title:
            raise ValueError("Task title must not be empty.")
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock:
            task_id = self._next_id
            self._next_id += 1
            record = {
                "id": task_id,
                "title": clean_title,
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

    def list_tasks(self) -> List[Dict[str, Any]]:
        """Return copies of all tasks ordered by ID."""
        with self._lock:
            return [copy.deepcopy(self._tasks[k]) for k in sorted(self._tasks.keys())]

    def update_task(
        self,
        task_id: int,
        title: str,
        expected_version: int,
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

            current_version = int(record["version"])
            check = validate_version(
                current_version=current_version,
                expected_version=expected_version,
            )
            if not check.is_valid:
                return UpdateResult(
                    status=UpdateStatus.VERSION_CONFLICT,
                    task_id=task_id,
                    current_version=current_version,
                    message="The task was modified by another request",
                )

            assert check.next_version is not None
            record["title"] = clean_title
            record["version"] = check.next_version
            record["user_id"] = user_id
            record["updated_at"] = datetime.now(timezone.utc).isoformat()
            updated_copy = copy.deepcopy(record)

        return UpdateResult(
            status=UpdateStatus.SUCCESS,
            task_id=task_id,
            new_version=updated_copy["version"],
            message="Task updated successfully",
            task=updated_copy,
        )

    def handle_scheduled_update(self, request: UpdateRequest) -> UpdateResult:
        """Adapter matching the scheduler's `Callable[[UpdateRequest], UpdateResult]` signature."""
        return self.update_task(
            task_id=request.task_id,
            title=request.title,
            expected_version=request.expected_version,
            user_id=request.user_id,
        )


@dataclass
class _QueuedItem:
    """Internal envelope holding a request and its completion Future."""

    request: UpdateRequest
    future: Future[UpdateResult]
    enqueue_seq: int


@dataclass
class _RecordQueueBucket:
    """
    Internal state for a single record ID's FIFO queue.
    Access to `pending_count` and `worker_active` is synchronized by the
    scheduler's `_registry_lock`.
    """

    task_id: int
    fifo_queue: "queue.Queue[_QueuedItem]" = field(default_factory=queue.Queue)
    pending_count: int = 0
    worker_active: bool = False
    processed_count: int = 0


class PerRecordScheduler:
    """
    Application-level workload-aware scheduler that maintains a separate FIFO
    queue for each `task_id`.

    - Updates to the SAME `task_id` are placed in that record's `queue.Queue`
      and processed strictly sequentially by a single active drain worker for
      that record.
    - Updates to DIFFERENT `task_id`s are placed in separate `queue.Queue`
      instances and processed concurrently across the worker thread pool.
    """

    def __init__(
        self,
        update_handler: Optional[Callable[[UpdateRequest], UpdateResult]] = None,
        max_workers: int = 8,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be at least 1")

        self._standalone_store = InMemoryTaskStore()
        self._update_handler: Callable[[UpdateRequest], UpdateResult] = (
            update_handler or self._standalone_store.handle_scheduled_update
        )
        self._max_workers = max_workers
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="record-scheduler-worker",
        )

        self._registry_lock = threading.Lock()
        self._buckets: Dict[int, _RecordQueueBucket] = {}
        self._execution_history: List[Dict[str, Any]] = []
        self._sequence_counter = 0
        self._enqueue_counter = 0
        self._closed = False
        self._conflict_detector = ConflictDetector()

    @property
    def store(self) -> InMemoryTaskStore:
        """Return the built-in in-memory store used in standalone demonstrations."""
        return self._standalone_store

    @property
    def conflict_detector(self) -> ConflictDetector:
        """Return the conflict detector tracking contention statistics."""
        return self._conflict_detector

    def submit_update_async(self, request: UpdateRequest) -> Future[UpdateResult]:
        """
        Validate and enqueue an `UpdateRequest` into the target record's FIFO queue.
        Returns a `Future[UpdateResult]` that resolves when the update is processed.
        """
        result_future: Future[UpdateResult] = Future()

        # Basic input validation before enqueueing
        if request.task_id < 1:
            result_future.set_result(
                UpdateResult(
                    status=UpdateStatus.ERROR,
                    task_id=request.task_id,
                    message="task_id must be a positive integer.",
                )
            )
            return result_future

        if request.expected_version < 1:
            result_future.set_result(
                UpdateResult(
                    status=UpdateStatus.ERROR,
                    task_id=request.task_id,
                    message="expected_version must be >= 1.",
                )
            )
            return result_future

        if not request.title or not request.title.strip():
            result_future.set_result(
                UpdateResult(
                    status=UpdateStatus.ERROR,
                    task_id=request.task_id,
                    message="Task title must not be empty.",
                )
            )
            return result_future

        with self._registry_lock:
            if self._closed:
                result_future.set_result(
                    UpdateResult(
                        status=UpdateStatus.ERROR,
                        task_id=request.task_id,
                        message="Scheduler is shut down and cannot accept new requests.",
                    )
                )
                return result_future

            bucket = self._buckets.get(request.task_id)
            if bucket is None:
                bucket = _RecordQueueBucket(task_id=request.task_id)
                self._buckets[request.task_id] = bucket

            self._enqueue_counter += 1
            item = _QueuedItem(
                request=request,
                future=result_future,
                enqueue_seq=self._enqueue_counter,
            )
            bucket.fifo_queue.put(item)
            bucket.pending_count += 1
            current_depth = bucket.pending_count

            self._conflict_detector.record_enqueue(
                task_id=request.task_id,
                current_queue_depth=current_depth,
            )

            # Start a drain worker for this record only if none is currently running.
            # This guarantees at most one worker processes a given task_id at a time.
            if not bucket.worker_active:
                bucket.worker_active = True
                try:
                    self._executor.submit(self._drain_record_queue, request.task_id)
                except RuntimeError as exc:
                    bucket.worker_active = False
                    bucket.pending_count -= 1
                    try:
                        bucket.fifo_queue.get_nowait()
                        bucket.fifo_queue.task_done()
                    except queue.Empty:
                        pass
                    result_future.set_result(
                        UpdateResult(
                            status=UpdateStatus.ERROR,
                            task_id=request.task_id,
                            message=f"Failed to schedule worker: {exc}",
                        )
                    )

        return result_future

    def submit_update(self, request: UpdateRequest, timeout: float = 15.0) -> UpdateResult:
        """
        Submit an update request to the per-record FIFO queue and wait for its
        completion result.
        """
        future = self.submit_update_async(request)
        try:
            return future.result(timeout=timeout)
        except TimeoutError:
            return UpdateResult(
                status=UpdateStatus.ERROR,
                task_id=request.task_id,
                message=f"Scheduled update timed out after {timeout}s.",
            )
        except Exception as exc:  # Defensive safeguard
            return UpdateResult(
                status=UpdateStatus.ERROR,
                task_id=request.task_id,
                message=f"Unexpected scheduler execution failure: {exc}",
            )

    def _drain_record_queue(self, task_id: int) -> None:
        """
        Worker loop that sequentially drains all queued updates for `task_id`
        in strict FIFO order. Exits cleanly once the queue is empty, and removes
        idle queue buckets under `_registry_lock`.
        """
        while True:
            with self._registry_lock:
                bucket = self._buckets.get(task_id)
                if bucket is None:
                    return
                try:
                    item = bucket.fifo_queue.get_nowait()
                except queue.Empty:
                    # No more items waiting for this task_id: mark worker inactive
                    # and clean up idle bucket so inactive records don't leak memory.
                    bucket.worker_active = False
                    if bucket.pending_count == 0 and bucket.fifo_queue.empty():
                        del self._buckets[task_id]
                    return

            # Execute outside `_registry_lock` so other task_ids can be enqueued
            # and processed concurrently without blocking on this record's update!
            try:
                raw_result = self._update_handler(item.request)
            except Exception as exc:
                raw_result = UpdateResult(
                    status=UpdateStatus.ERROR,
                    task_id=item.request.task_id,
                    message=f"Update handler error: {exc}",
                )

            with self._registry_lock:
                self._sequence_counter += 1
                seq_num = self._sequence_counter
                bucket = self._buckets.get(task_id)
                if bucket is not None:
                    bucket.pending_count = max(0, bucket.pending_count - 1)
                    bucket.processed_count += 1

                final_result = UpdateResult(
                    status=raw_result.status,
                    task_id=raw_result.task_id,
                    message=raw_result.message,
                    new_version=raw_result.new_version,
                    current_version=raw_result.current_version,
                    task=copy.deepcopy(raw_result.task) if raw_result.task else None,
                    sequence_order=seq_num,
                )

                self._execution_history.append(
                    {
                        "sequence_order": seq_num,
                        "enqueue_seq": item.enqueue_seq,
                        "task_id": item.request.task_id,
                        "user_id": item.request.user_id,
                        "expected_version": item.request.expected_version,
                        "title": item.request.title,
                        "status": final_result.status,
                        "new_version": final_result.new_version,
                        "current_version": final_result.current_version,
                    }
                )

            self._conflict_detector.record_outcome(
                task_id=item.request.task_id,
                status=final_result.status,
            )
            bucket_ref_queue = bucket.fifo_queue if bucket is not None else None
            if bucket_ref_queue is not None:
                bucket_ref_queue.task_done()

            if not item.future.done():
                item.future.set_result(final_result)

    def get_queue_snapshot(self) -> Dict[str, Any]:
        """
        Return a safe, immutable snapshot of current per-record queues, execution
        history, and conflict detector statistics without exposing mutable internals.
        """
        with self._registry_lock:
            active_queues = {
                task_id: {
                    "task_id": task_id,
                    "pending_count": bucket.pending_count,
                    "approx_queue_size": bucket.fifo_queue.qsize(),
                    "worker_active": bucket.worker_active,
                    "processed_count": bucket.processed_count,
                }
                for task_id, bucket in sorted(self._buckets.items())
            }
            history_copy = copy.deepcopy(self._execution_history)
            closed_state = self._closed
            max_workers = self._max_workers

        return {
            "closed": closed_state,
            "max_workers": max_workers,
            "active_record_queues": active_queues,
            "active_queue_count": len(active_queues),
            "execution_history": history_copy,
            "contention_summary": self._conflict_detector.get_summary_snapshot(),
        }

    def shutdown(self, wait: bool = True) -> None:
        """
        Gracefully stop accepting new update requests and wait for all queued
        requests to finish processing before shutting down worker threads.
        """
        with self._registry_lock:
            self._closed = True

        self._executor.shutdown(wait=wait)
