# ConcurrentHub Phase 2 — Concurrency & Scheduling Design

**Project ID:** OSDBMS-V-2026-T185  
**Team Name:** Team Nexus

---

## 1. Per-Record FIFO Queue Architecture

Instead of protecting all database updates with one global lock (which serializes unrelated records) or using a bare lock dictionary (which does not preserve arrival order across threads), `PerRecordScheduler` in [`scheduler/scheduler.py`](file:///c:/Users/Aanchal/OneDrive/Desktop/ConcurrentHub/scheduler/scheduler.py) maintains a mapping of active record IDs to `_RecordQueueBucket` instances:

- Each `_RecordQueueBucket` owns a thread-safe `queue.Queue[_QueuedItem]` (`fifo_queue`), an integer `pending_count`, and a boolean `worker_active` flag.
- Each `_QueuedItem` pairs an immutable `UpdateRequest` with a `concurrent.futures.Future[UpdateResult]`.

---

## 2. Worker Lifecycle & Synchronization Rules

1. **Enqueue Synchronization (`_registry_lock`):**
   - When `submit_update_async(request)` is called, the scheduler acquires `_registry_lock` only long enough to look up (or create) the `_RecordQueueBucket` for `request.task_id`, put the `_QueuedItem` into `bucket.fifo_queue`, and increment `bucket.pending_count`.
   - If `bucket.worker_active` is `False`, the scheduler sets `bucket.worker_active = True` and submits `_drain_record_queue(request.task_id)` to the `ThreadPoolExecutor`.
   - If `bucket.worker_active` is already `True`, no second worker is spawned for `request.task_id`; the existing active worker for that record will pick up the newly enqueued item on its next loop iteration.
2. **Lock-Free Execution of Update Handlers:**
   - Inside `_drain_record_queue(task_id)`, the worker pops the next item under `_registry_lock` and then **releases `_registry_lock` before invoking `_update_handler(item.request)`**.
   - Because `_update_handler` executes outside `_registry_lock`, workers for different `task_id`s execute their database updates in parallel without blocking one another or blocking new enqueues.
3. **Race-Free Bucket Cleanup:**
   - When `bucket.fifo_queue.get_nowait()` raises `queue.Empty` while holding `_registry_lock`, no new item can be enqueued concurrently because `_registry_lock` is held.
   - The worker atomically sets `bucket.worker_active = False` and deletes `self._buckets[task_id]` if `pending_count == 0`, preventing unbounded growth of the bucket dictionary over time.

---

## 3. Two-Layer Optimistic Version Validation

1. **Application Layer (`scheduler/version_check.py`):**
   - `validate_version(current_version, expected_version)` checks `current_version == expected_version` and computes `next_version = current_version + 1`.
   - Used directly by `InMemoryTaskStore` for standalone scheduler demonstrations and unit tests.
2. **Database Layer (`database/repository.py`):**
   - In the integrated system, `PostgresTaskRepository.update_task` does **not** rely on a non-atomic read-then-write check in Python.
   - Instead, it executes an atomic conditional `UPDATE ... WHERE id = %(task_id)s AND version = %(expected_version)s RETURNING ...`.
   - If the conditional `UPDATE` returns 0 rows, no change was made to the table. A follow-up `SELECT id, version FROM tasks WHERE id = %(task_id)s` is then run inside the same transaction solely to report whether the row is missing (`NOT_FOUND`) or has a newer version (`VERSION_CONFLICT`).

---

## 4. Durability & Multi-Instance Limitations

- **Single-Instance Queue Scope:** `PerRecordScheduler` coordinates requests within a single Python process. In a deployment with multiple backend instances behind a load balancer, requests targeting the same `task_id` on different nodes are not serialized by the in-memory queue, but PostgreSQL's atomic conditional `UPDATE` still prevents any lost updates across all instances.
- **Crash Semantics:** Queued requests reside in volatile RAM until processed. A request is reported as `"SUCCESS"` to the HTTP client (and broadcast on `/ws`) **only after** PostgreSQL commits the transaction. If the server crashes while a request is still queued, the client never receives a `200 OK` response and the uncommitted update is never falsely reported as persisted.
