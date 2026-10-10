# Module 1: Custom Per-Record Update Scheduler (`scheduler/`)

**Owner:** Aanchal Singh (Member 1)  
**Project:** ConcurrentHub — OSDBMS-V-2026-T185 (Team Nexus)

---

## 1. What This Module Does

The `scheduler/` package implements an application-level, workload-aware update scheduler inspired by the MxTasks execution model:

- **Per-Record FIFO Queues:** Each active `task_id` is assigned its own thread-safe `queue.Queue` bucket.
- **Sequential Conflict Serialization:** Updates targeting the **same** `task_id` are processed one at a time in strict FIFO order.
- **Independent Record Parallelism:** Updates targeting **different** `task_id`s are dispatched concurrently across a bounded `ThreadPoolExecutor`.
- **Optimistic Version Checking:** Validates `expected_version` against the record's `current_version` so stale writes are rejected without modifying the record.
- **Standalone & Integrated Modes:** Includes `InMemoryTaskStore` for standalone demonstrations and unit testing without PostgreSQL, while accepting any repository callback (`Callable[[UpdateRequest], UpdateResult]`) when connected to PostgreSQL.

> **Important Note:** This is an educational application-level scheduler inspired by the MxTasks concept, not a reproduction of the low-level C++ MxTasks hardware-conscious runtime. In integrated mode, PostgreSQL remains the final authority for ACID transactions and atomic version validation.

---

## 2. Files and Responsibilities

| File | Responsibility |
| :--- | :--- |
| [`__init__.py`](file:///c:/Users/Aanchal/OneDrive/Desktop/ConcurrentHub/scheduler/__init__.py) | Public interface exported to `backend/` and tests. |
| [`scheduler.py`](file:///c:/Users/Aanchal/OneDrive/Desktop/ConcurrentHub/scheduler/scheduler.py) | `PerRecordScheduler`, `InMemoryTaskStore`, `UpdateRequest`, `UpdateResult`, `UpdateStatus`. |
| [`version_check.py`](file:///c:/Users/Aanchal/OneDrive/Desktop/ConcurrentHub/scheduler/version_check.py) | Reusable optimistic concurrency control functions (`validate_version`, `check_version_match`). |
| [`conflict_detector.py`](file:///c:/Users/Aanchal/OneDrive/Desktop/ConcurrentHub/scheduler/conflict_detector.py) | `ConflictDetector` for tracking queue contention and version conflict statistics safely. |
| [`demo_scheduler.py`](file:///c:/Users/Aanchal/OneDrive/Desktop/ConcurrentHub/scheduler/demo_scheduler.py) | Standalone demonstration script requiring no database. |
| [`test_scheduler.py`](file:///c:/Users/Aanchal/OneDrive/Desktop/ConcurrentHub/scheduler/test_scheduler.py) | Deterministic Pytest suite covering FIFO ordering, stale version rejection, parallel progress, and worker cleanup. |

---

## 3. Dependencies

Uses only Python's standard library (`queue`, `threading`, `concurrent.futures`, `dataclasses`, `copy`, `datetime`) for runtime execution, plus `pytest` for unit tests.

---

## 4. Exact Run & Test Commands

Run from the `ConcurrentHub/` project root:

```bash
# Run standalone demonstration (no PostgreSQL required)
python -m scheduler.demo_scheduler

# Run scheduler unit tests
pytest scheduler/test_scheduler.py -v
```

---

## 5. Sample Output (`python -m scheduler.demo_scheduler`)

```text
========================================================================
ConcurrentHub Phase 2 — Standalone Per-Record Scheduler Demo
Owner: Aanchal Singh (Member 1)
========================================================================

[Step 1] Created initial tasks in standalone store:
  -> Task #1: title='Design OS scheduler queues', version=1
  -> Task #2: title='Configure PostgreSQL pool', version=1
  -> Task #3: title='Build FastAPI endpoints', version=1

[Step 2] Submitting two competing updates to Task #1 (both expecting version 1):
  -> Request 1A (expects v1): {'status': 'SUCCESS', 'task_id': 1, 'new_version': 2, 'message': 'Task updated successfully', ...}
  -> Request 1B (expects stale v1): {'status': 'VERSION_CONFLICT', 'task_id': 1, 'current_version': 2, 'message': 'The task was modified by another request'}
  -> Request 1C (expects v2): {'status': 'SUCCESS', 'task_id': 1, 'new_version': 3, 'message': 'Task updated successfully', ...}

[Step 3] Demonstrating concurrent progress across independent records (#2 and #3):
  -> Executed concurrently: task_id=1 on record-scheduler-worker_0
  -> Executed concurrently: task_id=2 on record-scheduler-worker_1
  -> Record #1 outcome: status=SUCCESS, new_version=2
  -> Record #2 outcome: status=SUCCESS, new_version=2

[Step 4] Scheduler Contention & Queue Snapshot:
  -> Active queues after drain: 0
  -> Total requests enqueued:   3
  -> Successful updates:        2
  -> Version conflicts caught:  1

[Done] Scheduler shut down cleanly.
========================================================================
```

---

## 6. Current Limitations & Integration Status

- **Single-Process Scope:** In-memory queues coordinate updates within a single backend process. Multi-node deployments still rely on PostgreSQL's atomic conditional `UPDATE ... WHERE version = expected_version` for cross-instance correctness.
- **Volatile Queue State:** Unprocessed items sitting in memory when the OS process crashes are not persisted; a client only treats an update as committed after receiving HTTP `200 OK` following PostgreSQL transaction commit.
- **Integration Status:** Fully integrated with `backend/services.py` and `database/repository.py`.
