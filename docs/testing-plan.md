# ConcurrentHub Phase 2 — Testing Plan

**Project ID:** OSDBMS-V-2026-T185  
**Team Name:** Team Nexus

---

## 1. Testing Strategy Overview

Our testing suite is organized by module ownership so each team member can run their module's tests independently with `pytest`, and the entire project can be verified with a single root `pytest` command.

All concurrency tests use deterministic synchronization primitives (`threading.Event`, `threading.Barrier`, controlled callback handlers) rather than arbitrary `time.sleep()` delays.

---

## 2. Module Test Breakdown

### 2.1 Scheduler Tests (`scheduler/test_scheduler.py` — Aanchal Singh)

- `test_version_validation_helper`: Verifies matching `expected_version` advances version by 1 and stale versions are rejected.
- `test_create_and_update_record`: Verifies creating a task at version 1 and updating it to version 2.
- `test_two_sequential_updates_to_same_record`: Verifies chained updates (`v1 -> v2 -> v3`) on the same record.
- `test_stale_version_rejection_does_not_modify_record`: Verifies that when current version is 3, Request A (`expected=3`) succeeds (`v4`) and Request B (`expected=3`) is rejected without overwriting Request A's changes.
- `test_preserve_fifo_queue_order_for_same_record`: Uses a `threading.Event` gate to hold the first update while 4 additional updates are enqueued for the same record, verifying they execute in exact FIFO order (`step-1` through `step-5`).
- `test_different_record_ids_progress_independently`: Holds Record #1 inside its update handler via `threading.Event` and verifies that Record #2 completes immediately on a separate worker thread.
- `test_multiple_concurrent_requests_same_version_at_most_one_succeeds`: Releases 10 threads simultaneously via `threading.Barrier(10)` with `expected_version=1`; verifies exactly 1 succeeds and 9 return `VERSION_CONFLICT`.
- `test_handler_exception_and_shutdown_explicit_results`: Verifies that exceptions, missing records, and post-shutdown requests return explicit `UpdateResult` objects and idle queues are cleaned up.

### 2.2 Resource Pool Tests (`resource_pool/test_pool.py` — Parth Gauniyal)

- `test_pool_reuse_and_automatic_reset`: Verifies that a capacity-1 pool reuses the exact same `ReusableUpdateBuffer` instance and clears metadata upon release.
- `test_pool_capacity_exhaustion_and_unblocking`: Verifies `PoolExhaustedError` when all slots are leased and verifies that releasing a slot unblocks a waiting caller.
- `test_double_release_and_invalid_object_errors`: Verifies `PoolDoubleReleaseError` and `PoolInvalidObjectError`.
- `test_pool_close_cleanup`: Verifies `PoolClosedError` after closing the pool.

### 2.3 Database & Repository Tests (`database/test_database.py` — Parth Gauniyal)

- **Offline Unit Tests (always run):**
  - `test_repository_uses_parameterized_sql_and_Distinguishes_conflict_vs_not_found`: Verifies parameterized SQL execution and `VERSION_CONFLICT` vs `NOT_FOUND` classification.
  - `test_repository_releases_connection_when_exception_occurs`: Verifies pooled connections are always returned when an exception is raised.
- **Live PostgreSQL Integration Tests (run when PostgreSQL is reachable; explicitly reported as `SKIPPED` when PostgreSQL is offline):**
  - `test_postgres_schema_creation_and_create_read_task`
  - `test_postgres_successful_version_checked_update`
  - `test_postgres_reject_stale_version`
  - `test_postgres_two_concurrent_updates_same_expected_version`
  - `test_postgres_connection_pool_cleanup`

### 2.4 Backend API Tests (`backend/test_api.py` — Gargi Patra)

- `test_health_endpoint_reports_storage_mode_clearly`
- `test_create_get_list_and_update_task_lifecycle`
- `test_version_conflict_returns_409_and_does_not_modify_record`
- `test_missing_task_returns_404_and_invalid_payload_returns_422`
- `test_injected_custom_scheduler_and_database_error_handling`
- `test_integrated_http_and_websocket_event_ordering`

### 2.5 Real-Time WebSocket Tests (`realtime/test_websocket.py` — Angel Odilo)

- `test_event_builders_match_contract`
- `test_websocket_connect_ping_and_multi_client_broadcast`
- `test_disconnected_or_broken_client_is_removed_without_crashing_server`

---

## 3. Exact Commands to Execute Tests

```bash
# Run the entire test suite
pytest

# Run individual module test suites
pytest scheduler/test_scheduler.py -v
pytest resource_pool/test_pool.py -v
pytest database/test_database.py -v
pytest backend/test_api.py -v
pytest realtime/test_websocket.py -v
```
