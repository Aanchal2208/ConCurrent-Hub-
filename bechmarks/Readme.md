# Performance Benchmarks (`benchmarks/`)

**Project:** ConcurrentHub — OSDBMS-V-2026-T185 (Team Nexus)

---

## 1. Purpose

`benchmarks/run_benchmarks.py` empirically compares two execution paths under identical concurrent workloads:

1. **Baseline (`Baseline (Direct)`):** Worker threads call the repository's version-checked `update_task` directly without application-level per-record queuing.
2. **Scheduler-Enabled (`Scheduler-Enabled`):** Worker threads submit `UpdateRequest` objects to `PerRecordScheduler`, which serializes updates targeting the same `task_id` in a FIFO `queue.Queue`, leases a `ReusableUpdateBuffer` from `BoundedObjectPool`, and invokes the repository.

---

## 2. Workloads Evaluated

- **`low_contention`:** 200 concurrent requests distributed across 40 distinct task IDs. Independent record queues run concurrently across worker threads.
- **`high_contention`:** 200 concurrent requests all targeting a single hot record (`task_id = 1`). Demonstrates conflict serialization and optimistic version-conflict rejection when multiple clients submit the same `expected_version`.
- **`mixed_workload`:** 200 concurrent requests where 70% target a hot record (`task_id = 1`) and 30% target independent records (`task_id = 2..40`).

---

## 3. Metrics Collected

- `completed_requests`: Total requests finished.
- `successful_updates`: Requests that matched `expected_version` and incremented the record version.
- `version_conflicts`: Requests rejected with `VERSION_CONFLICT` because a concurrent request already advanced the version.
- `failed_requests`: Unexpected errors (`0` expected in healthy runs).
- `throughput_req_per_sec`: Completed requests per second.
- `p50_ms`, `p95_ms`, `p99_ms`: End-to-end latency percentiles in milliseconds.
- `cpu_percent` and `rss_memory_mb`: Measured via `psutil` (or labeled `"unavailable"` if `psutil` is not installed).
- Database connection pool configuration (`min_size`, `max_size`, driver) when PostgreSQL is reachable, or explicitly labeled fallback when PostgreSQL is offline.

---

## 4. How to Run

From the `ConcurrentHub/` root directory:

```bash
# Run with PostgreSQL (if Docker is available)
docker compose up -d postgres
python -m database.init_db
python -m benchmarks.run_benchmarks

# Or run directly in offline fallback mode (when PostgreSQL is not running)
python -m benchmarks.run_benchmarks
```
