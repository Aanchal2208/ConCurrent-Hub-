# ConcurrentHub Phase 2 — Demonstration & Viva Guide

**Project ID:** OSDBMS-V-2026-T185  
**Team Name:** Team Nexus

This guide provides a step-by-step evaluation order so each of the four team members can demonstrate their module independently first, followed by the full integrated demonstration and performance benchmarks.

---

## Part 1: Independent Module Demonstrations (No Frontend Required)

Run all commands from the `ConcurrentHub/` root directory.

### 1. Member 1 — Aanchal Singh (`scheduler/`)

**Concept:** Per-record FIFO queues, sequential execution of conflicting updates, stale-version rejection, and concurrent progress across independent records.

```bash
python -m scheduler.demo_scheduler
pytest scheduler/test_scheduler.py -v
```

**What to point out during viva:**
- Show how `Request 1A` (`expected_version=1`) and `Request 1B` (`expected_version=1`) are queued in Record #1's FIFO queue: `1A` succeeds (`new_version=2`) and `1B` is rejected with `VERSION_CONFLICT` (`current_version=2`).
- Show how Record #1 and Record #2 pass a `threading.Barrier(2)` simultaneously on separate worker threads, proving independent records are never blocked by each other.

---

### 2. Member 2 — Parth Gauniyal (`resource_pool/` & `database/`)

**Concept:** BareHeap-inspired bounded object pool (`BoundedObjectPool` / `ReusableUpdateBuffer`) vs. Psycopg 3 PostgreSQL connection pool (`PostgresPoolManager`) and atomic conditional SQL updates.

```bash
# 2A. Demonstrate bounded object reuse, capacity exhaustion, and double-release protection
python -m resource_pool.pool_demo
python -m resource_pool.benchmark
pytest resource_pool/test_pool.py -v

# 2B. Initialize PostgreSQL (when Docker is running) and run database tests
docker compose up -d postgres
python -m database.init_db
pytest database/test_database.py -v
```

**What to point out during viva:**
- Explain the difference between reusing Python buffer objects in `resource_pool/object_pool.py` and reusing PostgreSQL TCP connections in `database/postgres_db.py`.
- Show `CONDITIONAL_UPDATE_TASK_SQL` in `database/repository.py` and explain why `WHERE id = %(task_id)s AND version = %(expected_version)s` is the atomic correctness guarantee in PostgreSQL.

---

### 3. Member 3 — Gargi Patra (`backend/`)

**Concept:** FastAPI REST API validation, scheduler-routed updates, HTTP status codes (`201`, `200`, `404`, `409`, `422`), and Swagger UI.

```bash
# Run automated API test suite
pytest backend/test_api.py -v

# Start the FastAPI backend server
uvicorn backend.main:app --reload
```

Then open `http://127.0.0.1:8000/docs` in a browser and execute:
1. `GET /health`
2. `POST /tasks` (`{"title": "Prepare project report", "user_id": "user_1"}`) -> `201 Created`
3. `PUT /tasks/1` (`{"expected_version": 1, "title": "Complete project report", "user_id": "user_1"}`) -> `200 OK` (`new_version: 2`)
4. `PUT /tasks/1` (`{"expected_version": 1, "title": "Stale update", "user_id": "user_2"}`) -> `409 Conflict` (`current_version: 2`)

---

### 4. Member 4 — Angel Odilo (`realtime/`)

**Concept:** Multi-client WebSocket connection management (`/ws`), post-commit `task_updated` broadcasts, and `version_conflict` notifications.

```bash
# Run standalone two-client WebSocket broadcast demo
python -m realtime.demo_realtime

# Run WebSocket automated tests
pytest realtime/test_websocket.py -v
```

**What to point out during viva:**
- Both Client A and Client B receive `task_created` and `task_updated` immediately after persistence commits.
- When a stale update is rejected (`409 Conflict`), `task_updated` is **not** broadcast; instead, clients receive `version_conflict` with `"modified": false`.

---

## Part 2: Comparative Performance Benchmark

```bash
python -m benchmarks.run_benchmarks
```

Compare `Baseline (Direct)` against `Scheduler-Enabled` across `low_contention`, `high_contention`, and `mixed_workload`, reviewing throughput (`Req/s`), tail latencies (`p50`, `p95`, `p99`), successful updates, and version conflicts.
