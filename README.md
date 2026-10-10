ConcurrentHub — A Multi-User Collaborative Data Workspace (Phase 2)

Project ID: OSDBMS-V-2026-T185
Team Name: Team Nexus
Course Evaluation: Operating Systems (OS) & Database Management Systems (DBMS)

1. Project Overview

ConcurrentHub is a modular collaborative task-management backend built in Python that demonstrates how an application-level, workload-aware scheduler coordinates concurrent updates to database records alongside PostgreSQL optimistic concurrency control (OCC), bounded resource pooling, REST APIs, and real-time WebSockets.

Core Capabilities
MxTasks-Inspired Per-Record FIFO Scheduler (scheduler/):
Maintains a dedicated queue.Queue per active task_id.
Serializes conflicting updates targeting the same record in FIFO order while allowing independent record IDs to execute concurrently across worker threads.
Validates optimistic record versions (expected_version) and never silently drops accepted requests.
BareHeap-Inspired Object Pool & PostgreSQL Persistence (resource_pool/ & database/):
Clearly separates application-level buffer pooling (BoundedObjectPool / ReusableUpdateBuffer) from PostgreSQL connection pooling (psycopg_pool.ConnectionPool).
Enforces ACID transactions and zero lost updates using atomic conditional SQL (UPDATE tasks SET ... WHERE id = %(task_id)s AND version = %(expected_version)s RETURNING ...).
PostgreSQL remains the final authority for persistent data correctness.
FastAPI REST Backend (backend/):
Exposes GET /health, POST /tasks, GET /tasks, GET /tasks/{task_id}, PUT /tasks/{task_id}, and /docs (Swagger UI).
Routes all updates through the per-record scheduler and returns standardized JSON responses (201, 200, 404, 409, 422, 500/503).
Real-Time WebSocket Broadcasting (realtime/):
Manages multiple concurrent /ws clients, automatically prunes broken connections, and broadcasts task_created, task_updated (only after database commit), and version_conflict events.
2. Team Members & Module Ownership
Member	Primary Directories	Key Responsibilities
Member 1 — Aanchal Singh	scheduler/	Per-record FIFO queues, concurrent worker dispatch, optimistic version validation, conflict detector, standalone scheduler demo & unit tests.
Member 2 — Parth Gauniyal	resource_pool/, database/	BareHeap-inspired BoundedObjectPool, Psycopg 3 PostgreSQL connection pool, schema.sql, atomic conditional UPDATE repository, benchmarks & tests.
Member 3 — Gargi Patra	backend/	FastAPI application factory, Pydantic schemas, scheduler/repository service integration, error handling, API tests & Swagger UI demo.
Member 4 — Angel Odilo	realtime/	/ws WebSocket endpoint, multi-client ConnectionManager, post-commit event broadcasting, standalone two-client demo & WebSocket tests.
3. Repository Structure
text
ConcurrentHub/
├── README.md
├── requirements.txt
├── .gitignore
├── .env.example
├── docker-compose.yml
├── Dockerfile
├── pytest.ini
│
├── scheduler/
│   ├── __init__.py
│   ├── scheduler.py
│   ├── version_check.py
│   ├── conflict_detector.py
│   ├── demo_scheduler.py
│   ├── test_scheduler.py
│   └── README.md
│
├── resource_pool/
│   ├── __init__.py
│   ├── object_pool.py
│   ├── pool_demo.py
│   ├── benchmark.py
│   ├── test_pool.py
│   └── README.md
│
├── database/
│   ├── __init__.py
│   ├── postgres_db.py
│   ├── repository.py
│   ├── schema.sql
│   ├── init_db.py
│   ├── test_database.py
│   └── README.md
│
├── backend/
│   ├── __init__.py
│   ├── main.py
│   ├── routes.py
│   ├── schemas.py
│   ├── services.py
│   ├── test_api.py
│   └── README.md
│
├── realtime/
│   ├── __init__.py
│   ├── events.py
│   ├── connection_manager.py
│   ├── websocket_server.py
│   ├── demo_realtime.py
│   ├── test_websocket.py
│   └── README.md
│
├── docs/
│   ├── architecture.md
│   ├── api-contract.md
│   ├── concurrency-design.md
│   ├── testing-plan.md
│   └── phase2-demo-guide.md
│
└── benchmarks/
    ├── run_benchmarks.py
    └── README.md
4. Quick Start & Installation
4.1 Set Up Python Virtual Environment
bash
python -m venv .venv
# Windows PowerShell:
.\.venv\Scripts\Activate.ps1
# Linux / macOS / GitHub Codespaces:
# source .venv/bin/activate
pip install -r requirements.txt
4.2 Run Standalone Module Demonstrations (No Database Required)

Each team member can demonstrate their module independently:

bash
# Member 1 (Aanchal Singh) — Per-Record Scheduler Demo
python -m scheduler.demo_scheduler
# Member 2 (Parth Gauniyal) — Bounded Object Pool Demo & Benchmark
python -m resource_pool.pool_demo
python -m resource_pool.benchmark
# Member 3 (Gargi Patra) — FastAPI Backend & Swagger UI (/docs)
uvicorn backend.main:app --reload
# Member 4 (Angel Odilo) — Two-Client WebSocket Broadcast Demo
python -m realtime.demo_realtime
4.3 Start PostgreSQL & Run Full Integrated Mode

When Docker is available locally or in a GitHub Codespace:

bash
# 1. Start PostgreSQL 15 container
docker compose up -d postgres
# 2. Initialize schema and seed initial records
python -m database.init_db
# 3. Start the integrated FastAPI + Scheduler + PostgreSQL + WebSocket server
uvicorn backend.main:app --reload

Then open http://127.0.0.1:8000/docs for the interactive Swagger UI or http://127.0.0.1:8000/health to inspect live PostgreSQL pool, object pool, and scheduler metrics.

5. Running Automated Tests & Benchmarks
bash
# Run all unit, API, WebSocket, and database tests
pytest
# Run individual module test suites
pytest scheduler/test_scheduler.py -v
pytest resource_pool/test_pool.py -v
pytest database/test_database.py -v
pytest backend/test_api.py -v
pytest realtime/test_websocket.py -v
# Run comparative performance benchmark (Baseline vs Scheduler-Enabled)
python -m benchmarks.run_benchmarks
6. Design Scope & Honest Limitations
Educational MxTasks & BareHeap Inspiration: PerRecordScheduler models per-record FIFO conflict serialization at the Python application layer rather than low-level hardware task scheduling. BoundedObjectPool reuses Python buffer objects in managed memory and is kept strictly separate from psycopg_pool.ConnectionPool.
Single-Instance Queue Scope: In-memory queues coordinate requests within a single backend process. Across multiple processes, PostgreSQL's atomic conditional UPDATE remains the authoritative guard against lost updates.
No Cosmetic Auth: Authentication is outside the Phase 2 prototype scope (user_id is passed explicitly in request payloads).
Explicit Demo Fallback: If PostgreSQL is offline when uvicorn backend.main:app starts, the server logs an explicit warning and reports "storage_backend": "in_memory_demo" and "persisted_in_postgresql": false in GET /health.
