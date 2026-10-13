# ConcurrentHub Phase 2 — System Architecture

**Project ID:** OSDBMS-V-2026-T185  
**Team Name:** Team Nexus

---

## 1. High-Level Architecture Diagram

```mermaid
flowchart TD
    Client["HTTP / WebSocket Clients"]
    FastAPI["FastAPI Layer (backend/routes.py & schemas.py)"]
    Service["TaskService Integration Layer (backend/services.py)"]
    Scheduler["Per-Record FIFO Scheduler (scheduler/scheduler.py)"]
    QueueA["Record #1 FIFO Queue"]
    QueueB["Record #2 FIFO Queue"]
    QueueN["Record #N FIFO Queue"]
    ObjPool["BareHeap-Inspired BoundedObjectPool (resource_pool/object_pool.py)"]
    Repo["PostgresTaskRepository (database/repository.py)"]
    DBPool["Psycopg 3 ConnectionPool (database/postgres_db.py)"]
    Postgres[("PostgreSQL Database (ACID & Conditional UPDATE)")]
    WSManager["WebSocket ConnectionManager (realtime/connection_manager.py)"]

    Client -->|"REST POST / GET / PUT"| FastAPI
    Client <-->|"WebSocket /ws"| WSManager
    FastAPI --> Service
    Service -->|"Submit UpdateRequest"| Scheduler
    Scheduler --> QueueA
    Scheduler --> QueueB
    Scheduler --> QueueN
    QueueA -->|"Sequential Worker"| ObjPool
    QueueB -->|"Concurrent Worker"| ObjPool
    QueueN -->|"Concurrent Worker"| ObjPool
    ObjPool -->|"Lease ReusableUpdateBuffer"| Repo
    Repo --> DBPool
    DBPool -->|"Parameterized Atomic SQL"| Postgres
    Service -->|"Broadcast After Commit"| WSManager
```

---

## 2. Sequence Diagram: Successful Version-Checked Update

```mermaid
sequenceDiagram
    participant C as Client
    participant API as FastAPI (backend/)
    participant S as PerRecordScheduler (scheduler/)
    participant P as BoundedObjectPool (resource_pool/)
    participant R as PostgresTaskRepository (database/)
    participant DB as PostgreSQL
    participant WS as ConnectionManager (realtime/)

    C->>API: PUT /tasks/101 {"expected_version": 1, "title": "Complete project report", "user_id": "user_1"}
    API->>S: submit_update(UpdateRequest(task_id=101, expected_version=1, ...))
    Note over S: Enqueued in Record #101 FIFO queue;<br/>drained sequentially by Record #101 worker
    S->>P: lease() ReusableUpdateBuffer
    P-->>S: ReusableUpdateBuffer (slot_id=0)
    S->>R: update_task(task_id=101, expected_version=1, title=...)
    R->>DB: UPDATE tasks SET title=..., version=version+1 WHERE id=101 AND version=1 RETURNING ...
    DB-->>R: Row returned (id=101, version=2) + COMMIT
    R-->>S: UpdateResult(status="SUCCESS", task_id=101, new_version=2)
    S->>P: release() & reset() buffer
    S-->>API: UpdateResult(status="SUCCESS", new_version=2)
    API->>WS: broadcast({"event": "task_updated", "task_id": 101, "version": 2, "title": "Complete project report"})
    API-->>C: HTTP 200 OK {"status": "SUCCESS", "task_id": 101, "new_version": 2, "message": "Task updated successfully"}
```

---

## 3. Sequence Diagram: Stale-Version Conflict Rejection

```mermaid
sequenceDiagram
    participant C2 as Client 2 (Stale Version)
    participant API as FastAPI (backend/)
    participant S as PerRecordScheduler (scheduler/)
    participant R as PostgresTaskRepository (database/)
    participant DB as PostgreSQL
    participant WS as ConnectionManager (realtime/)

    C2->>API: PUT /tasks/101 {"expected_version": 1, "title": "Stale edit", "user_id": "user_2"}
    API->>S: submit_update(UpdateRequest(task_id=101, expected_version=1, ...))
    S->>R: update_task(task_id=101, expected_version=1, title="Stale edit")
    R->>DB: UPDATE tasks SET title=..., version=version+1 WHERE id=101 AND version=1 RETURNING ...
    DB-->>R: 0 rows returned (no modification occurred)
    R->>DB: SELECT id, version FROM tasks WHERE id=101
    DB-->>R: Row (id=101, version=2)
    R-->>S: UpdateResult(status="VERSION_CONFLICT", task_id=101, current_version=2)
    S-->>API: UpdateResult(status="VERSION_CONFLICT", current_version=2)
    API->>WS: broadcast({"event": "version_conflict", "task_id": 101, "expected_version": 1, "current_version": 2, "modified": false})
    API-->>C2: HTTP 409 Conflict {"status": "VERSION_CONFLICT", "task_id": 101, "current_version": 2, "message": "The task was modified by another request"}
```

---

## 4. Core Operating Systems (OS) Concepts Demonstrated

1. **Workload-Aware Scheduling:** Instead of treating all incoming requests as uniform tasks competing for a single lock, `PerRecordScheduler` partitions work by `task_id`.
2. **Per-Key FIFO Queues & Independent Progress:** Conflicting updates to the same record are serialized in a per-record `queue.Queue`, while updates to distinct record IDs run in parallel across worker threads in a `ThreadPoolExecutor`.
3. **Synchronization Primitives:** Fine-grained mutex locking (`threading.Lock`) protects the scheduler's bucket registry and the object pool's lease tracking set, while `asyncio.Lock` synchronizes WebSocket client registration.
4. **Bounded Resource Pooling:** `BoundedObjectPool` bounds the number of in-flight `ReusableUpdateBuffer` instances and recycles them across requests, preventing unbounded memory allocation bursts.

---

## 5. Core Database Management System (DBMS) Concepts Demonstrated

1. **ACID Transactions:** Every create and update operation runs inside a managed PostgreSQL transaction (`with pool.connection() as conn:`) that commits atomically on success or rolls back automatically on exception.
2. **Optimistic Concurrency Control (OCC):** Every task carries an integer `version` column initialized to `1`. Clients supply `expected_version` with every update.
3. **Atomic Conditional Updates (Lost-Update Prevention):**
   ```sql
   UPDATE tasks
   SET title = %(title)s,
       version = version + 1,
       updated_at = CURRENT_TIMESTAMP
   WHERE id = %(task_id)s
     AND version = %(expected_version)s
   RETURNING id, title, version, created_at, updated_at;
   ```
   PostgreSQL evaluates the `WHERE id = ... AND version = ...` predicate and increments `version` atomically while holding the row-level lock. The application queue reduces lock contention inside a single backend instance, while PostgreSQL remains the **final authority** guaranteeing zero lost updates even if multiple processes or external clients access the database simultaneously.
4. **Database Connection Pooling:** `PostgresPoolManager` reuses established PostgreSQL backend sessions via `psycopg_pool.ConnectionPool`, eliminating per-request TCP/TLS/authentication overhead.
