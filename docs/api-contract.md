# ConcurrentHub Phase 2 — Shared API & WebSocket Contract

**Project ID:** OSDBMS-V-2026-T185  
**Team Name:** Team Nexus

> **Scope Note on Authentication:** Phase 2 focuses on concurrent scheduling, resource pooling, optimistic concurrency control in PostgreSQL, and real-time WebSocket broadcasting. Cosmetic authentication or role-based access control is intentionally omitted from this prototype; requests accept a plain `user_id` string for attribution.

---

## 1. REST API Endpoints

### 1.1 `GET /health`

Returns current service health, active storage mode (`"postgres"` vs `"in_memory_demo"`), scheduler queue metrics, object pool metrics, and active WebSocket count.

**Response (`200 OK`):**
```json
{
  "status": "ok",
  "storage_backend": "postgres",
  "persisted_in_postgresql": true,
  "active_websocket_clients": 2,
  "scheduler": {
    "closed": false,
    "max_workers": 8,
    "active_queue_count": 0,
    "contention_summary": {
      "total_records_seen": 1,
      "total_enqueued": 2,
      "total_contended_enqueues": 0,
      "total_version_conflicts": 1,
      "total_successful_updates": 1,
      "per_record": {}
    }
  },
  "object_pool": {
    "capacity": 64,
    "allocated_objects": 64,
    "in_use": 0,
    "available": 64,
    "total_acquisitions": 2,
    "reused_acquisitions": 0,
    "reuse_ratio": 0.0,
    "closed": false
  },
  "database_pool": {
    "min_size": 2,
    "max_size": 10,
    "timeout_seconds": 5.0,
    "is_open": true,
    "closed": false,
    "driver": "psycopg3 + psycopg_pool"
  }
}
```

---

### 1.2 `POST /tasks` — Create Task

**Request Body:**
```json
{
  "title": "Prepare project report",
  "user_id": "user_1"
}
```

**Success Response (`201 Created`):**
```json
{
  "id": 1,
  "title": "Prepare project report",
  "version": 1,
  "user_id": "user_1",
  "created_at": "2026-10-10T10:20:00+00:00",
  "updated_at": "2026-10-10T10:20:00+00:00"
}
```

---

### 1.3 `GET /tasks/{task_id}` — Retrieve Task by ID

**Success Response (`200 OK`):**
```json
{
  "id": 1,
  "title": "Prepare project report",
  "version": 1,
  "user_id": "user_1",
  "created_at": "2026-10-10T10:20:00+00:00",
  "updated_at": "2026-10-10T10:20:00+00:00"
}
```

**Missing Task Response (`404 Not Found`):**
```json
{
  "status": "NOT_FOUND",
  "task_id": 999,
  "message": "Task 999 not found"
}
```

---

### 1.4 `GET /tasks` — List Tasks

**Success Response (`200 OK`):**
Returns a JSON array of task objects ordered by ascending `id`.

---

### 1.5 `PUT /tasks/{task_id}` — Version-Checked Scheduled Update

**Request Body:**
```json
{
  "expected_version": 1,
  "title": "Complete project report",
  "user_id": "user_1"
}
```

**Success Response (`200 OK`):**
```json
{
  "status": "SUCCESS",
  "task_id": 1,
  "new_version": 2,
  "message": "Task updated successfully",
  "task": {
    "id": 1,
    "title": "Complete project report",
    "version": 2,
    "user_id": "user_1",
    "created_at": "2026-10-10T10:20:00+00:00",
    "updated_at": "2026-10-10T10:21:00+00:00"
  }
}
```

**Version Conflict Response (`409 Conflict`):**
```json
{
  "status": "VERSION_CONFLICT",
  "task_id": 1,
  "current_version": 2,
  "message": "The task was modified by another request"
}
```

**Missing Record Response (`404 Not Found`):**
```json
{
  "status": "NOT_FOUND",
  "task_id": 999,
  "message": "Task 999 not found"
}
```

**Validation Error Response (`422 Unprocessable Entity`):**
```json
{
  "status": "VALIDATION_ERROR",
  "message": "Invalid request payload",
  "details": [...]
}
```

---

## 2. WebSocket Endpoint (`/ws`) Event Schemas

Clients connect to `ws://<host>:<port>/ws`. Events are broadcast as JSON objects:

### 2.1 `task_created`
```json
{
  "event": "task_created",
  "task_id": 1,
  "version": 1,
  "title": "Prepare project report",
  "user_id": "user_1"
}
```

### 2.2 `task_updated` (Broadcast ONLY after DB transaction commits)
```json
{
  "event": "task_updated",
  "task_id": 1,
  "version": 2,
  "title": "Complete project report",
  "user_id": "user_1"
}
```

### 2.3 `version_conflict` (Record was NOT modified)
```json
{
  "event": "version_conflict",
  "task_id": 1,
  "expected_version": 1,
  "current_version": 2,
  "modified": false,
  "message": "The task was modified by another request"
}
```
