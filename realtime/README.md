# Real-Time WebSockets & Event Broadcasting

---

## 1. What This Module Does

The `realtime/` package provides real-time WebSocket event delivery at `/ws`:

- **Multi-Client Connection Management (`connection_manager.py`):** Tracks connected WebSocket clients safely using an `asyncio.Lock` and prunes dead/disconnected sockets automatically during broadcasts so a broken client never crashes the server.
- **Standardized Event Schema (`events.py`):** Builds structured JSON payloads for `task_created`, `task_updated`, `version_conflict`, and `server_error`.
- **Commit-Ordered Publishing:** In the integrated backend (`backend/services.py`), `task_updated` is broadcast **only after** the scheduler finishes and the database transaction has committed. Rejected stale updates never emit `task_updated`; they emit `version_conflict` with `"modified": false`.

---

## 2. Files and Responsibilities

| File | Responsibility |
| :--- | :--- |
| [`__init__.py`](ConcurrentHub/realtime/__init__.py) | Package exports for `ConnectionManager`, event builders, and router factory. |
| [`events.py`](ConcurrentHub/realtime/events.py) | `EventType` constants and JSON payload builders (`build_task_created_event`, `build_task_updated_event`, `build_version_conflict_event`). |
| [`connection_manager.py`](ConcurrentHub/realtime/connection_manager.py) | `ConnectionManager` supporting `connect`, `disconnect`, and resilient `broadcast`. |
| [`websocket_server.py`](ConcurrentHub/realtime/websocket_server.py) | FastAPI `APIRouter` providing `/ws` (mounted in `backend/main.py`) and standalone test app factory. |
| [`demo_realtime.py`](ConcurrentHub/realtime/demo_realtime.py) | Standalone two-client demonstration script (supports both self-contained and live server modes). |
| [`test_websocket.py`](ConcurrentHub/realtime/test_websocket.py) | Automated tests for multi-client broadcasts, disconnect cleanup, and event schemas. |

---

## 3. Exact Run & Test Commands

Run from the `ConcurrentHub/` root directory:

```bash
# Run self-contained two-client broadcast demonstration (no frontend or external server needed)
python -m realtime.demo_realtime

# Or run against a live Uvicorn backend (start `uvicorn backend.main:app --reload` in Terminal 1 first):
python -m realtime.demo_realtime --live

# Run WebSocket unit tests
pytest realtime/test_websocket.py -v
```

---

## 4. Sample Output (`python -m realtime.demo_realtime`)

```text
========================================================================
ConcurrentHub Phase 2 — Two-Client WebSocket Broadcast Demo
Owner: Angel Odilo (Member 4)
========================================================================

[Step 1] Connected Client A and Client B to `/ws`.

[Step 2] Sending POST /tasks {'title': 'Prepare project report', 'user_id': 'user_1'}...
  -> HTTP 201: {'id': 1, 'title': 'Prepare project report', 'version': 1, ...}
  -> [WS Client A received]: {"event": "task_created", "task_id": 1, "version": 1, "title": "Prepare project report", "user_id": "user_1"}
  -> [WS Client B received]: {"event": "task_created", "task_id": 1, "version": 1, "title": "Prepare project report", "user_id": "user_1"}

[Step 3] Sending PUT /tasks/1 (expected_version=1 -> valid update)...
  -> HTTP 200: {'status': 'SUCCESS', 'task_id': 1, 'new_version': 2, 'message': 'Task updated successfully'}
  -> [WS Client A received]: {"event": "task_updated", "task_id": 1, "version": 2, "title": "Complete project report", "user_id": "user_1"}
  -> [WS Client B received]: {"event": "task_updated", "task_id": 1, "version": 2, "title": "Complete project report", "user_id": "user_1"}

[Step 4] Sending PUT /tasks/1 (expected_version=1 -> stale version conflict)...
  -> HTTP 409: {'status': 'VERSION_CONFLICT', 'task_id': 1, 'current_version': 2, 'message': 'The task was modified by another request'}
  -> [WS Client A received]: {"event": "version_conflict", "task_id": 1, "expected_version": 1, "current_version": 2, "modified": false, "message": "The task was modified by another request"}
  -> [WS Client B received]: {"event": "version_conflict", "task_id": 1, "expected_version": 1, "current_version": 2, "modified": false, "message": "The task was modified by another request"}
```

---

## 5. Current Limitations & Integration Status

- Broadcasts are delivered to clients connected to the single running FastAPI process (in-memory `ConnectionManager`).
- **Integration Status:** Mounted directly into `backend/main.py` at `/ws` so HTTP endpoints and WebSocket clients share a single `ConnectionManager` instance.