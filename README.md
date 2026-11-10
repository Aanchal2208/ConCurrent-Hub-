# ConcurrentHub

**OS & DBMS Project — Team Nexus (OSDBMS-V-2026-T185)**

A production-quality multi-user collaborative task management application demonstrating:

- **Workload-aware per-record scheduling** (FIFO queue per task ID — serializes conflicting updates, allows independent tasks to progress concurrently)
- **Optimistic concurrency control** (atomic `WHERE id = ? AND version = ?` SQL — prevents lost updates without locks)
- **Resource/object pooling** (32 pre-allocated reusable buffers — eliminates allocation overhead)
- **PostgreSQL persistence** (the only data store — no in-memory fallback)
- **Real-time WebSocket broadcasts** (all connected users see updates instantly)
- **Full authentication** (PostgreSQL-backed user accounts, PBKDF2 password hashing, HMAC-signed tokens)

---

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.11 or 3.12 recommended (3.14 works) |
| PostgreSQL | 14 or later |

> **Docker is NOT required.** Everything runs natively.

---

## 1 — PostgreSQL Setup (one-time)

### Install PostgreSQL
Download from [postgresql.org/download/windows](https://www.postgresql.org/download/windows/) and install.

### Automatic project database setup (recommended)
After completing the project setup below, run:
```powershell
.\.venv\Scripts\python.exe .\setup_database.py
```
Enter the `postgres` password chosen during installation in the masked password
window. This creates `concurrenthub` if needed and updates `.env` with the correct,
URL-encoded connection. You do not need to edit the URL by hand.

### Create the database
Open **SQL Shell (psql)** from the Windows Start menu. Press Enter to accept
`localhost`, port `5432`, and database `postgres`; enter username `postgres`
and the password you chose during PostgreSQL installation. At the `postgres=#`
prompt, run:
```sql
CREATE DATABASE concurrenthub;
```

### Configure the project connection
Open the project's `.env` file and set `DATABASE_URL` to use the password you
chose during installation. Do not share that password:
```
DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@localhost:5432/concurrenthub
```
Replace `YOUR_PASSWORD` with your actual password. If it contains URL-reserved
characters such as `@`, `:`, `/`, `#`, or `%`, URL-encode those characters.

If you want a dedicated user:
```sql
CREATE USER concurrenthub WITH PASSWORD 'yourpassword';
GRANT ALL PRIVILEGES ON DATABASE concurrenthub TO concurrenthub;
```

---

## 2 — Project Setup

```powershell
# Open a terminal in this folder
cd C:\Users\Aanchal\OneDrive\Desktop\CC

# Create virtual environment
python -m venv .venv
.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt

# Configure PostgreSQL and .env
.\.venv\Scripts\python.exe .\setup_database.py
```

### Edit `.env`
```
DATABASE_URL=postgresql://postgres:yourpassword@localhost:5432/concurrenthub
AUTH_SECRET_KEY=replace-with-a-long-random-string-at-least-32-chars
ALLOWED_ORIGINS=http://localhost:8000,http://127.0.0.1:8000
```

---

## 3 — Run the Application

```powershell
cd C:\Users\Aanchal\OneDrive\Desktop\CC
.\start.ps1
```

Open your browser: **http://localhost:8000**

The schema (users, tasks, audit_logs) is created automatically on first startup.
If you serve `frontend/` through VS Code Live Server instead, its API and
WebSocket requests automatically target the backend on port 8000. The default
allowed origins include Live Server on port 5500.

> **If PostgreSQL is not running**, the application will print a clear error and exit — there is no silent in-memory fallback.
> The application loads `.env` from the project root automatically.

---

## 4 — Multi-Laptop Demo (Same Network)

1. Connect both devices to the same trusted Wi-Fi. Keep the terminal running
   `.\start.ps1` open on the computer hosting the app.
2. On the host, run `ipconfig` in PowerShell and find the Wi-Fi adapter's
   **IPv4 Address**.
3. If Windows marks this trusted home/lab Wi-Fi as a **Public** network, change
   its profile to **Private** in **Settings → Network & internet → Wi-Fi →
   [your connected network] → Network profile type**. Do not do this on public
   or untrusted Wi-Fi.
4. If Windows Firewall prompts about Python, allow it on **Private networks
   only**. Do not expose port 8000 to public networks.
5. On each device, open `http://<host-ip>:8000` (for example,
   `http://192.168.1.42:8000`). No `.env` or CORS change is needed when using
   this same-origin app URL.
6. Register/sign in on both devices. Create a task on either device and wait
   for it to appear on the other.
7. To show a version conflict, open that same task for editing on both devices
   before either saves. Save a change on device A, then save the still-open
   edit form on device B. Device B should report a **Version conflict** (HTTP
   409), because it submitted the old task version. Successful changes also
   appear in the live activity feed and refresh the task list over WebSockets.

The host IP can change after reconnecting to Wi-Fi; run `ipconfig` again if
the other device cannot connect. Some guest Wi-Fi networks block device-to-device
connections even when both devices have internet access.

---

## 5 — Running Tests

Tests that need PostgreSQL (integration):
```powershell
pytest tests/test_app.py -v
```

Tests that are pure Python (no database):
```powershell
pytest tests/test_scheduler.py tests/test_pool.py -v
```

All tests:
```powershell
pytest -v
```

---

## 6 — API Reference

| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/auth/register` | No | Register a new user |
| POST | `/auth/login` | No | Log in, receive token |
| GET | `/auth/me` | Yes | Current user profile |
| GET | `/auth/users` | Admin | List all users |
| GET | `/health` | No | System health |
| GET | `/metrics/scheduler` | Yes | Scheduler live metrics |
| GET | `/metrics/pool` | Yes | Object pool live metrics |
| GET | `/audit` | Yes | Audit log |
| GET | `/tasks` | Yes | List tasks (filter/search/sort) |
| POST | `/tasks` | Yes | Create a task |
| GET | `/tasks/{id}` | Yes | Get one task |
| PUT | `/tasks/{id}` | Yes | Update task (OCC — may return 409) |
| DELETE | `/tasks/{id}` | Yes | Delete task |

Interactive docs: **http://localhost:8000/api/docs**

---

## 7 — Concurrency Concepts Demonstrated

### Optimistic Concurrency Control (OCC)
Every task has a `version` column. Updates use:
```sql
UPDATE tasks SET ..., version = version + 1
WHERE id = $1 AND version = $expected_version
RETURNING ...;
```
If another user updated the row first, `version` changed and this UPDATE affects 0 rows → HTTP 409 Conflict is returned to the client, who must refresh and retry.

### Per-Record Scheduler
Each task ID gets its own FIFO queue. The `PerRecordScheduler` dispatches updates through a thread pool of 8 workers — conflicting updates to the same task are serialized; independent tasks progress in parallel.

### Object Pool
`BoundedObjectPool` pre-allocates 32 `ReusableUpdateBuffer` slots. Each scheduled update leases a buffer, stages its payload, calls the repository, and returns the buffer — no allocation during hot path.

### WebSocket Broadcast
After every successful create, update, or delete, `ConnectionManager.broadcast()` sends a JSON event to all connected browsers in real time.

---

## Project Structure

```
CC/
├── backend/
│   ├── main.py        # FastAPI app factory + lifespan
│   ├── routes.py      # All HTTP route handlers
│   ├── services.py    # TaskService (scheduler + pool + repo + WS)
│   ├── auth.py        # UserRepository, token helpers
│   └── schemas.py     # Pydantic request/response models
├── database/
│   ├── postgres_db.py # PostgresPoolManager (psycopg3)
│   ├── repository.py  # PostgresTaskRepository (CRUD + OCC)
│   └── schema.sql     # CREATE TABLE statements
├── scheduler/
│   ├── scheduler.py   # PerRecordScheduler + InMemoryTaskStore
│   ├── conflict_detector.py
│   └── version_check.py
├── resource_pool/
│   └── object_pool.py # BoundedObjectPool + ReusableUpdateBuffer
├── realtime/
│   ├── connection_manager.py
│   ├── events.py
│   └── websocket_server.py
├── frontend/
│   ├── index.html     # Single-page application
│   ├── style.css
│   └── app.js
├── tests/
│   ├── test_app.py    # Integration tests (needs PostgreSQL)
│   ├── test_scheduler.py
│   └── test_pool.py
├── requirements.txt
├── pytest.ini
├── .env.example
└── README.md
```

---

## Team

**Team Nexus** — Project ID: OSDBMS-V-2026-T185
