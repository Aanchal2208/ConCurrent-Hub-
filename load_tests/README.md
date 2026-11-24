# ConcurrentHub comparative benchmark suite

This suite compares two real PostgreSQL-backed paths without changing the app:

- **Baseline (Direct):** calls `PostgresTaskRepository.get_task()` and
  `PostgresTaskRepository.update_task()` directly. This bypasses the scheduler
  and object-pool path while retaining the application's real validation,
  parameterized SQL, version check, and conflict detection.
- **Scheduler-Enabled:** sends authenticated `GET /tasks/{task_id}` and
  `PUT /tasks/{task_id}` requests to the running FastAPI app. The PUT follows
  `TaskService.update_task_via_scheduler()` → `PerRecordScheduler` → the same
  `PostgresTaskRepository.update_task()` operation used by the direct mode.

The direct baseline is therefore not a second API endpoint or a simulated
scheduler. A preflight creates a task through the API and verifies that the
direct repository sees that exact task. The run aborts if the API does not
report PostgreSQL, the direct connection cannot reach PostgreSQL, or the two
paths do not share the same database.

## Requirements

- Python environment with the project's `requirements.txt` installed.
- PostgreSQL running with the same `.env` configuration used by ConcurrentHub.
- ConcurrentHub running locally on the machine being benchmarked. RSS is read
  from the Python process listening on the API port; remote hosts are rejected.

The project already declares `httpx`, `psutil`, `psycopg`, `psycopg-pool`, and
`python-dotenv`; no application dependency or source changes are required.

## Run the full benchmark

From the project root in PowerShell:

```powershell
.\.venv\Scripts\python.exe .\load_tests\benchmark.py
```

This runs all three workloads, both modes, and **10, 50, and 100 VUs** with
**100 update cycles per VU per workload/mode**. It writes one CSV, one detailed
JSON file, and one TXT report to `load_tests/results/`.

Example with a smaller, configurable run:

```powershell
.\.venv\Scripts\python.exe .\load_tests\benchmark.py --base-url http://127.0.0.1:8000 --vus 10 50 --iterations 20
```

Other supported settings:

```text
--timeout SECONDS
--sample-interval SECONDS
--health-path PATH
--register-path PATH
--login-path PATH
--tasks-path PATH
--task-path-template '/tasks/{task_id}'
```

The URL must point to a local interface on the machine running the benchmark.
For a valid backend RSS measurement, do not point it at a remote or production
server. Use a disposable development/test PostgreSQL database.

## Workloads

- **low_contention:** Each VU reads and updates its own task.
- **high_contention:** All VUs read one shared task, synchronize after the
  reads, then attempt an update with that same expected version. One current
  version can win; stale updates are expected HTTP 409 conflicts.
- **mixed_workload:** Even-numbered iterations use the shared task; odd-numbered
  iterations use each VU's own task (50% each).

Each update cycle consists of one task read and one update attempt. The same
number of VUs and cycles is used in each mode. Setup, login, and cleanup are not
included in timing metrics.

## Metric definitions

- **Done:** completed request/call attempts, including attempts that end in
  errors.
- **OK:** successful task reads and successful task updates.
- **Conf:** expected `VERSION_CONFLICT` responses/results from updates only.
- **Fail:** unexpected HTTP status, invalid response, repository error, or
  transport failure. A GET 409 is a failure, not a conflict.
- **Req/s:** Done divided by total wall-clock benchmark duration. Each GET and
  update counts as one request/call.
- **P50/P95/P99:** linearly interpolated percentiles over individual GET and
  update call latencies, in milliseconds.
- **RSS:** peak sampled resident memory (MiB) for the Python process(es)
  listening on the configured local API port. The benchmark runner's memory is
  never reported as backend RSS.

The Direct rows measure repository method latency; Scheduler-Enabled rows
measure HTTP request latency. The comparison intentionally shows the added
API, authentication, scheduler, and object-pool path. It is not a comparison
of two equally sized network stacks.

## Safety and cleanup

The suite creates a uniquely named, non-admin benchmark account and task rows.
It tracks only IDs returned by its own task-creation calls and deletes those
exact IDs using version-checked deletes after each case. It removes the
benchmark account's sessions and marks that exact account inactive. It never
deletes or updates unrelated tasks or users. Cleanup errors are printed and
recorded in the JSON/TXT report; they are not silently ignored.

Use only a local development/test database. Do not run against production.
The benchmark does not fall back to in-memory storage if PostgreSQL is
unavailable.

## Files

- `benchmark.py`: orchestrator and report writer.
- `results/`: generated CSV, JSON, and TXT reports.
- `concurrenthub.js`: existing k6 scenario; this suite does not modify it.
