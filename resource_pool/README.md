# Module 2A: BareHeap-Inspired Object Pool (`resource_pool/`)

**Owner:** Parth Gauniyal (Member 2)  
**Project:** ConcurrentHub — OSDBMS-V-2026-T185 (Team Nexus)

---

## 1. What This Module Does

The `resource_pool/` package implements a bounded, thread-safe application-level object pool (`BoundedObjectPool` and `ReusableUpdateBuffer`) used to stage and reuse request update buffers during concurrent execution.

### How This Relates to BareHeap (and How It Differs)

- **What is BareHeap-inspired:**
  - Pre-allocates a bounded set of fixed-size buffer objects (`ReusableUpdateBuffer`) at startup.
  - Enforces an explicit lease (`acquire` / `release` or `with pool.lease():`) lifecycle.
  - Zeroes/resets object state (`reset()`) on release so memory slots are safely reused across thousands of operations without re-allocating new wrapper objects per request.
- **What is distinct from the original BareHeap system:**
  - This component operates inside Python's managed heap (CPython garbage collector) rather than intercepting OS virtual-memory pages or replacing C `malloc`/`free`.
  - **Strict Separation from Database Connection Pooling:** An application-level object pool reuses in-memory Python data structures, whereas the PostgreSQL connection pool in [`database/postgres_db.py`](file:///c:/Users/Aanchal/OneDrive/Desktop/ConcurrentHub/database/postgres_db.py) manages network sockets and PostgreSQL backend sessions via `psycopg_pool.ConnectionPool`.

---

## 2. Files and Responsibilities

| File | Responsibility |
| :--- | :--- |
| [`__init__.py`](file:///c:/Users/Aanchal/OneDrive/Desktop/ConcurrentHub/resource_pool/__init__.py) | Public exports for `BoundedObjectPool` and `ReusableUpdateBuffer`. |
| [`object_pool.py`](file:///c:/Users/Aanchal/OneDrive/Desktop/ConcurrentHub/resource_pool/object_pool.py) | Thread-safe bounded object pool, double-release protection, automatic reset, and metrics. |
| [`pool_demo.py`](file:///c:/Users/Aanchal/OneDrive/Desktop/ConcurrentHub/resource_pool/pool_demo.py) | Standalone demonstration of buffer reuse, capacity exhaustion, and double-release detection. |
| [`benchmark.py`](file:///c:/Users/Aanchal/OneDrive/Desktop/ConcurrentHub/resource_pool/benchmark.py) | Single-threaded and multi-threaded object pool reuse benchmark. |
| [`test_pool.py`](file:///c:/Users/Aanchal/OneDrive/Desktop/ConcurrentHub/resource_pool/test_pool.py) | Unit tests for reuse, capacity limits, state reset, double release, and pool cleanup. |

---

## 3. Exact Run & Test Commands

Run from the `ConcurrentHub/` root directory:

```bash
# Run standalone object pool demonstration
python -m resource_pool.pool_demo

# Run object pool benchmark
python -m resource_pool.benchmark

# Run object pool unit tests
pytest resource_pool/test_pool.py -v
```

---

## 4. Sample Output (`python -m resource_pool.pool_demo`)

```text
========================================================================
ConcurrentHub Phase 2 — Standalone Object Pool Demonstration
Owner: Parth Gauniyal (Member 2)
========================================================================

[Step 1] Initialized BoundedObjectPool with capacity=2:
  -> Initial stats: {'capacity': 2, 'allocated_objects': 2, 'in_use': 0, 'available': 2, 'total_acquisitions': 0, 'reused_acquisitions': 0, 'reuse_ratio': 0.0, 'closed': False}

[Step 2] Acquiring and releasing a buffer across 4 sequential operations:
  -> Operation 1: leased slot_id=0, reuse_count=0, metadata={'task_id': 101, 'expected_version': 1, 'title': 'Update #1', 'user_id': 'parth'}
  -> Operation 2: leased slot_id=1, reuse_count=0, metadata={'task_id': 101, 'expected_version': 2, 'title': 'Update #2', 'user_id': 'parth'}
  -> Operation 3: leased slot_id=0, reuse_count=1, metadata={'task_id': 101, 'expected_version': 3, 'title': 'Update #3', 'user_id': 'parth'}
  -> Operation 4: leased slot_id=1, reuse_count=1, metadata={'task_id': 101, 'expected_version': 4, 'title': 'Update #4', 'user_id': 'parth'}

[Step 3] Demonstrating bounded capacity enforcement (capacity=2):
  -> Acquired 2 buffers (slot 0 and slot 1).
  -> Attempting 3rd concurrent acquire with timeout=0.1s...
  -> Expected PoolExhaustedError caught: Object pool exhausted (capacity=2); no object became available.

[Step 4] Demonstrating double-release prevention:
  -> Expected PoolDoubleReleaseError caught: Attempted to release an object that is already in the pool (double release).
```

---

## 5. Current Limitations & Integration Status

- Python integers, strings, and dictionaries allocated by FastAPI/Pydantic during HTTP parsing are still managed by CPython's memory allocator; `BoundedObjectPool` specifically reuses pre-allocated `ReusableUpdateBuffer` instances inside the service layer.
- **Integration Status:** Integrated with `backend/services.py` (which leases a `ReusableUpdateBuffer` during scheduled task updates) and benchmarked in `resource_pool/benchmark.py`.
