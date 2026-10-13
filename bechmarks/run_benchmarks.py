"""
benchmarks/run_benchmarks.py
Project: ConcurrentHub Phase 2 — OSDBMS-V-2026-T185 (Team Nexus)

Comparative benchmark evaluating:
  1. Baseline: Direct version-checked repository updates without application-level
     per-record scheduling.
  2. Scheduler-Enabled: Updates routed through `PerRecordScheduler` and `BoundedObjectPool`
     before invoking the same repository.

Workloads tested (with identical request sequences for both modes):
  - Low contention: 200 requests distributed across 50 independent records.
  - High contention: 200 concurrent requests targeting a single hot record (Record #1).
  - Mixed workload: 200 concurrent requests where 70% target a hot record (#1) and
    30% target independent records (#2..#30).

Note:
- Automatically uses PostgreSQL when reachable (`PostgresTaskRepository`), or
  clearly labels when running against `InMemoryTaskStore` if PostgreSQL is offline.
- Never hardcodes or fabricates metrics. All reported numbers are measured live.

Run command:
    python -m benchmarks.run_benchmarks
"""

from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Tuple

try:
    import psutil
except ImportError:
    psutil = None  # type: ignore[assignment]

from database.postgres_db import PostgresPoolManager
from database.repository import PostgresTaskRepository
from resource_pool.object_pool import BoundedObjectPool, ReusableUpdateBuffer
from scheduler.scheduler import (
    InMemoryTaskStore,
    PerRecordScheduler,
    UpdateRequest,
    UpdateResult,
    UpdateStatus,
)


def _percentile(sorted_values: List[float], fraction: float) -> float:
    if not sorted_values:
        return 0.0
    idx = min(len(sorted_values) - 1, max(0, int(round(fraction * (len(sorted_values) - 1)))))
    return sorted_values[idx]


def _generate_workload(
    workload_name: str,
    total_requests: int,
    num_records: int,
) -> List[Tuple[int, str]]:
    """
    Generate a deterministic list of `(record_index_1_based, title)` pairs so
    Baseline and Scheduler-Enabled runs execute the exact same workload.
    """
    plan: List[Tuple[int, str]] = []
    for i in range(total_requests):
        if workload_name == "low_contention":
            rec_idx = (i % num_records) + 1
        elif workload_name == "high_contention":
            rec_idx = 1
        elif workload_name == "mixed_workload":
            # 70% target hot record #1, 30% target independent records #2..num_records
            if (i % 10) < 7:
                rec_idx = 1
            else:
                rec_idx = ((i % max(1, num_records - 1)) + 2)
        else:
            raise ValueError(f"Unknown workload: {workload_name}")
        plan.append((rec_idx, f"{workload_name}-req-{i}"))
    return plan


def _run_single_experiment(
    mode_label: str,
    workload_name: str,
    plan: List[Tuple[int, str]],
    record_id_map: Dict[int, int],
    get_current_version_fn: Callable[[int], int],
    execute_update_fn: Callable[[UpdateRequest], UpdateResult],
    concurrency: int = 16,
) -> Dict[str, Any]:
    """
    Execute a workload using `concurrency` client threads.
    Each worker reads the current version of its target record right before
    submitting an update, reflecting realistic concurrent clients that may
    either succeed or experience legitimate optimistic version conflicts under contention.
    """
    latencies_ms: List[float] = []
    statuses: List[str] = []
    lock = threading.Lock()

    proc = psutil.Process(os.getpid()) if psutil is not None else None
    cpu_start = proc.cpu_times() if proc is not None else None
    t_start = time.perf_counter()

    def client_task(req: UpdateRequest, barrier: threading.Barrier) -> None:
        barrier.wait()
        t0 = time.perf_counter()
        try:
            res = execute_update_fn(req)
            status_code = res.status
        except Exception:
            status_code = UpdateStatus.ERROR
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        with lock:
            latencies_ms.append(elapsed_ms)
            statuses.append(status_code)

    with ThreadPoolExecutor(max_workers=concurrency) as client_pool:
        for offset in range(0, len(plan), concurrency):
            wave = plan[offset : offset + concurrency]
            wave_barrier = threading.Barrier(len(wave), timeout=10.0)
            # All concurrent clients in this wave read the current version before submitting
            wave_requests = [
                UpdateRequest(
                    task_id=record_id_map[rec_idx],
                    expected_version=get_current_version_fn(record_id_map[rec_idx]),
                    title=title,
                    user_id="bench_user",
                )
                for rec_idx, title in wave
            ]
            futures = [
                client_pool.submit(client_task, req, wave_barrier)
                for req in wave_requests
            ]
            for fut in as_completed(futures):
                fut.result()

    wall_seconds = time.perf_counter() - t_start
    cpu_end = proc.cpu_times() if proc is not None else None
    rss_mb: Any = (
        round(proc.memory_info().rss / (1024 * 1024), 2)
        if proc is not None
        else "unavailable"
    )
    if cpu_start is not None and cpu_end is not None and wall_seconds > 0:
        cpu_used_s = (cpu_end.user - cpu_start.user) + (cpu_end.system - cpu_start.system)
        cpu_pct: Any = round((cpu_used_s / wall_seconds) * 100.0, 2)
    else:
        cpu_pct = "unavailable"

    sorted_lat = sorted(latencies_ms)
    completed = len(statuses)
    success_count = sum(1 for s in statuses if s == UpdateStatus.SUCCESS)
    conflict_count = sum(1 for s in statuses if s == UpdateStatus.VERSION_CONFLICT)
    failed_count = sum(
        1 for s in statuses if s not in (UpdateStatus.SUCCESS, UpdateStatus.VERSION_CONFLICT)
    )

    return {
        "mode": mode_label,
        "workload": workload_name,
        "completed_requests": completed,
        "successful_updates": success_count,
        "version_conflicts": conflict_count,
        "failed_requests": failed_count,
        "wall_time_sec": round(wall_seconds, 4),
        "throughput_req_per_sec": round(completed / wall_seconds, 2) if wall_seconds > 0 else 0.0,
        "p50_ms": round(_percentile(sorted_lat, 0.50), 3),
        "p95_ms": round(_percentile(sorted_lat, 0.95), 3),
        "p99_ms": round(_percentile(sorted_lat, 0.99), 3),
        "cpu_percent": cpu_pct,
        "rss_memory_mb": rss_mb,
    }


def run_all_benchmarks(total_requests: int = 200, num_records: int = 40, concurrency: int = 16) -> List[Dict[str, Any]]:
    print("=" * 88)
    print("ConcurrentHub Phase 2 — Comparative Performance Benchmark Suite")
    print("=" * 88)

    probe_pool = PostgresPoolManager(min_size=2, max_size=10, open_immediately=False)
    using_postgres = probe_pool.check_connectivity()

    if using_postgres:
        probe_pool.open()
        db_config_summary: Any = probe_pool.get_pool_stats()
        storage_label = "PostgreSQL (psycopg3 + ConnectionPool)"
    else:
        probe_pool.close()
        db_config_summary = "unavailable (PostgreSQL offline; using InMemoryTaskStore fallback)"
        storage_label = "InMemoryTaskStore (PostgreSQL offline fallback)"

    print(f"Storage Backend          : {storage_label}")
    print(f"DB Connection Pool Config: {db_config_summary}")
    print(f"Requests per Workload    : {total_requests} (Client Concurrency = {concurrency})")
    print("-" * 88)

    workloads = ["low_contention", "high_contention", "mixed_workload"]
    all_results: List[Dict[str, Any]] = []

    try:
        for wl in workloads:
            plan = _generate_workload(wl, total_requests=total_requests, num_records=num_records)

            for mode in ("baseline_direct", "scheduler_enabled"):
                if using_postgres:
                    repo: Any = PostgresTaskRepository(probe_pool)
                    repo.init_schema()
                else:
                    repo = InMemoryTaskStore()

                # Seed fresh records for this run so both modes start from version=1
                record_id_map: Dict[int, int] = {}
                for r_idx in range(1, num_records + 1):
                    created = repo.create_task(f"Bench Record {r_idx}", user_id="bench_seed")
                    record_id_map[r_idx] = int(created["id"])

                def get_version(tid: int, _repo=repo) -> int:
                    rec = _repo.get_task(tid)
                    return int(rec["version"]) if rec is not None else 1

                if mode == "baseline_direct":
                    def direct_exec(req: UpdateRequest, _repo=repo) -> UpdateResult:
                        return _repo.update_task(
                            task_id=req.task_id,
                            title=req.title,
                            expected_version=req.expected_version,
                            user_id=req.user_id,
                        )

                    metrics = _run_single_experiment(
                        mode_label="Baseline (Direct)",
                        workload_name=wl,
                        plan=plan,
                        record_id_map=record_id_map,
                        get_current_version_fn=get_version,
                        execute_update_fn=direct_exec,
                        concurrency=concurrency,
                    )
                else:
                    obj_pool: BoundedObjectPool[ReusableUpdateBuffer] = BoundedObjectPool(
                        capacity=32,
                        preallocate=True,
                    )

                    def pooled_handler(req: UpdateRequest, _repo=repo, _pool=obj_pool) -> UpdateResult:
                        with _pool.lease(timeout=5.0) as buf:
                            buf.write_update(
                                task_id=req.task_id,
                                expected_version=req.expected_version,
                                title=req.title,
                                user_id=req.user_id,
                            )
                            return _repo.update_task(
                                task_id=buf.metadata["task_id"],
                                title=buf.metadata["title"],
                                expected_version=buf.metadata["expected_version"],
                                user_id=buf.metadata["user_id"],
                            )

                    scheduler = PerRecordScheduler(update_handler=pooled_handler, max_workers=concurrency)
                    try:
                        metrics = _run_single_experiment(
                            mode_label="Scheduler-Enabled",
                            workload_name=wl,
                            plan=plan,
                            record_id_map=record_id_map,
                            get_current_version_fn=get_version,
                            execute_update_fn=scheduler.submit_update,
                            concurrency=concurrency,
                        )
                    finally:
                        scheduler.shutdown(wait=True)
                        obj_pool.close()

                all_results.append(metrics)

        # Print formatted table
        header = (
            f"{'Workload':<16} | {'Mode':<18} | {'Done':>4} | {'OK':>4} | "
            f"{'Conf':>4} | {'Fail':>4} | {'Req/s':>9} | {'p50(ms)':>7} | "
            f"{'p95(ms)':>7} | {'p99(ms)':>7} | {'RSS(MB)':>7}"
        )
        print(header)
        print("-" * len(header))
        for row in all_results:
            print(
                f"{row['workload']:<16} | {row['mode']:<18} | "
                f"{row['completed_requests']:>4} | {row['successful_updates']:>4} | "
                f"{row['version_conflicts']:>4} | {row['failed_requests']:>4} | "
                f"{row['throughput_req_per_sec']:>9.2f} | {row['p50_ms']:>7.3f} | "
                f"{row['p95_ms']:>7.3f} | {row['p99_ms']:>7.3f} | "
                f"{str(row['rss_memory_mb']):>7}"
            )
        print("=" * 88)
        return all_results
    finally:
        if using_postgres:
            probe_pool.close()


if __name__ == "__main__":
    run_all_benchmarks()
