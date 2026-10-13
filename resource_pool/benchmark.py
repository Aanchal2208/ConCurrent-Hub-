resource_pool/benchmark.py
Owner: Parth Gauniyal (Member 2)

Micro-benchmark measuring object reuse and contention behaviour of
BoundedObjectPool across single-threaded and multi-threaded workloads.

Run command:
    python -m resource_pool.benchmark
"""

import statistics
import threading
import time
from typing import Dict, List

from resource_pool.object_pool import (
    BoundedObjectPool,
    ReusableUpdateBuffer,
    measure_pool_vs_allocation,
)
def run_multithreaded_pool_benchmark(
    num_threads: int = 8,
    ops_per_thread: int = 500,
    pool_capacity: int = 8,
) -> Dict[str, object]:
    """Measure latency and reuse metrics when multiple threads share a bounded object pool."""
    pool: BoundedObjectPool[ReusableUpdateBuffer] = BoundedObjectPool(
        capacity=pool_capacity,
        preallocate=True,
    )
    latencies_ms: List[float] = []
    lock = threading.Lock()
    barrier = threading.Barrier(num_threads, timeout=10.0)

    def worker(thread_idx: int) -> None:
        local_latencies: List[float] = []
        barrier.wait()
        for op in range(ops_per_thread):
            t0 = time.perf_counter()
            with pool.lease(timeout=5.0) as buf:
                buf.write_update(
                    task_id=(op % 10) + 1,
                    expected_version=1,
                    title=f"Thread {thread_idx} op {op}",
                    user_id=f"user_{thread_idx}",
                )
            local_latencies.append((time.perf_counter() - t0) * 1000.0)
        with lock:
            latencies_ms.extend(local_latencies)
                t_start = time.perf_counter()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15.0)
    elapsed_s = time.perf_counter() - t_start

    stats = pool.get_stats()
    pool.close()

    sorted_lat = sorted(latencies_ms)
    total_ops = len(sorted_lat)
    p50 = sorted_lat[int(0.50 * (total_ops - 1))] if total_ops else 0.0
    p95 = sorted_lat[int(0.95 * (total_ops - 1))] if total_ops else 0.0
    p99 = sorted_lat[int(0.99 * (total_ops - 1))] if total_ops else 0.0

    return {
        "threads": num_threads,
        "ops_per_thread": ops_per_thread,
        "total_completed_ops": total_ops,
        "pool_capacity": pool_capacity,
        "allocated_objects": stats["allocated_objects"],
        "reused_acquisitions": stats["reused_acquisitions"],
        "reuse_ratio": stats["reuse_ratio"],
        "elapsed_seconds": round(elapsed_s, 4),
        "throughput_ops_per_sec": round(total_ops / elapsed_s, 2) if elapsed_s > 0 else 0.0,
        "p50_latency_ms": round(p50, 4),
        "p95_latency_ms": round(p95, 4),
        "p99_latency_ms": round(p99, 4),
        "mean_latency_ms": round(statistics.mean(sorted_lat), 4) if sorted_lat else 0.0,
    }
def main() -> None:
    print("=" * 72)
    print("ConcurrentHub Phase 2 — Resource Pool Benchmark")
    print("Owner: Parth Gauniyal (Member 2)")
    print("=" * 72)

    single_result = measure_pool_vs_allocation(iterations=5000, capacity=16)
    print("\n[1] Single-Thread Object Reuse vs Fresh Allocation (5,000 iterations):")
    for k, v in single_result.items():
        print(f"  {k:30s}: {v}")

    mt_result = run_multithreaded_pool_benchmark(
        num_threads=8,
        ops_per_thread=500,
        pool_capacity=8,
    )
    print("\n[2] Multi-Threaded Bounded Pool Contention Benchmark (8 threads x 500 ops):")
    for k, v in mt_result.items():
        print(f"  {k:30s}: {v}")
    print("=" * 72)


if _name_ == "_main_":
    main()