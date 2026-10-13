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