"""
resource_pool/pool_demo.py
Owner: Parth Gauniyal (Member 2)

Standalone demonstration of the BareHeap-inspired BoundedObjectPool.
Shows object reuse, capacity limits, automatic buffer reset on release,
and double-release protection.

Run command:
    python -m resource_pool.pool_demo
"""

from resource_pool.object_pool import (
    BoundedObjectPool,
    PoolDoubleReleaseError,
    PoolExhaustedError,
    ReusableUpdateBuffer,
)
def run_pool_demo() -> None:
    print("=" * 72)
    print("ConcurrentHub Phase 2 — Standalone Object Pool Demonstration")
    print("Owner: Parth Gauniyal (Member 2)")
    print("=" * 72)

    pool: BoundedObjectPool[ReusableUpdateBuffer] = BoundedObjectPool(
        capacity=2,
        preallocate=True,
    )
    print("\n[Step 1] Initialized BoundedObjectPool with capacity=2:")
    print(f"  -> Initial stats: {pool.get_stats()}")
    # 1. Lease and release an object multiple times to demonstrate reuse
    print("\n[Step 2] Acquiring and releasing a buffer across 4 sequential operations:")
    observed_slots = []
    for i in range(1, 5):
        with pool.lease(timeout=1.0) as buf:
            buf.write_update(
                task_id=101,
                expected_version=i,
                title=f"Update #{i}",
                user_id="parth",
            )
            observed_slots.append((i, buf.slot_id, buf.reuse_count, dict(buf.metadata)))