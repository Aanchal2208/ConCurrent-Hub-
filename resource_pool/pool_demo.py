

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

    for step, slot_id, reuse_count, meta in observed_slots:
        print(
            f"  -> Operation {step}: leased slot_id={slot_id}, "
            f"reuse_count={reuse_count}, metadata={meta}"
        )

    # 2. Demonstrate capacity limit (exhaustion when >2 buffers requested simultaneously)
    print("\n[Step 3] Demonstrating bounded capacity enforcement (capacity=2):")
    buf1 = pool.acquire(timeout=0.5)
    buf2 = pool.acquire(timeout=0.5)
    print(f"  -> Acquired 2 buffers (slot {buf1.slot_id} and slot {buf2.slot_id}).")
    print(f"  -> Pool stats while full: {pool.get_stats()}")

    try:
        print("  -> Attempting 3rd concurrent acquire with timeout=0.1s...")
        pool.acquire(timeout=0.1)
    except PoolExhaustedError as exc:
        print(f"  -> Expected PoolExhaustedError caught: {exc}")

    # Release both buffers
    pool.release(buf1)
    pool.release(buf2)

    # 3. Demonstrate double-release protection
    print("\n[Step 4] Demonstrating double-release prevention:")
    try:
        pool.release(buf1)
    except PoolDoubleReleaseError as exc:
        print(f"  -> Expected PoolDoubleReleaseError caught: {exc}")

    print(f"\n[Step 5] Final pool stats before close: {pool.get_stats()}")
    pool.close()
    print("[Done] BoundedObjectPool closed cleanly.")
    print("=" * 72)


if __name__ == "__main__":
    run_pool_demo()