resource_pool/test_pool.py
Owner: Parth Gauniyal (Member 2)

Unit tests for BoundedObjectPool covering object reuse, capacity limits,
automatic state reset, double-release prevention, foreign-object rejection,
and cleanup.
"""

import threading
import pytest

from resource_pool.object_pool import (
    BoundedObjectPool,
    PoolClosedError,
    PoolDoubleReleaseError,
    PoolExhaustedError,
    PoolInvalidObjectError,
    ReusableUpdateBuffer,
)
def test_pool_reuse_and_automatic_reset() -> None:
    """Releasing a buffer resets its state and allows subsequent acquisitions to reuse it."""
    pool: BoundedObjectPool[ReusableUpdateBuffer] = BoundedObjectPool(
        capacity=1,
        preallocate=True,
    )
    try:
        with pool.lease(timeout=1.0) as buf1:
            first_id = id(buf1)
            buf1.write_update(task_id=10, expected_version=1, title="Secret", user_id="u1")
            assert buf1.metadata["title"] == "Secret"

        # Acquire again from capacity-1 pool: must be the exact same Python object, cleanly reset
        with pool.lease(timeout=1.0) as buf2:
            assert id(buf2) == first_id
            assert buf2.metadata == {}
            assert buf2.reuse_count == 1

        stats = pool.get_stats()
        assert stats["allocated_objects"] == 1
        assert stats["total_acquisitions"] == 2
        assert stats["reused_acquisitions"] == 1
    finally:
        pool.close()
