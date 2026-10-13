"""resource_pool/test_pool.py
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

def test_pool_capacity_exhaustion_and_unblocking() -> None:
    """
    When all objects are leased, acquiring with a short timeout raises PoolExhaustedError,
    and releasing an object allows a waiting thread to succeed.
    """
    pool: BoundedObjectPool[ReusableUpdateBuffer] = BoundedObjectPool(
        capacity=2,
        preallocate=True,
    )
    try:
        b1 = pool.acquire(timeout=0.5)
        b2 = pool.acquire(timeout=0.5)

        with pytest.raises(PoolExhaustedError):
            pool.acquire(timeout=0.05)

        # Release b1 from a background thread while main thread waits
        entered_wait = threading.Event()

        def delayed_release() -> None:
            entered_wait.wait(timeout=2.0)
            pool.release(b1)

        releaser = threading.Thread(target=delayed_release)
        releaser.start()
        entered_wait.set()

        b3 = pool.acquire(timeout=2.0)
        releaser.join(timeout=2.0)
        assert id(b3) == id(b1)

        pool.release(b2)
        pool.release(b3)
    finally:
        pool.close()
    
    def test_double_release_and_invalid_object_errors() -> None:
    """Releasing the same object twice or releasing an external object raises explicit errors."""
    pool: BoundedObjectPool[ReusableUpdateBuffer] = BoundedObjectPool(
        capacity=2,
        preallocate=True,
    )
    try:
        buf = pool.acquire(timeout=0.5)
        pool.release(buf)

        with pytest.raises(PoolDoubleReleaseError):
            pool.release(buf)

        foreign = ReusableUpdateBuffer(slot_id=999)
        with pytest.raises(PoolInvalidObjectError):
            pool.release(foreign)
    finally:
        pool.close()
def test_pool_close_cleanup() -> None:
    """Closing a pool prevents further acquisitions and drains available objects."""
    pool: BoundedObjectPool[ReusableUpdateBuffer] = BoundedObjectPool(
        capacity=3,
        preallocate=True,
    )
    pool.close()
    stats = pool.get_stats()
    assert stats["closed"] is True
    assert stats["available"] == 0

    with pytest.raises(PoolClosedError):
        pool.acquire(timeout=0.1)