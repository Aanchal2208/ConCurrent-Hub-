"""
Experimental BareHeap-inspired bounded, thread-safe object/buffer pool.
OS & DBMS Phase 2 (Team Nexus - OSDBMS-V-2026-T185)

Note on BareHeap comparison:
- What is inspired by BareHeap: bounded capacity, pre-allocated reusable buffers,
  explicit acquire/release lifecycle, and resetting buffers before return to
  reduce repeated per-request object churn.
- What is distinct: this is a safe, application-level Python object pool inside
  CPython's managed runtime, NOT a low-level C/OS virtual-memory allocator, and
  it is completely separate from the PostgreSQL database connection pool in
  `database/postgres_db.py`.
"""

from __future__ import annotations

import queue
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Generic, Iterator, Optional, Set, TypeVar

T = TypeVar("T")


class PoolError(Exception):
    """Base exception for object pool errors."""


class PoolExhaustedError(PoolError):
    """Raised when no pooled object becomes available before timeout."""


class PoolDoubleReleaseError(PoolError):
    """Raised when releasing an object already in the pool."""


class PoolInvalidObjectError(PoolError):
    """Raised when releasing an object that did not originate from this pool."""


class PoolClosedError(PoolError):
    """Raised when acquiring from a closed pool."""


@dataclass
class ReusableUpdateBuffer:
    """
    Reusable request context/buffer object for staging task update payloads
    and metadata during request execution.
    """

    slot_id: int
    payload: bytearray = field(default_factory=lambda: bytearray(512))
    metadata: Dict[str, Any] = field(default_factory=dict)
    reuse_count: int = 0
    is_active: bool = False

    def write_update(
        self,
        task_id: int,
        expected_version: int,
        title: Optional[str],
        description: Optional[str] = None,
        status: Optional[str] = None,
        priority: Optional[str] = None,
        assignee: Optional[str] = None,
        user_id: str = "anonymous",
    ) -> None:
        """Populate buffer metadata and encoded title bytes."""
        self.metadata["task_id"] = task_id
        self.metadata["expected_version"] = expected_version
        self.metadata["title"] = title
        self.metadata["description"] = description
        self.metadata["status"] = status
        self.metadata["priority"] = priority
        self.metadata["assignee"] = assignee
        self.metadata["user_id"] = user_id
        encoded = (title or "").encode("utf-8")[: len(self.payload)]
        self.payload[: len(encoded)] = encoded
        self.is_active = True

    def reset(self) -> None:
        """Clear sensitive/request-specific state before returning to free list."""
        self.metadata.clear()
        self.metadata["task_id"] = None
        self.payload[:] = b"\x00" * len(self.payload)
        self.is_active = False


class BoundedObjectPool(Generic[T]):
    """
    Thread-safe bounded pool of reusable objects.
    Pre-allocates up to `capacity` objects, tracks leased objects by id(obj),
    and enforces explicit acquire/release lifecycles.
    """

    def __init__(
        self,
        capacity: int = 16,
        factory: Optional[Callable[[int], T]] = None,
        reset_fn: Optional[Callable[[T], None]] = None,
        preallocate: bool = True,
    ) -> None:
        if capacity < 1:
            raise ValueError("capacity must be at least 1")

        self._capacity = capacity
        self._factory: Callable[[int], T] = factory or (
            lambda slot: ReusableUpdateBuffer(slot_id=slot)  # type: ignore[return-value]
        )
        self._reset_fn = reset_fn
        self._free_queue: "queue.Queue[T]" = queue.Queue(maxsize=capacity)
        self._lock = threading.Lock()

        self._all_object_ids: Set[int] = set()
        self._in_use_ids: Set[int] = set()
        self._allocated_count = 0
        self._total_acquisitions = 0
        self._reused_acquisitions = 0
        self._seen_leased_before: Set[int] = set()
        self._closed = False

        if preallocate:
            for slot in range(capacity):
                obj = self._factory(slot)
                obj_id = id(obj)
                self._all_object_ids.add(obj_id)
                self._allocated_count += 1
                self._free_queue.put_nowait(obj)

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def available(self) -> int:
        return self._free_queue.qsize()

    @property
    def in_use(self) -> int:
        with self._lock:
            return len(self._in_use_ids)

    def acquire(self, timeout: Optional[float] = 2.0) -> T:
        """
        Acquire an object from the bounded pool.
        Raises `PoolExhaustedError` if no object becomes available within timeout.
        """
        with self._lock:
            if self._closed:
                raise PoolClosedError("Cannot acquire from a closed BoundedObjectPool.")

            if self._free_queue.empty() and self._allocated_count < self._capacity:
                slot = self._allocated_count
                obj = self._factory(slot)
                obj_id = id(obj)
                self._all_object_ids.add(obj_id)
                self._allocated_count += 1
                self._in_use_ids.add(obj_id)
                self._seen_leased_before.add(obj_id)
                self._total_acquisitions += 1
                return obj

        block = timeout is None or timeout > 0
        try:
            obj = self._free_queue.get(block=block, timeout=timeout)
        except queue.Empty as exc:
            raise PoolExhaustedError(
                f"Object pool exhausted (capacity={self._capacity}); no object became available."
            ) from exc

        with self._lock:
            if self._closed:
                self._free_queue.put_nowait(obj)
                raise PoolClosedError("Cannot acquire from a closed BoundedObjectPool.")

            obj_id = id(obj)
            self._in_use_ids.add(obj_id)
            self._total_acquisitions += 1
            if obj_id in self._seen_leased_before:
                self._reused_acquisitions += 1
                if isinstance(obj, ReusableUpdateBuffer):
                    obj.reuse_count += 1
            else:
                self._seen_leased_before.add(obj_id)

        return obj

    def release(self, obj: T) -> None:
        """Reset and return a borrowed object back to the pool."""
        obj_id = id(obj)
        with self._lock:
            if obj_id not in self._all_object_ids:
                raise PoolInvalidObjectError(
                    "Attempted to release an object that does not belong to this pool."
                )
            if obj_id not in self._in_use_ids:
                raise PoolDoubleReleaseError(
                    "Attempted to release an object that is already in the pool (double release)."
                )

            if self._reset_fn is not None:
                self._reset_fn(obj)
            elif hasattr(obj, "reset") and callable(getattr(obj, "reset")):
                obj.reset()  # type: ignore[attr-defined]

            self._in_use_ids.remove(obj_id)
            if not self._closed:
                self._free_queue.put_nowait(obj)

    @contextmanager
    def lease(self, timeout: Optional[float] = 2.0) -> Iterator[T]:
        """Context manager that guarantees `release()` in finally."""
        obj = self.acquire(timeout=timeout)
        try:
            yield obj
        finally:
            self.release(obj)

    def get_stats(self) -> Dict[str, Any]:
        """Return an immutable snapshot of pool metrics."""
        with self._lock:
            in_use = len(self._in_use_ids)
            available = self._free_queue.qsize()
            total_acq = self._total_acquisitions
            reused_acq = self._reused_acquisitions
            reuse_ratio = (reused_acq / total_acq) if total_acq > 0 else 0.0
            return {
                "capacity": self._capacity,
                "allocated_objects": self._allocated_count,
                "in_use": in_use,
                "available": available,
                "total_acquisitions": total_acq,
                "reused_acquisitions": reused_acq,
                "reuse_ratio": round(reuse_ratio, 4),
                "total_leases": total_acq,
                "total_reuses": reused_acq,
                "reuse_ratio_pct": round(reuse_ratio * 100, 2),
                "closed": self._closed,
            }

    def close(self) -> None:
        """Mark pool as closed and drain idle objects."""
        with self._lock:
            self._closed = True
            while not self._free_queue.empty():
                try:
                    self._free_queue.get_nowait()
                except queue.Empty:
                    break


def measure_pool_vs_allocation(iterations: int = 5000, capacity: int = 16) -> Dict[str, Any]:
    """Compare acquiring/releasing pre-allocated buffers vs fresh object allocation."""
    pool: BoundedObjectPool[ReusableUpdateBuffer] = BoundedObjectPool(
        capacity=capacity,
        preallocate=True,
    )

    t0 = time.perf_counter()
    for i in range(iterations):
        with pool.lease(timeout=1.0) as buf:
            buf.write_update(task_id=1, expected_version=i + 1, title="Benchmark Task", user_id="u1")
    pooled_elapsed = time.perf_counter() - t0
    stats = pool.get_stats()
    pool.close()

    t1 = time.perf_counter()
    for i in range(iterations):
        fresh = ReusableUpdateBuffer(slot_id=i)
        fresh.write_update(task_id=1, expected_version=i + 1, title="Benchmark Task", user_id="u1")
        fresh.reset()
    unpooled_elapsed = time.perf_counter() - t1

    return {
        "iterations": iterations,
        "pool_capacity": capacity,
        "pooled_seconds": round(pooled_elapsed, 6),
        "unpooled_seconds": round(unpooled_elapsed, 6),
        "pooled_objects_allocated": stats["allocated_objects"],
        "pooled_reused_acquisitions": stats["reused_acquisitions"],
        "unpooled_objects_created": iterations,
    
