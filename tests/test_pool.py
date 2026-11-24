"""
Object pool unit tests — no database required.
"""

from __future__ import annotations

import threading
import time

import pytest

from resource_pool.object_pool import BoundedObjectPool, ReusableUpdateBuffer


def test_pool_basic_lease():
    pool = BoundedObjectPool[ReusableUpdateBuffer](capacity=4, preallocate=True)
    try:
        with pool.lease(timeout=1.0) as buf:
            assert buf is not None
            # is_active starts False; becomes True after write_update()
            assert buf.is_active is False
            buf.write_update(task_id=1, expected_version=1, title="Test")
            assert buf.is_active is True
        stats = pool.get_stats()
        assert stats["total_leases"] >= 1
    finally:
        pool.close()


def test_pool_preallocated_reuse():
    pool = BoundedObjectPool[ReusableUpdateBuffer](capacity=2, preallocate=True)
    try:
        for _ in range(5):
            with pool.lease(timeout=1.0) as buf:
                buf.write_update(task_id=1, expected_version=1, title="T")
        stats = pool.get_stats()
        # With capacity=2 preallocated and 5 total leases:
        # first 2 leases establish the "seen" set → not counted as reuse
        # remaining 3 leases hit previously-seen objects → counted as reuse
        assert stats["total_reuses"] >= 3
        assert stats["reuse_ratio_pct"] > 0
    finally:
        pool.close()


def test_pool_capacity_respected():
    pool = BoundedObjectPool[ReusableUpdateBuffer](capacity=2, preallocate=True)
    try:
        stats = pool.get_stats()
        assert stats["capacity"] == 2
    finally:
        pool.close()


def test_pool_concurrent_leases():
    pool = BoundedObjectPool[ReusableUpdateBuffer](capacity=8, preallocate=True)
    results = []
    errors = []

    def worker():
        try:
            with pool.lease(timeout=2.0) as buf:
                buf.write_update(task_id=1, expected_version=1, title="concurrent")
                time.sleep(0.05)
                results.append(True)
        except Exception as e:
            errors.append(str(e))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()

    pool.close()
    assert len(errors) == 0
    assert len(results) == 8


def test_pool_write_update_stores_metadata():
    pool = BoundedObjectPool[ReusableUpdateBuffer](capacity=2, preallocate=True)
    try:
        with pool.lease(timeout=1.0) as buf:
            buf.write_update(
                task_id=42, expected_version=3,
                title="Metadata test", description="desc",
                status="IN_PROGRESS", priority="HIGH",
                assignee="alice", user_id="alice",
            )
            assert buf.metadata["task_id"] == 42
            assert buf.metadata["expected_version"] == 3
            assert buf.metadata["title"] == "Metadata test"
    finally:
        pool.close()
