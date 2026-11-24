"""
Scheduler unit tests — no database required.
Tests: per-record serialization, conflict detection, version checking.
"""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock

import pytest

from scheduler.scheduler import (
    InMemoryTaskStore,
    PerRecordScheduler,
    UpdateRequest,
    UpdateResult,
    UpdateStatus,
)
from scheduler.conflict_detector import ConflictDetector
from scheduler.version_check import check_version_match, validate_version


# ---------------------------------------------------------------------------
# Version check
# ---------------------------------------------------------------------------

def test_version_check_pass():
    assert check_version_match(current_version=3, expected_version=3) is True

def test_version_check_fail():
    assert check_version_match(current_version=5, expected_version=2) is False

def test_version_check_zero():
    assert check_version_match(current_version=1, expected_version=0) is False

def test_validate_version_returns_result():
    r = validate_version(current_version=2, expected_version=2)
    assert r.is_valid is True
    assert r.next_version == 3

def test_validate_version_conflict():
    r = validate_version(current_version=5, expected_version=2)
    assert r.is_valid is False


# ---------------------------------------------------------------------------
# In-memory store (used in scheduler integration)
# ---------------------------------------------------------------------------

def test_in_memory_create_and_get():
    store = InMemoryTaskStore()
    t = store.create_task(title="Hello", description="World", status="TODO",
                          priority="HIGH", assignee="alice", user_id="alice")
    assert t["version"] == 1
    fetched = store.get_task(t["id"])
    assert fetched is not None
    assert fetched["title"] == "Hello"


def test_in_memory_optimistic_update_success():
    store = InMemoryTaskStore()
    t = store.create_task(title="OCC test", description="", status="TODO",
                          priority="MEDIUM", assignee="bob", user_id="bob")
    result = store.update_task(
        task_id=t["id"], expected_version=1, title="Updated",
        description=None, status=None, priority=None, assignee=None, user_id="bob",
    )
    assert result.status == UpdateStatus.SUCCESS
    assert result.new_version == 2


def test_in_memory_optimistic_update_conflict():
    store = InMemoryTaskStore()
    t = store.create_task(title="Stale test", description="", status="TODO",
                          priority="MEDIUM", assignee="carol", user_id="carol")
    # Advance version first
    store.update_task(task_id=t["id"], expected_version=1, title="v2",
                      description=None, status=None, priority=None, assignee=None, user_id="carol")
    # Now try with stale expected_version=1
    result = store.update_task(task_id=t["id"], expected_version=1, title="v3",
                               description=None, status=None, priority=None, assignee=None, user_id="carol")
    assert result.status == UpdateStatus.VERSION_CONFLICT


# ---------------------------------------------------------------------------
# Per-record scheduler
# ---------------------------------------------------------------------------

def make_mock_handler():
    """Handler that always succeeds."""
    def handler(req: UpdateRequest) -> UpdateResult:
        return UpdateResult(
            status=UpdateStatus.SUCCESS,
            task_id=req.task_id,
            new_version=req.expected_version + 1,
            message="ok",
        )
    return handler


def test_scheduler_submit_single():
    sched = PerRecordScheduler(update_handler=make_mock_handler(), max_workers=2)
    try:
        req = UpdateRequest(task_id=1, expected_version=1, title="T", user_id="u")
        result = sched.submit_update(req)
        assert result.status == UpdateStatus.SUCCESS
    finally:
        sched.shutdown(wait=True)


def test_scheduler_serial_per_record():
    """Updates to same task_id must be serialized (no interleaving)."""
    order = []
    lock = threading.Lock()

    def handler(req: UpdateRequest) -> UpdateResult:
        with lock:
            order.append(req.title)
        time.sleep(0.01)
        return UpdateResult(status=UpdateStatus.SUCCESS, task_id=req.task_id,
                            new_version=req.expected_version + 1, message="ok")

    sched = PerRecordScheduler(update_handler=handler, max_workers=4)
    try:
        reqs = [
            UpdateRequest(task_id=1, expected_version=i, title=f"v{i}", user_id="u")
            for i in range(1, 6)
        ]
        results = [sched.submit_update(r) for r in reqs]
        assert all(r.status in (UpdateStatus.SUCCESS, UpdateStatus.VERSION_CONFLICT)
                   for r in results)
    finally:
        sched.shutdown(wait=True)


def test_scheduler_independent_records_concurrent():
    """Updates to DIFFERENT task_ids may run concurrently."""
    sched = PerRecordScheduler(update_handler=make_mock_handler(), max_workers=4)
    try:
        reqs = [
            UpdateRequest(task_id=i, expected_version=1, title=f"T{i}", user_id="u")
            for i in range(1, 6)
        ]
        results = [sched.submit_update(r) for r in reqs]
        assert all(r.status == UpdateStatus.SUCCESS for r in results)
    finally:
        sched.shutdown(wait=True)


# ---------------------------------------------------------------------------
# Conflict detector
# ---------------------------------------------------------------------------

def test_conflict_detector_tracking():
    cd = ConflictDetector()
    # Simulate two concurrent enqueues to task 1 (queue_depth > 1 means contention)
    cd.record_enqueue(task_id=1, current_queue_depth=1)
    cd.record_enqueue(task_id=1, current_queue_depth=2)  # contended
    cd.record_outcome(task_id=1, is_conflict=False, is_success=True)
    cd.record_outcome(task_id=1, is_conflict=True, is_success=False)

    snap = cd.get_snapshot(task_id=1)
    assert snap.enqueued_total == 2
    assert snap.version_conflicts == 1
    assert snap.successful_updates == 1

def test_conflict_detector_global_summary():
    cd = ConflictDetector()
    cd.record_enqueue(task_id=10, current_queue_depth=1)
    cd.record_enqueue(task_id=20, current_queue_depth=1)
    summary = cd.get_global_summary()
    assert summary["total_records_seen"] == 2
    assert summary["total_enqueued"] == 2
