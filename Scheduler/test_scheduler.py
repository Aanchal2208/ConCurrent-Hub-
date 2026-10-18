
import threading
from typing import List

from scheduler.scheduler import (
    InMemoryTaskStore,
    PerRecordScheduler,
    UpdateRequest,
    UpdateResult,
    UpdateStatus,
)
from scheduler.version_check import check_version_match, validate_version


def test_version_validation_helper() -> None:
    """Verify optimistic version check accepts matching version and rejects stale ones."""
    valid = validate_version(current_version=3, expected_version=3)
    assert valid.is_valid is True
    assert valid.next_version == 4

    stale = validate_version(current_version=4, expected_version=3)
    assert stale.is_valid is False
    assert stale.next_version is None
    assert check_version_match(4, 3) is False


def test_create_and_update_record() -> None:
    """Creating a task at version 1 and updating it with expected_version=1 succeeds."""
    scheduler = PerRecordScheduler(max_workers=2)
    try:
        created = scheduler.store.create_task("Write OS scheduler report", user_id="aanchal")
        assert created["id"] == 1
        assert created["version"] == 1

        result = scheduler.submit_update(
            UpdateRequest(
                task_id=1,
                expected_version=1,
                title="Write OS scheduler report - revised",
                user_id="aanchal",
            )
        )
        assert result.status == UpdateStatus.SUCCESS
        assert result.new_version == 2

        stored = scheduler.store.get_task(1)
        assert stored is not None
        assert stored["title"] == "Write OS scheduler report - revised"
        assert stored["version"] == 2
    finally:
        scheduler.shutdown(wait=True)


def test_two_sequential_updates_to_same_record() -> None:
    """Two updates to the same record with sequential expected versions 1 -> 2 -> 3 succeed."""
    scheduler = PerRecordScheduler(max_workers=2)
    try:
        scheduler.store.create_task("Initial title", user_id="u1")

        f1 = scheduler.submit_update_async(
            UpdateRequest(task_id=1, expected_version=1, title="Second title", user_id="u1")
        )
        f2 = scheduler.submit_update_async(
            UpdateRequest(task_id=1, expected_version=2, title="Third title", user_id="u2")
        )

        r1 = f1.result(timeout=5.0)
        r2 = f2.result(timeout=5.0)

        assert r1.status == UpdateStatus.SUCCESS
        assert r1.new_version == 2
        assert r2.status == UpdateStatus.SUCCESS
        assert r2.new_version == 3

        final_task = scheduler.store.get_task(1)
        assert final_task is not None
        assert final_task["title"] == "Third title"
        assert final_task["version"] == 3
    finally:
        scheduler.shutdown(wait=True)


def test_stale_version_rejection_does_not_modify_record() -> None:
    """
    When current version is 3, Request A (expected=3) succeeds -> version 4,
    and Request B (expected=3) is rejected without modifying the record.
    """
    scheduler = PerRecordScheduler(max_workers=2)
    try:
        scheduler.store.create_task("v1", user_id="u1")
        scheduler.submit_update(UpdateRequest(task_id=1, expected_version=1, title="v2"))
        scheduler.submit_update(UpdateRequest(task_id=1, expected_version=2, title="v3"))

        before_conflict = scheduler.store.get_task(1)
        assert before_conflict is not None
        assert before_conflict["version"] == 3

        res_a = scheduler.submit_update(
            UpdateRequest(task_id=1, expected_version=3, title="v4-valid", user_id="user_a")
        )
        res_b = scheduler.submit_update(
            UpdateRequest(task_id=1, expected_version=3, title="v4-stale-attempt", user_id="user_b")
        )

        assert res_a.status == UpdateStatus.SUCCESS
        assert res_a.new_version == 4

        assert res_b.status == UpdateStatus.VERSION_CONFLICT
        assert res_b.current_version == 4

        # Verify record was NOT overwritten by Request B
        after_conflict = scheduler.store.get_task(1)
        assert after_conflict is not None
        assert after_conflict["version"] == 4
        assert after_conflict["title"] == "v4-valid"
        assert after_conflict["user_id"] == "user_a"
    finally:
        scheduler.shutdown(wait=True)


def test_preserve_fifo_queue_order_for_same_record() -> None:
    """
    Requests enqueued for the same record while the first request is blocked must
    execute strictly in the order they were enqueued.
    """
    first_entered = threading.Event()
    release_first = threading.Event()
    observed_titles: List[str] = []
    store = InMemoryTaskStore()
    store.create_task("Seed", user_id="seed")

    def controlled_handler(request: UpdateRequest) -> UpdateResult:
        if request.expected_version == 1:
            first_entered.set()
            assert release_first.wait(timeout=5.0), "Timed out waiting to release first update"
        observed_titles.append(request.title)
        return store.handle_scheduled_update(request)

    scheduler = PerRecordScheduler(update_handler=controlled_handler, max_workers=4)
    try:
        # Submit first item which pauses inside the handler
        f1 = scheduler.submit_update_async(
            UpdateRequest(task_id=1, expected_version=1, title="step-1")
        )
        assert first_entered.wait(timeout=5.0)

        # Enqueue 4 subsequent updates in strict sequence while worker is holding item 1
        f2 = scheduler.submit_update_async(
            UpdateRequest(task_id=1, expected_version=2, title="step-2")
        )
        f3 = scheduler.submit_update_async(
            UpdateRequest(task_id=1, expected_version=3, title="step-3")
        )
        f4 = scheduler.submit_update_async(
            UpdateRequest(task_id=1, expected_version=4, title="step-4")
        )
        f5 = scheduler.submit_update_async(
            UpdateRequest(task_id=1, expected_version=5, title="step-5")
        )

        # Verify snapshot shows queued items for task_id=1 without exposing mutable internals
        snap = scheduler.get_queue_snapshot()
        assert 1 in snap["active_record_queues"]
        assert snap["active_record_queues"][1]["pending_count"] >= 4

        # Release the first update and collect all results
        release_first.set()
        results = [f.result(timeout=5.0) for f in (f1, f2, f3, f4, f5)]

        assert [r.status for r in results] == [UpdateStatus.SUCCESS] * 5
        assert [r.new_version for r in results] == [2, 3, 4, 5, 6]
        assert observed_titles == ["step-1", "step-2", "step-3", "step-4", "step-5"]
    finally:
        scheduler.shutdown(wait=True)


def test_different_record_ids_progress_independently() -> None:
    """
    A blocked update on Record #1 must NOT prevent Record #2 from completing
    immediately on another worker thread.
    """
    record1_entered = threading.Event()
    release_record1 = threading.Event()
    store = InMemoryTaskStore()
    store.create_task("Record 1", user_id="u1")
    store.create_task("Record 2", user_id="u2")

    def handler(request: UpdateRequest) -> UpdateResult:
        if request.task_id == 1:
            record1_entered.set()
            assert release_record1.wait(timeout=5.0)
        return store.handle_scheduled_update(request)

    scheduler = PerRecordScheduler(update_handler=handler, max_workers=4)
    try:
        f_rec1 = scheduler.submit_update_async(
            UpdateRequest(task_id=1, expected_version=1, title="Rec 1 slow update")
        )
        assert record1_entered.wait(timeout=5.0)

        # While Record 1 is blocked, submit an update to Record 2 and verify it completes!
        res_rec2 = scheduler.submit_update(
            UpdateRequest(task_id=2, expected_version=1, title="Rec 2 fast update"),
            timeout=3.0,
        )
        assert res_rec2.status == UpdateStatus.SUCCESS
        assert res_rec2.new_version == 2
        assert not f_rec1.done()

        # Now unblock Record 1
        release_record1.set()
        res_rec1 = f_rec1.result(timeout=5.0)
        assert res_rec1.status == UpdateStatus.SUCCESS
        assert res_rec1.new_version == 2
    finally:
        scheduler.shutdown(wait=True)


def test_multiple_concurrent_requests_same_version_at_most_one_succeeds() -> None:
    """
    When 10 concurrent threads submit an update to the same task with expected_version=1,
    exactly 1 succeeds (moving version to 2) and 9 receive VERSION_CONFLICT.
    """
    scheduler = PerRecordScheduler(max_workers=4)
    try:
        scheduler.store.create_task("Contended task", user_id="owner")
        start_barrier = threading.Barrier(10, timeout=5.0)
        results: List[UpdateResult] = []
        results_lock = threading.Lock()

        def worker(idx: int) -> None:
            start_barrier.wait()
            res = scheduler.submit_update(
                UpdateRequest(
                    task_id=1,
                    expected_version=1,
                    title=f"Concurrent writer {idx}",
                    user_id=f"user_{idx}",
                )
            )
            with results_lock:
                results.append(res)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)

        assert len(results) == 10
        successes = [r for r in results if r.status == UpdateStatus.SUCCESS]
        conflicts = [r for r in results if r.status == UpdateStatus.VERSION_CONFLICT]
        assert len(successes) == 1
        assert len(conflicts) == 9
        assert successes[0].new_version == 2

        task = scheduler.store.get_task(1)
        assert task is not None
        assert task["version"] == 2
    finally:
        scheduler.shutdown(wait=True)


def test_handler_exception_and_shutdown_explicit_results() -> None:
    """
    Ensure handler exceptions, missing records, and post-shutdown submissions
    all return explicit UpdateResult failures rather than hanging or dropping requests.
    """
    def failing_handler(request: UpdateRequest) -> UpdateResult:
        if request.task_id == 999:
            raise RuntimeError("Simulated database crash")
        return UpdateResult(
            status=UpdateStatus.NOT_FOUND,
            task_id=request.task_id,
            message="Task does not exist",
        )

    scheduler = PerRecordScheduler(update_handler=failing_handler, max_workers=2)
    res_missing = scheduler.submit_update(
        UpdateRequest(task_id=404, expected_version=1, title="Missing")
    )
    assert res_missing.status == UpdateStatus.NOT_FOUND

    res_crash = scheduler.submit_update(
        UpdateRequest(task_id=999, expected_version=1, title="Crash")
    )
    assert res_crash.status == UpdateStatus.ERROR
    assert "Simulated database crash" in res_crash.message

    # Verify queue cleanup after draining
    snapshot = scheduler.get_queue_snapshot()
    assert snapshot["active_queue_count"] == 0

    scheduler.shutdown(wait=True)

    # Submitting after shutdown returns an explicit ERROR
    res_after_shutdown = scheduler.submit_update(
        UpdateRequest(task_id=1, expected_version=1, title="After shutdown")
    )
    assert res_after_shutdown.status == UpdateStatus.ERROR
    assert "shut down" in res_after_shutdown.message.lower()
