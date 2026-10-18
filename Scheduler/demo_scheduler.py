
import threading
from typing import List

from scheduler.scheduler import PerRecordScheduler, UpdateRequest


def run_demo() -> None:
    print("=" * 72)
    print("ConcurrentHub Phase 2 — Standalone Per-Record Scheduler Demo")
    print("Owner: Aanchal Singh (Member 1)")
    print("=" * 72)

    scheduler = PerRecordScheduler(max_workers=4)

    # 1. Create three tasks in the standalone in-memory store
    task_a = scheduler.store.create_task("Design OS scheduler queues", user_id="aanchal")
    task_b = scheduler.store.create_task("Configure PostgreSQL pool", user_id="parth")
    task_c = scheduler.store.create_task("Build FastAPI endpoints", user_id="gargi")

    print("\n[Step 1] Created initial tasks in standalone store:")
    for t in (task_a, task_b, task_c):
        print(f"  -> Task #{t['id']}: title='{t['title']}', version={t['version']}")

    # 2. Demonstrate sequential updates and stale version rejection on the same record
    print("\n[Step 2] Submitting two competing updates to Task #1 (both expecting version 1):")
    req_1a = UpdateRequest(
        task_id=task_a["id"],
        expected_version=1,
        title="Design OS scheduler queues (v2 by user_1)",
        user_id="user_1",
    )
    req_1b_stale = UpdateRequest(
        task_id=task_a["id"],
        expected_version=1,
        title="Overwrite Task #1 with stale version 1 (by user_2)",
        user_id="user_2",
    )
    req_1c_valid = UpdateRequest(
        task_id=task_a["id"],
        expected_version=2,
        title="Finalize OS scheduler queues (v3 by user_1)",
        user_id="user_1",
    )

    f1 = scheduler.submit_update_async(req_1a)
    f2 = scheduler.submit_update_async(req_1b_stale)
    f3 = scheduler.submit_update_async(req_1c_valid)

    for label, fut in [
        ("Request 1A (expects v1)", f1),
        ("Request 1B (expects stale v1)", f2),
        ("Request 1C (expects v2)", f3),
    ]:
        res = fut.result(timeout=5.0)
        print(f"  -> {label}: {res.to_dict()}")

    # 3. Demonstrate independent record IDs progressing concurrently
    print("\n[Step 3] Demonstrating concurrent progress across independent records (#2 and #3):")
    barrier = threading.Barrier(2, timeout=5.0)
    concurrent_threads: List[str] = []
    lock = threading.Lock()

    def slow_independent_handler(request: UpdateRequest):
        # Both record #2 and record #3 workers must arrive at the barrier simultaneously,
        # proving that independent record queues execute in parallel on separate threads.
        barrier.wait()
        with lock:
            concurrent_threads.append(
                f"task_id={request.task_id} on {threading.current_thread().name}"
            )
        return scheduler.store.handle_scheduled_update(request)

    parallel_scheduler = PerRecordScheduler(
        update_handler=slow_independent_handler,
        max_workers=4,
    )
    # Seed tasks #1 and #2 in parallel_scheduler's store
    p_task1 = parallel_scheduler.store.create_task("Record One", user_id="aanchal")
    p_task2 = parallel_scheduler.store.create_task("Record Two", user_id="parth")

    pf1 = parallel_scheduler.submit_update_async(
        UpdateRequest(task_id=p_task1["id"], expected_version=1, title="Record One Updated")
    )
    pf2 = parallel_scheduler.submit_update_async(
        UpdateRequest(task_id=p_task2["id"], expected_version=1, title="Record Two Updated")
    )

    pres1 = pf1.result(timeout=5.0)
    pres2 = pf2.result(timeout=5.0)
    for entry in concurrent_threads:
        print(f"  -> Executed concurrently: {entry}")
    print(f"  -> Record #1 outcome: status={pres1.status}, new_version={pres1.new_version}")
    print(f"  -> Record #2 outcome: status={pres2.status}, new_version={pres2.new_version}")
    parallel_scheduler.shutdown(wait=True)

    # 4. Inspect read-only scheduler snapshot
    snapshot = scheduler.get_queue_snapshot()
    summary = snapshot["contention_summary"]
    print("\n[Step 4] Scheduler Contention & Queue Snapshot:")
    print(f"  -> Active queues after drain: {snapshot['active_queue_count']}")
    print(f"  -> Total requests enqueued:   {summary['total_enqueued']}")
    print(f"  -> Successful updates:        {summary['total_successful_updates']}")
    print(f"  -> Version conflicts caught:  {summary['total_version_conflicts']}")

    scheduler.shutdown(wait=True)
    print("\n[Done] Scheduler shut down cleanly.")
    print("=" * 72)


if __name__ == "__main__":
    run_demo()
