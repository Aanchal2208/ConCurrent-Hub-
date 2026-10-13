
from __future__ import annotations

from fastapi.testclient import TestClient

from backend.main import create_app
from scheduler.scheduler import (
    InMemoryTaskStore,
    PerRecordScheduler,
    UpdateRequest,
    UpdateResult,
    UpdateStatus,
)


def test_health_endpoint_reports_storage_mode_clearly() -> None:
    app = create_app(use_in_memory_db=True)

    with TestClient(app) as client:
        resp = client.get("/health")

        assert resp.status_code == 200

        body = resp.json()
        assert body["status"] == "ok"
        assert body["storage_backend"] == "in_memory_demo"
        assert body["persisted_in_postgresql"] is False
        assert "scheduler" in body
        assert "object_pool" in body


def test_create_get_list_and_update_task_lifecycle() -> None:
    app = create_app(use_in_memory_db=True)

    with TestClient(app) as client:
        create_resp = client.post(
            "/tasks",
            json={
                "title": "Prepare project report",
                "user_id": "user_1",
            },
        )

        assert create_resp.status_code == 201

        task = create_resp.json()
        task_id = task["id"]

        assert task["title"] == "Prepare project report"
        assert task["version"] == 1

        get_resp = client.get(f"/tasks/{task_id}")

        assert get_resp.status_code == 200
        assert get_resp.json()["title"] == "Prepare project report"

        list_resp = client.get("/tasks")

        assert list_resp.status_code == 200
        assert len(list_resp.json()) == 1

        update_resp = client.put(
            f"/tasks/{task_id}",
            json={
                "expected_version": 1,
                "title": "Complete project report",
                "user_id": "user_1",
            },
        )

        assert update_resp.status_code == 200

        upd_body = update_resp.json()
        assert upd_body["status"] == "SUCCESS"
        assert upd_body["task_id"] == task_id
        assert upd_body["new_version"] == 2
        assert upd_body["message"] == "Task updated successfully"


def test_version_conflict_returns_409_and_does_not_modify_record() -> None:
    app = create_app(use_in_memory_db=True)

    with TestClient(app) as client:
        created = client.post(
            "/tasks",
            json={
                "title": "Initial draft",
                "user_id": "user_1",
            },
        ).json()

        task_id = created["id"]

        r1 = client.put(
            f"/tasks/{task_id}",
            json={
                "expected_version": 1,
                "title": "Second draft",
                "user_id": "user_1",
            },
        )

        assert r1.status_code == 200

        r2 = client.put(
            f"/tasks/{task_id}",
            json={
                "expected_version": 1,
                "title": "Stale overwrite",
                "user_id": "user_2",
            },
        )

        assert r2.status_code == 409

        conflict_body = r2.json()

        assert conflict_body == {
            "status": "VERSION_CONFLICT",
            "task_id": task_id,
            "current_version": 2,
            "message": "The task was modified by another request",
        }

        current = client.get(f"/tasks/{task_id}").json()

        assert current["title"] == "Second draft"
        assert current["version"] == 2


def test_missing_task_returns_404_and_invalid_payload_returns_422() -> None:
    app = create_app(use_in_memory_db=True)

    with TestClient(app) as client:
        get_404 = client.get("/tasks/9999")

        assert get_404.status_code == 404
        assert get_404.json()["status"] == "NOT_FOUND"

        put_404 = client.put(
            "/tasks/9999",
            json={
                "expected_version": 1,
                "title": "Does not exist",
                "user_id": "user_1",
            },
        )

        assert put_404.status_code == 404
        assert put_404.json()["status"] == "NOT_FOUND"

        put_422 = client.put(
            "/tasks/1",
            json={
                "expected_version": 0,
                "title": "Invalid version",
                "user_id": "user_1",
            },
        )

        assert put_422.status_code == 422
        assert put_422.json()["status"] == "VALIDATION_ERROR"

        post_422 = client.post(
            "/tasks",
            json={
                "title": "   ",
                "user_id": "user_1",
            },
        )

        assert post_422.status_code == 422


def test_injected_custom_scheduler_and_database_error_handling() -> None:
    store = InMemoryTaskStore()
    store.create_task("Seed task", user_id="u1")

    def failing_db_handler(req: UpdateRequest) -> UpdateResult:
        return UpdateResult(
            status=UpdateStatus.ERROR,
            task_id=req.task_id,
            message="Database unavailable: simulated connection pool timeout",
        )

    custom_scheduler = PerRecordScheduler(
        update_handler=failing_db_handler,
        max_workers=2,
    )

    app = create_app(
        repository=store,
        scheduler=custom_scheduler,
    )

    with TestClient(app) as client:
        resp = client.put(
            "/tasks/1",
            json={
                "expected_version": 1,
                "title": "Should fail with 503",
                "user_id": "u1",
            },
        )

        assert resp.status_code == 503

        body = resp.json()
        assert body["status"] == "ERROR"
        assert "unavailable" in body["message"].lower()


def test_integrated_http_and_websocket_event_ordering() -> None:
    app = create_app(use_in_memory_db=True)

    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            created = client.post(
                "/tasks",
                json={
                    "title": "WS integrated task",
                    "user_id": "user_1",
                },
            ).json()

            tid = created["id"]

            evt1 = ws.receive_json()

            assert evt1["event"] == "task_created"
            assert evt1["task_id"] == tid
            assert evt1["version"] == 1

            ok_resp = client.put(
                f"/tasks/{tid}",
                json={
                    "expected_version": 1,
                    "title": "WS updated title",
                    "user_id": "user_1",
                },
            )

            assert ok_resp.status_code == 200

            evt2 = ws.receive_json()

            assert evt2["event"] == "task_updated"
            assert evt2["task_id"] == tid
            assert evt2["version"] == 2
            assert evt2["title"] == "WS updated title"

            conflict_resp = client.put(
                f"/tasks/{tid}",
                json={
                    "expected_version": 1,
                    "title": "Rejected stale title",
                    "user_id": "user_2",
                },
            )

            assert conflict_resp.status_code == 409

            evt3 = ws.receive_json()

            assert evt3["event"] == "version_conflict"
            assert evt3["task_id"] == tid
            assert evt3["current_version"] == 2
            assert evt3["modified"] is False
