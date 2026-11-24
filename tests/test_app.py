"""
Tests for ConcurrentHub Phase 3.
Run: pytest tests/ -v
Requires: PostgreSQL running + .env configured.
"""

from __future__ import annotations

import os
import pytest
from httpx import ASGITransport, AsyncClient

# Attempt to load .env from project root
from pathlib import Path
env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    from dotenv import load_dotenv
    load_dotenv(env_path)


@pytest.fixture(scope="module")
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="module")
async def client():
    """Create a test client against the FastAPI app."""
    from backend.main import app
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# Auth tests
# ---------------------------------------------------------------------------

def test_database_url_escapes_password_characters():
    from setup_database import build_database_url

    url = build_database_url(
        "postgresql://postgres:old@localhost:5432/concurrenthub",
        "new@pass:/#%",
    )

    assert url == (
        "postgresql://postgres:new%40pass%3A%2F%23%25@localhost:5432/concurrenthub"
    )


@pytest.mark.anyio
async def test_register_and_login(client: AsyncClient):
    """Register a user, then log in and receive a token."""
    import time
    username = f"testuser_{int(time.time())}"
    email = f"{username}@test.com"

    # Register
    r = await client.post("/auth/register", json={
        "username": username,
        "email": email,
        "password": "Test1234!",
        "full_name": "Test User",
        "role": "editor",
    })
    assert r.status_code == 201, r.text

    # Login
    r = await client.post("/auth/login", json={
        "username": username,
        "password": "Test1234!",
    })
    assert r.status_code == 200, r.text
    data = r.json()
    assert "access_token" in data
    assert data["user"]["username"] == username


@pytest.mark.anyio
async def test_register_reports_database_unavailable(client: AsyncClient):
    from backend.main import app
    from backend.routes import get_user_repo
    from database.postgres_db import DatabaseUnavailableError

    class UnavailableUserRepository:
        def register(self, **kwargs):
            raise DatabaseUnavailableError("database connection failed")

    previous_override = app.dependency_overrides.get(get_user_repo)
    app.dependency_overrides[get_user_repo] = lambda: UnavailableUserRepository()
    try:
        response = await client.post("/auth/register", json={
            "username": "newuser",
            "email": "newuser@example.com",
            "password": "Test1234!",
        })
    finally:
        if previous_override is None:
            app.dependency_overrides.pop(get_user_repo, None)
        else:
            app.dependency_overrides[get_user_repo] = previous_override

    assert response.status_code == 503
    assert "PostgreSQL cannot be reached" in response.json()["detail"]


@pytest.mark.anyio
async def test_live_server_origin_can_post_to_register(client: AsyncClient):
    response = await client.options("/auth/register", headers={
        "Origin": "http://localhost:5500",
        "Access-Control-Request-Method": "POST",
        "Access-Control-Request-Headers": "content-type",
    })

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://localhost:5500"


@pytest.mark.anyio
async def test_me_requires_auth(client: AsyncClient):
    r = await client.get("/auth/me")
    assert r.status_code == 401


@pytest.mark.anyio
async def test_duplicate_username_rejected(client: AsyncClient):
    import time
    username = f"dup_{int(time.time())}"
    payload = {"username": username, "email": f"{username}@a.com",
               "password": "Test1234!", "role": "editor"}
    r1 = await client.post("/auth/register", json=payload)
    assert r1.status_code == 201

    payload2 = {**payload, "email": f"{username}2@a.com"}
    r2 = await client.post("/auth/register", json=payload2)
    assert r2.status_code == 400


# ---------------------------------------------------------------------------
# Task CRUD tests
# ---------------------------------------------------------------------------

async def _get_token(client: AsyncClient) -> str:
    """Register a unique user and return a bearer token."""
    import time
    u = f"crud_{int(time.time())}"
    await client.post("/auth/register", json={
        "username": u, "email": f"{u}@t.com",
        "password": "Test1234!", "role": "editor",
    })
    r = await client.post("/auth/login", json={"username": u, "password": "Test1234!"})
    return r.json()["access_token"]


@pytest.mark.anyio
async def test_create_and_list_task(client: AsyncClient):
    token = await _get_token(client)
    headers = {"Authorization": f"Bearer {token}"}

    r = await client.post("/tasks", json={"title": "Test task from pytest"},
                          headers=headers)
    assert r.status_code == 201
    task = r.json()
    assert task["title"] == "Test task from pytest"
    assert task["version"] == 1

    r2 = await client.get("/tasks", headers=headers)
    assert r2.status_code == 200
    ids = [t["id"] for t in r2.json()]
    assert task["id"] in ids


@pytest.mark.anyio
async def test_get_task_by_id(client: AsyncClient):
    token = await _get_token(client)
    headers = {"Authorization": f"Bearer {token}"}

    r = await client.post("/tasks", json={"title": "Get by ID test"}, headers=headers)
    task_id = r.json()["id"]

    r2 = await client.get(f"/tasks/{task_id}", headers=headers)
    assert r2.status_code == 200
    assert r2.json()["id"] == task_id


@pytest.mark.anyio
async def test_update_task_success(client: AsyncClient):
    token = await _get_token(client)
    headers = {"Authorization": f"Bearer {token}"}

    r = await client.post("/tasks", json={"title": "Update me"}, headers=headers)
    task = r.json()

    r2 = await client.put(f"/tasks/{task['id']}", headers=headers, json={
        "expected_version": task["version"],
        "title": "Updated title",
        "status": "IN_PROGRESS",
    })
    assert r2.status_code == 200
    data = r2.json()
    assert data["status"] == "SUCCESS"
    assert data["new_version"] == task["version"] + 1


@pytest.mark.anyio
async def test_version_conflict(client: AsyncClient):
    """Two concurrent updates to the same task — only one should succeed."""
    token = await _get_token(client)
    headers = {"Authorization": f"Bearer {token}"}

    r = await client.post("/tasks", json={"title": "Conflict target"}, headers=headers)
    task = r.json()
    task_id = task["id"]
    version = task["version"]

    # First update — should succeed
    r1 = await client.put(f"/tasks/{task_id}", headers=headers, json={
        "expected_version": version,
        "title": "Update A",
    })
    assert r1.status_code == 200

    # Second update with stale version — should conflict
    r2 = await client.put(f"/tasks/{task_id}", headers=headers, json={
        "expected_version": version,  # stale!
        "title": "Update B",
    })
    assert r2.status_code == 409
    data = r2.json()
    assert data["status"] == "VERSION_CONFLICT"


@pytest.mark.anyio
async def test_delete_task(client: AsyncClient):
    token = await _get_token(client)
    headers = {"Authorization": f"Bearer {token}"}

    r = await client.post("/tasks", json={"title": "Delete me"}, headers=headers)
    task = r.json()

    r2 = await client.delete(
        f"/tasks/{task['id']}?expected_version={task['version']}",
        headers=headers
    )
    assert r2.status_code == 200

    r3 = await client.get(f"/tasks/{task['id']}", headers=headers)
    assert r3.status_code == 404


@pytest.mark.anyio
async def test_tasks_require_auth(client: AsyncClient):
    r = await client.get("/tasks")
    assert r.status_code == 401


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_health(client: AsyncClient):
    r = await client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert data["persisted_in_postgresql"] is True
    assert "scheduler" in data
    assert "object_pool" in data
