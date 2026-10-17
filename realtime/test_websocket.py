from __future__ import annotations

import asyncio
from typing import Any, Dict

from fastapi.testclient import TestClient

from realtime.connection_manager import ConnectionManager
from realtime.events import (
    EventType,
    build_task_created_event,
    build_task_updated_event,
    build_version_conflict_event,
)
from realtime.websocket_server import create_standalone_ws_app


def test_event_builders_match_contract() -> None:
    created_evt = build_task_created_event(
        task_id=101,
        version=1,
        title="Prepare project report",
        user_id="user_1",
    )
    assert created_evt["event"] == EventType.TASK_CREATED
    assert created_evt["task_id"] == 101
    assert created_evt["version"] == 1
    assert created_evt["title"] == "Prepare project report"

    updated_evt = build_task_updated_event(
        task_id=101,
        version=2,
        title="Updated task",
        user_id="user_1",
    )
    assert updated_evt["event"] == EventType.TASK_UPDATED
    assert updated_evt["task_id"] == 101
    assert updated_evt["version"] == 2
    assert updated_evt["title"] == "Updated task"

    conflict_evt = build_version_conflict_event(
        task_id=101,
        expected_version=1,
        current_version=2,
    )
    assert conflict_evt["event"] == EventType.VERSION_CONFLICT
    assert conflict_evt["task_id"] == 101
    assert conflict_evt["current_version"] == 2
    assert conflict_evt["modified"] is False
