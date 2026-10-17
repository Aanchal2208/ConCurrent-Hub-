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



def test_websocket_connect_ping_and_multi_client_broadcast() -> None:
    manager = ConnectionManager()
    app = create_standalone_ws_app(manager)

    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws1, client.websocket_connect("/ws") as ws2:
            assert manager.active_count == 2


            ws1.send_text("ping")
            assert ws1.receive_json() == {"event": "pong"}


            event = build_task_updated_event(
                task_id=101,
                version=2,
                title="Updated task",
                user_id="user_1",
            )
            client.portal.call(manager.broadcast, event)  

            msg1 = ws1.receive_json()
            msg2 = ws2.receive_json()
            assert msg1 == event
            assert msg2 == event

        assert manager.active_count == 0


def test_disconnected_or_broken_client_is_removed_without_crashing_server() -> None:

    class _HealthySocket:
        def __init__(self) -> None:
            self.received = []

        async def accept(self) -> None:
            pass

        async def send_json(self, data: Dict[str, Any]) -> None:
            self.received.append(data)

    class _BrokenSocket:
        async def accept(self) -> None:
            pass

        async def send_json(self, data: Dict[str, Any]) -> None:
            raise RuntimeError("Simulated abrupt client socket reset")

    async def _run() -> None:
        manager = ConnectionManager()
        healthy = _HealthySocket()
        broken = _BrokenSocket()

        await manager.connect(healthy)  
        await manager.connect(broken)  
        assert manager.active_count == 2

        payload = build_task_updated_event(task_id=5, version=2, title="Resilient broadcast")
        delivered = await manager.broadcast(payload)

        assert delivered == 1
        assert manager.active_count == 1
        assert len(healthy.received) == 1
        assert healthy.received[0]["title"] == "Resilient broadcast"

    asyncio.run(_run())