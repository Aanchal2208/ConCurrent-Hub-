from __future__ import annotations

import argparse
import asyncio
import json
from fastapi.testclient import TestClient
import httpx
import websockets

from backend.main import create_app


def run_inprocess_two_client_demo() -> None:
    print("=" * 72)
    print("ConcurrentHub Phase 2 — Two-Client WebSocket Broadcast Demo")
    print("Owner: Angel Odilo (Member 4)")
    print("=" * 72)
    print("Note: Running self-contained two-client demonstration against the")
    print("      integrated FastAPI + WebSocket (`/ws`) application.")
    print("      (To run against an external Uvicorn server, start")
    print("       `uvicorn backend.main:app --reload` and pass `--live`.)\n")

    app = create_app(use_in_memory_db=True)

    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws_client_a, client.websocket_connect(
            "/ws"
        ) as ws_client_b:
            print("[Step 1] Connected Client A and Client B to `/ws`.")

            # 1. Create a task via POST /tasks
            print("\n[Step 2] Sending POST /tasks {'title': 'Prepare project report', 'user_id': 'user_1'}...")
            create_resp = client.post(
                "/tasks",
                json={"title": "Prepare project report", "user_id": "user_1"},
            )
            created_task = create_resp.json()
            task_id = created_task["id"]
            print(f"  -> HTTP {create_resp.status_code}: {created_task}")

            evt_a1 = ws_client_a.receive_json()
            evt_b1 = ws_client_b.receive_json()
            print(f"  -> [WS Client A received]: {json.dumps(evt_a1)}")
            print(f"  -> [WS Client B received]: {json.dumps(evt_b1)}")

            # 2. Update the task via PUT /tasks/{task_id} with valid expected_version=1
            print(f"\n[Step 3] Sending PUT /tasks/{task_id} (expected_version=1 -> valid update)...")
            update_resp = client.put(
                f"/tasks/{task_id}",
                json={
                    "expected_version": 1,
                    "title": "Complete project report",
                    "user_id": "user_1",
                },
            )
            print(f"  -> HTTP {update_resp.status_code}: {update_resp.json()}")

            evt_a2 = ws_client_a.receive_json()
            evt_b2 = ws_client_b.receive_json()
            print(f"  -> [WS Client A received]: {json.dumps(evt_a2)}")
            print(f"  -> [WS Client B received]: {json.dumps(evt_b2)}")

            # 3. Trigger a stale version conflict (expected_version=1 when current is 2)
            print(f"\n[Step 4] Sending PUT /tasks/{task_id} (expected_version=1 -> stale version conflict)...")
            conflict_resp = client.put(
                f"/tasks/{task_id}",
                json={
                    "expected_version": 1,
                    "title": "Overwrite with stale version",
                    "user_id": "user_2",
                },
            )
            print(f"  -> HTTP {conflict_resp.status_code}: {conflict_resp.json()}")

            evt_a3 = ws_client_a.receive_json()
            evt_b3 = ws_client_b.receive_json()
            print(f"  -> [WS Client A received]: {json.dumps(evt_a3)}")
            print(f"  -> [WS Client B received]: {json.dumps(evt_b3)}")

    print("\n[Done] Both WebSocket clients disconnected cleanly.")
    print("=" * 72)


async def run_live_two_client_demo(http_base: str, ws_url: str) -> None:
    print("=" * 72)
    print(f"Connecting two live WebSocket clients to {ws_url}...")
    print("=" * 72)

    async with websockets.connect(ws_url) as ws_a, websockets.connect(ws_url) as ws_b:
        async with httpx.AsyncClient(base_url=http_base, timeout=5.0) as http_client:
            r1 = await http_client.post(
                "/tasks",
                json={"title": "Prepare project report", "user_id": "user_1"},
            )
            task = r1.json()
            task_id = task["id"]
            print(f"Created task #{task_id}: {task}")
            print("Client A received:", await asyncio.wait_for(ws_a.recv(), timeout=3.0))
            print("Client B received:", await asyncio.wait_for(ws_b.recv(), timeout=3.0))

            r2 = await http_client.put(
                f"/tasks/{task_id}",
                json={
                    "expected_version": 1,
                    "title": "Complete project report",
                    "user_id": "user_1",
                },
            )
            print(f"Updated task #{task_id}: {r2.json()}")
            print("Client A received:", await asyncio.wait_for(ws_a.recv(), timeout=3.0))
            print("Client B received:", await asyncio.wait_for(ws_b.recv(), timeout=3.0))


def main() -> None:
    parser = argparse.ArgumentParser(description="ConcurrentHub Two-Client WebSocket Demo")
    parser.add_argument(
        "--live",
        action="store_true",
        help="Connect to an already running server at http://127.0.0.1:8000 instead of in-process mode.",
    )
    parser.add_argument("--http-url", default="http://127.0.0.1:8000")
    parser.add_argument("--ws-url", default="ws://127.0.0.1:8000/ws")
    args = parser.parse_args()

    if args.live:
        asyncio.run(run_live_two_client_demo(args.http_url, args.ws_url))
    else:
        run_inprocess_two_client_demo()


if __name__ == "__main__":
    main()