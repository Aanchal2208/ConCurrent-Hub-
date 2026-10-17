from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, FastAPI, WebSocket, WebSocketDisconnect

from realtime.connection_manager import ConnectionManager


def create_websocket_router(manager: ConnectionManager) -> APIRouter:
    router = APIRouter(tags=["realtime"])

    @router.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await manager.connect(websocket)
        try:
            while True:
                # Keep connection open and listen for optional client ping/messages
                message = await websocket.receive_text()
                if message.strip().lower() == "ping":
                    await websocket.send_json({"event": "pong"})
        except WebSocketDisconnect:
            await manager.disconnect(websocket)
        except Exception:
            await manager.disconnect(websocket)

    return router


