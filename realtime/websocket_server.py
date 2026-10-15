from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, FastAPI, WebSocket, WebSocketDisconnect

from realtime.connection_manager import ConnectionManager


def create_websocket_router(manager: ConnectionManager) -> APIRouter:
    """
    Create an `APIRouter` with the `/ws` endpoint bound to `manager`.
    """
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


def create_standalone_ws_app(manager: Optional[ConnectionManager] = None) -> FastAPI:
    """
    Create a lightweight standalone FastAPI app containing `/ws` for isolated
    WebSocket unit tests.
    """
    conn_manager = manager or ConnectionManager()
    app = FastAPI(title="ConcurrentHub Standalone WebSocket Server")
    app.state.connection_manager = conn_manager
    app.include_router(create_websocket_router(conn_manager))
    return app