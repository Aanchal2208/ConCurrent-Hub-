from __future__ import annotations

import asyncio
from typing import Any, Dict, List

from fastapi import WebSocket


class ConnectionManager:

    def __init__(self) -> None:
        self._active_connections: List[WebSocket] = []
        self._lock = asyncio.Lock()
        self._broadcast_count = 0

    @property
    def active_count(self) -> int:
        return len(self._active_connections)

    @property
    def broadcast_count(self) -> int:
        return self._broadcast_count

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            if websocket not in self._active_connections:
                self._active_connections.append(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            if websocket in self._active_connections:
                self._active_connections.remove(websocket)

    async def broadcast(self, event: Dict[str, Any]) -> int:
        async with self._lock:
            snapshot = list(self._active_connections)

        delivered = 0
        failed_sockets: List[WebSocket] = []

        for connection in snapshot:
            try:
                await connection.send_json(event)
                delivered += 1
            except Exception:
                failed_sockets.append(connection)

        if failed_sockets:
            async with self._lock:
                for dead_ws in failed_sockets:
                    if dead_ws in self._active_connections:
                        self._active_connections.remove(dead_ws)

        self._broadcast_count += 1
        return delivered