from typing import Any, Dict, Optional


class EventType:
    """Supported WebSocket event names."""

    TASK_CREATED = "task_created"
    TASK_UPDATED = "task_updated"
    VERSION_CONFLICT = "version_conflict"
    SERVER_ERROR = "server_error"


def build_task_created_event(
    task_id: int,
    version: int,
    title: str,
    user_id: str = "anonymous",
) -> Dict[str, Any]:
    """Build a `task_created` broadcast payload."""
    return {
        "event": EventType.TASK_CREATED,
        "task_id": task_id,
        "version": version,
        "title": title,
        "user_id": user_id,
    }


def build_task_updated_event(
    task_id: int,
    version: int,
    title: str,
    user_id: str = "anonymous",
) -> Dict[str, Any]:
    """
    Build a `task_updated` broadcast payload.
    Matches the required schema:
      {
        "event": "task_updated",
        "task_id": 101,
        "version": 2,
        "title": "Updated task"
      }
    """
    return {
        "event": EventType.TASK_UPDATED,
        "task_id": task_id,
        "version": version,
        "title": title,
        "user_id": user_id,
    }


def build_version_conflict_event(
    task_id: int,
    expected_version: int,
    current_version: int,
    message: str = "The task was modified by another request",
) -> Dict[str, Any]:
    """
    Build a `version_conflict` notification payload.
    Explicitly indicates `modified: False` so clients never assume the rejected
    write modified persistent state.
    """
    return {
        "event": EventType.VERSION_CONFLICT,
        "task_id": task_id,
        "expected_version": expected_version,
        "current_version": current_version,
        "modified": False,
        "message": message,
    }


def build_server_error_event(
    message: str,
    task_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Build a `server_error` notification payload."""
    payload: Dict[str, Any] = {
        "event": EventType.SERVER_ERROR,
        "message": message,
    }
    if task_id is not None:
        payload["task_id"] = task_id
    return payload