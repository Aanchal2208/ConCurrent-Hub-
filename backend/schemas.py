
from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TaskCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Task title",
    )
    user_id: str = Field(
        default="user_1",
        min_length=1,
        max_length=100,
        description="Identifier of the acting user",
    )

    @field_validator("title")
    @classmethod
    def validate_non_blank_title(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Task title must not be blank or whitespace only.")
        return cleaned


class TaskUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_version: int = Field(
        ...,
        ge=1,
        description="Optimistic concurrency expected version (>= 1)",
    )
    title: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Updated task title",
    )
    user_id: str = Field(
        default="user_1",
        min_length=1,
        max_length=100,
        description="Identifier of the acting user",
    )

    @field_validator("title")
    @classmethod
    def validate_non_blank_title(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Task title must not be blank or whitespace only.")
        return cleaned


class TaskResponse(BaseModel):
    id: int
    title: str
    version: int
    user_id: Optional[str] = "user_1"
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class TaskUpdateSuccessResponse(BaseModel):
    status: str = "SUCCESS"
    task_id: int
    new_version: int
    message: str = "Task updated successfully"


class TaskVersionConflictResponse(BaseModel):
    status: str = "VERSION_CONFLICT"
    task_id: int
    current_version: int
    message: str = "The task was modified by another request"


class ErrorResponse(BaseModel):
    status: str
    message: str
    task_id: Optional[int] = None
    details: Optional[Any] = None


class HealthResponse(BaseModel):
    status: str
    storage_backend: str
    persisted_in_postgresql: bool
    active_websocket_clients: int
    scheduler: Dict[str, Any]
    object_pool: Dict[str, Any]
    database_pool: Optional[Dict[str, Any]] = None
