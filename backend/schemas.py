

from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator



class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)


class RegisterRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=8, max_length=128)
    email: str = Field(..., min_length=5, max_length=255)
    full_name: Optional[str] = Field(default="", max_length=200)
    role: Optional[str] = Field(default="editor")

    @field_validator("username")
    @classmethod
    def validate_username(cls, v: str) -> str:
        v = v.strip().lower()
        if not v.replace("_", "").replace("-", "").isalnum():
            raise ValueError("Username may only contain letters, numbers, hyphens, and underscores.")
        return v

    @field_validator("role")
    @classmethod
    def validate_role(cls, v: Optional[str]) -> str:
        if v not in ("admin", "editor", "viewer"):
            return "editor"
        return v


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: Dict[str, Any]


class UserProfile(BaseModel):
    username: str
    email: str
    full_name: str
    role: str
    is_active: bool
    created_at: Optional[str] = None
    last_login: Optional[str] = None



class TaskCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = Field(..., min_length=1, max_length=500)
    description: Optional[str] = Field(default="", max_length=2000)
    status: Optional[str] = Field(default="TODO")
    priority: Optional[str] = Field(default="MEDIUM")
    assignee: Optional[str] = Field(default="unassigned", max_length=100)

    @field_validator("title")
    @classmethod
    def non_blank_title(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Title must not be blank.")
        return v

    @field_validator("status")
    @classmethod
    def valid_status(cls, v: Optional[str]) -> str:
        if not v:
            return "TODO"
        v = v.upper().strip()
        return v if v in ("TODO", "IN_PROGRESS", "REVIEW", "DONE") else "TODO"

    @field_validator("priority")
    @classmethod
    def valid_priority(cls, v: Optional[str]) -> str:
        if not v:
            return "MEDIUM"
        v = v.upper().strip()
        return v if v in ("LOW", "MEDIUM", "HIGH", "CRITICAL") else "MEDIUM"


class TaskUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_version: int = Field(..., ge=1, description="Optimistic concurrency version")
    title: Optional[str] = Field(default=None, min_length=1, max_length=500)
    description: Optional[str] = Field(default=None, max_length=2000)
    status: Optional[str] = Field(default=None)
    priority: Optional[str] = Field(default=None)
    assignee: Optional[str] = Field(default=None, max_length=100)

    @field_validator("title")
    @classmethod
    def non_blank_title(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v = v.strip()
        if not v:
            raise ValueError("Title must not be blank.")
        return v

    @field_validator("status")
    @classmethod
    def valid_status(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v = v.upper().strip()
        return v if v in ("TODO", "IN_PROGRESS", "REVIEW", "DONE") else None

    @field_validator("priority")
    @classmethod
    def valid_priority(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v = v.upper().strip()
        return v if v in ("LOW", "MEDIUM", "HIGH", "CRITICAL") else None


class TaskResponse(BaseModel):
    id: int
    title: str
    description: Optional[str] = ""
    status: str = "TODO"
    priority: str = "MEDIUM"
    assignee: str = "unassigned"
    version: int
    user_id: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class TaskUpdateSuccessResponse(BaseModel):
    status: str = "SUCCESS"
    task_id: int
    new_version: int
    message: str = "Task updated successfully"
    task: Optional[TaskResponse] = None


class TaskVersionConflictResponse(BaseModel):
    status: str = "VERSION_CONFLICT"
    task_id: int
    current_version: int
    message: str = "The task was modified by another user"
    task: Optional[TaskResponse] = None


class TaskDeleteResponse(BaseModel):
    status: str = "SUCCESS"
    task_id: int
    message: str = "Task deleted successfully"



class AuditLogEntry(BaseModel):
    id: Optional[int] = None
    timestamp: str
    actor: str
    task_id: Optional[int] = None
    operation: str
    old_version: Optional[int] = None
    new_version: Optional[int] = None
    outcome: str
    details: Optional[str] = ""


class HealthResponse(BaseModel):
    status: str
    storage_backend: str
    persisted_in_postgresql: bool
    active_websocket_clients: int
    scheduler: Dict[str, Any]
    object_pool: Dict[str, Any]
    database_pool: Optional[Dict[str, Any]] = None
    auth_status: Dict[str, Any]
