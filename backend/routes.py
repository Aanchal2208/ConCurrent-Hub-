

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Path, Query, Request, status
from fastapi.responses import JSONResponse

from backend.auth import UserRepository, generate_token, verify_token
from backend.schemas import (
    AuditLogEntry,
    HealthResponse,
    LoginRequest,
    LoginResponse,
    RegisterRequest,
    TaskCreateRequest,
    TaskDeleteResponse,
    TaskResponse,
    TaskUpdateRequest,
    TaskUpdateSuccessResponse,
    TaskVersionConflictResponse,
)
from backend.services import TaskService
from database.postgres_db import DatabaseUnavailableError
from scheduler.scheduler import UpdateStatus

router = APIRouter()


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

def get_task_service(request: Request) -> TaskService:
    svc = getattr(request.app.state, "task_service", None)
    if svc is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service not initialized. Is PostgreSQL running?",
        )
    return svc


def get_user_repo(request: Request) -> UserRepository:
    repo = getattr(request.app.state, "user_repo", None)
    if repo is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="User repository not available.",
        )
    return repo


def _extract_token(authorization: Optional[str]) -> Optional[Dict[str, Any]]:
    if not authorization:
        return None
    parts = authorization.split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return verify_token(parts[1])
    return None


def get_current_user_required(
    authorization: Optional[str] = Header(None),
) -> Dict[str, Any]:
    """Require a valid Bearer token; raise 401 if missing or invalid."""
    user = _extract_token(authorization)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. Please log in.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


def get_current_user_optional(
    authorization: Optional[str] = Header(None),
) -> Optional[Dict[str, Any]]:
    """Extract user from token if present; return None otherwise."""
    return _extract_token(authorization)


def require_admin(user: Dict[str, Any] = Depends(get_current_user_required)) -> Dict[str, Any]:
    if user.get("role") != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required.")
    return user


# ---------------------------------------------------------------------------
# System health & telemetry
# ---------------------------------------------------------------------------

@router.get("/health", response_model=HealthResponse, summary="System health snapshot")
async def health_check(service: TaskService = Depends(get_task_service)) -> HealthResponse:
    return HealthResponse(**service.get_health_snapshot())


@router.get("/metrics/scheduler", summary="Per-record scheduler live metrics")
async def scheduler_metrics(
    service: TaskService = Depends(get_task_service),
    _: Dict[str, Any] = Depends(get_current_user_required),
) -> Dict[str, Any]:
    return service.scheduler.get_metrics()


@router.get("/metrics/pool", summary="Object pool live metrics")
async def pool_metrics(
    service: TaskService = Depends(get_task_service),
    _: Dict[str, Any] = Depends(get_current_user_required),
) -> Dict[str, Any]:
    return service.object_pool.get_stats()


@router.get("/audit", response_model=List[AuditLogEntry], summary="Audit log")
async def list_audit(
    limit: int = Query(default=100, ge=1, le=500),
    service: TaskService = Depends(get_task_service),
    _: Dict[str, Any] = Depends(get_current_user_required),
) -> List[AuditLogEntry]:
    records = service.get_activity_snapshot(limit=limit)
    return [AuditLogEntry(**r) for r in records]


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

@router.post("/auth/register", status_code=status.HTTP_201_CREATED, summary="Register a new user")
async def register(
    payload: RegisterRequest,
    user_repo: UserRepository = Depends(get_user_repo),
) -> Dict[str, Any]:
    try:
        user = user_repo.register(
            username=payload.username,
            password=payload.password,
            email=payload.email,
            full_name=payload.full_name or payload.username,
            role=payload.role or "editor",
        )
        return {"status": "SUCCESS", "message": f"Account created for {user['username']}", "user": user}
    except DatabaseUnavailableError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Registration is temporarily unavailable because PostgreSQL cannot be reached. "
                "Check that PostgreSQL is running and DATABASE_URL is correct."
            ),
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.post("/auth/login", response_model=LoginResponse, summary="Log in and receive a token")
async def login(
    payload: LoginRequest,
    user_repo: UserRepository = Depends(get_user_repo),
) -> LoginResponse:
    user = user_repo.authenticate(payload.username, payload.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password.",
        )
    token = generate_token(user["username"], user.get("role", "editor"))
    return LoginResponse(access_token=token, user=user)


@router.get("/auth/me", summary="Get current authenticated user profile")
async def get_me(
    user: Dict[str, Any] = Depends(get_current_user_required),
    user_repo: UserRepository = Depends(get_user_repo),
) -> Dict[str, Any]:
    profile = user_repo.get_user(user["username"])
    return {
        "authenticated": True,
        "username": user["username"],
        "role": user.get("role"),
        "user": profile,
    }


@router.get("/auth/users", summary="List all users (admin only)")
async def list_users(
    user_repo: UserRepository = Depends(get_user_repo),
    _: Dict[str, Any] = Depends(require_admin),
) -> List[Dict[str, Any]]:
    return user_repo.list_users()


# ---------------------------------------------------------------------------
# Task CRUD + Optimistic Concurrency
# ---------------------------------------------------------------------------

@router.get("/tasks", response_model=List[TaskResponse], summary="List tasks")
async def list_tasks(
    task_status: Optional[str] = Query(None, alias="status"),
    priority: Optional[str] = Query(None),
    assignee: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    sort_by: str = Query("id"),
    sort_dir: str = Query("asc"),
    service: TaskService = Depends(get_task_service),
    _: Dict[str, Any] = Depends(get_current_user_required),
) -> List[TaskResponse]:
    try:
        records = await service.list_tasks(
            status=task_status, priority=priority, assignee=assignee,
            search=search, sort_by=sort_by, sort_dir=sort_dir,
        )
        return [TaskResponse(**r) for r in records]
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail=f"Failed to list tasks: {exc}")


@router.post("/tasks", response_model=TaskResponse, status_code=status.HTTP_201_CREATED,
             summary="Create a new task")
async def create_task(
    payload: TaskCreateRequest,
    service: TaskService = Depends(get_task_service),
    current_user: Dict[str, Any] = Depends(get_current_user_required),
) -> TaskResponse:
    try:
        record = await service.create_task(
            title=payload.title,
            description=payload.description or "",
            status=payload.status or "TODO",
            priority=payload.priority or "MEDIUM",
            assignee=payload.assignee or "unassigned",
            user_id=current_user["username"],
        )
        return TaskResponse(**record)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail=f"Failed to create task: {exc}")


@router.get("/tasks/{task_id}", response_model=TaskResponse, summary="Get a task by ID")
async def get_task(
    task_id: int = Path(..., ge=1),
    service: TaskService = Depends(get_task_service),
    _: Dict[str, Any] = Depends(get_current_user_required),
) -> TaskResponse:
    record = await service.get_task(task_id)
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Task {task_id} not found")
    return TaskResponse(**record)


@router.put("/tasks/{task_id}", summary="Optimistic-concurrency update")
async def update_task(
    payload: TaskUpdateRequest,
    task_id: int = Path(..., ge=1),
    service: TaskService = Depends(get_task_service),
    current_user: Dict[str, Any] = Depends(get_current_user_required),
) -> JSONResponse:
    result = await service.update_task_via_scheduler(
        task_id=task_id,
        expected_version=payload.expected_version,
        title=payload.title,
        description=payload.description,
        status=payload.status,
        priority=payload.priority,
        assignee=payload.assignee,
        user_id=current_user["username"],
    )
    if result.status == UpdateStatus.SUCCESS:
        return JSONResponse(status_code=status.HTTP_200_OK, content=result.to_dict())
    if result.status == UpdateStatus.VERSION_CONFLICT:
        return JSONResponse(status_code=status.HTTP_409_CONFLICT, content=result.to_dict())
    if result.status == UpdateStatus.NOT_FOUND:
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content=result.to_dict())
    return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=result.to_dict())


@router.delete("/tasks/{task_id}", response_model=TaskDeleteResponse, summary="Delete a task")
async def delete_task(
    task_id: int = Path(..., ge=1),
    expected_version: Optional[int] = Query(None),
    service: TaskService = Depends(get_task_service),
    current_user: Dict[str, Any] = Depends(get_current_user_required),
) -> JSONResponse:
    res = await service.delete_task(
        task_id=task_id,
        expected_version=expected_version,
        user_id=current_user["username"],
    )
    if res.get("status") == "NOT_FOUND":
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content=res)
    if res.get("status") == "VERSION_CONFLICT":
        return JSONResponse(status_code=status.HTTP_409_CONFLICT, content=res)
    return JSONResponse(status_code=status.HTTP_200_OK, content=res)
