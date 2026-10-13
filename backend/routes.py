
from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, Path, Request, status
from fastapi.responses import JSONResponse

from backend.schemas import (
    HealthResponse,
    TaskCreateRequest,
    TaskResponse,
    TaskUpdateRequest,
    TaskUpdateSuccessResponse,
)
from backend.services import TaskService
from scheduler.scheduler import UpdateStatus


def get_task_service(request: Request) -> TaskService:
    return request.app.state.task_service


router = APIRouter()


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Inspect service, scheduler, pool, and storage mode health",
)
async def health_check(
    service: TaskService = Depends(get_task_service),
) -> HealthResponse:
    return HealthResponse(**service.get_health_snapshot())


@router.post(
    "/tasks",
    response_model=TaskResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new collaborative task at version 1",
)
async def create_task(
    payload: TaskCreateRequest,
    service: TaskService = Depends(get_task_service),
):
    try:
        record = await service.create_task(
            title=payload.title,
            user_id=payload.user_id,
        )
        return TaskResponse(**record)

    except ValueError as exc:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                "status": "VALIDATION_ERROR",
                "message": str(exc),
            },
        )

    except Exception as exc:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "ERROR",
                "message": f"Failed to create task: {exc}",
            },
        )


@router.get(
    "/tasks",
    response_model=List[TaskResponse],
    status_code=status.HTTP_200_OK,
    summary="List all tasks ordered by ID",
)
async def list_tasks(
    service: TaskService = Depends(get_task_service),
):
    try:
        records = await service.list_tasks()
        return [TaskResponse(**record) for record in records]

    except Exception as exc:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "ERROR",
                "message": f"Failed to list tasks: {exc}",
            },
        )


@router.get(
    "/tasks/{task_id}",
    response_model=TaskResponse,
    status_code=status.HTTP_200_OK,
    summary="Retrieve a single task by ID",
)
async def get_task(
    task_id: int = Path(..., ge=1, description="Task primary key ID"),
    service: TaskService = Depends(get_task_service),
):
    try:
        record = await service.get_task(task_id)

    except Exception as exc:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "ERROR",
                "task_id": task_id,
                "message": f"Failed to retrieve task {task_id}: {exc}",
            },
        )

    if record is None:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={
                "status": "NOT_FOUND",
                "task_id": task_id,
                "message": f"Task {task_id} not found",
            },
        )

    return TaskResponse(**record)


@router.put(
    "/tasks/{task_id}",
    response_model=TaskUpdateSuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="Schedule and execute an optimistic version-checked task update",
)
async def update_task(
    payload: TaskUpdateRequest,
    task_id: int = Path(..., ge=1, description="Task primary key ID"),
    service: TaskService = Depends(get_task_service),
):
    result = await service.update_task_via_scheduler(
        task_id=task_id,
        expected_version=payload.expected_version,
        title=payload.title,
        user_id=payload.user_id,
    )

    if result.status == UpdateStatus.SUCCESS:
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=result.to_dict(),
        )

    if result.status == UpdateStatus.VERSION_CONFLICT:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content=result.to_dict(),
        )

    if result.status == UpdateStatus.NOT_FOUND:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content=result.to_dict(),
        )

    http_code = (
        status.HTTP_503_SERVICE_UNAVAILABLE
        if "unavailable" in result.message.lower()
        else status.HTTP_500_INTERNAL_SERVER_ERROR
    )

    return JSONResponse(
        status_code=http_code,
        content=result.to_dict(),
    )
