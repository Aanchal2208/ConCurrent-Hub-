1. Module Overview

The backend/ package implements the FastAPI REST API and integrates all four project modules into a single lifecycle-managed service.

Its main responsibilities are:

Request Validation (schemas.py): Validates POST /tasks and PUT /tasks/{task_id} requests using Pydantic v2. It enforces expected_version >= 1, rejects blank task titles, and maintains a consistent JSON error format.

Scheduler-First Update Routing (services.py): Routes every PUT /tasks/{task_id} request through the application-scoped PerRecordScheduler (scheduler/). It leases a reusable buffer from BoundedObjectPool (resource_pool/) and performs an atomic conditional update through the repository (database/).

Post-Commit Real-Time Broadcasting (services.py): Publishes task_created and task_updated events to connected WebSocket clients (realtime/) only after the corresponding database transaction commits.

Storage Mode Reporting (main.py): Connects to PostgreSQL when available. If PostgreSQL is unavailable during local demo startup, or DB_MODE=memory is configured, the application starts in explicitly labeled in_memory_demo mode. The /health endpoint reports "persisted_in_postgresql": false in this mode.

2. Files and Responsibilities

File

Responsibility

__init__.py

Exports app, create_app, and TaskService.

main.py

Provides the FastAPI application factory (create_app), lifespan management, and validation error formatting.

routes.py

Defines HTTP endpoints: GET /health, POST /tasks, GET /tasks, GET /tasks/{task_id}, and PUT /tasks/{task_id}.

schemas.py

Defines Pydantic v2 request and response schemas consistent with docs/api-contract.md.

services.py

Implements TaskService, integrating the scheduler, object pool, repository, and WebSocket ConnectionManager.

test_api.py

Contains Pytest tests covering HTTP status codes (201, 200, 404, 409, 422, 503), dependency injection, and WebSocket event ordering.

3. Run and Test Commands

Run these commands from the ConcurrentHub/ project root.

Start the FastAPI server

uvicorn backend.main:app --reload

The server will be available at:

http://127.0.0.1:8000

Run the API tests

pytest backend/test_api.py -v

4. Swagger UI Demonstration

Swagger UI provides an interactive interface for testing the API endpoints.

Step 1: Start the server

uvicorn backend.main:app --reload

Step 2: Open Swagger UI

Navigate to:

http://127.0.0.1:8000/docs

Step 3: Check the application health

Expand GET /health.

Click Try it out, then Execute.

Inspect the response fields:

storage_backend

persisted_in_postgresql

scheduler

object_pool

These fields provide information about the storage mode and the status of the application's supporting components.

Step 4: Create a task

Expand POST /tasks.

Click Try it out.

Enter the following request body:

{
  "title": "Prepare project report",
  "user_id": "user_1"
}

Click Execute.

Expected result: HTTP 201 Created.

Record the returned task ID. For example, the response may contain "id": 1 and "version": 1.

Step 5: Update the task successfully

Expand PUT /tasks/{task_id}.

Set task_id to the ID returned in Step 4.

Enter the following request body:

{
  "expected_version": 1,
  "title": "Complete project report",
  "user_id": "user_1"
}

Click Execute.

Expected result: HTTP 200 OK, with a response indicating:

"status": "SUCCESS"

"new_version": 2

The task version increments after a successful update.

Step 6: Test optimistic concurrency control

Repeat the same update request using "expected_version": 1.

Expected result: HTTP 409 Conflict, with a response indicating:

"status": "VERSION_CONFLICT"

"current_version": 2

The update is rejected because the task's current version is now 2, while the request expects version 1. This demonstrates stale-version detection and prevents outdated updates from silently overwriting newer data.

5. Expected Demonstration Results

Operation

Expected HTTP Status

Purpose

Check application health

200 OK

Verify application and component status.

Create a task

201 Created

Verify task creation and initial version assignment.

Retrieve tasks

200 OK

Verify task listing and retrieval.

Update a task successfully

200 OK

Verify version-checked updates.

Update a nonexistent task

404 Not Found

Verify missing-task handling.

Submit a stale-version update

409 Conflict

Verify concurrency conflict detection.

Submit invalid input

422 Unprocessable Entity

Verify request validation.

Trigger an unavailable-service condition

503 Service Unavailable

Verify service availability error handling, where applicable.

