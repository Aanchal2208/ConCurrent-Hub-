from __future__ import annotations

import argparse
import csv
import json
import secrets
import socket
import sys
import threading
import time
import urllib.parse
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import psutil
import httpx
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from database.postgres_db import PostgresPoolManager  # noqa: E402
from database.repository import PostgresTaskRepository  # noqa: E402
from scheduler.scheduler import UpdateStatus  # noqa: E402


WORKLOADS = ("low_contention", "high_contention", "mixed_workload")
MODES = ("Baseline (Direct)", "Scheduler-Enabled")
CSV_FIELDS = (
    "workload",
    "mode",
    "vus",
    "iterations_per_vu",
    "done",
    "ok",
    "conf",
    "fail",
    "req_per_sec",
    "successful_ops_per_sec",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "rss_mb",
    "elapsed_seconds",
    "scheduler_timing_ms",
    "database_timing_ms",
)


class BenchmarkError(RuntimeError):
    pass


@dataclass(frozen=True)
class HttpResponse:
    status: Optional[int]
    payload: Any
    elapsed_ms: float
    error: Optional[str] = None


class ApiClient:
    def __init__(
        self,
        base_url: str,
        timeout: float,
        health_path: str,
        register_path: str,
        login_path: str,
        tasks_path: str,
        task_path_template: str,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.health_path = health_path
        self.register_path = register_path
        self.login_path = login_path
        self.tasks_path = tasks_path
        self.task_path_template = task_path_template
        self.token: Optional[str] = None
        self._thread_local = threading.local()
        self._clients: list[httpx.Client] = []
        self._clients_lock = threading.Lock()

    def task_path(self, task_id: int) -> str:
        return self.task_path_template.format(task_id=task_id)

    def _client(self) -> httpx.Client:
        client = getattr(self._thread_local, "client", None)
        if client is None:
            client = httpx.Client(timeout=self.timeout, trust_env=False)
            self._thread_local.client = client
            with self._clients_lock:
                self._clients.append(client)
        return client

    def close_thread_clients(self) -> None:
        with self._clients_lock:
            clients = self._clients
            self._clients = []
        for client in clients:
            client.close()
        if hasattr(self._thread_local, "client"):
            del self._thread_local.client

    def request(self, method: str, path: str, body: Optional[dict[str, Any]] = None) -> HttpResponse:
        url = f"{self.base_url}/{path.lstrip('/')}"
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        started = time.perf_counter()
        try:
            response = self._client().request(method, url, json=body, headers=headers)
        except httpx.HTTPError as exc:
            elapsed_ms = (time.perf_counter() - started) * 1000
            return HttpResponse(None, None, elapsed_ms, str(exc))

        elapsed_ms = (time.perf_counter() - started) * 1000
        try:
            payload = response.json() if response.content else None
        except (UnicodeDecodeError, json.JSONDecodeError):
            payload = response.text
        return HttpResponse(response.status_code, payload, elapsed_ms)

    def require(self, method: str, path: str, expected_status: int, body: Optional[dict[str, Any]] = None) -> Any:
        response = self.request(method, path, body)
        if response.status != expected_status:
            detail = response.error or response.payload
            raise BenchmarkError(
                f"{method} {path}: expected HTTP {expected_status}, "
                f"received {response.status}; response={detail!r}"
            )
        return response.payload

@dataclass
class Metrics:
    lock: threading.Lock = field(default_factory=threading.Lock)
    done: int = 0
    ok: int = 0
    conf: int = 0
    fail: int = 0
    latencies_ms: list[float] = field(default_factory=list)
    failure_samples: list[str] = field(default_factory=list)

    def record(self, outcome: str, elapsed_ms: float, detail: Optional[str] = None) -> None:
        with self.lock:
            self.done += 1
            if outcome == "ok":
                self.ok += 1
            elif outcome == "conf":
                self.conf += 1
            else:
                self.fail += 1
                if detail and len(self.failure_samples) < 20:
                    self.failure_samples.append(detail)
            self.latencies_ms.append(elapsed_ms)

    def snapshot(self, elapsed_seconds: float, rss_mb: float) -> dict[str, Any]:
        values = sorted(self.latencies_ms)
        return {
            "done": self.done,
            "ok": self.ok,
            "conf": self.conf,
            "fail": self.fail,
            "req_per_sec": self.done / elapsed_seconds if elapsed_seconds else 0.0,
            "successful_ops_per_sec": self.ok / elapsed_seconds if elapsed_seconds else 0.0,
            "p50_ms": percentile(values, 0.50),
            "p95_ms": percentile(values, 0.95),
            "p99_ms": percentile(values, 0.99),
            "rss_mb": rss_mb,
            "elapsed_seconds": elapsed_seconds,
            "failure_samples": list(self.failure_samples),
        }
