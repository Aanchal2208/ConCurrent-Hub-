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

def percentile(sorted_values: list[float], quantile: float) -> Optional[float]:
    if not sorted_values:
        return None
    index = (len(sorted_values) - 1) * quantile
    lower = int(index)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = index - lower
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * fraction


def operation_is_contended(workload: str, iteration: int) -> bool:
    if workload == "high_contention":
        return True
    return workload == "mixed_workload" and iteration % 2 == 0


def validate_local_base_url(base_url: str) -> tuple[str, int]:
    parsed = urllib.parse.urlsplit(base_url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise BenchmarkError("Base URL must be an absolute http:// or https:// URL.")
    if parsed.username or parsed.password:
        raise BenchmarkError("Do not put credentials in the base URL.")

    host = parsed.hostname
    local_addresses = {"127.0.0.1", "::1"}
    for addresses in psutil.net_if_addrs().values():
        for address in addresses:
            if address.family in (socket.AF_INET, socket.AF_INET6):
                local_addresses.add(address.address.split("%", 1)[0])
    try:
        resolved = {
            str(item[4][0]).split("%", 1)[0]
            for item in socket.getaddrinfo(host, parsed.port or (443 if parsed.scheme == "https" else 80))
        }
    except socket.gaierror as exc:
        raise BenchmarkError(f"Could not resolve base URL host {host!r}: {exc}") from exc

    is_loopback_name = host.lower() == "localhost"
    is_local_ip = bool(resolved.intersection(local_addresses))
    if not is_loopback_name and not is_local_ip:
        raise BenchmarkError(
            "This suite measures the local backend process RSS and refuses non-local hosts. "
            "Run it on the machine hosting ConcurrentHub."
        )
    return host, parsed.port or (443 if parsed.scheme == "https" else 80)


class BackendRssSampler:
    def __init__(self, port: int, interval_seconds: float) -> None:
        self.port = port
        self.interval_seconds = interval_seconds
        self.root_pids = self._find_listener_pids(port)
        self.pids: set[int] = set()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._error: Optional[str] = None
        self._samples: list[float] = []

    @staticmethod
    def _find_listener_pids(port: int) -> list[int]:
        try:
            connections = psutil.net_connections(kind="inet")
        except (psutil.AccessDenied, OSError) as exc:
            raise BenchmarkError(
                f"Cannot inspect local listening processes for RSS measurement: {exc}"
            ) from exc
        pids = sorted({
            connection.pid
            for connection in connections
            if connection.status == psutil.CONN_LISTEN
            and connection.laddr
            and connection.laddr.port == port
            and connection.pid is not None
        })
        if not pids:
            raise BenchmarkError(
                f"No local process is listening on port {port}; start the ConcurrentHub backend first."
            )
        non_python: list[str] = []
        for pid in pids:
            try:
                name = psutil.Process(pid).name().lower()
            except (psutil.NoSuchProcess, psutil.AccessDenied) as exc:
                raise BenchmarkError(f"Cannot inspect backend listener PID {pid}: {exc}") from exc
            if "python" not in name:
                non_python.append(name)
        if non_python:
            raise BenchmarkError(
                f"Port {port} is not served directly by a Python backend process: {non_python!r}."
            )
        return pids

    def _process_tree_pids(self, refresh_if_missing: bool = True) -> list[int]:
        live_roots: list[int] = []
        discovered: set[int] = set()
        for root_pid in self.root_pids:
            try:
                root = psutil.Process(root_pid)
                root.name()
                live_roots.append(root_pid)
                discovered.add(root_pid)
                for child in root.children(recursive=True):
                    try:
                        if "python" in child.name().lower():
                            discovered.add(child.pid)
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        if not live_roots:
            if not refresh_if_missing:
                raise psutil.NoSuchProcess(self.root_pids[0] if self.root_pids else 0)
            self.root_pids = self._find_listener_pids(self.port)
            return self._process_tree_pids(refresh_if_missing=False)
        self.pids.update(discovered)
        return sorted(discovered)

    def _sample(self) -> None:
        for attempt in range(2):
            try:
                pids = self._process_tree_pids()
                total_bytes = sum(psutil.Process(pid).memory_info().rss for pid in pids)
                self._samples.append(total_bytes / (1024 * 1024))
                return
            except psutil.NoSuchProcess:
                if attempt == 0:
                    continue
                self._error = "Backend process exited while sampling RSS."
            except BenchmarkError as exc:
                self._error = str(exc)
                break
            except (psutil.AccessDenied, OSError) as exc:
                self._error = f"Unable to sample backend RSS: {exc}"
                break
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            self._sample()

    def start(self) -> None:
        self._sample()
        if self._error:
            raise BenchmarkError(self._error)
        self._thread = threading.Thread(target=self._run, name="backend-rss-sampler", daemon=True)
        self._thread.start()

    def stop(self) -> float:
        self._stop.set()
        if self._thread:
            self._thread.join()
        self._sample()
        if self._error:
            raise BenchmarkError(self._error)
        if not self._samples:
            raise BenchmarkError("No backend RSS samples were collected.")
        return max(self._samples)


def _validate_payload(response: HttpResponse) -> Optional[dict[str, Any]]:
    if response.status != 200 or not isinstance(response.payload, dict):
        return None
    task_id = response.payload.get("id")
    version = response.payload.get("version")
    if not isinstance(task_id, int) or not isinstance(version, int):
        return None
    return response.payload


def _measure_api_get(client: ApiClient, task_id: int, metrics: Metrics) -> Optional[dict[str, Any]]:
    response = client.request("GET", client.task_path(task_id))
    task = _validate_payload(response)
    detail = None
    if task is None:
        detail = response.error or f"GET {client.task_path(task_id)} returned HTTP {response.status} or an invalid task body"
    metrics.record("ok" if task else "fail", response.elapsed_ms, detail)
    return task


def _measure_direct_get(repository: PostgresTaskRepository, task_id: int, metrics: Metrics) -> Optional[dict[str, Any]]:
    started = time.perf_counter()
    try:
        task = repository.get_task(task_id)
        elapsed_ms = (time.perf_counter() - started) * 1000
        metrics.record("ok" if task is not None else "fail", elapsed_ms, None if task is not None else f"Task {task_id} was not found")
        return task
    except Exception as exc:
        metrics.record("fail", (time.perf_counter() - started) * 1000, str(exc))
        return None


def _measure_update(
    mode: str,
    client: ApiClient,
    repository: PostgresTaskRepository,
    task_id: int,
    expected_version: int,
    title: str,
    username: str,
    metrics: Metrics,
) -> None:
    if mode == "Scheduler-Enabled":
        response = client.request(
            "PUT",
            client.task_path(task_id),
            {"expected_version": expected_version, "title": title},
        )
        if response.status == 200 and isinstance(response.payload, dict) and response.payload.get("status") == UpdateStatus.SUCCESS:
            metrics.record("ok", response.elapsed_ms)
        elif response.status == 409 and isinstance(response.payload, dict) and response.payload.get("status") == UpdateStatus.VERSION_CONFLICT:
            metrics.record("conf", response.elapsed_ms)
        else:
            detail = response.error or f"PUT {client.task_path(task_id)} returned HTTP {response.status}: {response.payload!r}"
            metrics.record("fail", response.elapsed_ms, detail)
        return

    started = time.perf_counter()
    try:
        result = repository.update_task(
            task_id=task_id,
            expected_version=expected_version,
            title=title,
            user_id=username,
        )
        elapsed_ms = (time.perf_counter() - started) * 1000
        if result.status == UpdateStatus.SUCCESS:
            metrics.record("ok", elapsed_ms)
        elif result.status == UpdateStatus.VERSION_CONFLICT:
            metrics.record("conf", elapsed_ms)
        else:
            metrics.record("fail", elapsed_ms, result.message)
    except Exception as exc:
        metrics.record("fail", (time.perf_counter() - started) * 1000, str(exc))


def _run_case(
    workload: str,
    mode: str,
    vus: int,
    iterations: int,
    run_id: str,
    username: str,
    client: ApiClient,
    repository: PostgresTaskRepository,
    personal_task_ids: list[int],
    shared_task_id: int,
    rss_sampler: BackendRssSampler,
    timeout: float,
) -> dict[str, Any]:
    metrics = Metrics()
    barrier = threading.Barrier(vus) if workload != "low_contention" else None
    rss_sampler.start()
    started = time.perf_counter()

    def virtual_user(user_index: int) -> None:
        for iteration in range(iterations):
            contended = operation_is_contended(workload, iteration)
            task_id = shared_task_id if contended else personal_task_ids[user_index]
            if mode == "Scheduler-Enabled":
                task = _measure_api_get(client, task_id, metrics)
            else:
                task = _measure_direct_get(repository, task_id, metrics)

            if contended and barrier is not None:
                barrier.wait(timeout=max(timeout * 2, 10.0))
            if task is None:
                continue

            title = f"bench-{run_id[:8]}-{user_index}-{iteration}"
            _measure_update(
                mode,
                client,
                repository,
                task_id,
                int(task["version"]),
                title,
                username,
                metrics,
            )

    try:
        with ThreadPoolExecutor(max_workers=vus, thread_name_prefix="benchmark-vu") as executor:
            futures = [executor.submit(virtual_user, index) for index in range(vus)]
            for future in futures:
                future.result()
    except threading.BrokenBarrierError as exc:
        raise BenchmarkError(f"{workload}/{mode}: contention synchronization failed: {exc}") from exc
    finally:
        elapsed_seconds = time.perf_counter() - started
        rss_mb = rss_sampler.stop()
        client.close_thread_clients()

    result = metrics.snapshot(elapsed_seconds, rss_mb)
    result.update({
        "workload": workload,
        "mode": mode,
        "vus": vus,
        "iterations_per_vu": iterations,
    })
    return result


def _timing_delta(before: Any, after: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(before, dict) or not isinstance(after, dict):
        return {}
    delta: dict[str, dict[str, Any]] = {}
    for name, end in after.items():
        start = before.get(name, {})
        if not isinstance(start, dict) or not isinstance(end, dict):
            continue
        count = int(end.get("count", 0)) - int(start.get("count", 0))
        total_ms = float(end.get("total_ms", 0.0)) - float(start.get("total_ms", 0.0))
        if count > 0:
            delta[name] = {
                "count": count,
                "total_ms": total_ms,
                "average_ms": total_ms / count,
            }
    return delta


def _health_timings(health: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(health, dict):
        return {}, {}
    scheduler = health.get("scheduler")
    database = health.get("database_pool")
    scheduler_timings = scheduler.get("timings", {}) if isinstance(scheduler, dict) else {}
    database_timings = database.get("timings", {}) if isinstance(database, dict) else {}
    return scheduler_timings, database_timings


def _create_task(client: ApiClient, title: str, username: str) -> int:
    payload = client.require(
        "POST",
        client.tasks_path,
        201,
        {"title": title, "description": "Temporary ConcurrentHub benchmark record", "assignee": username},
    )
    if not isinstance(payload, dict) or not isinstance(payload.get("id"), int):
        raise BenchmarkError("Task creation response did not contain an integer task ID.")
    return payload["id"]


def _cleanup_tasks(
    task_ids: set[int],
    username: str,
    repository: PostgresTaskRepository,
) -> tuple[set[int], list[str]]:
    cleaned: set[int] = set()
    errors: list[str] = []
    for task_id in sorted(task_ids):
        try:
            record = repository.get_task(task_id)
            if record is None:
                cleaned.add(task_id)
                continue
            result = repository.delete_task(
                task_id=task_id,
                expected_version=int(record["version"]),
                user_id=username,
            )
            if result.get("status") == UpdateStatus.SUCCESS:
                cleaned.add(task_id)
            else:
                errors.append(f"task {task_id}: {result.get('status', 'unknown status')}")
        except Exception as exc:
            errors.append(f"task {task_id}: {exc}")
    return cleaned, errors


def _deactivate_account(
    username: str,
    email: str,
    full_name: str,
    pool_manager: PostgresPoolManager,
) -> list[str]:
    errors: list[str] = []
    try:
        with pool_manager.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM sessions WHERE username = %s", (username,))
                cursor.execute(
                    "UPDATE users SET is_active = FALSE "
                    "WHERE username = %s AND email = %s AND full_name = %s",
                    (username, email, full_name),
                )
    except Exception as exc:
        errors.append(f"benchmark account cleanup: {exc}")
    return errors


def _pool_config(health_payload: Any, pool_manager: PostgresPoolManager) -> dict[str, Any]:
    if not isinstance(health_payload, dict):
        raise BenchmarkError("Health endpoint returned an invalid response.")
    if health_payload.get("storage_backend") != "postgres" or health_payload.get("persisted_in_postgresql") is not True:
        raise BenchmarkError("The API does not report PostgreSQL as its active persistent storage backend.")
    api_pool = health_payload.get("database_pool")
    if not isinstance(api_pool, dict) or not api_pool.get("is_open"):
        raise BenchmarkError("The API PostgreSQL connection pool is not open.")
    actual = {
        "min_size": api_pool.get("min_size"),
        "max_size": api_pool.get("max_size"),
        "timeout_seconds": api_pool.get("timeout_seconds"),
    }
    expected = {
        "min_size": pool_manager.min_size,
        "max_size": pool_manager.max_size,
        "timeout_seconds": pool_manager.timeout,
    }
    if actual != expected:
        raise BenchmarkError(
            f"Direct benchmark pool config {expected!r} differs from API pool config {actual!r}."
        )
    return actual


def _write_results(
    output_dir: Path,
    run_id: str,
    report: dict[str, Any],
) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"{run_id}.csv"
    json_path = output_dir / f"{run_id}.json"
    txt_path = output_dir / f"{run_id}.txt"

    with csv_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for result in report["results"]:
            row = {key: result.get(key) for key in CSV_FIELDS}
            for key in ("scheduler_timing_ms", "database_timing_ms"):
                row[key] = json.dumps(result.get(key, {}), sort_keys=True)
            writer.writerow(row)
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    lines = [
        "ConcurrentHub - Comparative Performance Benchmark Suite",
        f"Status: {report['status']}",
        f"Storage Backend: {report.get('storage_backend', 'unverified')}",
        f"DB Connection Pool Config: {report.get('db_pool_config', 'unavailable')}",
        f"Base URL: {report['config']['base_url']}",
        f"Iterations per VU per workload/mode: {report['config']['iterations']}",
        "",
        "Workload | Mode | VUs | Done | OK | Conf | Fail | Req/s | OK/s | P50(ms) | P95(ms) | P99(ms) | RSS(MB)",
    ]
    for row in report["results"]:
        lines.append(
            f"{row['workload']} | {row['mode']} | {row['vus']} | {row['done']} | {row['ok']} | "
            f"{row['conf']} | {row['fail']} | {row['req_per_sec']:.2f} | "
            f"{row['successful_ops_per_sec']:.2f} | {row['p50_ms']:.2f} | "
            f"{row['p95_ms']:.2f} | {row['p99_ms']:.2f} | "
            f"{row['rss_mb']:.2f}"
        )
        lines.append(f"  Scheduler timings (ms): {json.dumps(row.get('scheduler_timing_ms', {}), sort_keys=True)}")
        lines.append(f"  Database timings (ms): {json.dumps(row.get('database_timing_ms', {}), sort_keys=True)}")
    if report.get("error"):
        lines.extend(("", f"Error: {report['error']}"))
    if report.get("cleanup_errors"):
        lines.extend(("", "Cleanup errors:"))
        lines.extend(f"- {item}" for item in report["cleanup_errors"])
    lines.extend(("", f"Backend listener PID(s): {report.get('backend_pids', [])}"))
    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return csv_path, json_path, txt_path


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare ConcurrentHub's direct PostgreSQL repository path with its real scheduler API path."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--vus", nargs="+", type=positive_int, default=[10, 50, 100])
    parser.add_argument("--iterations", type=positive_int, default=100, help="update cycles per VU in each workload/mode")
    parser.add_argument("--timeout", type=float, default=15.0, help="HTTP and contention barrier timeout in seconds")
    parser.add_argument("--sample-interval", type=float, default=0.1, help="backend RSS sampling interval in seconds")
    parser.add_argument("--health-path", default="/health")
    parser.add_argument("--register-path", default="/auth/register")
    parser.add_argument("--login-path", default="/auth/login")
    parser.add_argument("--tasks-path", default="/tasks")
    parser.add_argument("--task-path-template", default="/tasks/{task_id}")
    return parser


def run(args: argparse.Namespace) -> dict[str, Any]:
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"concurrenthub-benchmark-{run_stamp}-{uuid.uuid4().hex[:8]}"
    report: dict[str, Any] = {
        "run_id": run_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "status": "failed",
        "storage_backend": "unverified",
        "db_pool_config": None,
        "backend_pids": [],
        "config": {
            "base_url": args.base_url,
            "vus": args.vus,
            "iterations": args.iterations,
            "timeout_seconds": args.timeout,
            "rss_sample_interval_seconds": args.sample_interval,
            "health_path": args.health_path,
            "register_path": args.register_path,
            "login_path": args.login_path,
            "tasks_path": args.tasks_path,
            "task_path_template": args.task_path_template,
        },
        "workloads": {
            "low_contention": "Each VU updates its own task.",
            "high_contention": "All VUs update one shared task using the version read before a synchronized update wave.",
            "mixed_workload": "Even iterations target one shared task; odd iterations target each VU's own task.",
        },
        "metric_definitions": {
            "done": "Completed HTTP or repository-call attempts, including attempts ending in errors.",
            "ok": "Successful task reads and updates.",
            "conf": "Expected VERSION_CONFLICT outcomes from task updates only.",
            "fail": "Unexpected status, invalid response, repository error, or transport failure.",
            "req_per_sec": "Done divided by total wall-clock benchmark duration; GET and update calls are counted separately.",
            "successful_ops_per_sec": "Successful task reads and updates divided by total wall-clock benchmark duration.",
            "latency_percentiles": "Linear-interpolated percentiles across individual GET and update call latencies.",
            "stage_timings": "Per-case deltas of bounded scheduler and PostgreSQL pool timing aggregates; average_ms is total_ms/count.",
            "rss_mb": "Peak sampled RSS in MiB for Python process(es) listening on the configured API port; benchmark runner RSS is excluded.",
        },
        "results": [],
        "cleanup_errors": [],
        "error": None,
    }
    pool_manager: Optional[PostgresPoolManager] = None
    repository: Optional[PostgresTaskRepository] = None
    client: Optional[ApiClient] = None
    run_user = ""
    run_email = ""
    run_full_name = "ConcurrentHub load benchmark"
    all_task_ids: set[int] = set()
    account_creation_attempted = False

    try:
        _, port = validate_local_base_url(args.base_url)
        if args.timeout <= 0 or args.sample_interval <= 0:
            raise BenchmarkError("Timeout and RSS sample interval must be greater than zero.")
        if any(vus > 500 for vus in args.vus):
            raise BenchmarkError("This benchmark caps concurrency at 500 VUs per workload.")
        if args.iterations > 10000:
            raise BenchmarkError("This benchmark caps iterations at 10,000 per VU.")
        client = ApiClient(
            args.base_url,
            args.timeout,
            args.health_path,
            args.register_path,
            args.login_path,
            args.tasks_path,
            args.task_path_template,
        )
        health = client.require("GET", args.health_path, 200)
        if not isinstance(health, dict) or health.get("status") != "ok":
            raise BenchmarkError("ConcurrentHub health endpoint did not report status=ok.")

        pool_manager = PostgresPoolManager(open_immediately=True)
        repository = PostgresTaskRepository(pool_manager)
        if not pool_manager.check_connectivity():
            raise BenchmarkError("Direct PostgreSQL connectivity check failed; no valid comparison can be run.")
        report["db_pool_config"] = _pool_config(health, pool_manager)
        report["storage_backend"] = health["storage_backend"]

        rss_sampler = BackendRssSampler(port, args.sample_interval)
        report["backend_pids"] = rss_sampler.root_pids

        run_user = f"loadbench_{uuid.uuid4().hex[:16]}"
        run_email = f"{run_user}@example.test"
        password = secrets.token_urlsafe(24)
        account_creation_attempted = True
        client.require(
            "POST",
            args.register_path,
            201,
            {"username": run_user, "password": password, "email": run_email, "full_name": run_full_name, "role": "editor"},
        )
        login = client.require(
            "POST",
            args.login_path,
            200,
            {"username": run_user, "password": password},
        )
        if not isinstance(login, dict) or not isinstance(login.get("access_token"), str):
            raise BenchmarkError("Login response did not include an access token.")
        client.token = login["access_token"]

        marker_id = _create_task(client, f"benchmark-preflight-{run_id}", run_user)
        all_task_ids.add(marker_id)
        marker = repository.get_task(marker_id)
        if not marker or marker.get("title") != f"benchmark-preflight-{run_id}":
            raise BenchmarkError(
                "The task created through the API was not visible through the direct PostgreSQL repository. "
                "The API and direct baseline do not share the same database."
            )
        result = repository.delete_task(marker_id, int(marker["version"]), run_user)
        if result.get("status") != UpdateStatus.SUCCESS:
            raise BenchmarkError(f"Could not remove database identity preflight task {marker_id}.")
        all_task_ids.remove(marker_id)

        for vus in args.vus:
            for workload in WORKLOADS:
                for mode in MODES:
                    case_tasks: set[int] = set()
                    shared_id = _create_task(
                        client,
                        f"benchmark-{run_id[-8:]}-{workload}-{mode[:3]}-shared",
                        run_user,
                    )
                    case_tasks.add(shared_id)
                    all_task_ids.add(shared_id)
                    personal_ids: list[int] = []
                    for index in range(vus):
                        task_id = _create_task(
                            client,
                            f"benchmark-{run_id[-8:]}-{workload}-{index}",
                            run_user,
                        )
                        personal_ids.append(task_id)
                        case_tasks.add(task_id)
                        all_task_ids.add(task_id)
                    if mode == "Scheduler-Enabled":
                        timing_health_before = client.require("GET", args.health_path, 200)
                        scheduler_before, database_before = _health_timings(timing_health_before)
                    else:
                        scheduler_before = {}
                        database_before = pool_manager.get_pool_stats().get("timings", {})
                    result = _run_case(
                        workload,
                        mode,
                        vus,
                        args.iterations,
                        run_id,
                        run_user,
                        client,
                        repository,
                        personal_ids,
                        shared_id,
                        rss_sampler,
                        args.timeout,
                    )
                    if mode == "Scheduler-Enabled":
                        timing_health_after = client.require("GET", args.health_path, 200)
                        scheduler_after, database_after = _health_timings(timing_health_after)
                        result["scheduler_timing_ms"] = _timing_delta(
                            scheduler_before, scheduler_after
                        )
                        result["database_timing_ms"] = _timing_delta(
                            database_before, database_after
                        )
                    else:
                        result["scheduler_timing_ms"] = {}
                        result["database_timing_ms"] = _timing_delta(
                            database_before, pool_manager.get_pool_stats().get("timings", {})
                        )
                    report["results"].append(result)
                    report["backend_pids"] = sorted(rss_sampler.pids)
                    cleaned, cleanup = _cleanup_tasks(case_tasks, run_user, repository)
                    report["cleanup_errors"].extend(cleanup)
                    all_task_ids.difference_update(cleaned)
                    if cleanup:
                        raise BenchmarkError(f"Failed to clean up benchmark records: {cleanup!r}")

        if not pool_manager.check_connectivity():
            raise BenchmarkError("Post-run PostgreSQL connectivity check failed.")
        final_health = client.require("GET", args.health_path, 200)
        _pool_config(final_health, pool_manager)
        report["status"] = "completed"
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if pool_manager and repository and account_creation_attempted:
            cleaned, cleanup_errors = _cleanup_tasks(all_task_ids, run_user, repository)
            all_task_ids.difference_update(cleaned)
            report["cleanup_errors"].extend(cleanup_errors)
            if not all_task_ids:
                report["cleanup_errors"].extend(
                    _deactivate_account(run_user, run_email, run_full_name, pool_manager)
                )
            else:
                report["cleanup_errors"].append(
                    "Benchmark account left active because one or more test tasks could not be removed."
                )
        if report["cleanup_errors"]:
            report["status"] = "failed"
        if pool_manager:
            pool_manager.close()
        if client:
            client.close_thread_clients()
        report["completed_at"] = datetime.now(timezone.utc).isoformat()

    return report


def main() -> int:
    args = build_parser().parse_args()
    output_dir = Path(__file__).resolve().parent / "results"
    report = run(args)
    paths = _write_results(output_dir, report["run_id"], report)
    print()
    print(paths[2].read_text(encoding="utf-8"))
    print("Saved results:")
    print(f"  CSV:  {paths[0]}")
    print(f"  JSON: {paths[1]}")
    print(f"  TXT:  {paths[2]}")
    return 0 if report["status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
