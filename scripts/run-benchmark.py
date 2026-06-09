#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = []
# [tool.uv]
# exclude-newer = "2026-06-09T00:00:00Z"
# ///
"""Run an Online Boutique curl benchmark and collect latency/resource data."""

import argparse
import csv
import json
import math
import os
import queue
import random
import re
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from typing import TextIO, TypedDict


DEFAULT_OUTPUT_ROOT = "/local/benchmark-results"
DEFAULT_TARGET_RPS = 5.0
DEFAULT_DURATION_SECONDS = 60.0
DEFAULT_WARMUP_SECONDS = 10.0
DEFAULT_CONCURRENCY = 8
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30.0
DEFAULT_REQUEST_PATHS = "/"
DEFAULT_RTT_SAMPLES = 10
DEFAULT_RTT_INTERVAL_SECONDS = 1.0
DEFAULT_USER_AGENT = "cloudlab-online-boutique-benchmark/1.0"
DEFAULT_TRACE_LIMIT = 500
DEFAULT_TRACE_LOOKBACK = "1h"
DEFAULT_TRACE_SERVICE = "all"
DEFAULT_TRACE_FLUSH_WAIT_SECONDS = 10.0
DEFAULT_RESOURCE_WINDOW = "5m"

type CsvValue = str | int | float | None
type CsvRow = dict[str, CsvValue]
type JsonObject = Mapping[str, object]
type TomlScalar = str | int | float | bool | None
type TomlValue = TomlScalar | list[TomlScalar] | dict[str, "TomlValue"]
type TomlTable = dict[str, TomlValue]


class RequestPath(TypedDict):
    path: str
    weight: float


class BenchmarkConfig(TypedDict):
    target_rps: float
    duration_seconds: float
    warmup_seconds: float
    concurrency: int
    request_timeout_seconds: float
    request_paths: list[RequestPath]
    rtt_samples: int
    rtt_interval_seconds: float
    trace_service: str
    trace_limit: int
    trace_lookback: str
    trace_flush_wait_seconds: float
    resource_window: str
    user_agent: str
    random_seed: int


class CurlSample(TypedDict, total=False):
    task_id: int
    worker_id: int
    timestamp: str
    scheduled_offset_ms: float | None
    launch_delay_ms: float
    path: str
    url: str
    http_code: int | None
    wall_time_ms: float
    dns_ms: float | None
    tcp_connect_ms: float | None
    tls_handshake_ms: float | None
    request_time_ms: float | None
    time_to_first_byte_ms: float | None
    response_time_ms: float | None
    response_transfer_ms: float | None
    size_download_bytes: int | None
    error: str


class BenchmarkTask(TypedDict):
    task_id: int
    scheduled_at: float
    scheduled_offset_seconds: float
    path: str


class LoadResult(TypedDict):
    label: str
    samples: list[CurlSample]
    started_at: str
    finished_at: str
    wall_seconds: float
    scheduled_requests: int


class SummaryStats(TypedDict):
    count: int
    min: float | None
    avg: float | None
    p50: float | None
    p90: float | None
    p95: float | None
    p99: float | None
    max: float | None

CONTAINER_COUNTER_METRICS = [
    "container_cpu_cfs_periods_total",
    "container_cpu_cfs_throttled_periods_total",
    "container_cpu_cfs_throttled_seconds_total",
    "container_cpu_system_seconds_total",
    "container_cpu_usage_seconds_total",
    "container_cpu_user_seconds_total",
    "container_fs_reads_bytes_total",
    "container_fs_reads_total",
    "container_fs_writes_bytes_total",
    "container_fs_writes_total",
    "container_memory_failcnt",
    "container_memory_failures_total",
    "container_network_receive_bytes_total",
    "container_network_receive_errors_total",
    "container_network_receive_packets_dropped_total",
    "container_network_receive_packets_total",
    "container_network_transmit_bytes_total",
    "container_network_transmit_errors_total",
    "container_network_transmit_packets_dropped_total",
    "container_network_transmit_packets_total",
    "container_oom_events_total",
    "container_pressure_cpu_stalled_seconds_total",
    "container_pressure_cpu_waiting_seconds_total",
    "container_pressure_io_stalled_seconds_total",
    "container_pressure_io_waiting_seconds_total",
    "container_pressure_memory_stalled_seconds_total",
    "container_pressure_memory_waiting_seconds_total",
]

NODE_COUNTER_METRICS = [
    "node_context_switches_total",
    "node_cpu_seconds_total",
    "node_disk_io_time_seconds_total",
    "node_disk_read_bytes_total",
    "node_disk_reads_completed_total",
    "node_disk_write_time_seconds_total",
    "node_disk_writes_completed_total",
    "node_disk_written_bytes_total",
    "node_forks_total",
    "node_intr_total",
    "node_network_receive_bytes_total",
    "node_network_receive_drop_total",
    "node_network_receive_errs_total",
    "node_network_receive_packets_total",
    "node_network_transmit_bytes_total",
    "node_network_transmit_drop_total",
    "node_network_transmit_errs_total",
    "node_network_transmit_packets_total",
    "node_pressure_cpu_waiting_seconds_total",
    "node_pressure_io_stalled_seconds_total",
    "node_pressure_io_waiting_seconds_total",
    "node_pressure_memory_stalled_seconds_total",
    "node_pressure_memory_waiting_seconds_total",
    "node_schedstat_running_seconds_total",
    "node_schedstat_timeslices_total",
    "node_schedstat_waiting_seconds_total",
    "node_softnet_dropped_total",
    "node_softnet_processed_total",
    "node_softnet_times_squeezed_total",
]

HTTP_FIELDS = [
    "task_id",
    "worker_id",
    "timestamp",
    "scheduled_offset_ms",
    "launch_delay_ms",
    "path",
    "url",
    "http_code",
    "wall_time_ms",
    "dns_ms",
    "tcp_connect_ms",
    "tls_handshake_ms",
    "request_time_ms",
    "time_to_first_byte_ms",
    "response_time_ms",
    "response_transfer_ms",
    "size_download_bytes",
    "error",
]

SPAN_FIELDS = [
    "trace_id",
    "span_id",
    "parent_span_id",
    "service",
    "operation",
    "duration_ms",
    "start_time_unix_us",
]

CONTAINER_METRIC_FIELDS = [
    "metric",
    "namespace",
    "pod",
    "container",
    "node",
    "interface",
    "device",
    "operation",
    "value",
]

NODE_METRIC_FIELDS = [
    "metric",
    "node",
    "instance",
    "cpu",
    "mode",
    "device",
    "mountpoint",
    "fstype",
    "value",
]

RESOURCE_LIMIT_FIELDS = [
    "namespace",
    "pod",
    "container",
    "node",
    "resource",
    "unit",
    "value",
]

DEPLOYMENT_REPLICA_FIELDS = [
    "namespace",
    "deployment",
    "replicas",
]

SERVICE_TRAFFIC_FIELDS = [
    "source_service",
    "destination_service",
    "traffic_type",
    "bytes",
]

KUBERNETES_METADATA_FILES = [
    "kubectl-nodes.txt",
    "kubectl-pods.txt",
    "kubectl-services.txt",
    "kubectl-deployments.txt",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(argv: Sequence[str], check: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, check=check, text=True, capture_output=True)


def as_object_mapping(value: object) -> JsonObject:
    if isinstance(value, Mapping):
        return {str(key): child for key, child in value.items()}
    return {}


def as_object_list(value: object) -> list[object]:
    if isinstance(value, list):
        return value
    return []


def as_string(value: object, default: str = "") -> str:
    if isinstance(value, str):
        return value
    if value is None:
        return default
    return str(value)


def as_float(value: object, default: float = 0.0) -> float:
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


def as_csv_value(value: object) -> CsvValue:
    if value is None or isinstance(value, str | int | float):
        return value
    return str(value)


def stats_to_csv_row(stats: SummaryStats) -> CsvRow:
    return {
        "count": stats["count"],
        "min": stats["min"],
        "avg": stats["avg"],
        "p50": stats["p50"],
        "p90": stats["p90"],
        "p95": stats["p95"],
        "p99": stats["p99"],
        "max": stats["max"],
    }


def percentile(values: Sequence[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct / 100.0
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return ordered[low]
    weight = rank - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def summarize(values: Iterable[float | int | str | None]) -> SummaryStats:
    clean = [float(value) for value in values if value is not None and value != ""]
    if not clean:
        return {
            "count": 0,
            "min": None,
            "avg": None,
            "p50": None,
            "p90": None,
            "p95": None,
            "p99": None,
            "max": None,
        }
    return {
        "count": len(clean),
        "min": min(clean),
        "avg": sum(clean) / len(clean),
        "p50": percentile(clean, 50),
        "p90": percentile(clean, 90),
        "p95": percentile(clean, 95),
        "p99": percentile(clean, 99),
        "max": max(clean),
    }


def parse_request_paths(value: str) -> list[RequestPath]:
    paths: list[RequestPath] = []
    for item in value.split(","):
        path = item.strip()
        if not path:
            continue
        paths.append({"path": path, "weight": 1.0})
    if not paths:
        paths.append({"path": "/", "weight": 1.0})
    return paths


def choose_path(paths: Sequence[RequestPath]) -> str:
    total = sum(path["weight"] for path in paths)
    marker = random.uniform(0.0, total)
    seen = 0.0
    for path in paths:
        seen += path["weight"]
        if marker <= seen:
            return path["path"]
    return paths[-1]["path"]


def build_url(endpoint: str, path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if not path.startswith("/"):
        path = "/" + path
    return endpoint.rstrip("/") + path


def fetch_bytes(url: str, timeout: int = 30) -> bytes:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read()


def fetch_text(url: str, timeout: int = 30) -> str:
    return fetch_bytes(url, timeout=timeout).decode("utf-8")


def fetch_json(url: str, timeout: int = 30) -> object:
    return json.loads(fetch_text(url, timeout=timeout))


def curl_sample(
    task_id: int,
    scheduled_at: float,
    endpoint: str,
    path: str,
    timeout: float,
    user_agent: str,
) -> CurlSample:
    url = build_url(endpoint, path)
    fmt = "\t".join(
        [
            "%{http_code}",
            "%{time_namelookup}",
            "%{time_connect}",
            "%{time_appconnect}",
            "%{time_pretransfer}",
            "%{time_starttransfer}",
            "%{time_total}",
            "%{size_download}",
        ]
    )
    start_monotonic = time.monotonic()
    start_timestamp = now_iso()
    result = run(
        [
            "curl",
            "-sS",
            "-o",
            "/dev/null",
            "--max-time",
            str(timeout),
            "-w",
            fmt,
            "-H",
            "User-Agent: " + user_agent,
            url,
        ]
    )
    end_monotonic = time.monotonic()

    sample = {
        "task_id": task_id,
        "timestamp": start_timestamp,
        "scheduled_offset_ms": None,
        "launch_delay_ms": max(0.0, (start_monotonic - scheduled_at) * 1000.0),
        "path": path,
        "url": url,
        "http_code": None,
        "wall_time_ms": (end_monotonic - start_monotonic) * 1000.0,
        "dns_ms": None,
        "tcp_connect_ms": None,
        "tls_handshake_ms": None,
        "request_time_ms": None,
        "time_to_first_byte_ms": None,
        "response_time_ms": None,
        "response_transfer_ms": None,
        "size_download_bytes": None,
        "error": "",
    }

    if result.returncode != 0:
        sample["error"] = result.stderr.strip() or "curl failed"
        return sample

    parts = result.stdout.strip().split("\t")
    if len(parts) != 8:
        sample["error"] = "unexpected curl timing output: " + result.stdout.strip()
        return sample

    try:
        (
            http_code,
            time_namelookup,
            time_connect,
            time_appconnect,
            time_pretransfer,
            time_starttransfer,
            time_total,
            size_download,
        ) = parts
        lookup_s = float(time_namelookup)
        connect_s = float(time_connect)
        appconnect_s = float(time_appconnect)
        pretransfer_s = float(time_pretransfer)
        starttransfer_s = float(time_starttransfer)
        total_s = float(time_total)

        sample.update(
            {
                "http_code": int(http_code),
                "dns_ms": lookup_s * 1000.0,
                "tcp_connect_ms": max(0.0, (connect_s - lookup_s) * 1000.0),
                "tls_handshake_ms": max(0.0, (appconnect_s - connect_s) * 1000.0),
                "request_time_ms": max(0.0, (starttransfer_s - pretransfer_s) * 1000.0),
                "time_to_first_byte_ms": starttransfer_s * 1000.0,
                "response_time_ms": total_s * 1000.0,
                "response_transfer_ms": max(0.0, (total_s - starttransfer_s) * 1000.0),
                "size_download_bytes": int(float(size_download)),
            }
        )
    except ValueError as exc:
        sample["error"] = "could not parse curl timing output: " + str(exc)

    return sample


def benchmark_worker(
    worker_id: int,
    tasks: queue.Queue[BenchmarkTask | None],
    results: list[CurlSample],
    lock: threading.Lock,
    endpoint: str,
    timeout: float,
    user_agent: str,
) -> None:
    while True:
        task = tasks.get()
        if task is None:
            tasks.task_done()
            return
        wait_seconds = task["scheduled_at"] - time.monotonic()
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        sample = curl_sample(
            task["task_id"],
            task["scheduled_at"],
            endpoint,
            task["path"],
            timeout,
            user_agent,
        )
        sample["worker_id"] = worker_id
        sample["scheduled_offset_ms"] = task["scheduled_offset_seconds"] * 1000.0
        with lock:
            results.append(sample)
        tasks.task_done()


def run_load(
    endpoint: str,
    config: BenchmarkConfig,
    duration_seconds: float,
    label: str,
) -> LoadResult:
    target_rps = config["target_rps"]
    concurrency = config["concurrency"]
    timeout = config["request_timeout_seconds"]
    user_agent = config["user_agent"]
    paths = config["request_paths"]

    if target_rps <= 0 or duration_seconds <= 0:
        return {
            "label": label,
            "samples": [],
            "started_at": now_iso(),
            "finished_at": now_iso(),
            "wall_seconds": 0.0,
            "scheduled_requests": 0,
        }

    scheduled_requests = round(target_rps * duration_seconds)
    interval = 1.0 / target_rps
    tasks: queue.Queue[BenchmarkTask | None] = queue.Queue()
    results: list[CurlSample] = []
    lock = threading.Lock()
    workers = []

    for worker_id in range(max(1, concurrency)):
        worker = threading.Thread(
            target=benchmark_worker,
            args=(worker_id, tasks, results, lock, endpoint, timeout, user_agent),
        )
        worker.start()
        workers.append(worker)

    started_monotonic = time.monotonic()
    started_at = now_iso()
    for task_id in range(scheduled_requests):
        offset = task_id * interval
        tasks.put(
            {
                "task_id": task_id,
                "scheduled_at": started_monotonic + offset,
                "scheduled_offset_seconds": offset,
                "path": choose_path(paths),
            }
        )

    tasks.join()
    finished_monotonic = time.monotonic()
    finished_at = now_iso()

    for _ in workers:
        tasks.put(None)
    tasks.join()
    for worker in workers:
        worker.join()

    return {
        "label": label,
        "samples": sorted(results, key=lambda sample: sample["task_id"]),
        "started_at": started_at,
        "finished_at": finished_at,
        "wall_seconds": finished_monotonic - started_monotonic,
        "scheduled_requests": scheduled_requests,
    }


def ping_rtt_ms(host: str | None) -> float | None:
    if not host:
        return None
    result = run(["ping", "-c", "1", "-W", "1", host])
    if result.returncode != 0:
        return None
    match = re.search(r"time=([0-9.]+)\s*ms", result.stdout)
    if not match:
        return None
    return float(match.group(1))


def collect_rtt_samples(host: str | None, count: int, interval_seconds: float) -> list[CsvRow]:
    samples: list[CsvRow] = []
    for index in range(max(0, count)):
        samples.append(
            {
                "sample_id": index,
                "timestamp": now_iso(),
                "host": host,
                "rtt_ms": ping_rtt_ms(host),
            }
        )
        if index + 1 < count:
            time.sleep(interval_seconds)
    return samples


def write_csv(path: str, fields: Sequence[str], rows: Iterable[Mapping[str, object]]) -> None:
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def prometheus_query(prometheus_url: str, query: str) -> list[JsonObject]:
    params = urllib.parse.urlencode({"query": query})
    url = prometheus_url.rstrip("/") + "/api/v1/query?" + params
    payload = as_object_mapping(fetch_json(url, timeout=60))
    if payload.get("status") != "success":
        raise RuntimeError("Prometheus query failed: " + json.dumps(payload, sort_keys=True))
    data = as_object_mapping(payload.get("data"))
    return [as_object_mapping(item) for item in as_object_list(data.get("result"))]


def metric_value(item: JsonObject) -> float:
    values = as_object_list(item.get("value"))
    if len(values) < 2:
        return 0.0
    return as_float(values[1])


def collect_container_metrics(prometheus_url: str, window: str) -> tuple[list[CsvRow], list[str]]:
    rows: list[CsvRow] = []
    errors: list[str] = []
    for metric_name in CONTAINER_COUNTER_METRICS:
        query = 'rate({metric}{{image!="",container!="POD"}}[{window}])'.format(
            metric=metric_name,
            window=window,
        )
        try:
            results = prometheus_query(prometheus_url, query)
        except Exception as exc:
            errors.append(metric_name + ": " + str(exc))
            continue
        for item in results:
            metric = as_object_mapping(item.get("metric"))
            rows.append(
                {
                    "metric": metric_name,
                    "namespace": as_string(metric.get("namespace")),
                    "pod": as_string(metric.get("pod")),
                    "container": as_string(metric.get("container")),
                    "node": as_string(metric.get("node")),
                    "interface": as_string(metric.get("interface")),
                    "device": as_string(metric.get("device")),
                    "operation": as_string(metric.get("operation")),
                    "value": metric_value(item),
                }
            )
    return rows, errors


def collect_node_metrics(prometheus_url: str, window: str) -> tuple[list[CsvRow], list[str]]:
    rows: list[CsvRow] = []
    errors: list[str] = []
    for metric_name in NODE_COUNTER_METRICS:
        query = "rate({metric}[" + window + "])"
        try:
            results = prometheus_query(prometheus_url, query.format(metric=metric_name))
        except Exception as exc:
            errors.append(metric_name + ": " + str(exc))
            continue
        for item in results:
            metric = as_object_mapping(item.get("metric"))
            rows.append(
                {
                    "metric": metric_name,
                    "node": as_string(metric.get("node")),
                    "instance": as_string(metric.get("instance")),
                    "cpu": as_string(metric.get("cpu")),
                    "mode": as_string(metric.get("mode")),
                    "device": as_string(metric.get("device")),
                    "mountpoint": as_string(metric.get("mountpoint")),
                    "fstype": as_string(metric.get("fstype")),
                    "value": metric_value(item),
                }
            )
    return rows, errors


def collect_container_resource_limits(prometheus_url: str) -> tuple[list[CsvRow], list[str]]:
    rows: list[CsvRow] = []
    errors: list[str] = []
    query = 'kube_pod_container_resource_limits{resource=~"cpu|memory"}'
    try:
        results = prometheus_query(prometheus_url, query)
    except Exception as exc:
        return rows, ["kube_pod_container_resource_limits: " + str(exc)]
    for item in results:
        metric = as_object_mapping(item.get("metric"))
        rows.append(
            {
                "namespace": as_string(metric.get("namespace")),
                "pod": as_string(metric.get("pod")),
                "container": as_string(metric.get("container")),
                "node": as_string(metric.get("node")),
                "resource": as_string(metric.get("resource")),
                "unit": as_string(metric.get("unit")),
                "value": metric_value(item),
            }
        )
    return rows, errors


def collect_deployment_replicas(prometheus_url: str) -> tuple[list[CsvRow], list[str]]:
    rows: list[CsvRow] = []
    errors: list[str] = []
    query = "kube_deployment_status_replicas"
    try:
        results = prometheus_query(prometheus_url, query)
    except Exception as exc:
        return rows, ["kube_deployment_status_replicas: " + str(exc)]
    for item in results:
        metric = as_object_mapping(item.get("metric"))
        rows.append(
            {
                "namespace": as_string(metric.get("namespace")),
                "deployment": as_string(metric.get("deployment")),
                "replicas": int(metric_value(item)),
            }
        )
    return rows, errors


def collect_service_traffic_bytes(prometheus_url: str, window: str) -> tuple[list[CsvRow], list[str]]:
    rows: list[CsvRow] = []
    errors: list[str] = []
    request_query = """
sum by (source_workload, destination_workload) (
  increase(istio_request_bytes_sum{
    reporter="source",
    source_workload!="unknown",
    destination_workload!="unknown"
  }[%s])
)
""".strip() % window
    response_query = """
sum by (source_workload, destination_workload) (
  increase(istio_response_bytes_sum{
    reporter="source",
    source_workload!="unknown",
    destination_workload!="unknown"
  }[%s])
)
""".strip() % window

    try:
        request_results = prometheus_query(prometheus_url, request_query)
    except Exception as exc:
        request_results = []
        errors.append("istio_request_bytes_sum: " + str(exc))
    for item in request_results:
        metric = as_object_mapping(item.get("metric"))
        rows.append(
            {
                "source_service": as_string(metric.get("source_workload")),
                "destination_service": as_string(metric.get("destination_workload")),
                "traffic_type": "request",
                "bytes": round(metric_value(item), 2),
            }
        )

    try:
        response_results = prometheus_query(prometheus_url, response_query)
    except Exception as exc:
        response_results = []
        errors.append("istio_response_bytes_sum: " + str(exc))
    for item in response_results:
        metric = as_object_mapping(item.get("metric"))
        rows.append(
            {
                "source_service": as_string(metric.get("destination_workload")),
                "destination_service": as_string(metric.get("source_workload")),
                "traffic_type": "response",
                "bytes": round(metric_value(item), 2),
            }
        )

    return sorted(
        rows,
        key=lambda row: (
            row["source_service"],
            row["destination_service"],
            row["traffic_type"],
        ),
    ), errors


def query_traces(jaeger_url: str, service: str, limit: int, lookback: str) -> list[JsonObject]:
    query = urllib.parse.urlencode(
        {
            "service": service,
            "limit": str(limit),
            "lookback": lookback,
        }
    )
    url = jaeger_url.rstrip("/") + "/api/traces?" + query
    payload = as_object_mapping(fetch_json(url, timeout=60))
    return [as_object_mapping(item) for item in as_object_list(payload.get("data"))]


def list_jaeger_services(jaeger_url: str) -> list[str]:
    url = jaeger_url.rstrip("/") + "/api/services"
    payload = as_object_mapping(fetch_json(url, timeout=30))
    services = [as_string(service) for service in as_object_list(payload.get("data"))]
    return sorted(service for service in services if service and service != "jaeger")


def collect_traces(
    jaeger_url: str,
    services: Sequence[str],
    limit: int,
    lookback: str,
) -> tuple[list[CsvRow], list[float]]:
    unique_spans: dict[str, CsvRow] = {}
    unique_roots: set[str] = set()
    root_durations_ms: list[float] = []

    for service in services:
        traces = query_traces(jaeger_url, service, limit, lookback)
        for trace in traces:
            trace_id = as_string(trace.get("traceID"))
            processes = as_object_mapping(trace.get("processes"))
            for span_object in as_object_list(trace.get("spans")):
                span = as_object_mapping(span_object)
                span_id = as_string(span.get("spanID"))
                key = trace_id + ":" + span_id
                references = [as_object_mapping(reference) for reference in as_object_list(span.get("references"))]
                duration_ms = as_float(span.get("duration")) / 1000.0

                if not references and key not in unique_roots:
                    unique_roots.add(key)
                    root_durations_ms.append(duration_ms)

                if key in unique_spans:
                    continue

                process_id = as_string(span.get("processID"))
                process = as_object_mapping(processes.get(process_id))
                service_name = as_string(process.get("serviceName"), "unknown")

                unique_spans[key] = {
                    "trace_id": trace_id,
                    "span_id": span_id,
                    "parent_span_id": as_string(references[0].get("spanID")) if references else "",
                    "service": service_name,
                    "operation": as_string(span.get("operationName")),
                    "duration_ms": duration_ms,
                    "start_time_unix_us": as_string(span.get("startTime")),
                }

    return list(unique_spans.values()), root_durations_ms


def group_summary(rows: Iterable[Mapping[str, object]], key_field: str, value_field: str) -> dict[str, SummaryStats]:
    grouped: dict[str, list[float | int | str | None]] = {}
    for row in rows:
        grouped.setdefault(as_string(row.get(key_field)), []).append(as_csv_value(row.get(value_field)))
    return {key: summarize(values) for key, values in sorted(grouped.items())}


def write_service_summary(path: str, spans: Iterable[Mapping[str, object]]) -> dict[str, dict[str, TomlValue]]:
    rows: list[CsvRow] = []
    for service, summary in group_summary(spans, "service", "duration_ms").items():
        row: CsvRow = {"service": service}
        row.update(stats_to_csv_row(summary))
        rows.append(row)
    write_csv(path, ["service", "count", "min", "avg", "p50", "p90", "p95", "p99", "max"], rows)
    return {as_string(row["service"]): {k: row[k] for k in row if k != "service"} for row in rows}


def write_operation_summary(path: str, spans: Iterable[Mapping[str, object]]) -> None:
    grouped: dict[str, dict[str, object]] = {}
    for span in spans:
        service = as_string(span.get("service"))
        operation = as_string(span.get("operation"))
        key = service + " " + operation
        grouped.setdefault(key, {"service": service, "operation": operation, "values": []})
        values = grouped[key]["values"]
        if isinstance(values, list):
            values.append(span.get("duration_ms"))

    rows: list[CsvRow] = []
    for item in sorted(grouped.values(), key=lambda row: (row["service"], row["operation"])):
        values = item.get("values", [])
        row: CsvRow = {"service": as_string(item.get("service")), "operation": as_string(item.get("operation"))}
        row.update(stats_to_csv_row(summarize(values if isinstance(values, list) else [])))
        rows.append(row)

    write_csv(
        path,
        ["service", "operation", "count", "min", "avg", "p50", "p90", "p95", "p99", "max"],
        rows,
    )


def capture_command(output_dir: str, filename: str, argv: Sequence[str]) -> str:
    result = run(argv)
    path = os.path.join(output_dir, filename)
    with open(path, "w") as handle:
        handle.write("$ " + " ".join(argv) + "\n\n")
        handle.write(result.stdout)
        if result.stderr:
            handle.write("\n--- stderr ---\n")
            handle.write(result.stderr)
        if result.returncode != 0:
            handle.write("\n--- exit code: {} ---\n".format(result.returncode))
    return path


def capture_kubernetes_metadata(output_dir: str, metadata_url: str | None) -> dict[str, str]:
    outputs: dict[str, str] = {}
    if metadata_url:
        for filename in KUBERNETES_METADATA_FILES:
            path = os.path.join(output_dir, filename)
            try:
                text = fetch_text(metadata_url.rstrip("/") + "/" + filename, timeout=20)
            except Exception as exc:
                text = "Could not fetch {} from {}: {}\n".format(filename, metadata_url, exc)
            with open(path, "w") as handle:
                handle.write(text)
            outputs[filename.replace(".txt", "").replace("-", "_")] = path
        return outputs

    return {
        "kubectl_nodes": capture_command(output_dir, "kubectl-nodes.txt", ["kubectl", "get", "nodes", "-o", "wide"]),
        "kubectl_pods": capture_command(output_dir, "kubectl-pods.txt", ["kubectl", "get", "pods", "-A", "-o", "wide"]),
        "kubectl_services": capture_command(output_dir, "kubectl-services.txt", ["kubectl", "get", "services", "-A", "-o", "wide"]),
        "kubectl_deployments": capture_command(output_dir, "kubectl-deployments.txt", ["kubectl", "get", "deployments", "-A", "-o", "wide"]),
    }


def status_counts(samples: Iterable[Mapping[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for sample in samples:
        key = str(sample.get("http_code") or "error")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def is_error_sample(sample: Mapping[str, object]) -> bool:
    if sample.get("error"):
        return True
    code = sample.get("http_code")
    return code is None or int(as_float(code, 500.0)) >= 500


def metric_summaries(rows: Iterable[Mapping[str, object]], value_field: str) -> dict[str, SummaryStats]:
    grouped: dict[str, list[float | int | str | None]] = {}
    for row in rows:
        grouped.setdefault(as_string(row.get("metric")), []).append(as_csv_value(row.get(value_field)))
    return {key: summarize(values) for key, values in sorted(grouped.items())}


def traffic_summary(rows: Iterable[Mapping[str, object]]) -> dict[str, float | int]:
    materialized = list(rows)
    request_bytes = sum(as_float(row.get("bytes")) for row in materialized if row.get("traffic_type") == "request")
    response_bytes = sum(as_float(row.get("bytes")) for row in materialized if row.get("traffic_type") == "response")
    return {
        "rows": len(materialized),
        "request_bytes": request_bytes,
        "response_bytes": response_bytes,
        "total_bytes": request_bytes + response_bytes,
    }


def toml_key(key: str) -> str:
    if re.match(r"^[A-Za-z_][A-Za-z0-9_-]*$", key):
        return key
    return '"' + key.replace("\\", "\\\\").replace('"', '\\"') + '"'


def toml_value(value: object) -> str:
    if value is None:
        return "nan"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "nan"
        if math.isinf(value):
            return "inf" if value > 0 else "-inf"
        return repr(value)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(toml_value(item) for item in value) + "]"
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return '"' + escaped + '"'


def write_toml_table(handle: TextIO, table: Mapping[str, object], path: list[str] | None = None) -> None:
    if path is None:
        path = []
    scalars = []
    nested = []
    for key, value in table.items():
        if isinstance(value, dict):
            nested.append((key, value))
        else:
            scalars.append((key, value))

    if path:
        handle.write("\n[" + ".".join(toml_key(part) for part in path) + "]\n")
    for key, value in scalars:
        handle.write(toml_key(key) + " = " + toml_value(value) + "\n")
    for key, value in nested:
        write_toml_table(handle, value, path + [key])


def write_toml(path: str, table: Mapping[str, object]) -> None:
    with open(path, "w") as handle:
        write_toml_table(handle, table)


def format_value(value: object) -> str:
    if value is None:
        return "nan"
    if isinstance(value, float):
        return "{:.3f}".format(value)
    return str(value)


def write_summary_markdown(path: str, summary: Mapping[str, object]) -> None:
    http = as_object_mapping(summary.get("http"))
    run_summary = as_object_mapping(summary.get("run"))
    config_summary = as_object_mapping(summary.get("config"))
    resource = as_object_mapping(summary.get("resource"))
    traffic = as_object_mapping(summary.get("traffic"))
    outputs = as_object_mapping(summary.get("outputs"))
    errors = [as_string(error) for error in as_object_list(summary.get("errors"))]
    with open(path, "w") as handle:
        handle.write("# Online Boutique Benchmark Result\n\n")
        handle.write("* Endpoint: `{}`\n".format(run_summary.get("endpoint", "")))
        handle.write("* Target RPS: `{}`\n".format(config_summary.get("target_rps", "")))
        handle.write("* Achieved RPS: `{:.3f}`\n".format(as_float(http.get("achieved_rps"))))
        handle.write("* Completed requests: `{}`\n".format(http.get("completed_requests", "")))
        handle.write("* Error requests: `{}`\n".format(http.get("error_requests", "")))
        handle.write("* Jaeger URL: `{}`\n".format(run_summary.get("jaeger_url", "")))
        handle.write("* Prometheus URL: `{}`\n\n".format(run_summary.get("prometheus_url", "")))
        handle.write("## HTTP Response Time MS\n\n")
        response_time = as_object_mapping(http.get("response_time_ms"))
        for key in ["min", "avg", "p50", "p90", "p95", "p99", "max"]:
            handle.write("* {}: `{}`\n".format(key, format_value(response_time.get(key))))
        handle.write("\n## Resource Rows\n\n")
        handle.write("* Container metrics: `{}`\n".format(resource["container_metric_rows"]))
        handle.write("* Node metrics: `{}`\n".format(resource["node_metric_rows"]))
        handle.write("* Resource limits: `{}`\n".format(resource["container_resource_limit_rows"]))
        handle.write("* Deployment replicas: `{}`\n".format(resource["deployment_replica_rows"]))
        handle.write("* Service traffic rows: `{}`\n".format(traffic["rows"]))
        handle.write("* Service traffic bytes: `{}`\n".format(format_value(traffic["total_bytes"])))
        handle.write("\n## Outputs\n\n")
        for name, output in sorted(outputs.items()):
            if isinstance(output, dict):
                for child_name, child_output in sorted(output.items()):
                    handle.write("* {}.{}: `{}`\n".format(name, child_name, child_output))
            else:
                handle.write("* {}: `{}`\n".format(name, output))
        if errors:
            handle.write("\n## Collection Errors\n\n")
            for error in errors:
                handle.write("* `{}`\n".format(error))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--jaeger-url", default="http://jaeger:16686")
    parser.add_argument("--prometheus-url", default=None)
    parser.add_argument("--metadata-url", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--target-rps", type=float, default=DEFAULT_TARGET_RPS)
    parser.add_argument("--duration-seconds", type=float, default=DEFAULT_DURATION_SECONDS)
    parser.add_argument("--warmup-seconds", type=float, default=DEFAULT_WARMUP_SECONDS)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--request-timeout-seconds", type=float, default=DEFAULT_REQUEST_TIMEOUT_SECONDS)
    parser.add_argument("--request-paths", default=DEFAULT_REQUEST_PATHS)
    parser.add_argument("--rtt-samples", type=int, default=DEFAULT_RTT_SAMPLES)
    parser.add_argument("--rtt-interval-seconds", type=float, default=DEFAULT_RTT_INTERVAL_SECONDS)
    parser.add_argument("--trace-service", default=DEFAULT_TRACE_SERVICE)
    parser.add_argument("--trace-limit", type=int, default=DEFAULT_TRACE_LIMIT)
    parser.add_argument("--lookback", default=DEFAULT_TRACE_LOOKBACK)
    parser.add_argument("--trace-flush-wait-seconds", type=float, default=DEFAULT_TRACE_FLUSH_WAIT_SECONDS)
    parser.add_argument("--resource-window", default=DEFAULT_RESOURCE_WINDOW)
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    parser.add_argument("--random-seed", type=int, default=1)
    args = parser.parse_args()

    random.seed(args.random_seed)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = args.output_dir or os.path.join(DEFAULT_OUTPUT_ROOT, timestamp)
    os.makedirs(output_dir, exist_ok=True)

    config: BenchmarkConfig = {
        "target_rps": args.target_rps,
        "duration_seconds": args.duration_seconds,
        "warmup_seconds": args.warmup_seconds,
        "concurrency": args.concurrency,
        "request_timeout_seconds": args.request_timeout_seconds,
        "request_paths": parse_request_paths(args.request_paths),
        "rtt_samples": args.rtt_samples,
        "rtt_interval_seconds": args.rtt_interval_seconds,
        "trace_service": args.trace_service,
        "trace_limit": args.trace_limit,
        "trace_lookback": args.lookback,
        "trace_flush_wait_seconds": args.trace_flush_wait_seconds,
        "resource_window": args.resource_window,
        "user_agent": args.user_agent,
        "random_seed": args.random_seed,
    }

    parsed_endpoint = urllib.parse.urlparse(args.endpoint)
    ping_host = parsed_endpoint.hostname
    rtt_samples = collect_rtt_samples(
        ping_host,
        int(args.rtt_samples),
        float(args.rtt_interval_seconds),
    )

    warmup = run_load(args.endpoint, config, float(args.warmup_seconds), "warmup")
    warmup_csv = ""
    if warmup["samples"]:
        warmup_csv = os.path.join(output_dir, "warmup-http-samples.csv")
        write_csv(warmup_csv, HTTP_FIELDS, warmup["samples"])

    benchmark = run_load(args.endpoint, config, float(args.duration_seconds), "benchmark")

    if args.trace_flush_wait_seconds > 0:
        time.sleep(args.trace_flush_wait_seconds)

    errors: list[str] = []
    trace_services: list[str] = []
    spans: list[CsvRow] = []
    root_durations_ms: list[float] = []
    try:
        if args.trace_service == "all":
            trace_services = list_jaeger_services(args.jaeger_url)
        else:
            trace_services = [
                service.strip()
                for service in args.trace_service.split(",")
                if service.strip()
            ]
        spans, root_durations_ms = collect_traces(
            args.jaeger_url,
            trace_services,
            int(args.trace_limit),
            args.lookback,
        )
    except Exception as exc:
        errors.append("jaeger: " + str(exc))

    http_csv = os.path.join(output_dir, "http-samples.csv")
    rtt_csv = os.path.join(output_dir, "rtt-samples.csv")
    spans_csv = os.path.join(output_dir, "jaeger-spans.csv")
    service_csv = os.path.join(output_dir, "service-latency.csv")
    operation_csv = os.path.join(output_dir, "operation-latency.csv")
    container_metrics_csv = os.path.join(output_dir, "container-metrics.csv")
    node_metrics_csv = os.path.join(output_dir, "node-metrics.csv")
    limits_csv = os.path.join(output_dir, "container-resource-limits.csv")
    replicas_csv = os.path.join(output_dir, "deployment-replicas.csv")
    traffic_csv = os.path.join(output_dir, "service-traffic-bytes.csv")
    summary_toml = os.path.join(output_dir, "summary.toml")
    summary_md = os.path.join(output_dir, "summary.md")

    write_csv(http_csv, HTTP_FIELDS, benchmark["samples"])
    write_csv(rtt_csv, ["sample_id", "timestamp", "host", "rtt_ms"], rtt_samples)
    write_csv(spans_csv, SPAN_FIELDS, spans)
    service_summary = write_service_summary(service_csv, spans)
    write_operation_summary(operation_csv, spans)

    container_rows: list[CsvRow] = []
    node_rows: list[CsvRow] = []
    limit_rows: list[CsvRow] = []
    replica_rows: list[CsvRow] = []
    traffic_rows: list[CsvRow] = []
    if args.prometheus_url:
        container_rows, container_errors = collect_container_metrics(args.prometheus_url, args.resource_window)
        node_rows, node_errors = collect_node_metrics(args.prometheus_url, args.resource_window)
        limit_rows, limit_errors = collect_container_resource_limits(args.prometheus_url)
        replica_rows, replica_errors = collect_deployment_replicas(args.prometheus_url)
        traffic_rows, traffic_errors = collect_service_traffic_bytes(args.prometheus_url, args.resource_window)
        errors.extend(container_errors)
        errors.extend(node_errors)
        errors.extend(limit_errors)
        errors.extend(replica_errors)
        errors.extend(traffic_errors)
    else:
        errors.append("prometheus-url was not provided; resource CSVs are empty")

    write_csv(container_metrics_csv, CONTAINER_METRIC_FIELDS, container_rows)
    write_csv(node_metrics_csv, NODE_METRIC_FIELDS, node_rows)
    write_csv(limits_csv, RESOURCE_LIMIT_FIELDS, limit_rows)
    write_csv(replicas_csv, DEPLOYMENT_REPLICA_FIELDS, replica_rows)
    write_csv(traffic_csv, SERVICE_TRAFFIC_FIELDS, traffic_rows)
    kubernetes_outputs = capture_kubernetes_metadata(output_dir, args.metadata_url)

    completed = len(benchmark["samples"])
    transport_error_requests = len([sample for sample in benchmark["samples"] if sample.get("error")])
    error_requests = len([sample for sample in benchmark["samples"] if is_error_sample(sample)])
    success_response_times = [
        sample.get("response_time_ms")
        for sample in benchmark["samples"]
        if not sample.get("error") and sample.get("http_code") and 200 <= sample["http_code"] < 500
    ]

    outputs: dict[str, str | dict[str, str]] = {
        "http_samples_csv": http_csv,
        "rtt_samples_csv": rtt_csv,
        "jaeger_spans_csv": spans_csv,
        "service_latency_csv": service_csv,
        "operation_latency_csv": operation_csv,
        "container_metrics_csv": container_metrics_csv,
        "node_metrics_csv": node_metrics_csv,
        "container_resource_limits_csv": limits_csv,
        "deployment_replicas_csv": replicas_csv,
        "service_traffic_bytes_csv": traffic_csv,
        "summary_toml": summary_toml,
        "summary_md": summary_md,
        "kubernetes": kubernetes_outputs,
    }
    if warmup_csv:
        outputs["warmup_http_samples_csv"] = warmup_csv

    summary_config: TomlTable = {
        "target_rps": config["target_rps"],
        "duration_seconds": config["duration_seconds"],
        "warmup_seconds": config["warmup_seconds"],
        "concurrency": config["concurrency"],
        "request_timeout_seconds": config["request_timeout_seconds"],
        "request_paths": [path["path"] for path in config["request_paths"]],
        "rtt_samples": config["rtt_samples"],
        "rtt_interval_seconds": config["rtt_interval_seconds"],
        "trace_service": config["trace_service"],
        "trace_limit": config["trace_limit"],
        "trace_lookback": config["trace_lookback"],
        "trace_flush_wait_seconds": config["trace_flush_wait_seconds"],
        "resource_window": config["resource_window"],
        "user_agent": config["user_agent"],
        "random_seed": config["random_seed"],
    }

    summary: dict[str, object] = {
        "run": {
            "endpoint": args.endpoint,
            "jaeger_url": args.jaeger_url,
            "prometheus_url": args.prometheus_url or "",
            "metadata_url": args.metadata_url or "",
            "queried_trace_services": trace_services,
            "started_at": benchmark["started_at"],
            "finished_at": benchmark["finished_at"],
            "hostname": run(["hostname", "-f"]).stdout.strip(),
        },
        "config": summary_config,
        "http": {
            "target_rps": float(args.target_rps),
            "achieved_rps": completed / benchmark["wall_seconds"] if benchmark["wall_seconds"] else 0.0,
            "scheduled_requests": benchmark["scheduled_requests"],
            "completed_requests": completed,
            "error_requests": error_requests,
            "transport_error_requests": transport_error_requests,
            "status_counts": status_counts(benchmark["samples"]),
            "rtt_ms": summarize([sample.get("rtt_ms") for sample in rtt_samples]),
            "launch_delay_ms": summarize([sample.get("launch_delay_ms") for sample in benchmark["samples"]]),
            "tcp_connect_ms": summarize([sample.get("tcp_connect_ms") for sample in benchmark["samples"]]),
            "request_time_ms": summarize([sample.get("request_time_ms") for sample in benchmark["samples"]]),
            "time_to_first_byte_ms": summarize([sample.get("time_to_first_byte_ms") for sample in benchmark["samples"]]),
            "response_time_ms": summarize(success_response_times),
            "all_response_time_ms": summarize([sample.get("response_time_ms") for sample in benchmark["samples"]]),
        },
        "trace": {
            "root_duration_ms": summarize(root_durations_ms),
            "service_duration_ms": service_summary,
        },
        "resource": {
            "container_metric_rows": len(container_rows),
            "node_metric_rows": len(node_rows),
            "container_resource_limit_rows": len(limit_rows),
            "deployment_replica_rows": len(replica_rows),
            "container_metric_summaries": metric_summaries(container_rows, "value"),
            "node_metric_summaries": metric_summaries(node_rows, "value"),
        },
        "traffic": traffic_summary(traffic_rows),
        "outputs": outputs,
        "errors": errors,
    }

    write_toml(summary_toml, summary)
    write_summary_markdown(summary_md, summary)

    print("output_dir = {}".format(output_dir))
    print("summary_toml = {}".format(summary_toml))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
