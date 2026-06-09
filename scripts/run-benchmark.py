#!/usr/bin/env python3
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
from datetime import datetime, timezone


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


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def run(argv, check=False):
    return subprocess.run(argv, check=check, text=True, capture_output=True)


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct / 100.0
    low = int(math.floor(rank))
    high = int(math.ceil(rank))
    if low == high:
        return ordered[low]
    weight = rank - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def summarize(values):
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


def parse_request_paths(value):
    paths = []
    for item in value.split(","):
        path = item.strip()
        if not path:
            continue
        paths.append({"path": path, "weight": 1.0})
    if not paths:
        paths.append({"path": "/", "weight": 1.0})
    return paths


def choose_path(paths):
    total = sum(path["weight"] for path in paths)
    marker = random.uniform(0.0, total)
    seen = 0.0
    for path in paths:
        seen += path["weight"]
        if marker <= seen:
            return path["path"]
    return paths[-1]["path"]


def build_url(endpoint, path):
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if not path.startswith("/"):
        path = "/" + path
    return endpoint.rstrip("/") + path


def fetch_bytes(url, timeout=30):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read()


def fetch_text(url, timeout=30):
    return fetch_bytes(url, timeout=timeout).decode("utf-8")


def fetch_json(url, timeout=30):
    return json.loads(fetch_text(url, timeout=timeout))


def curl_sample(task_id, scheduled_at, endpoint, path, timeout, user_agent):
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


def benchmark_worker(worker_id, tasks, results, lock, endpoint, timeout, user_agent):
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


def run_load(endpoint, config, duration_seconds, label):
    target_rps = float(config["target_rps"])
    concurrency = int(config["concurrency"])
    timeout = float(config["request_timeout_seconds"])
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

    scheduled_requests = int(round(target_rps * duration_seconds))
    interval = 1.0 / target_rps
    tasks = queue.Queue()
    results = []
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


def ping_rtt_ms(host):
    if not host:
        return None
    result = run(["ping", "-c", "1", "-W", "1", host])
    if result.returncode != 0:
        return None
    match = re.search(r"time=([0-9.]+)\s*ms", result.stdout)
    if not match:
        return None
    return float(match.group(1))


def collect_rtt_samples(host, count, interval_seconds):
    samples = []
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


def write_csv(path, fields, rows):
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def prometheus_query(prometheus_url, query):
    params = urllib.parse.urlencode({"query": query})
    url = prometheus_url.rstrip("/") + "/api/v1/query?" + params
    payload = fetch_json(url, timeout=60)
    if payload.get("status") != "success":
        raise RuntimeError("Prometheus query failed: " + json.dumps(payload, sort_keys=True))
    return payload.get("data", {}).get("result", [])


def metric_value(item):
    value = item.get("value", [None, None])[1]
    if value is None:
        return 0.0
    return float(value)


def collect_container_metrics(prometheus_url, window):
    rows = []
    errors = []
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
            metric = item.get("metric", {})
            rows.append(
                {
                    "metric": metric_name,
                    "namespace": metric.get("namespace", ""),
                    "pod": metric.get("pod", ""),
                    "container": metric.get("container", ""),
                    "node": metric.get("node", ""),
                    "interface": metric.get("interface", ""),
                    "device": metric.get("device", ""),
                    "operation": metric.get("operation", ""),
                    "value": metric_value(item),
                }
            )
    return rows, errors


def collect_node_metrics(prometheus_url, window):
    rows = []
    errors = []
    for metric_name in NODE_COUNTER_METRICS:
        query = "rate({metric}[" + window + "])"
        try:
            results = prometheus_query(prometheus_url, query.format(metric=metric_name))
        except Exception as exc:
            errors.append(metric_name + ": " + str(exc))
            continue
        for item in results:
            metric = item.get("metric", {})
            rows.append(
                {
                    "metric": metric_name,
                    "node": metric.get("node", ""),
                    "instance": metric.get("instance", ""),
                    "cpu": metric.get("cpu", ""),
                    "mode": metric.get("mode", ""),
                    "device": metric.get("device", ""),
                    "mountpoint": metric.get("mountpoint", ""),
                    "fstype": metric.get("fstype", ""),
                    "value": metric_value(item),
                }
            )
    return rows, errors


def collect_container_resource_limits(prometheus_url):
    rows = []
    errors = []
    query = 'kube_pod_container_resource_limits{resource=~"cpu|memory"}'
    try:
        results = prometheus_query(prometheus_url, query)
    except Exception as exc:
        return rows, ["kube_pod_container_resource_limits: " + str(exc)]
    for item in results:
        metric = item.get("metric", {})
        rows.append(
            {
                "namespace": metric.get("namespace", ""),
                "pod": metric.get("pod", ""),
                "container": metric.get("container", ""),
                "node": metric.get("node", ""),
                "resource": metric.get("resource", ""),
                "unit": metric.get("unit", ""),
                "value": metric_value(item),
            }
        )
    return rows, errors


def collect_deployment_replicas(prometheus_url):
    rows = []
    errors = []
    query = "kube_deployment_status_replicas"
    try:
        results = prometheus_query(prometheus_url, query)
    except Exception as exc:
        return rows, ["kube_deployment_status_replicas: " + str(exc)]
    for item in results:
        metric = item.get("metric", {})
        rows.append(
            {
                "namespace": metric.get("namespace", ""),
                "deployment": metric.get("deployment", ""),
                "replicas": int(metric_value(item)),
            }
        )
    return rows, errors


def collect_service_traffic_bytes(prometheus_url, window):
    rows = []
    errors = []
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
        metric = item.get("metric", {})
        rows.append(
            {
                "source_service": metric.get("source_workload", ""),
                "destination_service": metric.get("destination_workload", ""),
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
        metric = item.get("metric", {})
        rows.append(
            {
                "source_service": metric.get("destination_workload", ""),
                "destination_service": metric.get("source_workload", ""),
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


def query_traces(jaeger_url, service, limit, lookback):
    query = urllib.parse.urlencode(
        {
            "service": service,
            "limit": str(limit),
            "lookback": lookback,
        }
    )
    url = jaeger_url.rstrip("/") + "/api/traces?" + query
    return fetch_json(url, timeout=60).get("data", [])


def list_jaeger_services(jaeger_url):
    url = jaeger_url.rstrip("/") + "/api/services"
    services = fetch_json(url, timeout=30).get("data", [])
    return sorted(service for service in services if service and service != "jaeger")


def collect_traces(jaeger_url, services, limit, lookback):
    unique_spans = {}
    unique_roots = set()
    root_durations_ms = []

    for service in services:
        traces = query_traces(jaeger_url, service, limit, lookback)
        for trace in traces:
            trace_id = trace.get("traceID", "")
            processes = trace.get("processes", {})
            for span in trace.get("spans", []):
                span_id = span.get("spanID", "")
                key = trace_id + ":" + span_id
                references = span.get("references", [])
                duration_ms = float(span.get("duration", 0.0)) / 1000.0

                if not references and key not in unique_roots:
                    unique_roots.add(key)
                    root_durations_ms.append(duration_ms)

                if key in unique_spans:
                    continue

                process = processes.get(span.get("processID", ""), {})
                service_name = process.get("serviceName", "unknown")

                unique_spans[key] = {
                    "trace_id": trace_id,
                    "span_id": span_id,
                    "parent_span_id": references[0].get("spanID", "") if references else "",
                    "service": service_name,
                    "operation": span.get("operationName", ""),
                    "duration_ms": duration_ms,
                    "start_time_unix_us": span.get("startTime", ""),
                }

    return list(unique_spans.values()), root_durations_ms


def group_summary(rows, key_field, value_field):
    grouped = {}
    for row in rows:
        grouped.setdefault(row[key_field], []).append(row[value_field])
    return {key: summarize(values) for key, values in sorted(grouped.items())}


def write_service_summary(path, spans):
    rows = []
    for service, summary in group_summary(spans, "service", "duration_ms").items():
        row = {"service": service}
        row.update(summary)
        rows.append(row)
    write_csv(path, ["service", "count", "min", "avg", "p50", "p90", "p95", "p99", "max"], rows)
    return {row["service"]: {k: row[k] for k in row if k != "service"} for row in rows}


def write_operation_summary(path, spans):
    grouped = {}
    for span in spans:
        key = span["service"] + " " + span["operation"]
        grouped.setdefault(key, {"service": span["service"], "operation": span["operation"], "values": []})
        grouped[key]["values"].append(span["duration_ms"])

    rows = []
    for item in sorted(grouped.values(), key=lambda row: (row["service"], row["operation"])):
        row = {"service": item["service"], "operation": item["operation"]}
        row.update(summarize(item["values"]))
        rows.append(row)

    write_csv(
        path,
        ["service", "operation", "count", "min", "avg", "p50", "p90", "p95", "p99", "max"],
        rows,
    )


def capture_command(output_dir, filename, argv):
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


def capture_kubernetes_metadata(output_dir, metadata_url):
    outputs = {}
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


def status_counts(samples):
    counts = {}
    for sample in samples:
        key = str(sample.get("http_code") or "error")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def is_error_sample(sample):
    if sample.get("error"):
        return True
    code = sample.get("http_code")
    return code is None or int(code) >= 500


def metric_summaries(rows, value_field):
    grouped = {}
    for row in rows:
        grouped.setdefault(row["metric"], []).append(row.get(value_field))
    return {key: summarize(values) for key, values in sorted(grouped.items())}


def traffic_summary(rows):
    request_bytes = sum(float(row["bytes"]) for row in rows if row["traffic_type"] == "request")
    response_bytes = sum(float(row["bytes"]) for row in rows if row["traffic_type"] == "response")
    return {
        "rows": len(rows),
        "request_bytes": request_bytes,
        "response_bytes": response_bytes,
        "total_bytes": request_bytes + response_bytes,
    }


def toml_key(key):
    if re.match(r"^[A-Za-z_][A-Za-z0-9_-]*$", key):
        return key
    return '"' + str(key).replace("\\", "\\\\").replace('"', '\\"') + '"'


def toml_value(value):
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


def write_toml_table(handle, table, path=None):
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


def write_toml(path, table):
    with open(path, "w") as handle:
        write_toml_table(handle, table)


def format_value(value):
    if value is None:
        return "nan"
    if isinstance(value, float):
        return "{:.3f}".format(value)
    return str(value)


def write_summary_markdown(path, summary):
    http = summary["http"]
    with open(path, "w") as handle:
        handle.write("# Online Boutique Benchmark Result\n\n")
        handle.write("* Endpoint: `{}`\n".format(summary["run"]["endpoint"]))
        handle.write("* Target RPS: `{}`\n".format(summary["config"]["target_rps"]))
        handle.write("* Achieved RPS: `{:.3f}`\n".format(http["achieved_rps"]))
        handle.write("* Completed requests: `{}`\n".format(http["completed_requests"]))
        handle.write("* Error requests: `{}`\n".format(http["error_requests"]))
        handle.write("* Jaeger URL: `{}`\n".format(summary["run"]["jaeger_url"]))
        handle.write("* Prometheus URL: `{}`\n\n".format(summary["run"].get("prometheus_url", "")))
        handle.write("## HTTP Response Time MS\n\n")
        for key in ["min", "avg", "p50", "p90", "p95", "p99", "max"]:
            handle.write("* {}: `{}`\n".format(key, format_value(http["response_time_ms"].get(key))))
        handle.write("\n## Resource Rows\n\n")
        resource = summary["resource"]
        handle.write("* Container metrics: `{}`\n".format(resource["container_metric_rows"]))
        handle.write("* Node metrics: `{}`\n".format(resource["node_metric_rows"]))
        handle.write("* Resource limits: `{}`\n".format(resource["container_resource_limit_rows"]))
        handle.write("* Deployment replicas: `{}`\n".format(resource["deployment_replica_rows"]))
        handle.write("* Service traffic rows: `{}`\n".format(summary["traffic"]["rows"]))
        handle.write("* Service traffic bytes: `{}`\n".format(format_value(summary["traffic"]["total_bytes"])))
        handle.write("\n## Outputs\n\n")
        for name, output in sorted(summary["outputs"].items()):
            if isinstance(output, dict):
                for child_name, child_output in sorted(output.items()):
                    handle.write("* {}.{}: `{}`\n".format(name, child_name, child_output))
            else:
                handle.write("* {}: `{}`\n".format(name, output))
        if summary["errors"]:
            handle.write("\n## Collection Errors\n\n")
            for error in summary["errors"]:
                handle.write("* `{}`\n".format(error))


def main():
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

    config = {
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

    errors = []
    trace_services = []
    spans = []
    root_durations_ms = []
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

    container_rows = []
    node_rows = []
    limit_rows = []
    replica_rows = []
    traffic_rows = []
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
        if not sample.get("error") and sample.get("http_code") and 200 <= int(sample["http_code"]) < 500
    ]

    outputs = {
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

    summary_config = dict(config)
    summary_config["request_paths"] = [path["path"] for path in config["request_paths"]]

    summary = {
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
