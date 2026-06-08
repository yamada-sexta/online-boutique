#!/usr/bin/env python3
"""Run a configured Online Boutique benchmark and collect Jaeger latency data."""

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


DEFAULT_CONFIG = "/local/repository/benchmark/config.json"
DEFAULT_OUTPUT_ROOT = "/local/benchmark-results"


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def run(argv, check=False):
    return subprocess.run(argv, check=check, text=True, capture_output=True)


def load_config(path):
    with open(path) as handle:
        return json.load(handle)


def apply_overrides(config, args):
    if args.target_rps is not None:
        config["target_rps"] = args.target_rps
    if args.duration_seconds is not None:
        config["duration_seconds"] = args.duration_seconds
    if args.warmup_seconds is not None:
        config["warmup_seconds"] = args.warmup_seconds
    if args.concurrency is not None:
        config["concurrency"] = args.concurrency
    if args.request_timeout_seconds is not None:
        config["request_timeout_seconds"] = args.request_timeout_seconds
    if args.request_paths:
        config["request_paths"] = [
            {"path": path.strip(), "weight": 1}
            for path in args.request_paths.split(",")
            if path.strip()
        ]
    if args.rtt_samples is not None:
        config["rtt_samples"] = args.rtt_samples
    if args.trace_limit is not None:
        config.setdefault("jaeger", {})["trace_limit"] = args.trace_limit
    if args.lookback:
        config.setdefault("jaeger", {})["lookback"] = args.lookback
    return config


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
    clean = [float(value) for value in values if value is not None]
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


def kubectl_jsonpath(resource, jsonpath):
    env = os.environ.copy()
    env.setdefault("KUBECONFIG", "/etc/rancher/k3s/k3s.yaml")
    result = subprocess.run(
        ["kubectl", "get", resource, "-o", "jsonpath=" + jsonpath],
        text=True,
        capture_output=True,
        env=env,
    )
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def default_jaeger_url():
    cluster_ip = kubectl_jsonpath("svc/jaeger", "{.spec.clusterIP}")
    if cluster_ip:
        return "http://{}:16686".format(cluster_ip)
    return "http://jaeger:16686"


def fetch_json(url):
    with urllib.request.urlopen(url, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def normalize_paths(config):
    entries = config.get("request_paths") or [{"path": "/", "weight": 1}]
    paths = []
    for entry in entries:
        if isinstance(entry, str):
            paths.append({"path": entry, "weight": 1})
            continue
        path = entry.get("path", "/")
        weight = float(entry.get("weight", 1))
        if weight <= 0:
            continue
        paths.append({"path": path, "weight": weight})
    if not paths:
        paths.append({"path": "/", "weight": 1})
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
    target_rps = float(config.get("target_rps", 1))
    concurrency = int(config.get("concurrency", 1))
    timeout = float(config.get("request_timeout_seconds", 30))
    user_agent = config.get("user_agent", "cloudlab-online-boutique-benchmark/1.0")
    paths = normalize_paths(config)

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


def query_traces(jaeger_url, service, limit, lookback):
    query = urllib.parse.urlencode(
        {
            "service": service,
            "limit": str(limit),
            "lookback": lookback,
        }
    )
    url = jaeger_url.rstrip("/") + "/api/traces?" + query
    return fetch_json(url).get("data", [])


def list_jaeger_services(jaeger_url):
    url = jaeger_url.rstrip("/") + "/api/services"
    services = fetch_json(url).get("data", [])
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


def capture_kubernetes_metadata(output_dir):
    return {
        "nodes": capture_command(output_dir, "kubectl-nodes.txt", ["kubectl", "get", "nodes", "-o", "wide"]),
        "pods": capture_command(output_dir, "kubectl-pods.txt", ["kubectl", "get", "pods", "-o", "wide"]),
        "services": capture_command(output_dir, "kubectl-services.txt", ["kubectl", "get", "services", "-o", "wide"]),
        "deployments": capture_command(output_dir, "kubectl-deployments.txt", ["kubectl", "get", "deployments", "-o", "wide"]),
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


def write_summary_markdown(path, summary):
    http = summary["http"]
    with open(path, "w") as handle:
        handle.write("# Online Boutique Benchmark Result\n\n")
        handle.write("* Endpoint: `{}`\n".format(summary["endpoint"]))
        handle.write("* Target RPS: `{}`\n".format(summary["config"]["target_rps"]))
        handle.write("* Achieved RPS: `{:.3f}`\n".format(http["achieved_rps"]))
        handle.write("* Completed requests: `{}`\n".format(http["completed_requests"]))
        handle.write("* Error requests: `{}`\n".format(http["error_requests"]))
        handle.write("* Jaeger URL: `{}`\n\n".format(summary["jaeger_url"]))
        handle.write("## HTTP Response Time MS\n\n")
        for key in ["min", "avg", "p50", "p90", "p95", "p99", "max"]:
            value = http["response_time_ms"].get(key)
            handle.write("* {}: `{}`\n".format(key, "{:.3f}".format(value) if value is not None else "null"))
        handle.write("\n## Outputs\n\n")
        for name, output in sorted(summary["outputs"].items()):
            handle.write("* {}: `{}`\n".format(name, output))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--jaeger-url", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--target-rps", type=float, default=None)
    parser.add_argument("--duration-seconds", type=float, default=None)
    parser.add_argument("--warmup-seconds", type=float, default=None)
    parser.add_argument("--concurrency", type=int, default=None)
    parser.add_argument("--request-timeout-seconds", type=float, default=None)
    parser.add_argument(
        "--request-paths",
        default=None,
        help="Comma-separated request paths, overriding config request_paths.",
    )
    parser.add_argument("--rtt-samples", type=int, default=None)
    parser.add_argument("--trace-limit", type=int, default=None)
    parser.add_argument("--lookback", default=None)
    args = parser.parse_args()

    config = apply_overrides(load_config(args.config), args)
    random.seed(config.get("random_seed", 1))

    output_root = config.get("output_root", DEFAULT_OUTPUT_ROOT)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = args.output_dir or os.path.join(output_root, timestamp)
    os.makedirs(output_dir, exist_ok=True)

    parsed_endpoint = urllib.parse.urlparse(args.endpoint)
    ping_host = parsed_endpoint.hostname
    rtt_samples = collect_rtt_samples(
        ping_host,
        int(config.get("rtt_samples", 0)),
        float(config.get("rtt_interval_seconds", 1)),
    )

    warmup = run_load(args.endpoint, config, float(config.get("warmup_seconds", 0)), "warmup")
    if warmup["samples"]:
        write_csv(
            os.path.join(output_dir, "warmup-http-samples.csv"),
            HTTP_FIELDS,
            warmup["samples"],
        )

    benchmark = run_load(args.endpoint, config, float(config.get("duration_seconds", 60)), "benchmark")

    jaeger_config = config.get("jaeger", {})
    jaeger_url = args.jaeger_url or default_jaeger_url()
    flush_wait = float(jaeger_config.get("flush_wait_seconds", 10))
    if flush_wait > 0:
        time.sleep(flush_wait)

    jaeger_error = ""
    trace_services = []
    spans = []
    root_durations_ms = []
    try:
        trace_service = jaeger_config.get("trace_service", "all")
        if trace_service == "all":
            trace_services = list_jaeger_services(jaeger_url)
        else:
            trace_services = [
                service.strip()
                for service in trace_service.split(",")
                if service.strip()
            ]
        spans, root_durations_ms = collect_traces(
            jaeger_url,
            trace_services,
            int(jaeger_config.get("trace_limit", 500)),
            jaeger_config.get("lookback", "1h"),
        )
    except Exception as exc:
        jaeger_error = str(exc)

    http_csv = os.path.join(output_dir, "http-samples.csv")
    rtt_csv = os.path.join(output_dir, "rtt-samples.csv")
    spans_csv = os.path.join(output_dir, "jaeger-spans.csv")
    service_csv = os.path.join(output_dir, "service-latency.csv")
    operation_csv = os.path.join(output_dir, "operation-latency.csv")
    summary_json = os.path.join(output_dir, "summary.json")
    summary_md = os.path.join(output_dir, "summary.md")

    write_csv(http_csv, HTTP_FIELDS, benchmark["samples"])
    write_csv(rtt_csv, ["sample_id", "timestamp", "host", "rtt_ms"], rtt_samples)
    write_csv(spans_csv, SPAN_FIELDS, spans)
    service_summary = write_service_summary(service_csv, spans)
    write_operation_summary(operation_csv, spans)
    kubernetes_outputs = capture_kubernetes_metadata(output_dir)

    completed = len(benchmark["samples"])
    transport_error_requests = len([sample for sample in benchmark["samples"] if sample.get("error")])
    error_requests = len([sample for sample in benchmark["samples"] if is_error_sample(sample)])
    success_response_times = [
        sample.get("response_time_ms")
        for sample in benchmark["samples"]
        if not sample.get("error") and sample.get("http_code") and 200 <= int(sample["http_code"]) < 500
    ]

    summary = {
        "endpoint": args.endpoint,
        "jaeger_url": jaeger_url,
        "jaeger_error": jaeger_error,
        "queried_trace_services": trace_services,
        "config": config,
        "started_at": benchmark["started_at"],
        "finished_at": benchmark["finished_at"],
        "hostname": run(["hostname", "-f"]).stdout.strip(),
        "http": {
            "target_rps": float(config.get("target_rps", 1)),
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
        "trace_root_duration_ms": summarize(root_durations_ms),
        "trace_service_duration_ms": service_summary,
        "outputs": {
            "http_samples_csv": http_csv,
            "rtt_samples_csv": rtt_csv,
            "jaeger_spans_csv": spans_csv,
            "service_latency_csv": service_csv,
            "operation_latency_csv": operation_csv,
            "summary_json": summary_json,
            "summary_md": summary_md,
            "kubernetes": kubernetes_outputs,
        },
    }

    with open(summary_json, "w") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")
    write_summary_markdown(summary_md, summary)

    print(json.dumps({"output_dir": output_dir, "summary_json": summary_json}, indent=2))


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


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
