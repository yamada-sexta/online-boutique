#!/usr/bin/env python3
"""Collect Online Boutique HTTP timing and Jaeger span latency.

Run on the CloudLab control node after the profile has finished:

    /local/repository/scripts/collect-latency.py \
        --endpoint http://$(hostname -f):30080/

Outputs are written under /local/latency/<timestamp>/ by default.
"""

import argparse
import csv
import json
import math
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone


DEFAULT_OUTPUT_ROOT = "/local/latency"
DEFAULT_JAEGER_LOOKBACK = "1h"
DEFAULT_TRACE_LIMIT = 100


def run(argv, check=False):
    return subprocess.run(argv, check=check, text=True, capture_output=True)


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
    clean = [float(v) for v in values if v is not None]
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


def curl_sample(endpoint, ping_host):
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
    result = run(
        [
            "curl",
            "-sS",
            "-o",
            "/dev/null",
            "--max-time",
            "30",
            "-w",
            fmt,
            endpoint,
        ]
    )
    timestamp = datetime.now(timezone.utc).isoformat()
    if result.returncode != 0:
        return {
            "timestamp": timestamp,
            "http_code": None,
            "ping_rtt_ms": ping_rtt_ms(ping_host),
            "error": result.stderr.strip() or "curl failed",
        }

    parts = result.stdout.strip().split("\t")
    if len(parts) != 8:
        return {
            "timestamp": timestamp,
            "http_code": None,
            "ping_rtt_ms": ping_rtt_ms(ping_host),
            "error": "unexpected curl timing output: " + result.stdout.strip(),
        }

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

    return {
        "timestamp": timestamp,
        "http_code": int(http_code),
        "ping_rtt_ms": ping_rtt_ms(ping_host),
        "dns_ms": lookup_s * 1000.0,
        "tcp_connect_ms": max(0.0, (connect_s - lookup_s) * 1000.0),
        "tls_handshake_ms": max(0.0, (appconnect_s - connect_s) * 1000.0),
        "request_time_ms": max(0.0, (starttransfer_s - pretransfer_s) * 1000.0),
        "time_to_first_byte_ms": starttransfer_s * 1000.0,
        "response_time_ms": total_s * 1000.0,
        "response_transfer_ms": max(0.0, (total_s - starttransfer_s) * 1000.0),
        "size_download_bytes": int(float(size_download)),
        "error": "",
    }


def write_http_samples(path, samples):
    fields = [
        "timestamp",
        "http_code",
        "ping_rtt_ms",
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
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for sample in samples:
            writer.writerow({field: sample.get(field, "") for field in fields})


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


def write_spans(path, spans):
    fields = [
        "trace_id",
        "span_id",
        "parent_span_id",
        "service",
        "operation",
        "duration_ms",
        "start_time_unix_us",
    ]
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for span in spans:
            writer.writerow(span)


def write_service_summary(path, spans):
    by_service = {}
    for span in spans:
        by_service.setdefault(span["service"], []).append(span["duration_ms"])

    fields = ["service", "count", "min", "avg", "p50", "p90", "p95", "p99", "max"]
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for service in sorted(by_service):
            row = summarize(by_service[service])
            row["service"] = service
            writer.writerow(row)

    return {service: summarize(values) for service, values in sorted(by_service.items())}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True, help="HTTP endpoint to sample")
    parser.add_argument("--samples", type=int, default=50)
    parser.add_argument("--interval-seconds", type=float, default=0.2)
    parser.add_argument("--jaeger-url", default=None)
    parser.add_argument(
        "--trace-service",
        default="all",
        help="Jaeger service to query, comma-separated services, or 'all'",
    )
    parser.add_argument("--trace-limit", type=int, default=DEFAULT_TRACE_LIMIT)
    parser.add_argument("--lookback", default=DEFAULT_JAEGER_LOOKBACK)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    parsed_endpoint = urllib.parse.urlparse(args.endpoint)
    ping_host = parsed_endpoint.hostname
    jaeger_url = args.jaeger_url or default_jaeger_url()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = args.output_dir or os.path.join(DEFAULT_OUTPUT_ROOT, timestamp)
    os.makedirs(output_dir, exist_ok=True)

    samples = []
    for i in range(args.samples):
        samples.append(curl_sample(args.endpoint, ping_host))
        if i + 1 < args.samples:
            time.sleep(args.interval_seconds)

    # Give the BatchSpanProcessor in the services time to flush.
    time.sleep(5)

    if args.trace_service == "all":
        trace_services = list_jaeger_services(jaeger_url)
    else:
        trace_services = [
            service.strip()
            for service in args.trace_service.split(",")
            if service.strip()
        ]

    spans, root_durations_ms = collect_traces(
        jaeger_url,
        trace_services,
        args.trace_limit,
        args.lookback,
    )

    http_csv = os.path.join(output_dir, "http-samples.csv")
    spans_csv = os.path.join(output_dir, "jaeger-spans.csv")
    service_csv = os.path.join(output_dir, "service-latency.csv")
    summary_path = os.path.join(output_dir, "summary.json")

    write_http_samples(http_csv, samples)
    write_spans(spans_csv, spans)
    service_summary = write_service_summary(service_csv, spans)

    summary = {
        "endpoint": args.endpoint,
        "jaeger_url": jaeger_url,
        "queried_trace_services": trace_services,
        "samples": args.samples,
        "http": {
            "rtt_ms": summarize([sample.get("ping_rtt_ms") for sample in samples]),
            "tcp_connect_ms": summarize([sample.get("tcp_connect_ms") for sample in samples]),
            "request_time_ms": summarize([sample.get("request_time_ms") for sample in samples]),
            "time_to_first_byte_ms": summarize([sample.get("time_to_first_byte_ms") for sample in samples]),
            "response_time_ms": summarize([sample.get("response_time_ms") for sample in samples]),
        },
        "trace_root_duration_ms": summarize(root_durations_ms),
        "trace_service_duration_ms": service_summary,
        "outputs": {
            "http_samples_csv": http_csv,
            "jaeger_spans_csv": spans_csv,
            "service_latency_csv": service_csv,
            "summary_json": summary_path,
        },
    }

    with open(summary_path, "w") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")

    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
