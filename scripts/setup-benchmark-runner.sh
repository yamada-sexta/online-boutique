#!/usr/bin/env bash
set -euo pipefail

if [ "${EUID}" -ne 0 ]; then
    exec sudo "$0" "$@"
fi

CONTROL_IP="${1:?control IP is required}"
NODE_PORT="${2:-30080}"
JAEGER_UI_PORT="${3:-30686}"
PROMETHEUS_PORT="${4:-30090}"
METADATA_PORT="${5:-18080}"
RESULTS_PUSH_ENABLED="${6:-true}"
RESULTS_REPO="${7:-git@github.com:yamada-sexta/online-boutique-bench-res.git}"
RESULTS_BRANCH="${8:-main}"
GITHUB_KEY_USER="${9:-yamada-sexta}"
SSH_KEY_LOGIN="${10:-angl5}"
BENCHMARK_TARGET_RPS="${11:-5}"
BENCHMARK_DURATION_SECONDS="${12:-60}"
BENCHMARK_WARMUP_SECONDS="${13:-10}"
BENCHMARK_CONCURRENCY="${14:-8}"
BENCHMARK_REQUEST_TIMEOUT_SECONDS="${15:-30}"
BENCHMARK_REQUEST_PATHS="${16:-/}"
BENCHMARK_RTT_SAMPLES="${17:-10}"
BENCHMARK_TRACE_LIMIT="${18:-500}"
BENCHMARK_LOOKBACK="${19:-1h}"
RESOURCE_WINDOW="${20:-5m}"

LOG_DIR="/local/logs"
OUTPUT_ROOT="/local/benchmark-results"
UV_INSTALL_DIR="/usr/local/bin"

mkdir -p "${LOG_DIR}"
exec > >(tee -a "${LOG_DIR}/benchmark-runner-setup.log") 2>&1

echo "Starting benchmark runner setup at $(date -Is)"
echo "Control IP: ${CONTROL_IP}"
echo "Frontend NodePort: ${NODE_PORT}"
echo "Jaeger UI NodePort: ${JAEGER_UI_PORT}"
echo "Prometheus NodePort: ${PROMETHEUS_PORT}"
echo "Metadata HTTP port: ${METADATA_PORT}"
echo "Results push enabled: ${RESULTS_PUSH_ENABLED}"
echo "Results repository: ${RESULTS_REPO}"
echo "Results branch: ${RESULTS_BRANCH}"

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y ca-certificates curl git iputils-ping openssh-client

/local/repository/scripts/install-github-keys.sh "${GITHUB_KEY_USER}" "${SSH_KEY_LOGIN}"

install_uv() {
    if command -v uv >/dev/null 2>&1; then
        return
    fi

    export PATH="${UV_INSTALL_DIR}:/root/.local/bin:/users/${SSH_KEY_LOGIN}/.local/bin:/users/geniuser/.local/bin:${PATH}"
    for uv_path in \
        "${UV_INSTALL_DIR}/uv" \
        "/root/.local/bin/uv" \
        "/users/${SSH_KEY_LOGIN}/.local/bin/uv" \
        "/users/geniuser/.local/bin/uv"; do
        if [ -x "${uv_path}" ]; then
            export PATH="$(dirname "${uv_path}"):${PATH}"
            return
        fi
    done

    curl -LsSf https://astral.sh/uv/install.sh -o /tmp/install-uv.sh
    UV_INSTALL_DIR="${UV_INSTALL_DIR}" sh /tmp/install-uv.sh
    chmod 755 "${UV_INSTALL_DIR}/uv" "${UV_INSTALL_DIR}/uvx" 2>/dev/null || true

    if ! command -v uv >/dev/null 2>&1; then
        echo "uv installation completed but uv is not on PATH."
        exit 1
    fi
}

wait_url() {
    label="${1:?label required}"
    url="${2:?url required}"
    timeout_seconds="${3:-900}"
    deadline=$((SECONDS + timeout_seconds))

    echo "Waiting for ${label}: ${url}"
    while [ "${SECONDS}" -lt "${deadline}" ]; do
        if curl -fsS --max-time 10 "${url}" >/dev/null; then
            echo "${label} is reachable."
            return
        fi
        sleep 5
    done

    echo "Timed out waiting for ${label}: ${url}"
    exit 1
}

install_uv
export PATH="${UV_INSTALL_DIR}:/root/.local/bin:/users/${SSH_KEY_LOGIN}/.local/bin:/users/geniuser/.local/bin:${PATH}"
uv --version

FRONTEND_ENDPOINT="http://${CONTROL_IP}:${NODE_PORT}/"
JAEGER_URL="http://${CONTROL_IP}:${JAEGER_UI_PORT}"
PROMETHEUS_URL="http://${CONTROL_IP}:${PROMETHEUS_PORT}"
METADATA_URL="http://${CONTROL_IP}:${METADATA_PORT}"

wait_url "frontend" "${FRONTEND_ENDPOINT}" 1200
wait_url "jaeger" "${JAEGER_URL}/api/services" 1200
wait_url "prometheus" "${PROMETHEUS_URL}/-/ready" 1200
wait_url "control metadata" "${METADATA_URL}/ready.txt" 1200

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
BENCHMARK_OUTPUT_DIR="${OUTPUT_ROOT}/${RUN_ID}"
mkdir -p "${BENCHMARK_OUTPUT_DIR}"

echo "Starting benchmark run ${RUN_ID}"
cd /local/repository
uv run --script scripts/run-benchmark.py \
    --endpoint "${FRONTEND_ENDPOINT}" \
    --jaeger-url "${JAEGER_URL}" \
    --prometheus-url "${PROMETHEUS_URL}" \
    --metadata-url "${METADATA_URL}" \
    --output-dir "${BENCHMARK_OUTPUT_DIR}" \
    --target-rps "${BENCHMARK_TARGET_RPS}" \
    --duration-seconds "${BENCHMARK_DURATION_SECONDS}" \
    --warmup-seconds "${BENCHMARK_WARMUP_SECONDS}" \
    --concurrency "${BENCHMARK_CONCURRENCY}" \
    --request-timeout-seconds "${BENCHMARK_REQUEST_TIMEOUT_SECONDS}" \
    --request-paths "${BENCHMARK_REQUEST_PATHS}" \
    --rtt-samples "${BENCHMARK_RTT_SAMPLES}" \
    --trace-limit "${BENCHMARK_TRACE_LIMIT}" \
    --lookback "${BENCHMARK_LOOKBACK}" \
    --resource-window "${RESOURCE_WINDOW}"
echo "Finished benchmark run ${RUN_ID}; output: ${BENCHMARK_OUTPUT_DIR}"

if [ "${RESULTS_PUSH_ENABLED}" = "true" ]; then
    /local/repository/scripts/push-benchmark-results.sh \
        "${BENCHMARK_OUTPUT_DIR}" \
        "${RESULTS_REPO}" \
        "${RESULTS_BRANCH}"
fi

echo "Finished benchmark runner setup at $(date -Is)"
