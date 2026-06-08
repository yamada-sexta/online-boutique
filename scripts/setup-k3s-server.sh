#!/usr/bin/env bash
set -euo pipefail

if [ "${EUID}" -ne 0 ]; then
    exec sudo "$0" "$@"
fi

CONTROL_IP="${1:?control IP is required}"
K3S_TOKEN="${2:?K3s token is required}"
EXPECTED_NODES="${3:?expected node count is required}"
REPO_URL="${4:-https://github.com/GoogleCloudPlatform/microservices-demo.git}"
REPO_REF="${5:-main}"
NODE_PORT="${6:-30080}"
JAEGER_UI_PORT="${7:-30686}"
BENCHMARK_ENABLED="${8:-true}"
RESULTS_PUSH_ENABLED="${9:-true}"
RESULTS_REPO="${10:-git@github.com:yamada-sexta/online-boutique-bench-res.git}"
RESULTS_BRANCH="${11:-main}"
GITHUB_KEY_USER="${12:-yamada-sexta}"
SSH_KEY_LOGIN="${13:-angl5}"
BENCHMARK_TARGET_RPS="${14:-5}"
BENCHMARK_DURATION_SECONDS="${15:-60}"
BENCHMARK_WARMUP_SECONDS="${16:-10}"
BENCHMARK_CONCURRENCY="${17:-8}"
BENCHMARK_REQUEST_TIMEOUT_SECONDS="${18:-30}"
BENCHMARK_REQUEST_PATHS="${19:-/}"
BENCHMARK_RTT_SAMPLES="${20:-10}"
BENCHMARK_TRACE_LIMIT="${21:-500}"
BENCHMARK_LOOKBACK="${22:-1h}"

LOG_DIR="/local/logs"
APP_DIR="/local/online-boutique"
ACCESS_FILE="/local/online-boutique-access.txt"
BENCHMARK_CONFIG="/local/repository/benchmark/config.json"
BENCHMARK_OUTPUT_ROOT="/local/benchmark-results"

mkdir -p "${LOG_DIR}"
exec > >(tee -a "${LOG_DIR}/online-boutique-setup.log") 2>&1

echo "Starting multi-node Online Boutique setup at $(date -Is)"
echo "Control IP: ${CONTROL_IP}"
echo "Expected Kubernetes nodes: ${EXPECTED_NODES}"
echo "Repository: ${REPO_URL}"
echo "Ref: ${REPO_REF}"
echo "NodePort: ${NODE_PORT}"
echo "Jaeger UI NodePort: ${JAEGER_UI_PORT}"
echo "Benchmark enabled: ${BENCHMARK_ENABLED}"
echo "Results push enabled: ${RESULTS_PUSH_ENABLED}"
echo "Results repository: ${RESULTS_REPO}"
echo "Results branch: ${RESULTS_BRANCH}"
echo "GitHub key user: ${GITHUB_KEY_USER}"
echo "SSH key login: ${SSH_KEY_LOGIN}"
echo "Benchmark target RPS: ${BENCHMARK_TARGET_RPS}"
echo "Benchmark duration seconds: ${BENCHMARK_DURATION_SECONDS}"
echo "Benchmark warmup seconds: ${BENCHMARK_WARMUP_SECONDS}"
echo "Benchmark concurrency: ${BENCHMARK_CONCURRENCY}"
echo "Benchmark request timeout seconds: ${BENCHMARK_REQUEST_TIMEOUT_SECONDS}"
echo "Benchmark request paths: ${BENCHMARK_REQUEST_PATHS}"
echo "Benchmark RTT samples: ${BENCHMARK_RTT_SAMPLES}"
echo "Benchmark trace limit: ${BENCHMARK_TRACE_LIMIT}"
echo "Benchmark lookback: ${BENCHMARK_LOOKBACK}"

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y ca-certificates curl git jq openssh-client

/local/repository/scripts/install-github-keys.sh "${GITHUB_KEY_USER}" "${SSH_KEY_LOGIN}"

LAN_IFACE="$(ip -o -4 addr show | awk -v ip="${CONTROL_IP}" '$0 ~ ip {print $2; exit}')"
if [ -z "${LAN_IFACE}" ]; then
    echo "Could not find interface with IP ${CONTROL_IP}"
    ip -o -4 addr show
    exit 1
fi

if ! command -v k3s >/dev/null 2>&1; then
    curl -sfL https://get.k3s.io | \
        K3S_TOKEN="${K3S_TOKEN}" \
        INSTALL_K3S_EXEC="server --write-kubeconfig-mode 0644 --disable traefik --node-ip ${CONTROL_IP} --advertise-address ${CONTROL_IP} --flannel-iface ${LAN_IFACE}" \
        sh -
fi

export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

until kubectl get nodes >/dev/null 2>&1; do
    echo "Waiting for Kubernetes API..."
    sleep 5
done

echo "Waiting for ${EXPECTED_NODES} Kubernetes nodes to register..."
for _ in $(seq 1 120); do
    node_count="$(kubectl get nodes --no-headers 2>/dev/null | wc -l)"
    if [ "${node_count}" -ge "${EXPECTED_NODES}" ]; then
        break
    fi
    kubectl get nodes -o wide || true
    sleep 5
done

node_count="$(kubectl get nodes --no-headers 2>/dev/null | wc -l)"
if [ "${node_count}" -lt "${EXPECTED_NODES}" ]; then
    echo "Only ${node_count}/${EXPECTED_NODES} Kubernetes nodes registered."
    kubectl get nodes -o wide || true
    exit 1
fi

kubectl wait --for=condition=Ready node --all --timeout=600s

if [ -d "${APP_DIR}/.git" ]; then
    git -C "${APP_DIR}" fetch --all --tags
else
    rm -rf "${APP_DIR}"
    git clone "${REPO_URL}" "${APP_DIR}"
fi

cd "${APP_DIR}"
git fetch origin "${REPO_REF}" || true
git checkout "${REPO_REF}" || git checkout "origin/${REPO_REF}" || git checkout --detach "${REPO_REF}"

kubectl apply -f release/kubernetes-manifests.yaml

kubectl patch service frontend-external --type merge -p "{
  \"spec\": {
    \"type\": \"NodePort\",
    \"ports\": [
      {
        \"name\": \"http\",
        \"port\": 80,
        \"targetPort\": 8080,
        \"nodePort\": ${NODE_PORT}
      }
    ]
  }
}"

kubectl apply -f /local/repository/k8s/jaeger.yaml
kubectl patch service jaeger-ui --type merge -p "{
  \"spec\": {
    \"ports\": [
      {
        \"name\": \"ui\",
        \"port\": 16686,
        \"targetPort\": 16686,
        \"nodePort\": ${JAEGER_UI_PORT}
      }
    ]
  }
}"
kubectl rollout status deployment/jaeger --timeout=300s

for service in \
    checkoutservice \
    currencyservice \
    emailservice \
    frontend \
    paymentservice \
    productcatalogservice \
    recommendationservice; do
    kubectl set env "deployment/${service}" \
        ENABLE_TRACING=1 \
        "COLLECTOR_SERVICE_ADDR=jaeger:4317" \
        "OTEL_SERVICE_NAME=${service}"
done

kubectl rollout status deployment/frontend --timeout=600s
for service in \
    checkoutservice \
    currencyservice \
    emailservice \
    paymentservice \
    productcatalogservice \
    recommendationservice; do
    kubectl rollout status "deployment/${service}" --timeout=600s
done
kubectl get nodes -o wide
kubectl get pods -o wide
kubectl get service frontend-external -o wide
kubectl get service jaeger jaeger-ui -o wide

BENCHMARK_OUTPUT_DIR=""
if [ "${BENCHMARK_ENABLED}" = "true" ]; then
    RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
    BENCHMARK_OUTPUT_DIR="${BENCHMARK_OUTPUT_ROOT}/${RUN_ID}"
    FRONTEND_ENDPOINT="http://$(hostname -f):${NODE_PORT}/"
    JAEGER_CLUSTER_IP="$(kubectl get svc/jaeger -o jsonpath='{.spec.clusterIP}')"
    JAEGER_URL="http://${JAEGER_CLUSTER_IP}:16686"

    echo "Starting benchmark run ${RUN_ID}"
    /local/repository/scripts/run-benchmark.py \
        --config "${BENCHMARK_CONFIG}" \
        --endpoint "${FRONTEND_ENDPOINT}" \
        --jaeger-url "${JAEGER_URL}" \
        --output-dir "${BENCHMARK_OUTPUT_DIR}" \
        --target-rps "${BENCHMARK_TARGET_RPS}" \
        --duration-seconds "${BENCHMARK_DURATION_SECONDS}" \
        --warmup-seconds "${BENCHMARK_WARMUP_SECONDS}" \
        --concurrency "${BENCHMARK_CONCURRENCY}" \
        --request-timeout-seconds "${BENCHMARK_REQUEST_TIMEOUT_SECONDS}" \
        --request-paths "${BENCHMARK_REQUEST_PATHS}" \
        --rtt-samples "${BENCHMARK_RTT_SAMPLES}" \
        --trace-limit "${BENCHMARK_TRACE_LIMIT}" \
        --lookback "${BENCHMARK_LOOKBACK}"
    echo "Finished benchmark run ${RUN_ID}; output: ${BENCHMARK_OUTPUT_DIR}"

    if [ "${RESULTS_PUSH_ENABLED}" = "true" ]; then
        /local/repository/scripts/push-benchmark-results.sh \
            "${BENCHMARK_OUTPUT_DIR}" \
            "${RESULTS_REPO}" \
            "${RESULTS_BRANCH}"
    fi
fi

cat > "${ACCESS_FILE}" <<EOF
Online Boutique is deployed on a multi-node K3s cluster.

Frontend:
  http://$(hostname -f):${NODE_PORT}

Jaeger UI:
  http://$(hostname -f):${JAEGER_UI_PORT}

Useful commands:
  sudo tail -f ${LOG_DIR}/online-boutique-setup.log
  sudo kubectl get nodes -o wide
  sudo kubectl get pods -o wide
  sudo kubectl get service frontend-external
  sudo kubectl get service jaeger jaeger-ui
  /local/repository/scripts/collect-latency.py --endpoint http://$(hostname -f):${NODE_PORT}/
  /local/repository/scripts/run-benchmark.py --endpoint http://$(hostname -f):${NODE_PORT}/

Benchmark:
  Enabled: ${BENCHMARK_ENABLED}
  Target RPS: ${BENCHMARK_TARGET_RPS}
  Duration seconds: ${BENCHMARK_DURATION_SECONDS}
  Warmup seconds: ${BENCHMARK_WARMUP_SECONDS}
  Concurrency: ${BENCHMARK_CONCURRENCY}
  Request paths: ${BENCHMARK_REQUEST_PATHS}
  Results push enabled: ${RESULTS_PUSH_ENABLED}
  Last output directory: ${BENCHMARK_OUTPUT_DIR:-not run}
EOF

cat "${ACCESS_FILE}"
echo "Finished multi-node Online Boutique setup at $(date -Is)"
