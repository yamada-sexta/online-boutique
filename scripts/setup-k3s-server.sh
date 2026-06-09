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
PROMETHEUS_PORT="${8:-30090}"
METADATA_PORT="${9:-18080}"
GITHUB_KEY_USER="${10:-yamada-sexta}"
SSH_KEY_LOGIN="${11:-angl5}"

LOG_DIR="/local/logs"
APP_DIR="/local/online-boutique"
ACCESS_FILE="/local/online-boutique-access.txt"
METADATA_DIR="/local/online-boutique-metadata"
ISTIO_PARENT="/local"

mkdir -p "${LOG_DIR}"
exec > >(tee -a "${LOG_DIR}/online-boutique-setup.log") 2>&1

echo "Starting multi-node Online Boutique setup at $(date -Is)"
echo "Control IP: ${CONTROL_IP}"
echo "Expected Kubernetes nodes: ${EXPECTED_NODES}"
echo "Repository: ${REPO_URL}"
echo "Ref: ${REPO_REF}"
echo "NodePort: ${NODE_PORT}"
echo "Jaeger UI NodePort: ${JAEGER_UI_PORT}"
echo "Prometheus NodePort: ${PROMETHEUS_PORT}"
echo "Metadata HTTP port: ${METADATA_PORT}"
echo "GitHub key user: ${GITHUB_KEY_USER}"
echo "SSH key login: ${SSH_KEY_LOGIN}"

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y ca-certificates curl git jq openssh-client python3

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

install_istio() {
    if kubectl get namespace istio-system >/dev/null 2>&1 && kubectl -n istio-system get deployment/istiod >/dev/null 2>&1; then
        echo "Istio already appears to be installed."
    else
        echo "Installing Istio."
        cd "${ISTIO_PARENT}"
        curl -L https://istio.io/downloadIstio | sh -
        ISTIO_DIR="$(find "${ISTIO_PARENT}" -maxdepth 1 -type d -name 'istio-*' -printf '%T@ %p\n' | sort -nr | awk 'NR==1 {print $2}')"
        if [ -z "${ISTIO_DIR}" ] || [ ! -x "${ISTIO_DIR}/bin/istioctl" ]; then
            echo "Could not find downloaded istioctl."
            exit 1
        fi
        "${ISTIO_DIR}/bin/istioctl" install -f "${ISTIO_DIR}/samples/bookinfo/demo-profile-no-gateways.yaml" -y
    fi

    kubectl label namespace default istio-injection=enabled --overwrite
    kubectl rollout status deployment/istiod -n istio-system --timeout=600s
}

patch_nodeport() {
    service_name="${1:?service name required}"
    namespace="${2:?namespace required}"
    port_name="${3:?port name required}"
    port="${4:?port required}"
    target_port="${5:?target port required}"
    node_port="${6:?node port required}"

    kubectl -n "${namespace}" patch service "${service_name}" --type merge -p "{
      \"spec\": {
        \"type\": \"NodePort\",
        \"ports\": [
          {
            \"name\": \"${port_name}\",
            \"port\": ${port},
            \"targetPort\": ${target_port},
            \"nodePort\": ${node_port}
          }
        ]
      }
    }"
}

capture_metadata_file() {
    filename="${1:?filename required}"
    shift
    {
        echo "$ $*"
        echo
        "$@" || echo
    } > "${METADATA_DIR}/${filename}" 2>&1
}

publish_metadata() {
    mkdir -p "${METADATA_DIR}"
    capture_metadata_file "kubectl-nodes.txt" kubectl get nodes -o wide
    capture_metadata_file "kubectl-pods.txt" kubectl get pods -A -o wide
    capture_metadata_file "kubectl-services.txt" kubectl get services -A -o wide
    capture_metadata_file "kubectl-deployments.txt" kubectl get deployments -A -o wide
    date -u +%Y-%m-%dT%H:%M:%SZ > "${METADATA_DIR}/ready.txt"

    if pgrep -f "http.server ${METADATA_PORT}.*${METADATA_DIR}" >/dev/null 2>&1; then
        echo "Metadata HTTP server already running."
        return
    fi

    nohup python3 -m http.server "${METADATA_PORT}" \
        --bind "${CONTROL_IP}" \
        --directory "${METADATA_DIR}" \
        > "${LOG_DIR}/metadata-server.log" 2>&1 &
    echo "Started metadata HTTP server on ${CONTROL_IP}:${METADATA_PORT}"
}

install_istio

kubectl apply -f /local/repository/k8s/prometheus.yaml
kubectl apply -f /local/repository/k8s/node-exporter.yaml
kubectl apply -f /local/repository/k8s/kube-state-metrics.yaml

kubectl rollout status deployment/prometheus -n istio-system --timeout=600s
kubectl rollout status deployment/kube-state-metrics -n kube-system --timeout=300s
kubectl rollout status daemonset/prometheus-node-exporter -n istio-system --timeout=300s
patch_nodeport prometheus istio-system http 9090 9090 "${PROMETHEUS_PORT}"

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
patch_nodeport frontend-external default http 80 8080 "${NODE_PORT}"

kubectl apply -f /local/repository/k8s/jaeger.yaml
patch_nodeport jaeger-ui default ui 16686 16686 "${JAEGER_UI_PORT}"
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
kubectl get pods -A -o wide
kubectl get service frontend-external -o wide
kubectl get service jaeger jaeger-ui -o wide
kubectl get service prometheus -n istio-system -o wide

publish_metadata

cat > "${ACCESS_FILE}" <<EOF
Online Boutique is deployed on a multi-node K3s cluster.

Frontend:
  http://$(hostname -f):${NODE_PORT}
  http://${CONTROL_IP}:${NODE_PORT}

Jaeger UI:
  http://$(hostname -f):${JAEGER_UI_PORT}
  http://${CONTROL_IP}:${JAEGER_UI_PORT}

Prometheus:
  http://$(hostname -f):${PROMETHEUS_PORT}
  http://${CONTROL_IP}:${PROMETHEUS_PORT}

Benchmark:
  Runs automatically from the dedicated benchmark node.

Useful commands:
  sudo tail -f ${LOG_DIR}/online-boutique-setup.log
  sudo tail -f ${LOG_DIR}/metadata-server.log
  sudo kubectl get nodes -o wide
  sudo kubectl get pods -A -o wide
  sudo kubectl get service frontend-external
  sudo kubectl get service jaeger jaeger-ui
  sudo kubectl get service prometheus -n istio-system

Metadata:
  http://${CONTROL_IP}:${METADATA_PORT}/ready.txt
EOF

cat "${ACCESS_FILE}"
echo "Finished multi-node Online Boutique setup at $(date -Is)"
