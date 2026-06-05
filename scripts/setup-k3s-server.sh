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

LOG_DIR="/local/logs"
APP_DIR="/local/online-boutique"
ACCESS_FILE="/local/online-boutique-access.txt"

mkdir -p "${LOG_DIR}"
exec > >(tee -a "${LOG_DIR}/online-boutique-setup.log") 2>&1

echo "Starting multi-node Online Boutique setup at $(date -Is)"
echo "Control IP: ${CONTROL_IP}"
echo "Expected Kubernetes nodes: ${EXPECTED_NODES}"
echo "Repository: ${REPO_URL}"
echo "Ref: ${REPO_REF}"
echo "NodePort: ${NODE_PORT}"

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y ca-certificates curl git jq

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

kubectl rollout status deployment/frontend --timeout=600s
kubectl get nodes -o wide
kubectl get pods -o wide
kubectl get service frontend-external -o wide

cat > "${ACCESS_FILE}" <<EOF
Online Boutique is deployed on a multi-node K3s cluster.

Frontend:
  http://$(hostname -f):${NODE_PORT}

Useful commands:
  sudo tail -f ${LOG_DIR}/online-boutique-setup.log
  sudo kubectl get nodes -o wide
  sudo kubectl get pods -o wide
  sudo kubectl get service frontend-external
EOF

cat "${ACCESS_FILE}"
echo "Finished multi-node Online Boutique setup at $(date -Is)"
