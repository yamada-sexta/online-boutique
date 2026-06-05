#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${1:-https://github.com/GoogleCloudPlatform/microservices-demo.git}"
REPO_REF="${2:-main}"
NODE_PORT="${3:-30080}"

LOG_DIR="/local/logs"
APP_DIR="/local/online-boutique"
ACCESS_FILE="/local/online-boutique-access.txt"

mkdir -p "${LOG_DIR}"
exec > >(tee -a "${LOG_DIR}/online-boutique-setup.log") 2>&1

echo "Starting Online Boutique setup at $(date -Is)"
echo "Repository: ${REPO_URL}"
echo "Ref: ${REPO_REF}"
echo "NodePort: ${NODE_PORT}"

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y ca-certificates curl git jq

if ! command -v k3s >/dev/null 2>&1; then
    curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="--write-kubeconfig-mode 0644 --disable traefik" sh -
fi

export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

until kubectl get nodes >/dev/null 2>&1; do
    echo "Waiting for Kubernetes API..."
    sleep 5
done

kubectl wait --for=condition=Ready node --all --timeout=300s

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
kubectl get pods -o wide
kubectl get service frontend-external -o wide

cat > "${ACCESS_FILE}" <<EOF
Online Boutique is deployed.

Frontend:
  http://$(hostname -f):${NODE_PORT}

Useful commands:
  sudo tail -f ${LOG_DIR}/online-boutique-setup.log
  sudo kubectl get pods
  sudo kubectl get service frontend-external
EOF

cat "${ACCESS_FILE}"
echo "Finished Online Boutique setup at $(date -Is)"
