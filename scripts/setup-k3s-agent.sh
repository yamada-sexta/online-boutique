#!/usr/bin/env bash
set -euo pipefail

if [ "${EUID}" -ne 0 ]; then
    exec sudo "$0" "$@"
fi

CONTROL_IP="${1:?control IP is required}"
WORKER_IP="${2:?worker IP is required}"
K3S_TOKEN="${3:?K3s token is required}"
GITHUB_KEY_USER="${4:-yamada-sexta}"
SSH_KEY_LOGIN="${5:-angl5}"

LOG_DIR="/local/logs"
mkdir -p "${LOG_DIR}"
exec > >(tee -a "${LOG_DIR}/k3s-agent-setup.log") 2>&1

echo "Starting K3s agent setup at $(date -Is)"
echo "Control IP: ${CONTROL_IP}"
echo "Worker IP: ${WORKER_IP}"
echo "GitHub key user: ${GITHUB_KEY_USER}"
echo "SSH key login: ${SSH_KEY_LOGIN}"

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y ca-certificates curl

/local/repository/scripts/install-github-keys.sh "${GITHUB_KEY_USER}" "${SSH_KEY_LOGIN}"

LAN_IFACE="$(ip -o -4 addr show | awk -v ip="${WORKER_IP}" '$0 ~ ip {print $2; exit}')"
if [ -z "${LAN_IFACE}" ]; then
    echo "Could not find interface with IP ${WORKER_IP}"
    ip -o -4 addr show
    exit 1
fi

until timeout 3 bash -c ":</dev/tcp/${CONTROL_IP}/6443" >/dev/null 2>&1; do
    echo "Waiting for K3s server API at ${CONTROL_IP}:6443..."
    sleep 5
done

if ! command -v k3s >/dev/null 2>&1; then
    curl -sfL https://get.k3s.io | \
        K3S_URL="https://${CONTROL_IP}:6443" \
        K3S_TOKEN="${K3S_TOKEN}" \
        INSTALL_K3S_EXEC="agent --node-ip ${WORKER_IP} --flannel-iface ${LAN_IFACE}" \
        sh -
fi

echo "Finished K3s agent setup at $(date -Is)"
