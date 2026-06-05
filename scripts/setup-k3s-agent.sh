#!/usr/bin/env bash
set -euo pipefail

CONTROL_IP="${1:?control IP is required}"
WORKER_IP="${2:?worker IP is required}"
K3S_TOKEN="${3:?K3s token is required}"

LOG_DIR="/local/logs"
mkdir -p "${LOG_DIR}"
exec > >(tee -a "${LOG_DIR}/k3s-agent-setup.log") 2>&1

echo "Starting K3s agent setup at $(date -Is)"
echo "Control IP: ${CONTROL_IP}"
echo "Worker IP: ${WORKER_IP}"

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y ca-certificates curl

LAN_IFACE="$(ip -o -4 addr show | awk -v ip="${WORKER_IP}" '$0 ~ ip {print $2; exit}')"
if [ -z "${LAN_IFACE}" ]; then
    echo "Could not find interface with IP ${WORKER_IP}"
    ip -o -4 addr show
    exit 1
fi

until curl -kfsS "https://${CONTROL_IP}:6443/readyz" >/dev/null 2>&1; do
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
