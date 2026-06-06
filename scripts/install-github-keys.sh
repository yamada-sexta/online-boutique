#!/usr/bin/env bash
set -euo pipefail

if [ "${EUID}" -ne 0 ]; then
    exec sudo "$0" "$@"
fi

GITHUB_USER="${1:?GitHub user is required}"
LOGIN="${2:?CloudLab login is required}"
USER_HOME="/users/${LOGIN}"
SSH_DIR="${USER_HOME}/.ssh"
AUTHORIZED_KEYS="${SSH_DIR}/authorized_keys"
TMP_KEYS="$(mktemp)"

cleanup() {
    rm -f "${TMP_KEYS}"
}
trap cleanup EXIT

echo "Installing GitHub public keys for ${GITHUB_USER} into ${AUTHORIZED_KEYS}"

if [ ! -d "${USER_HOME}" ]; then
    echo "User home ${USER_HOME} does not exist; skipping GitHub key install."
    exit 0
fi

curl -fsSL "https://github.com/${GITHUB_USER}.keys" -o "${TMP_KEYS}"
if [ ! -s "${TMP_KEYS}" ]; then
    echo "No public keys were returned for GitHub user ${GITHUB_USER}."
    exit 1
fi

mkdir -p "${SSH_DIR}"
touch "${AUTHORIZED_KEYS}"
chmod 700 "${SSH_DIR}"
chmod 600 "${AUTHORIZED_KEYS}"
chown -R "${LOGIN}:${LOGIN}" "${SSH_DIR}" 2>/dev/null || chown -R "${LOGIN}" "${SSH_DIR}" || true

while IFS= read -r key; do
    if [ -z "${key}" ]; then
        continue
    fi
    if ! grep -qxF "${key}" "${AUTHORIZED_KEYS}"; then
        echo "${key}" >> "${AUTHORIZED_KEYS}"
    fi
done < "${TMP_KEYS}"

chmod 600 "${AUTHORIZED_KEYS}"
chown "${LOGIN}:${LOGIN}" "${AUTHORIZED_KEYS}" 2>/dev/null || chown "${LOGIN}" "${AUTHORIZED_KEYS}" || true

echo "Finished installing GitHub public keys for ${GITHUB_USER}."
