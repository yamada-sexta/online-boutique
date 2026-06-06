#!/usr/bin/env bash
set -euo pipefail

if [ "${EUID}" -ne 0 ]; then
    exec sudo "$0" "$@"
fi

RESULT_DIR="${1:?benchmark result directory is required}"
RESULTS_REPO="${2:?results repository is required}"
RESULTS_BRANCH="${3:-main}"

KEY_B64_SRC="/local/repository/keys/submission-key.b64"
KEY_DST="/root/.ssh/submission-key"
WORKTREE="/local/benchmark-results-repo"
RUN_ID="$(basename "${RESULT_DIR}")"
RUN_HOST="$(hostname -f 2>/dev/null || hostname)"

if [ ! -d "${RESULT_DIR}" ]; then
    echo "Benchmark result directory ${RESULT_DIR} does not exist."
    exit 1
fi

if [ ! -f "${KEY_B64_SRC}" ]; then
    echo "Deploy key ${KEY_B64_SRC} does not exist."
    exit 1
fi

install -d -m 700 /root/.ssh
base64 --decode "${KEY_B64_SRC}" > "${KEY_DST}"
chmod 600 "${KEY_DST}"
ssh-keyscan github.com >> /root/.ssh/known_hosts 2>/dev/null || true

export GIT_SSH_COMMAND="ssh -i ${KEY_DST} -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"

if [ -d "${WORKTREE}/.git" ]; then
    git -C "${WORKTREE}" fetch origin "${RESULTS_BRANCH}" || true
    if git -C "${WORKTREE}" show-ref --verify --quiet "refs/remotes/origin/${RESULTS_BRANCH}"; then
        git -C "${WORKTREE}" checkout -B "${RESULTS_BRANCH}" "origin/${RESULTS_BRANCH}"
    else
        git -C "${WORKTREE}" checkout -B "${RESULTS_BRANCH}"
    fi
else
    git clone "${RESULTS_REPO}" "${WORKTREE}" || {
        mkdir -p "${WORKTREE}"
        git -C "${WORKTREE}" init
        git -C "${WORKTREE}" remote add origin "${RESULTS_REPO}"
    }
    git -C "${WORKTREE}" fetch origin "${RESULTS_BRANCH}" || true
    if git -C "${WORKTREE}" show-ref --verify --quiet "refs/remotes/origin/${RESULTS_BRANCH}"; then
        git -C "${WORKTREE}" checkout -B "${RESULTS_BRANCH}" "origin/${RESULTS_BRANCH}"
    else
        git -C "${WORKTREE}" checkout -B "${RESULTS_BRANCH}"
    fi
fi

git -C "${WORKTREE}" config user.name "CloudLab Online Boutique Benchmark"
git -C "${WORKTREE}" config user.email "cloudlab-benchmark@localhost"

DEST_REL="runs/${RUN_HOST}/${RUN_ID}"
DEST="${WORKTREE}/${DEST_REL}"
if [ -e "${DEST}" ]; then
    DEST_REL="runs/${RUN_HOST}/${RUN_ID}-$(date -u +%s)"
    DEST="${WORKTREE}/${DEST_REL}"
fi

mkdir -p "${DEST}"
cp -a "${RESULT_DIR}/." "${DEST}/"

git -C "${WORKTREE}" add "${DEST_REL}"
if git -C "${WORKTREE}" diff --cached --quiet; then
    echo "No benchmark result changes to commit."
    exit 0
fi

git -C "${WORKTREE}" commit -m "Add benchmark result ${RUN_HOST} ${RUN_ID}"
git -C "${WORKTREE}" push -u origin "${RESULTS_BRANCH}"

echo "Pushed benchmark results to ${RESULTS_REPO}:${RESULTS_BRANCH}/${DEST_REL}"
