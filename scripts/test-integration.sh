#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"

container_runtime="${CONTAINER_RUNTIME:-docker}"
docling_image="${DOCLING_SERVE_IMAGE:-quay.io/docling-project/docling-serve-cpu:v1.28.0}"
docling_port="${DOCLING_SERVE_PORT:-5001}"
container_name="${DOCLING_SERVE_CONTAINER_NAME:-docling-spark-integration-$$}"
manage_container=0

if [[ -z "${DOCLING_SERVE_URL:-}" ]]; then
    DOCLING_SERVE_URL="http://127.0.0.1:${docling_port}"
    export DOCLING_SERVE_URL
    manage_container=1
fi

cleanup() {
    local status=$?
    trap - EXIT INT TERM
    if [[ "$manage_container" -eq 1 ]]; then
        if [[ "$status" -ne 0 ]]; then
            "$container_runtime" logs --tail 200 "$container_name" || true
        fi
        "$container_runtime" stop "$container_name" >/dev/null 2>&1 || true
    fi
    exit "$status"
}
trap cleanup EXIT INT TERM

if [[ "$manage_container" -eq 1 ]]; then
    echo "Starting Docling Serve from ${docling_image}"
    "$container_runtime" run \
        --rm \
        --detach \
        --name "$container_name" \
        --publish "127.0.0.1:${docling_port}:5001" \
        "$docling_image" >/dev/null
fi

echo "Waiting for Docling Serve at ${DOCLING_SERVE_URL}"
ready=0
for _ in $(seq 1 120); do
    if curl --fail --silent "${DOCLING_SERVE_URL}/health" >/dev/null; then
        ready=1
        break
    fi
    sleep 1
done

if [[ "$ready" -ne 1 ]]; then
    echo "Docling Serve did not become healthy within 120 seconds" >&2
    exit 1
fi

echo "Running the Spark integration test"
DOCLING_SPARK_RUN_INTEGRATION=1 \
    uv run --frozen pytest -q -m integration tests/test_integration.py
