#!/usr/bin/env bash
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

SERVER_URL=${SERVER_URL:-nats://127.0.0.1:4222}
SUBJECT=${SUBJECT:-bench.wisp}
PUBLISHERS=${PUBLISHERS:-1}
SUBSCRIBERS=${SUBSCRIBERS:-1}
TOPICS=${TOPICS:-1}
MSGS=${MSGS:-100000}
SIZE=${SIZE:-64}
BUILD_RELEASE=${BUILD_RELEASE:-1}
START_SERVER=${START_SERVER:-1}

WISP_BIN="./target/release/wisp"
WISP_PID=""
SUB_PID=""
PUB_PID=""
TMP_DIR=""
SERVER_LOG=""
PUB_STATUS=0
SUB_STATUS=0

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Wisp Pub/Sub Benchmark${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "Parameters:"
echo "  Publishers: $PUBLISHERS"
echo "  Subscribers: $SUBSCRIBERS"
echo "  Topics: $TOPICS"
echo "  Messages: $MSGS"
echo "  Payload: $SIZE"
echo "  Server: $SERVER_URL"
echo "  Subject: $SUBJECT"
echo "  Start server: $START_SERVER"
echo ""

cleanup() {
    if [[ -n "${PUB_PID}" ]]; then
        kill "${PUB_PID}" 2>/dev/null || true
        wait "${PUB_PID}" 2>/dev/null || true
    fi
    if [[ -n "${SUB_PID}" ]]; then
        kill "${SUB_PID}" 2>/dev/null || true
        wait "${SUB_PID}" 2>/dev/null || true
    fi
    if [[ -n "${WISP_PID}" ]]; then
        kill "${WISP_PID}" 2>/dev/null || true
        wait "${WISP_PID}" 2>/dev/null || true
    fi
    if [[ -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
        rm -rf "${TMP_DIR}"
    fi
}

trap cleanup EXIT

parse_results() {
    local output="$1"
    echo "$output" | grep -E "(duration|rate|throughput|bandwidth|latency|p50|p90|p99|msgs/sec|msg/s)" -i || true
}

wait_for_server() {
    local attempts=50
    local host port

    host="$(echo "${SERVER_URL}" | sed -E 's#^nats://([^:/]+).*#\1#')"
    port="$(echo "${SERVER_URL}" | sed -E 's#^nats://[^:/]+:([0-9]+).*$#\1#')"

    while (( attempts > 0 )); do
        if nc -z "${host}" "${port}" 2>/dev/null; then
            return 0
        fi
        attempts=$((attempts - 1))
        sleep 0.1
    done

    return 1
}

server_host() {
    echo "${SERVER_URL}" | sed -E 's#^nats://([^:/]+).*#\1#'
}

server_port() {
    echo "${SERVER_URL}" | sed -E 's#^nats://[^:/]+:([0-9]+).*$#\1#'
}

if [[ "${BUILD_RELEASE}" == "1" && ! -x "${WISP_BIN}" ]]; then
    echo -e "${GREEN}=== Building Wisp release binary ===${NC}"
    cargo build --release
fi

if ! command -v nats >/dev/null 2>&1; then
    echo -e "${RED}Missing 'nats' CLI in PATH${NC}"
    exit 1
fi

if ! command -v nc >/dev/null 2>&1; then
    echo -e "${RED}Missing 'nc' (netcat) in PATH${NC}"
    exit 1
fi

if [[ ! -x "${WISP_BIN}" ]]; then
    echo -e "${RED}Missing Wisp binary at ${WISP_BIN}${NC}"
    exit 1
fi

TMP_DIR="$(mktemp -d)"
SUB_OUT="${TMP_DIR}/sub.out"
PUB_OUT="${TMP_DIR}/pub.out"
SERVER_LOG="${TMP_DIR}/server.out"

NATS_SUB_ARGS=(
    bench sub
    --server "${SERVER_URL}"
    --clients "${SUBSCRIBERS}"
    --msgs "${MSGS}"
    --size "${SIZE}"
    --no-progress
)

NATS_PUB_ARGS=(
    bench pub
    --server "${SERVER_URL}"
    --clients "${PUBLISHERS}"
    --msgs "${MSGS}"
    --size "${SIZE}"
    --no-progress
)

if (( TOPICS > 1 )); then
    NATS_SUB_ARGS+=(--multisubject)
    NATS_PUB_ARGS+=(--multisubject --multisubjectmax "${TOPICS}")
fi

NATS_SUB_ARGS+=("${SUBJECT}")
NATS_PUB_ARGS+=("${SUBJECT}")

if [[ "${START_SERVER}" == "1" ]]; then
    if nc -z "$(server_host)" "$(server_port)" 2>/dev/null; then
        echo -e "${RED}Refusing to start Wisp: ${SERVER_URL} is already in use${NC}"
        echo "Set START_SERVER=0 to benchmark against an already running server."
        exit 1
    fi

    echo -e "${GREEN}=== Starting Wisp Server ===${NC}"
    "${WISP_BIN}" >"${SERVER_LOG}" 2>&1 &
    WISP_PID=$!

    if ! wait_for_server; then
        echo -e "${RED}Failed to start Wisp server${NC}"
        if [[ -s "${SERVER_LOG}" ]]; then
            cat "${SERVER_LOG}"
        fi
        exit 1
    fi

    if ! kill -0 "${WISP_PID}" 2>/dev/null; then
        echo -e "${RED}Wisp exited before the benchmark began${NC}"
        if [[ -s "${SERVER_LOG}" ]]; then
            cat "${SERVER_LOG}"
        fi
        exit 1
    fi

    echo "Wisp server started (PID: $WISP_PID)"
else
    echo -e "${GREEN}=== Using Existing Server ===${NC}"
    if ! wait_for_server; then
        echo -e "${RED}No server is listening at ${SERVER_URL}${NC}"
        exit 1
    fi
fi

echo ""
echo -e "${GREEN}=== Starting Subscribers ===${NC}"
nats "${NATS_SUB_ARGS[@]}" >"${SUB_OUT}" 2>&1 &
SUB_PID=$!
sleep 1

echo -e "${GREEN}=== Running Publisher Benchmark ===${NC}"
nats "${NATS_PUB_ARGS[@]}" >"${PUB_OUT}" 2>&1 &
PUB_PID=$!
set +e
wait "${PUB_PID}"
PUB_STATUS=$?
PUB_PID=""
wait "${SUB_PID}"
SUB_STATUS=$?
SUB_PID=""
set -e

SUB_OUTPUT="$(cat "${SUB_OUT}")"
PUB_OUTPUT="$(cat "${PUB_OUT}")"

echo "--- Publisher ---"
echo "${PUB_OUTPUT}"
echo ""
echo "--- Subscriber ---"
echo "${SUB_OUTPUT}"

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Results Summary${NC}"
echo -e "${BLUE}========================================${NC}"
echo "Publisher:"
parse_results "${PUB_OUTPUT}"
echo ""
echo "Subscriber:"
parse_results "${SUB_OUTPUT}"

if (( PUB_STATUS != 0 || SUB_STATUS != 0 )); then
    echo ""
    echo -e "${RED}Benchmark command failed${NC}"
    echo "  Publisher exit: ${PUB_STATUS}"
    echo "  Subscriber exit: ${SUB_STATUS}"
    exit 1
fi

echo ""
echo -e "${GREEN}Benchmark complete${NC}"
