#!/usr/bin/env bash
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

NATS_SERVER_URL=${NATS_SERVER_URL:-nats://127.0.0.1:4222}
WISP_SERVER_URL=${WISP_SERVER_URL:-nats://127.0.0.1:4223}
WISP_PORT=${WISP_PORT:-4223}
SUBJECT=${SUBJECT:-bench.wisp}
PUBLISHERS=${PUBLISHERS:-1}
SUBSCRIBERS=${SUBSCRIBERS:-1}
TOPICS=${TOPICS:-1}
MSGS=${MSGS:-100000}
SIZE=${SIZE:-64}
BUILD_RELEASE=${BUILD_RELEASE:-1}
START_NATS_SERVER=${START_NATS_SERVER:-auto}

TMP_DIR=""
NATS_LOG=""
NATS_PID=""
NATS_STATUS=0
WISP_STATUS=0

cleanup() {
    if [[ -n "${NATS_PID}" ]]; then
        kill "${NATS_PID}" 2>/dev/null || true
        wait "${NATS_PID}" 2>/dev/null || true
    fi
    if [[ -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
        rm -rf "${TMP_DIR}"
    fi
}

trap cleanup EXIT

server_host() {
    echo "$1" | sed -E 's#^nats://([^:/]+).*#\1#'
}

server_port() {
    echo "$1" | sed -E 's#^nats://[^:/]+:([0-9]+).*$#\1#'
}

wait_for_server() {
    local url="$1"
    local attempts=50
    local host port

    host="$(server_host "${url}")"
    port="$(server_port "${url}")"

    while (( attempts > 0 )); do
        if nc -z "${host}" "${port}" 2>/dev/null; then
            return 0
        fi
        attempts=$((attempts - 1))
        sleep 0.1
    done

    return 1
}

summary_line() {
    grep -E "NATS Core NATS (publisher|subscriber) stats:" | awk '!seen[$0]++'
}

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Wisp vs NATS Baseline${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "Parameters:"
echo "  Publishers: $PUBLISHERS"
echo "  Subscribers: $SUBSCRIBERS"
echo "  Topics: $TOPICS"
echo "  Messages: $MSGS"
echo "  Payload: $SIZE"
echo "  Subject: $SUBJECT"
echo "  NATS URL: $NATS_SERVER_URL"
echo "  Wisp URL: $WISP_SERVER_URL"
echo ""

if ! command -v nc >/dev/null 2>&1; then
    echo -e "${RED}Missing 'nc' (netcat) in PATH${NC}"
    exit 1
fi

if ! command -v nats-server >/dev/null 2>&1; then
    echo -e "${RED}Missing 'nats-server' in PATH${NC}"
    exit 1
fi

TMP_DIR="$(mktemp -d)"
NATS_LOG="${TMP_DIR}/nats-server.out"

if [[ "${START_NATS_SERVER}" == "1" || "${START_NATS_SERVER}" == "auto" ]]; then
    if nc -z "$(server_host "${NATS_SERVER_URL}")" "$(server_port "${NATS_SERVER_URL}")" 2>/dev/null; then
        if [[ "${START_NATS_SERVER}" == "1" ]]; then
            echo -e "${RED}Refusing to start nats-server: ${NATS_SERVER_URL} is already in use${NC}"
            exit 1
        fi
    else
        echo -e "${GREEN}=== Starting nats-server ===${NC}"
        nats-server -a "$(server_host "${NATS_SERVER_URL}")" -p "$(server_port "${NATS_SERVER_URL}")" \
            >"${NATS_LOG}" 2>&1 &
        NATS_PID=$!

        if ! wait_for_server "${NATS_SERVER_URL}"; then
            echo -e "${RED}Failed to start nats-server${NC}"
            if [[ -s "${NATS_LOG}" ]]; then
                cat "${NATS_LOG}"
            fi
            exit 1
        fi
    fi
fi

echo -e "${GREEN}=== Benchmarking NATS ===${NC}"
set +e
NATS_OUTPUT="$(SERVER_URL="${NATS_SERVER_URL}" \
    SUBJECT="${SUBJECT}" \
    PUBLISHERS="${PUBLISHERS}" \
    SUBSCRIBERS="${SUBSCRIBERS}" \
    TOPICS="${TOPICS}" \
    MSGS="${MSGS}" \
    SIZE="${SIZE}" \
    BUILD_RELEASE=0 \
    START_SERVER=0 \
    bash ./benchmark.sh)"
NATS_STATUS=$?

echo -e "${GREEN}=== Benchmarking Wisp ===${NC}"
WISP_OUTPUT="$(SERVER_URL="${WISP_SERVER_URL}" \
    SUBJECT="${SUBJECT}" \
    PUBLISHERS="${PUBLISHERS}" \
    SUBSCRIBERS="${SUBSCRIBERS}" \
    TOPICS="${TOPICS}" \
    MSGS="${MSGS}" \
    SIZE="${SIZE}" \
    BUILD_RELEASE="${BUILD_RELEASE}" \
    START_SERVER=1 \
    WISP_PORT="${WISP_PORT}" \
    bash ./benchmark.sh)"
WISP_STATUS=$?
set -e

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Baseline Summary${NC}"
echo -e "${BLUE}========================================${NC}"
echo "NATS:"
echo "${NATS_OUTPUT}" | summary_line
echo ""
echo "Wisp:"
echo "${WISP_OUTPUT}" | summary_line

if (( NATS_STATUS != 0 || WISP_STATUS != 0 )); then
    echo ""
    echo -e "${RED}Comparison run failed${NC}"
    echo "  NATS exit: ${NATS_STATUS}"
    echo "  Wisp exit: ${WISP_STATUS}"
    exit 1
fi
