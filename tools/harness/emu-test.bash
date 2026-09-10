#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

#shellcheck source=harness.bash
source "$SCRIPT_DIR/harness.bash"

if ! command -v iperf3 &> /dev/null; then
  echoerr "this requires iperf3"
  exit 1
fi

if ! command -v ss &> /dev/null; then
  echoerr "this requires ss"
  exit 1
fi

iperf_ready() {
  ss -ltnH "sport = :$1"
}

emu_set_params rate 0.5mbit
emu_enter

iperf3 -s -1 -B 127.0.0.2 -p "${TR_LISTEN_PORT}" &
iperf3 -s -1 -B 127.0.0.2 -p "${TR_API_PORT}" &
await 5 iperf_ready "${TR_LISTEN_PORT}"
await 5 iperf_ready "${TR_API_PORT}"

echoerr "netem port (should be slow)"
iperf3 -c 127.0.0.2 -p "${TR_LISTEN_PORT}" -t 10

echoerr "API port (should be fast)"
iperf3 -c 127.0.0.2 -p "${TR_API_PORT}" -t 10
