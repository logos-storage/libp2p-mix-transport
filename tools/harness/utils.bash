#!/usr/bin/env bash
set -euo pipefail

echoerr() { echo "$@" >&2; }

require_binary() {
  local binary=$1
  # Needs to be a file, and needs to be executable
  if [[ ! -f "$binary" || ! -x "$binary" ]]; then
    echoerr "Binary not found or not executable: $binary"
    exit 1
  fi
  echoerr "Found binary $binary"
}

await() {
  local timeout=$1
  shift
  local cmd=("$@")
  local start=$SECONDS

  while true; do
    echoerr "Awaiting for predicate to be true: ${cmd[*]}"
    if "${cmd[@]}"; then
      return 0
    fi
    if (( SECONDS - start >= timeout )); then
      return 1
    fi
    sleep 1
  done
}