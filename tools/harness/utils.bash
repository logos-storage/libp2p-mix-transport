#!/usr/bin/env bash
set -euo pipefail

echoerr() { echo "$@" >&2; }

require_binary() {
  local binary=$1
  if ! command -v "$binary" &> /dev/null; then
    echoerr "Binary not found: $binary"
    exit 1
  fi
  echoerr "Found binary $binary"
}
