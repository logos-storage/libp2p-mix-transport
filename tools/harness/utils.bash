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
    if ((SECONDS - start >= timeout)); then
      return 1
    fi
    sleep 0.1
  done
}

array_remove() {
  local -n arr=$1
  local value=$2
  local new_arr=()
  for item in "${arr[@]}"; do
    if [[ "$item" != "$value" ]]; then
      new_arr+=("$item")
    fi
  done
  arr=("${new_arr[@]}")
}

timestamp() {
  local reply="$1" now secs frac stamp off
  now=$EPOCHREALTIME
  secs=${now%[.,]*}
  frac=${now#*[.,]}
  printf -v stamp '%(%Y-%m-%d %H:%M:%S)T' "$secs"
  printf -v off '%(%z)T' "$secs"
  printf -v "$reply" '%s.%s%s' "$stamp" "$frac" "$off"
}

label() {
  local label line ts
  label="${1:-unlabeled}"
  while IFS= read -r line || [[ -n $line ]]; do
    timestamp ts
    printf '[%s] [%s]: %s\n' "$label" "$ts" "$line"
  done
}

echo_log() {
  local message=$1 label=$2 logfile=$3
  echo "$message" | label "$label" | tee -a "$logfile" 2>&1
}

with_log() {
  local label="$1" logfile="$2" cmd=("${@:3}")
  "${cmd[@]}" 2>&1 | label "$label" | tee -a "$logfile" 2>&1
}
