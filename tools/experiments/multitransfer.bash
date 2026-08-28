#!/usr/bin/env bash
#
# Transfers multiple files concurrently over pairs of nodes in an
# arbitrarily sized network.
#
# multitransfer.bash <n_nodes> <n_transfers> <concurrent> <filesize_bytes> <use_mix>
set -euo pipefail
SCRIPT_DIR=${SCRIPT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}
# shellcheck source=../harness/harness.bash
source "${SCRIPT_DIR}/../harness/harness.bash"

# How many nodes should the network have?
N_NODES=${1:-40}
# How many transfers we want to run?
N_TRANSFERS=${2:-50}
# How many to run concurrently?
N_CONCURRENT=${3:-5}
# What file size?
N_BYTES=${4:-1048576}
# Use mix?
USE_MIX=${5:-true}

echoerr "Running multitransfer experiment with:"
echoerr "  Nodes: ${N_NODES}"
echoerr "  Transfers: ${N_TRANSFERS}"
echoerr "  Concurrent: ${N_CONCURRENT}"
echoerr "  File size: ${N_BYTES}"
echoerr "  Use mix: ${USE_MIX}"

_running=()
_remaining="${N_TRANSFERS}"

tr_destroy
tr_init
tr_start_network "${N_NODES}"

poll_transfers() {
  echoerr "Running transfer PIDs: ${_running[*]}"
  if [ ${#_running[@]} -gt 0 ]; then
    # Blocks till a transfer exits.
    wait -n "${_running[@]}"
    # We don't know from `wait`` what's the PID of the
    # process that quit, so check all of them and update
    # the running process array.
    for pid in "${_running[@]}"; do
      if ! kill -0 "$pid" 2>/dev/null; then
        array_remove _running "$pid"
        # bash treats arithmetic expressions as errors when
        # they evaluate to 0, so we use `|| true` or the script
        # crashes (!!!)
        ((--_remaining)) || true
        echoerr "-> ${_remaining}/${N_TRANSFERS} remaining"
      fi
    done
  else
    # Nothing to wait on, just sleeps a bit.
    sleep 0.5
  fi
}

if [ "$USE_MIX" = true ]; then
    _tr_transfer=tr_transfer_mix
else
    _tr_transfer=tr_transfer_regular
fi

while [ "${_remaining}" -gt 0 ]; do
    _to_launch=$((N_CONCURRENT - ${#_running[@]}))
    _max_launch=$((_remaining - ${#_running[@]}))
    _to_launch=$(( _to_launch < _max_launch ? _to_launch : _max_launch ))
    if [ $_to_launch -gt 0 ]; then
        for _ in $(seq 1 $_to_launch); do
            mapfile -t _pair < <(shuf -i "${MIX_PATH_LENGTH}"-$((N_NODES - 1)) -n 2 | sort -nr)
            echoerr "Transfer ${_pair[0]} -> ${_pair[1]} (${N_BYTES} bytes)"
            $_tr_transfer "${_pair[0]}" "${_pair[1]}" "${N_BYTES}" &
            _running+=($!)
            echo "LAUNCHED ${_pair[0]} -> ${_pair[1]}"
        done
    fi
    poll_transfers
done

echoerr "Done."