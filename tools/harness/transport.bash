#!/usr/bin/env bash
#
# Simple harness for running transport experiments.
set -euo pipefail

LIB_SRC=${LIB_SRC:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}

# shellcheck source=config.bash
source "$LIB_SRC/config.bash"
# shellcheck source=utils.bash
source "$LIB_SRC/utils.bash"

require_binary "$TR_NODE_BINARY"

MIX_PATH_LENGTH=3
_base_api_port=8000
_base_listen_port=9000

TR_TRANSFER_LOGS="${TR_LOGS_FOLDER}/transfers"

_tr_urls() {
  local -n urls_ref=$1
  local idx
  for idx in "${!_node_pids[@]}"; do
    urls_ref+=("http://127.0.0.1:$((_base_api_port + idx))")
  done
}

_node_ready() {
  local node_index=$1
  tr_status "$node_index" | jq -e '.running == true' >/dev/null
}

tr_status() {
  local node_index=$1
  local api_port=$((_base_api_port + node_index))
  curl -fsS "http://127.0.0.1:$api_port/status" 2>/dev/null | jq .
}

tr_start_node() {
  local node_index=$1
  local api_port=$((_base_api_port + node_index))
  local listen_port=$((_base_listen_port + node_index))
  shift
  local args=("$@")
  args+=(
    "--api-port=$api_port"
    "--listen-port=$listen_port"
    "--log-level=$TR_LOG_LEVEL"
  )

  local tr_cmd=(
    "$TR_NODE_BINARY"
    "${args[@]}"
  )

  local urls=()
  _tr_urls urls
  if [[ ${#urls[@]} -gt 0 ]]; then
    tr_cmd+=("${urls[@]}")
  fi

  "${tr_cmd[@]}" &>"${TR_LOGS_FOLDER}/node-${node_index}.log" &
  _node_pids[$node_index]=$!
}

tr_start_network() {
  local node_count=$1
  local max_connections=$((node_count * 2))
  echoerr "Starting network with $node_count nodes"
  for ((i = 0; i < node_count; i++)); do
    echoerr "Starting node $i"
    tr_start_node "$i" "--max-connections=${max_connections}"
    await 10 _node_ready "$i"
    echoerr "Node $i is ready"
  done
}

tr_transfer_regular() {
  local source_node=$1 dest_node=$2 size=$3
  local source_api_port=$((_base_api_port + source_node))
  local dest_listen_port=$((_base_listen_port + dest_node))
  local label="${source_node} -> ${dest_node}"
  local logfile="${TR_TRANSFER_LOGS}/regular-${source_node}-${dest_node}-${RANDOM}.log"

  echo_log "Starting transfer." "$label" "$logfile"
  with_log "$label" "$logfile" \
    curl -fsS -X POST "http://127.0.0.1:$source_api_port/request" \
    -H "Content-Type: application/json" \
    -d '{"address": "/ip4/127.0.0.1/tcp/'"$dest_listen_port/"'", "size": '"$size"'}'
}

tr_transfer_mix() {
  local source_node=$1 dest_node=$2 size=$3
  local source_api_port=$((_base_api_port + source_node))
  local dest_peer_id
  dest_peer_id=$(tr_peer_id "$dest_node")
  local label="${source_node} -> ${dest_node}"
  local logfile="${TR_TRANSFER_LOGS}/mix-${source_node}-${dest_node}-${RANDOM}.log"

  # This ensures that the node knows enough mix nodes to build a path, assuming that
  # they're not launching non-contiguous node IDs. To get a more accurate predicate
  # we'd need to know how many mix nodes the node knows, which for now I don't see
  # as needed.
  if [[ $source_node -lt $MIX_PATH_LENGTH ]]; then
    echoerr "Source index must be larger or equal to $MIX_PATH_LENGTH (was $source_node)"
    return 1
  fi

  echo_log "Starting mix transfer ($source_node -> $dest_node)." "$label" "$logfile"
  with_log "$label" "$logfile" \
    curl --fail-with-body --no-progress-meter -X POST "http://127.0.0.1:$source_api_port/request" \
    -H "Content-Type: application/json" \
    -d '{"peerId": "'"$dest_peer_id"'", "size": '"$size"'}'
}

tr_kill_node() {
  local node_index=$1
  local pid=${_node_pids[$node_index]}
  kill "$pid" || true
}

tr_kill_nodes() {
  for idx in "${!_node_pids[@]}"; do
    tr_kill_node "$idx"
  done
}

tr_peer_id() {
  tr_status "$1" | jq -r '.mixInfo.peerId'
}

tr_list_nodes() {
  for idx in "${!_node_pids[@]}"; do
    local pid=${_node_pids[$idx]}
    local status
    if kill -0 "$pid" 2>/dev/null; then
      status="RUNNING"
    else
      status="DEAD"
    fi
    echo " - index: $idx, PID: $pid, Status: $status, PeerId: $(tr_peer_id "$idx")"
  done
}

tr_init() {
  init_folders || true
  mkdir -p "${TR_TRANSFER_LOGS}"
  unset _node_pids
  declare -gA _node_pids
}

tr_destroy() {
  tr_kill_nodes
  clean_folders
}
