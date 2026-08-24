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

_base_api_port=8000
_base_listen_port=9000

_tr_urls() {
  local -n urls_ref=$1
  local idx
  for idx in "${!_node_pids[@]}"; do
    urls_ref+=("http://127.0.0.1:$(( _base_api_port + idx ))")
  done
}

tr_start_node() {
  local node_index=$1
  local api_port=$(( _base_api_port + node_index ))
  local listen_port=$(( _base_listen_port + node_index ))
  local args=("--api-port=$api_port" "--listen-port=$listen_port")

  local tr_cmd=(
    "$TR_NODE_BINARY"
    "${args[@]}"
  )

  local urls=()
  _tr_urls urls
  if [[ ${#urls[@]} -gt 0 ]]; then
    tr_cmd+=("${urls[@]}")
  fi

  echoerr "Command: ${tr_cmd[*]}"

  "${tr_cmd[@]}" &> "${TR_LOGS_FOLDER}/node-${node_index}.log" &
  _node_pids[$node_index]=$!
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

tr_list_nodes() {
  for pid in "${_node_pids[@]}"; do
    local status
    if kill -0 "$pid" 2>/dev/null; then
      status="RUNNING"
    else
      status="DEAD"
    fi
    echo " - PID: $pid, Status: $status"
  done
}

tr_init() {
  init_folders || true
  unset _node_pids
  declare -gA _node_pids
}

tr_destroy() {
  tr_kill_nodes
  clean_folders
}

