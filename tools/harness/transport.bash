#!/usr/bin/env bash
#
# Simple harness for running transport experiments.
set -euo pipefail

require_binary "$TR_NODE_BINARY"

MIX_PATH_LENGTH=3

TR_TRANSFER_LOGS="${TR_LOGS_FOLDER}/transfers"
TR_MEASUREMENTS="${TR_BASE}/${TR_RUN_ID}-transfer-times.csv"

# Extra measurement columns, appended to every row. Experiments use tr_field to
# record their own parameters (network size, concurrency, ...). Values are
# constant for the whole run, so set them before calling tr_init.
declare -gA TR_FIELDS
_tr_ext_values=""

tr_field() {
  TR_FIELDS["$1"]="$2"
}

_tr_node_ip() {
  local node_index=$1
  echo "127.0.0.$((1 + node_index))"
}

_tr_timeformat() {
  local reply=$1 source_node=$2 dest_node=$3 size=$4 ts
  timestamp ts
  printf -v "$reply" '%s,%s,%s,%s,%%E,%%U%s' \
    "$ts" "$size" "$source_node" "$dest_node" "$_tr_ext_values"
}

_tr_urls() {
  local -n urls_ref=$1
  local idx
  for idx in "${!_node_pids[@]}"; do
    urls_ref+=("http://$(_tr_node_ip "$idx"):$TR_API_PORT")
  done
}

_node_ready() {
  local node_index=$1
  tr_status "$node_index" | jq -e '.running == true' > /dev/null
}

tr_status() {
  local node_index=$1
  curl -fsS "http://$(_tr_node_ip "$node_index"):$TR_API_PORT/status" 2> /dev/null | jq .
}

tr_start_node() {
  local node_index=$1
  shift
  local args=("$@")
  args+=(
    "--api-port=$TR_API_PORT"
    "--listen-port=$TR_LISTEN_PORT"
    "--listen-ip=$(_tr_node_ip "${node_index}")"
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
  shift

  echoerr "Starting network with $node_count nodes"
  for ((i = 0; i < node_count; i++)); do
    echoerr "Starting node $i"
    tr_start_node "$i" "--max-connections=${max_connections}" "$@"
    await 10 _node_ready "$i"
    echoerr "Node $i is ready"
  done
}

tr_transfer_regular() {
  local source_node=$1 dest_node=$2 size=$3
  local label="${source_node} -> ${dest_node}"
  local logfile="${TR_TRANSFER_LOGS}/regular-${source_node}-${dest_node}-${RANDOM}.log"
  local src_ip dest_ip
  src_ip=$(_tr_node_ip "$source_node")
  dest_ip=$(_tr_node_ip "$dest_node")

  _tr_timeformat TIMEFORMAT "$source_node" "$dest_node" "$size"

  echo_log "Starting transfer." "$label" "$logfile"
  { time with_log "$label" "$logfile" \
      curl -fsS -X POST "http://${src_ip}:${TR_API_PORT}/request" \
        -H "Content-Type: application/json" \
        -d '{"address": "/ip4/'"${dest_ip}/tcp/$TR_LISTEN_PORT/"'", "size": '"$size"'}' ; } \
          2>> "${TR_MEASUREMENTS}"
}

tr_transfer_mix() {
  local source_node=$1 dest_node=$2 size=$3
  local src_ip
  src_ip=$(_tr_node_ip "$source_node")
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

  _tr_timeformat TIMEFORMAT "$source_node" "$dest_node" "$size"

  echo_log "Starting mix transfer ($source_node -> $dest_node)." "$label" "$logfile"
  { time with_log "$label" "$logfile" \
      curl --fail-with-body --no-progress-meter -X POST "http://${src_ip}:${TR_API_PORT}/request" \
        -H "Content-Type: application/json" \
        -d '{"peerId": "'"${dest_peer_id}"'", "size": '"${size}"'}' ; } 2>> "${TR_MEASUREMENTS}"
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

_tr_init_measurements() {
  local key columns=""
  _tr_ext_values=""
  for key in "${!TR_FIELDS[@]}"; do
    columns+=",${key}"
    _tr_ext_values+=",${TR_FIELDS[$key]}"
  done
  echo "timestamp,filesize,source,destination,wallclock,cpu${columns}" > "$TR_MEASUREMENTS"
}

tr_init() {
  init_folders || true
  mkdir -p "${TR_TRANSFER_LOGS}"
  unset _node_pids
  declare -gA _node_pids
  _tr_init_measurements
}
