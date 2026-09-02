#!/usr/bin/env bash
set -euo pipefail

LIB_SRC=${LIB_SRC:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}
# shellcheck source=./utils.bash
source "$LIB_SRC/utils.bash"

# Stuff you might want to change:
TR_RUN_ID=$(date +%Y%m%d%H%M%S-${RANDOM})
TR_BASE=$(realpath "${LIB_SRC}/../../runtime")
TR_RUNTIME_FOLDER="${TR_BASE}/${TR_RUN_ID}"
TR_NODE_BINARY="$(realpath "${LIB_SRC}/../node/node")"
TR_LOGS_FOLDER="${TR_RUNTIME_FOLDER}/logs"
TR_LOG_LEVEL="INFO"

# Use for debugging:
# TR_LOG_LEVEL="INFO;trace:mix_transport,transport"

export TR_BASE
export TR_RUNTIME_FOLDER
export TR_NODE_BINARY
export TR_LOGS_FOLDER
export TR_LOG_LEVEL

echoerr "Configured variables:"
env | grep "^TR_" --color=never
echoerr ""
echoerr "The base folder for this session is: $TR_RUNTIME_FOLDER"
echoerr ""

init_folders() {
  mkdir -p "$TR_RUNTIME_FOLDER"
  mkdir -p "$TR_LOGS_FOLDER"
}
