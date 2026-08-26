#!/usr/bin/env bash
set -euo pipefail

LIB_SRC=${LIB_SRC:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}
# shellcheck source=./utils.bash
source "$LIB_SRC/utils.bash"

TR_RUNTIME_FOLDER=$(realpath "${LIB_SRC}/../runtime")
export TR_RUNTIME_FOLDER
TR_NODE_BINARY="$(realpath "${LIB_SRC}/../node/node")"
export TR_NODE_BINARY
export TR_LOGS_FOLDER="${TR_RUNTIME_FOLDER}/logs"

echoerr "Configured variables:"
env | grep "^TR_"
echoerr ""

init_folders() {
  mkdir -p "$TR_RUNTIME_FOLDER"
  mkdir -p "$TR_LOGS_FOLDER"
}

clean_folders() {
  rm -rf "$TR_RUNTIME_FOLDER"
  rm -rf "$TR_LOGS_FOLDER"
}
