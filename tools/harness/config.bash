#!/usr/bin/env bash

LIB_SRC=${LIB_SRC:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}

echoerr "Overrides are:"
set | grep "^TR_" --color=never || true

# Stuff you might want to change:
TR_NODE_BINARY=${TR_NODE_BINARY:-$(realpath "${LIB_SRC}/../node/node")}
TR_BASE=${TR_BASE:-$(realpath "${LIB_SRC}/../../experiment-output")}
TR_LOG_LEVEL=${TR_LOG_LEVEL:-"INFO"}
# Use for debugging:
#   TR_LOG_LEVEL="INFO;trace:mix-transport"
#   TR_LOG_LEVEL="INFO;trace:mix-transport-messages"
TR_API_PORT=${TR_API_PORT:-8000}
TR_LISTEN_PORT=${TR_LISTEN_PORT:-9000}

# Stuff you probably want to leave alone.
TR_RUN_ID=${TR_RUN_ID:-$(date +%Y%m%d%H%M%S-${RANDOM})}
TR_RUNTIME_FOLDER="${TR_BASE}/${TR_RUN_ID}"
TR_LOGS_FOLDER="${TR_RUNTIME_FOLDER}/logs"

echoerr "Configured variables:"
TR_ENV=()
while IFS= read -r line; do
  name=$(echo "$line" | grep -o '^[^=]*')
  if [[ "$name" != "TR_ENV" ]]; then
    export "${name?}"
    TR_ENV+=("${name}=${!name}")
    echoerr "  ${name}=${!name}"
  fi
done < <(set | grep '^TR_')

init_folders() {
  mkdir -p "$TR_RUNTIME_FOLDER"
  mkdir -p "$TR_LOGS_FOLDER"
}
