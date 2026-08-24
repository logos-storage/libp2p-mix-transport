#!/usr/bin/env bash
set -euo pipefail

LIB_SRC=${LIB_SRC:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}

# shellcheck source=transport.bash
source "${LIB_SRC}/transport.bash"

if [[ $- =~ i ]]; then
  echoerr "You are sourcing this from an interactive shell. Setting set +e."
  set +e
else
  trap tr_kill_nodes EXIT
fi

reload() {
  # shellcheck source=harness.bash
  source "${BASH_SOURCE[0]}"
}