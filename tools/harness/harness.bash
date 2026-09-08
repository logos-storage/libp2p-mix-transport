#!/usr/bin/env bash

LIB_SRC=${LIB_SRC:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}

# shellcheck source=utils.bash
source "${LIB_SRC}/utils.bash"
# shellcheck source=config.bash
source "${LIB_SRC}/config.bash"
# shellcheck source=emu.bash
source "${LIB_SRC}/emu.bash"
# shellcheck source=transport.bash
source "${LIB_SRC}/transport.bash"

if [[ $- =~ i ]]; then
  echoerr "Harness loaded in an interactive shell; caller shell options are unchanged."
else
  trap cleanup EXIT
fi

reload() {
  # shellcheck source=harness.bash
  source "${BASH_SOURCE[0]}"
}

cleanup() {
  tr_kill_nodes
  stop_sudo_keepalive
}

_sourced=1
