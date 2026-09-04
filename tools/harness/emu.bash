#!/usr/bin/env bash
set -euo pipefail

LIB_SRC=${LIB_SRC:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}

_netem_params=()

MIX_NS="mixtests"
ARGV=("$@")

_emu_tc() {
  sudo ip netns exec "${MIX_NS}" tc "$@"
}

emu_set_params() {
  _netem_params=("$@")
}

emu_is_inside() {
  [[ -v _emu_inside ]]
}

emu_setup() {
  echoerr "Setting up emulation environment"
  sudo ip netns add "${MIX_NS}"

  # Brings up the namespace's loopback interface (by default it's down),
  # and sets the MTU to 1500 (by default it's 65536):
  sudo ip -n "${MIX_NS}" link set dev lo up mtu 1500

  # We want to apply netem to the libp2p connections, but we don't
  # want to apply it to API calls as that would make our lives miserable :-)
  # We therefore create a two-class qdisc - one class gets netem, the other
  # doesn't. We use PRIO (stateful qdisc) to implement that for simplicity.

  # Sets PRIO as root qdisc with 2 classes (bands 0 and 1). Band 0 will be
  # unshaped, Band 1 will get netem. In the absence of filters, PRIO will classify
  # packets based on the priomap, which maps the value of the IP TOS field to a band.
  # We set a priomap that maps everything to band 1 (netem).
  _emu_tc qdisc add dev lo root handle 1:0 prio bands 2 \
    priomap 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1

  # As configured so far, every packet goes to netem. Here we add filters which tag packets
  # coming from and to the API ports. We need to do both because clients (e.g. curl) may
  # use random source ports, but the API port will always be in either source or destination.
  _emu_tc filter add dev lo parent 1:0 protocol ip u32 match ip sport "${TR_API_PORT}" 0xffff flowid 1:1
  _emu_tc filter add dev lo parent 1:0 protocol ip u32 match ip dport "${TR_API_PORT}" 0xffff flowid 1:1

  # Finally, we set the qdiscs in each class:
  # Band 0 (for the APIs) gets a simple pfifo.
  _emu_tc qdisc add dev lo parent 1:1 handle 10: pfifo

  # Band 1 (for libp2p) gets netem.
  _emu_tc qdisc add dev lo parent 1:2 handle 20: netem "${_netem_params[@]}"
}

## Enters the emulation environment. Can be used both from an interactive shell or as a command.
## When run as part of an experiment, should be the first command to be called.
# shellcheck disable=SC2120
emu_enter() {
  if emu_is_inside; then
    echoerr "Inside $MIX_NS namespace."
    return 0
  fi

  if [[ ${#_netem_params[@]} -eq 0 ]]; then
    echoerr "No netem parameters set. Please call emu_set_params"
    echoerr "before calling emu_enter."
    return 1
  fi

  # TODO implement this rootless
  echoerr "Entering emulation environment."
  echoerr "WARNING: this requires 'sudo'"

  if ! emu_setup; then
    emu_teardown || true
    return 1
  fi

  local -a cmd=("$@")
  local result=0 reexec=0
  # No command.
  if [[ ${#cmd[@]} -eq 0 ]]; then
    # If we're in an interactive shell, we'll enter interactive mode.
    if [[ $- =~ i ]]; then
      echoerr "Entering in interactive mode, you will need to re-source the harness:"
      echoerr "  source ${LIB_SRC}/harness.bash"
      cmd=(bash -i)
    # Otherwise, we'll relaunch the current script within the environment.
    else
      cmd=(bash "$0" "${ARGV[@]}")
      reexec=1
    fi
  fi

  sudo ip netns exec "${MIX_NS}" sudo -u "$(id -un)" -H \
    env _emu_inside=1 "${cmd[@]}" || result=$?

  emu_teardown || true
  # Need to exit or the script will execute twice.
  if [[ $reexec -eq 1 ]]; then
    exit "$result"
  fi
  return "$result"
}

emu_teardown() {
  echoerr "Tearing down emulation environment"
  sudo ip netns del "${MIX_NS}" || true
}
