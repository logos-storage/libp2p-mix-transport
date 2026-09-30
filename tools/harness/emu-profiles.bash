#!/usr/bin/env bash

# TODO implement informed profiles (i.e. research some data). Note that
#   capped profiles apply network-wide, i.e., the bandwidth is split across
#   the whole network.
declare -gA EMU_PROFILES=(
  [wired]="delay 10ms 2ms distribution normal loss 0.1%"
  [wired-lossy]="delay 10ms 2ms distribution normal loss 1%"
  [wired-capped]="delay 10ms 2ms distribution normal loss 0.1% rate 100mbit"
  [wired-very-capped]="delay 10ms 2ms distribution normal loss 0.1% rate 1mbit"
  [hi-delay-jittery]="delay 100ms 30ms distribution normal loss 0.1%"
  [hi-delay-jittery-lossy]="delay 100ms 30ms distribution normal loss 1%"
)

export EMU_PROFILES

emu_profile() {
  local profile=$1
  echo "${EMU_PROFILES[$profile]}"
}