#!/usr/bin/env bash

# TODO implement informed profiles (i.e. research some data)
declare -gA EMU_PROFILES=(
  [wired]="delay 10ms 2ms distribution normal loss 0.1% rate 100mbit"
  [wired-slow]="delay 10ms 2ms distribution normal loss 0.1% rate 1mbit"
  [wired-lossy]="delay 10ms 2ms distribution normal loss 1% rate 100mbit"
  [hi-delay-jittery]="delay 100ms 30ms distribution normal loss 0.1% rate 100mbit"
  [hi-delay-jittery-lossy]="delay 100ms 30ms distribution normal loss 1% rate 100mbit"
)

export EMU_PROFILES

emu_profile() {
  local profile=$1
  echo "${EMU_PROFILES[$profile]}"
}