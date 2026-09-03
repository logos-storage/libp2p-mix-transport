#!/usr/bin/env bash
SCRIPT_DIR=${SCRIPT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}

# [network size] [total transfers] [concurrent transfers] [file size] [use mix] [strategy]
PARAMS=(
  "100 500 20 1000000 true default"
  "100 500 20 1000000 true exponential"
  "100 500 80 1000000 true default"
  "100 500 80 1000000 true exponential"
  "200 500 20 1000000 true default"
  "200 500 20 1000000 true exponential"
  "200 500 40 1000000 true default"
  "200 500 40 1000000 true exponential"
  "200 500 160 1000000 true default"
  "200 500 160 1000000 true exponential"
)

for param in "${PARAMS[@]}"; do
  echo "Running: $param"
  bash -c "bash ${SCRIPT_DIR}/multitransfer.bash $param"
done
