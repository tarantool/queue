#!/bin/bash

set -exu  # Strict shell (w/o -o pipefail)

# At the time of adding the changes, tarantool 1.10 is absent in the
# repositories Ubuntu impish and jammy.
if [[ $DIST == "impish" ]] || [[ $DIST == "jammy" ]]; then
  curl -LsSf https://www.tarantool.io/release/2/installer.sh | sudo bash
else
  curl -LsSf https://www.tarantool.io/release/1.10/installer.sh | sudo bash
fi
