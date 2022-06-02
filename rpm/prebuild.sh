#!/bin/bash

set -exu  # Strict shell (w/o -o pipefail)

curl -LsSf https://www.tarantool.io/release/1.10/installer.sh | sudo bash
