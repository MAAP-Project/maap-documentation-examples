#!/usr/bin/env bash

set -euo pipefail

basedir=$(dirname "$(readlink -f "$0")")

cmd="${basedir}/subset.py"

echo "${cmd}"
eval "${cmd}"
