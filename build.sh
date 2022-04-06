#!/usr/bin/env bash

basedir=$(dirname "$(readlink -f "$0")")

set -euo pipefail
set -x

mamba env create -f "${basedir}/environment.yml"
