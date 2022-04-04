#!/usr/bin/env bash

if [[ ${CONDA_DEFAULT_ENV} != "gedi_subset" ]]; then
    source activate gedi_subset
fi

basedir=$(dirname "$(readlink -f "$0")")

set -euo pipefail
set -x

if test -d input; then
    # We are executing within a DPS job, so the AOI file was automatically
    # downloaded to the `input` directory.
    aoi=$(ls input/*)
    ${basedir}/subset.py --verbose --aoi "${aoi}" "$@"
else
    # This was invoked directly, so simply pass all arguments through to the
    # Python script.
    ${basedir}/subset.py "$@"
fi
