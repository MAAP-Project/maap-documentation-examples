#!/usr/bin/env bash

set -xeuo pipefail

conda list

basedir=$(dirname "$(readlink -f "$0")")

if conda env list | grep -q gedi_subset; then
    env=gedi_subset
else
    env=base
    # List everything to help discover why things
    # fail to run in the context of a DPS job.
    conda list
fi

subset_py="conda run -n ${env} ${basedir}/subset.py"

if test -d input; then
    # We are executing within a DPS job, so the AOI file was automatically
    # downloaded to the `input` directory.
    aoi=$(ls input/*)
    ${subset_py} --verbose --aoi "${aoi}" --limit "${1:-10000}"
else
    # This was invoked directly, so simply pass all arguments through to the
    # Python script.
    ${subset_py} --verbose "$@"
fi
