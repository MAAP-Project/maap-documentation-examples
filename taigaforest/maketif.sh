#!/bin/bash
INPUT=$1
OUTPUT="$(basename -- $INPUT)"
OUTPATH="/projects/maap-users/alexdevseed/gee/"
echo rio cogeo create "/vsizip/${INPUT}" "${OUTPATH}${OUTPUT}.tif"
rio cogeo create "/vsizip/${INPUT}" "${OUTPATH}${OUTPUT}.tif"