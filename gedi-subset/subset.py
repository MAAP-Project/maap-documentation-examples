#!/usr/bin/env python
import logging
import multiprocessing
import operator
import os
import os.path
import warnings
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional, Tuple, TypeVar

import geopandas as gpd
import typer
from fp import K, filter, map
from gedi_utils import (
    append_gdf_file,
    chext,
    df_assign,
    gdf_to_file,
    granule_intersects,
    subset_h5,
)
from maap.maap import MAAP, Collection, Granule
from returns.curry import curry, partial
from returns.functions import identity, raise_exception, tap
from returns.io import IO, IOFailure, IOResult, IOResultE, IOSuccess, impure_safe
from returns.iterables import Fold
from returns.maybe import Maybe, Nothing, Some
from returns.methods import unwrap_or_failure
from returns.pipeline import flow, is_successful, managed, pipe
from returns.pointfree import bind_ioresult, bind_result, lash, map_
from returns.result import Failure, Success, safe
from returns.unsafe import unsafe_perform_io

logging.basicConfig(
    format="%(asctime)s [%(processName)s:%(name)s] [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def set_log_level(level: int):
    global logger
    logger.setLevel(level)


@curry
def find_collection(
    maap: MAAP,
    cmr_host: str,
    params: Mapping[str, str],
) -> IOResultE[Collection]:
    return flow(
        impure_safe(maap.searchCollection)(cmr_host=cmr_host, **dict(params, limit=1)),
        bind_result(safe(operator.itemgetter(0))),
        lash(K(IOFailure(ValueError(f"No collection found at {cmr_host}: {params}")))),
    )


def subset_granules(
    aoi_gdf: gpd.GeoDataFrame,
    output_directory: Path,
    dest: Path,
    overwrite: bool,
    log_level: int,
    granules: Iterable[Granule],
) -> IOResultE[Tuple[str, ...]]:
    # Must declare nested function as global to allow multiprocessing to pickle it.
    global process_granule

    @impure_safe
    def process_granule(granule: Granule) -> str:
        filter_cols = [
            "agbd",
            "agbd_se",
            "l4_quality_flag",
            "sensitivity",
            "lat_lowestmode",
            "lon_lowestmode",
        ]

        logger.debug(f"Downloading granule {granule['Granule']['GranuleUR']}")
        inpath = granule.getData(str(output_directory))
        outpath = chext(".fgb", inpath)
        logger.debug(f"Subsetting {inpath} to {outpath}")

        if overwrite or not os.path.exists(outpath):
            flow(
                subset_h5(inpath, aoi_gdf, filter_cols),
                df_assign("filename", inpath),
                gdf_to_file(outpath, dict(index=False, driver="FlatGeobuf")),
                lash(raise_exception),
            )

        return outpath

    # Use half the CPUs available to this process (or at least 1 CPU)
    processes = max(1, len(os.sched_getaffinity(0)) // 2)
    # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.pool.Pool.imap
    chunksize = 10

    logger.info(f"Subsetting on {processes} processes (chunksize={chunksize})")

    with multiprocessing.Pool(processes, set_log_level, (log_level,)) as pool:
        return flow(
            pool.imap_unordered(process_granule, granules, chunksize),
            filter(lambda r: map_(impure_safe(os.path.exists))(r).value_or(True)),
            map(tap(map_(pipe(f"Appending {{}} to {dest}".format, logger.debug)))),
            map(tap(bind_ioresult(append_gdf_file(dest)))),
            partial(Fold.collect, acc=IOSuccess(())),
        )


def main(
    aoi: Path = typer.Option(
        ...,
        help="Area of Interest (path to GeoJSON file)",
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
    ),
    doi=typer.Option(
        # "10.3334/ORNLDAAC/1986",  # GEDI L4A DOI, v2
        "10.3334/ORNLDAAC/2056",  # GEDI L4A DOI, v2.1
        help="Digital Object Identifier of collection to subset (https://www.doi.org/)",
    ),
    cmr_host=typer.Option(
        # "cmr.maap-project.org",  # Doesn't properly handle HTTPS fallback from S3
        "cmr.earthdata.nasa.gov",
        help="CMR hostname",
    ),
    limit: int = typer.Option(
        10_000,
        help="Maximum number of granules to subset",
    ),
    output_directory: Path = typer.Option(
        f"{os.path.join(os.path.abspath(os.path.curdir), 'output')}",
        "-d",
        "--output-directory",
        help="Output directory for generated subset file",
        exists=False,
        file_okay=False,
        dir_okay=True,
        writable=True,
        readable=True,
        resolve_path=True,
    ),
    overwrite: bool = typer.Option(
        False,
        help="Overwrite individual subset files",
    ),
    verbose: bool = typer.Option(False, help="Provide verbose output"),
) -> None:
    log_level = logging.DEBUG if verbose else logging.INFO
    set_log_level(log_level)

    os.makedirs(output_directory, exist_ok=True)
    dest = output_directory / "gedi_subset.gpkg"

    # Remove existing combined subset file, primarily to support
    # testing.  When running in the context of a DPS job, there
    # should be no existing file since every job uses a unique
    # output directory.
    impure_safe(os.remove)(dest)

    maap = MAAP("api.ops.maap-project.org")
    result = IO.do(
        Success(subsets)
        if subsets
        else Failure(ValueError("No granules intersect AOI"))
        for aoi_gdf in impure_safe(gpd.read_file)(aoi)
        for collection in find_collection(maap)(cmr_host)({"doi": doi})
        for granules in impure_safe(maap.searchGranule)(
            cmr_host=cmr_host,
            collection_concept_id=collection["concept-id"],
            bounding_box=",".join(map(str)(aoi_gdf.total_bounds)),
            limit=limit,
        )
        for subsets in subset_granules(
            aoi_gdf,
            output_directory,
            dest,
            overwrite,
            log_level,  # Yuck! Must be better way to deal with process initialization!
            filter(granule_intersects(aoi_gdf.geometry[0]))(granules),
        )
    )

    flow(
        unsafe_perform_io(result),
        map_(pipe(len, f"Subset {{}} granule(s) to {dest}".format, logger.info)),
        lash(raise_exception),
    )


if __name__ == "__main__":
    typer.run(main)
