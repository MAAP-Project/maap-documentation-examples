#!/usr/bin/env python
import multiprocessing
import os
import os.path
from functools import partial
from pathlib import Path
from typing import Optional, Tuple

import geopandas as gpd
import typer
from maap.maap import MAAP, Collection, Granule
from returns.curry import curry
from returns.functions import identity, raise_exception, tap
from returns.io import impure_safe, IO, IOResultE, IOSuccess
from returns.pipeline import flow, is_successful, pipe
from returns.unsafe import unsafe_perform_io
from returns.iterables import Fold
from returns.pointfree import bind_ioresult, map_, lash

from gedi_utils import chext, append_gdf_file, df_assign, for_each, subset_h5, when


def main(
    geojson: Path = typer.Option(
        ...,
        help="Path to GeoJSON file (AOI)",
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
        help="DOI of collection to subset (https://www.doi.org/)",
    ),
    cmr_host=typer.Option(
        # "cmr.maap-project.org",  # Doesn't properly handle HTTPS fallback from S3
        "cmr.earthdata.nasa.gov",
        help="CMR hostname",
    ),
    limit: int = typer.Option(  # Temporary, for testing w/o code changes
        2,
        help="Maximum number of granules to subset",
    ),
) -> None:
    # TODO: where's the correct destination?
    subset_name, _ = os.path.splitext(os.path.basename(geojson))
    dest_dir = Path("/") / "projects" / "tmp" / "subsets" / subset_name
    dest = dest_dir / "subset.gpkg"
    os.makedirs(dest_dir, exist_ok=True)

    # Remove existing combined subset file, primarily to support
    # testing.  When running in the context of a DPS job, there
    # should be no existing file since every job uses a unique
    # output directory.
    if os.path.exists(dest):
        os.remove(dest)

    aoi = gpd.read_file(geojson)
    maap = MAAP("api.ops.maap-project.org")

    collection = maap.searchCollection(cmr_host=cmr_host, doi=doi, limit=1)[0]
    collection_name = collection["Collection"]["ShortName"]
    collection_version = collection["Collection"]["VersionId"]

    granules = maap.searchGranule(
        cmr_host=cmr_host,
        collection_concept_id=collection["concept-id"],
        bounding_box=",".join(map(str, aoi.total_bounds)),
        limit=limit,
    )

    typer.echo(
        f"Found {len(granules)} granule(s) for DOI {doi} "
        + f"({collection_name}, v{collection_version})"
    )

    filter_cols = [
        "agbd",
        "agbd_se",
        "l4_quality_flag",
        "sensitivity",
        "lat_lowestmode",
        "lon_lowestmode",
    ]

    # We must declare our nested function as global to allow
    # multiprocessing to pickle it.
    global subset_granule

    @impure_safe
    def subset_granule(granule: Granule) -> str:
        typer.echo(f"Downloading granule {granule['Granule']['GranuleUR']}...")
        inpath = granule.getData(str(dest_dir))
        typer.echo(f"Finished downloading {inpath}")
        typer.echo(f"Subsetting {inpath}...")
        gdf = df_assign("filename", inpath, subset_h5(inpath, aoi, filter_cols))
        outpath = chext(".fgb", inpath) if len(gdf.index) > 0 else ""

        # Don't write empty DataFrame, otherwise we'll get an error:
        # ValueError: Cannot write empty DataFrame to file.
        if outpath:
            gdf.to_file(outpath, driver="FlatGeobuf")
            typer.echo(f"Finished subsetting {inpath} to {outpath}")
        else:
            typer.echo(f"Subset of {inpath} is empty; nothing written")

        return outpath

    # Use no more than half the CPUs available to this process
    processes = min(len(granules), len(os.sched_getaffinity(0)) // 2)

    # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.pool.Pool.imap
    # chunksize = max(1, round(len(granules) / processes))
    chunksize = max(1, min(10, round(len(granules) / processes)))

    typer.echo(f"Multiprocessing with {processes} processes (chunksize={chunksize})...")

    with multiprocessing.Pool(processes) as pool:
        result: IOResultE[None] = flow(
            granules,
            partial(pool.imap_unordered, subset_granule, chunksize=chunksize),
            partial(filter, lambda r: map_(bool)(r).value_or(True) == IO(True)),
            partial(map, bind_ioresult(append_gdf_file(dest))),
            partial(Fold.collect, acc=IOSuccess(())),
        )

    unsafe_perform_io(result).alt(raise_exception)
    typer.echo(f"Wrote combined subsets to {dest}")


if __name__ == "__main__":
    typer.run(main)
