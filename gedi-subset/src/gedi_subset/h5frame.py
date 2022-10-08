import os
import re
from typing import Sequence

import geopandas as gpd
import h5py
import pandas as pd
from pandas.core.computation.ops import UndefinedVariableError

import gedi_subset.fp as fp


class H5DataFrame(pd.DataFrame):
    _metadata = ["_h5", "_aliases"]

    def __init__(
        self,
        data: h5py.Group,
        *,
        columns: Sequence[str] | None = None,
        aliases: dict[str, str] | None = None,
        # **kwargs: Any,
    ) -> None:
        if not isinstance(data, h5py.Group):
            raise ValueError(
                "Data object must be an instance of `h5py.Group`"
                f" (typically an instance of an open `h5py.File`): {type(data)}"
            )

        super().__init__()
        self._h5 = data
        self._aliases = aliases or {}

        # Fetch the specified columns from the data source
        fp.for_each(self.get)(columns or [])

    def eval(self, expr: str, inplace: bool = False, **kwargs):
        try:
            return super().eval(expr, inplace, **kwargs)
        except UndefinedVariableError as e:
            if match := re.search(r"'(?P<name>\w+)'", e.args[0]):
                self[match.group("name")]
            return self.eval(expr, inplace, **kwargs)

    def __getitem__(self, key):
        # If the key is not a string (e.g., a slice), or there is already a value
        # associated with it, delegate to the superclass.
        print(f"{key=}, type={type(key)}")

        # TODO: Should we allow slices, etc. to pass through to the h5 fancy indexing?
        if not isinstance(key, str) or key in self.keys():
            return super().__getitem__(key)

        try:
            # Fetch the value from the backing h5 file, after checking for a relative
            # path in the aliases dict.
            path = self._aliases.get(key, key)
            value: h5py.Group | h5py.Dataset = self._h5[path]

            if isinstance(value, h5py.Group):
                # TODO: pass along aliases? if so, remove leading segments from paths?
                return H5DataFrame(value)
            if value.ndim == 2:
                return pd.DataFrame(value)
            elif value.ndim == 1:
                # Assign a Series to automatically align indexes
                self[key] = pd.Series(value, index=pd.RangeIndex(0, len(value)))

            return super().__getitem__(key)
        except KeyError:
            # If the specified key is of the form <NAME><INDEX>, then we're assuming
            # that the key <NAME> represents a 2D dataset, so we attempt to fetch it
            # and grab the column at index <INDEX>.  For example, if the key is "rh50",
            # we look for dataset "rh", assume it's 2D, and set column "rh50" to the
            # value of column 50 (0-based) of the "rh" dataset (i.e., rh[50]).
            if match := re.fullmatch(r"(?P<name>[a-zA-Z_]+)(?P<index>\d+)", key):
                name, index = match.groups()
                self[key] = self[name][int(index)]

            return super().__getitem__(key)


def subset(
    h5: h5py.Group,
    aoi: gpd.GeoDataFrame,
    columns: Sequence[str],
    query: str,
) -> gpd.GeoDataFrame:
    def subset_beam(beam: h5py.Group) -> gpd.GeoDataFrame:
        beam_df = H5DataFrame(beam, columns=columns)
        # print(type(beam_df))
        beam_df.query(query, inplace=True)
        geometry = gpd.points_from_xy(beam_df[lon_column], beam_df[lat_column])
        beam_df.drop(columns=list(set(beam_df.columns) - set(columns)), inplace=True)
        beam_df.insert(0, "BEAM", beam.name[5:])
        gdf = gpd.GeoDataFrame(beam_df, geometry=geometry, crs="EPSG:4326")

        return gdf.clip(aoi.set_crs(epsg=4326))

        # return flow(
        #     beam_df,
        #     DF.drop(columns=list(set(beam_df.columns) - set(columns))),
        #     DF.assign(BEAM=beam.name[5:]),
        #     GDF.make(geometry=geometry, crs="EPSG:4326"),
        #     GDF.clip(aoi.set_crs(epsg=4326)),
        # )

    # TODO: define as func params
    lat_column = "lat_lowestmode"
    lon_column = "lon_lowestmode"

    # columns_plus_latlon = list(set([*columns, lat_column, lon_column]))

    beams = (group for name, group in h5.items() if name.startswith("BEAM"))
    beams_gdf = pd.concat(map(subset_beam, beams), copy=False)
    beams_gdf.insert(0, "filename", os.path.basename(h5.file.filename))

    return beams_gdf


if __name__ == "__main__":
    import time

    aoi = gpd.read_file("input/MONTS_BIROUGOU_NATIONAL_PARK.geojson")
    path = "output/GEDI02_A_2019146134206_O02558_01_T05641_02_003_01_V002.h5"

    with h5py.File(path) as hdf5:
        start = int(round(time.time() * 1000))
        print(
            subset(
                hdf5,
                aoi,
                ["beam", "energy_total"],
                "`quality_flag`==1 and sensitivity>0.95",
            )
        )
        stop = int(round(time.time() * 1000))
        print(stop - start)
