# Chuck's Magic
#

import os
from typing import Any, Callable, List, Mapping, Optional, Sequence, TypeVar

import geopandas as gpd
import pandas as pd
import requests
from returns.curry import curry

_A = TypeVar('_A')
_B = TypeVar('_B')
_C = TypeVar('_C')
_D = TypeVar('_D')
_DF = TypeVar('_DF', bound=pd.DataFrame)
_T = TypeVar('_T')


def pprint(value: Any) -> None:
    print(json.dumps(value, indent=2))


# str -> str -> str
@curry
def chext(ext: str, path: str) -> str:
    return f'{os.path.splitext(path)[0]}{ext}'


# (_T -> None) -> Sequence _T -> None
@curry
def for_each(f: Callable[[_T], None], xs: Sequence[_T]) -> None:
    for x in xs:
        f(x)


# (_B -> _C -> _D) -> (_A -> _B) -> (_A -> _C) -> (_A -> _D)
@curry
def converge(
    join: Callable[[_B], Callable[[_C], _D]],
    f: Callable[[_A], _B],
    g: [[_A], _C]
) -> Callable[[_A], _D]:
    def do_join(x: _A) -> _D:
        return join(f(x))(g(x))

    return do_join


# str -> Any -> _DF -> _DF
@curry
def df_assign(col_name: str, val: Any, df: _DF) -> _DF:
    return df.assign(**{col_name: val})

    
def get_geo_boundary(iso: str, level: int) -> gpd.GeoDataFrame:
    file_path = f'/projects/my-public-bucket/iso3/{iso}-ADM{level}.json'

    if not os.path.exists(file_path):
        r = requests.get(
            'https://www.geoboundaries.org/gbRequest.html',
            dict(ISO=iso, ADM=f'ADM{level}')
        )
        r.raise_for_status()
        dl_url = r.json()[0]['gjDownloadURL']
        geo_boundary = requests.get(dl_url).json()

        with open(file_path, 'w') as out:
            out.write(json.dumps(geo_boundary))

    return gpd.read_file(file_path)


def subset_gedi_granule(granule: str, aoi, filter_cols: List = ["lat_lowestmode", "lon_lowestmode"]):
    """
    Subset a GEDI granule by a polygon in CRS 4326
    granule = path to a granule h5 file that's already been downloaded
    aoi = a shapely polygon of the aoi
    
    return path to geojson output
    """
    infile = granule

    hf_in = h5py.File(infile, 'r')

    result = subset_h5(hf_in, aoi, filter_cols)

    return result


def spatial_filter(beam, aoi):
    """
    Find the record indices within the aoi
    TODO: Make this faster
    """
    lat = beam['lat_lowestmode'][:]
    lon = beam['lon_lowestmode'][:]
    i = np.arange(0, len(lat), 1) # index
    geo_arr = list(zip(lat,lon, i))
    l4adf = pd.DataFrame(geo_arr, columns=["lat_lowestmode", "lon_lowestmode", "i"])
    l4agdf = gpd.GeoDataFrame(l4adf, geometry=gpd.points_from_xy(l4adf.lon_lowestmode, l4adf.lat_lowestmode))
    l4agdf.crs = "EPSG:4326"
    # TODO: is it faster with a spatial index, or rough pass with BBOX first?
    bbox = aoi.geometry[0].bounds
    l4agdf_clip = l4agdf.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]]
    l4agdf_gsrm = l4agdf_clip[l4agdf_clip['geometry'].within(aoi.geometry[0])]
    indices = l4agdf_gsrm.i

    return indices


def subset_h5(hf_in, aoi, filter_cols):
    """
    Extract the beam data only for the aoi and only columns of interest
    """
    subset_df = pd.DataFrame()

    # loop through BEAMXXXX groups
    for v in list(hf_in.keys()):
        if v.startswith('BEAM'):
            col_names = []
            col_val = []
            beam = hf_in[v]

            indices = spatial_filter(beam, aoi)

            # TODO: when to spatial subset?
            for key, value in beam.items():
                # looping through subgroups
                if isinstance(value, h5py.Group):
                    for key2, value2 in value.items():
                        if (key2 not in filter_cols):
                            continue
                        if (key2 != "shot_number"):
                             # xvar variables have 2D
                            if (key2.startswith('xvar')):
                                for r in range(4):
                                    col_names.append(key2 + '_' + str(r+1))
                                    col_val.append(value2[:, r][indices].tolist())
                            else:
                                col_names.append(key2)
                                col_val.append(value2[:][indices].tolist())

                #looping through base group
                else:
                    if (key not in filter_cols):
                        continue
                    # xvar variables have 2D
                    if (key.startswith('xvar')):
                        for r in range(4):
                            col_names.append(key + '_' + str(r+1))
                            col_val.append(value[:, r][indices].tolist())

                    else:
                        col_names.append(key)
                        col_val.append(value[:][indices].tolist())

            # create a pandas dataframe
            beam_df = pd.DataFrame(map(list, zip(*col_val)), columns=col_names)
            # Inserting BEAM names
            beam_df.insert(0, 'BEAM', np.repeat(str(v), len(beam_df.index)).tolist())
            # Appending to the subset_df dataframe
            subset_df = subset_df.append(beam_df)

    hf_in.close()
    # all_gdf = gpd.GeoDataFrame(subset_df, geometry=gpd.points_from_xy(subset_df.lon_lowestmode, subset_df.lat_lowestmode))
    all_gdf = gpd.GeoDataFrame(subset_df.loc[:,~subset_df.columns.isin(['lon_lowestmode', 'lat_lowestmode'])],
                               geometry=gpd.points_from_xy(subset_df.lon_lowestmode, subset_df.lat_lowestmode))
    all_gdf.crs = "EPSG:4326"
    # TODO: Drop the lon and lat columns after geometry creation(or during)
    # TODO: document how many points before and after filtering
    #print(f"All points {all_gdf.shape}")
    #subset_gdf = all_gdf[all_gdf['geometry'].within(aoi.geometry[0])]
    subset_gdf = all_gdf #Doing the spatial search first didn't help at all, so maybe the spatial query is the slow part.
    #print(f"Subset points {subset_gdf.shape}")

    return subset_gdf


def write_subset(infile, gdf):
    """
    Write GeoDataFrame to Flatgeobuf
    TODO: What's the most efficient format?
    """
    outfile = infile.replace('.h5', '.fgb')
    gdf.to_file(outfile, driver='FlatGeobuf')

    return outfile
