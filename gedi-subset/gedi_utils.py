# Chuck's Magic
#

from typing import Any, Callable, Mapping, Optional, Sequence, TypeVar
import geopandas as gpd
import os
import requests

T = TypeVar('T')

def for_each(f: Callable[[T], None], xs: Sequence[T]) -> None:
    for x in xs:
        f(x)


def pprint(value: Any) -> None:
    print(json.dumps(value, indent=2))

    
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