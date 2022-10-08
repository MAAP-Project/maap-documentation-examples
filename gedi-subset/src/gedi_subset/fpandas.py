import functools
from dataclasses import dataclass
from typing import Callable, Hashable, Literal, Sequence

import geopandas as gpd
import pandas as pd

from gedi_subset.fp import thread_first


def drop(
    labels: Hashable | Sequence[Hashable] | None = None,
    *,
    axis: str | int = 0,
    index: Hashable | Sequence[Hashable] | None = None,
    columns: Hashable | Sequence[Hashable] | pd.Index | None = None,
    level: Hashable | int | None = None,
    errors: Literal["ignore", "raise"] = "raise",
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    @functools.wraps(pd.DataFrame.drop)
    def go(df: pd.DataFrame):
        return df.drop(
            labels,
            axis=axis,
            index=index,
            columns=columns,
            level=level,
            inplace=False,
            errors=errors,
        )

    return go


@dataclass
class DataFrame:
    assign = thread_first(pd.DataFrame.assign)
    drop = drop


@dataclass
class GeoDataFrame:
    clip = thread_first(gpd.GeoDataFrame.clip)  # type: ignore
    make = thread_first(gpd.GeoDataFrame)  # type: ignore
