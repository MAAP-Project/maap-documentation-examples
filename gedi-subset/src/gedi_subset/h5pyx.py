from collections import defaultdict
from itertools import chain, filterfalse, tee
from typing import Iterable

import geopandas as gpd
import h5py
from pandas.core.computation.expr import Expr
from pandas.core.computation.ops import Op, Term
from pandas.core.computation.scope import ensure_scope


def parse(expr: str) -> Term | Op:
    resolver = defaultdict(int, __foo__=0)
    env = ensure_scope(0, global_dict={}, local_dict={}, resolvers=(resolver,))
    return Expr(expr, env=env).parse()


# def expr_names(expr: Expr) -> FrozenSet[str]:
#     """Return frozen set of variable names parsed from a query expression."""

#     chain.from_iterable(
#         expr_names(subexpr)
#         for subexpr in expr
#         if isinstance(expr, collections.abc.Iterable)
#     )

#     return frozenset(name for name in parsed_expr.names if isinstance(name, str))


def partition(pred):
    def go(iterable):
        "Use a predicate to partition entries into true entries and false entries"
        # partition(is_odd)(range(10)) --> 1 3 5 7 9  and  0 2 4 6 8
        t1, t2 = tee(iterable)
        return filter(pred, t1), filterfalse(pred, t2)

    return go


def flatmap(f):
    def go(iterable):
        yield from chain.from_iterable(map(f, iterable))

    return go


def isinst(types):
    def go(value):
        return isinstance(value, *types)

    return go


def flatten(group: h5py.Group) -> Iterable[h5py.Dataset]:
    return chain.from_iterable(
        [value] if isinstance(value, h5py.Dataset) else flatten(value)
        for value in group.values()
    )


def fpflatten(group: h5py.Group) -> Iterable[h5py.Dataset]:
    datasets, subgroups = partition(isinst([h5py.Dataset]))(group.values())
    return chain(datasets, flatmap(fpflatten)(subgroups))


if __name__ == "__main__":
    import time

    from gedi_subset.gedi_utils import subset_hdf5

    aoi = gpd.read_file("input/MONTS_BIROUGOU_NATIONAL_PARK.geojson")
    path = "output/GEDI02_A_2019146134206_O02558_01_T05641_02_003_01_V002.h5"

    with h5py.File(path, "r+") as hdf5:
        start = int(round(time.time() * 1000))
        print(
            subset_hdf5(
                hdf5,
                aoi,
                ["beam", "energy_total"],
                "`quality_flag`==1 and sensitivity>0.95",
            )
        )
        stop = int(round(time.time() * 1000))
        print(stop - start)
