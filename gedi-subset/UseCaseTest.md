# Use Case: GEDI L4A Subset

Authors: Alex Mandel and Chuck Daniels

## CMR Query

`setup.ipynb`

* 1009 granules matched a BBOX search of CMR (Initially discovered MAAP CMR had the collection BBOX for every granule, now fixed)
* Tried Polygon search but couldn't get it to work. Would be good to fix as some granules do not actually have data in the AOI polygon of Gabon, but BBOX of Gabon interesects with BBOX of Granule.
* `maap-py` has some performance issues

## Download

`setup.ipynb`

* Takes ~11 Sec to download a ? MB granule

## Subset

`subset.ipynb`

### Process

1. Open H5 file with python
2. Loop over the BEAM groups, extracting preselected attributes (keys)
3. Convert lists into Pandas dataframe
4. Convert Pandas into Geopandas
5. Apply spatial filter


### Notes

* GeoJSON files were very large, switched to FileGeoBuffer format. Ideally partial spatial subsets can be read from this format (has a spatial index), otherwise it applies some compression and is relatively fast to read back in GeoPandas.
* Need to skip writing granules with no matching data for the AOI
* Subsetting needs to also subset attributes to include, otherwise the files are extremely large still.
* TODO: Spatial filter applies before extracting data (2) could cut down on time and memory requirement significantly.

###Questions

1. For several attributes there is a numbered version that applies to each BEAM? Still trying to understand what these are and if we need to keep them. Seems there are several alogrithms `Predicted AGBD using algorithm setting N` are we ok with just the "optimal"
```
 'agbd_a1',
 'agbd_a10',
 'agbd_a2',
 'agbd_a3',
 'agbd_a4',
 'agbd_a5',
 'agbd_a6',
```

### Timing

```
  subset_gedi_granule (/tmp/ipykernel_12978/815998746.py:1):
    46.730 seconds

Subset points (64209, 8)

  write_subset (/tmp/ipykernel_12978/815998746.py:82):
    8.495 seconds

All points (1338484, 8)
Subset points (24866, 8)

  subset_gedi_granule (/tmp/ipykernel_12978/815998746.py:1):
    27.611 seconds


  write_subset (/tmp/ipykernel_12978/815998746.py:82):
    3.922 seconds

All points (609748, 8)

  subset_gedi_granule (/tmp/ipykernel_12978/815998746.py:1):
    5.021 seconds

Subset points (0, 8)
All points (395012, 8)

  subset_gedi_granule (/tmp/ipykernel_12978/815998746.py:1):
    13.178 seconds

Subset points (1846, 8)

  write_subset (/tmp/ipykernel_12978/815998746.py:82):
    1.736 seconds

All points (1338733, 8)
Subset points (14567, 8)

  subset_gedi_granule (/tmp/ipykernel_12978/815998746.py:1):
    33.706 seconds


  write_subset (/tmp/ipykernel_12978/815998746.py:82):
    2.952 seconds

All points (1037928, 8)

  subset_gedi_granule (/tmp/ipykernel_12978/815998746.py:1):
    33.965 seconds

Subset points (39284, 8)

  write_subset (/tmp/ipykernel_12978/815998746.py:82):
    5.866 seconds

All points (515477, 8)

  subset_gedi_granule (/tmp/ipykernel_12978/815998746.py:1):
    29.621 seconds

Subset points (37779, 8)

  write_subset (/tmp/ipykernel_12978/815998746.py:82):
    5.283 seconds

All points (522780, 8)

  subset_gedi_granule (/tmp/ipykernel_12978/815998746.py:1):
    22.457 seconds

Subset points (0, 8)
All points (1045559, 8)

  subset_gedi_granule (/tmp/ipykernel_12978/815998746.py:1):
    15.608 seconds

Subset points (0, 8)
All points (912543, 8)
Subset points (66053, 8)

  subset_gedi_granule (/tmp/ipykernel_12978/815998746.py:1):
    48.031 seconds


  write_subset (/tmp/ipykernel_12978/815998746.py:82):
    9.183 seconds

All points (508165, 8)

  subset_gedi_granule (/tmp/ipykernel_12978/815998746.py:1):
    46.880 seconds

Subset points (58430, 8)

  write_subset (/tmp/ipykernel_12978/815998746.py:82):
    7.914 seconds

All points (1338521, 8)

  subset_gedi_granule (/tmp/ipykernel_12978/815998746.py:1):
    33.797 seconds

Subset points (23275, 8)

  write_subset (/tmp/ipykernel_12978/815998746.py:82):
    3.834 seconds

All points (1054709, 8)

  subset_gedi_granule (/tmp/ipykernel_12978/815998746.py:1):
    11.281 seconds

Subset points (0, 8)
All points (372346, 8)

  subset_gedi_granule (/tmp/ipykernel_12978/815998746.py:1):
    6.934 seconds

Subset points (0, 8)
```

## Combine

`combine.ipynb`
