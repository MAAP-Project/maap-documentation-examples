# ATL08 as a Cloud Optimized Point Cloud (COPC)

This example set of notebooks shows how to convert ICESat2 ATL08 products to COPC format.
1. [pdal_setup](pdal_setup.ipynb) shows how to setup a PDAL environment with Conda
2. [ATL08_to_COPC](ATL08_to_COPC.ipynb) uses PDAL pipelines to convert an HDF5 granule to COPC format
3. TODO: Show how to use COPC in a data workflow with spatial and attribute subsetting on read.