# Tiaga Forest Map Import

This example shows how to export an Asset from Google Earth Engine for use in MAAP.
Original data is from to Paul Montensano [@pahbs](https://github.com/pahbs)
Code by [@wildintellect](https://github.com/wildintellect) and [@slesaad](https://github.com/slesaad)

## Steps

1. Install Dependencies 
  - GEE python
  - rio-cogeo
  - https://github.com/ucd-cws/EE-Download
2. Create and Asset in Google Earth Engine
3. [GeeExport.ipynb](GeeExport.ipynb) Export the Asset in chunks (10,000 pixels per chunk max) - the chunk area will depend on the scale of your outputs. 
In this example 1x1 degree chunks were exported at 30m scale, in EPSG:4326
4. [to_tif.ipynb](to_tif.ipynb) Convert Exported Zipped Tiff files int Cloud Optimized Tiff files (uses maketif.sh)
5. Generate a mosaicjson for visualizing the dataset (optional)
