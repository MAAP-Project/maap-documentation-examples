# Tiaga Forest Map Import

This example shows how to export an Asset from Google Earth Engine for use in MAAP.
Todo: Add Citation to Paul M.

## Steps

1. Install Dependencies
2. Create and Asset in Google Earth Engine
3. Export the Asset in chunks (10,000 pixels per chunk max) - the chunk area will depend on the scale of your outputs. 
In this example 1x1 degree chunks were exported at 30m scale, in EPSG:4326
4. Convert Exported Zipped Tiff files int Cloud Optimized Tiff files
5. Generate a mosaicjson for visualizing the dataset
