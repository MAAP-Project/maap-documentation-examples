# ICESat-2 Boreal

Since we do not have the permanent event-based infrastructure set up for item creation and ingestion, we are managing STAC metadata more manually.

## STAC Item generation

1. Run item-gen.py with: `uv run -p 3.13 item-gen.py`
    - this will use a coiled cluster to generate 9900 STAC items and write them to a ndjson file in S3
2. Execute item-load.ipynb
    - this will create the collection JSON documents, post them to the ingestor API, then load the items from the ndjson and post them to the ingestor API
