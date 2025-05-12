# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "boto3",
#     "coiled",
#     "icesat2-boreal-stac",
#     "smart-open",
#     "tqdm",
# ]
#
# [tool.uv.sources]
# icesat2-boreal-stac = { git = "https://github.com/MAAP-project/icesat2-boreal-stac.git", rev = "0.2.3" }
# ///

import json
import logging
import re
from typing import Any, Dict, List
from urllib.parse import urlparse

import boto3
import coiled
import smart_open
from icesat2_boreal_stac.stac import create_item

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

S3_ASSET_PATH = "s3://nasa-maap-data-store/file-staging/nasa-map/icesat2-boreal-v2.1/"
ITEMS_NDJSON_KEY = (
    "s3://maap-ops-workspace/henrydevseed/icesat2-boreal-v2.1/items.ndjson"
)


def scan_s3_files(bucket: str, prefix: str, filename_regex: str) -> List[str]:
    """Scan S3 bucket for files matching the given regex pattern within specified prefix."""
    s3_client = boto3.client("s3")
    matching_files = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" not in page:
            continue

        # Check each object against the regex pattern
        for obj in page["Contents"]:
            key = obj["Key"]
            if re.match(
                filename_regex, key.split("/")[-1]
            ):  # Match against filename only
                matching_files.append(f"s3://{bucket}/{key}")

    return matching_files


@coiled.function(region="us-west-2", threads_per_worker=-1, n_workers=[0, 100])
def _create_item(key: str) -> Dict[str, Any]:
    item = create_item(key)
    variable = "ht" if item.id.startswith("boreal_ht") else "agb"
    item_dict = item.to_dict()
    item_dict["collection"] = f"icesat2-boreal-v2.1-{variable}"

    return item_dict


def write_ndjson(items: List[Dict[str, Any]], ndjson_key: str) -> None:
    s3_client = boto3.client("s3")
    with smart_open.open(
        ndjson_key,
        "wt",
        encoding="utf-8",
        transport_params={"client": s3_client},
    ) as fout:
        count = 0
        for item in items:
            fout.write(json.dumps(item) + "\n")
            count += 1

    logger.info(f"wrote {count} items to {ndjson_key}")


def main():
    # Parse S3 path
    s3_url = urlparse(S3_ASSET_PATH)

    bucket = s3_url.netloc
    prefix = s3_url.path.lstrip("/")
    print("scanning for matching files in S3")
    inventory = scan_s3_files(
        bucket=bucket,
        prefix=prefix,
        filename_regex=r".*\.tif$",
    )

    logger.info(f"found {len(inventory)} files")
    with open("/tmp/inventory.txt", "w") as f:
        for key in inventory:
            f.write(key + "\n")

    logger.info("generating item metadata")
    _items = _create_item.map(inventory)

    write_ndjson(_items, ndjson_key=ITEMS_NDJSON_KEY)


if __name__ == "__main__":
    main()
