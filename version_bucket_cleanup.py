"""
    Cleanup S3 Bucket Versions Script

    This script deletes all versions of objects in a prefix in an S3 bucket.
"""

import boto3

# Set up the S3 client
session = boto3.Session(profile_name="mcp")
s3 = session.client('s3')

# Set the bucket name and prefix
bucket_name = {REPLACE_YOUR_BUCKET} # example: 'nasa-maap-data-store'
prefix = {REPLACE_YOUR_PREFIX} # example: 'file-staging/nasa-map/BIOMASS/S1_DGM__1S/'

# Verify bucket prefix
print(f"s3://{bucket_name}/{prefix}")

# Initialize the markers
next_key_marker = None
next_version_id_marker = None

# Get the object versions
count = 0
while True:
    # Get the object versions
    if next_key_marker and next_version_id_marker:
        response = s3.list_object_versions(
            Bucket=bucket_name, 
            Prefix=prefix, 
            KeyMarker=next_key_marker, 
            VersionIdMarker=next_version_id_marker,
        )
    else:
        response = s3.list_object_versions(
            Bucket=bucket_name, 
            Prefix=prefix,
        )

    # Delete versions
    # There should be be at least one DeleteMarker or Version in the response
    if delete_markers := [
        {'Key': version['Key'], 'VersionId': version['VersionId']}
        for version in (response.get('DeleteMarkers', []) + response.get('Versions', []))
    ]:
        count += len(delete_markers)
        s3.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_markers})

    # Check if there are more versions to retrieve
    if response['IsTruncated']:
        next_key_marker = response['NextKeyMarker']
        next_version_id_marker = response['NextVersionIdMarker']
    else:
        break

print(f'Deleted {count} versions')
