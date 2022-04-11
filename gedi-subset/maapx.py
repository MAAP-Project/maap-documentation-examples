from operator import itemgetter
from typing import Mapping, Optional, TypedDict

import boto3
from cachetools.func import ttl_cache
from returns.curry import curry
from returns.io import impure_safe, IOFailure, IOResult, IOResultE
from returns.methods import cond
from returns.pipeline import flow
from returns.pointfree import bind, bind_result, lash, map_
from returns.result import safe

from fp import K
from maap.maap import MAAP
from maap.AWS import AWSCredentials
from maap.Result import Granule

# https://nasa-openscapes.github.io/2021-Cloud-Workshop-AGU/how-tos/Earthdata_Cloud__Single_File__Direct_S3_Access_COG_Example.html
_S3_CREDENTIALS_ENDPOINT_BY_DAAC: Mapping[str, str] = dict(
    po="https://archive.podaac.earthdata.nasa.gov/s3credentials",
    gesdisc="https://data.gesdisc.earthdata.nasa.gov/s3credentials",
    lp="https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials",
    ornl="https://data.ornldaac.earthdata.nasa.gov/s3credentials",
    ghrc="https://data.ghrc.earthdata.nasa.gov/s3credentials",
)


@ttl_cache(ttl=55 * 60)
def earthdata_s3_credentials(maap: MAAP, daac: str) -> IOResultE[AWSCredentials]:
    """Returns short-term AWS credentials for access to DAAC's S3 distribution bucket.
    
    Returns an `AWSCredentials` object (wrapped in an `IO[Success]`) if successful;
    otherwise an error (wrapped in an `IO[Failure]`).  If the credentials endpoint for
    the specified DAAC is unknown, returns an `IO[Failure[KeyError]]` indicating so.
    """
    return flow(
        _S3_CREDENTIALS_ENDPOINT_BY_DAAC,
        safe(itemgetter(daac)),
        lash(K(IOFailure(KeyError(f"No S3 credentials endpoint for DAAC '{daac}'")))),
        bind(impure_safe(maap.aws.earthdata_s3_credentials)),
    )


@curry
def refresh_boto3_default_session(maap: MAAP, daac: str) -> IOResultE[None]:
    return cond(
        IOResult,
        daac in _S3_CREDENTIALS_ENDPOINT_BY_DAAC,
        flow(
            _S3_CREDENTIALS_ENDPOINT_BY_DAAC[daac],
            impure_safe(maap.aws.earthdata_s3_credentials),
            lambda creds: boto3.setup_default_session(
                aws_access_key_id=creds["accessKeyId"],
                aws_secret_access_key=creds["secretAccessKey"],
                aws_session_token=creds["sessionToken"],
                region_name="us-west-2",
            ),
        ),
        # Do we want to do this? What if getData doesn't use S3 for a granule?
        # Perhaps we silently ignore this, and let getData fail, if it does use S3.
        KeyError(f"No S3 credentials endpoint configured for DAAC '{daac}'"),
    )


@curry
def download_granule(maap: MAAP, todir: str, granule: Granule) -> IOResultE[str]:
    return flow(
        refresh_boto3_default_session(maap, xxx),
        impure_safe(granule.getData)(todir),
    )
