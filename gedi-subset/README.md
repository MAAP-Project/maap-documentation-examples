# GEDI Subsetting

- [Getting the GeoJSON URL for an AOI](#getting-the-geojson-url-for-an-aoi)
- [Running the GEDI Subsetting DPS Job](#running-the-gedi-subsetting-dps-job)
- [Citations](#citations)

## Getting the GeoJSON URL for an AOI

In order to subset GEDI data, you must obtain the URL for the GeoJSON file for
an area of interest (AOI), representing the extent of the subset.

To obtain the URL for the GeoJSON of an AOI, you must know the ISO3 code and
level for the AOI's geoBoundary.  If you do not know your AOI's ISO3 code, you
may search for it on the [geoBoundaries] website.

Once you know the ISO3 code and level, construct a [geoBoundaries API] URL
(this is _not_ the GeoJSON URL) of the following form, replacing `<ISO3>` and
`<LEVEL>` with appropriate values:

```plain
https://www.geoboundaries.org/api/current/gbOpen/<ISO3>/<LEVEL>/
```

For example, the ISO3 code for Gabon is GAB.  Therefore, the geoBoundaries API
URL for Gabon level 0 is
<https://www.geoboundaries.org/api/current/gbOpen/GAB/ADM0/>.

You may use the geoBoundaries API URL in various ways to obtain your AOI's
GeoJSON URL, such as one of the following:

- Use a browser to navigate to the API URL.  If your browser directly displays
  the result, locate the value of `"gjDownloadURL"`.  If your browser forces
  you to download the result, do so, and locate the value of `"gjDownloadURL"`
  within the downloaded file.  In either case, the value associated with
  `"gjDownloadURL"` is your AOI's GeoJSON URL.

- Alternatively, open a terminal window and run the following command,
  replacing `<API_URL>` appropriately.  The output should be the GeoJSON URL:

  ```sh
  curl -s <API_URL> | tr ',' '\n' | grep "gjDownloadURL.*gbOpen" | sed -E 's/.*"(https.+)"/\1/'
  ```

Continuing with the Gabon example, entering the geoBoundaries API URL for Gabon
(shown above) in a browser should result in the following (abridged) output
(either in the browser, or within a downloaded file):

```plain
{
  "boundaryID": "GAB-ADM0-25889322",
  "boundaryName": "Gabon",
  "boundaryISO": "GAB",
  ...
  "gjDownloadURL": "https://github.com/wmgeolab/geoBoundaries/raw/9f8c9e0f3aa13c5d07efaf10a829e3be024973fa/releaseData/gbOpen/GAB/ADM0/geoBoundaries-GAB-ADM0.geojson",
  ...
  "gbHumanitarian": {
    ...
  }
}
```

Alternatively, using `curl` from the terminal should also yield the same GeoJSON URL:

```bash
$ curl -s https://www.geoboundaries.org/api/current/gbOpen/GAB/ADM0/ | tr ',' '\n' | grep "gjDownloadURL.*gbOpen" | sed -E 's/.*"(https.+)"/\1/'
https://github.com/wmgeolab/geoBoundaries/raw/9f8c9e0f3aa13c5d07efaf10a829e3be024973fa/releaseData/gbOpen/GAB/ADM0/geoBoundaries-GAB-ADM0.geojson
```

The AOI's GeoJSON URL is the value you're expected to supply for the `aoi`
input when running the GEDI subsetting DPS job.

## Running the GEDI Subsetting DPS Job

The GEDI Subsetting DPS Job is named `gedi-subset_ubuntu`, and may be executed
via the ADE GUI, but since the ADE GUI does not currently allow you to choose
which queue to use, you must use the MAAP API for greater control:

```python
import urllib.parse
import uuid
import xml.etree.ElementTree as ET

from maap.maap import MAAP

maap = MAAP(maap_host='api.ops.maap-project.org')
id = uuid.uuid4()
aoi = "<AOI GeoJSON URL>"  # See previous section
limit = 2000

result = maap.submitJob(
    identifier=f"{id}",
    algo_id="gedi-subset_ubuntu",
    version="gedi-subset",
    queue="maap-dps-worker-8gb",
    username="<USERNAME>",  # Your Earthdata Login username
    aoi=aoi,
    limit=limit,
)
```

To check the job status, use the `"job_id"` value from `result`:

```python
job_id = result["job_id"]
root = ET.fromstring(maap.getJobStatus(job_id).text)
status = root.findall('./{http://www.opengis.net/wps/2.0}Status')[0].text
status
```

The value of `status` should be one of the following:

- `Accepted`
- `Running`
- `Failed`
- `Succeeded`

Once a job has succeeded, you can find the ADE-based path to the output
directory, as follows:

```python
root = ET.fromstring(maap.getJobResult('36f902ca-54fe-4ca1-a6d1-8ebb97e65021').text)
data_elements = root.findall('./*/{http://www.opengis.net/wps/2.0}Data')
data = urllib.parse.urlparse(data_elements[0].text) if len(data_elements) == 3 else None
key = '/'.join(data.path.split('/')[2:]) if data else None
f"/my-private-bucket/{key}" if len(data_elements) == 3 else 'Check job status'
```

For a successful job, you should see something similar to the following:

```plain
/my-private-bucket/dps_output/gedi-subset_ubuntu/gedi-subset/2022/04/19/02/12/04/570895
```

## Citations

Country Boundaries from:

Runfola, D. et al. (2020) geoBoundaries: A global database of political administrative boundaries. PLoS ONE 15(4): e0231866. https://doi.org/10.1371/journal.pone.0231866

[geoBoundaries]:
    https://www.geoboundaries.org
[geoBoundaries API]:
    https://www.geoboundaries.org/api.html
