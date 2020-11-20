# SHREAD Snow Data Product Aquisition and Processing Tool
SHREAD, the Snow-Hydrology Repo for Evaluation, Analysis, and Decision-making, is a tool for downloading, processing, and archiving snow data products, nominally to allow them to better support water management decision making. The tool is still in development, but currently provides access to the following products -
  * NOHRSC SNODAS Snow Water Equivalent (SWE)
  * NOHRSC SNODAS Fractional Snow-Covered Area (fSCA)
  * NOHRSC National Snow Analysis Snowfall Depth
  * NOHRSC Aggregated Station Snowfall Reports
  * NOHRSC Aggregated Station Snow Depth (SD) Reports
  * NOHRSC Aggregated Station Snow Water Equivalent (SWE) Reports
  * JPL MODSCAG MODIS-Derived Fractional Snow-Covered Area (fSCA)

Support for additional products is in development or planned, including -
  * NASA MODIS Fractional Snow-Covered Area (fSCA)
  * USGS Landsat Fractional Snow-Covered Area (fSCA)
  * ESA Sentinel-3 Snow Products
  * Theia Sentinel-3 Snow Products

## Requirements
* Python 3.X with the following libraries installed -
  * [gdal](https://pypi.org/project/GDAL/)<sup>a</sup>
    * osr
    * ogr
  * [geojson](https://pypi.python.org/pypi/geojson/)
  * [json](https://docs.python.org/3/library/json.html)
  * [requests](https://pypi.org/project/requests/)
  * [os](https://docs.python.org/3/library/os.html)
  * [tarfile](https://docs.python.org/3/library/tarfile.html)
  * [gzip](https://docs.python.org/3/library/gzip.html)
  * [ftplib](https://docs.python.org/3/library/ftplib.html)
  * [csv](https://docs.python.org/3/library/csv.html)
  * [logging](https://docs.python.org/2/library/logging.html)
  * [glob](https://docs.python.org/3/library/glob.html)
  * [zipfile](https://docs.python.org/3/library/zipfile.html)
  * [fileinput](https://docs.python.org/2/library/fileinput.html)
  * [datetime](https://docs.python.org/3/library/datetime.html)
  * [configparser](https://docs.python.org/2/library/configparser.html)
  * [requests.auth](https://requests.readthedocs.io/en/master/user/authentication/)
  * [sys](https://docs.python.org/3/library/sys.html)
  * [argparse](https://docs.python.org/3/library/argparse.html)
  * [urllib.request](https://docs.python.org/3/library/urllib.request.html)

<sup>a</sup> SHREAD currently requires GDAL to perform geospatial tasks. Installation instructions for GDAL can be found XXX. Model development is adding in optional ArcPy support to replace GDAL, however this support is not yet complete.

## Use
XXX

## Disclaimer
The software as originally published constitutes a work of the United States Government and is not subject to domestic copyright protection under 17 USC ¤ 105. Subsequent contributions by members of the public, however, retain their original copyright.

## Acknowledgements
This repository has made use of bits of code from the [RHEAS](https://github.com/nasa/RHEAS) repository and the [cdss-app-snodas-tools](https://github.com/OpenWaterFoundation/cdss-app-snodas-tools) repository and would like to acknowledge their contribution to the success of this code.
