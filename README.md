# SHREAD Snow Data Product Aquisition and Processing Tool
SHREAD, the Snow-Hydrology Repo for Evaluation, Analysis, and Decision-making, is a tool for downloading, processing, and archiving snow data products, nominally to allow them to better support water management decision making. The tool is still in development, but currently provides access to the following products -
  * NOHRSC SNODAS Snow Water Equivalent (SWE)
  * NOHRSC SNODAS Fractional Snow-Covered Area (fSCA)
  * NOHRSC Snow Reporters Snow Depth (SD) and SWE
  * JPL MODSCAG MODIS-Derived Fractional Snow-Covered Area (fSCA)

Support for additional products is in development or planned, including -
  * JPL MODDRFS MODIS-Derived Dust Radiative Forcing
  * NASA MODIS Fractional Snow-Covered Area (fSCA)
  * USGS Landsat Fractional Snow-Covered Area (fSCA)
  * ESA Sentinel-3 Snow Products
  * Theia Sentinel-3 Snow Products
  * NOHRSC National Snow Analysis Snowfall Depth

## Requirements
* Python 3.X (currently tested using Python 3.8)
* Libraries listed in the *environment.yml* file

<sup>a</sup> SHREAD currently requires GDAL to perform geospatial tasks. Installation instructions for GDAL can be found XXX. Model development is replacing GDAL dependencies with Python rasterio calls, however this transition is not yet complete.

## Use
The easiest way to get started with SHREAD is to install Anaconda 3 and create a new Python environment using the provided *environment.yml* file.

    conda env create -f environment.yml
    conda activate shread

To use SHREAD call it from the command line. An example *config file* is provided in the repo and additional documentation on the code will be forthcoming.  

    python shread.py -i [config_file] -s [%Y%m%d] -d [%Y%m%d] -t [D] -p [snodas,srpt,modscag]

## Disclaimer
The software as originally published constitutes a work of the United States Government and is not subject to domestic copyright protection under 17 USC ¤ 105. Subsequent contributions by members of the public, however, retain their original copyright.

## Acknowledgements
This repository has made use of bits of code from the [RHEAS](https://github.com/nasa/RHEAS) repository, the [cdss-app-snodas-tools](https://github.com/OpenWaterFoundation/cdss-app-snodas-tools) repository, and the [stationsweRegression](https://github.com/hoargroup/stationsweRegression) repository and would like to acknowledge their contribution to the success of this code.
