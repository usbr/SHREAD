<div class="inline-block">
 <img src="https://github.com/usbr/SHREAD/blob/master/resources/images/logo_shread.png" width="20%" />
 <img src="https://github.com/usbr/SHREAD/blob/master/resources/images/logo_shreadtools.png" width="31%" />
</div>

# SHREAD Snow Data Product Aquisition and Processing Tool
SHREAD, the Snow-Hydrology Repo for Evaluation, Analysis, and Decision-making, is a tool for downloading, processing, and archiving snow data products, nominally to allow them to better support water management decision making. The tool is still in development, but currently provides access to the following products -
  * [NOHRSC SNODAS Snow Water Equivalent (SWE) and Fractional Snow-Covered Area (fSCA)](https://www.nohrsc.noaa.gov/technology/pdf/wsc2001.pdf)
  * [NOHRSC Snow Reporters Snow Depth (SD) and SWE](https://www.nohrsc.noaa.gov/nsa/)
  * [JPL MODSCAG MODIS-Derived Fractional Snow-Covered Area (fSCA)](https://doi.org/10.1016/j.rse.2009.01.001)
  * [JPL MODDRFS MODIS-Derived Dust Radiative Forcing](https://doi.org/10.1029/2012GL052457)
  * [NASA MODIS Normalized Difference Snow Index (NDSI)](https://nsidc.org/sites/nsidc.org/files/files/MODIS-snow-user-guide-C6.pdf)
  * [University of Arizona Snow Water Artificial Neural Network Modelling System (SWANN) SD and SWE](https://doi.org/10.5067/0GGPB220EX6A)

Support for additional products is in development or planned, including -
  
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

    python shread.py -i [config_file] -s [%Y%m%d] -d [%Y%m%d] -t [D] -p [snodas,srpt,modscag,modis,swann]

## Disclaimer
The software as originally published constitutes a work of the United States Government and is not subject to domestic copyright protection under 17 USC ¤ 105. Subsequent contributions by members of the public, however, retain their original copyright.

## Acknowledgements
This repository has made use of bits of code from the [RHEAS](https://github.com/nasa/RHEAS) repository, the [cdss-app-snodas-tools](https://github.com/OpenWaterFoundation/cdss-app-snodas-tools) repository, and the [stationsweRegression](https://github.com/hoargroup/stationsweRegression) repository and would like to acknowledge their contribution to the success of this code.
