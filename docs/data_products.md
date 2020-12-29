## Data Products

SHREAD provides access to snow-related datasets that quantify snow as snow-covered
area (SCA), fractional snow-covered area (fSCA), snow depth, or snow-water equivalent (SWE).

### SNODAS
The National Operational Hydrologic Remote Sensing Center (NOHRSC) runs the
Snow Data Assimilation System (SNODAS), which assimilates snow observations into
a spatially distributed energy-and-mass-balance snow model that is run each
day in near-real time. This model produces several snow-related datasets
including SWE and snow depth at a 1km spatial resolution across the continental United States.

SHREAD provides access to SNODAS through the data archive at the [National
Snow and Ice Data Center (NSIDC)](https://nsidc.org/data/g02158). A summary
of SNODAS data is shown in the table below. Currently SHREAD only provides
access to SWE and snow depth.


| Parameters                                    | Available | Units                     | Product Code         | Description                                                 | Variable Type |
|-----------------------------------------------|-----------|---------------------------|----------------------|-------------------------------------------------------------|---------------|
| SWE                                           | Yes       | mm (metric); in (english) | 1034                 | Snapshot at 06:00 UTC                                       | State         |
| Snow Depth                                    | Yes       | mm (metric); in (english) | 1036                 | Snapshot at 06:00 UTC                                       | State         |
| Snow Melt Runoff at the Base of the Snow Pack | No        | NA                        | 1044                 | Total of 24 per hour melt rates, 06:00 UTC-06:00 UTC        | Diagnostic    |
| Sublimation from the Snow Pack                | No        | NA                        | 1050                 | Total of 24 per hour sublimation rates, 06:00 UTC-06:00 UT  | Diagnostic    |
| Sublimation of Blowing Snow                   | No        | NA                        | 1039                 | Total of 24 per hour sublimation rates, 06:00 UTC-06:00 UTC | Diagnostic    |
| Solid Precipitation                           | No        | NA                        | 1025 (v code = IL01) | 24 hour total, 06:00 UTC-06:00 UTC                          | Driving       |
| Liquid Precipitation                          | No        | NA                        | 1025 (v code = IL00) | 24 hour total, 06:00 UTC-06:00 UTC                          | Driving       |
| Snow Pack Average Temperature                 | No        | NA                        | 1038                 |                                                             | State         |

Additional
data description can be found at the NSIDC data page and in
[Carroll et al. (2001)](https://www.nohrsc.noaa.gov/technology/pdf/wsc2001.pdf).
