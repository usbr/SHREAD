---
layout: default
title: Overview
nav_order: 1
---

## SHREAD Documentation

The Snow-Hydrology Repo for Evaluation, Analysis, and Decision-making or SHREAD
is a snow data product aquisition and processing tool that provides access to
snow data products and related data products to support hydrologic research and
water management. The tool locally downloads datasets and consistently formats
data to allow for data intercomparison, visualization, and use in modeling tools.

SHREAD is a command-line tool written in Python. Inputs are provided by the user
at the command-line, and in a configuration file, to specify data products to
retrieve, data formatting, and desired output products.

### Getting Started

To get started, clone the SHREAD repository or download a zip file of the
repository.

SHREAD requires Python 3.x, along with a set of Python packages. Users can use
any installation of Python 3.x presuming that they can install the [required
packages](https://raw.githubusercontent.com/usbr/SHREAD/master/environment.yml).

We recommend installing [Anaconda](https://www.anaconda.com/products/individual)
and using the provided [environment file](https://raw.githubusercontent.com/usbr/SHREAD/master/environment.yml)
to create a virtual environment for running SHREAD.

From an Anaconda prompt navigate to the SHREAD directory and run:
```
conda env create -f environment.yml
```

This will create a virtual environment named *shread* that contains all of the
required packages.

Activate the *shread* virtual environment:

```
conda activate shread
```
