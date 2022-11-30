"""
Name: shread.py
Author: Dan Broman, Reclamation Technical Service Center
Description: Utilities for downloading and processing snow products
ADD CLASSES / FUNCTIONS DEFINED
ADD WHA
"""

import ftplib
import os
import tarfile
import gzip
from osgeo import gdal
import csv
import logging
import glob
from osgeo import osr
import zipfile
from osgeo import ogr
import fileinput
import datetime as dt
import configparser
import sys
import argparse
import urllib.request
import requests
from requests.auth import HTTPDigestAuth
import time
import geojson
import json
import rasterstats
from rasterstats import zonal_stats
import numpy as np
import pandas as pd
import geopandas as gpd
from lxml import etree
import fiona
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
from pyproj import Transformer
import base64
import itertools
import ssl
import pytz
import xarray as xr
import rioxarray
from tzlocal import get_localzone
import pygrib
import shutil
from joblib import Parallel,delayed

from getpass import getpass
try:
    from urllib.parse import urlparse
    from urllib.request import urlopen, Request, build_opener, HTTPCookieProcessor
    from urllib.error import HTTPError, URLError
except ImportError:
    from urlparse import urlparse
    from urllib2 import urlopen, Request, HTTPError, URLError, build_opener, HTTPCookieProcessor

def main(config_path, start_date, end_date, time_int, prod_str):
    """SHREAD main function

    Parameters
    ---------
    config_path : string
        relative file path to config file
    start_date : string
        first day of data in %Y%m%d format
    end_date : string
        last day of data in %Y%m%d format
    time_int : string
        Accepted: any Python 'freq' arguments
            - D: day
            - W: week
            - M: month
            - Y, YS: year
            - SM, SMS
            - twoweekstart

    prod_str : string
        comma separated list of products
        Accepted:
            - snodas
            - srpt
            - modscag
            - modis # not yet supported

    Returns
    -------
    None

    Notes
    -----
    -i, --ini, config_path : INI file path
    -s, --start : start date in %Y%m%d format
    -e, --end : end date in %Y%m%d format
    -t, --time : time interval
    -p, --prod : product list

    """

    # read config file
    cfg = config_params()
    cfg.read_config(config_path)
    cfg.proc_config()
    # develop date list
    start_date = dt.datetime.strptime(start_date, '%Y%m%d')
    end_date = dt.datetime.strptime(end_date, '%Y%m%d')

    date_list = pd.date_range(start_date, end_date, freq=time_int).tolist()
    # create list of products
    prod_list = prod_str.split(',')

    # download data

    # snodas
    
     # snodas function
    def snodas_func(date_dn,cfg=cfg,logger=logger):
        error_flag = False
        print(f"trying {date_dn}")
        try:
            download_snodas(cfg, date_dn)
        except:
            logger.info("download_ndfd: error downloading ndfd {} for '{}'".format(parameter))
            error_flag = True
            print("fail!")
        if error_flag is False:
            try:
                org_snodas(cfg, date_dn)
            except:
                logger.info("org_snodas: error processing snodas for '{}'".format(date_dn))

    if 'snodas' in prod_list:
        Parallel(n_jobs=6)(delayed(snodas_func)(d) for d in date_list)

    # srpt
    if 'srpt' in prod_list:
        for date_dn in date_list:
            error_flag = False
            try:
                download_srpt(cfg, date_dn)
            except:
                logger.info("download_srpt: error downloading srpt for '{}'".format(date_dn))
                error_flag = True
            if error_flag is False:
                try:
                    org_srpt(cfg, date_dn)
                except:
                    logger.info("org_srpt: error processing srpt for '{}'".format(date_dn))
    # modscag
    if 'modscag' in prod_list:
        for date_dn in date_list:
            error_flag = False
            try:
                download_modscag(cfg, date_dn)
            except:
                logger.info("download_modscag: error downloading modscag for '{}'".format(date_dn))
                error_flag = True
            if error_flag is False:
                try:
                    org_modscag(cfg, date_dn)
                except:
                    logger.info("org_modscag: error processing modscag for '{}'".format(date_dn))

    # moddrfs
    if 'moddrfs' in prod_list:
        for date_dn in date_list:
            error_flag = False
            try:
                download_moddrfs(cfg, date_dn)
            except:
                logger.info("download_moddrfs: error downloading moddrfs for '{}'".format(date_dn))
                error_flag = True
            if error_flag is False:
                try:
                    org_moddrfs(cfg, date_dn)
                except:
                    logger.info("org_moddrfs: error processing moddrfs for '{}'".format(date_dn))

    # modis
    if 'modis' in prod_list:
        for date_dn in date_list:
            error_flag = False
            try:
                download_modis(cfg, date_dn)
            except:
                logger.info("download_modis: error downloading modis for '{}'".format(date_dn))
                error_flag = True
            # if error_flag is False:
            #     try:
            #         org_modis(cfg, date_dn)
            #     except:
            #         logger.info("org_modis: error processing modis for '{}'".format(date_dn))
    # swann
    if 'swann' in prod_list:
        try:
            batch_swann(cfg, date_list, time_int)
        except:
            logger.info("batch_swann: error downloading swann")

    # ndfd
    
    # ndfd function
    def ndfd_func(parameter,flen=3,crs_out=cfg.proj,cfg=cfg,overwrite_flag=False,logger=logger):

        print(f"trying {parameter}")
        try:
            # forecast length hard-coded to 3 for now # TJC changed from 7 to 3.
            download_ndfd(parameter,flen,crs_out,cfg,overwrite_flag=False)
        except:
            logger.info("download_ndfd: error downloading ndfd {} for '{}'".format(parameter))
            error_flag = True
            print("fail!")

    
    if 'ndfd' in prod_list:
        import_flag = True
        for date_dn in date_list:
            print(date_dn)
            error_flag = False
            if import_flag:
                Parallel(n_jobs=6)(delayed(ndfd_func)(p) for p in cfg.ndfd_parameters)
                import_flag = False
            else:
                print("Importing ndfd only once...skipping")

def parse_args():
    parser = argparse.ArgumentParser(
        description=' SHREAD',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-i', '--ini', metavar='PATH',
        type=lambda x: is_valid_file(parser, x), help='Input file')
    parser.add_argument(
        '-s', '--start', metavar='start_date', help='start date')
    parser.add_argument(
        '-e', '--end', metavar='end_date', help='end date')
    parser.add_argument(
        '-t', '--time', metavar='time_interval', help='time interval')
    parser.add_argument(
        '-p', '--prod', metavar='product_list', help='product list')
    args = parser.parse_args()
    return args

def is_valid_file(parser, arg):
    if not os.path.isfile(arg):
        parser.error('The file {} does not exist!'.format(arg))
    else:
        return arg

logger = logging.getLogger(__name__)

class config_params:
    """config params container

    Attributes
    ----------

    """

    def __init__(self):
        """ """

    def __str__(self):
        """ """
        return '<config_params>'

    def read_config(self, config_path):
        """Read and parse config file

        Parameters
        ---------
        config_path : string
            relative file path to config file

        Returns
        -------
        None

        Notes
        -----

        """
        config = configparser.RawConfigParser()
        error_flag = False
        error_wd_sec_flag = False
        error_earthdata_sec_flag = False
        error_snodas_sec_flag = False
        error_modis_sec_flag = False
        error_nohrsc_sec_flag = False
        error_jpl_sec_flag = False
        error_swann_sec_flag = False
        error_noaa_sec_flag = False
        try:
            config.read_file(open(config_path))
            logger.info("read_config: reading config file '{}'".format(config_path))
        except:
            logger.error("read_config: config file could not be read, " +
                          "is not an input file, or does not exist")
            error_flag = True

        # check that all sections are present
        wd_sec = "wd"
        earthdata_sec = "earthdata"
        snodas_sec = "snodas"
        modis_sec = "modis"
        nohrsc_sec = "nohrsc"
        jpl_sec = "jpl"
        noaa_sec = "noaa"
        swann_sec = "swann"
        # ADD SECTIONS AS NEW SNOW PRODUCTS ARE ADDED

        cfg_secs = config.sections()

        # verify existence of common required sections
        if wd_sec not in cfg_secs:
            logger.error(
                    "read_config: config file missing [{}] section".format(wd_sec))
            error_flag = True
            error_wd_sec_flag = True

        if earthdata_sec not in cfg_secs:
            logger.error(
                    "read_config: config file missing [{}] section".format(earthdata_sec))
            error_flag = True
            error_earthdata_sec_flag = True

        if snodas_sec not in cfg_secs:
            logger.error(
                    "read_config: config file missing [{}] section".format(snodas_sec))
            error_flag = True
            error_snodas_sec_flag = True

        if modis_sec not in cfg_secs:
            logger.error(
                    "read_config: config file missing [{}] section".format(modis_sec))
            error_flag = True
            error_modis_sec_flag = True

        if nohrsc_sec not in cfg_secs:
            logger.error(
                    "read_config: config file missing [{}] section".format(nohrsc_sec))
            error_flag = True
            error_nohrsc_sec_flag = True

        if jpl_sec not in cfg_secs:
            logger.error(
                    "read_config: config file missing [{}] section".format(jpl_sec))
            error_flag = True
            error_jpl_sec_flag = True

        # read file
        # wd section
        if error_wd_sec_flag == False:
            logger.info("[wd]")
            #- dir_work
            try:
                self.dir_work = config.get(wd_sec, "dir_work")
                logger.info("read config: reading 'dir_work' {}".format(self.dir_work))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("dir_work", wd_sec))
                error_flag = True

            #- dir_db
            try:
                self.dir_db = config.get(wd_sec, "dir_db")
                logger.info("read config: reading 'dir_db' {}".format(self.dir_db))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("dir_db", wd_sec))
                error_flag = True

            #- dir_arch
            try:
                self.dir_arch = config.get(wd_sec, "dir_arch")
                logger.info("read config: reading 'dir_arch' {}".format(self.dir_arch))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("dir_arch", wd_sec))
                error_flag = True

            #- arch_flag
            try:
                self.arch_flag = config.get(wd_sec, "arch_flag")
                logger.info("read config: reading 'arch_flag' {}".format(self.arch_flag))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("arch_flag", wd_sec))
                error_flag = True

            #- proj
            try:
                self.proj = config.get(wd_sec, "proj")
                logger.info("read config: reading 'proj' {}".format(self.proj))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("proj", wd_sec))
                error_flag = True

            #- null_value
            try:
                self.null_value = config.get(wd_sec, "null_value")
                logger.info("read config: reading 'null_value' {}".format(self.null_value))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("null_value", wd_sec))
                error_flag = True
                self.null_value = int(cfg.null_value)

            #- unit_sys
            try:
                self.unit_sys = config.get(wd_sec, "unit_sys")
                logger.info("read config: reading 'unit_sys' {}".format(self.unit_sys))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("unit_sys", wd_sec))
                error_flag = True

            #- gdal_path
            try:
                self.gdal_path = config.get(wd_sec, "gdal_path")
                logger.info("read config: reading 'gdal_path' {}".format(self.gdal_path))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("gdal_path", wd_sec))
                error_flag = True

            #- basin_poly_path
            try:
                self.basin_poly_path = config.get(wd_sec, "basin_poly_path")
                logger.info("read config: reading 'basin_poly_path' {}".format(self.basin_poly_path))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("basin_poly_path", wd_sec))
                error_flag = True

            #- basin_points_path
            try:
                self.basin_points_path = config.get(wd_sec, "basin_points_path")
                logger.info("read config: reading 'basin_points_path' {}".format(self.basin_points_path))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("basin_points_path", wd_sec))
                error_flag = True

            #- outut_type
            try:
                self.output_type = config.get(wd_sec, "output_type")
                logger.info("read config: reading 'output_type' {}".format(self.output_type))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("output_type", wd_sec))
                error_flag = True

            #- output_format
            try:
                self.output_format = config.get(wd_sec, "output_format")
                logger.info("read config: reading 'output_format' {}".format(self.output_format))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("output_format", wd_sec))
                error_flag = True

        # earthdata section
        logger.info("[earthdata]")
        if error_earthdata_sec_flag == False:
            #- host_nohrsc
            try:
                self.username_earthdata = config.get(earthdata_sec, "username_earthdata")
                logger.info("read config: reading 'username_earthdata' {}".format(self.username_earthdata))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("username_earthdata", earthdata_sec))
                error_flag = True

            #- password_earthdata
            try:
                self.password_earthdata = config.get(earthdata_sec, "password_earthdata")
                logger.info("read config: reading 'password_earthdata'")
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("password_earthdata", earthdata_sec))
                error_flag = True

        # snodas section
        logger.info("[snodas]")
        if error_snodas_sec_flag == False:
            #- host_snodas
            try:
                self.host_snodas = config.get(snodas_sec, "host_snodas")
                logger.info("read config: reading 'host_snodas' {}".format(self.host_snodas))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("host_snodas", snodas_sec))
                error_flag = True

            #- username_snodas
            try:
                self.username_snodas = config.get(snodas_sec, "username_snodas")
                logger.info("read config: reading username_snodas {}".format(self.username_snodas))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("username_snodas", snodas_sec))
                error_flag = True

            #- password_snodas
            try:
                self.password_snodas = config.get(snodas_sec, "password_snodas")
                logger.info("read config: reading password_snodas {}".format(self.password_snodas))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("password_snodas", snodas_sec))
                error_flag = True

            #- dir_ftp_snodas
            try:
                self.dir_ftp_snodas = config.get(snodas_sec, "dir_ftp_snodas")
                logger.info("read config: reading host_snodas {}".format(self.dir_ftp_snodas))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("dir_ftp_snodas", snodas_sec))
                error_flag = True

            #- null_value_snodas
            try:
                self.null_value_snodas = config.get(snodas_sec, "null_value_snodas")
                logger.info("read config: reading host_snodas {}".format(self.null_value_snodas))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("null_value_snodas", snodas_sec))
                error_flag = True

        # modis section
        # logger.info("[modis]")
        # if error_modis_sec_flag == False:
        #     #- host_modis
        #     try:
        #         self.host_modis = config.get(modis_sec, "host_modis")
        #         logger.info("read config: reading 'host_modis' {}".format(self.modis_sec))
        #     except:
        #         logger.error("read_config: '{}' missing from [{}] section".format("host_modis", modis_sec))
        #         error_flag = True
        #
        #     #- dir_http_aqua
        #     try:
        #         self.dir_http_aqua = config.get(modis_sec, "dir_http_aqua")
        #         logger.info("read config: reading 'dir_http_aqua {}'".format(self.dir_http_aqua))
        #     except:
        #         logger.error("read_config: '{}' missing from [{}] section".format("dir_http_aqua", modis_sec))
        #         error_flag = True
        #
        #     #- dir_http_terra
        #     try:
        #         self.dir_http_terra = config.get(modis_sec, "dir_http_terra")
        #         logger.info("read config: reading 'dir_http_terra {}'".format(self.dir_http_terra))
        #     except:
        #         logger.error("read_config: '{}' missing from [{}] section".format("dir_http_terra", modis_sec))
        #         error_flag = True

        # nohrsc section
        logger.info("[nohrsc]")
        if error_nohrsc_sec_flag == False:
            #- host_nohrsc
            try:
                self.host_nohrsc = config.get(nohrsc_sec, "host_nohrsc")
                logger.info("read config: reading 'host_nohrsc' {}".format(self.host_nohrsc))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("host_nohrsc", nohrsc_sec))
                error_flag = True

            #- dir_http_srpt
            try:
                self.dir_http_srpt = config.get(nohrsc_sec, "dir_http_srpt")
                logger.info("read config: reading 'dir_http_srpt' {}".format(self.dir_http_srpt))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("dir_http_srpt", nohrsc_sec))
                error_flag = True

            #- dir_http_nsa
            try:
                self.dir_http_nsa = config.get(nohrsc_sec, "dir_http_nsa")
                logger.info("read config: reading 'dir_http_nsa' {}".format(self.dir_http_nsa))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("dir_http_nsa", nohrsc_sec))
                error_flag = True

            #- srpt_flag
            try:
                self.srpt_flag = config.get(nohrsc_sec, "srpt_flag")
                logger.info("read config: reading 'srpt_flag' {}".format(self.srpt_flag))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("srpt_flag", nohrsc_sec))
                error_flag = True

        # jpl section
        logger.info("[jpl]")
        if error_jpl_sec_flag == False:
            #- host_jpl
            try:
                self.host_jpl = config.get(jpl_sec, "host_jpl")
                logger.info("read config: reading 'host_jpl' {}".format(self.host_jpl))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("host_jpl", jpl_sec))
                error_flag = True

            #- username_jpl
            try:
                self.username_jpl = config.get(jpl_sec, "username_jpl")
                logger.info("read config: reading 'username_jpl' {}".format(self.username_jpl))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("username_jpl", jpl_sec))
                error_flag = True

            #- password_jpl
            try:
                self.password_jpl = config.get(jpl_sec, "password_jpl")
                logger.info("read config: reading 'password_jpl'")
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("password_jpl", jpl_sec))
                error_flag = True

            #- dir_http_modscag
            try:
                self.dir_http_modscag = config.get(jpl_sec, "dir_http_modscag")
                logger.info("read config: reading 'dir_http_modscag' {}".format(self.dir_http_modscag))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("dir_http_modscag", jpl_sec))
                error_flag = True

            #- dir_http_moddrfs
            try:
                self.dir_http_moddrfs = config.get(jpl_sec, "dir_http_moddrfs")
                logger.info("read config: reading 'dir_http_moddrfs' {}".format(self.dir_http_moddrfs))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("dir_http_moddrfs", jpl_sec))
                error_flag = True

            #- ssl_verify
            try:
                self.ssl_verify = config.get(jpl_sec, "ssl_verify")
                self.ssl_verify = str2bool(self.ssl_verify)
                logger.info("read config: reading 'ssl_verify' {}".format(self.ssl_verify))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("ssl_verify", jpl_sec))
                error_flag = True

        # noaa section
        logger.info("[noaa]")
        if error_noaa_sec_flag == False:

            #- host_noaa
            try:
                self.host_ndfd = config.get(noaa_sec, "host_ndfd")
                logger.info("read_config")
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("host_ndfd", noaa_sec))
                error_flag = True
            #- ndfd_parameters
            try:
                self.ndfd_parameters = config.get(noaa_sec, "ndfd_parameters")
                self.ndfd_parameters = self.ndfd_parameters.split(',')
                logger.info("read_config")
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("ndfd_parameters", noaa_sec))
                error_flag = True

        # swann section
        logger.info("[swann]")
        if error_swann_sec_flag == False:
            #- host_ua
            try:
                self.host_ua = config.get(swann_sec, "host_ua")
                logger.info("read config: reading 'host_ua' {}".format(self.host_ua))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("host_ua", swann_sec))
                error_flag = True

            #- dir_ftp_swann_arc
            try:
                self.dir_ftp_swann_arc = config.get(swann_sec, "dir_ftp_swann_arc")
                logger.info("read config: reading 'dir_ftp_swann_arc' {}".format(self.dir_ftp_swann_arc))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("dir_ftp_swann_arc", swann_sec))
                error_flag = True

            #- dir_ftp_swann_rt
            try:
                self.dir_ftp_swann_rt = config.get(swann_sec, "dir_ftp_swann_rt")
                logger.info("read config: reading 'dir_ftp_swann_rt' {}".format(self.dir_ftp_swann_rt))
            except:
                logger.error("read_config: '{}' missing from [{}] section".format("dir_ftp_swann_rt", swann_sec))
                error_flag = True
            if error_flag == True:
                sys.exit()

    def proc_config(self):
        """Read and parse config file

        Parameters
        ---------
        config_path : string
            relative file path to config file

        Returns
        -------
        None

        Notes
        -----

        """

        # add gdal path
        sys.path.append(self.gdal_path)
        # add error checking

        # open basin_poly
        self.basin_poly = gpd.read_file(self.basin_poly_path)

        # open basin points
        if 'points' in self.output_type:
            self.basin_points = gpd.read_file(self.basin_points_path)

        # find bounding box for basin_poly
        self.basin_poly_bbox_raw = self.basin_poly.total_bounds.tolist()

        # if basin poly is not in EPSG:4326'convert bounding box coordinates
        if self.proj != 'EPSG:4326':
            transformer = Transformer.from_crs(self.proj, 'EPSG:4326')
            xmin,ymin = transformer.transform(self.basin_poly_bbox_raw[0], self.basin_poly_bbox_raw[1])
            xmax,ymax = transformer.transform(self.basin_poly_bbox_raw[2], self.basin_poly_bbox_raw[3])
            self.basin_poly_bbox = [xmin,ymin,xmax,ymax]
        elif self.proj == 'EPSG:4326':
            self.basin_poly_bbox = self.basin_poly_bbox_raw

        # find modis sinusodial grid tiles overlapping basin_poly
        self.singrd_tile_list = find_tiles(self.basin_poly_bbox)

def download_snodas(cfg, date_dn, overwrite_flag = False):
    """Download snodas zip

    Parameters
    ---------
        cfg ():
            config_params Class object
        date_dn: datetime
            date
        overwrite_flag: boolean
            True : overwrite existing files

    Returns
    -------
        None

    """
    site_url = cfg.host_snodas + cfg.dir_ftp_snodas
    dir_work_snodas = cfg.dir_work + 'snodas/'
    zip_name = "SNODAS_" + ("{}.tar".format(date_dn.strftime('%Y%m%d')))
    zip_url = site_url + date_dn.strftime('%Y') + "/" + date_dn.strftime('%m') + "_" \
    + date_dn.strftime('%b') + '/' + zip_name
    zip_path = dir_work_snodas + zip_name

    if not os.path.isdir(cfg.dir_work):
        os.makedirs(cfg.dir_work)

    if not os.path.isdir(dir_work_snodas):
        os.makedirs(dir_work_snodas)

    if os.path.isfile(zip_path) and overwrite_flag:
        os.remove(zip_path)
    if not os.path.isfile(zip_path):
        logger.info("download_snodas: downloading {}".format(date_dn.strftime('%Y-%m-%d')))
        logger.info("download_snodas: downloading from {}".format(zip_url))
        logger.info("download_snodas: downloading to {}".format(zip_path))
        try:
            urllib.request.urlretrieve(zip_url, zip_path)
        except IOError as e:
            logger.error("download_snodas: error downloading {}".format(date_dn.strftime('%Y-%m-%d')))
            logging.error(e)

def org_snodas(cfg, date_dn):

    dir_work_snodas = cfg.dir_work + 'snodas/'
    dir_arch_snodas = cfg.dir_arch + 'snodas/'
    date_str = str(date_dn.strftime('%Y%m%d'))
    chr_rm = [":"]
    proj_str = ''.join(i for i in cfg.proj if not i in chr_rm)
    crs_raw = 'EPSG:4326'
    dtype_out = 'float64' # convert snodas raster from int16 to float64 to perform
        # unit conversion.
    basin_str = os.path.splitext(os.path.basename(cfg.basin_poly_path))[0]

    # clean up working directory
    for file in glob.glob("{0}/*.tif".format(dir_work_snodas)):
        file_path = dir_work_snodas + file
        try:
            os.remove(file_path)
            logger.info("org_snodas: removing {}".format(file_path))
        except:
            logger.error("org_snodas: error removing {}".format(file_path))

    # untar files
    zip_name = "SNODAS_" + ("{}.tar".format(date_dn.strftime('%Y%m%d')))
    zip_path = dir_work_snodas + zip_name
    zip_arch = dir_arch_snodas + zip_name

    try:
        tar_con = tarfile.open(zip_path)
        tar_con.extractall(path=dir_work_snodas)
        tar_con.close()
        logger.info("org_snodas: untaring {0}".format(zip_path))
    except:
        logger.error("download_snodas: error untaring {0}".format(zip_path))
    if cfg.arch_flag == True:
        os.rename(zip_path, zip_arch)
        logger.info("org_snodas: archiving {0} to {1}".format(zip_path, zip_arch))
    else:
        os.remove(zip_path)
        logger.info("org_snodas: removing {0}".format(zip_path))

    # ungz files
    for file_gz in os.listdir(dir_work_snodas):
        if file_gz.endswith('.gz'):
            file_path = dir_work_snodas + file_gz
            file_out = os.path.splitext(file_path)[0]

            # currently only keeping swe (1034) and snow depth (1036)
            if '1034' in str(file_gz) or '1036' in str(file_gz):
                try:
                    gz_con = gzip.GzipFile(file_path, 'rb')
                    gz_in = gz_con.read()
                    gz_con.close()

                    gz_out = open(file_out, 'wb')
                    gz_out.write(gz_in)
                    gz_out.close()

                    logger.info("org_snodas: unzipping {}".format(file_path))
                    os.remove(file_path)
                    logger.info("org_snodas: removing {}".format(file_path))
                except:
                    logger.error("org_snodas: error unzipping {}".format(file_path))
            else:
                os.remove(file_path)
                logger.info("org_snodas: removing {}".format(file_path))

    # convert dat to bil
    for file_dat in os.listdir(dir_work_snodas):
        if file_dat.endswith('.dat'):
            try:
                file_path = dir_work_snodas + file_dat
                file_out = file_path.replace('.dat', '.bil')
                os.rename(file_path, file_out)
                logger.info("org_snodas: converting {} to {}".format(file_path, file_out))
            except:
                logger.error("org_snodas: error converting {} to {}".format(file_path, file_out))

    # create header file - ADD NSIDC LINK
    for file_bil in os.listdir(dir_work_snodas):
        if file_bil.endswith('.bil'):
            try:
                file_path = dir_work_snodas + file_bil
                file_out = file_path.replace('.bil', '.hdr')
                file_con = open(file_out, 'w')

                file_con.write('units dd\n')
                file_con.write('nbands 1\n')
                file_con.write('nrows 3351\n')
                file_con.write('ncols 6935\n')
                file_con.write('nbits 16\n')
                file_con.write('pixeltype signedint')
                file_con.write('byteorder M\n')
                file_con.write('layout bil\n')
                file_con.write('ulxmap -124.729583333333\n')
                file_con.write('ulymap 52.8704166666666\n')
                file_con.write('xdim 0.00833333333333333\n')
                file_con.write('ydim 0.00833333333333333\n')
                file_con.close()

                logger.info("org_snodas: creating header file {}".format(file_out))
            except:
                logger.error("org_snodas: error creating header file {}".format(file_out))

    # convert bil to geotif
    for file_bil in os.listdir(dir_work_snodas):
        if file_bil.endswith('.bil'):
            try:
                file_path = dir_work_snodas + file_bil
                file_out = file_path.replace('.bil', '.tif')
                gdal.Translate(file_out, file_path, format = 'GTiff')
                logger.info("org_snodas: converting {} to {}".format(file_path, file_out))
            except:
                logger.error("org_snodas: error converting {} to {}".format(file_path, file_out))

    # remove unneeded files
    for file in os.listdir(dir_work_snodas):
        if not file.endswith('.tif'):
            file_path = dir_work_snodas + file
            try:
                os.remove(file_path)
                logger.info("org_snodas: removing {}".format(file_path))
            except:
                logger.error("org_snodas: error removing {}".format(file_path))

    # reproject geotif
    tif_list = glob.glob("{0}/*{1}{2}*.tif".format(dir_work_snodas, date_str, "05"))
    for tif in tif_list:
        tif_out = os.path.splitext(tif)[0] + "_" + proj_str + ".tif"
        try:
            gdal_raster_reproject(tif, tif_out, cfg.proj, crs_raw)
            # rasterio_raster_reproject(tif, tif_out, cfg.proj)
            logger.info("org_snodas: reprojecting {} to {}".format(tif, tif_out))
        except:
            logger.error("org_snodas: error reprojecting {} to {}".format(tif, tif_out))
    if not tif_list:
        logger.error("org_snodas: error finding tifs to reproject")

    # clip to basin polygon
    tif_list = glob.glob("{0}/*{1}{2}*{3}.tif".format(dir_work_snodas, date_str, "05", proj_str))
    for tif in tif_list:
        tif_out = os.path.splitext(tif)[0] + "_" + basin_str + ".tif"
        try:
            gdal_raster_clip(cfg.basin_poly_path, tif, tif_out, cfg.proj, cfg.proj, -9999)
            logger.info("org_snodas: clipping {} to {}".format(tif, tif_out))
        except:
            logger.error("org_snodas: error clipping {} to {}".format(tif, tif_out))
    if not tif_list:
        logger.error("org_snodas: error finding tifs to clip")

    # convert units
    if cfg.unit_sys == 'english':
        calc_exp = '(+ 1 (* .0393701 (read 1)))' # inches
    if cfg.unit_sys == 'metric':
        calc_exp = '(read 1)' # keep units in mm
    # SWE
    tif_list = glob.glob("{0}/*{1}*{2}{3}*{4}*{5}.tif".format(dir_work_snodas, '1034', date_str, "05", proj_str, basin_str))

    for tif in tif_list:
        tif_int = os.path.splitext(tif)[0] + "_" + dtype_out + ".tif"
        tif_out = cfg.dir_db + "snodas_swe_" + date_str + "_" + basin_str + "_" + cfg.unit_sys + ".tif"
        try:
            rio_dtype_conversion(tif, tif_int, dtype_out)
            rio_calc(tif_int, tif_out, calc_exp)
            logger.info("org_snodas: calc {} {} to {}".format(calc_exp, tif, tif_out))
        except:
            logger.error("org_snodas: error calc {} to {}".format(tif, tif_out))
    if not tif_list:
        logger.error("org_snodas: error finding tifs to calc")

    # Snow Depth
    tif_list = glob.glob("{0}/*{1}*{2}{3}*{4}*{5}.tif".format(dir_work_snodas, '1036', date_str, "05", proj_str, basin_str))

    for tif in tif_list:
        tif_int = os.path.splitext(tif)[0] + "_" + dtype_out + ".tif"
        tif_out = cfg.dir_db + "snodas_snowdepth_" + date_str + "_" + basin_str + "_" + cfg.unit_sys + ".tif"
        try:
            rio_dtype_conversion(tif, tif_int, dtype_out)
            rio_calc(tif_int, tif_out, calc_exp)
            logger.info("org_snodas: calc {} {} to {}".format(calc_exp, tif, tif_out))
        except:
            logger.error("org_snodas: error calc {} to {}".format(tif, tif_out))
    if not tif_list:
        logger.error("org_snodas: error finding tifs to calc")

# swe : 1034 [m *1000]
# snow depth : 1036 [m *1000]
# snow melt runoff at base of snowpack : 1044 [m *100,000]
# sublimation from snowpack : 1050 [m *100,000]
# sublimation of blowing snow: 1039 [m *100,000]
# solid precipitation: 1025(v code = IL01) [kg m-2 *10]
# liquid precipitation: 1025(v code = IL00) [kg m-2 *10]
# snowpack average temperature: 1038 [K *1]

    # calculate zonal statistics and export data
    tif_list = glob.glob("{0}/{1}*{2}*{3}*{4}.tif".format(cfg.dir_db, 'snodas', date_str, basin_str, cfg.unit_sys))

    for tif in tif_list:
        file_meta = os.path.basename(tif).replace('.', '_').split('_')

        if 'poly' in cfg.output_type:
            try:
                tif_stats = zonal_stats(cfg.basin_poly_path, tif, stats=['min', 'max', 'median', 'mean'], all_touched=True)
                tif_stats_df = pd.DataFrame(tif_stats)
                logger.info("org_snodas: computing zonal statistics")
            except:
                logger.error("org_snodas: error computing poly zonal statistics")
            try:
                frames = [cfg.basin_poly, tif_stats_df]
                basin_poly_stats = pd.concat(frames, axis=1)
                logger.info("org_snodas: merging poly zonal statistics")
            except:
                logger.error("org_snodas: error merging zonal statistics")

            if 'geojson' in cfg.output_format:
                try:
                    geojson_out = os.path.splitext(tif)[0] + "_poly.geojson"
                    basin_poly_stats.to_file(geojson_out, driver='GeoJSON')
                    logger.info("org_snodas: writing {0}".format(geojson_out))
                except:
                    logger.error("org_snodas: error writing {0}".format(geojson_out))
            if 'csv' in cfg.output_format:
                try:
                    csv_out = os.path.splitext(tif)[0] + "_poly.csv"
                    basin_poly_stats_df = pd.DataFrame(basin_poly_stats.drop(columns = 'geometry'))
                    basin_poly_stats_df.insert(0, 'Source', file_meta[0])
                    basin_poly_stats_df.insert(0, 'Type', file_meta[1])
                    basin_poly_stats_df.insert(0, 'Date', dt.datetime.strptime(file_meta[2], '%Y%m%d').strftime('%Y-%m-%d %H:%M'))
                    basin_poly_stats_df.to_csv(csv_out, index=False)
                    logger.info("org_snodas: writing {0}".format(csv_out))
                except:
                    logger.error("org_snodas: error writing {0}".format(csv_out))

        if 'points' in cfg.output_type:
            try:
                tif_stats = zonal_stats(cfg.basin_points_path, tif, stats=['min', 'max', 'median', 'mean'], all_touched=True)
                tif_stats_df = pd.DataFrame(tif_stats)
                logger.info("org_snodas: computing points zonal statistics")
            except:
                logger.error("org_snodas: error computing points zonal statistics")
            try:
                frames = [cfg.basin_points, tif_stats_df]
                basin_points_stats = pd.concat(frames, axis=1)
                logger.info("org_snodas: merging zonal statistics")
            except:
                logger.error("org_snodas: error merging zonal statistics")
            if 'geojson' in cfg.output_format:
                try:
                    geojson_out = os.path.splitext(tif)[0] + "_points.geojson"
                    basin_points_stats.to_file(geojson_out, driver='GeoJSON')
                    logger.info("org_snodas: writing {0}".format(geojson_out))
                except:
                    logger.error("org_snodas: error writing {0}".format(geojson_out))
            if 'csv' in cfg.output_format:
                try:
                    csv_out = os.path.splitext(tif)[0] + "_points.csv"
                    basin_points_stats_df = pd.DataFrame(basin_points_stats.drop(columns = 'geometry'))
                    basin_points_stats_df.insert(0, 'Source', file_meta[0])
                    basin_points_stats_df.insert(0, 'Type', file_meta[1])
                    basin_points_stats_df.insert(0, 'Date', dt.datetime.strptime(file_meta[2], '%Y%m%d').strftime('%Y-%m-%d %H:%M'))
                    basin_points_stats_df.to_csv(csv_out, index=False)
                    logger.info("org_snodas: writing {0}".format(csv_out, index=False))
                except:
                    logger.error("org_snodas: error writing {0}".format(csv_out))

    # clean up working directory
    for file in os.listdir(dir_work_snodas):
        file_path = dir_work_snodas + file
        try:
            os.remove(file_path)
            logger.info("org_snodas: removing {}".format(file_path))
        except:
            logger.error("org_snodas: error removing {}".format(file_path))


def download_srpt(cfg, date_dn, overwrite_flag = False):
    """Download snow reports from nohrsc

    Parameters
    ---------
        cfg ():
            config_params Class object
        date_dn: datetime
            date
        overwrite_flag: boolean
            True : overwrite existing files

    Returns
    -------
        None
    """

    site_url = cfg.host_nohrsc + cfg.dir_http_srpt + date_dn.strftime('%Y%m%d')
    dir_work_d = cfg.dir_work + 'srpt/'
    if not os.path.isdir(dir_work_d):
        os.makedirs(dir_work_d)

    # snow reports (stations)
    kmz_srpt_name = "snow_reporters_" + date_dn.strftime('%Y%m%d') + ".kmz"
    kmz_srpt_url = site_url + "/" + kmz_srpt_name
    kmz_srpt_path = dir_work_d + kmz_srpt_name

    if os.path.isfile(kmz_srpt_path) and overwrite_flag:
        os.remove(kmz_srpt_path)
    if os.path.isfile(kmz_srpt_path) and overwrite_flag == False:
        logger.info("download_srpt: skipping {} {}, {} exists".format('snow reports', date_dn.strftime('%Y-%m-%d'), kmz_srpt_path))
    if not os.path.isfile(kmz_srpt_path):
        logger.info("download_srpt: downloading {} {}".format('snow reports', date_dn.strftime('%Y-%m-%d')))
        logger.info("download_srpt: downloading from {}".format(kmz_srpt_url))
        logger.info("download_srpt: downloading to {}".format(kmz_srpt_path))
        try:
            urllib.request.urlretrieve(kmz_srpt_url, kmz_srpt_path)
        except IOError as e:
            logger.error("download_srpt: error downloading {} {}".format('snow reports', date_dn.strftime('%Y-%m-%d')))
            logging.error(e)

def org_srpt(cfg, date_dn):
    """Downloads daily snow reporters KMZ from NOHRSC
        formats to geoJSON and csv and clips to poly extents

    Parameters
    ---------
        cfg ():
            config_params Class object
        date_dn: datetime
            date

    Returns
    -------
        None

    Notes
    -----
        Writes out geoJSON and csv to file

    """
    # TODO - ADD ERROR CHECKING
    #  OPTION TO SPECIFY AN ALTERNATE BOUNDARY?
    # https://stackoverflow.com/questions/55586376/how-to-obtain-element-values-from-a-kml-by-using-lmxl

    basin_str = os.path.splitext(os.path.basename(cfg.basin_poly_path))[0]
    dir_work_srpt = cfg.dir_work + 'srpt/'

    # snow reports (stations)
    kmz_srpt_name = "snow_reporters_" + date_dn.strftime('%Y%m%d') + ".kmz"
    kmz_srpt_path = dir_work_srpt + kmz_srpt_name

    with zipfile.ZipFile(kmz_srpt_path,"r") as zip_ref:
        zip_ref.extractall(dir_work_srpt)

    kml_path = kmz_srpt_path.replace('.kmz', '.kml')

    gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'

    srpt_gpd = gpd.GeoDataFrame()
    # iterate over layers
    for layer in fiona.listlayers(kml_path):
        s = gpd.read_file(kml_path, driver='KML', layer=layer)
        srpt_gpd = srpt_gpd.append(s, ignore_index=True)
    ns = {"kml": "http://earth.google.com/kml/2.0"}

    # parse kmlfile
    tree = etree.parse(kml_path)

    # read name - character
    name_arr = []
    for simple_data in tree.xpath("/kml:kml/kml:Document/kml:Folder/kml:Placemark/kml:name", namespaces = ns):
        name_arr.append(simple_data.text)
    name_pd = pd.Series(name_arr, name = 'Name')

    # read beginDate - Datetime
    beginDate_arr = []
    for simple_data in tree.xpath("/kml:kml/kml:Document/kml:Folder/kml:Placemark/kml:ExtendedData/kml:Data[@name='beginDate']/kml:value", namespaces = ns):
        beginDate_arr.append(simple_data.text)
    beginDate_pd = pd.Series(beginDate_arr, name = 'beginDate')
    beginDate_pd = pd.to_datetime(beginDate_pd, format='%Y-%m-%d', errors='coerce')

    # read endDate - Datetime
    endDate_arr = []
    for simple_data in tree.xpath("/kml:kml/kml:Document/kml:Folder/kml:Placemark/kml:ExtendedData/kml:Data[@name='endDate']/kml:value", namespaces = ns):
        endDate_arr.append(simple_data.text)
    endDate_pd = pd.Series(endDate_arr, name = 'endDate')
    endDate_pd = pd.to_datetime(endDate_pd, format='%Y-%m-%d', errors='coerce')

    # read type - character
    type_arr = []
    for simple_data in tree.xpath("/kml:kml/kml:Document/kml:Folder/kml:Placemark/kml:ExtendedData/kml:Data[@name='type']/kml:value", namespaces = ns):
        type_arr.append(simple_data.text)
    type_pd = pd.Series(type_arr, name = 'type')

    # read elevationMeters - numeric
    elevationMeters_arr = []
    for simple_data in tree.xpath("/kml:kml/kml:Document/kml:Folder/kml:Placemark/kml:ExtendedData/kml:Data[@name='elevationMeters']/kml:value", namespaces = ns):
        elevationMeters_arr.append(simple_data.text)
    elevationMeters_pd = pd.Series(elevationMeters_arr, name = 'elevationMeters')
    elevationMeters_pd = pd.to_numeric(elevationMeters_pd, errors='coerce')

    # read latestSWEdateUTC - Datetime
    latestSWEdateUTC_arr = []
    for simple_data in tree.xpath("/kml:kml/kml:Document/kml:Folder/kml:Placemark/kml:ExtendedData/kml:Data[@name='latestSWEdateUTC']/kml:value", namespaces = ns):
        latestSWEdateUTC_arr.append(simple_data.text)
    latestSWEdateUTC_pd = pd.Series(latestSWEdateUTC_arr, name = 'latestSWEdateUTC')
    latestSWEdateUTC_pd = pd.to_datetime(latestSWEdateUTC_pd, format='%Y-%m-%d', errors='coerce')

    # read latestSWEcm - numeric
    latestSWEcm_arr = []
    for simple_data in tree.xpath("/kml:kml/kml:Document/kml:Folder/kml:Placemark/kml:ExtendedData/kml:Data[@name='latestSWEcm']/kml:value", namespaces = ns):
        latestSWEcm_arr.append(simple_data.text)
    latestSWEcm_pd = pd.Series(latestSWEcm_arr, name = 'latestSWEcm')
    latestSWEcm_pd = pd.to_numeric(latestSWEcm_pd, errors='coerce')

    # read latestDepthDateUTC - Datetime
    latestDepthDateUTC_arr = []
    for simple_data in tree.xpath("/kml:kml/kml:Document/kml:Folder/kml:Placemark/kml:ExtendedData/kml:Data[@name='latestDepthDateUTC']/kml:value", namespaces = ns):
        latestDepthDateUTC_arr.append(simple_data.text)
    latestDepthDateUTC_pd = pd.Series(latestDepthDateUTC_arr, name = 'latestDepthDateUTC')
    latestDepthDateUTC_pd = pd.to_datetime(latestDepthDateUTC_pd, format='%Y-%m-%d', errors='coerce')

    # read latestDepthCm - numeric
    latestDepthCm_arr = []
    for simple_data in tree.xpath("/kml:kml/kml:Document/kml:Folder/kml:Placemark/kml:ExtendedData/kml:Data[@name='latestDepthCm']/kml:value", namespaces = ns):
        latestDepthCm_arr.append(simple_data.text)
    latestDepthCm_pd = pd.Series(latestDepthCm_arr, name = 'latestDepthCm')
    latestDepthCm_pd = pd.to_numeric(latestDepthCm_pd, errors='coerce')

    srpt_pd = pd.concat([name_pd, beginDate_pd, endDate_pd, type_pd, elevationMeters_pd, latestSWEdateUTC_pd,
                   latestSWEcm_pd, latestDepthDateUTC_pd, latestDepthCm_pd], axis = 1)

    srpt_gpd = srpt_gpd.set_index('Name').join(srpt_pd.set_index('Name'))

    # unit conversion
    if cfg.unit_sys == 'english':
        srpt_gpd.loc[:, 'elevationFeet'] = srpt_gpd.loc[:, 'elevationMeters'].values * 3.28084
        srpt_gpd.loc[:, 'latestSWEin'] = srpt_gpd.loc[:, 'latestSWEcm'].values * 0.393701
        srpt_gpd.loc[:, 'latestDepthin'] = srpt_gpd.loc[:, 'latestDepthCm'].values * 0.393701
        srpt_gpd = srpt_gpd.drop(columns=['elevationMeters', 'latestSWEcm', 'latestDepthCm'])

    # clip to basin
    srpt_gpd_clip = gpd.clip(srpt_gpd.to_crs(cfg.proj), cfg.basin_poly, keep_geom_type = False)

    # write out data
    geojson_out = cfg.dir_db + 'snowreporters_obs_' + date_dn.strftime('%Y%m%d') + '_' + basin_str + '.geojson'
    srpt_gpd_clip.to_file(geojson_out, driver = 'GeoJSON')
    csv_out = cfg.dir_db + 'snowreporters_obs_' + date_dn.strftime('%Y%m%d') + '_' + basin_str + '.csv'
    srpt_gpd_clip_df = pd.DataFrame(srpt_gpd_clip.drop(columns = 'geometry'))
    srpt_gpd_clip_df.insert(1, 'Source', 'NOHRSCSnowReporters')
    srpt_gpd_clip_df.to_csv(csv_out, index=False)

    # clean up working directory
    for file in os.listdir(dir_work_srpt):
        file_path = dir_work_srpt + file
        try:
            os.remove(file_path)
            logger.info("org_srpt: removing {}".format(file_path))
        except:
            logger.error("org_srpt: error removing {}".format(file_path))

def download_nsa(cfg, date_dn, overwrite_flag = False):
    """Download national snow analysis from nohrsc

    Parameters
    ---------
        cfg ():
            config_params Class object
        date_dn: datetime
            date
        overwrite_flag: boolean
            True : overwrite existing files

    Returns
    -------
        None

    Notes
    -----
    Currently just downloads the 24hr product. 6hr, 48hr, 72hr, and 60day
    products are also available.

    24hr product is also avaiable twice a day, 00 and 12.

    """

    site_url = cfg.host_nohrsc + cfg.dir_http_nsa + date_dn.strftime('%Y%m')
    dir_work_d = cfg.dir_work + 'nsa/'
    if not os.path.isdir(dir_work_d):
        os.makedirs(dir_work_d)

    # snow covered area (sca)
    tif_24hr_name = "sfav2_CONUS_24h_" + date_dn.strftime('%Y%m%d') + "00.tif"
    tif_24hr_url = site_url + "/" + tif_24hr_name
    tif_24hr_path = dir_work_d + tif_24hr_name

    if os.path.isfile(tif_24hr_path) and overwrite_flag:
        os.remove(tif_24hr_path)
    if os.path.isfile(tif_24hr_path) and overwrite_flag == False:
        logger.info("download_nsa: skipping {} {}, {} exists".format('24hr', date_dn.strftime('%Y-%m-%d'), tif_24hr_path))
    if not os.path.isfile(tif_24hr_path):
        logger.info("download_nsa: downloading {} {}".format('24hr', date_dn.strftime('%Y-%m-%d')))
        logger.info("download_nsa: downloading from {}".format(tif_24hr_url))
        logger.info("download_nsa: downloading to {}".format(tif_24hr_path))
        try:
            urllib.request.urlretrieve(tif_24hr_url, tif_24hr_path)
        except IOError as e:
            logger.error("download_nsa: error downloading {} {}".format('24hr', date_dn.strftime('%Y-%m-%d')))
            logging.error(e)

def download_modscag(cfg, date_dn, overwrite_flag = False):
    """Download modscag from JPL

    Parameters
    ---------
        cfg ():
            config_params Class object
        date_dn: datetime
            date
        overwrite_flag: boolean
            True : overwrite existing files

    Returns
    -------
        None

    Notes
    -----
    Currently uses JPL data archive. May be moved in future.

    geotif specs - # work on this
        235 - cloud cover, guess
        250 - nodata, guess
    """

    site_url = cfg.host_jpl + cfg.dir_http_modscag + date_dn.strftime('%Y') + "/" + date_dn.strftime('%j')
    r = requests.get(site_url, auth=HTTPDigestAuth(cfg.username_jpl, cfg.password_jpl), verify=cfg.ssl_verify)
    if r.status_code == 200:
        dir_work_d = cfg.dir_work + 'modscag/'
        if not os.path.isdir(dir_work_d):
            os.makedirs(dir_work_d)

        # loop through modis sinusodial tiles
        for tile in cfg.singrd_tile_list:

            # snow fraction (fsca)
            tif_fsca_name = "MOD09GA.A" + date_dn.strftime('%Y') + date_dn.strftime('%j') + "." + tile + ".006.NRT.snow_fraction.tif"
            tif_fsca_url = site_url + "/" + tif_fsca_name
            tif_fsca_path = dir_work_d + tif_fsca_name

            if os.path.isfile(tif_fsca_path) and overwrite_flag:
                os.remove(tif_fsca_path)
            if os.path.isfile(tif_fsca_path) and overwrite_flag == False:
                logger.info("download_modscag: skipping {} {}, {} exists".format(date_dn.strftime('%Y-%m-%d'), tile, tif_fsca_path))
            if not os.path.isfile(tif_fsca_path):
                logger.info("download_modscag: downloading {} {} {}".format('snow_fraction', date_dn.strftime('%Y-%m-%d'), tile))
                logger.info("download_modscag: downloading from {}".format(tif_fsca_url))
                logger.info("download_modscag: downloading to {}".format(tif_fsca_path))
                try:
                    r = requests.get(tif_fsca_url, auth = HTTPDigestAuth(cfg.username_jpl, cfg.password_jpl), verify=cfg.ssl_verify)
                    if r.status_code == 200:
                        with open(tif_fsca_path, 'wb') as rfile:
                            rfile.write(r.content)
                    else:
                        logger.error("download_modscag: error downloading {} {} {}".format('snow_fraction', date_dn.strftime('%Y-%m-%d'), tile))
                except IOError as e:
                    logger.error("download_modscag: error downloading {} {} {}".format(date_dn.strftime('snow_fraction', '%Y-%m-%d'), tile))
                    logging.error(e)

                # vegetation fraction (vfrac)
                tif_vfrac_name = "MOD09GA.A" + date_dn.strftime('%Y') + date_dn.strftime('%j') + "." + tile + ".006.NRT.vegetation_fraction.tif"
                tif_vfrac_url = site_url + "/" + tif_vfrac_name
                tif_vfrac_path = dir_work_d + tif_vfrac_name

                if os.path.isfile(tif_vfrac_path) and overwrite_flag:
                    os.remove(tif_vfrac_path)
                if os.path.isfile(tif_vfrac_path) and overwrite_flag == False:
                    logger.info("download_modscag: skipping {} {}, {} exists".format(date_dn.strftime('%Y-%m-%d'), tile, tif_vfrac_path))
                if not os.path.isfile(tif_vfrac_path):
                    logger.info("download_modscag: downloading {} {} {}".format('vegetation_fraction', date_dn.strftime('%Y-%m-%d'), tile))
                    logger.info("download_modscag: downloading from {}".format(tif_vfrac_url))
                    logger.info("download_modscag: downloading to {}".format(tif_vfrac_path))
                    try:
                        r = requests.get(tif_vfrac_url, auth = HTTPDigestAuth(cfg.username_jpl, cfg.password_jpl), verify=cfg.ssl_verify)
                        if r.status_code == 200:
                            with open(tif_vfrac_path, 'wb') as rfile:
                                rfile.write(r.content)
                        else:
                            logger.error("download_modscag: error downloading {} {} {}".format('vegetation_fraction', date_dn.strftime('%Y-%m-%d'), tile))
                    except IOError as e:
                        logger.error("download_modscag: error downloading {} {} {}".format(date_dn.strftime('vegetation_fraction', '%Y-%m-%d'), tile))
                        logging.error(e)
    else:
        logger.error("download_modscag: error connecting {}".format(site_url))

def org_modscag(cfg, date_dn):
    """ Organize downloaded modscag data

    Parameters
    ---------
        cfg ():
            config_params Class object
        date_dn: datetime
            date

    Returns
    -------
        None

    Notes
    -----


    """

    # Proj4js.defs["SR-ORG:6974"] = "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs";
    dir_work_modscag = cfg.dir_work + 'modscag/'
    dir_arch_modscag = cfg.dir_arch + 'modscag/'
    date_str = str(date_dn.strftime('%Y%m%d'))
    chr_rm = [":"]
    proj_str = ''.join(i for i in cfg.proj if not i in chr_rm)
    basin_str = os.path.splitext(os.path.basename(cfg.basin_poly_path))[0]

    # merge and reproject snow fraction (fsca) files
    tif_list_fsca = glob.glob("{0}/*{1}*{2}.tif".format(dir_work_modscag, date_dn.strftime('%Y%j'), "snow_fraction"))
    tif_out_fsca = 'data\working\modscag\MOD09GA_' + date_str + '_{0}_fsca.tif'

    try:
        rasterio_raster_merge(tif_list_fsca, tif_out_fsca.format("ext"))
        logger.info("org_modscag: merging {} {} tiles".format(date_dn.strftime('%Y-%m-%d'), 'snow_fraction'))
    except:
        logger.error("org_modscag: error merging {} {} tiles".format(date_dn.strftime('%Y-%m-%d'), 'snow_fraction'))
    try:
        rasterio_raster_reproject(tif_out_fsca.format("ext"), tif_out_fsca.format(proj_str), cfg.proj, nodata=250)
        logger.info("org_modscag: reprojecting {} to {}".format('snow_fraction', date_dn.strftime('%Y-%m-%d')))
    except:
        logger.error("org_modscag: error reprojecting {} to {}".format(tif_out_fsca.format("ext"), tif_out_fsca.format(proj_str), cfg.proj))

    # merge and reproject vegetation fraction files (vfrac)
    tif_list_vfrac = glob.glob("{0}/*{1}*{2}.tif".format(dir_work_modscag, date_dn.strftime('%Y%j'), "vegetation_fraction"))
    tif_out_vfrac = 'data\working\modscag\MOD09GA_' + date_str + '_{0}_vfrac.tif'

    try:
        rasterio_raster_merge(tif_list_vfrac, tif_out_vfrac.format("ext"))
        logger.info("org_modscag: merging {} {} tiles".format(date_dn.strftime('%Y-%m-%d'), 'vegetation_fraction'))
    except:
        logger.error("org_modscag: error merging {} {} tiles".format(date_dn.strftime('%Y-%m-%d'), 'vegetation_fraction'))
    try:
        rasterio_raster_reproject(tif_out_vfrac.format("ext"), tif_out_vfrac.format(proj_str), cfg.proj, nodata=250)
        logger.info("org_modscag: reprojecting {} to {}".format('vegetation_fraction', date_dn.strftime('%Y-%m-%d')))
    except:
        logger.error("org_modscag: error reprojecting {} to {}".format(tif_out_vfrac.format("ext"), tif_out_vfrac.format(proj_str), cfg.proj))

    # clip to basin polygon (fsca)
    tif_list = glob.glob("{0}/*{1}*{2}*{3}.tif".format(dir_work_modscag, date_str, proj_str, "fsca"))
    for tif in tif_list:
        tif_out = dir_work_modscag + "modscag_fsca_" + date_str + "_" + basin_str + ".tif"
        try:
            gdal_raster_clip(cfg.basin_poly_path, tif, tif_out, cfg.proj, cfg.proj, 250)
            logger.info("org_modscag: clipping {} to {}".format(tif, tif_out))
        except:
            logger.error("org_modscag: error clipping {} to {}".format(tif, tif_out))
    if not tif_list:
        logger.error("org_modscag: error finding tifs to clip")

    # clip to basin polygon (vfrac)
    tif_list = glob.glob("{0}/*{1}*{2}*{3}.tif".format(dir_work_modscag, date_str, proj_str, "vfrac"))
    for tif in tif_list:
        tif_out = dir_work_modscag + "modscag_vfrac_" + date_str + "_" + basin_str + ".tif"
        try:
            gdal_raster_clip(cfg.basin_poly_path, tif, tif_out, cfg.proj, cfg.proj, 250)
            logger.info("org_modscag: clipping {} to {}".format(tif, tif_out))
        except:
            logger.error("org_modscag: error clipping {} to {}".format(tif, tif_out))
    if not tif_list:
        logger.error("org_modscag: error finding tifs to clip")

    # set filenames
    file_fsca = "modscag_fsca_" + date_str + "_" + basin_str + ".tif"
    file_vfrac = "modscag_vfrac_" + date_str + "_" + basin_str + ".tif"
    file_fscavegcor = "modscag_fscavegcor_" + date_str + "_" + basin_str + ".tif"

    # open connection to rasters
    rast_fsca = rasterio.open(dir_work_modscag + file_fsca)
    rast_vfrac = rasterio.open(dir_work_modscag + file_vfrac)

    # read in raster data to np array
    fsca = rast_fsca.read(1)

    # set pixels > 100 to nodata value (250)
    fsca_masked = np.where(fsca>100, 250, fsca)

    # read in raster data to np array
    vfrac = rast_vfrac.read(1)

    # set pixels > 100 to nodata value (250)
    vfrac_masked = np.where(vfrac>100, 250, vfrac)

    # write out masked files (fsca)
    with rasterio.Env():

        # Write an array as a raster band to a new 8-bit file. For
        # the new file's profile, we start with the profile of the source
        profile = rast_fsca.profile
        profile.update(
            dtype=rasterio.uint8,
            count=1)
        with rasterio.open(cfg.dir_db + file_fsca, 'w', **profile) as dst:
            dst.write(fsca_masked,indexes=1)

    # write out masked files (vfrac)
    with rasterio.Env():

        # Write an array as a raster band to a new 8-bit file. For
        # the new file's profile, we start with the profile of the source
        profile = rast_vfrac.profile
        profile.update(
            dtype=rasterio.uint8,
            count=1)
        with rasterio.open(cfg.dir_db + file_vfrac, 'w', **profile) as dst:
            dst.write(vfrac_masked,indexes=1)

    # fsca with vegetation correction
    vfrac_calc = np.where(vfrac_masked==100, 99, vfrac_masked)
    fsca_vegcor = fsca / (100 - vfrac_calc) * 100
    fsca_vegcor_masked = np.where(fsca>100, 250, fsca_vegcor)

    # write fsca with vegetation correction
    with rasterio.Env():

        # Write an array as a raster band to a new 8-bit file. For
        # the new file's profile, we start with the profile of the source
        profile = rast_vfrac.profile
        profile.update(
            dtype=rasterio.float64,
            count=1)
        with rasterio.open(cfg.dir_db + file_fscavegcor, 'w', **profile) as dst:
            dst.write(fsca_vegcor_masked,indexes=1)

    # close datasets
    rast_fsca.close()
    rast_vfrac.close()

    # calculate zonal statistics and export data
    tif_list = glob.glob("{0}/{1}*{2}*{3}*.tif".format(cfg.dir_db, 'modscag', date_str, basin_str))
    for tif in tif_list:
        file_meta = os.path.basename(tif).replace('.', '_').split('_')
        if 'poly' in cfg.output_type:
            try:
                tif_stats = zonal_stats(cfg.basin_poly_path, tif, stats=['median', 'mean'], all_touched=True)
                tif_stats_df = pd.DataFrame(tif_stats)
                logger.info("org_modscag: computing zonal statistics")
            except:
                logger.error("org_modscag: error computing poly zonal statistics")
            try:
                frames = [cfg.basin_poly, tif_stats_df]
                basin_poly_stats = pd.concat(frames, axis=1)
                logger.info("org_modscag: merging poly zonal statistics")
            except:
                logger.error("org_modscag: error merging zonal statistics")

            if 'geojson' in cfg.output_format:
                try:
                    geojson_out = os.path.splitext(tif)[0] + "_poly.geojson"
                    basin_poly_stats.to_file(geojson_out, driver='GeoJSON')
                    logger.info("org_modscag: writing {0}".format(geojson_out))
                except:
                    logger.error("org_modscag: error writing {0}".format(geojson_out))
            if 'csv' in cfg.output_format:
                try:
                    csv_out = os.path.splitext(tif)[0] + "_poly.csv"
                    basin_poly_stats_df = pd.DataFrame(basin_poly_stats.drop(columns = 'geometry'))
                    basin_poly_stats_df.insert(0, 'Source', file_meta[0])
                    basin_poly_stats_df.insert(0, 'Type', file_meta[1])
                    basin_poly_stats_df.insert(0, 'Date', dt.datetime.strptime(file_meta[2], '%Y%m%d').strftime('%Y-%m-%d %H:%M'))
                    basin_poly_stats_df.to_csv(csv_out, index=False)
                    logger.info("org_modscag: writing {0}".format(csv_out))
                except:
                    logger.error("org_modscag: error writing {0}".format(csv_out))

        if 'points' in cfg.output_type:
            try:
                tif_stats = zonal_stats(cfg.basin_points_path, tif, stats=['median', 'mean'], all_touched=True)
                tif_stats_df = pd.DataFrame(tif_stats)
                logger.info("org_modscag: computing points zonal statistics")
            except:
                logger.error("org_modscag: error computing points zonal statistics")
            try:
                frames = [cfg.basin_points, tif_stats_df]
                basin_points_stats = pd.concat(frames, axis=1)
                logger.info("org_modscag: merging zonal statistics")
            except:
                logger.error("org_modscag: error merging zonal statistics")
            if 'geojson' in cfg.output_format:
                try:
                    geojson_out = os.path.splitext(tif)[0] + "_points.geojson"
                    basin_points_stats.to_file(geojson_out, driver='GeoJSON')
                    logger.info("org_snodas: writing {0}".format(geojson_out))
                except:
                    logger.error("org_snodas: error writing {0}".format(geojson_out))
            if 'csv' in cfg.output_format:
                try:
                    csv_out = os.path.splitext(tif)[0] + "_points.csv"
                    basin_points_stats_df = pd.DataFrame(basin_points_stats.drop(columns = 'geometry'))
                    basin_points_stats_df.insert(0, 'Source', file_meta[0])
                    basin_points_stats_df.insert(0, 'Type', file_meta[1])
                    basin_points_stats_df.insert(0, 'Date', dt.datetime.strptime(file_meta[2], '%Y%m%d').strftime('%Y-%m-%d %H:%M'))
                    basin_points_stats_df.to_csv(csv_out, index=False)
                    logger.info("org_modscag: writing {0}".format(csv_out))
                except:
                    logger.error("org_modscag: error writing {0}".format(csv_out))

    # clean up working directory
    for file in os.listdir(dir_work_modscag):
        file_path = dir_work_modscag + file
        try:
            os.remove(file_path)
            logger.info("org_modscag: removing {}".format(file_path))
        except:
            logger.error("org_modscag: error removing {}".format(file_path))


def download_moddrfs(cfg, date_dn, overwrite_flag = False):
    """Download moddrfs from JPL

    Parameters
    ---------
        cfg ():
            config_params Class object
        date_dn: datetime
            date
        overwrite_flag: boolean
            True : overwrite existing files

    Returns
    -------
        None

    Notes
    -----
    Currently uses JPL data archive. May be moved in future.

    geotif specs - # work on this
        2350 - cloud cover, guess
        2500 - nodata, guess

    *.deltavis product also available - unsure what this is
    """

    site_url = cfg.host_jpl + cfg.dir_http_moddrfs + date_dn.strftime('%Y') + "/" + date_dn.strftime('%j')
    r = requests.get(site_url, auth=HTTPDigestAuth(cfg.username_jpl, cfg.password_jpl), verify=cfg.ssl_verify)
    if r.status_code == 200:
        dir_work_d = cfg.dir_work + 'moddrfs/'
        if not os.path.isdir(dir_work_d):
            os.makedirs(dir_work_d)

        # loop through modis sinusodial tiles
        for tile in cfg.singrd_tile_list:

            # forcing (forc)
            tif_forc_name = "MOD09GA.A" + date_dn.strftime('%Y') + date_dn.strftime('%j') + "." + tile + ".006.NRT.forcing.tif"
            tif_forc_url = site_url + "/" + tif_forc_name
            tif_forc_path = dir_work_d + tif_forc_name

            if os.path.isfile(tif_forc_path) and overwrite_flag:
                os.remove(tif_forc_path)
            if os.path.isfile(tif_forc_path) and overwrite_flag == False:
                logger.info("download_moddrfs: skipping {} {}, {} exists".format(date_dn.strftime('%Y-%m-%d'), tile, tif_forc_path))
            if not os.path.isfile(tif_forc_path):
                logger.info("download_moddrfs: downloading {} {} {}".format('forcing', date_dn.strftime('%Y-%m-%d'), tile))
                logger.info("download_moddrfs: downloading from {}".format(tif_forc_url))
                logger.info("download_moddrfs: downloading to {}".format(tif_forc_path))
                try:
                    r = requests.get(tif_forc_url, auth = HTTPDigestAuth(cfg.username_jpl, cfg.password_jpl), verify=cfg.ssl_verify)
                    if r.status_code == 200:
                        with open(tif_forc_path, 'wb') as rfile:
                            rfile.write(r.content)
                    else:
                        logger.error("download_moddrfs: error downloading {} {} {}".format('forcing', date_dn.strftime('%Y-%m-%d'), tile))
                except IOError as e:
                    logger.error("download_moddrfs: error downloading {} {} {}".format(date_dn.strftime('forcing', '%Y-%m-%d'), tile))
                    logging.error(e)

                # grain size (grnsz)
                tif_grnsz_name = "MOD09GA.A" + date_dn.strftime('%Y') + date_dn.strftime('%j') + "." + tile + ".006.NRT.drfs.grnsz.tif"
                tif_grnsz_url = site_url + "/" + tif_grnsz_name
                tif_grnsz_path = dir_work_d + tif_grnsz_name

                if os.path.isfile(tif_grnsz_path) and overwrite_flag:
                    os.remove(tif_grnsz_path)
                if os.path.isfile(tif_grnsz_path) and overwrite_flag == False:
                    logger.info("download_moddrfs: skipping {} {}, {} exists".format(date_dn.strftime('%Y-%m-%d'), tile, tif_grnsz_path))
                if not os.path.isfile(tif_grnsz_path):
                    logger.info("download_moddrfs: downloading {} {} {}".format('drfs.grnsz', date_dn.strftime('%Y-%m-%d'), tile))
                    logger.info("download_moddrfs: downloading from {}".format(tif_grnsz_url))
                    logger.info("download_moddrfs: downloading to {}".format(tif_grnsz_path))
                    try:
                        r = requests.get(tif_grnsz_url, auth = HTTPDigestAuth(cfg.username_jpl, cfg.password_jpl), verify=cfg.ssl_verify)
                        if r.status_code == 200:
                            with open(tif_grnsz_path, 'wb') as rfile:
                                rfile.write(r.content)
                        else:
                            logger.error("download_moddrfs: error downloading {} {} {}".format('drfs.grnsz', date_dn.strftime('%Y-%m-%d'), tile))
                    except IOError as e:
                        logger.error("download_moddrfs: error downloading {} {} {}".format(date_dn.strftime('drfs.grnsz', '%Y-%m-%d'), tile))
                        logging.error(e)
    else:
        logger.error("download_moddrfs: error connecting {}".format(site_url))


def org_moddrfs(cfg, date_dn):
    """ Organize downloaded moddrfs data

    Parameters
    ---------
        cfg ():
            config_params Class object
        date_dn: datetime
            date

    Returns
    -------
        None

    Notes
    -----

    """

    # Proj4js.defs["SR-ORG:6974"] = "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs";
    dir_work_moddrfs = cfg.dir_work + 'moddrfs/'
    dir_arch_moddrfs = cfg.dir_arch + 'moddrfs/'
    date_str = str(date_dn.strftime('%Y%m%d'))
    chr_rm = [":"]
    proj_str = ''.join(i for i in cfg.proj if not i in chr_rm)
    basin_str = os.path.splitext(os.path.basename(cfg.basin_poly_path))[0]

    # merge and reproject radiative forcing (forc) files
    tif_list_forc = glob.glob("{0}/*{1}*{2}.tif".format(dir_work_moddrfs, date_dn.strftime('%Y%j'), "forcing"))
    tif_out_forc = 'data\working\moddrfs\MOD09GA_' + date_str + '_{0}_forc.tif'

    try:
        rasterio_raster_merge(tif_list_forc, tif_out_forc.format("ext"))
        logger.info("org_moddrfs: merging {} {} tiles".format(date_dn.strftime('%Y-%m-%d'), 'forcing'))
    except:
        logger.error("org_moddrfs: error merging {} {} tiles".format(date_dn.strftime('%Y-%m-%d'), 'forcing'))
    try:
        rasterio_raster_reproject(tif_out_forc.format("ext"), tif_out_forc.format(proj_str), cfg.proj, nodata=2500)
        logger.info("org_moddrfs: reprojecting {} to {}".format('forcing', date_dn.strftime('%Y-%m-%d')))
    except:
        logger.error("org_moddrfs: error reprojecting {} to {}".format(tif_out_forc.format("ext"), tif_out_forc.format(proj_str), cfg.proj))

    # merge and reproject grain size files (grnsz)
    tif_list_grnsz = glob.glob("{0}/*{1}*{2}.tif".format(dir_work_moddrfs, date_dn.strftime('%Y%j'), "drfs.grnsz"))
    tif_out_grnsz = 'data\working\moddrfs\MOD09GA_' + date_str + '_{0}_grnsz.tif'

    try:
        rasterio_raster_merge(tif_list_grnsz, tif_out_grnsz.format("ext"))
        logger.info("org_moddrfs: merging {} {} tiles".format(date_dn.strftime('%Y-%m-%d'), 'drfs.grnsz'))
    except:
        logger.error("org_moddrfs: error merging {} {} tiles".format(date_dn.strftime('%Y-%m-%d'), 'drfs.grnsz'))
    try:
        rasterio_raster_reproject(tif_out_grnsz.format("ext"), tif_out_grnsz.format(proj_str), cfg.proj, nodata=2500)
        logger.info("org_moddrfs: reprojecting {} to {}".format('drfs.grnsz', date_dn.strftime('%Y-%m-%d')))
    except:
        logger.error("org_moddrfs: error reprojecting {} to {}".format(tif_out_grnsz.format("ext"), tif_out_grnsz.format(proj_str), cfg.proj))

    # clip to basin polygon (forc)
    tif_list = glob.glob("{0}/*{1}*{2}*{3}.tif".format(dir_work_moddrfs, date_str, proj_str, "forc"))
    for tif in tif_list:
        tif_out = dir_work_moddrfs + "moddrfs_forc_" + date_str + "_" + basin_str + ".tif"
        try:
            gdal_raster_clip(cfg.basin_poly_path, tif, tif_out, cfg.proj, cfg.proj, 2500)
            logger.info("org_moddrfs: clipping {} to {}".format(tif, tif_out))
        except:
            logger.error("org_moddrfs: error clipping {} to {}".format(tif, tif_out))
    if not tif_list:
        logger.error("org_moddrfs: error finding tifs to clip")

    # clip to basin polygon (grnsz)
    tif_list = glob.glob("{0}/*{1}*{2}*{3}.tif".format(dir_work_moddrfs, date_str, proj_str, "grnsz"))
    for tif in tif_list:
        tif_out = dir_work_moddrfs + "moddrfs_grnsz_" + date_str + "_" + basin_str + ".tif"
        try:
            gdal_raster_clip(cfg.basin_poly_path, tif, tif_out, cfg.proj, cfg.proj, 2500)
            logger.info("org_moddrfs: clipping {} to {}".format(tif, tif_out))
        except:
            logger.error("org_moddrfs: error clipping {} to {}".format(tif, tif_out))
    if not tif_list:
        logger.error("org_moddrfs: error finding tifs to clip")

    # set filenames
    file_forc = "moddrfs_forc_" + date_str + "_" + basin_str + ".tif"
    file_grnsz = "moddrfs_grnsz_" + date_str + "_" + basin_str + ".tif"

    # open connection to rasters
    rast_forc = rasterio.open(dir_work_moddrfs + file_forc)
    rast_grnsz = rasterio.open(dir_work_moddrfs + file_grnsz)

    # read in raster data to np array
    forc = rast_forc.read(1)

    # set pixels > 100 to nodata value (250)
    forc_masked = np.where(forc>1000, 2500, forc)

    # read in raster data to np array
    grnsz = rast_grnsz.read(1)

    # set pixels > 100 to nodata value (250)
    grnsz_masked = np.where(grnsz>1000, 2500, grnsz)

    # write out masked files (forc)
    with rasterio.Env():

        # Write an array as a raster band to a new 8-bit file. For
        # the new file's profile, we start with the profile of the source
        profile = rast_forc.profile
        profile.update(
            dtype=rasterio.uint16,
            count=1)
        with rasterio.open(cfg.dir_db + file_forc, 'w', **profile) as dst:
            dst.write(forc_masked,indexes=1)

    # write out masked files (grnsz)
    with rasterio.Env():

        # Write an array as a raster band to a new 8-bit file. For
        # the new file's profile, we start with the profile of the source
        profile = rast_grnsz.profile
        profile.update(
            dtype=rasterio.uint16,
            count=1)
        with rasterio.open(cfg.dir_db + file_grnsz, 'w', **profile) as dst:
            dst.write(grnsz_masked,indexes=1)

    # close datasets
    rast_forc.close()
    rast_grnsz.close()

    # calculate zonal statistics and export data
    tif_list = glob.glob("{0}/{1}*{2}*{3}*.tif".format(cfg.dir_db, 'moddrfs', date_str, basin_str))
    for tif in tif_list:
        file_meta = os.path.basename(tif).replace('.', '_').split('_')
        if 'poly' in cfg.output_type:
            try:
                tif_stats = zonal_stats(cfg.basin_poly_path, tif, stats=['median', 'mean'], all_touched=True)
                tif_stats_df = pd.DataFrame(tif_stats)
                logger.info("org_moddrfs: computing zonal statistics")
            except:
                logger.error("org_moddrfs: error computing poly zonal statistics")
            try:
                frames = [cfg.basin_poly, tif_stats_df]
                basin_poly_stats = pd.concat(frames, axis=1)
                logger.info("org_moddrfs: merging poly zonal statistics")
            except:
                logger.error("org_moddrfs: error merging zonal statistics")

            if 'geojson' in cfg.output_format:
                try:
                    geojson_out = os.path.splitext(tif)[0] + "_poly.geojson"
                    basin_poly_stats.to_file(geojson_out, driver='GeoJSON')
                    logger.info("org_moddrfs: writing {0}".format(geojson_out))
                except:
                    logger.error("org_moddrfs: error writing {0}".format(geojson_out))
            if 'csv' in cfg.output_format:
                try:
                    csv_out = os.path.splitext(tif)[0] + "_poly.csv"
                    basin_poly_stats_df = pd.DataFrame(basin_poly_stats.drop(columns = 'geometry'))
                    basin_poly_stats_df.insert(0, 'Source', file_meta[0])
                    basin_poly_stats_df.insert(0, 'Type', file_meta[1])
                    basin_poly_stats_df.insert(0, 'Date', dt.datetime.strptime(file_meta[2], '%Y%m%d').strftime('%Y-%m-%d %H:%M'))
                    basin_poly_stats_df.to_csv(csv_out, index=False)
                    logger.info("org_moddrfs: writing {0}".format(csv_out))
                except:
                    logger.error("org_moddrfs: error writing {0}".format(csv_out))

        if 'points' in cfg.output_type:
            try:
                tif_stats = zonal_stats(cfg.basin_points_path, tif, stats=['median', 'mean'], all_touched=True)
                tif_stats_df = pd.DataFrame(tif_stats)
                logger.info("org_moddrfs: computing points zonal statistics")
            except:
                logger.error("org_moddrfs: error computing points zonal statistics")
            try:
                frames = [cfg.basin_points, tif_stats_df]
                basin_points_stats = pd.concat(frames, axis=1)
                logger.info("org_moddrfs: merging zonal statistics")
            except:
                logger.error("org_moddrfs: error merging zonal statistics")
            if 'geojson' in cfg.output_format:
                try:
                    geojson_out = os.path.splitext(tif)[0] + "_points.geojson"
                    basin_points_stats.to_file(geojson_out, driver='GeoJSON')
                    logger.info("org_snodas: writing {0}".format(geojson_out))
                except:
                    logger.error("org_snodas: error writing {0}".format(geojson_out))
            if 'csv' in cfg.output_format:
                try:
                    csv_out = os.path.splitext(tif)[0] + "_points.csv"
                    basin_points_stats_df = pd.DataFrame(basin_points_stats.drop(columns = 'geometry'))
                    basin_points_stats_df.insert(0, 'Source', file_meta[0])
                    basin_points_stats_df.insert(0, 'Type', file_meta[1])
                    basin_points_stats_df.insert(0, 'Date', dt.datetime.strptime(file_meta[2], '%Y%m%d').strftime('%Y-%m-%d %H:%M'))
                    basin_points_stats_df.to_csv(csv_out, index=False)
                    logger.info("org_moddrfs: writing {0}".format(csv_out))
                except:
                    logger.error("org_moddrfs: error writing {0}".format(csv_out))

    # clean up working directory
    for file in os.listdir(dir_work_moddrfs):
        file_path = dir_work_moddrfs + file
        try:
            os.remove(file_path)
            logger.info("org_moddrfs: removing {}".format(file_path))
        except:
            logger.error("org_moddrfs: error removing {}".format(file_path))

def download_modis(cfg, date_dn):
    """ Download modis snow data

    Parameters
    ---------
        cfg ():
            config_params Class object
        date_dn: datetime
            date in local date

    Returns
    -------
        None

    Notes
    -----

    """

    # create working directory
    dir_work_d = cfg.dir_work + 'modis/'
    if not os.path.isdir(dir_work_d):
        os.makedirs(dir_work_d)

    # format date into UTC and API format
    # assumes local time zone for date_dn
    date_dn_ltz = date_dn.replace(tzinfo=get_localzone())
    date_dn_utc_start = date_dn_ltz.astimezone(pytz.utc)
    date_dn_start = date_dn_utc_start.isoformat()[0:16] + ':00Z'
    date_dn_utc_end = date_dn_utc_start + dt.timedelta(hours=24)
    date_dn_end = date_dn_utc_end.isoformat()[0:16] + ':00Z'

    # format bounding box
    basin_poly_bbox_tmp = [str(element) for element in cfg.basin_poly_bbox]
    lonlatod = [1, 0, 3, 2]
    basin_poly_bbox_tmp2 = [basin_poly_bbox_tmp[i] for i in lonlatod]
    basin_poly_bbox_fmt = ",".join(basin_poly_bbox_tmp2)

    short_name_aqua = 'MYD10A2'
    short_name_terra = 'MOD10A2'
    version = '6'
    polygon = ''
    filename_filter = ''

    # search for aqua data
    try:
        url_list_aqua = cmr_search(short_name_aqua, version, date_dn_start,
                        date_dn_end, bounding_box=basin_poly_bbox_fmt,
                        polygon=polygon, filename_filter=filename_filter)
    except:
        url_list_aqua = []

    # search for terra data
    try:
        url_list_terra = cmr_search(short_name_terra, version, date_dn_start,
                        date_dn_end, bounding_box=basin_poly_bbox_fmt,
                        polygon=polygon, filename_filter=filename_filter)
    except:
        url_list_terra = []

    # combine data results together
    url_list = url_list_aqua + url_list_terra
    print(url_list)
    if len(url_list) > 0:
        credentials = '{0}:{1}'.format(cfg.username_earthdata, cfg.password_earthdata)
        credentials = base64.b64encode(credentials.encode('ascii')).decode('ascii')

        for index, url in enumerate(url_list, start=1):
            file_name = url.split('/')[-1]
            file_path = dir_work_d + file_name
            try:
                req = Request(url)
                if credentials:
                    req.add_header('Authorization', 'Basic {0}'.format(credentials))
                opener = build_opener(HTTPCookieProcessor())
                data = opener.open(req).read()
                open(file_path, 'wb').write(data)
            except HTTPError as e:
                logger.info(('HTTP error {0}, {1}'.format(e.code, e.reason)))
            except URLError as e:
                logger.info(('URL error: {0}'.format(e.reason)))
            except IOError:
                raise
    else:
        logger.info("download_modis: no data found")

def batch_swann(cfg, date_list, time_int):
    """downloads and processes swann data
    Parameters
    ---------
        date_list: list of datetime dates
            dates to retrieve data

    Returns
    -------
        None

    Notes
    -----
    calls 'download_swann_arc', 'download_swann_rt', 'org_swann'

    hard-coded parameters found from this reference -
        https://nsidc.org/data/nsidc-0719/versions/1

    """

    # NSIDC short name for dataset
    short_name = 'NSIDC-0719'

    # look at data avaiable through NSIDC archive
    params = {
        'short_name': short_name
    }

    cmr_collections_url = 'https://cmr.earthdata.nasa.gov/search/collections.json'
    response = requests.get(cmr_collections_url, params=params)
    results = json.loads(response.content)
    # start date of archive data
    start_date_ds = dt.datetime.strptime([el['time_start'] for el in results['feed']['entry']][0][0:10], '%Y-%m-%d')

    # end date of archive data
    end_date_ds = dt.datetime.strptime([el['time_end'] for el in results['feed']['entry']][0][0:10], '%Y-%m-%d')

    # build list of data available
    date_list_ds = pd.date_range(start_date_ds, end_date_ds, freq=time_int).tolist()

    # find dates requested avaiable in archive
    date_list_arc = lint(date_list_ds, date_list)

    # get unique years as data are stored as water year netCDF
    year_list_arc = set([dt.datetime.strftime(i, "%Y") for i in date_list_arc])

    # build pandas dataframe of dates, year, month, and wyear for proc netCDF
    arc_dt = pd.DataFrame({'date':date_list_arc})
    arc_dt['year'] = arc_dt['date'].dt.strftime('%Y')
    arc_dt['month'] = arc_dt['date'].dt.strftime('%m')
    arc_dt['wyear'] = arc_dt.apply(lambda x: wyear_pd(x), axis=1)

    # download archive netCDF data
    for year_dn in year_list_arc:
        error_flag = False
        try:
            download_swann_arc(cfg, year_dn)
        except:
            logger.info("download_swann_arc: error downloading swann for '{}'".format(year_dn))
            error_flag = True
        if error_flag is False:
            # build list of dates for netCDF file
            date_list_nc = pd.to_datetime(arc_dt[arc_dt.wyear == int(year_dn)].date.to_numpy())

            for date_dn in date_list_nc:
                try:
                    org_swann(cfg, date_dn, ftype = 'arc')
                except:
                    logger.info("org_swann: error processing swann for '{}'".format(date_dn))

    # find dates requested available in real-time
    date_list_rt = ldif(date_list_arc, date_list)

    # download real-time netCDF data
    for date_dn in date_list_rt:
        error_flag = False
        try:
            download_swann_rt(cfg, date_dn)
        except:
            logger.info("download_swann_rt: error downloading swann` for '{}'".format(date_dn))
            error_flag = True
        if error_flag is False:
            try:
                org_swann(cfg, date_dn, ftype = 'rt')
            except:
                logger.info("org_swann: error processing swann for '{}'".format(date_dn))

def download_swann_arc(cfg, year_dn, overwrite_flag = False):
    """ Download SWANN snow data from NSIDC archive

    Parameters
    ---------
        cfg ():
            config_params Class object
        year_dn: datetime
            year format
        overwrite_flag: boolean
            True : overwrite existing files

    Returns
    -------
        None

    Notes
    -----
    called from 'batch_swann'

    """

    site_url = cfg.host_snodas + cfg.dir_ftp_swann_arc

    nc_name = "4km_SWE_Depth_WY" + ("{}_v01.nc".format(year_dn))
    nc_url = site_url + nc_name
    dir_work_swann = cfg.dir_work + 'swann/'
    nc_path = dir_work_swann + nc_name

    if not os.path.isdir(cfg.dir_work):
        os.makedirs(cfg.dir_work)

    if not os.path.isdir(dir_work_swann):
        os.makedirs(dir_work_swann)

    if os.path.isfile(nc_path) and overwrite_flag:
        os.remove(nc_path)

    if not os.path.isfile(nc_path):
        logger.info("download_swann_arc: downloading {}".format(year_dn))
        logger.info("download_swann_arc: downloading from {}".format(nc_url))
        logger.info("download_swann_arc: downloading to {}".format(nc_path))
        try:
            urllib.request.urlretrieve(nc_url, nc_path)
        except IOError as e:
            logger.error("download_swann_arc: error downloading {}".format(year_dn))
            logging.error(e)

def org_swann(cfg, date_dn, ftype):
    """ Organize SWANN snow data

    Parameters
    ---------
        cfg ():
            config_params Class object
        date_dn: datetime
            '%Y-%m-%d' format
        ftype: string
            arc: archive data file
            rt: real-time data file
    Returns
    -------
        None

    Notes
    -----
    called from 'batch_swann'

    """

    year_dn = wyear_dt(date_dn)
    dir_work_swann = cfg.dir_work + 'swann/'
    if ftype == 'arc':
        nc_name = "4km_SWE_Depth_WY" + ("{}_v01.nc".format(year_dn))
    elif ftype == 'rt':
        nc_name = "4km_SWE_Depth_" + ("{}_v01.nc".format(date_dn.strftime('%Y%m%d')))
    else:
        logger.error("org_swann: invalid type {}".format(type))

    nc_path = dir_work_swann + nc_name

    date_str = str(date_dn.strftime('%Y%m%d'))
    chr_rm = [":"]
    proj_str = ''.join(i for i in cfg.proj if not i in chr_rm)
    dtype_out = 'float64'
    basin_str = os.path.splitext(os.path.basename(cfg.basin_poly_path))[0]
    crs_raw = 'EPSG:4326'

    # open dataset with xarray
    with xr.open_dataset(nc_path) as file_nc:
        # set coordinate system to 'WGS84 (EPSG:4326)'
        swann_xr = file_nc.rio.write_crs(4326, inplace=True)
    file_nc.close()

    # extract swe data for 'date_dn' and save as geotif
    swe_swann_xr = swann_xr["SWE"].sel(
        time=np.datetime64(date_dn))
    swe_swann_xr_date_dn = swe_swann_xr.rio.set_spatial_dims(x_dim='lon', y_dim='lat', inplace=True)

    swe_file_path = dir_work_swann + 'swann_swe_' + date_str + '.tif'
    swe_swann_xr_date_dn.rio.to_raster(swe_file_path)

    # extract snow depth data for 'date_dn' and save as geotif
    sd_swann_xr = swann_xr["DEPTH"].sel(
        time=np.datetime64(date_dn))
    sd_swann_xr_date_dn = swe_swann_xr.rio.set_spatial_dims(x_dim='lon', y_dim='lat', inplace=True)

    sd_file_path = dir_work_swann + 'swann_sd_' + date_str + '.tif'
    sd_swann_xr_date_dn.rio.to_raster(sd_file_path)

    # close xr dataset
    swann_xr.close()

    # reproject geotif
    tif_list = glob.glob("{0}/*{1}*.tif".format(dir_work_swann, date_str))
    for tif in tif_list:
        tif_out = os.path.splitext(tif)[0] + "_" + proj_str + ".tif"
        try:
            gdal_raster_reproject(tif, tif_out, cfg.proj, crs_raw)
            # rasterio_raster_reproject(tif, tif_out, cfg.proj)
            logger.info("org_swann: reprojecting {} to {}".format(tif, tif_out))
        except:
            logger.error("org_swann: error reprojecting {} to {}".format(tif, tif_out))
    if not tif_list:
        logger.error("org_swann: error finding tifs to reproject")

    # clip to basin polygon
    tif_list = glob.glob("{0}/*{1}*{2}.tif".format(dir_work_swann, date_str, proj_str))
    for tif in tif_list:
        tif_out = os.path.splitext(tif)[0] + "_" + basin_str + ".tif"
        try:
            gdal_raster_clip(cfg.basin_poly_path, tif, tif_out, cfg.proj, cfg.proj, -9999)
            logger.info("org_swann: clipping {} to {}".format(tif, tif_out))
        except:
            logger.error("org_swann: error clipping {} to {}".format(tif, tif_out))
    if not tif_list:
        logger.error("org_swann: error finding tifs to clip")

    # convert units
    if cfg.unit_sys == 'english':
        calc_exp = '(+ 1 (* .0393701 (read 1)))' # inches
    if cfg.unit_sys == 'metric':
        calc_exp = '(read 1)' # keep units in mm
    # SWE
    tif_list = glob.glob("{0}/*{1}*{2}*{3}*{4}.tif".format(dir_work_swann, 'swann_swe', date_str, proj_str, basin_str))

    for tif in tif_list:
        tif_int = os.path.splitext(tif)[0] + "_" + dtype_out + ".tif"
        tif_out = cfg.dir_db + "swann_swe_" + date_str + "_" + basin_str + "_" + cfg.unit_sys + ".tif"
        try:
            rio_dtype_conversion(tif, tif_int, dtype_out)
            rio_calc(tif_int, tif_out, calc_exp)
            logger.info("org_swann: calc {} {} to {}".format(calc_exp, tif, tif_out))
        except:
            logger.error("org_swann: error calc {} to {}".format(tif, tif_out))
    if not tif_list:
        logger.error("org_swann: error finding tifs to calc")

    # Snow Depth
    tif_list = glob.glob("{0}/*{1}*{2}*{3}*{4}.tif".format(dir_work_swann, 'swann_sd', date_str, proj_str, basin_str))

    for tif in tif_list:
        tif_int = os.path.splitext(tif)[0] + "_" + dtype_out + ".tif"
        tif_out = cfg.dir_db + "swann_snowdepth_" + date_str + "_" + basin_str + "_" + cfg.unit_sys + ".tif"
        try:
            rio_dtype_conversion(tif, tif_int, dtype_out)
            rio_calc(tif_int, tif_out, calc_exp)
            logger.info("org_swann: calc {} {} to {}".format(calc_exp, tif, tif_out))
        except:
            logger.error("org_swann: error calc {} to {}".format(tif, tif_out))
    if not tif_list:
        logger.error("org_swann: error finding tifs to calc")

    # calculate zonal statistics and export data
    tif_list = glob.glob("{0}/{1}*{2}*{3}*{4}.tif".format(cfg.dir_db, 'swann', date_str, basin_str, cfg.unit_sys))

    for tif in tif_list:
        file_meta = os.path.basename(tif).replace('.', '_').split('_')

        if 'poly' in cfg.output_type:
            try:
                tif_stats = zonal_stats(cfg.basin_poly_path, tif, stats=['min', 'max', 'median', 'mean'], all_touched=True)
                tif_stats_df = pd.DataFrame(tif_stats)
                logger.info("org_swann: computing zonal statistics")
            except:
                logger.error("org_swann: error computing poly zonal statistics")
            try:
                frames = [cfg.basin_poly, tif_stats_df]
                basin_poly_stats = pd.concat(frames, axis=1)
                logger.info("org_swann: merging poly zonal statistics")
            except:
                logger.error("org_swann: error merging zonal statistics")

            if 'geojson' in cfg.output_format:
                try:
                    geojson_out = os.path.splitext(tif)[0] + "_poly.geojson"
                    basin_poly_stats.to_file(geojson_out, driver='GeoJSON')
                    logger.info("org_swann: writing {0}".format(geojson_out))
                except:
                    logger.error("org_swann: error writing {0}".format(geojson_out))
            if 'csv' in cfg.output_format:
                try:
                    csv_out = os.path.splitext(tif)[0] + "_poly.csv"
                    basin_poly_stats_df = pd.DataFrame(basin_poly_stats.drop(columns = 'geometry'))
                    basin_poly_stats_df.insert(0, 'Source', file_meta[0])
                    basin_poly_stats_df.insert(0, 'Type', file_meta[1])
                    basin_poly_stats_df.insert(0, 'Date', dt.datetime.strptime(file_meta[2], '%Y%m%d').strftime('%Y-%m-%d %H:%M'))
                    basin_poly_stats_df.to_csv(csv_out, index=False)
                    logger.info("org_swann: writing {0}".format(csv_out))
                except:
                    logger.error("org_swann: error writing {0}".format(csv_out))

        if 'points' in cfg.output_type:
            try:
                tif_stats = zonal_stats(cfg.basin_points_path, tif, stats=['min', 'max', 'median', 'mean'], all_touched=True)
                tif_stats_df = pd.DataFrame(tif_stats)
                logger.info("org_swann: computing points zonal statistics")
            except:
                logger.error("org_swann: error computing points zonal statistics")
            try:
                frames = [cfg.basin_points, tif_stats_df]
                basin_points_stats = pd.concat(frames, axis=1)
                logger.info("org_swann: merging zonal statistics")
            except:
                logger.error("org_swann: error merging zonal statistics")
            if 'geojson' in cfg.output_format:
                try:
                    geojson_out = os.path.splitext(tif)[0] + "_points.geojson"
                    basin_points_stats.to_file(geojson_out, driver='GeoJSON')
                    logger.info("org_swann: writing {0}".format(geojson_out))
                except:
                    logger.error("org_swann: error writing {0}".format(geojson_out))
            if 'csv' in cfg.output_format:
                try:
                    csv_out = os.path.splitext(tif)[0] + "_points.csv"
                    basin_points_stats_df = pd.DataFrame(basin_points_stats.drop(columns = 'geometry'))
                    basin_points_stats_df.insert(0, 'Source', file_meta[0])
                    basin_points_stats_df.insert(0, 'Type', file_meta[1])
                    basin_points_stats_df.insert(0, 'Date', dt.datetime.strptime(file_meta[2], '%Y%m%d').strftime('%Y-%m-%d %H:%M'))
                    basin_points_stats_df.to_csv(csv_out, index=False)
                    logger.info("org_swann: writing {0}".format(csv_out, index=False))
                except:
                    logger.error("org_swann: error writing {0}".format(csv_out))

    # clean up working directory
    for file in os.listdir(dir_work_swann):
        file_path = dir_work_swann + file
        try:
            os.remove(file_path)
            logger.info("org_swann: removing {}".format(file_path))
        except:
            logger.error("org_swann: error removing {}".format(file_path))

def download_swann_rt(cfg, year_dn):
    """ Download SWANN snow data from UA real-time

    Parameters
    ---------
        cfg ():
            config_params Class object
        year_dn: datetime
            year format

    Returns
    -------
        None

    Notes
    -----
    called from 'batch_swann'

    """
    date_str = str(date_dn.strftime('%Y%m%d'))

    site_url = cfg.host_ua + cfg.dir_ftp_swann_rt

    nc_name = "{}.nc".format(date_dn.strftime('%d'))
    nc_url = site_url + date_dn.strftime('%Y') + "/" \
    + date_dn.strftime('%m')+ "/" + nc_name
    nc_path = cfg.dir_work + 'swann/' + '4km_SWE_Depth_' + date_str + '_v01.nc'

    if not os.path.isdir(cfg.dir_work):
        os.makedirs(cfg.dir_work)

    if os.path.isfile(nc_path) and overwrite_flag:
        os.remove(nc_path)
    if not os.path.isfile(nc_path):
        logger.info("download_swann_rt: downloading {}".format(date_dn.strftime('%Y-%m-%d')))
        logger.info("download_swann_rt: downloading from {}".format(nc_url))
        logger.info("download_swann_rt: downloading to {}".format(nc_path))
        try:
            urllib.request.urlretrieve(nc_url, nc_path)
        except IOError as e:
            logger.error("download_swann_rt: error downloading {}".format(date_dn.strftime('%Y-%m-%d')))
            logging.error(e)


def download_ndfd(parameter, flen, crs_out, cfg, overwrite_flag=False):
    """download and format national digital forecast data
    Parameters
    ---------
        parameter: string
            ndfd parameter -
            https://www.nws.noaa.gov/xml/docs/elementInputNames.php
        flen: integer
            forecast length in days
                Default - 7
                values from 1 to 7 are valid
        crs_out: string
            EPSG spatial reference for output raster coordinate system in
                'EPSG:X' format
        overwrite_flag: boolean
    Returns
    -------
        None

    Notes
    -----
    function can only download latest forecast
    only valid right now for CONUS
    """

    dir_work_ndfd = cfg.dir_work + 'ndfd/'
    
    # create working directory if it doesn't exist
    if os.path.isdir(dir_work_ndfd)==False:
        print(dir_work_ndfd)
        os.mkdir(dir_work_ndfd)
    basin_str = os.path.splitext(os.path.basename(cfg.basin_poly_path))[0]
    chr_rm = [":"]
    proj_str = ''.join(i for i in crs_out if not i in chr_rm)
    dtype_out = 'float64'

    # retrieve data for forecast length desired
    # forecasts are stored in three files:
    #   1-3 days
    #   4-7 days
    #   8-450 days - not used
    flen_dirs = ['VP.001-003', 'VP.004-007']
    if flen in range(1,4):
        iflen = 1
    elif flen in range(3,8):
        iflen = 2
    else:
        error('flen must be between 1 and 7')
    print(iflen)

    # qpf only available for 1-3 days
    if parameter == 'qpf'or parameter == 'snow':
        iflen = 1

    for i in range(0,iflen):
        grib_name_url = 'ds.' + parameter + '.bin'
        grib_name_path = parameter + '_' + flen_dirs[i] + '.bin'
        grib_url = cfg.host_ndfd + flen_dirs[i] + '/' + grib_name_url
        grib_path = dir_work_ndfd + grib_name_path

        if os.path.isfile(grib_path) and overwrite_flag:
            os.remove(grib_path)

        if not os.path.isfile(grib_path):
            logger.info("download_ndfd: downloading from {}".format(grib_url))
            logger.info("download_ndfd: downloading to {}".format(grib_path))
            try:
                urllib.request.urlretrieve(grib_url, grib_path)
            except IOError as e:
                logger.error("download_ndfd: error downloading")
                logging.error(e)
        # read grib with pygrib to get message info
        grbs = pygrib.open(grib_path)
         # read first message to get forcast init time
        grb = grbs[1]
        date_init_str = str(grb).split('from ',1)[1].split(':',1)[0]
        date_init = dt.datetime.strptime(date_init_str, '%Y%m%d%H%M')


        # read grib as raster; reproject, write out individual forecast tifs,
        # clip data
        tif_out_head = dir_work_ndfd + parameter + '_' + date_init_str + '_'
        with rasterio.open(grib_path) as src:
            transform, width, height = calculate_default_transform(
                src.crs, crs_out, src.width, src.height, *src.bounds)
            kwargs = src.meta.copy()
            kwargs.update({
                'crs': crs_out,
                'transform': transform,
                'width': width,
                'height': height,
                'count': 1
            })

            for bnd in range(1, src.count + 1):
                grb = grbs[bnd]

                # read valid date from grb message
                valid_date_str = dt.datetime.strftime(grb.validDate, '%Y%m%d%H%M')
                tif_out_band = tif_out_head + valid_date_str + '.tif'
                with rasterio.open(tif_out_band, 'w', **kwargs) as dst:
                    reproject(
                        source=rasterio.band(src, bnd),
                        destination=rasterio.band(dst, 1),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=crs_out,
                        resampling=Resampling.nearest)
        grbs.close()

        # clip to basin polygon
        tif_list = glob.glob("{0}/*{1}*{2}*.tif".format(dir_work_ndfd, parameter, date_init_str))
        for tif in tif_list:
            date_valid_str = tif.split("_")[2].split(".")[0]
            tif_out = dir_work_ndfd + "ndfd_" + parameter + "_" + date_init_str + "_" + date_valid_str + "_" + basin_str + ".tif"
            try:
                gdal_raster_clip(cfg.basin_poly_path, tif, tif_out, crs_out, crs_out, -9999)
                logger.info("download_ndfd: clipping {} to {}".format(tif, tif_out))
            except:
                logger.error("download_ndfd: error clipping {} to {}".format(tif, tif_out))
        if not tif_list:
            logger.error("download_ndfd: error finding tifs to clip")

        # convert units
        tif_list = glob.glob("{0}/*{1}*{2}*{3}*.tif".format(dir_work_ndfd, 'ndfd', parameter, date_init_str))
        ct_flag = False
        # mm to inches conversion
        if parameter == 'snow':
            if cfg.unit_sys == 'english':
                calc_exp = '(+ 1 (* 39.3701 (read 1)))' # inches
            if cfg.unit_sys == 'metric':
                calc_exp = '(+ 1 (/ 1000 (read 1)))' # mm
                
        if parameter == "qpf":
            if cfg.unit_sys == 'english':
                calc_exp = '(+ 1 (* 0.04 (read 1)))' # convert from kg/m2 to inches of water
            if cfg.unit_sys == 'metric':
                calc_exp = '(read 1)' # keep units in percentage
                
        if (parameter == 'pop12') or (parameter == "sky") or (parameter == "rhm"):
            if cfg.unit_sys == 'english':
                calc_exp = '(read 1)' # keep units in percentage
            if cfg.unit_sys == 'metric':
                calc_exp = '(read 1)' # keep units in percentage

        # c to f conversion
        if parameter == 'mint' or parameter == 'maxt':
            if cfg.unit_sys == 'english':
                ct_flag = True
                calc_exp = '(+ 1 (* 1.8 (read 1)))' # deg. F (mult)
                calc_exp2 = '(+ 1 (+ 32 (read 1)))' # deg. F (add)

            if cfg.unit_sys == 'metric':
                calc_exp = '(read 1)' # keep units in deg. C

        tif_list = glob.glob("{0}/*{1}*{2}*{3}*{4}*.tif".format(dir_work_ndfd, 'ndfd', parameter, date_init_str, basin_str))

        for tif in tif_list:
            tif_int = os.path.splitext(tif)[0] + "_" + dtype_out + ".tif"
            if ct_flag == True:
                tif_int2 = os.path.splitext(tif)[0] + "_mult" + ".tif"
            tif_out = cfg.dir_db + os.path.splitext(os.path.basename(tif))[0] + "_" + cfg.unit_sys + ".tif"
            try:
                rio_dtype_conversion(tif, tif_int, dtype_out)
                if ct_flag == True:
                    rio_calc(tif_int, tif_int2, calc_exp)
                    rio_calc(tif_int2, tif_out, calc_exp2)
                if ct_flag == False:
                    rio_calc(tif_int, tif_out, calc_exp)
                logger.info("download_ndfd: calc {} {} to {}".format(calc_exp, tif, tif_out))
            except:
                logger.error("download_ndfd: error calc {} to {}".format(tif, tif_out))
        if not tif_list:
            logger.error("download_ndfd: error finding tifs to calc")

        # end of iflen loop
        # save grib file for archiving - currently saved to the database directory with init date appended to filename
        shutil.move(grib_path, cfg.dir_db + os.path.splitext(os.path.basename(grib_path))[0] + '_' + date_init_str + '.bin')

        # calculate zonal statistics and export data
        tif_list = glob.glob("{0}/{1}*{2}*{3}*{4}*{5}.tif".format(cfg.dir_db, 'ndfd', parameter, date_init_str, basin_str, cfg.unit_sys))

        for tif in tif_list:
            file_meta = os.path.basename(tif).replace('.', '_').split('_')

            if 'poly' in cfg.output_type:
                try:
                    tif_stats = zonal_stats(cfg.basin_poly_path, tif, stats=['min', 'max', 'median', 'mean'], all_touched=True)
                    tif_stats_df = pd.DataFrame(tif_stats)
                    logger.info("download_ndfd: computing zonal statistics")
                except:
                    logger.error("download_ndfd: error computing poly zonal statistics")
                try:
                    frames = [cfg.basin_poly, tif_stats_df]
                    basin_poly_stats = pd.concat(frames, axis=1)
                    logger.info("download_ndfd: merging poly zonal statistics")
                except:
                    logger.error("download_ndfd: error merging zonal statistics")

                if 'geojson' in cfg.output_format:
                    try:
                        geojson_out = os.path.splitext(tif)[0] + "_poly.geojson"
                        basin_poly_stats.to_file(geojson_out, driver='GeoJSON')
                        logger.info("download_ndfd: writing {0}".format(geojson_out))
                    except:
                        logger.error("download_ndfd: error writing {0}".format(geojson_out))
                if 'csv' in cfg.output_format:
                    try:
                        csv_out = os.path.splitext(tif)[0] + "_poly.csv"
                        basin_poly_stats_df = pd.DataFrame(basin_poly_stats.drop(columns = 'geometry'))
                        basin_poly_stats_df.insert(0, 'Source', file_meta[0])
                        basin_poly_stats_df.insert(0, 'Type', file_meta[1])
                        basin_poly_stats_df.insert(0, 'Date_Init', dt.datetime.strptime(file_meta[2], '%Y%m%d%H%M').strftime('%Y-%m-%d %H:%M'))
                        basin_poly_stats_df.insert(0, 'Date_Valid', dt.datetime.strptime(file_meta[3], '%Y%m%d%H%M').strftime('%Y-%m-%d %H:%M'))
                        basin_poly_stats_df.to_csv(csv_out, index=False)
                        logger.info("download_ndfd: writing {0}".format(csv_out))
                    except:
                        logger.error("download_ndfd: error writing {0}".format(csv_out))

            if 'points' in cfg.output_type:
                try:
                    tif_stats = zonal_stats(cfg.basin_points_path, tif, stats=['min', 'max', 'median', 'mean'], all_touched=True)
                    tif_stats_df = pd.DataFrame(tif_stats)
                    logger.info("download_ndfd: computing points zonal statistics")
                except:
                    logger.error("download_ndfd: error computing points zonal statistics")
                try:
                    frames = [cfg.basin_points, tif_stats_df]
                    basin_points_stats = pd.concat(frames, axis=1)
                    logger.info("download_ndfd: merging zonal statistics")
                except:
                    logger.error("download_ndfd: error merging zonal statistics")
                if 'geojson' in cfg.output_format:
                    try:
                        geojson_out = os.path.splitext(tif)[0] + "_points.geojson"
                        basin_points_stats.to_file(geojson_out, driver='GeoJSON')
                        logger.info("download_ndfd: writing {0}".format(geojson_out))
                    except:
                        logger.error("download_ndfd: error writing {0}".format(geojson_out))
                if 'csv' in cfg.output_format:
                    try:
                        csv_out = os.path.splitext(tif)[0] + "_points.csv"
                        basin_points_stats_df = pd.DataFrame(basin_points_stats.drop(columns = 'geometry'))
                        basin_points_stats_df.insert(0, 'Source', file_meta[0])
                        basin_points_stats_df.insert(0, 'Type', file_meta[1])
                        basin_points_stats_df.insert(0, 'Date_Init', dt.datetime.strptime(file_meta[2], '%Y%m%d%H%M').strftime('%Y-%m-%d %H:%M'))
                        basin_points_stats_df.insert(0, 'Date_Valid', dt.datetime.strptime(file_meta[3], '%Y%m%d%H%M').strftime('%Y-%m-%d %H:%M'))
                        basin_points_stats_df.to_csv(csv_out, index=False)
                        logger.info("download_ndfd: writing {0}".format(csv_out, index=False))
                    except:
                        logger.error("download_ndfd: error writing {0}".format(csv_out))

        # clean up working directory
        for file in os.listdir(dir_work_ndfd):
            file_path = dir_work_ndfd + file
            try:
                os.remove(file_path)
                logger.info("download_ndfd: removing {}".format(file_path))
            except:
                logger.error("download_ndfd: error removing {}".format(file_path))








def gdal_raster_reproject(file_in, file_out, crs_out, crs_in = None):
    """wrapper around gdalwarp for reprojecting rasters
    Parameters
    ---------
        file_in: string
            file path of input raster
        file_out: string
            file path of output raster
        crs_out: string
            EPSG spatial reference for output raster coordinate system in
                'EPSG:X' format
        crs_in: string
            EPSG spatial reference for input raster coordinate system
                Default - None as gdalwarp can read coordinate system of
                input raster if available
    Returns
    -------
        None

    Notes
    -----
    Requires gdal_path be set in config ini file and gdal be available

    """

    if crs_in != None:
        os.system("gdalwarp -s_srs {0} -t_srs {1} {2} {3}".format(crs_in, crs_out, file_in, file_out))
    else:
        os.system("gdalwarp -t_srs {0} {1} {2}".format(crs_out, file_in, file_out))
    # error handling

def rasterio_raster_reproject(file_in, file_out, crs_out, nodata = None):
    """wrapper around rasterio for reprojecting rasters
    Parameters
    ---------
        file_in: string
            file path of input raster
        file_out: string
            file path of output raster
        crs_out: string
            EPSG spatial reference for output raster coordinate system in
                'EPSG:X' format

    Returns
    -------
        None

    Notes
    -----
    Requires rasterio
        requires methods from rasterio.warp
        import rasterio
        from rasterio.warp import calculate_default_transform, reproject, Resampling
    """
    if nodata != None:
        with rasterio.open(file_in) as src:
            transform, width, height = calculate_default_transform(
                src.crs, crs_out, src.width, src.height, *src.bounds)
            kwargs = src.meta.copy()
            kwargs.update({
                'crs': crs_out,
                'transform': transform,
                'width': width,
                'height': height
            })

            with rasterio.open(file_out, 'w', **kwargs) as dst:
                for i in range(1, src.count + 1):
                    reproject(
                        source=rasterio.band(src, i),
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=crs_out,
                        dst_nodata=nodata,
                        resampling=Resampling.nearest)
    elif nodata == None:
            with rasterio.open(file_in) as src:
                transform, width, height = calculate_default_transform(
                    src.crs, crs_out, src.width, src.height, *src.bounds)
                kwargs = src.meta.copy()
                kwargs.update({
                    'crs': crs_out,
                    'transform': transform,
                    'width': width,
                    'height': height
                })

                with rasterio.open(file_out, 'w', **kwargs) as dst:
                    for i in range(1, src.count + 1):
                        reproject(
                            source=rasterio.band(src, i),
                            destination=rasterio.band(dst, i),
                            src_transform=src.transform,
                            src_crs=src.crs,
                            dst_transform=transform,
                            dst_crs=crs_out,
                            resampling=Resampling.nearest)

def gdal_raster_merge(file_list_in, file_out):
    """wrapper around gdalwarp for merging rasters
    Parameters
    ---------
        file_in_list: array
            array of file paths of input raster
        file_out: string
            file path of output raster

    Returns
    -------
        None

    Notes
    -----
    Requires gdal_path be set in config ini file and gdal be available


    """

    gdal_merge = os.path.join(cfg.gdal_path, 'gdal_merge.py')
    cmd_list = ["python", gdal_merge, "-o", file_out] + file_list_in
    cmd_string = " ".join(cmd_list)
    os.system(cmd_string)

def rasterio_raster_merge(file_list_in, file_out):
    """wrapper around rasterio for merging rasters
    Parameters
    ---------
        file_in_list: array
            array of file paths of input raster
        file_out: string
            file path of output raster

    Returns
    -------
        None

    Notes
    -----
    requires rasterio library
    only works with 1 band rasters
    """

    from rasterio.merge import merge

    rast_mg = merge(file_list_in)
    rast_con = rasterio.open(file_list_in[1])

    with rasterio.open(
        file_out,
        'w',
        driver='GTiff',
        height=rast_mg[0].shape[1],
        width=rast_mg[0].shape[2],
        count=1,
        dtype=rast_mg[0].dtype,
        crs=rast_con.crs,
        transform=rast_mg[1]) as dst:
            dst.write(rast_mg[0][0,], 1)

def rio_calc(rast_in, rast_out, calc_exp):
    """wrapper around rio calc from rasterio package for raster math
    Parameters
    ---------
        rast_in: string
            file path of input raster
        rast_out: string
            file path of output raster
        calc_exp: string
            raster math expression

    Returns
    -------
        None

    Notes
    -----
    Requires rasterio
    """
    os.system('rio calc "{0}" {1} {2}'.format(calc_exp, rast_in, rast_out))

def rio_dtype_conversion(rast_in, rast_out, dtype_out):
    """wrapper around rio calc from rasterio package for raster math
    Parameters
    ---------
        rast_in: string
            file path of input raster
        rast_out: string
            file path of output raster
        dtype_out: string
            data type

    Returns
    -------
        None

    Notes
    -----
    Requires rasterio
    """
    os.system('rio convert -t "{0}" {1} {2}'.format(dtype_out, rast_in, rast_out))

def gdal_raster_clip(poly_in, rast_in, rast_out, crs_in, crs_out, nodata):
    """wrapper around gdalwarp for clipping rasters with polygon
    Parameters
    ---------
        poly_in: string
            file path of input polygon file
                * shp, kml, geojson at least all work
        rast_in: string
            file path of input raster
        rast_out: string
            file path of output raster
        crs_out: string
            EPSG spatial reference for output raster coordinate system in
                'EPSG:X' format
        crs_in: string
            EPSG spatial reference for input raster coordinate system
                Default - None as gdalwarp can read coordinate system of
                input raster if available
        nodata:
            nodata value or character

    Returns
    -------
        None

    Notes
    -----
    Requires gdal_path be set in config ini file and gdal be available


    """

    os.system("gdalwarp -s_srs {0} -t_srs {1} -of GTiff -cutline {2} -crop_to_cutline -dstnodata {3} {4} {5}".format(crs_in, crs_out, poly_in, nodata, rast_in, rast_out))
    # error handling

def gdal_raster_singleband(rast_in, rast_out, band = 1):
    """wrapper around gdalwarp for converting multiband rasters into
        singleband rasters
    Parameters
    ---------
        rast_in: string
            file path of input raster
        rast_out: string
            file path of output raster
        band: integer
            band to retain
                default: 1

    Returns
    -------
        None

    Notes
    -----
    Requires gdal_path be set in config ini file and gdal be available


    """
    os.system("gdal_translate -b {0} {1} {2}".format(band, rast_in, rast_out))

# MODIS tile definition
tiles = [
    [0, 0, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 1, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 2, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 3, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 4, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 5, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 6, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 7, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 8, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 9, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 10, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 11, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 12, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 13, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 14, -180.0000, -172.7151, 80.0000, 80.4083],
    [0, 15, -180.0000, -115.1274, 80.0000, 83.6250],
    [0, 16, -180.0000, -57.5397, 80.0000, 86.8167],
    [0, 17, -180.0000, 57.2957, 80.0000, 90.0000],
    [0, 18, -0.0040, 180.0000, 80.0000, 90.0000],
    [0, 19, 57.5877, 180.0000, 80.0000, 86.8167],
    [0, 20, 115.1754, 180.0000, 80.0000, 83.6250],
    [0, 21, 172.7631, 180.0000, 80.0000, 80.4083],
    [0, 22, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 23, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 24, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 25, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 26, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 27, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 28, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 29, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 30, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 31, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 32, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 33, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 34, -999.0000, -999.0000, -99.0000, -99.0000],
    [0, 35, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 0, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 1, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 2, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 3, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 4, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 5, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 6, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 7, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 8, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 9, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 10, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 11, -180.0000, -175.4039, 70.0000, 70.5333],
    [1, 12, -180.0000, -146.1659, 70.0000, 73.8750],
    [1, 13, -180.0000, -116.9278, 70.0000, 77.1667],
    [1, 14, -180.0000, -87.6898, 70.0000, 80.0000],
    [1, 15, -172.7631, -58.4517, 70.0000, 80.0000],
    [1, 16, -115.1754, -29.2137, 70.0000, 80.0000],
    [1, 17, -57.5877, 0.0480, 70.0000, 80.0000],
    [1, 18, 0.0000, 57.6357, 70.0000, 80.0000],
    [1, 19, 29.2380, 115.2234, 70.0000, 80.0000],
    [1, 20, 58.4761, 172.8111, 70.0000, 80.0000],
    [1, 21, 87.7141, 180.0000, 70.0000, 80.0000],
    [1, 22, 116.9522, 180.0000, 70.0000, 77.1583],
    [1, 23, 146.1902, 180.0000, 70.0000, 73.8750],
    [1, 24, 175.4283, 180.0000, 70.0000, 70.5333],
    [1, 25, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 26, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 27, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 28, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 29, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 30, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 31, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 32, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 33, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 34, -999.0000, -999.0000, -99.0000, -99.0000],
    [1, 35, -999.0000, -999.0000, -99.0000, -99.0000],
    [2, 0, -999.0000, -999.0000, -99.0000, -99.0000],
    [2, 1, -999.0000, -999.0000, -99.0000, -99.0000],
    [2, 2, -999.0000, -999.0000, -99.0000, -99.0000],
    [2, 3, -999.0000, -999.0000, -99.0000, -99.0000],
    [2, 4, -999.0000, -999.0000, -99.0000, -99.0000],
    [2, 5, -999.0000, -999.0000, -99.0000, -99.0000],
    [2, 6, -999.0000, -999.0000, -99.0000, -99.0000],
    [2, 7, -999.0000, -999.0000, -99.0000, -99.0000],
    [2, 8, -999.0000, -999.0000, -99.0000, -99.0000],
    [2, 9, -180.0000, -159.9833, 60.0000, 63.6167],
    [2, 10, -180.0000, -139.9833, 60.0000, 67.1167],
    [2, 11, -180.0000, -119.9833, 60.0000, 70.0000],
    [2, 12, -175.4283, -99.9833, 60.0000, 70.0000],
    [2, 13, -146.1902, -79.9833, 60.0000, 70.0000],
    [2, 14, -116.9522, -59.9833, 60.0000, 70.0000],
    [2, 15, -87.7141, -39.9833, 60.0000, 70.0000],
    [2, 16, -58.4761, -19.9833, 60.0000, 70.0000],
    [2, 17, -29.2380, 0.0244, 60.0000, 70.0000],
    [2, 18, 0.0000, 29.2624, 60.0000, 70.0000],
    [2, 19, 20.0000, 58.5005, 60.0000, 70.0000],
    [2, 20, 40.0000, 87.7385, 60.0000, 70.0000],
    [2, 21, 60.0000, 116.9765, 60.0000, 70.0000],
    [2, 22, 80.0000, 146.2146, 60.0000, 70.0000],
    [2, 23, 100.0000, 175.4526, 60.0000, 70.0000],
    [2, 24, 120.0000, 180.0000, 60.0000, 70.0000],
    [2, 25, 140.0000, 180.0000, 60.0000, 67.1167],
    [2, 26, 160.0000, 180.0000, 60.0000, 63.6167],
    [2, 27, -999.0000, -999.0000, -99.0000, -99.0000],
    [2, 28, -999.0000, -999.0000, -99.0000, -99.0000],
    [2, 29, -999.0000, -999.0000, -99.0000, -99.0000],
    [2, 30, -999.0000, -999.0000, -99.0000, -99.0000],
    [2, 31, -999.0000, -999.0000, -99.0000, -99.0000],
    [2, 32, -999.0000, -999.0000, -99.0000, -99.0000],
    [2, 33, -999.0000, -999.0000, -99.0000, -99.0000],
    [2, 34, -999.0000, -999.0000, -99.0000, -99.0000],
    [2, 35, -999.0000, -999.0000, -99.0000, -99.0000],
    [3, 0, -999.0000, -999.0000, -99.0000, -99.0000],
    [3, 1, -999.0000, -999.0000, -99.0000, -99.0000],
    [3, 2, -999.0000, -999.0000, -99.0000, -99.0000],
    [3, 3, -999.0000, -999.0000, -99.0000, -99.0000],
    [3, 4, -999.0000, -999.0000, -99.0000, -99.0000],
    [3, 5, -999.0000, -999.0000, -99.0000, -99.0000],
    [3, 6, -180.0000, -171.1167, 50.0000, 52.3333],
    [3, 7, -180.0000, -155.5594, 50.0000, 56.2583],
    [3, 8, -180.0000, -140.0022, 50.0000, 60.0000],
    [3, 9, -180.0000, -124.4449, 50.0000, 60.0000],
    [3, 10, -160.0000, -108.8877, 50.0000, 60.0000],
    [3, 11, -140.0000, -93.3305, 50.0000, 60.0000],
    [3, 12, -120.0000, -77.7732, 50.0000, 60.0000],
    [3, 13, -100.0000, -62.2160, 50.0000, 60.0000],
    [3, 14, -80.0000, -46.6588, 50.0000, 60.0000],
    [3, 15, -60.0000, -31.1015, 50.0000, 60.0000],
    [3, 16, -40.0000, -15.5443, 50.0000, 60.0000],
    [3, 17, -20.0000, 0.0167, 50.0000, 60.0000],
    [3, 18, 0.0000, 20.0167, 50.0000, 60.0000],
    [3, 19, 15.5572, 40.0167, 50.0000, 60.0000],
    [3, 20, 31.1145, 60.0167, 50.0000, 60.0000],
    [3, 21, 46.6717, 80.0167, 50.0000, 60.0000],
    [3, 22, 62.2290, 100.0167, 50.0000, 60.0000],
    [3, 23, 77.7862, 120.0167, 50.0000, 60.0000],
    [3, 24, 93.3434, 140.0167, 50.0000, 60.0000],
    [3, 25, 108.9007, 160.0167, 50.0000, 60.0000],
    [3, 26, 124.4579, 180.0000, 50.0000, 60.0000],
    [3, 27, 140.0151, 180.0000, 50.0000, 60.0000],
    [3, 28, 155.5724, 180.0000, 50.0000, 56.2500],
    [3, 29, 171.1296, 180.0000, 50.0000, 52.3333],
    [3, 30, -999.0000, -999.0000, -99.0000, -99.0000],
    [3, 31, -999.0000, -999.0000, -99.0000, -99.0000],
    [3, 32, -999.0000, -999.0000, -99.0000, -99.0000],
    [3, 33, -999.0000, -999.0000, -99.0000, -99.0000],
    [3, 34, -999.0000, -999.0000, -99.0000, -99.0000],
    [3, 35, -999.0000, -999.0000, -99.0000, -99.0000],
    [4, 0, -999.0000, -999.0000, -99.0000, -99.0000],
    [4, 1, -999.0000, -999.0000, -99.0000, -99.0000],
    [4, 2, -999.0000, -999.0000, -99.0000, -99.0000],
    [4, 3, -999.0000, -999.0000, -99.0000, -99.0000],
    [4, 4, -180.0000, -169.6921, 40.0000, 43.7667],
    [4, 5, -180.0000, -156.6380, 40.0000, 48.1917],
    [4, 6, -180.0000, -143.5839, 40.0000, 50.0000],
    [4, 7, -171.1296, -130.5299, 40.0000, 50.0000],
    [4, 8, -155.5724, -117.4758, 40.0000, 50.0000],
    [4, 9, -140.0151, -104.4217, 40.0000, 50.0000],
    [4, 10, -124.4579, -91.3676, 40.0000, 50.0000],
    [4, 11, -108.9007, -78.3136, 40.0000, 50.0000],
    [4, 12, -93.3434, -65.2595, 40.0000, 50.0000],
    [4, 13, -77.7862, -52.2054, 40.0000, 50.0000],
    [4, 14, -62.2290, -39.1513, 40.0000, 50.0000],
    [4, 15, -46.6717, -26.0973, 40.0000, 50.0000],
    [4, 16, -31.1145, -13.0432, 40.0000, 50.0000],
    [4, 17, -15.5572, 0.0130, 40.0000, 50.0000],
    [4, 18, 0.0000, 15.5702, 40.0000, 50.0000],
    [4, 19, 13.0541, 31.1274, 40.0000, 50.0000],
    [4, 20, 26.1081, 46.6847, 40.0000, 50.0000],
    [4, 21, 39.1622, 62.2419, 40.0000, 50.0000],
    [4, 22, 52.2163, 77.7992, 40.0000, 50.0000],
    [4, 23, 65.2704, 93.3564, 40.0000, 50.0000],
    [4, 24, 78.3244, 108.9136, 40.0000, 50.0000],
    [4, 25, 91.3785, 124.4709, 40.0000, 50.0000],
    [4, 26, 104.4326, 140.0281, 40.0000, 50.0000],
    [4, 27, 117.4867, 155.5853, 40.0000, 50.0000],
    [4, 28, 130.5407, 171.1426, 40.0000, 50.0000],
    [4, 29, 143.5948, 180.0000, 40.0000, 50.0000],
    [4, 30, 156.6489, 180.0000, 40.0000, 48.1917],
    [4, 31, 169.7029, 180.0000, 40.0000, 43.7583],
    [4, 32, -999.0000, -999.0000, -99.0000, -99.0000],
    [4, 33, -999.0000, -999.0000, -99.0000, -99.0000],
    [4, 34, -999.0000, -999.0000, -99.0000, -99.0000],
    [4, 35, -999.0000, -999.0000, -99.0000, -99.0000],
    [5, 0, -999.0000, -999.0000, -99.0000, -99.0000],
    [5, 1, -999.0000, -999.0000, -99.0000, -99.0000],
    [5, 2, -180.0000, -173.1955, 30.0000, 33.5583],
    [5, 3, -180.0000, -161.6485, 30.0000, 38.9500],
    [5, 4, -180.0000, -150.1014, 30.0000, 40.0000],
    [5, 5, -169.7029, -138.5544, 30.0000, 40.0000],
    [5, 6, -156.6489, -127.0074, 30.0000, 40.0000],
    [5, 7, -143.5948, -115.4604, 30.0000, 40.0000],
    [5, 8, -130.5407, -103.9134, 30.0000, 40.0000],
    [5, 9, -117.4867, -92.3664, 30.0000, 40.0000],
    [5, 10, -104.4326, -80.8194, 30.0000, 40.0000],
    [5, 11, -91.3785, -69.2724, 30.0000, 40.0000],
    [5, 12, -78.3244, -57.7254, 30.0000, 40.0000],
    [5, 13, -65.2704, -46.1784, 30.0000, 40.0000],
    [5, 14, -52.2163, -34.6314, 30.0000, 40.0000],
    [5, 15, -39.1622, -23.0844, 30.0000, 40.0000],
    [5, 16, -26.1081, -11.5374, 30.0000, 40.0000],
    [5, 17, -13.0541, 0.0109, 30.0000, 40.0000],
    [5, 18, 0.0000, 13.0650, 30.0000, 40.0000],
    [5, 19, 11.5470, 26.1190, 30.0000, 40.0000],
    [5, 20, 23.0940, 39.1731, 30.0000, 40.0000],
    [5, 21, 34.6410, 52.2272, 30.0000, 40.0000],
    [5, 22, 46.1880, 65.2812, 30.0000, 40.0000],
    [5, 23, 57.7350, 78.3353, 30.0000, 40.0000],
    [5, 24, 69.2820, 91.3894, 30.0000, 40.0000],
    [5, 25, 80.8290, 104.4435, 30.0000, 40.0000],
    [5, 26, 92.3760, 117.4975, 30.0000, 40.0000],
    [5, 27, 103.9230, 130.5516, 30.0000, 40.0000],
    [5, 28, 115.4701, 143.6057, 30.0000, 40.0000],
    [5, 29, 127.0171, 156.6598, 30.0000, 40.0000],
    [5, 30, 138.5641, 169.7138, 30.0000, 40.0000],
    [5, 31, 150.1111, 180.0000, 30.0000, 40.0000],
    [5, 32, 161.6581, 180.0000, 30.0000, 38.9417],
    [5, 33, 173.2051, 180.0000, 30.0000, 33.5583],
    [5, 34, -999.0000, -999.0000, -99.0000, -99.0000],
    [5, 35, -999.0000, -999.0000, -99.0000, -99.0000],
    [6, 0, -999.0000, -999.0000, -99.0000, -99.0000],
    [6, 1, -180.0000, -170.2596, 20.0000, 27.2667],
    [6, 2, -180.0000, -159.6178, 20.0000, 30.0000],
    [6, 3, -173.2051, -148.9760, 20.0000, 30.0000],
    [6, 4, -161.6581, -138.3342, 20.0000, 30.0000],
    [6, 5, -150.1111, -127.6925, 20.0000, 30.0000],
    [6, 6, -138.5641, -117.0507, 20.0000, 30.0000],
    [6, 7, -127.0171, -106.4089, 20.0000, 30.0000],
    [6, 8, -115.4701, -95.7671, 20.0000, 30.0000],
    [6, 9, -103.9230, -85.1254, 20.0000, 30.0000],
    [6, 10, -92.3760, -74.4836, 20.0000, 30.0000],
    [6, 11, -80.8290, -63.8418, 20.0000, 30.0000],
    [6, 12, -69.2820, -53.2000, 20.0000, 30.0000],
    [6, 13, -57.7350, -42.5582, 20.0000, 30.0000],
    [6, 14, -46.1880, -31.9165, 20.0000, 30.0000],
    [6, 15, -34.6410, -21.2747, 20.0000, 30.0000],
    [6, 16, -23.0940, -10.6329, 20.0000, 30.0000],
    [6, 17, -11.5470, 0.0096, 20.0000, 30.0000],
    [6, 18, 0.0000, 11.5566, 20.0000, 30.0000],
    [6, 19, 10.6418, 23.1036, 20.0000, 30.0000],
    [6, 20, 21.2836, 34.6506, 20.0000, 30.0000],
    [6, 21, 31.9253, 46.1976, 20.0000, 30.0000],
    [6, 22, 42.5671, 57.7446, 20.0000, 30.0000],
    [6, 23, 53.2089, 69.2917, 20.0000, 30.0000],
    [6, 24, 63.8507, 80.8387, 20.0000, 30.0000],
    [6, 25, 74.4924, 92.3857, 20.0000, 30.0000],
    [6, 26, 85.1342, 103.9327, 20.0000, 30.0000],
    [6, 27, 95.7760, 115.4797, 20.0000, 30.0000],
    [6, 28, 106.4178, 127.0267, 20.0000, 30.0000],
    [6, 29, 117.0596, 138.5737, 20.0000, 30.0000],
    [6, 30, 127.7013, 150.1207, 20.0000, 30.0000],
    [6, 31, 138.3431, 161.6677, 20.0000, 30.0000],
    [6, 32, 148.9849, 173.2147, 20.0000, 30.0000],
    [6, 33, 159.6267, 180.0000, 20.0000, 30.0000],
    [6, 34, 170.2684, 180.0000, 20.0000, 27.2667],
    [6, 35, -999.0000, -999.0000, -99.0000, -99.0000],
    [7, 0, -180.0000, -172.6141, 10.0000, 19.1917],
    [7, 1, -180.0000, -162.4598, 10.0000, 20.0000],
    [7, 2, -170.2684, -152.3055, 10.0000, 20.0000],
    [7, 3, -159.6267, -142.1513, 10.0000, 20.0000],
    [7, 4, -148.9849, -131.9970, 10.0000, 20.0000],
    [7, 5, -138.3431, -121.8427, 10.0000, 20.0000],
    [7, 6, -127.7013, -111.6885, 10.0000, 20.0000],
    [7, 7, -117.0596, -101.5342, 10.0000, 20.0000],
    [7, 8, -106.4178, -91.3799, 10.0000, 20.0000],
    [7, 9, -95.7760, -81.2257, 10.0000, 20.0000],
    [7, 10, -85.1342, -71.0714, 10.0000, 20.0000],
    [7, 11, -74.4924, -60.9171, 10.0000, 20.0000],
    [7, 12, -63.8507, -50.7629, 10.0000, 20.0000],
    [7, 13, -53.2089, -40.6086, 10.0000, 20.0000],
    [7, 14, -42.5671, -30.4543, 10.0000, 20.0000],
    [7, 15, -31.9253, -20.3001, 10.0000, 20.0000],
    [7, 16, -21.2836, -10.1458, 10.0000, 20.0000],
    [7, 17, -10.6418, 0.0089, 10.0000, 20.0000],
    [7, 18, 0.0000, 10.6506, 10.0000, 20.0000],
    [7, 19, 10.1543, 21.2924, 10.0000, 20.0000],
    [7, 20, 20.3085, 31.9342, 10.0000, 20.0000],
    [7, 21, 30.4628, 42.5760, 10.0000, 20.0000],
    [7, 22, 40.6171, 53.2178, 10.0000, 20.0000],
    [7, 23, 50.7713, 63.8595, 10.0000, 20.0000],
    [7, 24, 60.9256, 74.5013, 10.0000, 20.0000],
    [7, 25, 71.0799, 85.1431, 10.0000, 20.0000],
    [7, 26, 81.2341, 95.7849, 10.0000, 20.0000],
    [7, 27, 91.3884, 106.4266, 10.0000, 20.0000],
    [7, 28, 101.5427, 117.0684, 10.0000, 20.0000],
    [7, 29, 111.6969, 127.7102, 10.0000, 20.0000],
    [7, 30, 121.8512, 138.3520, 10.0000, 20.0000],
    [7, 31, 132.0055, 148.9938, 10.0000, 20.0000],
    [7, 32, 142.1597, 159.6355, 10.0000, 20.0000],
    [7, 33, 152.3140, 170.2773, 10.0000, 20.0000],
    [7, 34, 162.4683, 180.0000, 10.0000, 20.0000],
    [7, 35, 172.6225, 180.0000, 10.0000, 19.1833],
    [8, 0, -180.0000, -169.9917, -0.0000, 10.0000],
    [8, 1, -172.6225, -159.9917, -0.0000, 10.0000],
    [8, 2, -162.4683, -149.9917, -0.0000, 10.0000],
    [8, 3, -152.3140, -139.9917, -0.0000, 10.0000],
    [8, 4, -142.1597, -129.9917, -0.0000, 10.0000],
    [8, 5, -132.0055, -119.9917, -0.0000, 10.0000],
    [8, 6, -121.8512, -109.9917, -0.0000, 10.0000],
    [8, 7, -111.6969, -99.9917, -0.0000, 10.0000],
    [8, 8, -101.5427, -89.9917, -0.0000, 10.0000],
    [8, 9, -91.3884, -79.9917, -0.0000, 10.0000],
    [8, 10, -81.2341, -69.9917, -0.0000, 10.0000],
    [8, 11, -71.0799, -59.9917, -0.0000, 10.0000],
    [8, 12, -60.9256, -49.9917, -0.0000, 10.0000],
    [8, 13, -50.7713, -39.9917, -0.0000, 10.0000],
    [8, 14, -40.6171, -29.9917, -0.0000, 10.0000],
    [8, 15, -30.4628, -19.9917, -0.0000, 10.0000],
    [8, 16, -20.3085, -9.9917, -0.0000, 10.0000],
    [8, 17, -10.1543, 0.0085, -0.0000, 10.0000],
    [8, 18, 0.0000, 10.1627, -0.0000, 10.0000],
    [8, 19, 10.0000, 20.3170, -0.0000, 10.0000],
    [8, 20, 20.0000, 30.4713, -0.0000, 10.0000],
    [8, 21, 30.0000, 40.6255, -0.0000, 10.0000],
    [8, 22, 40.0000, 50.7798, -0.0000, 10.0000],
    [8, 23, 50.0000, 60.9341, -0.0000, 10.0000],
    [8, 24, 60.0000, 71.0883, -0.0000, 10.0000],
    [8, 25, 70.0000, 81.2426, -0.0000, 10.0000],
    [8, 26, 80.0000, 91.3969, -0.0000, 10.0000],
    [8, 27, 90.0000, 101.5511, -0.0000, 10.0000],
    [8, 28, 100.0000, 111.7054, -0.0000, 10.0000],
    [8, 29, 110.0000, 121.8597, -0.0000, 10.0000],
    [8, 30, 120.0000, 132.0139, -0.0000, 10.0000],
    [8, 31, 130.0000, 142.1682, -0.0000, 10.0000],
    [8, 32, 140.0000, 152.3225, -0.0000, 10.0000],
    [8, 33, 150.0000, 162.4767, -0.0000, 10.0000],
    [8, 34, 160.0000, 172.6310, -0.0000, 10.0000],
    [8, 35, 170.0000, 180.0000, -0.0000, 10.0000],
    [9, 0, -180.0000, -169.9917, -10.0000, -0.0000],
    [9, 1, -172.6225, -159.9917, -10.0000, -0.0000],
    [9, 2, -162.4683, -149.9917, -10.0000, -0.0000],
    [9, 3, -152.3140, -139.9917, -10.0000, -0.0000],
    [9, 4, -142.1597, -129.9917, -10.0000, -0.0000],
    [9, 5, -132.0055, -119.9917, -10.0000, -0.0000],
    [9, 6, -121.8512, -109.9917, -10.0000, -0.0000],
    [9, 7, -111.6969, -99.9917, -10.0000, -0.0000],
    [9, 8, -101.5427, -89.9917, -10.0000, -0.0000],
    [9, 9, -91.3884, -79.9917, -10.0000, -0.0000],
    [9, 10, -81.2341, -69.9917, -10.0000, -0.0000],
    [9, 11, -71.0799, -59.9917, -10.0000, -0.0000],
    [9, 12, -60.9256, -49.9917, -10.0000, -0.0000],
    [9, 13, -50.7713, -39.9917, -10.0000, -0.0000],
    [9, 14, -40.6171, -29.9917, -10.0000, -0.0000],
    [9, 15, -30.4628, -19.9917, -10.0000, -0.0000],
    [9, 16, -20.3085, -9.9917, -10.0000, -0.0000],
    [9, 17, -10.1543, 0.0085, -10.0000, -0.0000],
    [9, 18, 0.0000, 10.1627, -10.0000, -0.0000],
    [9, 19, 10.0000, 20.3170, -10.0000, -0.0000],
    [9, 20, 20.0000, 30.4713, -10.0000, -0.0000],
    [9, 21, 30.0000, 40.6255, -10.0000, -0.0000],
    [9, 22, 40.0000, 50.7798, -10.0000, -0.0000],
    [9, 23, 50.0000, 60.9341, -10.0000, -0.0000],
    [9, 24, 60.0000, 71.0883, -10.0000, -0.0000],
    [9, 25, 70.0000, 81.2426, -10.0000, -0.0000],
    [9, 26, 80.0000, 91.3969, -10.0000, -0.0000],
    [9, 27, 90.0000, 101.5511, -10.0000, -0.0000],
    [9, 28, 100.0000, 111.7054, -10.0000, -0.0000],
    [9, 29, 110.0000, 121.8597, -10.0000, -0.0000],
    [9, 30, 120.0000, 132.0139, -10.0000, -0.0000],
    [9, 31, 130.0000, 142.1682, -10.0000, -0.0000],
    [9, 32, 140.0000, 152.3225, -10.0000, -0.0000],
    [9, 33, 150.0000, 162.4767, -10.0000, -0.0000],
    [9, 34, 160.0000, 172.6310, -10.0000, -0.0000],
    [9, 35, 170.0000, 180.0000, -10.0000, -0.0000],
    [10, 0, -180.0000, -172.6141, -19.1917, -10.0000],
    [10, 1, -180.0000, -162.4598, -20.0000, -10.0000],
    [10, 2, -170.2684, -152.3055, -20.0000, -10.0000],
    [10, 3, -159.6267, -142.1513, -20.0000, -10.0000],
    [10, 4, -148.9849, -131.9970, -20.0000, -10.0000],
    [10, 5, -138.3431, -121.8427, -20.0000, -10.0000],
    [10, 6, -127.7013, -111.6885, -20.0000, -10.0000],
    [10, 7, -117.0596, -101.5342, -20.0000, -10.0000],
    [10, 8, -106.4178, -91.3799, -20.0000, -10.0000],
    [10, 9, -95.7760, -81.2257, -20.0000, -10.0000],
    [10, 10, -85.1342, -71.0714, -20.0000, -10.0000],
    [10, 11, -74.4924, -60.9171, -20.0000, -10.0000],
    [10, 12, -63.8507, -50.7629, -20.0000, -10.0000],
    [10, 13, -53.2089, -40.6086, -20.0000, -10.0000],
    [10, 14, -42.5671, -30.4543, -20.0000, -10.0000],
    [10, 15, -31.9253, -20.3001, -20.0000, -10.0000],
    [10, 16, -21.2836, -10.1458, -20.0000, -10.0000],
    [10, 17, -10.6418, 0.0089, -20.0000, -10.0000],
    [10, 18, 0.0000, 10.6506, -20.0000, -10.0000],
    [10, 19, 10.1543, 21.2924, -20.0000, -10.0000],
    [10, 20, 20.3085, 31.9342, -20.0000, -10.0000],
    [10, 21, 30.4628, 42.5760, -20.0000, -10.0000],
    [10, 22, 40.6171, 53.2178, -20.0000, -10.0000],
    [10, 23, 50.7713, 63.8595, -20.0000, -10.0000],
    [10, 24, 60.9256, 74.5013, -20.0000, -10.0000],
    [10, 25, 71.0799, 85.1431, -20.0000, -10.0000],
    [10, 26, 81.2341, 95.7849, -20.0000, -10.0000],
    [10, 27, 91.3884, 106.4266, -20.0000, -10.0000],
    [10, 28, 101.5427, 117.0684, -20.0000, -10.0000],
    [10, 29, 111.6969, 127.7102, -20.0000, -10.0000],
    [10, 30, 121.8512, 138.3520, -20.0000, -10.0000],
    [10, 31, 132.0055, 148.9938, -20.0000, -10.0000],
    [10, 32, 142.1597, 159.6355, -20.0000, -10.0000],
    [10, 33, 152.3140, 170.2773, -20.0000, -10.0000],
    [10, 34, 162.4683, 180.0000, -20.0000, -10.0000],
    [10, 35, 172.6225, 180.0000, -19.1833, -10.0000],
    [11, 0, -999.0000, -999.0000, -99.0000, -99.0000],
    [11, 1, -180.0000, -170.2596, -27.2667, -20.0000],
    [11, 2, -180.0000, -159.6178, -30.0000, -20.0000],
    [11, 3, -173.2051, -148.9760, -30.0000, -20.0000],
    [11, 4, -161.6581, -138.3342, -30.0000, -20.0000],
    [11, 5, -150.1111, -127.6925, -30.0000, -20.0000],
    [11, 6, -138.5641, -117.0507, -30.0000, -20.0000],
    [11, 7, -127.0171, -106.4089, -30.0000, -20.0000],
    [11, 8, -115.4701, -95.7671, -30.0000, -20.0000],
    [11, 9, -103.9230, -85.1254, -30.0000, -20.0000],
    [11, 10, -92.3760, -74.4836, -30.0000, -20.0000],
    [11, 11, -80.8290, -63.8418, -30.0000, -20.0000],
    [11, 12, -69.2820, -53.2000, -30.0000, -20.0000],
    [11, 13, -57.7350, -42.5582, -30.0000, -20.0000],
    [11, 14, -46.1880, -31.9165, -30.0000, -20.0000],
    [11, 15, -34.6410, -21.2747, -30.0000, -20.0000],
    [11, 16, -23.0940, -10.6329, -30.0000, -20.0000],
    [11, 17, -11.5470, 0.0096, -30.0000, -20.0000],
    [11, 18, 0.0000, 11.5566, -30.0000, -20.0000],
    [11, 19, 10.6418, 23.1036, -30.0000, -20.0000],
    [11, 20, 21.2836, 34.6506, -30.0000, -20.0000],
    [11, 21, 31.9253, 46.1976, -30.0000, -20.0000],
    [11, 22, 42.5671, 57.7446, -30.0000, -20.0000],
    [11, 23, 53.2089, 69.2917, -30.0000, -20.0000],
    [11, 24, 63.8507, 80.8387, -30.0000, -20.0000],
    [11, 25, 74.4924, 92.3857, -30.0000, -20.0000],
    [11, 26, 85.1342, 103.9327, -30.0000, -20.0000],
    [11, 27, 95.7760, 115.4797, -30.0000, -20.0000],
    [11, 28, 106.4178, 127.0267, -30.0000, -20.0000],
    [11, 29, 117.0596, 138.5737, -30.0000, -20.0000],
    [11, 30, 127.7013, 150.1207, -30.0000, -20.0000],
    [11, 31, 138.3431, 161.6677, -30.0000, -20.0000],
    [11, 32, 148.9849, 173.2147, -30.0000, -20.0000],
    [11, 33, 159.6267, 180.0000, -30.0000, -20.0000],
    [11, 34, 170.2684, 180.0000, -27.2667, -20.0000],
    [11, 35, -999.0000, -999.0000, -99.0000, -99.0000],
    [12, 0, -999.0000, -999.0000, -99.0000, -99.0000],
    [12, 1, -999.0000, -999.0000, -99.0000, -99.0000],
    [12, 2, -180.0000, -173.1955, -33.5583, -30.0000],
    [12, 3, -180.0000, -161.6485, -38.9500, -30.0000],
    [12, 4, -180.0000, -150.1014, -40.0000, -30.0000],
    [12, 5, -169.7029, -138.5544, -40.0000, -30.0000],
    [12, 6, -156.6489, -127.0074, -40.0000, -30.0000],
    [12, 7, -143.5948, -115.4604, -40.0000, -30.0000],
    [12, 8, -130.5407, -103.9134, -40.0000, -30.0000],
    [12, 9, -117.4867, -92.3664, -40.0000, -30.0000],
    [12, 10, -104.4326, -80.8194, -40.0000, -30.0000],
    [12, 11, -91.3785, -69.2724, -40.0000, -30.0000],
    [12, 12, -78.3244, -57.7254, -40.0000, -30.0000],
    [12, 13, -65.2704, -46.1784, -40.0000, -30.0000],
    [12, 14, -52.2163, -34.6314, -40.0000, -30.0000],
    [12, 15, -39.1622, -23.0844, -40.0000, -30.0000],
    [12, 16, -26.1081, -11.5374, -40.0000, -30.0000],
    [12, 17, -13.0541, 0.0109, -40.0000, -30.0000],
    [12, 18, 0.0000, 13.0650, -40.0000, -30.0000],
    [12, 19, 11.5470, 26.1190, -40.0000, -30.0000],
    [12, 20, 23.0940, 39.1731, -40.0000, -30.0000],
    [12, 21, 34.6410, 52.2272, -40.0000, -30.0000],
    [12, 22, 46.1880, 65.2812, -40.0000, -30.0000],
    [12, 23, 57.7350, 78.3353, -40.0000, -30.0000],
    [12, 24, 69.2820, 91.3894, -40.0000, -30.0000],
    [12, 25, 80.8290, 104.4435, -40.0000, -30.0000],
    [12, 26, 92.3760, 117.4975, -40.0000, -30.0000],
    [12, 27, 103.9230, 130.5516, -40.0000, -30.0000],
    [12, 28, 115.4701, 143.6057, -40.0000, -30.0000],
    [12, 29, 127.0171, 156.6598, -40.0000, -30.0000],
    [12, 30, 138.5641, 169.7138, -40.0000, -30.0000],
    [12, 31, 150.1111, 180.0000, -40.0000, -30.0000],
    [12, 32, 161.6581, 180.0000, -38.9417, -30.0000],
    [12, 33, 173.2051, 180.0000, -33.5583, -30.0000],
    [12, 34, -999.0000, -999.0000, -99.0000, -99.0000],
    [12, 35, -999.0000, -999.0000, -99.0000, -99.0000],
    [13, 0, -999.0000, -999.0000, -99.0000, -99.0000],
    [13, 1, -999.0000, -999.0000, -99.0000, -99.0000],
    [13, 2, -999.0000, -999.0000, -99.0000, -99.0000],
    [13, 3, -999.0000, -999.0000, -99.0000, -99.0000],
    [13, 4, -180.0000, -169.6921, -43.7667, -40.0000],
    [13, 5, -180.0000, -156.6380, -48.1917, -40.0000],
    [13, 6, -180.0000, -143.5839, -50.0000, -40.0000],
    [13, 7, -171.1296, -130.5299, -50.0000, -40.0000],
    [13, 8, -155.5724, -117.4758, -50.0000, -40.0000],
    [13, 9, -140.0151, -104.4217, -50.0000, -40.0000],
    [13, 10, -124.4579, -91.3676, -50.0000, -40.0000],
    [13, 11, -108.9007, -78.3136, -50.0000, -40.0000],
    [13, 12, -93.3434, -65.2595, -50.0000, -40.0000],
    [13, 13, -77.7862, -52.2054, -50.0000, -40.0000],
    [13, 14, -62.2290, -39.1513, -50.0000, -40.0000],
    [13, 15, -46.6717, -26.0973, -50.0000, -40.0000],
    [13, 16, -31.1145, -13.0432, -50.0000, -40.0000],
    [13, 17, -15.5572, 0.0130, -50.0000, -40.0000],
    [13, 18, 0.0000, 15.5702, -50.0000, -40.0000],
    [13, 19, 13.0541, 31.1274, -50.0000, -40.0000],
    [13, 20, 26.1081, 46.6847, -50.0000, -40.0000],
    [13, 21, 39.1622, 62.2419, -50.0000, -40.0000],
    [13, 22, 52.2163, 77.7992, -50.0000, -40.0000],
    [13, 23, 65.2704, 93.3564, -50.0000, -40.0000],
    [13, 24, 78.3244, 108.9136, -50.0000, -40.0000],
    [13, 25, 91.3785, 124.4709, -50.0000, -40.0000],
    [13, 26, 104.4326, 140.0281, -50.0000, -40.0000],
    [13, 27, 117.4867, 155.5853, -50.0000, -40.0000],
    [13, 28, 130.5407, 171.1426, -50.0000, -40.0000],
    [13, 29, 143.5948, 180.0000, -50.0000, -40.0000],
    [13, 30, 156.6489, 180.0000, -48.1917, -40.0000],
    [13, 31, 169.7029, 180.0000, -43.7583, -40.0000],
    [13, 32, -999.0000, -999.0000, -99.0000, -99.0000],
    [13, 33, -999.0000, -999.0000, -99.0000, -99.0000],
    [13, 34, -999.0000, -999.0000, -99.0000, -99.0000],
    [13, 35, -999.0000, -999.0000, -99.0000, -99.0000],
    [14, 0, -999.0000, -999.0000, -99.0000, -99.0000],
    [14, 1, -999.0000, -999.0000, -99.0000, -99.0000],
    [14, 2, -999.0000, -999.0000, -99.0000, -99.0000],
    [14, 3, -999.0000, -999.0000, -99.0000, -99.0000],
    [14, 4, -999.0000, -999.0000, -99.0000, -99.0000],
    [14, 5, -999.0000, -999.0000, -99.0000, -99.0000],
    [14, 6, -180.0000, -171.1167, -52.3333, -50.0000],
    [14, 7, -180.0000, -155.5594, -56.2583, -50.0000],
    [14, 8, -180.0000, -140.0022, -60.0000, -50.0000],
    [14, 9, -180.0000, -124.4449, -60.0000, -50.0000],
    [14, 10, -160.0000, -108.8877, -60.0000, -50.0000],
    [14, 11, -140.0000, -93.3305, -60.0000, -50.0000],
    [14, 12, -120.0000, -77.7732, -60.0000, -50.0000],
    [14, 13, -100.0000, -62.2160, -60.0000, -50.0000],
    [14, 14, -80.0000, -46.6588, -60.0000, -50.0000],
    [14, 15, -60.0000, -31.1015, -60.0000, -50.0000],
    [14, 16, -40.0000, -15.5443, -60.0000, -50.0000],
    [14, 17, -20.0000, 0.0167, -60.0000, -50.0000],
    [14, 18, 0.0000, 20.0167, -60.0000, -50.0000],
    [14, 19, 15.5572, 40.0167, -60.0000, -50.0000],
    [14, 20, 31.1145, 60.0167, -60.0000, -50.0000],
    [14, 21, 46.6717, 80.0167, -60.0000, -50.0000],
    [14, 22, 62.2290, 100.0167, -60.0000, -50.0000],
    [14, 23, 77.7862, 120.0167, -60.0000, -50.0000],
    [14, 24, 93.3434, 140.0167, -60.0000, -50.0000],
    [14, 25, 108.9007, 160.0167, -60.0000, -50.0000],
    [14, 26, 124.4579, 180.0000, -60.0000, -50.0000],
    [14, 27, 140.0151, 180.0000, -60.0000, -50.0000],
    [14, 28, 155.5724, 180.0000, -56.2500, -50.0000],
    [14, 29, 171.1296, 180.0000, -52.3333, -50.0000],
    [14, 30, -999.0000, -999.0000, -99.0000, -99.0000],
    [14, 31, -999.0000, -999.0000, -99.0000, -99.0000],
    [14, 32, -999.0000, -999.0000, -99.0000, -99.0000],
    [14, 33, -999.0000, -999.0000, -99.0000, -99.0000],
    [14, 34, -999.0000, -999.0000, -99.0000, -99.0000],
    [14, 35, -999.0000, -999.0000, -99.0000, -99.0000],
    [15, 0, -999.0000, -999.0000, -99.0000, -99.0000],
    [15, 1, -999.0000, -999.0000, -99.0000, -99.0000],
    [15, 2, -999.0000, -999.0000, -99.0000, -99.0000],
    [15, 3, -999.0000, -999.0000, -99.0000, -99.0000],
    [15, 4, -999.0000, -999.0000, -99.0000, -99.0000],
    [15, 5, -999.0000, -999.0000, -99.0000, -99.0000],
    [15, 6, -999.0000, -999.0000, -99.0000, -99.0000],
    [15, 7, -999.0000, -999.0000, -99.0000, -99.0000],
    [15, 8, -999.0000, -999.0000, -99.0000, -99.0000],
    [15, 9, -180.0000, -159.9833, -63.6167, -60.0000],
    [15, 10, -180.0000, -139.9833, -67.1167, -60.0000],
    [15, 11, -180.0000, -119.9833, -70.0000, -60.0000],
    [15, 12, -175.4283, -99.9833, -70.0000, -60.0000],
    [15, 13, -146.1902, -79.9833, -70.0000, -60.0000],
    [15, 14, -116.9522, -59.9833, -70.0000, -60.0000],
    [15, 15, -87.7141, -39.9833, -70.0000, -60.0000],
    [15, 16, -58.4761, -19.9833, -70.0000, -60.0000],
    [15, 17, -29.2380, 0.0244, -70.0000, -60.0000],
    [15, 18, 0.0000, 29.2624, -70.0000, -60.0000],
    [15, 19, 20.0000, 58.5005, -70.0000, -60.0000],
    [15, 20, 40.0000, 87.7385, -70.0000, -60.0000],
    [15, 21, 60.0000, 116.9765, -70.0000, -60.0000],
    [15, 22, 80.0000, 146.2146, -70.0000, -60.0000],
    [15, 23, 100.0000, 175.4526, -70.0000, -60.0000],
    [15, 24, 120.0000, 180.0000, -70.0000, -60.0000],
    [15, 25, 140.0000, 180.0000, -67.1167, -60.0000],
    [15, 26, 160.0000, 180.0000, -63.6167, -60.0000],
    [15, 27, -999.0000, -999.0000, -99.0000, -99.0000],
    [15, 28, -999.0000, -999.0000, -99.0000, -99.0000],
    [15, 29, -999.0000, -999.0000, -99.0000, -99.0000],
    [15, 30, -999.0000, -999.0000, -99.0000, -99.0000],
    [15, 31, -999.0000, -999.0000, -99.0000, -99.0000],
    [15, 32, -999.0000, -999.0000, -99.0000, -99.0000],
    [15, 33, -999.0000, -999.0000, -99.0000, -99.0000],
    [15, 34, -999.0000, -999.0000, -99.0000, -99.0000],
    [15, 35, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 0, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 1, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 2, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 3, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 4, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 5, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 6, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 7, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 8, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 9, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 10, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 11, -180.0000, -175.4039, -70.5333, -70.0000],
    [16, 12, -180.0000, -146.1659, -73.8750, -70.0000],
    [16, 13, -180.0000, -116.9278, -77.1667, -70.0000],
    [16, 14, -180.0000, -87.6898, -80.0000, -70.0000],
    [16, 15, -172.7631, -58.4517, -80.0000, -70.0000],
    [16, 16, -115.1754, -29.2137, -80.0000, -70.0000],
    [16, 17, -57.5877, 0.0480, -80.0000, -70.0000],
    [16, 18, 0.0000, 57.6357, -80.0000, -70.0000],
    [16, 19, 29.2380, 115.2234, -80.0000, -70.0000],
    [16, 20, 58.4761, 172.8111, -80.0000, -70.0000],
    [16, 21, 87.7141, 180.0000, -80.0000, -70.0000],
    [16, 22, 116.9522, 180.0000, -77.1583, -70.0000],
    [16, 23, 146.1902, 180.0000, -73.8750, -70.0000],
    [16, 24, 175.4283, 180.0000, -70.5333, -70.0000],
    [16, 25, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 26, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 27, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 28, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 29, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 30, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 31, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 32, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 33, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 34, -999.0000, -999.0000, -99.0000, -99.0000],
    [16, 35, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 0, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 1, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 2, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 3, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 4, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 5, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 6, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 7, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 8, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 9, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 10, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 11, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 12, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 13, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 14, -180.0000, -172.7151, -80.4083, -80.0000],
    [17, 15, -180.0000, -115.1274, -83.6250, -80.0000],
    [17, 16, -180.0000, -57.5397, -86.8167, -80.0000],
    [17, 17, -180.0000, 57.2957, -90.0000, -80.0000],
    [17, 18, -0.0040, 180.0000, -90.0000, -80.0000],
    [17, 19, 57.5877, 180.0000, -86.8167, -80.0000],
    [17, 20, 115.1754, 180.0000, -83.6250, -80.0000],
    [17, 21, 172.7631, 180.0000, -80.4083, -80.0000],
    [17, 22, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 23, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 24, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 25, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 26, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 27, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 28, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 29, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 30, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 31, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 32, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 33, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 34, -999.0000, -999.0000, -99.0000, -99.0000],
    [17, 35, -999.0000, -999.0000, -99.0000, -99.0000]
]


def find_tiles(bbox):
    """uses tile list and bounding box to identify modis tiles
        intersecting bounding box
    Parameters
    ---------
        bbox: array
            array of bounding box coordinates
                [lat_min, lon_min, lat_max, lon_max]
    Returns
    -------
        ids_fmt: array
            array of modis tiles in 'hXXvXX' format

    Notes
    -----
    Requires gdal_path be set in config ini file and gdal be available

    Returns the tile IDs that need to be downloaded for
    a given region bounded by *bbox*.

    """
    logger.info("find_tiles: finding modis tiles for provided spatial area")
    def intersects(bbox, tile):
        if tile[2] != -999.0 and tile[3] != -999.0 and tile[4] != -99.0 and tile[5] != -99.0:
            tiler = ogr.Geometry(ogr.wkbLinearRing)
            tiler.AddPoint(tile[2], tile[4])
            tiler.AddPoint(tile[3], tile[4])
            tiler.AddPoint(tile[3], tile[5])
            tiler.AddPoint(tile[2], tile[5])
            tiler.AddPoint(tile[2], tile[4])
            polyr = ogr.Geometry(ogr.wkbPolygon)
            polyr.AddGeometry(tiler)
            bboxr = ogr.Geometry(ogr.wkbLinearRing)
            bboxr.AddPoint(bbox[1], bbox[0])
            bboxr.AddPoint(bbox[1], bbox[2])
            bboxr.AddPoint(bbox[3], bbox[2])
            bboxr.AddPoint(bbox[3], bbox[0])
            bboxr.AddPoint(bbox[1], bbox[0])
            polyb = ogr.Geometry(ogr.wkbPolygon)
            polyb.AddGeometry(bboxr)
            return polyr.Intersects(polyb)
        else:
            return False
    if bbox == None:
        logger.error("find_tiles: error with spatial area")
        ids = None
    else:
        ids = [(t[0], t[1]) for t in tiles if intersects(bbox, t)]
        ids_fmt = ["h" + "{:02d}".format(t[1]) + "v" + "{:02d}".format(t[0]) for t in ids]
    return ids_fmt

def get_credentials():
    """Build user credentials from config file username and password

    Parameters
    ---------
        None
    Returns
    -------
        credentials: array
            encoded array of username and password

    Notes
    -----
    Code modified from NSIDC retrieval script

    """
    credentials = None
    errprefix = ''

    credentials = '{0}:{1}'.format(cfg.username_earthdata, cfg.password_earthdata)
    credentials = base64.b64encode(credentials.encode('ascii')).decode('ascii')

    return credentials


def build_version_query_params(version):
    """Build API version query

    Parameters
    ---------
        version: string
            dataset version number
    Returns
    -------
        query_params: array
            API formatted filter for version number

    Notes
    -----
    Code modified from NSIDC retrieval script

    """
    desired_pad_length = 3
    if len(version) > desired_pad_length:
        print('Version string too long: "{0}"'.format(version))
        quit()

    version = str(int(version))  # Strip off any leading zeros
    query_params = ''

    while len(version) <= desired_pad_length:
        padded_version = version.zfill(desired_pad_length)
        query_params += '&version={0}'.format(padded_version)
        desired_pad_length -= 1
    return query_params

def filter_add_wildcards(filter):
    """Build API wildcards

    Parameters
    ---------
        filter: string
            data filter
    Returns
    -------
        filter: array
            API formatted filter
    Notes
    -----
    Code modified from NSIDC retrieval script

        """

    if not filter.startswith('*'):
        filter = '*' + filter
    if not filter.endswith('*'):
        filter = filter + '*'
    return filter


def build_filename_filter(filename_filter):
    """Build API filename filter

    Parameters
    ---------
        filename_filter: string
            filename filter
    Returns
    -------
        result: array
            API formatted filename filter
    Notes
    -----
    Code modified from NSIDC retrieval script

        """

    filters = filename_filter.split(',')
    result = '&options[producer_granule_id][pattern]=true'
    for filter in filters:
        result += '&producer_granule_id[]=' + filter_add_wildcards(filter)
    return result


def build_cmr_query_url(short_name, version, time_start, time_end,
                        bounding_box=None, polygon=None,
                        filename_filter=None):

    """Build NASA Earth Observing System (EOS) Common Metadata Repository (CMR)
        query

    Parameters
    ---------
        short_name: string
            dataset short name
        version: string
            dataset version number
        time_start: datetime
            start datetime for data filter
        time_end: datetime
            end datetime for data filter
        bounding_box: string
            cmd_string of bounding box coordinates
                'lon_min,lat_min,lon_max,lat_max'
                optional, but required if polygon not provided
        polygon: string
            string of points defining polygon
                'lon1,lat1,lon2,lat2,...,lonN,latN'
                optional, but required if bounding_box not provided
    Returns
    -------
        CMR_FILE_URL + params: string
            API query
    Notes
    -----
    Code modified from NSIDC retrieval script

    """
    CMR_URL = 'https://cmr.earthdata.nasa.gov'
    URS_URL = 'https://urs.earthdata.nasa.gov'
    CMR_PAGE_SIZE = 2000
    CMR_FILE_URL = ('{0}/search/granules.json?provider=NSIDC_ECS'
                    '&sort_key[]=start_date&sort_key[]=producer_granule_id'
                    '&scroll=true&page_size={1}'.format(CMR_URL, CMR_PAGE_SIZE))

    params = '&short_name={0}'.format(short_name)
    params += build_version_query_params(version)
    params += '&temporal[]={0},{1}'.format(time_start, time_end)
    if polygon:
        params += '&polygon={0}'.format(polygon)
    elif bounding_box:
        params += '&bounding_box={0}'.format(bounding_box)
    if filename_filter:
        params += build_filename_filter(filename_filter)
    return CMR_FILE_URL + params


def cmr_download(urls):
    """Download files from list of urls.

    Parameters
    ---------
        urls: list
            list of urls to download

    Returns
    -------
        None
    Notes
    -----
    Code modified from NSIDC retrieval script

    """
    if not urls:
        return

    url_count = len(urls)
    print('Downloading {0} files...'.format(url_count))
    credentials = None

    for index, url in enumerate(urls, start=1):
        if not credentials and urlparse(url).scheme == 'https':
            credentials = get_credentials()

        filename = url.split('/')[-1]
        print('{0}/{1}: {2}'.format(str(index).zfill(len(str(url_count))),
                                    url_count,
                                    filename))

        try:
            # In Python 3 we could eliminate the opener and just do 2 lines:
            # resp = requests.get(url, auth=(username, password))
            # open(filename, 'wb').write(resp.content)
            req = Request(url)
            if credentials:
                req.add_header('Authorization', 'Basic {0}'.format(credentials))
            opener = build_opener(HTTPCookieProcessor())
            data = opener.open(req).read()
            open(filename, 'wb').write(data)
        except HTTPError as e:
            print('HTTP error {0}, {1}'.format(e.code, e.reason))
        except URLError as e:
            print('URL error: {0}'.format(e.reason))
        except IOError:
            raise


def cmr_filter_urls(search_results):
    """Select only the desired data files from CMR response.

    Parameters
    ---------
        search_results: list
            list of search results

    Returns
    -------
        urls: list
            filtered list of urls
    Notes
    -----
    Code modified from NSIDC retrieval script

    """
    if 'feed' not in search_results or 'entry' not in search_results['feed']:
        return []

    entries = [e['links']
               for e in search_results['feed']['entry']
               if 'links' in e]
    # Flatten "entries" to a simple list of links
    links = list(itertools.chain(*entries))

    urls = []
    unique_filenames = set()
    for link in links:
        if 'href' not in link:
            # Exclude links with nothing to download
            continue
        if 'inherited' in link and link['inherited'] is True:
            # Why are we excluding these links?
            continue
        if 'rel' in link and 'data#' not in link['rel']:
            # Exclude links which are not classified by CMR as "data" or "metadata"
            continue

        if 'title' in link and 'opendap' in link['title'].lower():
            # Exclude OPeNDAP links--they are responsible for many duplicates
            # This is a hack; when the metadata is updated to properly identify
            # non-datapool links, we should be able to do this in a non-hack way
            continue

        filename = link['href'].split('/')[-1]
        if filename in unique_filenames:
            # Exclude links with duplicate filenames (they would overwrite)
            continue
        unique_filenames.add(filename)

        urls.append(link['href'])

    return urls


def cmr_search(short_name, version, time_start, time_end,
               bounding_box='', polygon='', filename_filter=''):
    """Perform a scrolling CMR query for files matching input criteria.

        Parameters
        ---------
            short_name: string
                dataset short name
            version: string
                dataset version number
            time_start: datetime
                start datetime for data filter
            time_end: datetime
                end datetime for data filter
            bounding_box: string
                cmd_string of bounding box coordinates
                    'lon_min,lat_min,lon_max,lat_max'
            polygon: string
                string of points defining polygon
                    'lon1,lat1,lon2,lat2,...,lonN,latN'
            filename_filter: string
                filename filter

        Returns
        -------
            urls: list
                list of urls
        Notes
        -----
        Code modified from NSIDC retrieval script

        """

    CMR_PAGE_SIZE = 2000
    cmr_query_url = build_cmr_query_url(short_name=short_name, version=version,
                                        time_start=time_start, time_end=time_end,
                                        bounding_box=bounding_box,
                                        polygon=polygon, filename_filter=filename_filter)
    print('Querying for data:\n\t{0}\n'.format(cmr_query_url))

    cmr_scroll_id = None
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    try:
        urls = []
        while True:
            req = Request(cmr_query_url)
            if cmr_scroll_id:
                req.add_header('cmr-scroll-id', cmr_scroll_id)
            response = urlopen(req, context=ctx)
            if not cmr_scroll_id:
                # Python 2 and 3 have different case for the http headers
                headers = {k.lower(): v for k, v in dict(response.info()).items()}
                cmr_scroll_id = headers['cmr-scroll-id']
                hits = int(headers['cmr-hits'])
                if hits > 0:
                    print('Found {0} matches.'.format(hits))
                else:
                    print('Found no matches.')
            search_page = response.read()
            search_page = json.loads(search_page.decode('utf-8'))
            url_scroll_results = cmr_filter_urls(search_page)
            if not url_scroll_results:
                break
            if hits > CMR_PAGE_SIZE:
                print('.', end='')
                sys.stdout.flush()
            urls += url_scroll_results

        if hits > CMR_PAGE_SIZE:
            print()
        return urls
    except KeyboardInterrupt:
        quit()

def str2bool(v):
    return str(v).lower() in ("yes", "true", "t", "1")

def lint(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3

def ldif(li1, li2):
    return (list(list(set(li1)-set(li2)) + list(set(li2)-set(li1))))

def wyear_pd(row):
    if row.date.month>=10:
        return(dt.datetime(row.date.year+1,1,1).year)
    else:
        return(dt.datetime(row.date.year,1,1).year)

def wyear_dt(date):
    month = int(dt.datetime.strftime(date, '%m'))
    year = int(dt.datetime.strftime(date, '%Y'))
    if month>=10:
        return(year+1)
    else:
        return(year)

if __name__ == '__main__':
    args = parse_args()
    main(args.ini, args.start, args.end, args.time, args.prod)
