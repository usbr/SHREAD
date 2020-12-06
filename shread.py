"""
Name: shread.py
Author: Dan Broman, Reclamation Technical Service Center
Description: SHREAD main script
ADD CLASSES / FUNCTIONS DEFINED
ADD WHA
"""
import argparse
import logging
import os
import subprocess
import sys

def main(config_path, start_date, end_date, time_int, prod_list):
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
        - day
        -

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
    print(config_path)
    print(start_date)
    print(end_date)
    print(time_int)
    print(prod_list)

def parse_args():
    parser = argparse.ArgumentParser(
        description= ' SHREAD',
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-i', '--ini', metavar='PATH',
        type = lambda x: is_valid_file(parser, x), help='Input file')
    parser.add_argument(
        '-s', '--start', metavar='start_date', help = 'start date')
    parser.add_argument(
        '-e', '--end', metavar='end_date', help = 'end date')
    parser.add_argument(
        '-t', '--time', metavar='time_interval', help = 'time interval')
    parser.add_argument(
        '-p', '--prod', metavar='product_list', help = 'product list')
    args = parser.parse_args()
    return args

def is_valid_file(parser, arg):
    if not os.path.isfile(arg):
        parser.error('The file {} does not exist!'.format(arg))
    else:
        return arg

if __name__ == '__main__':
    args = parse_args()
    main(args.ini, args.start, args.end, args.time, args.prod)
