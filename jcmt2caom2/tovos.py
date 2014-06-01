#!/usr/bin/env python2.7

import argparse
from ConfigParser import SafeConfigParser
from datetime import datetime
from datetime import timedelta
import logging
import os
import os.path
import re
import sys

import vos

from tools4caom2.logger import logger

from jcmt2caom2.jsa.utdate_string import utdate_string

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version

def run():
    """
    Copy files from a pickup directory into VOspace
    """
    myname = os.path.basename(sys.argv[0])
    mypath = os.path.join(sys.path[0], myname)
    
    ap = argparse.ArgumentParser('jcmt2vos')
    # directory paths
    ap.add_argument('--source',
                    default='/staging/gimli2/1/redman/daily/',
                    type=str,
                    help='file or directory containing files to be copied')
    ap.add_argument('--vos',
                    default='vos:jsaops',
                    type=str,
                    help='root of VOspace in which to store files')
    ap.add_argument('--keep',
                    action='store_true',
                    help='root of VOspace in which to store files')
    # logging
    ap.add_argument('--log',
                    default=myname + '_' + utdate_string() + '.log',
                    help='(optional) name of log file')
    ap.add_argument('--debug', '-d',
                    action='store_true',
                    help='run ingestion commands in debug mode')
    a = ap.parse_args()
    
    frompath = os.path.abspath(
                os.path.expandvars(
                    os.path.expanduser(a.source)))
    
    logpath = os.path.abspath(
                os.path.expanduser(
                    os.path.expandvars(a.log)))
    
    loglevel = logging.INFO
    if a.debug:
        loglevel = logging.DEBUG
    
    vosclient = vos.Client()
    
    rawingest_regex = re.compile(r'caom-(?P<collection>[^-]+)-'
                                  '(?P<instrument>[^_]+)_'
                                  '(?P<obsnum>\d+)_'
                                  '(?P<utdate>\d{8})[tT]'
                                  '(?P<uttime>\d{6})_'
                                  'stamp-(?P<year>\d{4})-'
                                  '(?P<month>\d{2})-'
                                  '(?P<day>\d{2})[Tt]'
                                  '(?P<tod>\d{6})')

    with logger(logpath, loglevel, True).record() as log:
        log.file(mypath)
        log.file('jcmt2caom2version    = ' + jcmt2caom2version)
        log.file('tools4caom2version   = ' + tools4caom2version)
        for attr in dir(a):
            if attr != 'id' and attr[0] != '_':
                log.file('%-15s= %s' % (attr, getattr(a, attr)))
        log.console('logfile = ' + logpath)
        
        if not vosclient.isdir(a.vos):
            log.console(a.vos + ' is not a VOspace directory',
                        logging.ERROR)
        
        filelist = []
        if os.path.isfile(frompath):
            filelist.append(frompath)
        elif os.path.isdir(frompath):
            for filename in os.listdir(frompath):
                filepath = os.path.join(frompath, filename)
                filelist.append(filepath)
        
        for filepath in filelist:
            filename = os.path.basename(filepath)
            file_id, ext = os.path.splitext(filename)
            
            if ext == '.log':
                log.console('file_id = ' + file_id,
                            logging.DEBUG)
                m = rawingest_regex.match(file_id)
                if m:
                    # This is a raw ingestion log, so store it in
                    # a.vos/raw_ingestion/year/month/day/filename
                    utdate = m.group('utdate')
                    utyear = utdate[:4]
                    utmonth = utdate[4:6]
                    utday = utdate[6:]
                    
                    # raw_ingestion should exist, so complain if it does not
                    rawpath = a.vos + '/' + 'raw_ingestion'
                    if not vosclient.isdir(rawpath):
                        log.console(rawpath + ' is not a VOspace directory',
                                    logging.ERROR)
                    
                    # As needed, create the /year/month/day/ directories
                    yearpath = rawpath + '/' + utyear
                    if not vosclient.isdir(yearpath):
                        vosclient.mkdir(yearpath)
                    
                    monthpath = yearpath + '/' + utmonth
                    if not vosclient.isdir(monthpath):
                        vosclient.mkdir(monthpath)
                    
                    daypath = monthpath + '/' + utday
                    if not vosclient.isdir(daypath):
                        vosclient.mkdir(daypath)
                    
                    # Hammer the copy a bit to ensure it happens
                    vospath = daypath + '/' + filename
                    filesize = os.stat(filepath).st_size
                    for n in range(1000):
                        if (filesize == vosclient.copy(filepath, vospath)):
                            log.file('copied ' + filepath)
                            if n:
                                log.file('retries = ' + str(n))
                            if not a.keep:
                                os.remove(filepath)
                            break
                    else:
                        if vosclient.isfile(vospath):
                            vosclient.delete(vospath)
                            log.console('failed to copy ' + filepath)
                            
                            
                    
                    
                    
                

