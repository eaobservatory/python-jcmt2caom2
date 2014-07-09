#!/usr/bin/env python2.7

import argparse
from ConfigParser import SafeConfigParser
from datetime import datetime
from datetime import timedelta
import os
import os.path
import re
import sys
import vos

from tools4caom2.utdate_string import utdate_string

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version

def run():
    """
    Read a set of log files from vos:jsaops and report any error, warning and 
    junk messages.  Optionally list all successful ingestions, and report
    missing observations.
    """

    userconfigpath = '~/.tools4caom2/jcmt2caom2.config'
    userconfig = SafeConfigParser()
    # The server and cred_db are used to get database credentials at the CADC.
    # Other sites should supply cadc_id, cadc_key in the section [cadc] of
    # the userconfig file.
    if not userconfig.has_section('cadc'):
        userconfig.add_section('cadc')
    userconfig.set('cadc', 'server', 'SYBASE')
    userconfig.set('cadc', 'cred_db', 'jcmt')
    userconfig.set('cadc', 'read_db', 'jcmt')
    userconfig.set('cadc', 'write_db', 'jcmt')

    # Set the site-dependent databases containing necessary tables
    if not userconfig.has_section('jcmt'):
        userconfig.add_section('jcmt')
    userconfig.set('jcmt', 'caom_db', 'jcmt')
    userconfig.set('jcmt', 'jcmt_db', 'jcmtmd')
    userconfig.set('jcmt', 'omp_db', 'jcmtmd')
    
    ap = argparse.ArgumentParser('jcmt2report')
    
    ap.add_argument('--userconfig',
                    default=userconfigpath,
                    help='Optional user configuration file '
                    '(default=' + userconfigpath + ')')
    
    # UTDATE constraints
    ap.add_argument('--utdate',
                    type=str,
                    help='include only a specific utdate')
    ap.add_argument('--begin',
                    type=str,
                    help='include only utdays on or after begin')
    ap.add_argument('--end',
                    type=str,
                    help='include only utdays on or before end')
    
    # Search criteria
    ap.add_argument('--success',
                    action='store_true',
                    help='list successful ingestions')
    
    # ap.add_argument('--missing',
    #                 action='store_true',
    #                 help='report missing raw data')

    # E-mail config for report
    ap.add_argument('--sender',
                    help='e-mail address of sender',
                    default='jsa@jach.hawaii.edu')
    ap.add_argument('--smtphost',
                    help='smtp relay host',
                    default='mailhost.jach.hawaii.edu')
    ap.add_argument('--subject',
                    help='Subject for e-mailed report:')
    ap.add_argument('--to',
                    nargs='*',
                    help='e-mail recipient')
    a = ap.parse_args()
    
    if a.userconfig:
        userconfigpath = a.userconfig
    
    if os.path.isfile(userconfigpath):
        with open(userconfigpath) as UC:
            userconfig.readfp(UC)

    caom_db = userconfig.get('jcmt', 'caom_db') + '.dbo.'
    jcmt_db = userconfig.get('jcmt', 'jcmt_db') + '.dbo.'
    omp_db =  userconfig.get('jcmt', 'omp_db')  + '.dbo.'

    cwd = os.path.abspath(
                os.path.expanduser(
                    os.path.expandvars('.')))
    
    # specifying utdate precludes begin and end
    if a.utdate and (a.begin or a.end):
        raise RuntimeError('specify either utdate or begin/end, not both')
    
    if (a.utdate is None and 
        a.begin is None and
        a.end is None and
        a.fromset is None):
        
        a.begin = '1'
        a.end = '1'
    
    if a.utdate is not None:
        a.begin = a.utdate
        a.end = a.utdate
    
    # if begin is present, but end is not, set end=now
    if (a.begin is not None and a.end is None):
        a.end = '1'
    
    # if end is present, but bot begin, set begin before start of observatory
    # Not a very useful option
    if (a.end is not None and a.begin is None):
        a.begin = '19880101'
            
    # utdate and begin/end can be absolute or relative to today (UTC),
    # with 0 = the UTC date at midnight tonight HST = 10:00:00 UTC. 
    # Note that this date changes at 0:00:00 HST, not UTC, a useful property 
    # for most people in Hawaii, North America and east Asia. 
    now = datetime.utcnow()
    zerotime = datetime(now.year, now.month, now.day, 10, 0, 0)
    if now.hour >= 10:
        zerotime += timedelta(1)
            
    # Beware of doing alphabetic comparisons of ASCII-coded integers.
    # since '2' > '19991231'
    this_begin = None
    if a.begin is not None:
        if int(a.begin) > 19800101:
            this_begin = a.begin
        else:
            thisutc = zerotime - timedelta(int(a.begin))
            this_begin = '%04d/%02d/%02d' % (thisutc.year, 
                                           thisutc.month, 
                                           thisutc.day)

    this_end = None
    if a.end is not None:
        if int(a.end) > 19800101:
            this_end = a.end
        else:
            thisutc = zerotime - timedelta(int(a.end))
            this_end = '%04d/%02d/%02d' % (thisutc.year, 
                                           thisutc.month, 
                                           thisutc.day)
    
    # Since this_begin and this_end are both YYYYMMDD strings
    # alphabetic comparisons are legitimate.
    if this_begin > this_end:
        store = this_begin
        this_begin = this_end
        this_end = store
    
    cwd = os.path.abspath('.')
    
    if a.success:
        ERRORWARNING_REGEX = \
            re.compile(r'^ERROR|^WARNING|java.lang|^INFO.*SUCCESS observationID')
    else:
        ERRORWARNING_REGEX = re.compile(r'^ERROR|^WARNING')
    EXTRA_REGEX = re.compile(r'.+(ERROR|java.lang)')
    END_REGEX = re.compile(r'^ERROR|^WARNING|^INFO|^DEBUG')
        
    # Read the list of directories from vos:jsaops/raw_ingestion
    vosclient = vos.Client()
    vosroot = 'vos:jsaops/raw_ingestion'
    vosdaylist = []
    firstday = vosroot + '/' + this_begin
    lastday = vosroot + '/' + this_end
    
    for year in vosclient.listdir(vosroot, force=True):
        if year >= this_begin[0:4] and year <= this_end[0:4]:
            vosyear = vosroot + '/' + year
            for month in vosclient.listdir(vosyear, force=True):
                vosmonth = vosyear + '/' + month
                for day in vosclient.listdir(vosmonth, force=True):
                    vosday = vosmonth + '/' + day
                    if vosday >= firstday and vosday <= lastday:
                        vosdaylist.append(vosday)
                               
    # Read the files for each day
    needspace = False
    somethingtoreport = False
    print 'REPORT OF INGESTION LOGS FOR ' + this_begin + ' TO ' + this_end
    for vosday in sorted(vosdaylist, reverse=True):
        if needspace:
            print
        needspace = True
        print 'SUMMARY OF LOGS FOR: ' + vosday
        for logfile in vosclient.listdir(vosday, force=True):
            logpath = vosday + '/' + logfile
            if a.success or re.search(r'ERRORS|WARNINGS|JUNK', logfile):
                localpath = os.path.join(cwd, logfile)
                vosclient.copy(logpath, localpath)
                with open(localpath) as LF:
                    text = LF.readlines()
                os.remove(localpath)
                reportfile = True
                everyline = False
                for line in text:
                    #if EXTRA_REGEX.search(line):
                    #    everyline = True
                    #    pass
                    #elif END_REGEX.search(line):
                    #    everyline = False
                        
                    if (everyline or
                        ERRORWARNING_REGEX.search(line)):
                        
                        line = re.sub(r'^(ERROR|WARNING) '
                               r'[\d]{4}-[\d]{2}-[\d]{2}T[\d]{2}:[\d]{2}:[\d]{2}',
                               r'\1 ',
                               line)
                        if reportfile:
                            print logpath
                            somethingtoreport = True
                            reportfile = False
                        print '   ' + line.rstrip()
                print
    
    if somethingtoreport:
        sys.exit(1)
