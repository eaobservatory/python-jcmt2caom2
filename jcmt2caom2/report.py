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

    userconfigpath = '~/.tools4caom2/tools4caom2.config'
    userconfig = SafeConfigParser()
    # The server and cred_db are used to get database credentials at the CADC.
    # Other sites should supply cadc_id, cadc_key in the section [cadc] of
    # the userconfig file.
    if not userconfig.has_section('database'):
        userconfig.add_section('database')
    userconfig.set('database', 'server', 'SYBASE')
    userconfig.set('database', 'cred_db', 'jcmt')
    userconfig.set('database', 'read_db', 'jcmt')
    userconfig.set('database', 'write_db', 'jcmt')

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
    ap.add_argument('--procdate',
                    action='store_true',
                    help='report on processed ingestions for the dates')
    
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
    
    ap.add_argument('fromset',
                    nargs='*',
                    help='a list of obsid files, rcinst files or wrap logs'
                         ' from which obsid or rcinst values can be parsed')
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
            this_begin = a.begin[0:4] + '/' + a.begin[4:6] + '/' + a.begin[6:8]
        else:
            thisutc = zerotime - timedelta(int(a.begin))
            this_begin = '%04d/%02d/%02d' % (thisutc.year, 
                                           thisutc.month, 
                                           thisutc.day)

    this_end = None
    if a.end is not None:
        if int(a.end) > 19800101:
            this_end = a.end[0:4] + '/' + a.end[4:6] + '/' + a.end[6:8]
        else:
            thisutc = zerotime - timedelta(int(a.end))
            this_end = '%04d/%02d/%02d' % (thisutc.year, 
                                           thisutc.month, 
                                           thisutc.day)
    
    # Since this_begin and this_end are both YYYY/MM/DD strings
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
    
    OBSID_REGEX = re.compile(r'^\s*(\w+_\d{5}_\d{8}[Tt]\d{6})(\s.*)?$')
    RCINST_REGEX = re.compile(r'^\s*(\d+)(\s.*)$')
    LOG_OBSID_REGEX = re.compile(r'--key=(\w+_\d{5}_\d{8}[Tt]\d{6})\b')
    LOG_RCINST_REGEX = re.compile(r'dp:(\d+)\b')
    
    # Parse fromset to get lists of obsid and rcinst sources
    obsid_dict = {}
    obsid_set = set()
    rcinst_dict = {}
    rcinst_set = set()
    
    for source in a.fromset:
        # if source == '-', read from stdin
        # if source is an obsid or rcinst, the "sourcefile" is cmdline.
        if OBSID_REGEX.match(source):
            obsid_set.add(source)
            if 'obsid_cmd' in obsid_dict:
                obsid_dict['obsid_cmd'].append(source)
            else:
                obsid_dict['obsid_cmd'] = [source]
        elif RCINST_REGEX.match(source):
            rcinst_set.add(source)
            if 'rcinst_cmd' in rcinst_dict:
                rcinst_dict['rcinst_cmd'].append(source)
            else:
                rcinst_dict['rcinst_cmd'] = [source]
        elif source == '-':
            for line in sys.stdin:
                m = OBSID_REGEX.match(line)
                if m:
                    obsid_set.add(m.group(1))
                    if 'obsid_stdin' in obsid_dict:
                        obsid_dict['obsid_stdin'].append(m.group(1))
                    else:
                        obsid_dict['obsid_stdin'] = [m.group(1)]
                else:
                    m = RCINST_REGEX.match(line)
                    if m:
                        rcinst_set.add(m.group(1))
                        if 'rcinst_stdin' in rcinst_dict:
                            rcinst_dict['rcinst_stdin'].append(m.group(1))
                        else:
                            rcinst_dict['rcinst_stdin'] = [m.group(1)]
        elif os.path.isfile(source):
            basename, ext = os.path.splitext(source)
            if ext == '.obsid':
                obsid_dict[source] = []
                with open(source, 'r') as OBSID:
                    for line in OBSID:
                        m = OBSID_REGEX.match(line)
                        if m:
                            obsid_set.add(m.group(1))
                            obsid_dict[source].append(m.group(1))
            elif ext == '.rcinst':
                rcinst_dict[source] = []
                with open(source, 'r') as RCINST:
                    for line in RCINST:
                        m = RCINST_REGEX.match(line)
                        if m:
                            rcinst_set.add(m.group(1))
                            rcinst_dict[source].append(m.group(1))
            elif ext == '.log':
                if re.search(r'jcmtrawwrap', basename):
                    obsid_dict[source] = []
                    with open(source, 'r') as OBSID:
                        for line in OBSID:
                            m = LOG_OBSID_REGEX.match(line)
                            if m:
                                obsid_set.add(m.group(1))
                                obsid_dict[source].append(m.group(1))
                elif re.search(r'jcmtprocwrap', basename):
                    rcinst_dict[source] = []
                    with open(source, 'r') as RCINST:
                        for line in RCINST:
                            m = LOG_RCINST_REGEX.match(line)
                            if m:
                                rcinst_set.add(m.group(1))
                                rcinst_dict[source].append(m.group(1))
    
    # Read the list of directories from vos:jsaops/raw_ingestion
    if a.procdate:
        vosroot = 'vos:jsaops/proc_ingestion_date'
    else:
        vosroot = 'vos:jsaops/raw_ingestion'
    
    vosclient = vos.Client()
    vosdaylist = []
    if this_begin and this_end:
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
    if vosdaylist:
        if a.procdate:
            print ('PROCESSED DATA INGESTION LOGS BY DATE FOR ' + 
                   this_begin + ' TO ' + this_end)
        else:
            print ('REPORT OF INGESTION LOGS FOR ' + 
                   this_begin + ' TO ' + this_end)
        for vosday in sorted(vosdaylist, reverse=True):
            if needspace:
                print
            needspace = True
            print 'SUMMARY OF LOGS FOR: ' + vosday
            for logfile in vosclient.listdir(vosday, force=True):
                logpath = vosday + '/' + logfile
                if a.success or re.search(r'ERRORS|WARNINGS|JUNK', logfile):
                    localpath = os.path.join(cwd, logfile)
                    summary(vosclient, logpath, localpath, ERRORWARNING_REGEX)
    
    if obsid_dict:
        pass
    
    if rcinst_dict and not a.procdate:
        print 'REPORT OF INGESTION LOGS FOR RCINST INPUTS'
        for source in sorted(rcinst_dict.keys()):
            print 'SUMMARY OF LOGS IN ' + source
            thousands = {}
            for rcinst in sorted(rcinst_dict[source]):
                thousand = rcinst[:-3]
                thoudir = thousand + '000-' + thousand + '999'
                if thoudir not in thousands:
                    thousands[thoudir] = []
                thousands[thoudir].append(rcinst)
            for thousand in sorted(thousands.keys()):
                logdir = 'vos:jsaops/proc_ingestion/' + thousand
                for filename in vosclient.listdir(logdir):
                    m = re.search(r'dp_(\d+)_', filename)
                    if m:
                        logpath = logdir + '/' + filename
                        localpath = os.path.join(cwd, filename)
                        summary(vosclient, 
                                logpath, 
                                localpath, 
                                ERRORWARNING_REGEX)
                         

def summary(vosclient, logpath, localpath, ERRORWARNING_REGEX):
    """
    Copy a file from VOspace and scan it for errors and warnings
    
    Arguments:
    logpath: the name of the log file in VOspace
    localpath: the name of the file on the local disk
    ERRORWARNINFG_REGEX: the regex to use to search for errors and warnings
    """
    fregex = re.compile(r'^INFO [\d]{4}-[\d]{2}-'
                        r'[\d]{2}T[\d]{2}:[\d]{2}:[\d]{2}\s+(vos:|/)')
    try:
        vosclient.copy(logpath, localpath)
    except KeyboardInterrupt:
        sys.exit(0)
    except:
        print 'Could not access ' + logpath + '\n'
        return
    
    with open(localpath) as LF:
        text = LF.readlines()
    os.remove(localpath)
    reportfile = True
    everyline = False
    prevline = ''
    fileline = ''
    for line in text:
        #if EXTRA_REGEX.search(line):
        #    everyline = True
        #    pass
        #elif END_REGEX.search(line):
        #    everyline = False
        
        if (everyline or
            ERRORWARNING_REGEX.search(line)):
            if reportfile:
                print logpath
                reportfile = False
            
            # Remove logging time stamp for clarity
            if not fileline and fregex.match(prevline):
                fileline = fregex.sub(r'\1', prevline)
                print '   ' + fileline.rstrip()
            
            repline = re.sub(r'^(ERROR|WARNING) '
                   r'[\d]{4}-[\d]{2}-[\d]{2}T[\d]{2}:[\d]{2}:[\d]{2}',
                   r'\1 ',
                   line)
            
            # JUNK observations are not real warnings
            if re.search(r'JUNK', repline):
                repline = re.sub(r'WARNING', r'INFO', line)
            
            print '   ' + repline.rstrip()
        else:
            fileline = ''
        prevline = line
    print
