#!/usr/bin/env python2.7

import argparse
from ConfigParser import SafeConfigParser
from datetime import datetime
from datetime import timedelta
import logging
import os.path
import re
import stat
import string
import subprocess
import textwrap

from tools4caom2.database import database
from tools4caom2.database import connection
from tools4caom2.gridengine import gridengine
from tools4caom2.logger import logger
from tools4caom2.utdate_string import utdate_string

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version

def run():
    """
    Construct a set of files containing all valid recipe instances stored in
    files by the utdate of the earliest (alphabetiaclly smallest) input file
    in dp_file_input.
    """

    userconfigpath = '~/.tools4caom2/tools4caom2.config'
    userconfig = SafeConfigParser()
        
    wrapper = textwrap.TextWrapper(initial_indent='',
                                   subsequent_indent='')
    description = '\n\n'.join([
            wrapper.fill(textwrap.dedent(
            """
            Generate obsid files for input to jcmtrawwrap.
            Output goes to stdout by default, so jcmt_obsid_files can be
            piped to jcmtrawwrap:
            """)),
            'EXAMPLE: jcmt_obsid_files --new | jcmtrawwrap --qsub',
            wrapper.fill(textwrap.dedent(
            """
            The options --daily and --monthly generate obsid files in the 
            current directory that can also be picked up by jcmtrawwrap,
            which can be useful if there are a large number of observations.
            Observations will be grouped by days or months respectively.
            """)),
            'EXAMPLE: jcmt_obsid_files --new --daily\n'
            '         jcmtrawwrap --qsub *.obsid',
            wrapper.fill(textwrap.dedent(
            """
            The list of observations to be considered can be restricted 
            with --fromset=<filepath> where the file contains a list of 
            obsid values, one per line as the first token on each line.
            Lines that do not match this pattern (blank, or starting with a 
            token that is not an obsid) will be ignored.
            """)),
            wrapper.fill(textwrap.dedent(
            """
            The --utdate, --begin and --end arguments can be used to restrict 
            the range of UT dates to be considered.  UT dates can be entered 
            as integers in the format YYYYMMDD, or as small integer offsets 
            from the coming night, i.e. the UT date at midnight HST tonight.  
            Thus --utdate=0 is tonight, --utdate=1 is last night, and so 
            forth.  If none of --utdate --begin, --end, or --fromset is 
            specified, the default is equivalent to --begin=1 --end=0.
            """)),
            wrapper.fill(textwrap.dedent(
            """
            The --new switch includes only observations that are not present 
            in CAOM-2, or have been updated since their last ingestion.
            """)),
            wrapper.fill(textwrap.dedent(
            """
            It is possible to combine --fromset with --utdate, --begin, 
            --end, and --new. 
            """))])

    ap = argparse.ArgumentParser(
                description=description,
                formatter_class=argparse.RawDescriptionHelpFormatter)
    
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

    # subsets
    ap.add_argument('--fromset',
                    type=str,
                    help='file containing a list of recipe instances')
    ap.add_argument('--new',
                    action='store_true',
                    help='include new or reprocessed recipe instances')
    
    # Output options, default = stdout
    ap.add_argument('--daily',
                    action='store_true',
                    help='group by day and output to daily files')
    ap.add_argument('--monthly',
                    action='store_true',
                    help='group by month and output to monthly files')

    # logging
    ap.add_argument('--log',
                    default='jcmt_obsid_files_' + utdate_string() + '.log',
                    help='(optional) name of log file')
    ap.add_argument('--logdir',
                    help='(optional) directory to hold log file')
    ap.add_argument('--debug', '-d',
                    action='store_true',
                    help='run in debug mode')
    a = ap.parse_args()
    
    if a.userconfig:
        userconfigpath = a.userconfig
    
    if os.path.isfile(userconfigpath):
        with open(userconfigpath) as UC:
            userconfig.readfp(UC)

    jcmt_db = userconfig.get('jcmt', 'jcmt_db') + '.dbo.'
    omp_db =  userconfig.get('jcmt', 'omp_db')  + '.dbo.'

    cwd = os.path.abspath(
                os.path.expanduser(
                    os.path.expandvars('.')))
    
    if a.logdir:
        logdir = os.path.abspath(
                os.path.expanduser(
                    os.path.expandvars(a.logdir)))
    else:
        logdir = cwd
    
    loglevel = logging.INFO
    if a.debug:
        loglevel = logging.DEBUG
    
    if os.path.dirname(a.log):
        logpath = os.path.abspath(
                    os.path.expanduser(
                        os.path.expandvars(a.log)))
    else:
        logpath = os.path.join(logdir, a.log)
    
    with logger(logpath, loglevel, True).record() as log:
        
        log.file('jcmt2caom2version    = ' + jcmt2caom2version)
        log.file('tools4caom2version   = ' + tools4caom2version)
        for attr in dir(a):
            if attr != 'id' and attr[0] != '_':
                log.file('%-15s= %s' % (attr, getattr(a, attr)))
        log.console('logfile = ' + logpath)

        # specifying utdate precludes begin and end
        if a.utdate and (a.begin or a.end):
            log.console('specify either utdate or begin/end, not both',
                        logging.ERROR)
        
        if (a.utdate is None and 
            a.begin is None and
            a.end is None and
            a.fromset is None):
            
            a.begin = '1'
            a.end = '0'
        
        # if begin is present, but end is not, set end=now
        if (a.begin is not None and a.end is None):
            a.end = '0'
        
        # if end is present, but bot begin, set begin well before start of 
        # observatory operations
        if (a.end is not None and a.begin is None):
            a.begin = '19880101'
                
        # utdate and begin/end can be absolute or relative to now
        now = datetime.utcnow()
        zerotime = datetime(now.year, now.month, now.day, 10, 0, 0)
        if now.hour >= 10:
            zerotime += timedelta(1)
        
        this_utdate = None
        if a.utdate is not None:
            if int(a.utdate) > 19800101:
                this_utdate = a.utdate
            else:
                thisutc = zerotime - timedelta(int(a.utdate))
                this_utdate = '%04d%02d%02d' % (thisutc.year, 
                                                thisutc.month, 
                                                thisutc.day)
                log.file('%-15s= %s' % ('utdate -> ', this_utdate))
        
        this_begin = None
        if a.begin is not None:
            if int(a.begin) > 19800101:
                this_begin = a.begin
            else:
                thisutc = zerotime - timedelta(int(a.begin))
                this_begin = '%04d%02d%02d' % (thisutc.year, 
                                               thisutc.month, 
                                               thisutc.day)
                log.file('%-15s= %s' % ('begin -> ', this_begin))

        this_end = None
        if a.end is not None:
            if int(a.end) > 19800101:
                this_end = a.end
            else:
                thisutc = zerotime - timedelta(int(a.end))
                this_end = '%04d%02d%02d' % (thisutc.year, 
                                               thisutc.month, 
                                               thisutc.day)
                log.file('%-15s= %s' % ('end -> ', this_end))
        
        if this_begin and this_end:
            if this_begin > this_end:
                store = this_begin
                this_begin = this_end
                this_end = store
        
        fromset = {}
        utdate_regex = None
        if a.fromset:
            fromsetfile = os.path.abspath(
                            os.path.expandvars(
                                os.path.expanduser(a.fromset)))
            log.console('PROGRESS: Reading fromset = ' + fromsetfile)
            obsid_regex = re.compile(r'^\s*(\w[^_]*_\d+_(\d{8})T\d{6})(\s.*)?$')
            with open(fromsetfile) as FS:
                for line in FS:
                    m = obsid_regex.match(line)
                    if m:
                        thisid, thisut, rest = m.groups()
                        if thisid not in fromset:
                            log.file('from includes: "' + thisid + '"',
                                        logging.DEBUG)
                            fromset[thisid] = thisut
            
        obsid_dict = {}
        retvals = None
        if a.new or this_utdate or this_begin:
            # Query the database only if necessary
            log.console('PROGRESS: Query database')
            with connection(userconfig, log) as db:
                obsidlist = [
                    'SELECT s.obsid,',
                    '       s.utdate,',
                    '       s.quality',
                    'FROM (',
                    '    SELECT',
                    '            u.obsid,',
                    '            u.utdate,',
                    '            u.date_obs,',
                    '            u.last_modified,',
                    '            MAX(u.qa) as quality',
                    '    FROM (',
                    '        SELECT',
                    '               c.obsid,',
                    '               c.utdate,',
                    '               c.date_obs,',
                    '               c.last_modified,',
                    '               ISNULL(ool.commentstatus, 0) as qa,',
                    '               ISNULL(ool.commentdate,',
                    '                      "1980-01-01 00:00:00") as cdate',
                    '        FROM ' + jcmt_db + 'COMMON c',
                    '            LEFT JOIN ' + omp_db + 'ompobslog ool',
                    '                ON c.obsid=ool.obsid',
                    '                    AND ool.obsactive = 1',
                    '                    AND ool.commentstatus <= 4',
                    '        ) u',
                    '    GROUP BY u.obsid',
                    '    HAVING u.cdate=max(u.cdate)',
                    '    ) s',
                    'ORDER BY s.date_obs']
            
                if this_utdate or this_begin:
                    i = obsidlist.index('        ) u')
                    obsidlist[i:i] = [
                    '        WHERE',
                    ]

                if this_utdate:
                    i = obsidlist.index('        ) u')
                    obsidlist[i:i] = [
                    '            c.utdate = ' + this_utdate,
                    ]
                
                if this_begin:
                    i = obsidlist.index('        ) u')
                    if this_end:
                        obsidlist[i:i] = [
                        '            c.utdate >= ' + this_begin + ' AND',
                        ]
                    else:
                        obsidlist[i:i] = [
                        '            c.utdate >= ' + this_begin,
                        ]
                
                if this_end:
                    i = obsidlist.index('        ) u')
                    obsidlist[i:i] = [
                    '            c.utdate <= ' + this_end,
                    ]
                
                if a.new:
                    i = obsidlist.index('       s.utdate,')
                    obsidlist[i:i] = [
                    '       s.last_modified,',
                    '       MIN(ISNULL(cp.maxLastModified,',
                    '           "1980-01-01 00:00:00")) AS caom_modified,']
                    
                    i = obsidlist.index('    ) s') + 1
                    obsidlist[i:i] = [
                    '    LEFT JOIN ' + caom_db + 'caom2_Observation co',
                    '        ON s.obsid=co.observationID',
                    '    LEFT JOIN ' + caom_db + 'caom2_Plane cp',
                    '        ON co.obsID=cp.obsID AND cp.productID like '
                    '"raw%"']
                    
                    
                    i = obsidlist.index('ORDER BY s.date_obs')
                    obsidlist[i:i] = [
                    'GROUP BY s.obsid, s.utdate',
                    ]
                
                obsidcmd = '\n'.join(obsidlist)
                retvals = db.read(obsidcmd)
                log.console('PROGRESS: len(results) = ' + str(len(retvals)))
                
                jcmt_start = datetime(1980, 01, 01, 0, 0, 1)
                if retvals:
                    log.file(repr(retvals))
                    lastmod = 0
                    caommod = datetime(1979, 12, 31, 23, 59, 59)
                    quality = 0
                    update = True
                    for row in retvals:
                        if a.new:
                            obsid, jcmtmod, caommod, utdate, quality = row
                            update = (jcmtmod > caommod)
                        else:
                            obsid, utdate, quality = row
                         
                        if a.fromset and obsid not in fromset:
                            continue
                        
                        # ignore junk observations that are NOT in CAOM
                        if quality == 4 and caommod < jcmt_start:
                            continue
                        
                        if update:
                            utdatestr = str(utdate)
                            if a.monthly:
                                utdatestr = utdatestr[0:6]
                            elif not a.daily:
                                utdatestr = 'all'
                            
                            if utdatestr not in obsid_dict:
                                obsid_dict[utdatestr] = []
                            obsid_dict[utdatestr].append(obsid)
                            log.file('PROGRESS: add ' + obsid + ' on ' + 
                                        utdatestr)

        else:
            # only fromset to process
            log.console('PROGRESS: Sort fromset = ' + fromsetfile)
            utdatestr = 'all'
            for obsid in fromset:
                if a.daily:
                    utdatestr = fromset[obsid]
                elif a.monthly:
                    utdatestr = fromset[obsid][:6]

                if utdatestr not in obsid_dict:
                    obsid_dict[utdatestr] = []
                obsid_dict[utdatestr].append(obsid)
                log.file('PROGRESS: add ' + obsid + ' on ' + 
                            utdatestr)
                
        # Print out the obsid file(s)
        for utdate in sorted(obsid_dict.keys()):
            if utdate == 'all':
                for obsid in obsid_dict[utdate]:
                    # Print to stdout to pipe into jcmtrawwrap
                    print obsid
            else:
                with open('ut' + utdate + '.obsid', 'w') as OID:
                    for obsid in obsid_dict[utdate]:
                        print >>OID, obsid
        log.console('DONE')
