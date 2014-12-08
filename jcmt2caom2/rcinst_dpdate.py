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

from tools4caom2.logger import logger
from tools4caom2.database import database
from tools4caom2.database import connection
from tools4caom2.gridengine import gridengine
from tools4caom2.utdate_string import utdate_string

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version

def run():
    """
    Construct a set of files containing all valid recipe instances stored in
    files by the utdate of the processing (not the raw data).
    """
    userconfig = SafeConfigParser()
    userconfigpath = '~/.tools4caom2/tools4caom2.config'
    
    wrapper = textwrap.TextWrapper(initial_indent='',
                                   subsequent_indent='')
    description = '\n\n'.join([
            wrapper.fill(textwrap.dedent(
            """
            Generate rcinst files for input to jcmtprocwrap.
            Output goes to stdout by default, so jcmt_rcinst_dpdate can be
            piped to jcmtprocwrap:
            """)),
            'EXAMPLE: jcmt_rcinst_dpdate --new | jcmtprocwrap --keeeplog --qsub',
            wrapper.fill(textwrap.dedent(
            """
            The options --daily and --monthly generate rcinst files in the 
            current directory that can also be picked up by jcmtprocwrap,
            which can be useful if there are a large number of recipe instances.
            Recipe instances will be grouped by days or months respectively.
            """)),
            'EXAMPLE: jcmt_rcinst_dpdate --new --daily\n'
            '         jcmtprocwrap --qsub --keeplog *.rcinst',
            wrapper.fill(textwrap.dedent(
            """
            The list of recipe instances to be considered can be restricted 
            with --fromset=<filepath>
            where the file contains a list of identity_instance_id values as 
            decimal integers, one per line as the first token on each line.
            Lines that do not match this pattern (blank, or a token that is not
            an integer at the start of the line) will be ignored.
            """)),
            wrapper.fill(textwrap.dedent(
            """
            The --utdate, --begin and --end arguments can be used to restrict the 
            range of UT dates to be considered, where recipe instances are dated by 
            the utdate for the earliest observation in their inputs.  For obs and
            night recipe instances, this is always the utdate on which all the 
            data was taken.
            """)),
            wrapper.fill(textwrap.dedent(
            """
            UT dates can be entered as integers in the format YYYYMMDD, or as small
            integer offsets from the coming night, i.e. the UT date at 
            midnight HST tonight.  Thus --utdate=0 is tonight, --utdate=1 is
            last night, and so forth.  If none of --utdate --begin, --end, or 
            --fromset is specified, the default is equivalent to --begin=1 --end=0.
            """)),
            wrapper.fill(textwrap.dedent(
            """
            The --new switch includes only recipe instances that are not already 
            present in CAOM-2, or have bee reprocessed since their last ingestion.
            """)),
            wrapper.fill(textwrap.dedent(
            """
            It is possible to combine --fromset with --utdate, --begin, --end, and 
            --new. 
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
                    default='jcmt_rcinst_dpdate_' + utdate_string() + '.log',
                    help='(optional) name of log file')
    ap.add_argument('--logdir',
                    help='(optional) directory to hold log file')
    ap.add_argument('--debug', '-d',
                    action='store_true',
                    help='run ingestion commands in debug mode')
    a = ap.parse_args()
    
    if a.userconfig:
        userconfigpath = a.userconfig
    
    if os.path.isfile(userconfigpath):
        with open(userconfigpath) as UC:
            userconfig.readfp(UC)

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

    log = logger(logpath, loglevel, True)
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
    
    # if end is present, but bot begin, set begin before start of observatory
    if (a.end is not None and a.begin is None):
        a.begin = '19880101'
            
    # utdate and begin/end can be absolute or relative to now
    # Beware of comaprisons between ASCII-coded integers, 
    # because '2' > '19991231'
    now = datetime.utcnow()
    zerotime = datetime(now.year, now.month, now.day, 10, 0, 0)
    if now.hour >= 10:
        zerotime += timedelta(1)
    
    this_utdate = None
    if a.utdate is not None:
        if int(a.utdate) > 19800101:
            this_begin = datetime(int(a.utdate[0:4]), 
                                  int(a.utdate[4:6]),
                                  int(a.utdate[6:8]),
                                  10,
                                  0,
                                  0)
        else:
            this_begin = zerotime - timedelta(int(a.utdate))
        this_end = this_begin + timedelta(1) 

        log.file('this_begin -> ' + this_begin.isoformat())
        log.file('this_end   -> ' + this_end.isoformat())
    
    this_begin = None
    if a.begin is not None:
        if int(a.begin) > 19800101:
            this_begin = datetime(int(a.begin[0:4]), 
                                  int(a.begin[4:6]),
                                  int(a.begin[6:8]),
                                  10,
                                  0,
                                  0)
        else:
            this_begin = zerotime - timedelta(int(a.begin))
        log.file('this_begin -> ' + this_begin.isoformat())

    this_end = None
    if a.end is not None:
        if int(a.end) > 19800101:
            this_end = datetime(int(a.end[0:4]), 
                                int(a.end[4:6]),
                                int(a.end[6:8]),
                                10,
                                0,
                                0)
        else:
            this_end = zerotime - timedelta(int(a.end))
        this_end = this_end + timedelta(1) 
        log.file('this_end   -> ' + this_end.isoformat())
    
    if this_begin and this_end:
        if this_begin > this_end:
            store = this_begin
            this_begin = this_end
            this_end = store
    
    fromset = set()
    if a.fromset:
        with open(a.fromset) as RCF:
            for line in RCF:
                m = re.match(r'^\s*((0[xX])?[0-9a-f]+)([^\d].*)?$', line)
                if m:
                    thisid = str(eval(m.group(1)))
                    log.file('from includes: "' + thisid + '"',
                                logging.DEBUG)
                    fromset.add(thisid)
    
    retvals = None
    with connection(userconfig, log) as db:
        rcinstlist = [
            'SELECT rcinst,',
            '       state,',
            '       outcount,',
            '       date_processed,',
            '       lastModified,',
            '       substring(uri, patindex("%[12]%", uri), 8) AS utdate',
            'FROM (',
            '    SELECT convert(char(20), s.identity_instance_id) AS rcinst,',
            '           s.state,',
            '           s.outcount,',
            '           s.date_processed,',
            '           MIN(dfi.dp_input) as uri,',
            '           MAX(ISNULL(u.lastModified, "1980-01-01")) as lastModified',
            '    FROM (',
            '        SELECT dri.identity_instance_id,',
            '               dri.state, ',
            '               MAX(ISNULL(dri.date_processed,',
            '                          "1979-12-31 00:00:00")) as date_processed,',
            '               COUNT(dro.dp_output) AS outcount',
            '        FROM data_proc.dbo.dp_recipe_instance dri',
            '            INNER JOIN data_proc.dbo.dp_recipe dr',
            '                ON dr.recipe_id=dri.recipe_id',
            '            LEFT JOIN data_proc.dbo.dp_recipe_output dro',
            '                ON dri.identity_instance_id=dro.identity_instance_id',
            '        WHERE  dr.script_name="jsawrapdr"',
            '        GROUP BY dri.identity_instance_id, dri.state',
            '         ) s',
            '        INNER JOIN data_proc.dbo.dp_file_input dfi',
            '            ON s.identity_instance_id=dfi.identity_instance_id',
            '        LEFT JOIN (',
            '            SELECT CASE WHEN charindex("x", provenance_runID) = 2 ',
            '                        THEN hextobigint(provenance_runID) ',
            '                        ELSE convert(bigint, provenance_runID) ',
            '                   END as identity_instance_id,',
            '                   lastModified',
            '            FROM ' + caom_db + 'caom2_Plane',
            '            WHERE productID not like "raw%") u',
            '                ON s.identity_instance_id=u.identity_instance_id',            
            '    GROUP BY s.identity_instance_id,',
            '             s.state,',
            '             s.outcount,',
            '             s.date_processed) t']
        # Insert the date_processed constraints, if any
        if this_end:
            endconstraint = ('            AND dri.date_processed < "' + 
                             re.sub(r'[tT]', ' ', this_end.isoformat()) + '"')
            rcinstlist.insert(25, endconstraint)
        if this_begin:
            beginconstraint = ('            AND dri.date_processed >= "' + 
                               re.sub(r'[tT]', ' ', this_begin.isoformat()) + '"')
            rcinstlist.insert(25, beginconstraint)
        rcinstcmd = '\n'.join(rcinstlist)
        retvals = db.read(rcinstcmd)
        
        rcinst_dict = {}
        if retvals:
            for (identity_instance_id, 
                 state, 
                 countout, 
                 date_processed, 
                 lastModified,
                 utdate) in retvals:
                 
                rcinst = str(identity_instance_id).strip()

                if a.fromset and rcinst not in fromset:
                    continue
                
                if (not a.new or (a.new and lastModified < date_processed)):

                    if state != 'Y':
                        log.console('RCINST = ' + rcinst + ' has state=' + 
                                    state + ' and cannot be ingested',
                                    logging.WARN)
                        continue
                
                    if not countout:
                        log.console('RCINST = ' + rcinst + ' produced no '
                                    'output and cannot be ingested',
                                    logging.WARN)
                        continue
                    
                    if utdate:
                        if not re.match(r'^\d{8}$', utdate):
                            log.console('RCINST = ' + rcinst +' has UTDATE ' + 
                                        utdate + ' which is not an 8-digit '
                                        'integer',
                                        logging.WARN)
                            continue
                        
                        utdatestr = str(utdate)
                        if a.monthly:
                            utdatestr = utdatestr[0:6]
                        elif not a.daily:
                            utdatestr = 'all'
                        
                        if utdatestr not in rcinst_dict:
                            rcinst_dict[utdatestr] = []
                        rcinst_dict[utdatestr].append(rcinst)
                        log.file('PROGRESS: add ' + rcinst + ' on ' + 
                                    utdatestr)
            
            for utdate in rcinst_dict:
                if utdate == 'all':
                    for rcinst in sorted(rcinst_dict[utdate]):
                        print rcinst
                else:
                    with open('ut' + utdate + '.rcinst', 'w') as RC:
                        for rcinst in sorted(rcinst_dict[utdate]):
                            print >>RC, rcinst
