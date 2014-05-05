#!/usr/bin/env python2.7

import argparse
import commands
from datetime import datetime
from datetime import timedelta
import logging
import os.path
import re
import stat
import string
import subprocess

from tools4caom2.config import config
from tools4caom2.logger import logger
from tools4caom2.database import database
from tools4caom2.database import connection
from tools4caom2.gridengine import gridengine

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version

def run():
    """
    Construct a set of files containing all valid recipe instances stored in
    files by the utdate of the earliest (alphabetiaclly smallest) input file
    in dp_file_input.
    """
    userconfig = None
    userconfigpath = '~/.tools4caom2/jcmt2caom2.config'

    ap = argparse.ArgumentParser('jcmt_rcinst_files')
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
    ap.add_argument('--month',
                    action='store_true',
                    help='group by month instead of day')

    # subsets
    ap.add_argument('--fromset',
                    type=str,
                    help='file containing a list of recipe instances')
    ap.add_argument('--new',
                    action='store_true',
                    help='include recipe instance not already in CAOM-2')
    
    # logging
    ap.add_argument('--log',
                    default='jcmt_recipe_instances.log',
                    help='(optional) name of log file')
    ap.add_argument('--logdir',
                    help='(optional) directory to hold log file')
    ap.add_argument('--debug', '-d',
                    action='store_true',
                    help='run ingestion commands in debug mode')
    a = ap.parse_args()
    
    userconfig = config(a.userconfig)
    userconfig['server'] = 'SYBASE'
    userconfig['caom_db'] = 'jcmt'
    userconfig.read()
            
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
            log.console('%-15s= %s' % (attr, getattr(a, attr)))

    # specifying utdate precludes begin and end
    if a.utdate and (a.begin or a.end):
        log.console('specify either utdate or begin/end, not both',
                    logging.ERROR)
    
    # if begin or end is present, both must be
    if (a.begin and not a.end) or (not a.begin and a.end):
        log.console('specify both of begin and end or neither',
                    logging.ERROR)
    
    # utdate and begin/end can be absolute or relative to now
    now = datetime.utcnow()
    this_utdate = None
    if a.utdate:
        if a.utdate > '19800101':
            this_utdate = a.utdate
        else:
            thisutc = now - timedelta(int(a.utdate))
            this_utdate = '%04d%02d%02d' % (thisutc.year, 
                                            thisutc.month, 
                                            thisutc.day)
    
    this_begin = None
    if a.begin:
        if a.begin > '19800101':
            this_begin = a.begin
        else:
            thisutc = now - timedelta(int(a.begin))
            this_begin = '%04d%02d%02d' % (thisutc.year, 
                                           thisutc.month, 
                                           thisutc.day)

    this_end = None
    if a.end:
        if a.end > '19800101':
            this_end = a.end
        else:
            thisutc = now - timedelta(int(a.end))
            this_end = '%04d%02d%02d' % (thisutc.year, 
                                           thisutc.month, 
                                           thisutc.day)
    
    if this_begin and this_end:
        if this_begin > this_end:
            store = this_begin
            this_begin = this_end
            this_end = store
    
    fromset = set()
    if a.fromset:
        with open(a.fromset) as RCF:
            for line in RCF:
                m = re.match(r'^\s*(\d+)([^\d].*)?$', line)
                if m:
                    thisid = m.group(1)
                    log.console('from includes: ' + thisid,
                                logging.DEBUG)
                    fromset.add(thisid)
    
    retvals = None
    with connection(userconfig, log) as db:
        rcinstcmd = '\n'.join([
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
            '               dri.date_processed,',
            '               count(dro.dp_output) AS outcount',
            '        FROM data_proc.dbo.dp_recipe_instance dri',
            '            INNER JOIN data_proc.dbo.dp_recipe dr',
            '                ON dr.recipe_id=dri.recipe_id',
            '            LEFT JOIN data_proc.dbo.dp_recipe_output dro',
            '                ON dri.identity_instance_id=dro.identity_instance_id',
            '        WHERE  dr.script_name="jsawrapdr"',
            '        GROUP BY dri.identity_instance_id',
            '         ) s',
            '        INNER JOIN data_proc.dbo.dp_file_input dfi',
            '            ON s.identity_instance_id=dfi.identity_instance_id',
            '        LEFT JOIN (',
            '            SELECT CASE WHEN charindex("x", provenance_runID) = 2 ',
            '                        THEN hextobigint(provenance_runID) ',
            '                        ELSE convert(bigint, provenance_runID) ',
            '                   END as identity_instance_id,',
            '                   lastModified',
            '            FROM jcmt.dbo.caom2_Plane',
            '            WHERE productID like "reduced%" ',
            '                  OR productID like "cube%") u',
            '                ON s.identity_instance_id=u.identity_instance_id',            
            '    GROUP BY s.identity_instance_id,',
            '             s.state,',
            '             s.outcount,',
            '             s.date_processed) t'])
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
                
                if ((not a.new or (a.new and lastModified < date_processed)) and
                    utdate and 
                    (not this_utdate or (this_utdate == utdate)) and
                    (not this_begin or ((this_begin <= utdate) and
                                        (this_end >= utdate)))):

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
                        if a.month:
                            utdatestr = utdatestr[0:6]
                        if utdatestr not in rcinst_dict:
                            rcinst_dict[utdatestr] = []
                        rcinst_dict[utdatestr].append(rcinst)
                        log.console('PROGRESS: add ' + rcinst + ' on ' + 
                                    utdatestr)
            
            for utdate in rcinst_dict:
                with open('ut' + utdate + '.rcinst', 'w') as RC:
                    for rcinst in sorted(rcinst_dict[utdate]):
                        print >>RC, rcinst
