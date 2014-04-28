#!/usr/bin/env python2.7

import argparse
import commands
import datetime
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

    ap = argparse.ArgumentParser('recipe_instances')
    ap.add_argument('--userconfig',
                    default=userconfigpath,
                    help='Optional user configuration file '
                    '(default=' + userconfigpath + ')')
    
    ap.add_argument('--log',
                    default='recipe_instances.log',
                    help='(optional) name of log file')
    ap.add_argument('--logdir',
                    help='(optional) directory to hold log and xml files')
    # verbosity
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

    userconfig = config(a.userconfig)
    userconfig['server'] = 'SYBASE'
    userconfig['caom_db'] = 'jcmt'
    userconfig.read()
            
    log = logger(logpath, loglevel, True)
    log.file('jcmt2caom2version    = ' + jcmt2caom2version)
    log.file('tools4caom2version   = ' + tools4caom2version)
    for attr in dir(a):
        if attr != 'id' and attr[0] != '_':
            log.console('%-15s= %s' % (attr, getattr(a, attr)))

    retvals = None
    with connection(userconfig, log) as db:
        rcinstcmd = '\n'.join([
            'SELECT rcinst,',
            '       state,',
            '       outcount,',
            '       substring(uri, patindex("%[12]%", uri), 8) AS utdate',
            'FROM (',
            '    SELECT convert(char(20), s.identity_instance_id) AS rcinst,',
            '           s.state,',
            '           s.outcount,',
            '           min(dfi.dp_input) as uri',
            '    FROM (',
            '        SELECT dri.identity_instance_id,',
            '               dri.state, ',
            '               count(dro.dp_output) AS outcount',
            '        FROM data_proc.dbo.dp_recipe_instance dri',
            '            INNER JOIN data_proc.dbo.dp_recipe dr',
            '                ON dr.recipe_id=dri.recipe_id',
            '            LEFT JOIN data_proc.dbo.dp_recipe_output dro',
            '                ON dri.identity_instance_id=dro.identity_instance_id',
            '        WHERE  dr.script_name="jsawrapdr"',
            '        GROUP BY dri.identity_instance_id',
            '         ) s',
            '         INNER JOIN data_proc.dbo.dp_file_input dfi',
            '             ON s.identity_instance_id=dfi.identity_instance_id',
            '    GROUP BY s.identity_instance_id) t'])
        retvals = db.read(rcinstcmd)
        rcinst_dict = {}
        if retvals:
            for identity_instance_id, state, countout, utdate in retvals:
                rcinst = str(identity_instance_id)

                if state != 'Y':
                    log.console('RCINST = ' + rcinst + ' has state=' + state +
                                ' and cannot be ingested',
                                logging.WARN)
                    continue
            
                if not countout:
                    log.console('RCINST = ' + rcinst + ' produced no output and '
                                'cannot be ingested',
                                logging.WARN)
                    continue

                if utdate:
                    if not re.match(r'^\d{8}$', utdate):
                        log.console('RCINST = ' + rcinst +' has UTDATE ' + 
                                    utdate + ' which is not an 8-digit integer',
                                    logging.WARN)
                        continue
                
                    if utdate not in rcinst_dict:
                        rcinst_dict[utdate] = []
                    rcinst_dict[utdate].append(rcinst)
            
            for utdate in rcinst_dict:
                with open(utdate + '.rcinst', 'w') as RC:
                    for rcinst in sorted(rcinst_dict[utdate]):
                        print >>RC, rcinst
