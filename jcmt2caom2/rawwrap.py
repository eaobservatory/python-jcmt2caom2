#!/usr/bin/env python2.7

import argparse
import commands
import datetime
import logging
import os.path
import re
import stat
import sys

from tools4caom2.logger import logger
from tools4caom2.database import database
from tools4caom2.database import connection
from tools4caom2.gridengine import gridengine

def run():
    """
    Ingest raw JCMT observation from a range of UTDATE's.
    This is just a high-level script to run jcmt2caom2raw many times.  
    
    Examples:
    rawutdate --debug --start=20100123 --end=20100131
    """
    ap = argparse.ArgumentParser('rawutdate')
    ap.add_argument('--log',
                    default='rawutdate.log',
                    help='(optional) name of log file')
    ap.add_argument('--sharelog',
                    action='store_true',
                    help='share the same log file for all ingestions')
    ap.add_argument('--debug', '-d',
                    action='store_true',
                    help='run ingestion commands in debug mode')
    ap.add_argument('--stop_on_error', '-s',
                    action='store_const',
                    dest='stop',
                    default=False,
                    const=True,
                    help='stop processing on the first error reported by '
                         'jcmt2caom2raw; if not set, report the error and '
                         'retain the xml file but continue '
                         'processing with the next observation')
    ap.add_argument('--script',
                    help='store commands to a script instead of running')
    ap.add_argument('--begin',
                    required=True,
                    help='ingest raw data with utdate >= this date')
    ap.add_argument('--end',
                    help='(optional) ingest raw data with utdate <= this date')
    ap.add_argument('--qsub',
                    action='store_const',
                    default=False,
                    const=True,
                    help='submit jobs to gridengine, one utdate for each job')
    a = ap.parse_args()

    loglevel = logging.INFO        
    if a.debug:
        loglevel = logging.DEBUG
    log = logger(a.log, loglevel)
    
    if not a.end:
        a.end = a.begin
    
    mygridengine = gridengine(log)
    
    # Find all the observations on the requested UTDATE and their quality
    log.console('UTDATE in [' + a.begin + ', ' + a.end + ']')
    
    retvals = None
    if a.qsub:
        with connection('SYBASE', 'jcmtmd', log) as db:
            sqlcmd = '\n'.join([
                'SELECT c.utdate',
                'FROM jcmtmd.dbo.COMMON c',
                '    LEFT JOIN jcmt2.dbo.caom2_Observation co',
                '        ON c.obsid=co.observationID',
                '    LEFT JOIN jcmt2.dbo.caom2_Plane cp',
                '        ON co.obsID=cp.obsID',
                '            AND substring(cp.productID,1,3) like "raw%"',
                'WHERE c.utdate >= %s AND c.utdate <= %s' % (a.begin, a.end),
                'GROUP BY c.utdate',
                'HAVING count(cp.planeID) = 0',
                'ORDER BY c.utdate'])
            retvals = db.read(sqlcmd)
            
        if retvals:
            for utd, in retvals:
                utdate = str(utd)
                dirpath = os.path.dirname(
                            os.path.abspath(
                                os.path.expanduser(
                                    os.path.expandvars(a.log))))
                logpath = os.path.join(dirpath, 
                                       'raw_' + utdate + '.log')
                cshpath = os.path.join(dirpath, 
                                       'raw_' + utdate + '.csh')

                cmd = 'jcmtrawwrap'
                if a.debug:
                    cmd += ' --debug' 
                if a.sharelog:
                    cmd += ' --sharelog'
                cmd += ' --begin=' + utdate
                cmd += ' --log=' + logpath
                mygridengine.submit(cmd, cshpath, logpath)

    else:
        with connection('SYBASE', 'jcmtmd', log) as db:
            sqlcmd = '\n'.join([
                'SELECT c.utdate,',
                '       c.obsnum,',
                '       c.obsid',
                'FROM jcmtmd.dbo.COMMON c',
                'WHERE c.utdate>=' + a.begin,
                '      AND c.utdate <= ' + a.end])
                        
            retvals = db.read(sqlcmd)

        rawdict = {}
        if retvals:
            for utd, obsnum, obsid in retvals:
                key = '%d%-05d' % (utd, obsnum)
                if key not in rawdict:
                    rawdict[key] = obsid
        else:
            print 'no observations found for utdate=' + a.utdate
        
        SCRIPT = None
        if rawdict:
            if a.debug:
                debugflag = ' --debug'
            else:
                debugflag = ''
            
            if a.sharelog:
                logflag = ' --log=' + a.log
            else:
                logflag = ''

            if a.script:
                scriptpath = os.path.abspath(a.script)
                SCRIPT = open(scriptpath, 'w')
                print >>SCRIPT, 'date'
            
            for key in sorted(rawdict.keys()):
                obsid = rawdict[key]
                cmd = 'jcmt2caom2raw%s --key=%s%s' % \
                    (debugflag, obsid, logflag)
                log.console('PROGRESS: ' + cmd)
                if SCRIPT:
                    print >>SCRIPT, cmd
                else:
                    status, output = commands.getstatusoutput(cmd)
                    if status:
                        if a.stop:
                            self.log.console(output, logging.ERROR)
                        else:
                            log.console('REPORT ERROR BUT CONTINUE: ' +
                                        'status=' + str(status) + ' :' +
                                        output, logging.WARN)
        else:
            log.console('WARNING: no raw data found for '
                        'utdate in [%s, %s]' % (a.begin, a.end))
        if SCRIPT:
            print >>SCRIPT, 'date'
