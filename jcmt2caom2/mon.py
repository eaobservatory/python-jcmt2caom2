#!/usr/bin/env python2.7

#################################
# Import required Python modules
#################################
import argparse
from ConfigParser import SafeConfigParser
from datetime import date
from datetime import datetime
from datetime import timedelta
import exceptions
import gzip
import logging
import os
import os.path
import re
import sys
import string
import time
import urllib
import smtplib

from tools4caom2.logger import logger
from tools4caom2.database import database
from tools4caom2.database import connection
from tools4caom2.utdate_string import utdate_string

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version
  
####################################
# Exception to skip further processing
####################################
class SkipProcessing( exceptions.Exception):
    def __init__( self, args=None):
        if args == None:
            self.args = []
        else:
            self.args = args

####################################
# utility functions
####################################
def utdate( dt):
    return 10000*dt.year + 100*dt.month + dt.day

def concatenate( listOfTuples):
    s = '('
    if (type(listOfTuples) == list and 
        len(listOfTuples)!=0 and 
        type(listOfTuples[0])==tuple):
        s += '\n,'.join([str(t[0]) for t in listOfTuples])
    s += ')'
    return s

def first( x):
    return x[0]

def pair( x):
    if len(x) > 1:
        return ( x[0], x[1])
    else:
        return (None, None)

def bigint( x):
    # Unpacks a Sybase BINARY 8 structure into a Python long long int
    # Pad string with 0's if Sybase returns fewer than 8 bytes
    # print 'len(x) = ',len(x)
    y = array.array('c',8 * '\x00')
    if len(x) < 8:
        # BEWARE: This is byte-order dependant and likely to break with new hardware
        offset = 8 - len(x)
        for i in range(min(8,len(x))):
            y[i] = x[i]
        # print y
        return struct.unpack('!Q',y)[0]
    else:
        # print array.array('c',x)
        return struct.unpack('!Q',x)[0]
    
####################################
# configuration structures
####################################
backend = {'ACSIS'  : 'ACSIS',
           'DAS' : 'DAS',
           'SCUBA2' : 'SCUBA-2'}

productList = {'ACSIS'  : ['cube', 'reduced', 'rimg', 'rsp',
                           'healpix', 'hpxrsp', 'hpxrimg'],
               'DAS'  : ['cube', 'reduced', 'rimg', 'rsp'],
               'SCUBA2' : ['reduced', 'healpix', 'hpxrimg'] }
               
associationList = ['obs', 'nit', 'pro', 'pub']

inst_abbrev = {'ACSIS' : 'h',
               'DAS' : 'a',
               'SCUBA2' : 's'}

thumbnail = ['64', '256', '1024']
thumbnail_resolutions = ','.join(thumbnail)

databaseuser = {}

####################################
# Monitor class foe JSA CAOM-2 tables
####################################
class mon(object):
    """
    Daily and weekly monitoring tools for JSA CAOM-2 tables.
    This routine is only intended to work at the CADC.
    """
    @staticmethod
    def offset_utdate(dt, offset):
        """
        Calculate a UTDATE as a string in the format YYYYMMDD
        offset into the past by an integer number of days.
        
        Arguments:
        dt: a date or datetime object from which a date can be derived
        offset: an integer number of days to offset into the past
        """
        d = dt.date() + timedelta(-offset)
        return re.sub(r'-', '', d.isoformat())
        

    def __init__(self):
        """
        Initialize the monitor, but not the database connection.
        """
        now = datetime.utcnow()
        # midnight HST, expressed in UTC
        self.midnight = now.replace(hour=10, minute=0, second=0, microsecond=0)
        if self.midnight > now:
            self.midnight += timedelta(-1)
        self.midnightiso = self.midnight.isoformat()
        self.midnightdate = re.sub(r'-', '', self.midnightiso[:10])
        self.midnightsuffix = re.sub(r':', '-', self.midnightiso)
        self.skip_processed = False
                
        self.date = None
        self.datestring = ''
        self.topclause = ''
        self.fileString = ''
        
        self.userconfigpath = '~/.tools4caom2/tools4caom2.config'
        self.userconfig = SafeConfigParser()
        # The server and cred_db are used to get database credentials at the CADC.
        # Other sites should supply cadc_id, cadc_key in the section [cadc] of
        # the userconfig file.
        if not self.userconfig.has_section('cadc'):
            self.userconfig.add_section('cadc')
        self.userconfig.set('cadc', 'server', 'SYBASE')
        self.userconfig.set('cadc', 'cred_db', 'jcmt')
        self.userconfig.set('cadc', 'read_db', 'jcmt')
        self.userconfig.set('cadc', 'write_db', 'jcmt')

        # Set the site-dependent databases containing necessary tables
        if not self.userconfig.has_section('jcmt'):
            self.userconfig.add_section('jcmt')
        self.userconfig.set('jcmt', 'caom_db', 'jcmt')
        self.userconfig.set('jcmt', 'jcmt_db', 'jcmtmd')
        self.userconfig.set('jcmt', 'omp_db', 'jcmtmd')

        self.log = None
        self.logdir = os.path.abspath('.')
        self.logfile = None
        self.loglevel = logging.INFO
        self.sender = ''
        self.to = []
        self.subject = 'JSA Monitor'
        
        self.db = None

    def parse_command_line(self):
        """
        Read command line arguments.
        """
        ap = argparse.ArgumentParser('jcmt2mon')

        ap.add_argument('--userconfig',
                        default=self.userconfigpath,
                        help='Optional user configuration file '
                        '(default=' + self.userconfigpath + ')')

        # Logging arguments
        ap.add_argument('--logdir',
                        help='(optional) name of log directory (default=".")')
        ap.add_argument('--debug',
                        action='store_true',
                        help='More verbose output')
        ap.add_argument('--log',
                        help='log file name (absolute or relative to logdir)')
        ap.add_argument('--sender',
                        help='e-mail sender for report')
        ap.add_argument('--to',
                        nargs='+',
                        help='e-mail address of recipient (may be several)')
        ap.add_argument('--subject',
                        help='subject line for report')
        
        ap.add_argument('-d', '--date',
                        type=str,
                        default=self.midnightdate,
                        help='absolute date formated as YYYYMMDD, or integer '
                             'offset in days from last midnight HST')

        ap.add_argument('--top',
                        type=int,
                        default=100,
                        help='max rows in large queries (0 = all)')
        
        args = ap.parse_args()

        if args.userconfig:
            self.userconfigpath = args.userconfig
        
        if os.path.isfile(self.userconfigpath):
            with open(self.userconfigpath) as UC:
                self.userconfig.readfp(UC)

        self.caom_db = self.userconfig.get('jcmt', 'caom_db') + '.dbo.'
        self.jcmt_db = self.userconfig.get('jcmt', 'jcmt_db') + '.dbo.'
        self.omp_db =  self.userconfig.get('jcmt', 'omp_db')  + '.dbo.'

        if args.logdir:
            self.logdir = os.path.abspath(
                            os.path.expanduser(
                                os.path.expandvars(args.logdir)))
        
        if args.top > 0:
            self.topclause = ' TOP %d' % (args.top)

        if args.date < '19800101':
            self.date = mon.offset_utdate(self.midnight, int(args.date))
        else:
            self.date = args.date
        self.datestring = 'ut-' + self.date
            
        if args.sender:
            self.sender = args.sender
        if args.to:
            self.to = args.to
        if args.subject:
            self.subject = args.subject
        
        self.logfile = '_'.join(['jcmt2mon', 
                                 'today-' + self.datestring, 
                                 utdate_string()]) + '.log'
        if args.log:
            if re.match(r'^/.*', args.log):
                self.logfile = os.path.abspath(
                                os.path.expanduser(
                                    os.path.expandvars(args.log)))
            else:
                self.logfile = os.path.abspath(
                                os.path.expanduser(
                                    os.path.expandvars(
                                        os.path.join(self.logdir, args.log))))
        else:
            self.logfile = os.path.abspath(
                            os.path.expanduser(
                                os.path.expandvars(
                                    os.path.join(self.logdir, self.logfile))))
        if args.debug:
            self.loglevel = logging.DEBUG
    
    def log_command_line_switches(self):
        """
        Logg cofiguration read from the command line switches
        """
        self.log.console('jcmt2caom2 version ' + jcmt2caom2version)
        self.log.console('tools4caom2 version ' + tools4caom2version)
        if self.sender and self.to and self.subject:
            self.log.console('%-20s = %s' % ('now', self.midnight))
            self.log.console('%-20s = %s' % ('nowday', self.midnightdate))
        self.log.console('%-20s = %s' % ('topclause', self.topclause))
        self.log.console('%-20s = %s' % ('date', self.date))
        self.log.console('%-20s = %s' % ('datestring', self.datestring))

    def print_query_table(self, label, header, format, sqlcmd):
        """
        Query the database and print the results as a table
        """
        answer = self.db.read(sqlcmd)
        self.log.console(label)
        if answer:
            self.log.console(header)
            for row in answer:
                self.log.console(format % row)
        else:
            self.log.console('none')

    def analyze_raw_data(self):
        """
        Run queries to verify the state of raw data ingestions
        """
        sqlcmd = '\n'.join([
            'SELECT',
            '    t.present,',
            '    t.qa,',
            '    count(t.obsid) as num',
            'FROM (',
            '    SELECT',
            '        s.qa,',
            '        CASE WHEN s.obsid=co.observationID THEN 1',
            '             ELSE 0',
            '        END as present,',
            '        s.obsid',
            '    FROM (',
            '        SELECT',
            '            u.obsid,',
            '            max(u.qa) as qa',
            '        FROM (',
            '            SELECT',
            '                c.obsid,',
            '                ISNULL(ool.commentstatus, 0) as qa,',
            '                ISNULL(ool.commentdate, "1980-01-01 00:00:00") as commentdate',
            '            FROM ' + self.jcmt_db + 'COMMON c',
            '                LEFT JOIN ' + self.omp_db + 'ompobslog ool',
            '                    ON c.obsid=ool.obsid',
            '                        AND ool.obsactive = 1',
            '                        AND ool.commentstatus <= 4',
            '            WHERE',
            '                c.utdate=' + self.date + ') u',
            '        GROUP BY u.obsid',
            '        HAVING u.commentdate=max(u.commentdate)) s',
            '        LEFT JOIN ' + self.caom_db + 'caom2_Observation co',
            '            ON s.obsid=co.observationID) t',
            'GROUP BY t.present, t.qa',
            'ORDER BY t.present, t.qa',
            ])

        self.print_query_table(
            'QACOUNT: Observations counted by presence, quality',
            'present qa      count',
            '%7d %2d %10d',
            sqlcmd)

        sqlcmd = '\n'.join([
            'SELECT' + self.topclause,
            '    t.present,',
            '    t.qa,',
            '    t.obsid',
            'FROM (SELECT',
            '        s.qa,',
            '        CASE WHEN s.obsid=co.observationID THEN 1',
            '             ELSE 0',
            '        END as present,',
            '        s.obsid,',
            '        s.date_obs',
            '    FROM (SELECT',
            '            u.obsid,',
            '            u.date_obs,',
            '            max(u.qa) as qa',
            '        FROM (SELECT',
            '                c.obsid,',
            '                c.date_obs,',
            '                ISNULL(ool.commentstatus, 0) as qa,',
            '                ISNULL(ool.commentdate, "1980-01-01 00:00:00") as commentdate',
            '            FROM ' + self.jcmt_db + 'COMMON c',
            '                LEFT JOIN ' + self.omp_db + 'ompobslog ool',
            '                    ON c.obsid=ool.obsid',
            '                        AND ool.obsactive = 1',
            '                        AND ool.commentstatus <= 4',
            '            WHERE',
            '                c.utdate=' + self.date + ') u',
            '        GROUP BY u.obsid, u.date_obs',
            '        HAVING u.commentdate=max(u.commentdate)',
            '        ) s',
            '        LEFT JOIN jcmt.dbo.caom2_Observation co',
            '            ON s.obsid=co.observationID',
            '    ) t',
            'HAVING t.present=0 or (t.present=1 AND t.qa=4)',
            'ORDER BY t.present, t.qa, t.date_obs',
            ])
        self.print_query_table(
            'QACOUNT: Problem observations listed by presence, quality',
            'present qa      obsid',
            '%7d %2d %40s',
            sqlcmd)

        # file queries need the file_id without the .sdf extension
        fileSelect = [
                '    (SELECT substring(f.file_id, 1, len(f.file_id)-4) as file_id',
                '     FROM ' + self.jcmt_db + 'FILES f',
                '         INNER JOIN ' + self.jcmt_db + 'COMMON c',
                '             ON f.obsid=c.obsid',
                '     WHERE c.utdate=' + self.date,
                '    ) s']
        self.fileString = '\n'.join(fileSelect)
        
        # Count raw data files
        numFiles = 0
        sqlcmd = '\n'.join([
            'SELECT COUNT(s.file_id)',
            'FROM',
            self.fileString])
        answer = self.db.read(sqlcmd)
        if answer:
            numFiles = answer[0][0]
        
        if numFiles == 0:
            self.skip_processed = True

        # Find files in FILES that are missing from AD
        sqlcmd = '\n'.join([
                    'SELECT s.file_id',
                    'FROM',
                    self.fileString,
                    '    LEFT JOIN ad.dbo.mfs_files m',
                    '        ON s.file_id = m.file_id',
                    '            AND m.status = "C"',
                    'WHERE m.file_id IS NULL',
                    'ORDER BY s.file_id'])

        answer = self.db.read(sqlcmd)
        numMissing = 0
        if answer:
            numMissing = len(answer)
            self.log.console('There are %d files in FILES that are missing from ad'
                             % (numMissing,))
            self.log.console('   - see the full list in ' + self.logfile)
            for row in answer:
                if len(row):
                    self.log.file('file_id: ' + row[0])
        else:
            self.log.console('All files in FILES are in ad')

        sqlcmd = '\n'.join([
                     'SELECT s.file_id',
                      'FROM',
                      self.fileString,
                      '    INNER JOIN ad.dbo.mfs_files m',
                      '        ON s.file_id = m.file_id',
                      '            AND m.status = "C"',
                      '    LEFT JOIN ' + self.caom_db + 'jcmt_received_new jrn',
                      '        ON s.file_id=jrn.file_id',
                      'WHERE ISNULL(jrn.received,"NULL")!="Y"',
                      'GROUP BY s.file_id',
                      'ORDER BY s.file_id'''])
        
        answer = self.db.read(sqlcmd)
        numNotReceived = 0
        if answer:
            numNotReceived = len(answer)
            self.log.console('There are %d files in FILES and ad that have not '
                             'been received' % (numNotReceived,))
            self.log.console('   - see the full list in ' + self.logfile)
            for row in answer:
                self.log.file('file_id: ' + row[0])
        else:
            self.log.console('All files in FILES and ad have been received')
        
        raw_daily = '/staging/gimli2/1/jcmtops/logs/raw_daily'
        if os.path.isdir(os.path.join(raw_daily, self.date)):
            raw_daily = os.path.join(raw_daily, self.date)
        self.log.console('Search for logs in ' + raw_daily,
                         logging.DEBUG)
        
        # Floating point time stamp for now - 30 hours
        time_boundary = time.time() - 30 * 3600.0
        if os.path.isdir(raw_daily):
            # Scrape the logs for error and warning messages
            errorwarning = []
            for filename in os.listdir(raw_daily):
                fullname = os.path.join(raw_daily, filename)
                if re.match(r'.*\.log\.gz', filename):
                    if (self.date != self.midnightdate or
                        os.stat(fullname).st_mtime > time_boundary):
                        
                        with gzip.open(fullname) as LOG:
                            for line in LOG:
                                if re.match(r'^(ERROR|WARNING).*', line):
                                    errorwarning.append(fullname + ': ' +
                                                        line.strip())
            if errorwarning:
                self.log.console('ERRORS and WARNINGS from raw ingestions')
                for line in errorwarning:
                    self.log.console(line)
            else:
                self.log.console('No errors or warnings from raw ingestions')
    
    def analyze_proc_data(self):
        """
        Run queries to verify the state of raw data ingestions
        """
        if self.skip_processed:
            self.log.console('No raw data, so no recipe instances to check')
        else:
            # examine list of data reduction recipe instances
            self.log.console( '---- STATE OF DATA REDUCTION ----')

            numScienceRecipeInstances = 0
            sqlcmd = '\n'.join([
                            'SELECT DISTINCT',
                            '   substring(dri.parameters, 8, ',
                            '       charindex("\'",',
                            '                 substring(dri.parameters,',
                            '                 8, len(dri.parameters))) - 1),',
                            '   dri.state,',
                            '   dri.identity_instance_id',
                            'FROM',
                            self.fileString,
                            '    INNER JOIN data_proc.dbo.dp_file_input dfi',
                            '        ON dfi.dp_input = "ad:JCMT/" + s.file_id',
                            '    INNER JOIN data_proc.dbo.dp_recipe_instance dri',
                            '        ON dfi.identity_instance_id=dri.identity_instance_id'])
            
            recipeInstances = self.db.read(sqlcmd)
            recipeDict = {}
            for mode, state, ii_id in recipeInstances:
                idinst = str(ii_id)
                if mode not in recipeDict:
                    recipeDict[mode] = {}
                if state not in recipeDict[mode]:
                    recipeDict[mode][state] = set()
                recipeDict[mode][state].add(idinst)
            
            self.log.console('Count of all recipe instances = %d' % 
                             (len(recipeInstances),))
            
            if recipeDict:
                self.log.console('Count of recipe instances by mode and state:')
                self.log.console('%-10s%-6s%-6s' % ('Mode', 'State', 'Count'))
                for mode in sorted(recipeDict.keys()):
                    for state in sorted(recipeDict[mode]):
                        self.log.console('%-10s%-6s%6d' % 
                                         (mode, 
                                          state,
                                          len(recipeDict[mode][state])))
                        if state != 'Y':
                            for idinst in sorted(list(recipeDict[mode][state])):
                                self.log.console(
                                    '         identity_instance_id = ' +
                                    idinst)


    def run(self):
        """
        Connect to the database and run queries
        """
        self.parse_command_line()
        with logger(self.logfile,
                    loglevel = self.loglevel,
                    sender=self.sender,
                    to=self.to,
                    subject=self.subject).record() as self.log:
            self.log_command_line_switches()
            with connection(self.userconfig, self.log) as self.db:
            
                self.log.console('---- RAW DATA ----')
                self.analyze_raw_data()
                
                self.log.console('---- PROCESSED DATA ----')
                self.analyze_proc_data()
            
        

