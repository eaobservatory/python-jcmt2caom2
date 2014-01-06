#!/usr/bin/env python2.7

#################################
# Import required Python modules
#################################
import argparse
from datetime import date
from datetime import datetime
from datetime import timedelta
import exceptions
import os
import os.path
import sys
import string
import re
import commands 
import urllib
import smtplib

from tools4caom2.logger import logger
from tools4caom2.database import database

from jcmt2caom2.__version__ import version
  
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
    if type(listOfTuples) == list and len(listOfTuples)!=0 and type(listOfTuples[0])==tuple:
        for tup in listOfTuples:
            if s!='(':
                s += ',\n'
            s += '"'+str(tup[0])+'"'
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

productList = {'ACSIS'  : ['cube', 'reduced', 'rimg', 'rsp'],
               'DAS'  : ['cube', 'reduced', 'rimg', 'rsp'],
               'SCUBA2' : ['reduced'] }
               
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
    def __init__(self):
        """
        Initialize the monitor, but not the database connection.
        """
        self.now = datetime.utcnow()
        self.nowiso = self.now.isoformat()
        self.nowday = re.sub(r'-', '', self.nowiso[:10])
        self.nowsuffix = re.sub(r':', '-', self.nowiso)
                
        self.begin = None
        self.end = None
        self.date = None
        self.datestring = ''
        self.topclause = ''
        
        self.server = 'SYBASE'
        
        self.log = None
        self.logdir = os.path.abspath('.')
        self.logfile = None
        self.sender = ''
        self.to = []
        self.subject = 'JSA Monitor'
        
        self.db = None

    def offset_utdate(dt, offset):
        """
        Calculate a UTDATE as a string in the format YYYYMMDD
        offset into the past by an integer number of days.
        
        Arguments:
        dt: a date or datetime object from which a date can be derived
        offset: an integer number of days to offset into the past
        """
        d = dt.date() - timedelta(offset)
        return re.sub(r'-', '', d.isoformat())
        
    def parse_command_line(self):
        """
        Read command line arguments.
        """
        ap = argparse.ArgumentParser(
                description='Values for --begin, --end, and '
                            '--date can be given as absolute dates in the '
                            'format YYYYMMDD as an integer giving the offset '
                            'in days from today.  Giving --date overrides '
                            'the --begin and --end switches')
        # Logging arguments
        ap.add_argument('--logdir',
                        help='(optional) name of log directory (default=".")')
        ap.add_argument('--log',
                        help='log file name (absolute or relative to logdir)')
        ap.add_argument('--sender',
                        help='e-mail sender for report')
        ap.add_argument('--to',
                        nargs='+',
                        help='e-mail address of recipient (may be several)')
        ap.add_argument('--subject',
                        help='subject line for report')
        
        # database server
        ap.add_argument('--dev',
                        action='store_true',
                        help='Use DEVSYBASE instead of SYBASE')
        
        # Date ranges in COMMON
        ap.add_argument('-b', '--begin',
                        type=str,
                        help='utdate >= begin in the format YYYYMMDD')
        ap.add_argument('-e', '--end',
                        type=str,
                        help='utdate <= end in the format YYYYMMDD')
        ap.add_argument('-d', '--date',
                        type=str,
                        help='utdate = date in the format YYYYMMDD')
        ap.add_argument('--top',
                        type=int,
                        default=100,
                        help='max rows in large queries (0 = all)')
        
        args = ap.parse_args()
        if args.logdir:
            self.logdir = os.path.abspath(
                            os.path.expanduser(
                                os.path.expandvars(args.logdir)))
        
        if args.dev:
            self.server = 'DEVSYBASE'
        
        if args.top > 0:
            self.topclause = ' TOP %d' % (args.top)

        if args.date:
            if args.date < '19800101':
                self.date = self.utdate_offset(self.now, int(args.date))
            else:
                self.date = args.date
            self.datestring = 'utdate_eq_' + self.date
        else:
            if args.begin or args.end:
                self.datestring = 'utdate'
            if args.begin:
                if args.begin < '19800101':
                    self.begin = self.utdate_offset(self.now, int(args.begin))
                else:
                    self.begin = args.begin
                self.datestring = (args.begin + '_ge_' + self.datestring)
            if args.end:
                if args.end < '19800101':
                    self.end = self.utdate_offset(self.now, int(args.end))
                else:
                    self.end = args.end
                self.datestring = (self.datestring + '_le_' + args.end)
        
        if args.sender:
            self.sender = args.sender
        if args.to:
            self.to = args.to
        if args.subject:
            self.subject = args.subject
        
        self.logfile = '_'.join(['jcmt2mon', self.datestring, self.nowsuffix]) \
                       + '.log'
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
    
    def log_command_line_switches(self):
        """
        Logg cofiguration read from the command line switches
        """
        self.log.console('jcmt2mon version ' + version)
        if self.sender and self.to and self.subject:
            self.log.console('%-20s = %s' % ('now', self.now))
            self.log.console('%-20s = %s' % ('nowday', self.nowday))
            self.log.console('%-20s = %s' % ('nowsuffix', self.nowsuffix))
        self.log.console('%-20s = %s' % ('topclause', self.topclause))
        if self.date:
            self.log.console('%-20s = %s' % ('date', self.date))
        else:
            if self.begin:
                self.log.console('%-20s = %s' % ('begin', self.begin))
            if self.end:
                self.log.console('%-20s = %s' % ('end', self.end))
        self.log.console('%-20s = %s' % ('datestring', self.datestring))
        self.log.console('%-20s = %s' % ('server', self.server))

    def print_query_table(self, label, header, format, sqlcmd):
        """
        Query the database and print the results as a table
        """
        answer = self.db.read(sqlcmd)
        self.log.console(label)
        self.log.console(header)
        for row in answer:
            self.log.console(format % row)

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
            '            c.obsid,',
            '            ISNULL(ool.commentstatus, 0) as qa',
            '        FROM jcmtmd.dbo.COMMON c',
            '            LEFT JOIN jcmtmd.dbo.ompobslog ool',
            '                ON c.obsid=ool.obsid',
            '        WHERE',
            '            ool.obsactive = 1',
            '            AND ool.commentstatus <= 4',])
        
        if self.date:
            sqlcmd += '\n            AND c.utdate=' + self.date
        else:
            if self.begin:
                sqlcmd += '\n            AND c.utdate >= ' + self.begin
            if self.end:
                sqlcmd += '\n            AND c.utdate <= ' + self.end
                
        sqlcmd = '\n'.join([sqlcmd,
            '        GROUP BY c.obsid',
            '        HAVING ool.commentdate=max(ool.commentdate)',
            '        ) s',
            '        LEFT JOIN jcmt.dbo.caom2_Observation co',
            '            ON s.obsid=co.observationID',
            '    ) t',
            'GROUP BY t.present, t.qa',
            'ORDER BY t.present, t.qa',
            ])
        self.print_query_table(
            'QACOUNT: Observations counted by quality',
            'present qa      count',
            '%7d %2d %10d',
            sqlcmd)

        sqlcmd = '\n'.join([
            'SELECT' + self.topclause,
            '    t.present,',
            '    t.qa,',
            '    t.obsid',
            'FROM (',
            '    SELECT',
            '        s.qa,',
            '        CASE WHEN s.obsid=co.observationID THEN 1',
            '             ELSE 0',
            '        END as present,',
            '        s.obsid',
            '    FROM (',
            '        SELECT',
            '            c.obsid,',
            '            ISNULL(ool.commentstatus, 0) as qa',
            '        FROM jcmtmd.dbo.COMMON c',
            '            LEFT JOIN jcmtmd.dbo.ompobslog ool',
            '                ON c.obsid=ool.obsid',
            '        WHERE',
            '            ool.obsactive = 1',
            '            AND ool.commentstatus <= 4'])
        
        if self.date:
            sqlcmd += '\n            AND c.utdate=' + self.date
        else:
            if self.begin:
                sqlcmd += '\n            AND c.utdate >= ' + self.begin
            if self.end:
                sqlcmd += '\n            AND c.utdate <= ' + self.end
                
        sqlcmd = '\n'.join([sqlcmd,
            '        GROUP BY c.obsid',
            '        HAVING ool.commentdate=max(ool.commentdate)',
            '        ) s',
            '        LEFT JOIN jcmt.dbo.caom2_Observation co',
            '            ON s.obsid=co.observationID',
            '    ) t',
            'GROUP BY t.present, t.qa',
            'ORDER BY t.present, t.qa',
            ])
        self.print_query_table(
            'QACOUNT: Observations listed by presence, quality',
            'present qa      obsid',
            '%7d %2d %40s',
            sqlcmd)

    def analyze_proc_data(self):
        """
        Run queries to verify the state of raw data ingestions
        """
        pass

    def run(self):
        """
        Connect to the database and run queries
        """
        self.parse_command_line()
        with logger(self.logfile,
                    sender=self.sender,
                    to=self.to,
                    subject=self.subject).record() as self.log:
            self.log_command_line_switches()
            self.db = database(self.server, 'jcmt', self.log)
            
            self.log.console('\nAnalyzing raw data ingestions')
            self.analyze_raw_data()
            
            self.log.console('\nAnalyzing processed data ingestions')
            self.analyze_proc_data()
            
        

