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
from tools4caom2.utdate_string import utdate_string
from tools4caom2.utdate_string import UTDATE_REGEX

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version

class tovos(object):
    """
    Abstract base class for copying files into VOspace.  Override
    the regex, and the match and push methods for each class of files 
    to be copied.
    """    
    def __init__(self, vosclient, vosroot, log=None):
        """
        Set up the log and vos client
        
        Arguments:
        vosclient: a vos.Client() object
        vosroot: base vos directory for these files, which must exist
        log: (optional) a tools4caom2.looger object
        """
        self.vosclient = vosclient
        self.vosroot = vosroot
        self.log = log
        self.regex = None
        
        self.copylist = {}

        if not vosclient.isdir(vosroot):
            if self.log:
                self.log.console(vosroot + ' is not a VOspace directory',
                            logging.ERROR)
            else:
                raise RuntimeError(vosroot + ' is not a VOspace directory')

    def make_subdir(self, dir, subdir):
        """
        If a requested subdirectory does not already exist, create it
        and return the expanded directory path.
        
        Arguments:
        dir: an existing vos directory (not checked)
        subdir: a subdirectory that will be created if it does not exist
        
        Returns:
        directory path to the subdirectory 
        """
        dirpath = dir + '/' + subdir
        if not self.vosclient.isdir(dirpath):
            self.vosclient.mkdir(dirpath)
        return dirpath
    
    def push_file(self, path, vospath):
        """
        Copy a file from disk to VOspace, repeating the operation until the
        file size matches in both places.
        
        Arguments:
        path: path to file on disk to be copied to VOspace
        vospath: path to file or directory in VOspace
        """
        success = False
        filesize = os.stat(path).st_size
        for n in range(1000):
            if (filesize == self.vosclient.copy(path, vospath)):
                success = True
                if self.log:
                    self.log.file('copied ' + path)
                    if n:
                        self.log.file('retries = ' + str(n))
                break
        else:
            if self.log:
                self.log.console('failed to copy ' + path)

        return success

    def match(self, filepath):
        """
        Match and record this file for copying to VOspace and/or
        deletion from disk
        
        Arguments:
        path: absolute path to the file to be matched
        """
        pass
    
    def push(self):
        """
        Copy all files to VOspace and delete any marked for removal.
        
        Arguments:
        <none>
        """
        pass
    
class raw_ingestion(tovos):
    """
    Identify raw ingestion logs and copy them to VOspace 
    """
    def __init__(self, vosclient, vosroot, log=None):
        tovos.__init__(self, 
                       vosclient, 
                       vosroot + '/raw_ingestion', 
                       log)
        self.regex = re.compile(r'(?P<root>'
                                r'caom-(?P<collection>[^-]+)-'
                                r'(?P<instrument>[^_]+)_'
                                r'(?P<obsnum>\d+)_'
                                r'(?P<utdate>\d{8})[tT]'
                                r'(?P<uttime>\d{6})'
                                r')_' + 
                                UTDATE_REGEX +
                                r'(?P<errors>(_ERRORS)?)'
                                r'(?P<warnings>(_JUNK|_WARNINGS)?)')
        self.copy = {}

    def match(self, path):
        """
        Identify raw ingestion logs, record them for deletion, and 
        select the most recent to copy to VOspace.
        
        Arguments:
        path: absolute path to the file to be matched
        """
        filename = os.path.basename(path)
        file_id, ext = os.path.splitext(filename)
        if ext == '.log':
            m = self.regex.match(file_id)
            if m:
                root = m.group('root')
                stamp = m.group('stamp').lower()
                utdate = m.group('utdate')
                
                if root not in self.copy:
                    self.copy[root] = {}
                    self.copy[root]['path'] = path
                    self.copy[root]['stamp'] = stamp
                    self.copy[root]['utdate'] = utdate
                    if self.log:
                        self.log.file('record for copy' + path,
                                      logging.DEBUG)
                elif self.copy[root]['stamp'] < stamp:
                    self.copy[root]['path'] = path
                    self.copy[root]['stamp'] = stamp
                    if self.log:
                        self.log.file('replace for copy' + path,
                                      logging.DEBUG)

    def push(self):
        """
        Copy all files to VOspace and delete any marked for removal.
        
        Arguments:
        <none>
        """
        for root in self.copy:
            path = self.copy[root]['path']
            if self.log:
                self.log.console('pushing ' + path,
                                 logging.DEBUG)
            utdate = self.copy[root]['utdate']
            utyear = utdate[:4]
            utmonth = utdate[4:6]
            utday = utdate[6:]
            
            # As needed, create the /year/month/day/ directories
            vosdir = self.make_subdir(self.vosroot, utyear)
            vosdir = self.make_subdir(vosdir, utmonth)
            vosdir = self.make_subdir(vosdir, utday)
            
            filename = os.path.basename(path)
            vospath = vosdir + '/' + filename
            
            success = self.push_file(path, vospath)
            
            # Try to clean up the directory by deleting old log files
            m = self.regex.search(filename)
            (root, errors, warnings, stamp) = m.group('root', 
                                                      'errors', 
                                                      'warnings', 
                                                      'stamp')
            for vfile in self.vosclient.listdir(vosdir, force=True):
                m = self.regex.search(vfile)
                if m:
                    (vroot, verrors, vwarnings, vstamp) = m.group('root', 
                                                                  'errors',
                                                                  'warnings',
                                                                  'stamp')
                    if vroot == root and vstamp < stamp:
                        # If no errors, delete all earlier logs
                        # if errors, delete earlier logs with errors
                        if not errors or (errors and verrors):
                            vpath = vosdir + '/' +  vfile
                            self.vosclient.delete(vpath)
        
class stdpipe_ingestion(tovos):
    """
    Identify stdpipe ingestion logs and copy them to VOspace 
    """
    def __init__(self, vosclient, vosroot, log=None):
        tovos.__init__(self, 
                       vosclient, 
                       vosroot + '/proc_ingestion', 
                       log)
        self.regex = re.compile(r'(?P<trunk>(?P<root>'
                                r'dp_'
                                r'(?P<rcinst>[^_]+)'
                                r')_)' + 
                                UTDATE_REGEX +
                                r'(?P<errors>(_ERRORS)?)'
                                r'(?P<warnings>(_WARNINGS)?)')
        self.copy = {}

    def match(self, path):
        """
        Identify raw ingestion logs, record them for deletion, and 
        select the most recent to copy to VOspace.
        
        Arguments:
        path: absolute path to the file to be matched
        """
        filename = os.path.basename(path)
        file_id, ext = os.path.splitext(filename)
        if ext == '.log':
            m = self.regex.match(file_id)
            if m:
                root = m.group('root')
                stamp = m.group('stamp').lower()
                rcinst = m.group('rcinst')
                
                if root not in self.copy:
                    self.copy[root] = {}
                    self.copy[root]['path'] = path
                    self.copy[root]['stamp'] = stamp
                    self.copy[root]['rcinst'] = rcinst
                    if self.log:
                        self.log.file('record for copy' + path,
                                      logging.DEBUG)
                elif self.copy[root]['stamp'] < stamp:
                    self.copy[root]['path'] = path
                    self.copy[root]['stamp'] = stamp
                    if self.log:
                        self.log.file('replace for copy' + path,
                                      logging.DEBUG)

    def push(self):
        """
        Copy all files to VOspace and delete any marked for removal.
        
        Arguments:
        <none>
        """
        for root in self.copy:
            path = self.copy[root]['path']
            rcinst = self.copy[root]['rcinst']
            thousands = rcinst[:-3] + '000-' + rcinst[:-3] + '999'
            
            # As needed, create the /thousands/ directory
            vosdir = self.make_subdir(self.vosroot, thousands)
            
            filename = os.path.basename(path)
            fileid = os.path.splitext(filename)[0]
            
            vospath = vosdir + '/' + filename
            success = self.push_file(path, vospath)
            
            # Try to clean up the directory by deleting old log files
            m = self.regex.search(fileid)
            (trunk, errors, warnings, stamp) = m.group('trunk', 
                                                       'errors', 
                                                       'warnings', 
                                                       'stamp')
            for vfile in self.vosclient.listdir(vosdir, force=True):
                m = self.regex.search(vfile)
                if m:
                    (vtrunk, verrors, vwarnings, vstamp) = m.group('trunk', 
                                                                   'errors',
                                                                   'warnings',
                                                                   'stamp')
                    if vtrunk == trunk and vstamp < stamp:
                        # If no errors, delete all earlier logs
                        # if errors, delete earlier logs with errors
                        if not errors or (errors and verrors):
                            vpath = vosdir + '/' +  vfile
                            self.vosclient.delete(vpath)
            
            # Try to create a link in the raw_ingestion directories
            # Read the necessary metadata from the log file
            utdate = None
            with open(filename, 'r') as L:
                for line in L:
                    m = re.search(r'Earliest utdate: '
                                  r'(?P<utdate>[-0-9]+)'
                                  r' for (?P<prefix>\S+)',
                                  line)
                    if m:
                        (utdate, prefix) = m.group('utdate', 'prefix')
                        break
            if utdate:
                (ryear, rmonth, rday) = re.split(r'-', utdate)
                # As needed, create the /year/month/day/ directories
                # Beware that these are URI's, not directory paths in the OS
                rvosdir = re.sub(r'proc', 'raw', self.vosroot)
                rvosdir = self.make_subdir(rvosdir, ryear)
                rvosdir = self.make_subdir(rvosdir, rmonth)
                rvosdir = self.make_subdir(rvosdir, rday)
                rvospath = rvosdir + '/' + prefix + '_stamp-' + stamp
                if errors:
                    rvospath += errors
                if warnings:
                    rvospath += warnings
                rvospath += '.log'
                self.vosclient.link(vospath, rvospath)
                
                # Now delete all links with the same trunk that
                # do not point to an existing file
                for vfile in self.vosclient.listdir(rvosdir, force=True):
                    m = re.search(r'^(.*?)_' + 
                                  UTDATE_REGEX + 
                                  r'(_ERRORS)?(_WARNINGS)?',
                                  vfile)
                    if m:
                        vtrunk = m.group(1)
                        if vtrunk == prefix:
                            vpath = '/'.join([rvosdir, vfile])
                            info = self.vosclient.getNode(vpath).getInfo()
                            target = info['target']
                            if not self.vosclient.isfile(target):
                                self.vosclient.delete(vpath)

class check_rms(tovos):
    """
    Identify check_rms logs and copy them to VOspace 
    """
    def __init__(self, vosclient, vosroot, log=None):
        tovos.__init__(self, 
                       vosclient, 
                       vosroot + '/check_rms_logs', 
                       log)
        # This regex should work for raw data, obs products and night composites,
        # all of which have the utdate embedded in their observationID.
        self.regex = re.compile(r'(?P<root>'
                                r'checkrms_'
                                r'(?P<obsid>(' # observationID
                                r'(?P<instrument>[^_]+)_' # raw/exposure obsid
                                r'(?P<obsnum>\d+)_'
                                r'(?P<utdate>\d{8})[tT]'
                                r'(?P<uttime>\d{6})|'
                                r'(?P<utdate>[^-]+)-' # night composite obsid
                                r'(?P<hex>[0-9a-f]+))'
                                r')_' + 
                                r'(?P<prodid>[^_]+)_'
                                r'(?P<rcinst>[\d]+)_)' +
                                UTDATE_REGEX)
        self.copy = {}

    def match(self, path):
        """
        Identify raw ingestion logs, record them for deletion, and 
        select the most recent to copy to VOspace.
        
        Arguments:
        path: absolute path to the file to be matched
        """
        filename = os.path.basename(path)
        file_id, ext = os.path.splitext(filename)
        if ext == '.log':
            m = self.regex.match(file_id)
            if m:
                root = m.group('root')
                stamp = m.group('stamp').lower()
                utdate = m.group('utdate')
                
                if root not in self.copy:
                    self.copy[root] = {}
                    self.copy[root]['path'] = path
                    self.copy[root]['stamp'] = stamp
                    self.copy[root]['utdate'] = utdate
                    if self.log:
                        self.log.file('record for copy' + path,
                                      logging.DEBUG)
                elif self.copy[root]['stamp'] < stamp:
                    self.copy[root]['path'] = path
                    self.copy[root]['stamp'] = stamp
                    if self.log:
                        self.log.file('replace for copy' + path,
                                      logging.DEBUG)

    def push(self):
        """
        Copy all files to VOspace and delete any marked for removal.
        
        Arguments:
        <none>
        """
        for root in self.copy:
            path = self.copy[root]['path']
            if self.log:
                self.log.console('pushing ' + path,
                                 logging.DEBUG)
            utdate = self.copy[root]['utdate']
            utyear = utdate[:4]
            utmonth = utdate[4:6]
            utday = utdate[6:]
            
            # As needed, create the /year/month/day/ directories
            vosdir = self.make_subdir(self.vosroot, utyear)
            vosdir = self.make_subdir(vosdir, utmonth)
            vosdir = self.make_subdir(vosdir, utday)
            
            filename = os.path.basename(path)
            vospath = vosdir + '/' + filename
            
            success = self.push_file(path, vospath)
            
            # Try to clean up the directory by deleting old log files
            m = self.regex.search(filename)
            (root, stamp) = m.group('root', 'stamp')
            for vfile in self.vosclient.listdir(vosdir, force=True):
                m = self.regex.search(vfile)
                if m:
                    (vroot, vstamp) = m.group('root', 'stamp')
                    if vroot == root and vstamp < stamp:
                        vpath = vosdir + '/' +  vfile
                        self.vosclient.delete(vpath)

def run():
    """
    Copy files from a pickup directory into VOspace
    """
    myname = os.path.basename(sys.argv[0])
    mypath = os.path.join(sys.path[0], myname)
    
    log = None
    logpath = None
    loglevel = logging.INFO
    
    vosroot = None
    vosclient = vos.Client()
        
    ap = argparse.ArgumentParser('jcmt2vos')
    # directory paths
    ap.add_argument('--source',
                    default='/staging/gimli2/1/redman/daily/',
                    help='file or directory containing files to '
                         'be copied')
    ap.add_argument('--vos',
                    default='vos:jsaops',
                    help='root of VOspace in which to store files')

    # logging
    ap.add_argument('--log',
                    default=myname + '_' + utdate_string() + '.log',
                    help='(optional) name of log file')
    ap.add_argument('--debug', '-d',
                    action='store_true',
                    help='run ingestion commands in debug mode')
    a = ap.parse_args()
    
    # source and destination
    frompath = os.path.abspath(
                   os.path.expandvars(
                       os.path.expanduser(a.source)))
    
    # setup logger
    logpath = os.path.abspath(
                  os.path.expanduser(
                      os.path.expandvars(a.log)))
    
    if a.debug:
        loglevel = logging.DEBUG
                
    with logger(logpath, loglevel, True).record() as log:
        log.file(mypath)
        log.file('jcmt2caom2version    = ' + jcmt2caom2version)
        log.file('tools4caom2version   = ' + tools4caom2version)
        for attr in dir(a):
            if attr != 'id' and attr[0] != '_':
                log.file('%-15s= %s' % (attr, getattr(a, attr)))
        log.file('abs(source) = ' + frompath)
        log.console('logfile = ' + logpath)
        
        vosclient = vos.Client()
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
        
        filehandlers = [raw_ingestion(vosclient, a.vos, log),
                        stdpipe_ingestion(vosclient, a.vos, log),
                        check_rms(vosclient, a.vos, log)]
        
        for filehandler in filehandlers:
            for filepath in filelist:
                filehandler.match(filepath)
            filehandler.push()
                
                            
                            
                    
                    
                    
                

