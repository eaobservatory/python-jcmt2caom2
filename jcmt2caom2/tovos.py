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
    def __init__(self, keep, vosclient, vosroot, log):
        """
        Set up the log and vos client
        
        Arguments:
        keep:  boolean, if True keep file on disk else delete after copy
        vosclient: a vos.Client() object
        vosroot: base vos directory for these files, which must exist
        log: a tools4caom2.looger object
        """
        self.keep = keep
        self.vosclient = vosclient
        self.vosroot = vosroot
        self.log = log
        self.regex = None
        
        self.copylist = {}
        self.deletelist = []

        if not vosclient.isdir(vosroot):
            self.log.console(vosroot + ' is not a VOspace directory',
                        logging.ERROR)

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
                self.log.file('copied ' + path)
                success = True
                if n:
                    self.log.file('retries = ' + str(n))
                break
        else:
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
    def __init__(self, keep, vosclient, vosroot, log):
        tovos.__init__(self, 
                       keep, 
                       vosclient, 
                       vosroot + '/raw_ingestion', 
                       log)
        self.regex = re.compile(r'(?P<root>'
                                 'caom-(?P<collection>[^-]+)-'
                                 '(?P<instrument>[^_]+)_'
                                 '(?P<obsnum>\d+)_'
                                 '(?P<utdate>\d{8})[tT]'
                                 '(?P<uttime>\d{6})'
                                 ')_' + 
                                 UTDATE_REGEX)
        self.delete = []
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
                if not self.keep:
                    self.delete.append(path)
                    self.log.file('record for deletion' + path,
                                  logging.DEBUG)
                
                if root not in self.copy:
                    self.copy[root] = {}
                    self.copy[root]['path'] = path
                    self.copy[root]['stamp'] = stamp
                    self.copy[root]['utdate'] = utdate
                    self.log.file('record for copy' + path,
                                  logging.DEBUG)
                elif self.copy[root]['stamp'] < stamp:
                    self.copy[root]['path'] = path
                    self.copy[root]['stamp'] = stamp
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
            self.log.console('pushing ' + path,
                             logging.DEBUG)
            utdate = self.copy[root]['utdate']
            utyear = utdate[:4]
            utmonth = utdate[4:6]
            utday = utdate[6:]
            
            # As needed, create the /year/month/day/ directories
            vospath = self.make_subdir(self.vosroot, utyear)
            vospath = self.make_subdir(vospath, utmonth)
            vospath = self.make_subdir(vospath, utday)
            
            filename = os.path.basename(path)
            vospath += ('/' + filename)
            
            success = self.push_file(path, vospath)
            if not success and path in self.delete:
                del self.delete[path]
        
        for path in self.delete:
            os.remove(path)

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
    ap.add_argument('--keep',
                    action='store_true',
                    help='keep original file if true, otherwise remove')

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
        
        filehandlers = [raw_ingestion(a.keep, vosclient, a.vos, log)]
        
        for filehandler in filehandlers:
            for filepath in filelist:
                filehandler.match(filepath)
            filehandler.push()
                
                            
                            
                    
                    
                    
                

