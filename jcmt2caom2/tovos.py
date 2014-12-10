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
        if self.vosclient.isfile(vospath):
            self.vosclient.delete(vospath)
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

    def push_link(self, existingpath, linkpath):
        """
        Create a link to an existing file if it does not already exist.
        
        Arguments:
        existingpath: path to existing file in VOspace
        linkpath: link path to create
        """
        success = False
        if self.vosclient.isfile(existingpath):
            info = None
            try:
                info = self.vosclient.getNode(linkpath).getInfo()
            except:
                pass
            if info:
                self.vosclient.delete(linkpath)
            self.vosclient.link(existingpath, linkpath)
            success = True
        else:
            self.log.console(existingpath + ' does not exist',
                             logging.ERROR)
        return success

    def clean_link(self, linkdir, trunk):
        """
        Delete all links with the same trunk that do not point to an 
        existing file.
        
        Arguments:
        linkpath: path to new link
        """
        success = False
        for vfile in self.vosclient.listdir(linkdir, force=True):
            m = re.search(r'^(.*?)_' + 
                          UTDATE_REGEX + 
                          r'(_ERRORS)?(_WARNINGS)?',
                          vfile)
            if m:
                vtrunk = m.group(1)
                if vtrunk == trunk:
                    vpath = '/'.join([linkdir, vfile])
                    info = self.vosclient.getNode(vpath).getInfo()
                    if ('target' in info and
                        not self.vosclient.isfile(info['target'])):
                        
                        self.vosclient.delete(vpath)
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

class jcmt2caom2_ingestion(tovos):
    """
    Identify jcmt2caom2ingest logs and copy them to VOspace 
    """
    def __init__(self, vosclient, vosroot, log=None):
        tovos.__init__(self, 
                       vosclient, 
                       vosroot + '/proc_ingestion', 
                       log)
        self.dateroot = vosroot + '/proc_ingestion_date'
        self.rawroot = vosroot + '/raw_ingestion'
        self.regex = re.compile(r'(?P<trunk>(?P<root>'
                                r'jsaingest_'
                                r'(?P<rcinst>[^_]+)'
                                r')_)' + 
                                UTDATE_REGEX +
                                r'(?P<errors>(_ERRORS)?)'
                                r'(?P<warnings>(_WARNINGS)?)')
        self.copy = {}

    def match(self, path):
        """
        Identify processed ingestion logs, record them for deletion, and 
        select the most recent to copy to VOspace.
        
        Arguments:
        path: absolute path to the file to be matched
        """
        filename = os.path.basename(path)
        file_id, ext = os.path.splitext(filename)
        self.log.file('jcmt2caom2_ingestion - examine ' + file_id,
                      logging.DEBUG)
        if ext == '.log':
            m = self.regex.match(file_id)
            if m:
                self.log.file('jcmt2caom2_ingestion - matched ' + file_id,
                              logging.DEBUG)
                root = m.group('root')
                stamp = m.group('stamp').lower()
                stampdate = m.group('stampdate')
                rcinst = m.group('rcinst')
                errors = m.group('errors')
                warnings = m.group('warnings')
                
                if root not in self.copy:
                    self.copy[root] = {}
                    self.copy[root]['path'] = path
                    self.copy[root]['stamp'] = stamp
                    self.copy[root]['rcinst'] = rcinst
                    self.copy[root]['stampdate'] = stampdate
                    self.copy[root]['errors'] = errors
                    self.copy[root]['warnings'] = warnings

                    if self.log:
                        self.log.file('record for copy' + path,
                                      logging.DEBUG)
                elif self.copy[root]['stamp'] < stamp:
                    self.copy[root]['path'] = path
                    self.copy[root]['stamp'] = stamp
                    self.copy[root]['stampdate'] = stampdate
                    self.copy[root]['errors'] = errors
                    self.copy[root]['warnings'] = warnings
                    if self.log:
                        self.log.file('replace for copy' + path,
                                      logging.DEBUG)

    def push(self):
        """
        Copy all files to VOspace and delete files and links 
        with older timestamps.
        
        Arguments:
        <none>
        """
        for root in self.copy:
            path = self.copy[root]['path']
            rcinst = self.copy[root]['rcinst']
            errors = self.copy[root]['errors']
            warnings = self.copy[root]['warnings']
            # The recipe instance ID can be a decimal number, a decimal number
            # with the prefix 'jac-', or a string startng with a project name
            # that in turn should start with 'jcmt'.
            if re.match(r'jcmt-.*', rcinst):
                thousands = 'vos'
            else:
                thousands = rcinst[:-3] + '000-999'
            stamp = self.copy[root]['stamp']
            stampdate = self.copy[root]['stampdate']
            stampyear = stampdate[:4]
            stampmonth = stampdate[4:6]
            stampday = stampdate[6:]
            
            # As needed, create directories and store files and links
            # vosdir: directory in proc_ingestion
            # dvosdir: directory in proc_ingestion_date for new link
            # rvosdir: directory in raw_ingestion for new link
            vosdir = self.make_subdir(self.vosroot, thousands)
            self.log.file('vosdir = ' + vosdir, logging.DEBUG)

            dvosdir = self.make_subdir(self.dateroot, stampyear)
            dvosdir = self.make_subdir(dvosdir, stampmonth)
            dvosdir = self.make_subdir(dvosdir, stampday)
            self.log.file('dvosdir = ' + dvosdir, logging.DEBUG)

            utdate = None
            prefix = None
            rvosdir = None
            
            # The earliest utdate is logged with a specific format in
            # jcmt2caom2ingest.py.  If that log message changes, make the 
            # corresponding change in the regex here and below.
            with open(path, 'r') as L:
                for line in L:
                    m = re.search(r'Earliest utdate: '
                                  r'(?P<utdate>[-0-9]+)'
                                  r' for (?P<prefix>\S+)',
                                  line)
                    if m:
                        (utdate, prefix) = m.group('utdate', 'prefix')
                        break
                self.log.file('utdate = ' + utdate, logging.DEBUG)
                self.log.file('prefix = ' + prefix, logging.DEBUG)
            
            if utdate and prefix:
                (ryear, rmonth, rday) = re.split(r'-', utdate)
                # As needed, create the /year/month/day/ directories
                # Beware that these are URI's, not directory paths in the OS
                rvosdir = self.make_subdir(self.rawroot, ryear)
                rvosdir = self.make_subdir(rvosdir, rmonth)
                rvosdir = self.make_subdir(rvosdir, rday)
                self.log.file('rvosdir = ' + rvosdir, logging.DEBUG)

            filename = os.path.basename(path)
            fileid = os.path.splitext(filename)[0]
            
            # Before pushing the new file, delete older files that are already 
            # present.  Do NOT delete more recent copies, nor copies without
            # errors if the current copy reports errors.  Record files being
            # deleted before doing the deletion.
            deleted_files = []
            for vfile in self.vosclient.listdir(vosdir, force=True):
                vpath = vosdir + '/' +  vfile
                vfile_id, vext = os.path.splitext(vfile)
                m = self.regex.search(vfile_id)
                if m:
                    (vroot, 
                     verrors, 
                     vstamp,
                     vstampdate) = m.group('root', 
                                           'errors',
                                           'stamp',
                                           'stampdate')

                    if vroot == root and vstamp < stamp:
                        # If no errors, delete all earlier logs
                        # if errors, delete earlier logs with errors
                        self.log.file('jcmt2caom2_ingestion - cleanup ' + 
                                      vfile)
                        if not errors or (errors and verrors):
                            # find directories and delete old links
                            # dolddir: directory in proc_ingestion_date
                            # rolddir: directory in raw_ingestion
                            doldyear = vstampdate[:4]
                            doldmonth = vstampdate[4:6]
                            doldday = vstampdate[6:]
                            dolddir = self.make_subdir(self.dateroot, doldyear)
                            dolddir = self.make_subdir(dolddir, doldmonth)
                            dolddir = self.make_subdir(dolddir, doldday)
                            self.log.file('dolddir = ' + dolddir, logging.DEBUG)

                            rutdate = None
                            rprefix = None
                            rolddir = None
                            # Copy the old file to the local disk
                            lpath = os.path.abspath(vfile)
                            try:
                                # The earliest utdate is logged with a 
                                # specific format in jcmt2caom2ingest.py.  
                                # If that log message changes, make the 
                                # corresponding change in the regex here
                                # and above.
                                self.vosclient.copy(vpath, lpath)
                                with open(filename, 'r') as L:
                                    for line in L:
                                        m = re.search(r'Earliest utdate: '
                                                      r'(?P<utdate>[-0-9]+)'
                                                      r' for (?P<prefix>\S+)',
                                                      line)
                                        if m:
                                            (rutdate, rprefix) = \
                                                m.group('utdate', 'prefix')
                                            break
                            finally:
                                if os.path.exists(lpath):
                                    os.remove(lpath)
                            
                            if rutdate and rprefix:
                                (royear, romonth, roday) = \
                                    re.split(r'-', rutdate)
                                # As needed, create the /year/month/day/ directories
                                # Beware that these are URI's, not directory paths in the OS
                                rolddir = self.make_subdir(self.rawroot, royear)
                                rolddir = self.make_subdir(rolddir, romonth)
                                rolddir = self.make_subdir(rolddir, roday)
                                self.log.file('rolddir = ' + rolddir, 
                                              logging.DEBUG)

                            self.log.file('clean ' + vpath)
                            self.clean(vpath, 
                                       root,
                                       stamp,
                                       dolddir,
                                       rolddir,
                                       rprefix)
                            if dolddir != dvosdir:
                                self.clean_empty(dolddir)
                            if rolddir != rvosdir:
                                self.clean_empty(rolddir)

            # Now, push the new file into vosdir
            filename = os.path.basename(path)
            vospath = vosdir + '/' + filename
            
            self.log.file('make file ' + vospath)
            success = self.push_file(path, vospath)
            if success:
                ##############################################################
                # Try to create a link in the proc_date_ingestion directories
                # Clean out links to older copies of the log
                ##############################################################
                dvospath = dvosdir + '/' + filename
                self.log.file('make link ' + dvospath + ' -> ' + vospath)
                self.push_link(vospath, dvospath)

                ##############################################################
                # Try to create a link in the raw_ingestion directories
                # Read the necessary metadata from the log file.
                ##############################################################
                if utdate and prefix:
                    # The link needs to sort properly, so the prefix must 
                    # match the raw data ingestion log
                    rvospath = rvosdir + '/' + prefix + '_stamp-' + stamp
                    if errors:
                        rvospath += errors
                    if warnings:
                        rvospath += warnings
                    rvospath += '.log'
                    self.log.file('make link ' + rvospath + ' -> ' + vospath)
                    self.push_link(vospath, rvospath)

    def clean_empty(self, datedir):
        """
        Remove empty VOspace directories up to three levels deep (YYYY/MM/DD)
        """
        # Clean up empty directories, up to three levels
        # corresponding to YYYY/MM/DD
        mydir = datedir
        for n in range(3):
            if len(self.vosclient.listdir(mydir, 
                                          force=True)) == 0:
                self.log.file('delete empty directory' + mydir)
                self.vosclient.delete(mydir)
                mydir = os.path.dirname(mydir)
            else:
                break        
        
    def clean(self, path, root, stamp, datedir, rawdir, rawprefix):
        """
        Delete a file from vosroot and clean up any links to that file
        It can be assumed that the arguments have already been parsed from the
        file name.
        
        Arguments:
        path:  path to the file in VOspace
        root:  part of the file name preceeding the time stamp
        stamp: timestamp of the link in raw_ingestions
        datedir: VOspace directory containing date link
        rawdir: VOspace directory containing raw links 
        rawprefix: prefix of the link in raw_ingestions
        
        If the observation has no members, rawdir and rawprefix will be None.
        """
        # Find any links in proc_ingestion_date matching the root-part of
        # the existing filename but with an earlier time stamp and
        # delete them.
        ddel = False
        for dfile in self.vosclient.listdir(datedir, force=True):
            dmm = self.regex.search(dfile)
            if dmm:
                if (dmm.group('root') == root and
                    dmm.group('stamp').lower() < stamp):
                    
                    dpath = datedir + '/' + dfile
                    self.log.file('delete ' + dpath)
                    ddel = True
                    self.vosclient.delete(dpath)
                    
        if not ddel:
            self.log.file('nothing to delete in ' + datedir +
                          ' matching ' + root +
                          ' with stamp < ' + stamp,
                          logging.WARN)
        
        # Find any links in raw_ingestion matching the rawprefix but with an 
        # earlier time stamp and delete them
        rdel = True
        if rawprefix and rawdir:
            rdel = False
            for rlink in self.vosclient.listdir(rawdir, force=True):
                dmm =  re.match(rawprefix + '.*' + 
                                UTDATE_REGEX +
                                r'(?P<errors>(_ERRORS)?)' +
                                r'(?P<warnings>(_WARNINGS)?)', 
                                rlink)
                if dmm:
                    if dmm.group('stamp').lower() < stamp:
                        rpath = rawdir + '/' + rlink
                        self.log.file('delete ' + rpath)
                        rdel = True
                        self.vosclient.delete(rpath)

        if not rdel:
            self.log.file('nothing to delete in ' + rawdir +
                          ' matching ' + rawprefix +
                          ' with stamp < ' + stamp,
                          logging.WARN)

        self.vosclient.delete(path)

class qa_logs(tovos):
    """
    Identify check_rms logs and copy them to VOspace 
    """
    def __init__(self, vosclient, vosroot, log=None):
        tovos.__init__(self, 
                       vosclient, 
                       vosroot + '/QA_logs', 
                       log)
        # This regex should work for logs from single-exposure recipe instances,
        # which use the JCMT obsid as their observationID.
        self.regex = re.compile(r'(?P<root>'
                                r'(checkrms|checkcal|mapstats|calstats)_'
                                r'(?P<obsid>(' # observationID
                                r'(?P<instrument>[^_]+)_' # raw/exposure obsid
                                r'(?P<obsnum>\d+)_'
                                r'(?P<utdate>\d{8})[tT]'
                                r'(?P<uttime>\d{6})|'
                                r'(?P<dateobs>[^-]+)-' # night composite obsid
                                r'(?P<hex>[0-9a-f]+))'
                                r')_' + 
                                r'(?P<prodid>[^_]+)_'
                                r'(?P<rcinst>[^_]+)_)' +
                                UTDATE_REGEX)
        self.log.file(self.regex.pattern,
                      logging.DEBUG)
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
        self.log.file('qa_logs - examine ' + file_id,
                      logging.DEBUG)
        if ext == '.log':
            m = self.regex.match(file_id)
            if m:
                mm = m.groupdict()
                root = mm['root']
                stamp = mm['stamp'].lower()
                utdate = mm['utdate']
                obsnum = mm['obsnum']
                
                if root not in self.copy:
                    self.copy[root] = {}
                    self.copy[root]['path'] = path
                    self.copy[root]['stamp'] = stamp
                    self.copy[root]['utdate'] = utdate
                    self.copy[root]['obsnum'] = obsnum
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
            obsnum = self.copy[root]['obsnum']
            
            # As needed, create the /year/month/day/ directories
            vosdir = self.make_subdir(self.vosroot, utyear)
            vosdir = self.make_subdir(vosdir, utmonth)
            vosdir = self.make_subdir(vosdir, utday)
            vosdir = self.make_subdir(vosdir, obsnum)
            
            filename = os.path.basename(path)
            vospath = vosdir + '/' + filename
            
            success = self.push_file(path, vospath)
            
            # Try to clean up the directory by deleting old log files
            m = self.regex.search(filename).groupdict()
            root = m['root']
            stamp = m['stamp']
            for vfile in self.vosclient.listdir(vosdir, force=True):
                m = self.regex.search(vfile)
                if m:
                    mm = m.groupdict()
                    vroot = mm['root']
                    vstamp = mm['stamp']
                    if vroot == root and vstamp < stamp:
                        vpath = vosdir + '/' +  vfile
                        self.vosclient.delete(vpath)

class oracdr_logs(tovos):
    """
    Identify check_rms logs and copy them to VOspace 
    """
    def __init__(self, vosclient, vosroot, log=None):
        tovos.__init__(self, 
                       vosclient, 
                       vosroot + '/oracdr_date_obs', 
                       log)
        self.linkroot = vosroot + '/oracdr_date_processed'
        # This regex should work for obs products and night composites,
        # both of which have the utdate embedded in their observationID.
        self.regex = re.compile(r'(?P<root>'
                                r'oracdr_'
                                r'(?P<obsid>(' # observationID
                                r'(?P<instrument>[^_]+)_' # raw/exposure obsid
                                r'(?P<obsnum>\d+)_'
                                r'(?P<utdate>\d{8})[tT]'
                                r'(?P<uttime>\d{6})|'
                                r'(?P<dateobs>[^-]+)-' # night composite obsid
                                r'(?P<hex>[0-9a-f]+))'
                                r')_' + 
                                r'(?P<prodid>[^_]+)_'
                                r'(?P<rcinst>[^_]+)_)' +
                                UTDATE_REGEX)
        self.log.file(self.regex.pattern,
                      logging.DEBUG)
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
        self.log.file('oracdr_logs - examine ' + file_id,
                      logging.DEBUG)
        if ext == '.log':
            m = self.regex.match(file_id)
            if m:
                mm = m.groupdict()
                root = mm['root']
                stamp = mm['stamp'].lower()
                utdate = '00000000'
                if 'utdate' in mm:
                    utdate = mm['utdate']
                elif 'dateobs' in mm:
                    utdate = mm['dateobs']
                rcinst = mm['rcinst']
                stampdate = mm['stampdate']
                
                if root not in self.copy:
                    self.copy[root] = {}
                    self.copy[root]['path'] = path
                    self.copy[root]['stamp'] = stamp
                    self.copy[root]['stampdate'] = stampdate
                    self.copy[root]['utdate'] = utdate
                    self.copy[root]['rcinst'] = rcinst
                    if self.log:
                        self.log.file('record for copy' + path,
                                      logging.DEBUG)
                elif self.copy[root]['stamp'] < stamp:
                    self.copy[root]['path'] = path
                    self.copy[root]['stamp'] = stamp
                    self.copy[root]['stampdate'] = stampdate
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
            rcinst = self.copy[root]['rcinst']
            stampdate = self.copy[root]['stampdate']
            stampyear = stampdate[:4]
            stampmonth = stampdate[4:6]
            stampday = stampdate[6:]
            
            # As needed, create the /year/month/day/rcinst directories
            vosdir = self.make_subdir(self.vosroot, utyear)
            vosdir = self.make_subdir(vosdir, utmonth)
            vosdir = self.make_subdir(vosdir, utday)
            
            # As needed, create the directory for the proceesing link
            linkdir = self.make_subdir(self.linkroot, stampyear)
            linkdir = self.make_subdir(linkdir, stampmonth)
            linkdir = self.make_subdir(linkdir, stampday)
            
            filename = os.path.basename(path)
            vospath = vosdir + '/' + filename
            linkpath = linkdir + '/' + filename
            
            m = self.regex.search(filename).groupdict()
            root = m['root']
            stamp = m['stamp']
            
            self.log.file('push new file ' + vospath,
                          logging.DEBUG)
            success = self.push_file(path, vospath)
            if success:
                self.log.file('make new link ' + linkpath,
                              logging.DEBUG)
                self.push_link(vospath, linkpath)
            
            # Delete all earlier versions of the same file
            # and remove their links.
            # Try to clean up the directory by deleting old log files
            for vfile in self.vosclient.listdir(vosdir, force=True):
                m = self.regex.search(vfile)
                if m:
                    mm = m.groupdict()
                    vroot = mm['root']
                    vstamp = mm['stamp']
                    vstampdate = mm['stampdate']
                    if vroot == root and vstamp < stamp:
                        # Delete the old file
                        oldpath = vosdir + '/' + vfile
                        self.log.file('remove old version of file ' + oldpath,
                                      logging.DEBUG)
                        self.vosclient.delete(oldpath)
                        
                        vrpath = (os.path.dirname(self.vosroot) + 
                                    '/oracdr_date_processed')
                        vypath = vrpath + '/' + vstampdate[:4]
                        vmpath = vypath + '/' + vstampdate[4:6]
                        vdpath = vmpath + '/' + vstampdate[6:]
                        vpath = vdpath + '/' +  vfile
                        self.log.file('remove old version of link ' + vpath,
                                      logging.DEBUG)
                        self.vosclient.delete(vpath)
                        
                        # Clean up empty directories
                        if len(self.vosclient.listdir(vdpath, force=True)) == 0:
                            self.vosclient.delete(vdpath)
                        if len(self.vosclient.listdir(vmpath, force=True)) == 0:
                            self.vosclient.delete(vmpath)
                        if len(self.vosclient.listdir(vypath, force=True)) == 0:
                            self.vosclient.delete(vypath)

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
                    default='.',
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
                        jcmt2caom2_ingestion(vosclient, a.vos, log),
                        qa_logs(vosclient, a.vos, log),
                        oracdr_logs(vosclient, a.vos, log)]
        
        for filehandler in filehandlers:
            for filepath in filelist:
                filehandler.match(filepath)
            filehandler.push()
                
                            
                            
                    
                    
                    
                

