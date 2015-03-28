import argparse
from ConfigParser import SafeConfigParser
import logging
import os
import re
import sys

import vos

from tools4caom2.utdate_string import UTDATE_REGEX

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version

logger = logging.getLogger(__name__)


class tovos(object):
    """
    Abstract base class for copying files into VOspace.  Override
    the regex, and the match and push methods for each class of files
    to be copied.
    """
    def __init__(self, vosclient, vosroot):
        """
        Set up the vos client

        Arguments:
        vosclient: a vos.Client() object
        vosroot: base vos directory for these files, which must exist
        """
        self.vosclient = vosclient
        self.vosroot = vosroot
        self.regex = None

        self.copylist = {}

        if not vosclient.isdir(vosroot):
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
        for n in range(2):
            if (filesize == self.vosclient.copy(path, vospath)):
                success = True
                logger.info('copied %s (retries = %i)', path, n)
                break
        else:
            logger.error('failed to copy %s', path)

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
            logger.error(existingpath + ' does not exist')

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


class qa_logs(tovos):
    """
    Identify check_rms logs and copy them to VOspace
    """
    def __init__(self, vosclient, vosroot):
        tovos.__init__(self,
                       vosclient,
                       vosroot + '/QA_logs')
        # This regex should work for logs from single-exposure recipe
        # instances, which use the JCMT obsid as their observationID.
        self.regex = re.compile(r'(?P<root>'
                                r'(checkrms|checkcal|mapstats|calstats)_'
                                r'(?P<obsid>('  # observationID
                                r'(?P<instrument>[^_]+)_'  # raw/exposure obsid
                                r'(?P<obsnum>\d+)_'
                                r'(?P<utdate>\d{8})[tT]'
                                r'(?P<uttime>\d{6})|'
                                r'(?P<dateobs>[^-]+)-'  # night composite obsid
                                r'(?P<hex>[0-9a-f]+))'
                                r')_' +
                                r'(?P<prodid>[^_]+)_'
                                r'(?P<rcinst>[^_]+)_)' +
                                UTDATE_REGEX)
        logger.debug(self.regex.pattern)
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
        logger.debug('qa_logs - examine %s', file_id)
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
                    logger.debug('record for copy %s', path)
                elif self.copy[root]['stamp'] < stamp:
                    self.copy[root]['path'] = path
                    self.copy[root]['stamp'] = stamp
                    logger.debug('replace for copy %s', path)

    def push(self):
        """
        Copy all files to VOspace and delete any marked for removal.

        Arguments:
        <none>
        """
        for root in self.copy:
            path = self.copy[root]['path']
            logger.debug('pushing %s', path)
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
                        vpath = vosdir + '/' + vfile
                        self.vosclient.delete(vpath)


class oracdr_logs(tovos):
    """
    Identify check_rms logs and copy them to VOspace
    """
    def __init__(self, vosclient, vosroot):
        tovos.__init__(self,
                       vosclient,
                       vosroot + '/oracdr_date_obs')
        self.linkroot = vosroot + '/oracdr_date_processed'
        # This regex should work for obs products and night composites,
        # both of which have the utdate embedded in their observationID.
        self.regex = re.compile(r'(?P<root>'
                                r'oracdr_'
                                r'(?P<obsid>('  # observationID
                                r'(?P<instrument>[^_]+)_'  # raw/exposure obsid
                                r'(?P<obsnum>\d+)_'
                                r'(?P<utdate>\d{8})[tT]'
                                r'(?P<uttime>\d{6})|'
                                r'(?P<dateobs>[^-]+)-'  # night composite obsid
                                r'(?P<hex>[0-9a-f]+))'
                                r')_' +
                                r'(?P<prodid>[^_]+)_'
                                r'(?P<rcinst>[^_]+)_)' +
                                UTDATE_REGEX)
        logger.debug(self.regex.pattern)
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
        logger.debug('oracdr_logs - examine %s', file_id)
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
                    logger.debug('record for copy %s', path)
                elif self.copy[root]['stamp'] < stamp:
                    self.copy[root]['path'] = path
                    self.copy[root]['stamp'] = stamp
                    self.copy[root]['stampdate'] = stampdate
                    logger.debug('replace for copy %s', path)

    def push(self):
        """
        Copy all files to VOspace and delete any marked for removal.

        Arguments:
        <none>
        """
        for root in self.copy:
            path = self.copy[root]['path']
            logger.debug('pushing %s', path)
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

            logger.debug('push new file %s', vospath)
            success = self.push_file(path, vospath)
            if success:
                logger.debug('make new link %s', linkpath)
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
                        logger.debug('remove old version of file %s', oldpath)
                        self.vosclient.delete(oldpath)

                        vrpath = (os.path.dirname(self.vosroot) +
                                  '/oracdr_date_processed')
                        vypath = vrpath + '/' + vstampdate[:4]
                        vmpath = vypath + '/' + vstampdate[4:6]
                        vdpath = vmpath + '/' + vstampdate[6:]
                        vpath = vdpath + '/' + vfile
                        logger.debug('remove old version of link %s', vpath)
                        self.vosclient.delete(vpath)

                        # Clean up empty directories
                        if (len(self.vosclient.listdir(vdpath, force=True)) ==
                                0):
                            self.vosclient.delete(vdpath)
                        if (len(self.vosclient.listdir(vmpath, force=True)) ==
                                0):
                            self.vosclient.delete(vmpath)
                        if (len(self.vosclient.listdir(vypath, force=True)) ==
                                0):
                            self.vosclient.delete(vypath)


def run():
    """
    Copy files from a pickup directory into VOspace
    """
    myname = os.path.basename(sys.argv[0])
    mypath = os.path.join(sys.path[0], myname)

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

    ap.add_argument('--debug', '-d',
                    action='store_true',
                    help='run ingestion commands in debug mode')
    a = ap.parse_args()

    # source and destination
    frompath = os.path.abspath(
        os.path.expandvars(
            os.path.expanduser(a.source)))

    if a.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info(mypath)
    logger.info('jcmt2caom2version    = %s', jcmt2caom2version)
    logger.info('tools4caom2version   = %s', tools4caom2version)
    for attr in dir(a):
        if attr != 'id' and attr[0] != '_':
            logger.info('%-15s= %s', attr, getattr(a, attr))
    logger.info('abs(source) = %s', frompath)

    vosclient = vos.Client()
    if not vosclient.isdir(a.vos):
        raise RuntimeError(a.vos + ' is not a VOspace directory')

    filelist = []
    if os.path.isfile(frompath):
        filelist.append(frompath)
    elif os.path.isdir(frompath):
        for filename in os.listdir(frompath):
            filepath = os.path.join(frompath, filename)
            filelist.append(filepath)

    filehandlers = [qa_logs(vosclient, a.vos),
                    oracdr_logs(vosclient, a.vos)]

    for filehandler in filehandlers:
        for filepath in filelist:
            filehandler.match(filepath)
        filehandler.push()
