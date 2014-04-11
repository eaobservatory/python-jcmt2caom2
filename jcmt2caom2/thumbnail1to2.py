#!/usr/bin/env python2.7

import argparse
import getpass
import logging
from PIL import Image
import os
import os.path
import re
import shutil
import subprocess
import sys
from threading import Event

from tools4caom2.logger import logger
from tools4caom2.database import database
from tools4caom2.database import connection
from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version

def thumbnail_name(collection, observationID, productID, size):
    """
    Return a properly formatted file name for a CAOM-2 thumbnail
    """
    return '_'.join([collection,
                     observationID,
                     productID, 
                     'preview', 
                     size]) + '.png'

class thumb1to2(object):
    """
    Transform CAOM-1 thumbnail images to the format and file naming conventions
    required for CAOM-2, and optionally persist them into AD.
    """
    def __init__(self):
        """
        Create a thumb1to2 object.
        """
        self.loglevel = logging.INFO
        self.logname = None
        self.logfile = None
        self.log = None
        
        self.obs = None
        self.utdate = None
        self.begin = None
        self.end = None
        
        self.rawingest = False
        self.procingest = False
        
        self.scuba2 = False
        self.debug = False
        self.persist = False
        self.transdir = ''
        self.transhost = ''
        self.transcount = 0
        self.transpause = 0.0
        self.timer = Event()
        self.jcmt2caom2raw = os.path.realpath(
                                os.path.join(sys.path[0], 'jcmt2caom2raw'))
        self.jcmt2caom2proc = os.path.realpath(
                                os.path.join(sys.path[0], 'jcmt2caom2proc'))
    
    def commandLineArguments(self):
        """
        Process command line arguments.
        Usage:
        One of 
            --obs
            --utdate
            --begin=YYYYMMDD --end=YYYYMMDD
        must be given or a tools4caom2.logger.logger.LoggerError will be raised.
        
        To use an externally defined log file give the file path as:
            --log=LOGFILEPATH
        Otherwise, a log file will be created in the logdir, which defaults to 
        the current directory or may be specified with:
            --logdir=LOGDIRECTORY
        
        The logic to find preview filenames is different for SCUBA-2 and ACSIS.
        The argument
            --scuba2
        says to use the SCUBA-2 rules and ignore heterodyne previews.  
        If omitted, heterodyne previews will be processed and SCUBA-2 previews 
        will be ignored.
        
        
        """
        ap = argparse.ArgumentParser('thumbnail')
        ap.add_argument('--obs',
                        help='convert thumbnails for the specified '
                             ' observation')
        ap.add_argument('--utdate',
                        help='date to convert thumbnails')
        ap.add_argument('--begin',
                        help='beginning date to convert thumbnails')
        ap.add_argument('--end',
                        help='ending date to convert thumbnails')

        ap.add_argument('--scuba2',
                        action='store_true',
                        help='search for SCUBA-2 thumbnails rather than '
                             'heterodyne')
        ap.add_argument('--rawingest',
                        action='store_true',
                        help='re-ingest raw data before converting thumbnails')
        ap.add_argument('--procingest',
                        action='store_true',
                        help='re-ingest recipe instances before converting thumbnails')

        ap.add_argument('--log',
                        help='(optional) name of log file')
        ap.add_argument('--logdir',
                        default='.',
                        help='(optional) name of directory to hold log files')

        ap.add_argument('--pngdir',
                        default='png',
                        help='(optional) working directory for png files')

        ap.add_argument('--persist',
                        action='store_true',
                        help='copy png files to e-transfer directory')
        
        ap.add_argument('--user',
                        help='username for copy to transfer directory')
        ap.add_argument('--transdir',
                        default='/staging/proc/cadcops/jcmtdp/pickup_lowpriority',
                        help='host name for transfer directory')
        ap.add_argument('--transhost',
                        default='etranscache1',
                        help='host name for transfer directory')
        ap.add_argument('--transcount',
                        type=int,
                        default=100,
                        help='number of files to ingest before pause')
        ap.add_argument('--transpause',
                        type=float,
                        default=300.0,
                        help='seconds to pause')
        
        ap.add_argument('--debug', '-d',
                        action='store_true',
                        help='set loglevel = debug')

        a = ap.parse_args()

        self.loglevel = logging.INFO        
        if a.debug:
            self.debug = True
            self.loglevel = logging.DEBUG
        
        logname = None
        basename = 'thumbnail_'
        
        if a.scuba2:
            self.scuba2 = True
            self.loglevel = logging.DEBUG
            basename += 'scuba-2_'
        
        if a.rawingest:
            self.rawingest = a.rawingest
        if a.procingest:
            self.procingest = a.procingest
            
        if a.obs:
            self.obs = a.obs
            self.logname = basename + a.obs
        elif a.utdate:
            self.utdate = a.utdate
            self.logname = basename + a.utdate
        elif a.begin and a.end:
            self.begin = a.begin
            self.end = a.end
            self.logname = basename + a.begin + '_' + a.end
        else:
            raise RuntimeError(
                'Must set --obs or --utdate or both of --begin and --end')
            
        if a.log:
            self.logfile = os.path.abspath(
                                os.path.expanduser(
                                    os.path.expandvars(self.a.log)))
        else:
            self.logdir = os.path.abspath(
                                os.path.expanduser(
                                    os.path.expandvars(a.logdir)))
            self.logfile = os.path.join(self.logdir, self.logname + '.log')
            
        self.log = logger(self.logfile, loglevel=self.loglevel)
        
        self.pngdir = os.path.abspath(
                        os.path.expanduser(
                            os.path.expandvars(a.pngdir)))
        if not os.path.exists(self.pngdir):
            os.makedirs(self.pngdir)
        if not os.path.isdir(self.pngdir):
            self.log.console('png dir does not exist: ' + self.pngdir)
        
        if a.persist:
            self.persist = True
            self.transdir = a.transdir
            self.transhost = a.transhost
            self.transcount = a.transcount
            self.transpause = a.transpause
            
            if a.user:
                self.user = a.user
            else:
                self.user = getpass.getuser()

        self.log.console('logfile =     ' + self.logfile)
        self.log.file('tools4caom2 = ' + 
                      tools4caom2version)
        self.log.file('jcmt2caom2  = ' + 
                      jcmt2caom2version)
        self.log.file('logdir =      ' + str(self.loglevel))
        self.log.file('pngdir =      ' + self.pngdir)
        
        if self.obs:
            self.log.file('obs =         ' + self.obs)
        if self.utdate:
            self.log.file('utdate =      ' + self.utdate)
        if self.begin:
            self.log.file('begin =       ' + self.begin)
            self.log.file('end =         ' + self.end)
        self.log.file('scuba2 =      ' + str(self.scuba2))
        
        self.log.file('debug =       ' + str(self.debug))
        self.log.file('persist =     ' + str(self.persist))
        if self.persist:
            self.log.file('user     =    ' + str(self.user))
            self.log.file('transdir  =   ' + str(self.transdir))
            self.log.file('transhost  =  ' + str(self.transhost))
            self.log.file('transcount  = ' + str(self.transcount))
            self.log.file('transpause  = ' + str(self.transpause))
        
    def convertThumbnails(self):
        """
        Read the CAOM-1 thumbnails from AD and reformat them with CAOM-2
        file names.
        """
        # ensure that the pngdir exists and is empty
        if os.path.exists(self.pngdir):
            shutil.rmtree(self.pngdir)
        os.mkdir(self.pngdir)
        rawset = set([])
        procset = set([])
        daylist = []
        
        with connection('SYBASE', 'jcmt', self.log) as db:
            count = 0
            if self.obs:
                daylist = [self.obs]
            elif self.utdate:
                daylist = [self.utdate]
            elif self.begin and self.end:
                sqlcmd = '\n'.join([
                    'SELECT utdate',
                    'FROM jcmtmd.dbo.COMMON',
                    'WHERE utdate >= ' + str(self.begin),
                    '      AND utdate <= ' + str(self.end)])
                if self.scuba2:
                    sqlcmd += '\n      AND backend = "SCUBA-2"'
                else:
                    sqlcmd += '\n      AND backend = "ACSIS"'
                    
                results = db.read(sqlcmd)
                if results:
                    daylist = [str(x[0]) for x in results]
            
            if not daylist:
                self.log.console('No entries to process')
            
            for day in daylist:
                if self.logdir:
                    mylogdir = os.path.join(self.logdir, str(day))
                else:
                    mylogdir = os.path.abspath(str(day))
                self.log.console('PROGRESS: mkdir ' + mylogdir)
                if not os.path.exists(mylogdir):
                    os.makedirs(mylogdir)
                        
                if self.rawingest and not self.obs:
                    # Delete existing log files
                    for f in os.listdir(mylogdir):
                        base, ext = os.path.splitext(f)
                        if ext == '.log':
                            self.log.file('PROGRESS: remove ' + f)
                            os.remove(f)
                        
                    sqlcmd = '\n'.join([
                        'SELECT obsid',
                        'FROM jcmtmd.dbo.COMMON',
                        'WHERE utdate=' + str(day)])
                    if self.scuba2:
                        sqlcmd += '\n      AND backend = "SCUBA-2"'
                    else:
                        sqlcmd += '\n      AND backend = "ACSIS"'
                    results = db.read(sqlcmd)
                    if results:
                        # use jcmt2Caom2DA to reingest each observation
                        for line in results:
                            obsid = line[0]
                            
                            mylogfile = os.path.join(mylogdir,
                                                     obsid + '.log')
                            
                            cmd = ['jcmt2Caom2DA',
                                   '--full',
                                   '--start=' + obsid,
                                   '--end=' + obsid,
                                   '--script=' + self.jcmt2caom2raw,
                                   '--log=' + mylogfile]
                            cmdline = ' '.join(cmd)
                            try:
                                self.log.console(cmdline)
                                if self.persist:
                                    output = subprocess.check_output(cmdline,
                                                    shell=True,
                                                    stderr=subprocess.STDOUT)
                            except Exception as e:
                                self.log.console('PROBLEM ingesting ' +
                                                 obsid,
                                                 logging.WARN)
                                self.log.file(e.output)
                            
                if self.procingest and not self.obs:
                    procset = set([])

                    sqlcmd = '\n'.join([
                        'SELECT distinct identity_instance_id',
                        'FROM data_proc.dbo.dp_file_input dfo',
                        'WHERE dfo.dp_input like "ad:JCMT/' +
                        ('s[48][abcd]' if self.scuba2 else 'a') +
                        day + '%"'])
                    results = db.read(sqlcmd)
                    if results:
                        # use jcmt2Caom2DA to reingest each observation
                        for line in results:
                            runid = line[0]
                            if runid not in procset:
                                procset.add(runid)
                                
                                mylogfile = os.path.join(mylogdir,
                                            'dp_' + str(runid) + '.log')
                                
                                cmd = ['jcmt2caom2proc',
                                       '--log=' + mylogfile,
                                       'dp:' + str(runid)]
                                try:
                                    self.log.console(' '.join(cmd))
                                    if self.persist:
                                        output = subprocess.check_output(cmd,
                                                        shell=True,
                                                        stderr=subprocess.STDOUT)
                                except Exception as e:
                                    self.log.console('PROBLEM ingesting ' +
                                                     'dp:' + str(runid),
                                                     logging.WARN)
                                    self.log.file(e.output)
        
                sqllist = [
                    'SELECT o.observationID,',
                    '       o.algorithm_name,',
                    '       p.productID,',
                    '       p.provenance_inputs,',
                    '       p.provenance_runID,',
                    '       a.uri',
                    'FROM jcmt.dbo.caom2_Observation o',
                    '    INNER JOIN jcmt.dbo.caom2_Plane p on o.obsID=p.obsID',
                    '    INNER JOIN jcmt.dbo.caom2_Artifact a ON p.planeID=a.planeID',
                    'WHERE']
                if self.obs:
                    sqllist.append('    o.observationID = "' + day + '"')
                else:
                    sqllist.append('    substring(a.uri, 14, 8) = "' + 
                                       day + '"')
                if self.scuba2:
                    sqllist.append('    AND a.uri like "ad:JCMT/jcmts%reduced%"')
                else:
                    sqllist.append('    AND a.productType = "preview"')
                
                sqllist.append('ORDER BY o.observationID, p.productID, a.uri')
                sqlcmd = '\n'.join(sqllist)
                
                retvals = db.read(sqlcmd)
                
                oldobs = None
                oldprod = None
                uridict = {}
                
                for obs, algorithm, prod, prov_inputs, prov_runid, uri in retvals:
                    self.log.file('observationID         = ' + obs)
                    self.log.file('  algorithm           = ' + algorithm)
                    self.log.file('  productID           = ' + prod)
                    if prov_inputs:
                        self.log.file('    provenance_inputs = ' + prov_inputs,)
                    else:
                        self.log.file('Cannot create image for raw plane because'
                                      ' provenance_inputs = NULL for ' +
                                      uri + ' from recipe instance ' + prov_runid,
                                      logging.WARN)
                    self.log.file('    provenance_runID  = ' + prov_runid)
                    self.log.file('    artifact_uri      = ' + uri)
                    
                    if obs not in uridict:
                        uridict[obs] = {}
                    
                    simple = (algorithm == 'exposure')
                    
                    if prod not in uridict[obs]:
                        uridict[obs][prod] = {}
                    uridict[obs][prod]['runid'] = prov_runid
                    
                    if simple:
                        # There should only be one input for a reduced plane in a
                        # single exposure observation, but in case there are
                        # more (bad hybrid processing) keep only the smallest
                        # matching a raw plane.
                        rawprod = 'zzzzzzzzz'
                        if prov_inputs:
                            for in_uri in re.split(r'\s+', prov_inputs):
                                this_rawprod = re.split(r'/', in_uri)[2]
                                if re.match(r'^raw_(\w+_)?\d+$', this_rawprod):
                                    if this_rawprod < rawprod:
                                        rawprod = this_rawprod
                            if re.match(r'^raw_(\w+_)?\d+$', rawprod):
                                uridict[obs][prod]['rawprod'] = rawprod
                    
                    if self.scuba2:
                        if not re.search(r'reduced\d{3}', uri):
                            self.log.console('SCUBA-2 file_id is not reduced',
                                        logging.WARN)
                            continue
                        uridict[obs][prod]['reduced'] = \
                            re.sub(r'(.*?reduced)\d{3}(.*)', r'\1\2', uri)
                    else:
                        if re.search(r'rsp', uri):
                            uridict[obs][prod]['rsp'] = uri
                        
                        elif re.search(r'rimg', uri):
                            uridict[obs][prod]['rimg'] = uri
                
                self.log.file('uridict:', logging.DEBUG)
                for obs in uridict:
                    self.log.file(obs, logging.DEBUG)
                    for prod in uridict[obs]:
                        self.log.file('    ' + prod, logging.DEBUG)
                        for key in uridict[obs][prod]:
                            self.log.file('        ' + key + ': ' + 
                                     uridict[obs][prod][key],
                                     logging.DEBUG)
                
                adGet = 'adGet -a'
                for obs in uridict:
                    for prod in uridict[obs]:
                        if not self.scuba2 and 'rsp' not in uridict[obs][prod]:
                            self.log.console('skipping caom:' + obs + '/' + prod +
                                        ' because no rsp image was stored from'
                                        ' recipe_instance_id = ' + 
                                        uridict[obs][prod]['runid'],
                                        logging.WARN)
                            continue
                        
                        self.log.console('PROGRESS obs=' + obs + 
                                         '  prod = ' + prod)
                        for size in ('256', '1024'):
                            if self.scuba2:
                                storage, archive, file_id = \
                                    re.match(r'^(\w+):([A-Z]+)/([^\s]+)\s*$', 
                                             uridict[obs][prod]['reduced']).groups()
                                old_id = (file_id +  '_' + size)
                                old_png = old_id + '.png'
                                
                                adGetCmd = ' '.join([adGet, archive, old_id])
                                try:
                                    output = subprocess.check_output(
                                                adGetCmd,
                                                shell=True,
                                                stderr=subprocess.STDOUT)
                                    self.log.file(output)
                                except subprocess.CalledProcessError as e:
                                    self.log.console('Could not get ' + old_id +
                                                ' from recipe_instance_id = ' +
                                                uridict[obs][prod]['runid'],
                                                logging.WARN)
                                    continue
                                
                                reduced_png = thumbnail_name('JCMT',
                                                             obs,
                                                             prod,
                                                             size)
                                shutil.copyfile(old_png, 
                                                os.path.join(self.pngdir,
                                                             reduced_png))
                                
                                if 'rawprod' in uridict[obs][prod]:
                                    rawprod = uridict[obs][prod]['rawprod']
                                    raw_png = thumbnail_name('JCMT',
                                                               obs,
                                                               rawprod,
                                                               size)
                                    shutil.copyfile(os.path.join(self.pngdir,
                                                                 reduced_png), 
                                                    os.path.join(self.pngdir,
                                                                 raw_png))
                                os.remove(old_png)
                                
                            else:
                                # ACSIS-like heterodyne backends
                                storage, archive, file_id = \
                                    re.match(r'^(\w+):([A-Z]+)/([^\s]+)\s*$', 
                                             uridict[obs][prod]['rsp']).groups()
                                rsp_id = (file_id +  '_' + size)
                                rsp_png = rsp_id + '.png'
                                
                                adGetCmd = ' '.join([adGet, archive, rsp_id])
                                try:
                                    output = subprocess.check_output(
                                                adGetCmd,
                                                shell=True,
                                                stderr=subprocess.STDOUT)
                                    self.log.file(output)
                                except subprocess.CalledProcessError as e:
                                    self.log.console('Could not get ' + rsp_id +
                                                ' from recipe_instance_id = ' +
                                                uridict[obs][prod]['runid'],
                                                logging.WARN)
                                    continue
                                rsp_thumb = Image.open(rsp_png)
                                os.remove(rsp_png)

                                nsize = int(size)
                                xysize = (2*nsize, nsize)
                                thumb = Image.new('RGB', xysize, 'grey')
                                thumb.paste(rsp_thumb, (0, 0))

                                if 'rimg' in uridict[obs][prod]:
                                    storage, archive, file_id = \
                                        re.match(r'^(\w+):([A-Z]+)/([^\s]+)\s*$', 
                                                 uridict[obs][prod]['rimg']).groups()
                                    rimg_id = (file_id +  '_' + size)
                                    rimg_png = rimg_id + '.png'
                                    
                                    adGetCmd = ' '.join([adGet, archive, rimg_id])
                                    try:
                                        output = subprocess.check_output(
                                                    adGetCmd,
                                                    shell=True,
                                                    stderr=subprocess.STDOUT)
                                        self.log.file(output)
                                    except subprocess.CalledProcessError as e:
                                        self.log.console('Could not get ' + rimg_id +
                                                    ' from recipe_instance_id = ' +
                                                    uridict[obs][prod]['runid'],
                                                    logging.WARN)
                                        continue

                                    rimg_thumb = Image.open(rimg_png)
                                    os.remove(rimg_png)
                                    thumb.paste(rimg_thumb, (nsize, 0))
                                
                                reduced_png = os.path.join(
                                                self.pngdir,
                                                thumbnail_name('JCMT',
                                                               obs,
                                                               prod,
                                                               size))
                                thumb.save(reduced_png)

                                if 'rawprod' in uridict[obs][prod]:
                                    rawprod = uridict[obs][prod]['rawprod']
                                    raw_png = os.path.join(
                                                    self.pngdir,
                                                    thumbnail_name('JCMT',
                                                                   obs,
                                                                   rawprod,
                                                                   size))
                                    shutil.copyfile(reduced_png, raw_png)
                        
                                    cubeprod = re.sub(r'^reduced_(\d+)$', 
                                                      r'cube_\1', 
                                                      prod)
                                    cube_png = os.path.join(
                                                    self.pngdir,
                                                    thumbnail_name('JCMT',
                                                                   obs,
                                                                   cubeprod,
                                                                   size))
                                    shutil.copyfile(reduced_png, cube_png)
                    
                    # persist in batches, if requested
                    count += 1
                    if self.persist and count >= self.transcount:
                        self.persistpng()
                        count = 0
                        if self.transpause > 0.0:
                            self.timer.wait(self.transpause)
        
        if self.persist and count:
            self.persistpng()
            
    def persistpng(self):
        """
        Persist png files through e-transfer.
        """
        cmd = ' '.join(['scp', 
                        os.path.join(self.pngdir, '*.png'),
                        self.user + "@" + self.transhost + ":" +
                        self.transdir])
        self.log.console('copy png files to transdir: ' + cmd)
        
        try:
            output = subprocess.check_output(cmd,
                                             shell=True,
                                             stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            output = e.output
        if output:
            self.log.console(output)

        for f in os.listdir(self.pngdir):
            fpath = os.path.join(self.pngdir, f)
            os.remove(fpath)

    def run(self):
        """
        Run thumbnail conversion
        """
        self.commandLineArguments()
        self.convertThumbnails()
