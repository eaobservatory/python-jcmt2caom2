#!/usr/bin/env python2.7

import argparse
import logging
from PIL import Image
import os.path
import re
import shutil
import subprocess

from tools4caom2.logger import logger
from tools4caom2.database import database
from tools4caom2.database import connection
from tools4caom2.gridengine import gridengine

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
        
        self.scuba2 = False
        self.debug = False
        self.persist = False
        self.qsub = False
    
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

        ap.add_argument('--log',
                        help='(optional) name of log file')
        ap.add_argument('--logdir',
                        default='.',
                        help='(optional) name of directory to hold log files')

        ap.add_argument('--pngdir',
                        default='png',
                        help='(optional) name of directory to hold png files')

        ap.add_argument('--persist',
                        action='store_true',
                        help='persist thumbnails using dpCapture')
        
        ap.add_argument('--qsub',
                        action='store_true',
                        help='submit one utdate per job to gridengine')

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
            raise logger.LoggerError(
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
            
        if a.persist:
            self.persist = True

        if a.qsub:
            self.qsub = True

        self.log = logger(self.logfile, loglevel=self.loglevel)
        
        self.pngdir = a.pngdir
        
        self.log.console('logfile = ' + self.logfile)
        if self.obs:
            self.log.file('obs =      ' + self.obs)
        if self.utdate:
            self.log.file('utdate =   ' + self.utdate)
        if self.begin:
            self.log.file('begin =    ' + self.begin)
            self.log.file('end =      ' + self.end)
        self.log.file('scuba2 =   ' + str(self.scuba2))
        
        if self.logdir:
            self.log.file('logdir =   ' + str(self.loglevel))

        self.log.file('pngdir =   ' + self.pngdir)
        self.log.file('debug =    ' + str(self.debug))
        self.log.file('persist =  ' + str(self.persist))
        self.log.file('qsub =     ' + str(self.qsub))
        
    def submitToGridengine(self):
        """
        Create jobs to process one utdate at a time under gridengine.
        """
        mygridengine = gridengine(self.log)
        
        cmd = 'jcmt1to2thumbnails'
        if self.debug:
            cmd += ' --debug' 
        if self.scuba2:
            cmd += ' --scuba2'
        if self.persist:
            cmd += ' --persist'
        if self.logdir:
            cmd += (' --logdir=' + self.logdir)
            dirpath = self.logdir
        else:
            cmd += (' --log=' + self.logfile)
            dirpath = os.path.dirname(self.logfile)
        cmd += ' --pngdir=' + self.pngdir

        if self.obs:
            cshpath = os.path.join(dirpath, self.logname + '.csh')
            cmd += ' --obs=' + self.obs
            mygridengine.submit(cmd, cshpath, self.logfile)
            
        elif self.utdate:
            cshpath = os.path.join(dirpath, self.logname + '.csh')
            cmd += ' --utdate=' + self.utdate
            mygridengine.submit(cmd, cshpath, self.logfile)
            
        elif self.begin and self.end:
            with connection('SYBASE', 'jcmtmd', self.log) as db:
                sqllist = [
                    'SELECT c.utdate',
                    'FROM jcmtmd.dbo.COMMON c',
                    'WHERE c.utdate >= %s AND c.utdate <= %s' % (self.begin, self.end)]
                if self.scuba2:
                    sqllist.append('    AND c.backend = "SCUBA-2"')
                else:
                    sqllist.append('    AND c.backend != "SCUBA-2"')
                sqllist.extend([
                    'GROUP BY c.utdate',
                    'ORDER BY c.utdate'])
                sqlcmd = '\n'.join(sqllist)
                retvals = db.read(sqlcmd)
                
            if retvals:
                for utd, in retvals:
                    utdate = str(utd)
                    cshpath = os.path.join(dirpath, 
                                           'thumbnail_' + utdate,
                                           '.csh')
                    thiscmd = cmd + ' --utdate=' + utdate
                    mygridengine.submit(thiscmd, cshpath, logpath)
        
    def convertThumbnails(self):
        """
        Read the CAOM-1 thumbnails from AD and reformat them with CAOM-2
        file names.
        """
        # ensure that the pngdir exists and is empty
        if os.path.exists(self.pngdir):
            shutil.rmtree(self.pngdir)
        os.mkdir(self.pngdir)
        
        with connection('SYBASE', 'jcmt', self.log) as db:
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
                sqllist.append('    o.observationID = "' + self.obs + '"')
            else:
                if self.utdate:
                    sqllist.append('    substring(a.uri, 14, 8) = "' + 
                                   self.utdate + '"')
                elif self.begin and self.end:
                    sqllist.extend(['    substring(a.uri, 14, 8) >= "' + 
                                   self.begin + '"',
                                   '    AND substring(a.uri, 14, 8) <= "' + 
                                   self.end + '"'])
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
                    
                    for size in ('256', '1024'):
                        if self.scuba2:
                            storage, archive, file_id = \
                                re.match(r'^(\w+):([A-Z]+)/([^\s]+)\s*$', 
                                         uridict[obs][prod]['reduced']).groups()
                            old_id = (file_id +  '_' + size)
                            old_png = reduced_id + '.png'
                            
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
                                shutil.copyfile(reduced_png, raw_png)
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

    def run(self):
        """
        Organize the conversion of thumbnails 
        """
        self.commandLineArguments()
        if self.qsub:
            self.submitToGridengine()
        else:
            self.convertThumbnails()
            if self.persist:
                os.chdir(self.pngdir)
                cmdlist = ['dpCapture', '-persist', '-archive=JCMT']
                try:
                    output = subprocess.check_output(
                                cmdlist,
                                shell=True,
                                stderr=subprocess.STDOUT)
                except subprocess.CalledProcessError as e:
                    output = e.output
                self.log.file(output)
                os.chdir('../')
                

