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

def put(archive, stream, filename, log):
    """
    Put a file into the archive
    """
    adPutCmd = 'adPut -a ' + archive + ' -as ' + stream + ' ' + filename
    log.console(adPutCmd)

#    try:
#        output = subprocess.check_output(
#                    adPutCmd,
#                    shell=True,
#                    stderr=subprocess.STDOUT)
#    except subprocess.CalledProcessError as e:
#        if re.search(r'already exists', e.output):
#            log.console('File already exists')
#        else:
#            raise
    
def run():
    """
    Convert thumbnail images from CAOM-1 to CAOM-2 format and naming 
    conventions.  The selection of observations to convert can be made by
    individual observation, for a single date, or for an inclusive range
    of dates.
    
    The wrapper jcmt1to2thumbnail is equivalent to
        python2.7 -m jcmt2caom2.thumbnail1to2 
        
    Examples:
    jcmt1to2thumbnails --obs=acsis_00037_20090327T062010
    jcmt1to2thumbnails --utdate=20120103
    jcmt1to2thumbnails --begin=20120101 --end=20120131
    """
    ap = argparse.ArgumentParser('thumbnail')
    ap.add_argument('--log',
                    default='thumbnail1to2.log',
                    help='(optional) name of log file')
    ap.add_argument('--debug', '-d', '-v',
                    default=False,
                    action='store_const',
                    const=True,
                    help='loglevel = debug')

    ap.add_argument('--scuba2',
                    action='store_true',
                    help='serch for SCUBA-2 thumbnails rather tan heterodyne')

    ap.add_argument('--obs',
                    help='(optional) convert thumbnails for one observation')
    ap.add_argument('--utdate',
                    help='(optional) date to convert thumbnails')
    ap.add_argument('--begin',
                    help='(optional) beginning date to convert thumbnails')
    ap.add_argument('--end',
                    help='(optional) ending date to convert thumbnails')

    ap.add_argument('--archive',
                    default='JCMT',
                    help='Archive within AD where thumbnail files are located')
    ap.add_argument('--stream',
                    default='product',
                    help='Stream within the archive to put new thumbnail files')
    
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
        
    mygridengine = gridengine(log)
    
    retvals = None
    
    if a.obs:
        log.console('observationID = ' + a.obs)
    elif a.utdate:
        log.console('UTDATE = ' + a.utdate)
    elif a.begin and a.end:
        log.console('UTDATE in [' + a.begin + ', ' + a.end + ']')
    else:
        log.console('Must set --obs or --utdate or both of --begin and --end',
                    logging.ERROR)

    if a.qsub:
        cmd = 'jcmt1to2thumbnail'
        if a.debug:
            cmd += ' --debug' 
        if a.scuba2:
            cmd += ' --scuba2'

        dirpath = os.path.dirname(
                    os.path.abspath(
                        os.path.expanduser(
                            os.path.expandvars(a.log))))

        if a.obs:
            logpath = os.path.join(dirpath, 
                                   'thumbnail_' + a.obs + '.log')
            cshpath = os.path.join(dirpath, 
                                   'thumbnail_' + a.obs + '.csh')

            cmd += ' --obs=' + a.obs
            cmd += ' --log=' + logpath
            mygridengine.submit(cmd, cshpath, logpath)
            
        elif a.utdate:
            logpath = os.path.join(dirpath, 
                                   'thumbnail_' + a.utdate + '.log')
            cshpath = os.path.join(dirpath, 
                                   'thumbnail_' + a.utdate + '.csh')

            cmd += ' --utdate=' + a.utdate
            cmd += ' --log=' + logpath
            mygridengine.submit(cmd, cshpath, logpath)
            
        elif a.begin and a.end:
            with connection('SYBASE', 'jcmtmd', log) as db:
                sqlcmd = '\n'.join([
                    'SELECT c.utdate',
                    'FROM jcmtmd.dbo.COMMON c',
                    'WHERE c.utdate >= %s AND c.utdate <= %s' % (a.begin, a.end),
                    'ORDER BY c.utdate'])
                retvals = db.read(sqlcmd)
                
            if retvals:
                for utd, in retvals:
                    utdate = str(utd)
                    logpath = os.path.join(dirpath, 
                                           'thumbnail_' + utdate + '.log')
                    cshpath = os.path.join(dirpath, 
                                           'thumbnail_' + utdate + '.csh')

                    thiscmd = cmd + ' --utdate=' + utdate
                    cmd += ' --log=' + logpath
                    mygridengine.submit(thiscmd, cshpath, logpath)

    else:
        with connection('SYBASE', 'jcmt', log) as db:
            sqllist = [
                'SELECT o.observationID,',
                '       o.algorithm_name,',
                '       p.productID,',
                '       p.provenance_inputs,',
                '       a.uri',
                'FROM jcmt.dbo.caom2_Observation o',
                '    INNER JOIN jcmt.dbo.caom2_Plane p on o.obsID=p.obsID',
                '    INNER JOIN jcmt.dbo.caom2_Artifact a ON p.planeID=a.planeID',
                'WHERE']
            
            if a.obs:
                sqllist.append('    o.observationID = "' + a.obs + '"')
            else:
                if a.utdate:
                    sqllist.append('    substring(a.uri, 14, 8) = "' + 
                                   a.utdate + '"')
                elif a.begin and a.end:
                    sqllist.extend(['    substring(a.uri, 14, 8) >= "' + 
                                   a.begin + '"',
                                   '    AND substring(a.uri, 14, 8) <= "' + 
                                   a.end + '"'])
            if a.scuba2:
                sqllist.append('    AND a.uri like "ad:JCMT/jcmts%reduced%"')
            else:
                sqllist.append('    AND a.productType = "preview"')
            
            sqllist.append('ORDER BY o.observationID, p.productID, a.uri')
            sqlcmd = '\n'.join(sqllist)
            
            retvals = db.read(sqlcmd)
            
            oldobs = None
            oldprod = None
            uridict = {}
            
            for obs, algorithm, prod, prov_inputs, uri in retvals:
                if obs not in uridict:
                    uridict[obs] = {}
                
                simple = (algorithm == 'exposure')
                
                if prod not in uridict[obs]:
                    uridict[obs][prod] = {}
                
                if simple:
                    # There should only be one input for a reduced plane in a
                    # single exposure observation, but in case there are
                    # more (bad hybrid processing) keep only the smallest
                    # matching a raw plane.
                    rawprod = 'zzzzzzzzz'
                    for in_uri in re.split(r'\s+', prov_inputs):
                        this_rawprod = re.split(r'/', in_uri)[2]
                        if re.match(r'^raw_\d+$', this_rawprod):
                            if this_rawprod < rawprod:
                                rawprod = this_rawprod
                    if re.match(r'^raw_\d+$', rawprod):
                        uridict[obs][prod]['rawprod'] = rawprod
                
                if a.scuba2:
                    if not re.search(r'reduced\d{3}', uri):
                        log.console('SCUBA-2 file_id is not reduced',
                                    logging.WARN)
                        continue
                    uridict[obs][prod]['reduced'] = \
                        re.sub(r'(.*?reduced)\d{3}(.*)', r'\1\2', uri)
                else:
                    if re.search(r'rsp', uri):
                        uridict[obs][prod]['rsp'] = uri
                    
                    elif re.search(r'rimg', uri):
                        uridict[obs][prod]['rimg'] = uri
            
            log.file('uridict:', logging.DEBUG)
            for obs in uridict:
                log.file(obs, logging.DEBUG)
                for prod in uridict[obs]:
                    log.file('    ' + prod, logging.DEBUG)
                    for key in uridict[obs][prod]:
                        log.file('        ' + key + ': ' + 
                                 uridict[obs][prod][key],
                                 logging.DEBUG)
            
            adGet = 'adGet -a'
            for obs in uridict:
                for prod in uridict[obs]:
                    if not a.scuba2 and 'rsp' not in uridict[obs][prod]:
                        log.console('skipping caom:' + obs + '/' + prod +
                                    ' because no rsp image is available',
                                    logging.WARN)
                        continue
                    
                    for size in ('64', '256', '1024'):
                        if a.scuba2:
                            storage, archive, file_id = \
                                re.match(r'^(\w+):([A-Z]+)/([^\s]+)\s*$', 
                                         uridict[obs][prod]['reduced']).groups()
                            reduced_id = (file_id +  '_' + size)
                            reduced_png = reduced_id + '.png'
                            
                            adGetCmd = ' '.join([adGet, archive, reduced_id])
                            try:
                                output = subprocess.check_output(
                                            adGetCmd,
                                            shell=True,
                                            stderr=subprocess.STDOUT)
                                log.file(output)
                            except subprocess.CalledProcessError as e:
                                log.console('Could not get ' + reduced_id,
                                            logging.WARN)
                                continue
                            
                            prod_thumb = thumbnail_name('JCMT',
                                                        obs,
                                                        prod,
                                                        size)
                            shutil.copyfile(reduced_png, prod_thumb)
                            put(a.archive, a.stream, prod_thumb, log)
                            
                            if 'rawprod' in uridict[obs][prod]:
                                rawprod = uridict[obs][prod]['rawprod']
                                raw_thumb = thumbnail_name('JCMT',
                                                           obs,
                                                           rawprod,
                                                           size)
                                shutil.copyfile(reduced_png, raw_thumb)
                                put(a.archive, a.stream, raw_thumb, log)
                            
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
                                log.file(output)
                            except subprocess.CalledProcessError as e:
                                log.console('Could not get ' + rsp_id,
                                            logging.WARN)
                                continue
                            rsp_thumb = Image.open(rsp_png)

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
                                    log.file(output)
                                except subprocess.CalledProcessError as e:
                                    log.console('Could not get ' + rimg_id,
                                                logging.WARN)
                                    continue

                                rimg_thumb = Image.open(rimg_png)
                                thumb.paste(rimg_thumb, (nsize, 0))
                            
                            reduced_png = thumbnail_name('JCMT',
                                                         obs,
                                                         prod,
                                                         size)
                            thumb.save(reduced_png)
                            put(a.archive, a.stream, reduced_png, log)

                            if 'rawprod' in uridict[obs][prod]:
                                rawprod = uridict[obs][prod]['rawprod']
                                raw_png = thumbnail_name('JCMT',
                                                         obs,
                                                         rawprod,
                                                         size)
                                shutil.copyfile(reduced_png, raw_png)
                                put(a.archive, a.stream, raw_png, log)
                    
                                cubeprod = re.sub(r'^reduced_(\d+)$', 
                                                  r'cube_\1', 
                                                  prod)
                                cube_png = thumbnail_name('JCMT',
                                                           obs,
                                                           cubeprod,
                                                           size)
                                shutil.copyfile(reduced_png, cube_png)
                                put(a.archive, a.stream, cube_png, log)

                    
                        

