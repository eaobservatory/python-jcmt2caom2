#!/usr/bin/env python2.7

import argparse
import commands
from ConfigParser import SafeConfigParser
import datetime
import logging
import os.path
import re
import stat
import subprocess
import sys

from tools4caom2.logger import logger
from tools4caom2.database import database
from tools4caom2.gridengine import gridengine
from tools4caom2.utdate_string import utdate_string

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version


def run():
    """
    Ingest raw JCMT observation from a range of UTDATE's.
    This is just a high-level script to run jcmt2caom2raw many times.  
    
    Examples:
    rawutdate --debug --start=20100123 --end=20100131
    """
    obsid_regex = re.compile('^\s*((acsis|scuba2|DAS|AOSC)_\d{1,5}_\d{8}T\d{6})')
    utdate_str = utdate_string()
    
    ap = argparse.ArgumentParser('jcmtrawwrap')
    ap.add_argument('--log',
                    default='jcmtrawwrap_' + utdate_str + '.log',
                    help='(optional) name of log file')
    ap.add_argument('--logdir',
                    help='(optional) directory to hold log files')

    ap.add_argument('--outdir',
                    help='(optional) output directory for working files')
    
    ap.add_argument('--debug', '-d',
                    action='store_true',
                    help='run ingestion commands in debug mode')
    ap.add_argument('--qsub',
                    action='store_const',
                    default=False,
                    const=True,
                    help='submit ingestion jobs to gridengine')
    ap.add_argument('--qsubrequirements',
                    help='(optional) requirements to pass to gridengine')
    ap.add_argument('--queue',
                    default='cadcproc',
                    help='gridengine queue to use if --qsub is set')

    ap.add_argument('--test',
                    action='store_true',
                    help='do not submit to gridengine or run commnands')

    ap.add_argument('id',
                    nargs='*',
                    help='list of directories, obsid files, or '
                    'OBSID values')
    a = ap.parse_args()

        # Open log and record switches
    cwd = os.path.abspath(
                os.path.expanduser(
                    os.path.expandvars('.')))
    
    if a.logdir:
        logdir = os.path.abspath(
                os.path.expanduser(
                    os.path.expandvars(a.logdir)))
    else:
        logdir = cwd
    
    loglevel = logging.INFO
    if a.debug:
        loglevel = logging.DEBUG
    
    if os.path.dirname(a.log):
        logpath = os.path.abspath(
                    os.path.expanduser(
                        os.path.expandvars(a.log)))
    else:
        logpath = os.path.join(logdir, a.log)

    with logger(logpath, loglevel).record() as log:
        log.file(sys.argv[0])
        log.file('tools4caom2version   = ' + tools4caom2version)
        log.file('jcmt2caom2version    = ' + jcmt2caom2version)
        log.console('log = ' + logpath)
        for attr in dir(a):
            if attr != 'id' and attr[0] != '_':
                log.console('%-15s= %s' % (attr, getattr(a, attr)),
                            logging.DEBUG)
        
        if a.qsubrequirements:
            mygridengine = gridengine(log, 
                                      queue=a.queue,
                                      options=a.qsubrequirements)
        else:
            mygridengine = gridengine(log, queue=a.queue)
         
        # idset is the set of recipe instances to ingest
        idset = []
        # obsidset is a set of abspaths to obsid files
        obsidset = set()
        if a.id:
            for id in a.id:
                # if id is a directory, add any obsid files in it to obsidset
                if os.path.isdir(id):
                    idpath = os.path.abspath(id)
                    for filename in os.listdir(idpath):
                        basename, ext = os.path.splitext(filename)
                        if ext == '.obsid':
                            obsidset.add(os.path.join(idpath, filename))
                elif os.path.exists(id):
                    # if id is an obsid file add it to obsidset
                    basename, ext = os.path.splitext(id)
                    if ext == '.obsid':
                        obsidset.add(os.path.abspath(id))
                elif obsid_regex.search(id):
                    # if id is an observationID string, add it to idset
                    m = obsid_regex.search(id)
                    idset.add(id.group[1])
                else:
                    log.console(id + ' is not a directory, an obsid file, '
                                'nor an OBSID value',
                                logging.WARN)
        else:
            # Try to read a list of obsids from stdin
            for line in sys.stdin:
                # if the line starts with an obsid string, 
                # add it to idset
                m = obsid_regex.search(line)
                if m:
                    id = m.group(1)
                    if id not in idset:
                        log.file('Add to idset: ' + id)
                        idset.append(id)
        
        log.file('idset = ' + repr(idset))
        log.file('obsidset = ' + repr(obsidset))
        
        retvals = None
        if a.qsub:
            rawcmd = os.path.join(sys.path[0], 'jcmtrawwrap')
            rawcmd += ' --outdir=${TMPDIR}'
            if a.debug:
                rawcmd += ' --debug'
                
            # submit obsid sets to gridengine
            # compose the jcmtrawwrap command
            for obsidfile in sorted(list(obsidset), reverse=True):
                cmd = rawcmd
                obsidbase = '_'.join([
                                os.path.splitext(
                                    os.path.basename(obsidfile))[0],
                                utdate_str])
                obsidlog = os.path.join(logdir, obsidbase + '.log')
                obsidcsh = os.path.join(logdir, obsidbase + '.csh')
                
                obsidlogs = os.path.join(logdir, obsidbase + '_logs')
                if not os.path.isdir(obsidlogs):
                    os.makedirs(obsidlogs)
                    
                cmd += ' --log=' + obsidlog
                cmd += ' --logdir=' + obsidlogs
                cmd += ' ' + obsidfile
                
                log.console('SUBMIT: ' + cmd)
                if not a.test:
                    mygridengine.submit(cmd, obsidcsh, obsidlog)
            
            # If any obsid values were specified in the command line, 
            # submit them as well
            if idset:
                idlist = [rawcmd]
                idlist.extend(idset)
                cmd = ' '.join(idlist)

                obsidcsh = os.path.join(logdir, 'obsid_list.csh')
                obsidlog = os.path.join(logdir, 'obsid_list.log')
                
                log.console('SUBMIT: ' + cmd)
                if not a.test:
                    mygridengine.submit(cmd, obsidcsh, obsidlog)

        else:
            # ingest the recipe instances in subprocesses
            rawpath = os.path.join(sys.path[0], 'jcmt2caom2raw')
            rawcmd = 'jcmt2Caom2DA --full'
            if a.debug:
                rawcmd += ' --debug'

            # jcmt2Caom2DA does not pass --log or --logdir to 
            # jcmt2caom2raw, so it is necessary for logdir to be the
            # current directory
            os.chdir(logdir)

            idlist = []
            if idset:
                idlist.append(sorted(idset, reverse=True))
            
            for obsidfile in sorted(list(obsidset), reverse=True):
                newlist = []
                with open(obsidfile) as OF:
                    for line in OF:
                        m = obsid_regex.search(line)
                        if m:
                            id = m.group(1)
                            if id not in newlist:
                                log.console('found ' + id,
                                            logging.DEBUG)
                                newlist.append(id)
                idlist.append(sorted(newlist, reverse=True))

            for ids in idlist:
                for obsid in ids:
                    thisrawcmd = rawcmd

                    # jcmt2Caom2DA only logs errors and does not share a log 
                    # with jcmt2caom2raw, so it can share the current log
                    thisrawcmd += (' --log=' + logpath)
                    thisrawcmd += (' --start=' + obsid)
                    thisrawcmd += (' --end=' + obsid)
                    thisrawcmd += ' --mode=raw'
                    thisrawcmd += (' --script=' + rawpath)
                
                    log.console('PROGRESS: ' + thisrawcmd)
                    
                    if not a.test:
                        try:
                            output = subprocess.check_output(
                                                        thisrawcmd,
                                                        shell=True,
                                                        stderr=subprocess.STDOUT)
                        except subprocess.CalledProcessError as e:
                            log.console('FAILED: ' + obsid,
                                        logging.WARN)
                            log.file('status = ' + str(e.returncode) + 
                                     ' output = \n' + e.output)
                        
                        # clean up
                        for filename in os.listdir(cwd):
                            filepath = os.path.join(cwd, filename)
                            basename, ext = os.path.splitext(filename)
                            if ext == '.xml':
                                log.console('rm ' + filepath, logging.DEBUG)
                                os.remove(filepath)

        log.console('DONE')
