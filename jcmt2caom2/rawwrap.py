#!/usr/bin/env python2.7

import argparse
import commands
from ConfigParser import SafeConfigParser
import datetime
import logging
import os
import os.path
import re
import stat
import subprocess
import sys
import traceback

from tools4caom2.logger import logger
from tools4caom2.database import database
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
    if sys.path[0]:
        exedir = sys.path[0]
    else:
        exedir = os.path.expanduser('~/')
    obsid_regex = re.compile(r'^[|\s]*((acsis|scuba2|DAS|AOSC)_'
                             r'\d{1,5}_\d{8}[Tt]\d{6}).*$')
    utdate_str = utdate_string()
    
    ap = argparse.ArgumentParser('jcmtrawwrap')
    ap.add_argument('--log',
                    default='jcmtrawwrap_' + utdate_str + '.log',
                    help='(optional) name of log file')
    ap.add_argument('--logdir',
                    default='.',
                    help='(optional) directory to hold log files')

    ap.add_argument('--outdir',
                    default='.',
                    help='(optional) output directory for working files')
    
    ap.add_argument('--debug', '-d',
                    action='store_true',
                    help='run ingestion commands in debug mode')

    ap.add_argument('--test',
                    action='store_true',
                    help='do not run commnands')

    ap.add_argument('id',
                    nargs='*',
                    help='list of directories, obsid files, or '
                    'OBSID values')
    a = ap.parse_args()

        # Open log and record switches
    cwd = os.getcwd()
    
    logdir = os.path.abspath(
                os.path.expanduser(
                    os.path.expandvars(a.logdir)))
    
    outdir = os.path.abspath(
                os.path.expanduser(
                    os.path.expandvars(a.outdir)))
    
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
        log.file('tools4caom2version   = ' + tools4caom2version)
        log.file('jcmt2caom2version    = ' + jcmt2caom2version)
        log.file('exedir = ' + exedir)
        log.console('log = ' + logpath)
        for attr in dir(a):
            if attr != 'id' and attr[0] != '_':
                log.console('%-15s= %s' % (attr, getattr(a, attr)),
                            logging.DEBUG)
        
        # idset is the set of obsid's to ingest
        idset = []
        # obsidset is a set of abspaths to obsid files
        obsidset = set()
        if a.id:
            for id in a.id:
                # if id is a directory, add any obsid files in it to obsidset
                # This is NOT recursive.
                if os.path.isdir(id):
                    idpath = os.path.abspath(id)
                    for filename in os.listdir(idpath):
                        if os.path.splitext(filename)[1] == '.obsid':
                            obsidset.add(os.path.join(idpath, filename))
                elif os.path.isfile(id):
                    # if id is an obsid file add it to obsidset
                    if os.path.splitext(id)[1] == '.obsid':
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
                m = obsid_regex.match(line)
                if m:
                    id = m.group(1)
                    log.file('Add to idset: ' + id)
                    idset.add(id)
                    
        log.file('idset = ' + repr(idset))
        log.file('obsidset = ' + repr(obsidset))
        
        # Read any obsid files and add the contents to idset
        for obsidfile in sorted(list(obsidset), reverse=True):
            with open(obsidfile) as OF:
                for line in OF:
                    m = obsid_regex.match(line)
                    if m:
                        id = m.group(1)
                        log.file('from ' + obsidfile + ' add ' + id)
                        idset.add(id)
        
        idlist = sorted(idset,
                        key=obsid_regex.match().group(4),
                        reverse=True)
        
        retvals = None
        # ingest the recipe instances in subprocesses
        rawcmd = [os.path.join(sys.path[0], 'jcmt2caom2raw'),
                  ' --logdir=' + logdir,
                  ' --outdir=' + outdir]
        if a.debug:
            rawcmd.append(' --debug')

        for id in idlist:
            thisrawcmd = rawcmd
            thisrawcmd.append(' --key=' + id)
            log.console('PROGRESS: ' + ' '.join(thisrawcmd))
            
            if not a.test:
                try:
                    output = subprocess.check_output(
                                            thisrawcmd,
                                            stderr=subprocess.STDOUT)
                except KeyboardError:
                    # Exit immediately if there is a keyboard interrupt
                    sys.exit(1)
                    
                except subprocess.CalledProcessError as e:
                    # Log ingestion errors, but continue
                    try:
                        log.console(traceback.format_exc(),
                                    logging.ERROR)
                    except logger.LoggerError:
                        pass
                        
                finally:
                    # clean up
                    for filename in os.listdir(outdir):
                        filepath = os.path.join(outdir, filename)
                        basename, ext = os.path.splitext(filename)
                        if ext == '.xml':
                            log.console('/bin/rm ' + filepath, 
                                        logging.DEBUG)
                            os.remove(filepath)

        log.console('DONE')
