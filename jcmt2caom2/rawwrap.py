#!/usr/bin/env python2.7

import argparse
import commands
import datetime
import logging
import os.path
import re
import stat
import sys

from tools4caom2.config import config
from tools4caom2.logger import logger
from tools4caom2.database import database
from tools4caom2.database import connection
from tools4caom2.gridengine import gridengine

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version


def run():
    """
    Ingest raw JCMT observation from a range of UTDATE's.
    This is just a high-level script to run jcmt2caom2raw many times.  
    
    Examples:
    rawutdate --debug --start=20100123 --end=20100131
    """
    userconfigpath = '~/.tools4caom2/jcmt2caom2.config'
    obsid_regex = re.compile('^\s*((acsis|scuba2|DAS|AOSC)_\d{5}_\d{8}T\d{6})')
    
    ap = argparse.ArgumentParser('jcmtrawwrap')
    ap.add_argument('--userconfig',
                    default=userconfigpath,
                    help='Optional user configuration file '
                    '(default=' + userconfigpath + ')')

    ap.add_argument('--log',
                    default='jcmtrawwrap.log',
                    help='(optional) name of log file')
    ap.add_argument('--logdir',
                    help='(optional) directory to hold log files')
    ap.add_argument('--sharelog',
                    action='store_true',
                    help='share the same log file for all ingestions')

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
                    help='list of directories, rcinst files, or '
                    'OBSID values')
    a = ap.parse_args()

    userconfig = config(a.userconfig)
    userconfig['server'] = 'SYBASE'
    userconfig['caom_db'] = 'jcmt'
    userconfig.read()
        
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

    log = logger(logpath, loglevel)
    log.file('jcmt2caom2version    = ' + jcmt2caom2version)
    log.file('tools4caom2version   = ' + tools4caom2version)
    for attr in dir(a):
        if attr != 'id' and attr[0] != '_':
            log.console('%-15s= %s' % (attr, getattr(a, attr)))
    log.console('id = ' + repr(a.id))
    
    if a.qsubrequirements:
        mygridengine = gridengine(log, 
                                  queue=a.queue,
                                  options=a.qsubrequirements)
    else:
        mygridengine = gridengine(log, queue=a.queue)
     
    # idset is the set of recipe instances to ingest
    idset = set()
    # obsidset is a set of abspaths to obsid files
    obsidset = set()
    for id in a.id:
        # if id is a directory, add any obsid files it contains to obsidset
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
            log.console(id + ' is not a directory, and obsid file, nor an '
                        'OBSID value',
                        logging.WARN)
    
    log.file('idset = ' + repr(idset))
    log.file('obsidset = ' + repr(obsidset))
    
    retvals = None
    if a.qsub:
        rawcmd = 'jcmtrawwrap'
        rawcmd += ' --outdir=${TMPDIR}'
        if a.debug:
            rawcmd += ' --debug'
        if a.keeplog or a.sharelog:
            rawcmd += ' --keeplog'
            
        # submit obsid sets to gridengine
        # compose the jcmtrawwrap command
        for obsidfile in sorted(list(obsidset)):
            cmd = rawcmd
            obsidbase = os.path.basename(obsidfile)
            obsidlog =  os.path.join(logdir, obsidbase + '.log')
            obsidcsh = os.path.join(logdir, 'csh_' + obsidbase + '.csh')
            
            if a.sharelog:
                cmd += ' --sharelog'
        
            obsidlogs = os.path.join(logdir, obsidbase + '.logs')
            if not os.path.isdir(obsidlogs):
                os.makedirs(obsidlogs)
            else:
                # make sure obsidlogs is empty
                for f in os.listdir(obsidlogs):
                    os.remove(os.path.join(obsidlogs, f))
                
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
            idlist.extend(sorted(list(idset), key=int, reverse=True))
            cmd = ' '.join(idlist)

            obsidcsh = os.path.join(logdir, 'obsid_list.csh')
            obsidlog = os.path.join(logdir, 'obsid_list.log')
            
            log.console('SUBMIT: ' + cmd)
            if not a.test:
                mygridengine.submit(cmd, obsidcsh, obsidlog)

    else:
        # ingest the recipe instances in subprocesses
        for obsidfile in list(obsidset):
            with open(obsidfile) as OF:
                for line in OF:
                    m = obsid_regex.search(line)
                    if m:
                        thisid = m.group(1)
                        log.console('found ' + thisid,
                                    logging.DEBUG)
                        idset.add(thisid)

        rawcmd = 'jcmt2Caom2DA --full'
        if a.debug:
            rawcmd += ' --debug'

        for obsid in idlist:
            thisrawcmd = rawcmd

            if a.sharelog:
                thisrawcmd += ' --log=' + logpath
            else:
                thislog = os.path.join(logdir,
                                       'raw_' + obsid + '.log')
                thisrawcmd += ' --log=' + thislog
            
            thisrawcmd += (' --begin=' + obsid)
            thisrawcmd += (' --end=' + obsid)
        
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
                        os.remove(filepath)
                                
                gzipcmd = 'gzip ' + thislog
                output = subprocess.check_output(
                                    gzipcmd,
                                    shell=True,
                                    stderr=subprocess.STDOUT)
