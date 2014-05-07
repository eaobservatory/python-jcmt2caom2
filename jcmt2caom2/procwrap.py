#!/usr/bin/env python2.7

import argparse
import commands
import datetime
import logging
import os.path
import re
import stat
import string
import subprocess

from tools4caom2.config import config
from tools4caom2.logger import logger
from tools4caom2.database import database
from tools4caom2.database import connection
from tools4caom2.gridengine import gridengine

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version

        
def run():
    """
    The run() method for jcmtprocwrap.

    
    A range of id's can be entered one at a time, or as a range first-last, 
    where first defaults to 10894 and last defaults to the current maximum 
    identity_instance_id. Thus, the default range if nothing is specified 
    is 10895-, implying all currently valid recipe instances.

    Examples:
    jcmtprocwrap --debug 10895-10900
    jcmtprocwrap \
        12707 12744 12784 12795 12817 14782 14784 30550 30594 \
        30605 30736 30739 30745 30748 30863 31192 32084 32971
    jcmtprocwrap --qsub --algorithm=project
    jcmtprocwrap --qsub --backend=SCUBA-2 --existing
    """
    userconfig = None
    userconfigpath = '~/.tools4caom2/jcmt2caom2.config'

    ap = argparse.ArgumentParser('jcmtprocwrap')
    ap.add_argument('--userconfig',
                    default=userconfigpath,
                    help='Optional user configuration file '
                    '(default=' + userconfigpath + ')')
    
    ap.add_argument('--log',
                    default='jcmtprocwrap.log',
                    help='(optional) name of log file')
    ap.add_argument('--logdir',
                    help='(optional) directory to hold log and xml files')
    ap.add_argument('--keeplog',
                    action='store_true',
                    help='Pass --keeplog switch to jcmt2caom2proc')
    ap.add_argument('--sharelog',
                    action='store_true',
                    help='Pass --sharelog switch to jcmt2caom2proc')
    
    ap.add_argument('--outdir',
                    help='(optional) output directory for working files')
    
    ap.add_argument('--qsub',
                    action='store_true',
                    help='rsubmit ingestion jobs to gridengine')
    ap.add_argument('--queue',
                    default='cadcproc',
                    help='gridengine queue to use if --qsub is set')
    ap.add_argument('--big',
                    action='store_true',
                    help='(optional) request extra heap space and RAM')
    ap.add_argument('--test',
                    action='store_true',
                    help='do not submit to gridengine or run commnands')
    # verbosity
    ap.add_argument('--debug', '-d',
                    action='store_true',
                    help='run ingestion commands in debug mode')

    ap.add_argument('--collection',
                    choices=['JCMT', 'JCMTLS', 'JCMTUSER', 'SANDBOX'],
                    help='destination collection')
    
    ap.add_argument('id',
                    nargs='*',
                    help='list of directories, rcinst files, or '
                    'identity_instance_id values')
    a = ap.parse_args()
    
    userconfig = config(a.userconfig)
    userconfig['server'] = 'SYBASE'
    userconfig['caom_db'] = 'jcmt'
    userconfig.read()
    
    if a.outdir and os.path.isdir(a.outdir):
        os.chdir(a.outdir)
    
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

    log = logger(logpath, loglevel, True)
    log.file('jcmt2caom2version    = ' + jcmt2caom2version)
    log.file('tools4caom2version   = ' + tools4caom2version)
    for attr in dir(a):
        if attr != 'id' and attr[0] != '_':
            log.console('%-15s= %s' % (attr, getattr(a, attr)))
    log.console('id = ' + repr(a.id))

    if a.big:
        mygridengine = gridengine(log, 
                                  queue=a.queue,
                                  options='-cwd -j yes -l cmem=32')
    else:
        mygridengine = gridengine(log, queue=a.queue)
    
    # idset is the set of recipe instances to ingest
    idset = set()
    # rcinstset is a set of abspaths to rcinst files
    rcinstset = set()
    for id in a.id:
        # if id is a directory, add any rcinst files it contains to rcinstset
        if os.path.isdir(id):
            idpath = os.path.abspath(id)
            for filename in os.listdir(idpath):
                basename, ext = os.path.splitext(filename)
                if ext == '.rcinst':
                    rcinstset.add(os.path.join(idpath, filename))
        elif os.path.exists(id):
            # if id is an rcinst file add it to rcinstset
            basename, ext = os.path.splitext(id)
            if ext == '.rcinst':
                rcinstset.add(os.path.abspath(id))
        elif re.match(r'^\d+$', id):
            # if id is an identity_instance_id string, add it to idset
            idset.add(id)
        else:
            log.console(id + ' is not a directory, and rcinst file, nor an '
                        'identity_instance_id value',
                        logging.WARN)
    
    log.file('idset = ' + repr(idset))
    log.file('rcinstset = ' + repr(rcinstset))
    
    if a.qsub:
        proccmd = 'jcmtprocwrap'
        proccmd += ' --outdir=${TMPDIR}'
        if a.collection:
            proccmd += ' --collection=' + a.collection
        if a.big:
            proccmd += ' --big'
        if a.debug:
            proccmd += ' --debug'
        if a.keeplog or a.sharelog:
            proccmd += ' --keeplog'
            
        # submit rcinst sets to gridengine
        # compose the jcmtprocwrap command
        for rcinstfile in sorted(list(rcinstset)):
            cmd = proccmd
            rcinstbase = os.path.basename(rcinstfile)
            rcinstlog = os.path.join(logdir, rcinstbase + '.log')
            rcinstcsh = os.path.join(logdir, rcinstbase + '.csh')
            
            if a.sharelog:
                cmd += ' --sharelog'
            
            rcinstlogs = os.path.join(logdir, rcinstbase + '.logs')
            if not os.path.isdir(rcinstlogs):
                os.makedirs(rcinstlogs)
            else:
                # make sure rcinstlogs is empty
                for f in os.listdir(rcinstlogs):
                    os.remove(os.path.join(rcinstlogs, f))
            
            cmd += ' --log=' + rcinstlog
            cmd += ' --logdir=' + rcinstlogs
            cmd += ' ' + rcinstfile
            
            log.console('SUBMIT: ' + cmd)
            if not a.test:
                mygridengine.submit(cmd, rcinstcsh, rcinstlog)
        
        # If any rcinst values were specified in the command line, 
        # submit them as well
        if idset:
            idlist = [proccmd]
            idlist.extend(sorted(list(idset), key=int, reverse=True))
            cmd = ' '.join(idlist)

            rcinstcsh = os.path.join(logdir, 'rcinst_list.csh')
            rcinstlog = os.path.join(logdir, 'rcinst_list.log')
            
            log.console('SUBMIT: ' + cmd)
            if not a.test:
                mygridengine.submit(cmd, rcinstcsh, rcinstlog)
            
    else:
        # ingest the recipe instances in subprocesses
        for rcinstfile in list(rcinstset):
            with open(rcinstfile) as RCF:
                for line in RCF:
                    m = re.match(r'^\s*(\d+)([^\d].*)?$', line)
                    if m:
                        thisid = m.group(1)
                        log.console('found ' + thisid,
                                    logging.DEBUG)
                        idset.add(thisid)

        # Handle one rcinst file at a time and adjust the logdir
        idlist = []
        if idset:
            idlist = sorted(list(idset), key=int, reverse=True)
        
        # process one recipe instance at a time
        proccmd = 'jcmt2caom2proc'
        if a.collection:
            proccmd += ' --collection=' + a.collection
        if a.big:
            proccmd += ' --big'
        if a.debug:
            proccmd += ' --debug'
        if a.keeplog or a.sharelog:
            proccmd += ' --keeplog'
        if a.sharelog:
            proccmd += ' --log=' + logpath
        else:
            proccmd += ' --logdir=' + logdir
            
        for rcinst in idlist:
            thisproccmd = proccmd

            thisproccmd += (' dp:' + rcinst)
        
            log.console('PROGRESS: ' + thisproccmd)
            
            if not a.test:
                try:
                    output = subprocess.check_output(
                                                thisproccmd,
                                                shell=True,
                                                stderr=subprocess.STDOUT)
                except subprocess.CalledProcessError as e:
                    log.console('FAILED: ' + rcinst,
                                logging.WARN)
                    log.file('status = ' + str(e.returncode) + 
                             ' output = \n' + e.output)
                
                # clean up
                for filename in os.listdir(cwd):
                    filepath = os.path.join(cwd, filename)
                    basename, ext = os.path.splitext(filename)
                    if ext in ['.fits', '.xml', '.override']:
                        os.remove(filepath)
                                
                for filename in os.listdir(rcinstpath):
                    filepath = os.path.join(rcinstpath, filename)
                    basename, ext = os.path.splitext(filename)
                    if ext == '.log':
                        gzipcmd = 'gzip ' + filepath
                        output = subprocess.check_output(
                                            gzipcmd,
                                            shell=True,
                                            stderr=subprocess.STDOUT)
