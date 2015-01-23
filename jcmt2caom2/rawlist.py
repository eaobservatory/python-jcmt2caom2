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
    Ingest raw JCMT observations from one or more obsid lists.
    This is just a mid-level script to run jsaraw many times.

    Examples:
    jsarawlist scuba2_00021_20141116T072609 scuba2_00020_20141116T072123
    jsarawlist reingest_list.obsid
    """
    if sys.path[0]:
        exedir = sys.path[0]
    else:
        exedir = os.path.expanduser('~/')
    obsid_regex = re.compile(r'^[|,\s]*((acsis|scuba2|DAS|AOSC)_' +
                             r'\d{1,5}_(\d{8}[Tt]\d{6}))([|,\s].*)?$')
    utdate_str = utdate_string()
    userconfigpath = '~/.tools4caom2/tools4caom2.config'

    ap = argparse.ArgumentParser('jsarawlist')
    ap.add_argument('--userconfig',
                    default=userconfigpath,
                    help='path to user configuration file')

    ap.add_argument('--log',
                    default='jsarawlist_' + utdate_str + '.log',
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

    ap.add_argument('--reverse',
                    action='store_true',
                    help='ingest in the reverse order of date_obs')

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

    # Not used inside jsarawlist, but passed to jsaraw
    userconfigpath = os.path.abspath(
        os.path.expanduser(
            os.path.expandvars(a.userconfig)))

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

        # obsid_set is the set of obsid's to ingest
        obsid_set = set()
        # obsid_file_set is a set of abspaths to obsid files
        obsid_file_set = set()
        if a.id:
            for id in a.id:
                # if id is a directory, add obsid files in it to obsid_file_set
                # This is NOT recursive.
                if os.path.isdir(id):
                    idpath = os.path.abspath(id)
                    for filename in os.listdir(idpath):
                        filepath = os.path.join(idpath, filename)
                        if (os.path.isfile(filepath) and
                                os.path.splitext(filename)[1] == '.obsid'):

                            obsid_file_set.add(filepath)
                elif os.path.isfile(id):
                    # if id is an obsid file add it to obsid_file_set
                    if os.path.splitext(id)[1] == '.obsid':
                        obsid_file_set.add(os.path.abspath(id))

                elif obsid_regex.search(id):
                    # if id is an observationID string, add it to obsid_set
                    m = obsid_regex.search(id)
                    if m:
                        id = m.group(1)
                        log.file('Add to obsid_set: ' + id,
                                 logging.DEBUG)
                        obsid_set.add(id)
                else:
                    log.console(id + ' is not a directory, an obsid file, '
                                'nor an OBSID value',
                                logging.WARN)
        else:
            # Try to read a list of obsids from stdin
            for line in sys.stdin:
                # if the line starts with an obsid string,
                # add it to obsid_set
                m = obsid_regex.match(line)
                if m:
                    obsid = m.group(1)
                    log.file('Add to obsid_set: ' + obsid,
                             logging.DEBUG)
                    obsid_set.add(obsid)

        # Read any obsid files and add the contents to obsid_set
        for obsidfile in sorted(list(obsid_file_set)):
            with open(obsidfile) as OF:
                for line in OF:
                    m = obsid_regex.match(line)
                    if m:
                        obsid = m.group(1)
                        log.file('from ' + obsidfile + ' add ' + obsid)
                        obsid_set.add(obsid)

        if obsid_set:
            obsid_list = sorted(obsid_set,
                                key=lambda t: obsid_regex.match(t).group(3),
                                reverse=a.reverse)
        else:
            log.console('no obsid values have been input',
                        logging.ERROR)

        retvals = None
        # ingest the recipe instances in subprocesses
        rawcmd = [os.path.join(sys.path[0], 'jsaraw'),
                  '--logdir=' + logdir,
                  '--outdir=' + outdir,
                  '--userconfig=' + userconfigpath]
        if a.debug:
            rawcmd.append('--debug')

        for obsid in obsid_list:
            thisrawcmd = []
            thisrawcmd.extend(rawcmd)
            thisrawcmd.append('--key=' + obsid)
            log.console('PROGRESS: ' + obsid)
            log.file(' '.join(thisrawcmd))

            if not a.test:
                try:
                    output = subprocess.check_output(
                        thisrawcmd,
                        stderr=subprocess.STDOUT)
                except KeyboardInterrupt:
                    # Exit immediately if there is a keyboard interrupt
                    sys.exit(1)

                except subprocess.CalledProcessError as e:
                    # Log ingestion errors, but continue
                    try:
                        log.console(traceback.format_exc(),
                                    logging.ERROR)
                    except logger.LoggerError:
                        pass

                except:
                    # other errors will be logged, but with an error
                    log.console(traceback.format_exec(),
                                logging.ERROR)

        log.console('DONE')

        # clean up outdir
        if not a.debug:
            for filename in os.listdir(outdir):
                filepath = os.path.join(outdir, filename)
                basename, ext = os.path.splitext(filename)
                if (ext == '.xml'):
                    log.console('remove ' + filepath,
                                logging.DEBUG)
                    os.remove(filepath)