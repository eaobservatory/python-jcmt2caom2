#!/usr/bin/env python2.7

import argparse
from collections import OrderedDict
from ConfigParser import SafeConfigParser
import csv
from datetime import datetime
import logging
import os.path
import re
import sys

from tools4caom2.logger import logger
from tools4caom2.gridengine import gridengine

from tools4caom2.utdate_string import utdate_string

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version


def run():
    """
    The run() method for jcmt_prepare_files.  This is intended to be a template
    for a custom program to prepare externally generated data files for ingestion
    into the JSA by changing the name and decorating the file with a standard set 
    of FITS headers.
    """
    progname = os.path.basename(os.path.splitext(sys.path[0])[0])
    ap = argparse.ArgumentParser('jcmt_prepare_files')
    ap.add_argument('-c', '--csv',
                    required=True,
                    help='comma-separated value file listing files to edit and '
                         'headers to change')

    ap.add_argument('--log',
                    default='jcmt_prepare_files_' + utdate_string() + '.log',
                    help='(optional) name of log file')

    # verbosity
    ap.add_argument('--debug', '-d',
                    action='store_true',
                    help='run in debug mode')

    ap.add_argument('keyvalue',
                    nargs='*',
                    help='set of key=value pairs for default headers'

    a = ap.parse_args()

    # logging arguments
    if a.logdir:
        logdir = os.path.abspath(
                        os.path.expandvars(
                            os.path.expanduser(a.logdir)))
    else:
        self.logdir = os.getcwd()

    logfile = None
    if a.log:
        if os.path.dirname(a.log) == '':
            logfile = os.path.join(logdir, a.log)
        else:
            logfile = os.path.abspath(a.log)

    if not logfile:
        logfile = os.path.join(logdir,
                               progname + '_' + utdate_string() + '.log')

    loglevel = logging.INFO
    if a.debug:
        loglevel = logging.DEBUG

    with logger(logfile, loglevel) as log:
        log.file(progname)
        for attr in dir(switches):
            if attr != 'id' and attr[0] != '_':
                log.file('%-15s= %s' % 
                                 (attr, str(getattr(switches, attr))))
        log.file('logdir = ' + logdir)
        log.console('log = ' + logfile)

        if a.keyvalue:
            keydict = OrderedDict()
            for keyvalue in a.keyvalue:
                m = re.match(r'^([A-Z0-9]+)=(\W+)$', keyvalue)
                if m:
                    keydict[m.group(1)] = m.group(2)
                else:
                    log.console(keyvalue + ' does not match key=value and is '
                                'being ignored',
                                logging.WARN)

        if not os.path.isfile(a.csv):
            log.console(a.csv + 'is not a file', logging.ERROR)

        with open(a.csv, 'rb') as csvfile:
            sample = csvfile.read(1024)
            dialect = csv.Sniffer().sniff(sample)
            if not csv.Sniffer.has_header(sample):
                log.console('The first row of ' + a.csv +
                            ' must be a header row',
                            logging.ERROR)

            csvfile.seek(0)
            reader = csv.DictReader(csvfile, dialect)
            for csvdict in reader:
                if 'inputfile' not in csvdict:
                    log.console('"inputfile" must be a column in ' + a.csv,
                                logging.ERROR)
                if 'outputfile' not in csvdict:
                    log.console('"outputfile" must be a column in ' + a.csv,
                                logging.ERROR)

                if not os.path.isfile(csvdict['inputfile']):
                    log.console('The FITS file ' + csvdict['inputfile'] +
                                ' does not exist',
                                logging.ERROR)

                # open the FITS file and modify the headers
                hdulist = pyfits.open(csvdict['inputfile'])
                hdu = hdulist[0]

                # Fill default values here, or supply code to generate values

                # hdu.update['INSTREAM'] = 'JCMTLS' # or 'JCMTUSER'
                # hdu.update['ASN_ID'] = <observationID for observation>
                # hdu.update['ASN_TYPE'] = 'custom'
                # hdu.update['MBRCNT'] = 0 # number of membership URIs
                # hdu.update['MBR1'] = <membership URI 1>
                # hdu.update['OBS-TYPE'] = 'science'
                # hdu.update['PROJECT'] = <JCMT observing proposal_id>
                # hdu.update['PI'] = <JCMT observing proposal PI>
                # hdu.update['TITLE'] = <JCMT observing proposal title>
                # hdu.update['SURVEY'] = <JCMT Legacy Survey acronym>
                # hdu.update['DPPROJ'] = <data processing project>
                # hdu.update['INSTRUME'] = <full instrument name or frontend>
                # hdu.update['INBEAM'] = <optical components in the beam>
                # hdu.update['BACKEND'] = <backend>
                # hdu.update['SW_MODE'] = <switching mode>
                # hdu.update['SCAN_PAT'] = <scan pattern>
                # hdu.update['OBS_SB'] = <signal sideband>
                # hdu.update['SB_MODE'] = <instrument sideband mode>
                # hdu.update['TELESCOPE'] = 'JCMT'
                # hdu.update['OBSGEO_X'] = -5464588.652191697
                # hdu.update['OBSGEO_Y'] = -2493003.0215722183
                # hdu.update['OBSGEO_Z'] = 2150655.6609171447
                # hdu.update['OBJECT'] = <target name>
                # hdu.update['TARGTYPE'] = 'OBJECT' # or 'FIELD'
                # hdu.update['ZSOURCE'] = <redshift of target in BARYCENT frame>
                # hdu.update['OBSGEO_Z'] = 2150655.6609171447
                # hdu.update['TARGKEYW'] = <target keyword string>
                # hdu.update['MOVING'] = F # or T
                # hdu.update['OBSGEO_Z'] = 2150655.6609171447
                # hdu.update['OBSRA'] = <target RA in ICRS>
                # hdu.update['OBSDEC'] = <target Dec in ICRS>
                # hdu.update['RADESYS'] = <RA/Dec system>
                # hdu.update['EQUINOX'] = <equinox of coordinates>
                # hdu.update['PRODID'] = <productID for plane>
                # hdu.update['PRODUCT'] = <kind of science product in the plane>
                # hdu.update['FILTER'] = <continuum characteristic wavelength>
                # hdu.update['RESTFREQ'] = <heterodyne rest frequency>
                # hdu.update['BWMODE'] = <ACSIS/DAS bandwidth mode>
                # hdu.update['SUBSYSNR'] = <ACSIS/DAS subsystem number>
                # hdu.update['RECIPE'] = <name of data processing software>
                # hdu.update['PROCVERS'] = <data processing software version>
                # hdu.update['ENGVERS'] = <data processing engine version>
                # hdu.update['REFERENC'] = <URI of a data processing reference>
                # hdu.update['PRODUCER'] = <name of data processing person/team>
                # hdu.update['DPDATE'] = <nominal UTC for data processing>
                # hdu.update['INPCNT'] = 0 # number of provenance input URIs
                # hdu.update['INP1'] = <provenance input URI 1>

                # Override default values with thos supplied on the command line
                for key in keydict:
                    hdu.update(key, keydict[key])

                # Override these with values supplied in the csv file
                for key in csvdict:
                    if key not in ('inputfile', 'outputfile'):
                        if csvdict[key]:
                            hdu.update(key, csvdict[key])

                hdulist.writeto(csvdict['outputfile'])
