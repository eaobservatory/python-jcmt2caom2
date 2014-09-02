#!/usr/bin/env python2.7

import argparse
from collections import OrderedDict
from ConfigParser import SafeConfigParser
import csv
from datetime import datetime
import logging
import pyfits
import os.path
import re
import shutil
import sys

from tools4caom2.logger import logger
from tools4caom2.gridengine import gridengine

from tools4caom2.utdate_string import utdate_string

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version


def rewrite_fits(infits, outfits, headerdict):
    """
    Customize this routine to suit the needs of your data.  
    """
    hdulist = pyfits.open(infits)
    hdu = hdulist[0].header

    # Uncomment lines below as required and supply an algorithmic
    # value for the specified header
    
    # headerdict['INSTREAM'] = 'JCMTLS' # or 'JCMTUSER'
    # headerdict['ASN_ID'] = <observationID for observation>
    # headerdict['ASN_TYPE'] = 'custom'
    # headerdict['MBRCNT'] = 0 # number of membership URIs
    # headerdict['MBR1'] = <membership URI 1>
    # headerdict['OBS-TYPE'] = 'science'
    # headerdict['PROJECT'] = <JCMT observing proposal_id>
    # headerdict['PI'] = <JCMT observing proposal PI>
    # headerdict['TITLE'] = <JCMT observing proposal title>
    # headerdict['SURVEY'] = <JCMT Legacy Survey acronym>
    # headerdict['DPPROJ'] = <data processing project>
    # headerdict['INSTRUME'] = <full instrument name or frontend>
    # headerdict['INBEAM'] = <optical components in the beam>
    # headerdict['BACKEND'] = <backend>
    # headerdict['SW_MODE'] = <switching mode>
    # headerdict['SCAN_PAT'] = <scan pattern>
    # headerdict['OBS_SB'] = <signal sideband>
    # headerdict['SB_MODE'] = <instrument sideband mode>
    # headerdict['TELESCOPE'] = 'JCMT'
    # headerdict['OBSGEO_X'] = -5464588.652191697
    # headerdict['OBSGEO_Y'] = -2493003.0215722183
    # headerdict['OBSGEO_Z'] = 2150655.6609171447
    # headerdict['OBJECT'] = <target name>
    # headerdict['TARGTYPE'] = 'OBJECT' # or 'FIELD'
    # headerdict['ZSOURCE'] = <redshift in BARYCENT frame>
    # headerdict['OBSGEO_Z'] = 2150655.6609171447
    # headerdict['TARGKEYW'] = <target keyword string>
    # headerdict['MOVING'] = F # or T
    # headerdict['OBSGEO_Z'] = 2150655.6609171447
    # headerdict['OBSRA'] = <target RA in ICRS>
    # headerdict['OBSDEC'] = <target Dec in ICRS>
    # headerdict['RADESYS'] = <RA/Dec system>
    # headerdict['EQUINOX'] = <equinox of coordinates>
    # headerdict['PRODID'] = <productID for plane>
    # headerdict['PRODUCT'] = <kind of product in the file>
    # headerdict['FILTER'] = <characteristic wavelength>
    # headerdict['RESTFREQ'] = <heterodyne rest frequency>
    # headerdict['BWMODE'] = <ACSIS/DAS bandwidth mode>
    # headerdict['SUBSYSNR'] = <ACSIS/DAS subsystem number>
    # headerdict['RECIPE'] = <name of data processing software>
    # headerdict['PROCVERS'] = <data processing software version>
    # headerdict['ENGVERS'] = <data processing engine version>
    # headerdict['PRODUCER'] = <name of processing person/team>
    # headerdict['DPDATE'] = <nominal UTC for data processing>
    # headerdict['INPCNT'] = 0 # number of provenance input URIs
    # headerdict['INP1'] = <provenance input URI 1>

    # Are there any new keywords in the headerdict
    newkeys = False
    for key in headerdict:
        if key not in hdu:
            newkeys = True
    
    # if so, add a comment to label the section containing new keys
    hdu.add_comment('JSA Headers')
    
    # update FITS headers with those supplied in headerdict
    for key in sorted(headerdict.keys()):
        hdu.update(key, headerdict[key])

    dirpath = os.path.dirname(outfits)
    if not os.path.isdir(dirpath):
        os.makedirs(dirpath)
    
    hdulist.writeto(outfits)


def fix_name(outdir, prefix, filename):
    """
    Compose a new name from the prefix, project and basename of the file.
    """
    dirpath, basename = os.path.split(filename)
    return os.path.join(outdir, dirpath, prefix + '_' + basename.lower())

def readfilelist(rootdir, indir, filter, filelist, log):
    """
    Construct a list of file names rooted at indir by reading names from indir
    and calling readfilelist recursively for each directory.  Include only
    filenames for which filter returns True.
    """  
    dirlist = []
    if indir:
        readdir = os.path.join(rootdir, indir)
    else:
        readdir = rootdir
    
    for f in os.listdir(readdir):
        log.file('examine: ' + f)
        filename = os.path.join(rootdir, indir, f)
        if os.path.isfile(filename) and filter(f):
            filelist.append(os.path.join(indir, f))
        if os.path.isdir(filename):
            dirlist.append(os.path.join(indir, f))
    for d in dirlist:
        readfilelist(rootdir, d, filter, filelist, log)

def fits_and_png(filename):
    """
    Return True if the extension is nor a FITS or PNg file, False otherwise
    """
    return (os.path.splitext(filename)[1].lower() in ('.fits', '.fit', '.png'))

def run():
    """
    The run() method for jcmt_prepare_files.  This is intended to be a template
    for a custom program to prepare externally generated data files for ingestion
    into the JSA by changing the name and decorating the file with a standard set 
    of FITS headers.
    """
    progname = os.path.basename(os.path.splitext(sys.path[0])[0])
    # Comment out header names that should not be in the csv file
    header_order =  [
                     'inputfile',
                     'outputfile',
                     'INSTREAM',
                     'ASN_ID',
                     'ASN_TYPE',
                     'MBRCNT',
                     'OBS-TYPE',
                     'PROJECT',
                     'PI',
                     'TITLE',
                     'SURVEY',
                     'DPPROJ',
                     'INSTRUME',
                     'INBEAM',
                     'BACKEND',
                     'SW_MODE',
                     'SCAN_PAT',
                     'OBS_SB',
                     'SB_MODE',
                     'TELESCOP',
                     'OBSGEO_X',
                     'OBSGEO_Y',
                     'OBSGEO_Z',
                     'OBJECT',
                     'TARGTYPE',
                     'ZSOURCE',
                     'TARGKEYW'
                     'MOVING',
                     'OBSRA',
                     'OBSDEC',
                     'RADESYS',
                     'EQUINOX',
                     'PRODID',
                     'PRODUCT',
                     'FILTER',
                     'RESTFREQ',
                     'BWMODE',
                     'SUBSYSNR',
                     'RECIPE',
                     'PROCVERS',
                     'ENGVERS',
                     'PRODUCER',
                     'DPDATE',
                     'INPCNT'
                    ]

    ap = argparse.ArgumentParser('jcmt_prepare_files')
    ap.add_argument('--major',
                    help='existing major release directory')
    ap.add_argument('--minor',
                    help='existing minor release subdirectory within the '
                         'major release; if omited, minor=major')
    ap.add_argument('--newmajor',
                    help='new major release directory to which files will be '
                         'written, preserving the minor release directory '
                         'structure')
    ap.add_argument('--prefix',
                    default='',
                    help='optional prefix for new file names')

    ap.add_argument('-c', '--csv',
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
                    help='set of key=value pairs for default headers')

    a = ap.parse_args()

    loglevel = logging.INFO
    if a.debug:
        loglevel = logging.DEBUG

    with logger(a.log, loglevel).record() as log:
        # Report all command line arguments
        log.file(progname)
        for attr in dir(a):
            if attr != 'id' and attr[0] != '_':
                log.file('%-15s= %s' % 
                                 (attr, str(getattr(a, attr))))
        log.console('log = ' + a.log)

        # if any keyvalue arguments were supplied save them in a dictionary
        keydict = {}
        if a.keyvalue:
            for keyvalue in a.keyvalue:
                m = re.match(r'^([A-Z0-9]+)=(\w+)$', keyvalue)
                if m:
                    keydict[m.group(1)] = m.group(2)
                else:
                    log.console(keyvalue + ' does not match key=value and is '
                                'being ignored',
                                logging.WARN)

        if not a.major and not a.csv:
            log.console('specify either --major or --csv for input; '
                        'if both are given the csv file will be output',
                        logging.ERROR)

        if a.major:
            if not a.minor:
                a.minor = ''
        
            if not a.newmajor:
                log.console('specify both --major and --newmajor, since it is '
                            'forbidden to overwrite the original files',
                            logging.ERROR)
            
            a.major = os.path.abspath(
                        os.path.expandvars(
                            os.path.expanduser(a.major)))
            
            a.newmajor = os.path.abspath(
                            os.path.expandvars(
                                os.path.expanduser(a.newmajor)))
            
            if not os.path.isdir(a.major):
                log.console('major directory ' + a.major + 
                            ' is not a directory',
                            logging.ERROR)

            abs_minor = os.path.abspath(os.path.join(a.major, a.minor))
            if not os.path.isdir(abs_minor):
                log.console('minor directory ' + abs_minor + 
                            ' is not a directory',
                            logging.ERROR)
            
            if not os.path.isdir(a.newmajor):
                log.console('output directory ' + a.newmajor + 
                            ' is not a directory',
                            logging.ERROR)
            
            filelist = []
            readfilelist(a.major, a.minor, fits_and_png, filelist, log)
            infile = [os.path.join(a.major, f) for f in filelist]
            outfile = [fix_name(a.newmajor, a.prefix, f) for f in filelist]
            
            # if a CSV filename is given, open it for output
            try:
                CSV = None
                csvwriter = None
                if a.csv:
                    CSV = open(a.csv, 'wb')
                    csvwriter = csv.DictWriter(CSV, header_order)
                    csvwriter.writeheader()
                
                # Open each FITS file and update it as required
                for i in range(len(filelist)):
                    if a.debug:
                        log.console('infile = ' + infile[i])
                        log.console('    outfile = ' + outfile[i])
                    else:
                        log.file('infile = ' + infile[i])
                        log.file('    outfile = ' + outfile[i])

                    if CSV and csvwriter:
                        # If a CSV file is open, write a row in the CSV file for
                        # each input file
                        rowdict = {}
                        rowdict.update(keydict)
                        rowdict['inputfile'] = infile[i]
                        rowdict['outputfile'] = outfile[i]
                        
                        csvwriter.writerow(rowdict)
                    else:
                        # If the file is a FITS file, copy the file and update
                        # the headers in the primary HDU
                        ext = os.path.splitext(infile[i])[1].lower()
                        if ext in ('.fits', '.fit'):
                            rewrite_fits(infile[i], outfile[i], keydict)
                        else:
                            shutil.copy(nfile[i], outfile[i])
            finally:
                if CSV:
                    CSV.close()

        else:
            if not os.path.isfile(a.csv):
                log.console(a.csv + 'is not a file', logging.ERROR)

            with open(a.csv, 'r') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
                for csvdict in reader:
                    if 'inputfile' in csvdict:
                        infile = csvdict.pop('inputfile')
                    else:
                        log.console('"inputfile" must be a column in ' + a.csv,
                                    logging.ERROR)
                    
                    if 'outputfile' in csvdict:
                        outfile = csvdict.pop('outputfile')
                    else:
                        log.console('"outputfile" must be a column in ' + a.csv,
                                    logging.ERROR)

                    if not os.path.isfile(csvdict['inputfile']):
                        log.console('The FITS file ' + csvdict['inputfile'] +
                                    ' does not exist',
                                    logging.ERROR)

                    headerdict = {}
                    headerdict.update(keydict)
                    headerdict.update(csvdict)
                    
                    rewrite_fits(infile, outfile, headerdict)

if __name__ == __main__:
    run()