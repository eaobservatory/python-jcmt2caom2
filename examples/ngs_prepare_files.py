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
import subprocess
from subprocess import CalledProcessError
import sys

from tools4caom2.logger import logger
from tools4caom2.gridengine import gridengine

from tools4caom2.utdate_string import utdate_string

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version


"""
Custom code to prepare files from the JLS Nearby Galaxy Survey (NGS)
for ingestion into the JSA.
"""

def rewrite_fits(insdf, outfits, workdir, log):
    """
    Rewrite a single sdf file into FITS, setting custom headers as needed.
    """
    # ndfcopy will update the PROVENANCE structure to avoid needless repetition
    mydir, myfile = os.path.split(insdf)
    # Extract the product from the sdf file name
    name_token = myfile.split('_')
    
    sdfcopy = os.path.join(workdir, 'copy_' + myfile)
    
    mydir, myfile = os.path.split(outfits)
    fitscopy = os.path.join(workdir, 'copy_' + myfile)
    
    ndfcopy = os.path.abspath(
                    os.path.expandvars('$KAPPA_DIR/ndfcopy'))
    fitsmod = os.path.abspath(
                    os.path.expandvars('$KAPPA_DIR/fitsmod'))
    ndf2fits = os.path.abspath(
                    os.path.expandvars('$CONVERT_DIR/ndf2fits'))
    
    ndfcopy_cmd = [ndfcopy, insdf, sdfcopy]
    log.console(' '.join(ndfcopy_cmd))
    output = subprocess.check_output(ndfcopy_cmd,
                                     stderr=subprocess.STDOUT)
    if output:
        log.file(output)
    
    # fitswrite will add the product header that is needed for the provenance 
    # to be written
    if len(name_token) == 3:
        product = 'reduced'
    elif len(name_token) == 4:
        product = name_token[2]
    elif len(name_token) == 5:
        product = name_token[2] + '-' + name_token[3]
    else:
        print 'filename = ' + insdf
        print repr(name_token)
    
    fitsmod_cmd = [fitsmod,
                   'edit=write',
                   'mode=interface',
                   'position=\!',
                   sdfcopy,
                   'product',
                   'value=' + product,
                   'comment="science product"']
    log.console(' '.join(fitsmod_cmd))
    output = subprocess.check_output(fitsmod_cmd,
                                     stderr=subprocess.STDOUT)
    if output:
        log.file(output)
    
    # Convert to a CADC-compliant FITS file
    ndf2fits_cmd = [ndf2fits,
                    sdfcopy,
                    fitscopy,
                    'provenance=cadc',
                    'proexts',
                    'profits',
                    'prohis',
                    'duplex',
                    'checksum',
                    'encoding="fits-wcs(cd)"',
                    'comp=dv']
    log.console(' '.join(ndf2fits_cmd))
    output = subprocess.check_output(ndf2fits_cmd,
                                     stderr=subprocess.STDOUT)
    if output:
        log.file(output)
    
    hdulist = pyfits.open(fitscopy)
    head = hdulist[0].header

    # Rather than query the JCMT database, which might not be available to 
    # everyone, use a TAP query to find the PI and Title associated with 
    # each PROJECT.  These values have been captured into the following 
    # dictionary.  The TAP query was:
    # tapquery --adql "SELECT DISTINCT \
    #                      Observation.proposal_id, \
    #                      Observation.proposal_pi, \
    #                      Observation.proposal_title \
    #                  FROM caom2.Observation AS Observation \
    #                  WHERE Observation.proposal_project = 'NGS'"
    
    pi_title = {
        'M11AEC30': ('Remo Tilanus', 
                     'SCUBA-2 commissioning'),
        'MJLSN01': ('Christine Wilson',
                    'Nearby Galaxy Survey Science Verification'),
        'MJLSN02': ('Christine Wilson',
                    'Nearby Galaxy Survey Science Verification (Part 2)'),
        'MJLSN04': ('Christine Wilson',
                    'Nearby Galaxy Survey Science Verification (Part 4)'),
        'MJLSN05': ('Christine Wilson',
                    'Nearby Galaxy Survey (Part 1)'),
        'MJLSN06': ('Christine Wilson',
                    'NGLS SCUBA-2 Science Verification'),
        'MJLSN07': ('Christine Wilson',
                    'Nearby Galaxies Legacy Survey (SCUBA-2)'),
        'MJLSN08': ('Christine Wilson',
                    'Nearby Galaxies Legacy Survey (Spectroscopic Extension)}')
        }
    
    # Gather all the modified headers into headerdict, 
    # then update them in head
    headerdict = {}
    
    # First, parse the filename for useful
    headerdict['INSTREAM'] = 'JCMTLS'
    # headerdict['ASN_ID'] = <observationID for observation>
    # headerdict['ASN_TYPE'] = 'custom'
    # headerdict['MBRCNT'] = 0 # number of membership URIs
    # headerdict['MBR1'] = <membership URI 1>
    headerdict['OBS-TYPE'] = 'science'
    if 'PROJECT' in head and head['PROJECT'] != pyfits.card.UNDEFINED:
        project = head['PROJECT']
        if project in pi_title:
            pi, title = pi_title[project]
            headerdict['PI'] = pi
            headerdict['TITLE'] = title
    
    headerdict['PROJECT'] = 'jcmt-ngs' # This is the data processing project
    headerdict['SURVEY'] = 'NGS'

    # headerdict['INSTRUME'] already set correctly
    # headerdict['INBEAM'] already set correctly
    # headerdict['BACKEND'] already set correctly
    # headerdict['SW_MODE'] already set correctly
    # headerdict['SCAN_PAT'] already set correctly
    # headerdict['OBS_SB'] already set correctly
    # headerdict['SB_MODE'] already set correctly
    headerdict['TELESCOP'] = 'JCMT'
    headerdict['OBSGEO_X'] = -5464588.652191697
    headerdict['OBSGEO_Y'] = -2493003.0215722183
    headerdict['OBSGEO_Z'] = 2150655.6609171447
    # headerdict['OBJECT'] already set correctly
    headerdict['TARGTYPE'] = 'OBJECT' # or 'FIELD'
    # headerdict['ZSOURCE'] = <redshift in BARYCENT frame>
    # headerdict['TARGKEYW'] = <target keyword string>
    headerdict['MOVING'] = 'F'
    # headerdict['OBSRA'] = <target RA in ICRS>
    # headerdict['OBSDEC'] = <target Dec in ICRS>
    # headerdict['RADESYS'] = <RA/Dec system>
    # headerdict['EQUINOX'] = <equinox of coordinates>
    
    # headerdict['PRODID'] = <productID for plane>
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
        if key not in head:
            newkeys = True
    
    # if so, add a comment to label the section containing new keys
    # Existing keywords will be updated in-place
    if newkeys:
        endcard = len(head)
        head.add_comment('JSA Headers', after=endcard)
    
    # update FITS headers with those supplied in headerdict
    for key in sorted(headerdict.keys()):
        if key in head:
            head.update(key, headerdict[key])
        else:
            head.update(key, headerdict[key], after=endcard)

    dirpath = os.path.dirname(outfits)
    if not os.path.isdir(dirpath):
        os.makedirs(dirpath)
    
    hdulist.writeto(outfits)
    os.remove(sdfcopy)
    os.remove(fitscopy)

def fix_name(outdir, prefix, filename):
    """
    Compose a new name from the prefix and basename of the file.
    """
    dirpath, basename = os.path.split(filename)
    file_id = os.path.splitext(basename)[0].lower()
    return os.path.join(outdir, dirpath, prefix + '_' + file_id + '.fits')

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
    Return True if the extension is a FITS or PNG file, False otherwise
    """
    return (os.path.splitext(filename)[1].lower() in ('.fits', '.fit', '.png'))

def sdf(filename):
    """
    Return True if the extension is an sdf file, False otherwise
    """
    return (os.path.splitext(filename)[1].lower() in ('.sdf'))

def run():
    """
    The run() method for ngs_prepare_files.  This is intended to be a template
    for a custom program to prepare externally generated data files for ingestion
    into the JSA by changing the name and decorating the file with a standard set 
    of FITS headers.
    """
    progname = os.path.basename(os.path.splitext(sys.path[1])[0])

    ap = argparse.ArgumentParser('ngs_prepare_files')
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

    ap.add_argument('--workdir',
                    default='.',
                    help='directory to hold working files (default=cwd)')
    
    ap.add_argument('--log',
                    default='ngs_prepare_files_' + utdate_string() + '.log',
                    help='(optional) name of log file')

    # verbosity
    ap.add_argument('--debug', '-d',
                    action='store_true',
                    help='run in debug mode')

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
        
        workdir = os.path.abspath(
                    os.path.expandvars(
                        os.path.expanduser(a.workdir)))

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
            readfilelist(a.major, a.minor, sdf, filelist, log)
            inoutfiles = [(os.path.join(a.major, f),
                           fix_name(a.newmajor, a.prefix, f)) 
                          for f in filelist]
            
            for infile, outfile in inoutfiles:
                rewrite_fits(infile, 
                             outfile, 
                             workdir,
                             log)

if __name__ == '__main__':
    run()