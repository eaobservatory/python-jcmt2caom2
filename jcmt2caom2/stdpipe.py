#!/usr/bin/env python2.7
"""
The stdpipe class immplements methods to collect metadata from a a set of FITS
files and from the jcmtmd database that will be passed to fits2caom2 to
construct a caom2 observation.  Once completed, it is serialized to a temporary
xml file in outdir and copied to the CAOM-2 repository.

This routine requires read access to the jcmtmd database, but does only reads.
It should therefore access SYBASE rather than DEVSYBASE unless heavy loading
makes SYBASE access problematic.
"""

__author__ = "Russell O. Redman"

from ConfigParser import SafeConfigParser
from contextlib import contextmanager
import datetime
import logging
import os.path
import pyfits
import re
import shutil
import string
import vos

from caom2.xml.caom2_observation_reader import ObservationReader
from caom2.xml.caom2_observation_writer import ObservationWriter

from caom2.caom2_enums import CalibrationLevel
from caom2.caom2_enums import ObservationIntentType
from caom2.wcs.caom2_axis import Axis
from caom2.wcs.caom2_coord_axis1d import CoordAxis1D
from caom2.wcs.caom2_coord_bounds1d import CoordBounds1D
from caom2.wcs.caom2_coord_range1d import CoordRange1D
from caom2.wcs.caom2_ref_coord import RefCoord
from caom2.wcs.caom2_temporal_wcs import TemporalWCS
from caom2.caom2_enums import ProductType
from caom2.caom2_simple_observation import SimpleObservation as SimpleObservation

from tools4caom2.database import database
from tools4caom2.ingest2caom2 import ingest2caom2
from tools4caom2.caom2repo_wrapper import Repository
from tools4caom2.timezone import UTC
from tools4caom2.mjd import utc2mjd

from jcmt2caom2.jsa.instrument_keywords import instrument_keywords
from jcmt2caom2.jsa.instrument_name import instrument_name
from jcmt2caom2.jsa.intent import intent
from jcmt2caom2.jsa.product_id import product_id
from jcmt2caom2.jsa.raw_product_id import raw_product_id
from jcmt2caom2.jsa.target_name import target_name

from jcmt2caom2 import tovos 

from jcmt2caom2.__version__ import version as jcmt2caom2version

# from caom2.caom2_enums import CalibrationLevel
# from caom2.caom2_enums import DataProductType

def jcmtcmp(f1, f2):
    # This comparison should order files so that
    # 1) Files with earlier versions are before files with later versions
    # 2) FITS products are ingested in the order
    #    NIT OBS / CUBE REDUCED RIMG RSP
    # 3) Catalogs are ingested immediately after the SMOOTH files from which
    #    they were generated
    orderprod = {'cube': 1,
                 'reduced': 2,
                 'rimg': 3,
                 'rsp': 4,
                 'healpix': 5,
                 'hpxrimg': 6,
                 'hpxrsp': 7
                 }
    
    # Associations will be ingested in "reverse" order for heteroyne data, 
    # i.e. all nit, pro and pub products will be ingested before obs products,
    # because masks used for obs products are derived from composite products
    
    # Associations will be ingested in the "normal" order for SCUBA-2, 
    # i.e. obs before nit, pro and pub, because composites can be built 
    # from obs products
    orderasn = {'jcmth': {'obs': '4',
                          'nit': '3',
                          'pro': '2',
                          'pub': '1'},
                'jcmts': {'obs': '1',
                          'nit': '2',
                          'pro': '3',
                          'pub': '4'}}
    try:
        (date1, obs1, subsys1, prod1, asntype1, version1) = \
            re.split(r'_', os.path.splitext(os.path.basename(f1))[0])
    except:
        raise RuntimeError('stdpipe.jcmtcmp: cannot extract parts from ' + f1)
    
    m1 = re.match(r'([a-z]+)(\d*)', prod1)
    prod1, tile1 = m1.groups()
    text1 = orderasn[date1[0:5]][asntype1] + date1 + obs1 + subsys1 + tile1

    try:
        (date2, obs2, subsys2, prod2, asntype2, version2) = \
            re.split(r'_', os.path.splitext(os.path.basename(f2))[0])
    except:
        raise RuntimeError('stdpipe.jcmtcmp: cannot extract parts from ' + f2)

    m2 = re.match(r'([a-z]+)(\d*)', prod1)
    prod2, tile2 = m2.groups()
    text2 = orderasn[date2[0:5]][asntype2] + date2 + obs2 + subsys2 + tile2

    val = cmp(orderprod[prod1], orderprod[prod2])
    if val == 0:
        val = cmp(text1, text2)
    return val

def isdefined(key, header):
    """
    Test whether a key is present and has a defined value in a FITS header
    
    Arguments:
    key:  a FITS keyword
    header: a FITS header or other dictiaonary-like structure
    """
    return (key in header and header[key] != pyfits.card.UNDEFINED)

# define the jcmt2caom class to manage the ingestions
class stdpipe(ingest2caom2):
    """
    A derived class of ingest2caom2 specialized to ingest JCMT standard pipeline
    products.
    """
    speedOfLight = 2.9979250e8 # Speed of light in m/s
    lambda_csotau = 225.0e9 # Frequency of CSO tau meter in Hz
    raw_acsis_regex = \
        r'^([ah])(20[\d]{2})(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])_' +\
        r'([\d]{5})_([\d]{2})_([\d]{4})$'
    raw_scuba2_regex = \
        r'^(s[48][abcd])(20[\d]{2})(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])_' +\
        r'([\d]{5})_([\d]{4})$'
    proc_acsis_regex = \
        r'jcmth(20[\d]{2})(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])_' +\
        r'([\d]{5})_(0[0-4])_(cube[\d]{3}|reduced[\d]{3}|rimg|rsp|rvel|' + \
        r'linteg[\d]{3}|sp[\d]{3}|std)_(obs|nit|pro|pub)_([\d]{3})$'
    proc_scuba2_regex = \
        r'jcmts(20[\d]{2})(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])_' +\
        r'([\d]{5})_([48]50)_(reduced[\d]{3})_(obs|nit|pro|pub)_([\d]{3})$'

    def __init__(self):
        ingest2caom2.__init__(self)
        self.archive = 'JCMT'
        self.stream = 'product'
        
        self.voscopy = None
        self.vosroot = 'vos:jsaops'
        
        # These defaults are for CADC use, but can be overriden in userconfig.

        # The server and cred_db are used to get database credentials at the CADC.
        # Other sites should supply cadc_id, cadc_key in the section [cadc] of
        # the userconfig file.
        self.userconfigpath = '~/.tools4caom2/jcmt2caom2.config'
        if not self.userconfig.has_section('cadc'):
            self.userconfig.add_section('cadc')
        self.userconfig.set('cadc', 'server', 'SYBASE')
        self.userconfig.set('cadc', 'cred_db', 'jcmt')
        self.userconfig.set('cadc', 'read_db', 'jcmt')
        self.userconfig.set('cadc', 'write_db', 'jcmt')

        # Set the site-dependent databases containing necessary tables
        if not self.userconfig.has_section('jcmt'):
            self.userconfig.add_section('jcmt')
        self.userconfig.set('jcmt', 'caom_db', 'jcmt')
        self.userconfig.set('jcmt', 'jcmt_db', 'jcmtmd')
        self.userconfig.set('jcmt', 'omp_db', 'jcmtmd')
        
        # This is needed for compatability with other uses of ingest2caom2, but
        # should not be used for the JCMT.
        self.database = 'jcmt'

        # set default locations for the fits2caom2 config files
        if not os.path.isdir(self.configpath):
            raise RuntimeError('The config directory ' + self.configpath +
                               ' does not exist')

        configpath = os.path.join(self.configpath, 'jcmt_stdpipe.config')
        if os.path.exists(configpath):
            self.config = configpath
        else:
            raise RuntimeError('The config file ' + configpath +
                               ' does not exist')

        defaultpath = os.path.join(self.configpath, 'jcmt_stdpipe.default')
        if os.path.exists(defaultpath):
            self.default = defaultpath
        else:
            raise RuntimeError('The default file ' + defaultpath +
                               ' does not exist')

        # set the JCMT comparison function
        self.cmpfunc = jcmtcmp

        self.UTC = UTC()

        # Connection to database
        self.conn = None

        # Log level for validity checking
        self.validitylevel = logging.ERROR

        # get xml file reader and writer, to allow insertion of the
        # time structures for chunks with WCS
        self.reader = ObservationReader(True)
        self.writer = ObservationWriter()
        
        self.member_cache = {}
        self.provenance_cache = {}
        self.remove_dict = {}
        self.remove_id = []
        self.repository = None

    #************************************************************************
    # Add the custom command line switchs
    #************************************************************************
    def defineCommandLineSwitches(self):
        """
        Add some JSA-specific switches

        Arguments:
        <none>
        """
        ingest2caom2.defineCommandLineSwitches(self)
        # Append additional arguments to the list of command line switches
        self.ap.add_argument('--check',
                             action='store_true',
                             help='Only do the validity tests for a FITS file')
        self.ap.add_argument('--collection',
                             choices=('JCMT', 'SANDBOX'),
                             default='JCMT',
                             help='collection to use for ingestion')

    #************************************************************************
    # Process the custom command line switchs
    #************************************************************************
    def processCommandLineSwitches(self):
        """
        Process some JSA-specific switches

        Arguments:
        <none>
        """
        ingest2caom2.processCommandLineSwitches(self)
        if self.switches.check:
            self.validitylevel = logging.WARN

        self.collection = self.switches.collection
        
        self.caom_db = (self.userconfig.get('jcmt', 'caom_db') + '.' + 
                        self.schema + '.')
        self.jcmt_db = (self.userconfig.get('jcmt', 'jcmt_db') + '.' + 
                        self.schema + '.')
        self.omp_db = (self.userconfig.get('jcmt', 'omp_db') + '.' + 
                       self.schema + '.')

    #************************************************************************
    # Include the custom command line switch in the log
    #************************************************************************
    def logCommandLineSwitches(self):
        """
        Log the values of the command line switches.

        Arguments:
        <none>
        """
        ingest2caom2.logCommandLineSwitches(self)
        self.log.file('jcmt2caom2version    = ' + jcmt2caom2version)
        self.log.console('collection = ' + repr(self.collection))

    # Utility for checking missing headers
    def check_missing(self, key, head):
        """
        Check whether a header expected in head is missing or undefined.
        If so, append the key to the list of missing headers.

        Arguments:
        key: name of a FITS keyword that should be in the header
        head: pyfits header or a dictionary of headers

        Return:
        True if the key is missing or undefined, False otherwise
        """
        if self.debug:
            self.log.console('enter check_missing: ' + 
                             key + ': ' +
                             datetime.datetime.now().isoformat(),
                             loglevel=logging.DEBUG)
        bad = False
        if key not in head or head[key] == pyfits.card.UNDEFINED:
            self.warnings = True
            self.errors = True
            self.log.console('Mandatory header ' + key + ' is missing',
                             logging.WARN)
            bad = True
        return bad

    # Utility for checking missing headers
    def check_values(self, key, head, acceptable):
        """
        Check whether a the value of a keyword is in the list of acceptable 
        values. If not, log the keyword, value and list of acceptable values.

        Arguments:
        key: name of a FITS keyword that should be in the header
        head: pyfits header or a dictionary of headers
        acceptable: list of acceptable values

        Return:
        True if the key is missing or not in the acceptable list, False otherwise
        """
        if self.debug:
            self.log.console('enter check_values: ' + 
                             key + ': ' +
                             datetime.datetime.now().isoformat(),
                             loglevel=logging.DEBUG)
        bad = False
        if key not in head:
            self.warnings = True
            self.errors = True
            self.log.console('Mandatory header ' + key + ' is missing',
                             logging.WARN)
            bad = True

        else:
            value = head[key]
            if isinstance(value, str):
                value = value.strip()
            
            if value not in acceptable:
                acceptable_list = \
                    ['UNDEFINED' if v == pyfits.card.UNDEFINED else v
                     for v in acceptable]
                acceptable_str = '[' + ', '.join(acceptable_list) + ']'
                self.warnings = True
                self.errors = True
                self.log.console('Mandatory header ' + key + 
                                 ' has the value "' +
                                 value + '" but should be in ' +
                                 acceptable_str,
                                 logging.WARN)
                bad = True
        return bad

    # Discover observations and planes to remove
    def build_remove_dict(self, run_id):
        """
        If identity_instance_id has not already been checked, read back a
        complete list of existing collections, observations and planes, 
        which will be deleted if they are not replaced or updated by the 
        current recipe instance.
        
        Arguments:
        run_id: an identity_instance_id as a decimal string to be compared 
                with Plane.provenance_runID
        """
        if run_id not in self.remove_id:
            self.remove_id.append(run_id)
            sqlcmd = '\n'.join([
                'SELECT',
                '    o.collection,',
                '    o.observationID,',
                '    p.productID,',
                '    p.provenance_runID',
                'FROM',
                '    ' + self.caom_db + 'caom2_Observation o',
                '        INNER JOIN ' + self.caom_db + 'caom2_Plane p',
                '            ON o.obsID=p.obsID',
                'WHERE',
                '    o.obsID in (',
                '        SELECT obsID',
                '        FROM (',
                '            SELECT ',
                '                obsID,',
                '                CASE WHEN charindex("x", provenance_runID) = 2', 
                '                     THEN hextobigint(provenance_runID)',
                '                     ELSE convert(bigint, provenance_runID)',
                '                     END as identity_instance_id',
                '            FROM ' + self.caom_db +'caom2_Plane',
                '            ) s',
                '        WHERE s.identity_instance_id = ' + run_id + ')',
                'ORDER BY o.collection, o.observationID, p.productID'])
            result = self.conn.read(sqlcmd)
            if result:
                for coll, obsid, prodid, run in result:
                    this_runID = str(eval(run) if isinstance(run, str) 
                                     else run)
                    eq = (1 if this_runID == run_id else 0)
                    # Ignore entries in other collections
                    if coll == self.collection:
                        if coll not in self.remove_dict:
                            self.remove_dict[coll] = {}
                        if obsid not in self.remove_dict[coll]:
                            self.remove_dict[coll][obsid] = {}
                        if prodid not in self.remove_dict[coll][obsid]:
                            self.remove_dict[coll][obsid][prodid] = eq

    #************************************************************************
    # archive-specific structures to write override files
    #************************************************************************
    def build_dict(self, header):
        '''Archive-specific code to read the common dictionary from the
               file header.
           The following keys must be defined:
               collection
               observationID
               productID
        '''
        if self.debug:
            self.log.console('enter build_dict: ' + 
                             datetime.datetime.now().isoformat(),
                             loglevel=logging.DEBUG)
        if self.repository is None:
            # note that this is similar to the repository in ingest2caom2, but 
            # that is not made a part of the structure - probably should be 
            self.repository = Repository(self.outdir, 
                                         self.log, 
                                         debug=self.debug,
                                         backoff=[10.0, 20.0, 40.0, 80.0])

        self.log.file('Entering build_dict')
        if 'file_id' not in header:
            self.errors = True
            self.log.console('No file_id in ' + repr(header),
                             logging.ERROR)
        file_id = header['file_id']

        self.log.file('Starting ' + file_id)
        # Doing all the required checks here simplifies the code
        # farther down and ensures error reporting of these basic problems
        # even if the ingestion fails before reaching the place where the
        # header would be used.

        someBAD = False

        # check that all headers are acceptable
        OK_IF_NOT = self.check_acceptable_headers(header)

        # Check that the mandatory file headers exist
        # it is not necessary to check here for keywords that have restricted
        # sets of acceptable values, since they will be subject to more detailed
        # testing below.
        mandatory = ('ASN_TYPE',
                     'BITPIX',
                     'CHECKSUM',
                     'DATASUM',
                     'DPDATE',
                     'DPRCINST',
                     'FILEID',
                     'INSTREAM',
                     'OBSCNT',
                     'OBSGEO-X',
                     'OBSGEO-Y',
                     'OBSGEO-Z',
                     'ENGVERS',
                     'PIPEVERS',
                     'PROCVERS',
                     'PRODUCT',
                     'PRVCNT',
                     'RECIPE',
                     'SIMULATE',
                     'TELESCOP')
        for key in mandatory:
            someBAD |= self.check_missing(key, header)

        # Conditionally mandatory
        if header['ASN_TYPE'] in ('obs',):
            someBAD |= self.check_missing('OBSID', header)
        else:
            # any other value for ASN_TYPE indicates a composite observation
            someBAD |= self.check_missing('ASN_ID', header)

        # We will need self.observationID and algorithm for later error
        # checking, so compute them here.
        if header['ASN_TYPE'] == 'obs':
            algorithm = 'exposure'
            self.observationID = header['OBSID']
        else:
            algorithm = header['ASN_TYPE']
            self.observationID = header['ASN_ID']

        self.log.console('observationID = ' + self.observationID,
                         logging.DEBUG)
        self.log.console('algorithm.name = ' + algorithm,
                         logging.DEBUG)

        if header['BACKEND'].strip() in ('SCUBA-2',):
            someBAD |= self.check_missing('FILTER', header)
        else:
            # ACSIS-like files must define the SUBSYSNR, RESTFRQ and BWMODE
            someBAD |= self.check_missing('SUBSYSNR', header)
            someBAD |= self.check_missing('RESTFRQ', header)
            someBAD |= self.check_missing('BWMODE', header)

        obscnt = int(header['OBSCNT'])
        if obscnt > 0:
            for n in range(obscnt):
                obsn = 'OBS' + str(n+1)
                someBAD |= self.check_missing(obsn, header)

        prvcnt = int(header['PRVCNT'])
        if prvcnt > 0:
            for n in range(prvcnt):
                prvn = 'PRV' + str(n+1)
                someBAD |= self.check_missing(prvn, header)

        # Check that headers with restricted sets of values are valid
        someBAD |= self.check_values('ASN_TYPE', header,
                                     ['obs', 'night', 'project', 'night'])

        backendBAD = self.check_values('BACKEND', header,
                                       ['ACSIS', 'DAS', 'AOS-C', 'SCUBA-2'])
        someBAD |= backendBAD

        if not backendBAD:
            # Only do these tests if the backend is OK
            backend = header['BACKEND'].upper().strip()
            if backend in ('ACSIS', 'DAS', 'AOS-C'):
                someBAD |= self.check_values('INBEAM', header,
                    [pyfits.card.UNDEFINED,
                     'POL'])

                someBAD |= self.check_values('OBS_TYPE', header,
                    ['pointing', 'science', 'focus', 'skydip'])

                someBAD |= self.check_values('SAM_MODE', header,
                    ['jiggle', 'grid', 'raster', 'scan'])

                someBAD |= self.check_values('SURVEY', header,
                    [pyfits.card.UNDEFINED,
                     'GBS', 'NGS', 'SLS'])

            elif backend == 'SCUBA-2':
                someBAD |= self.check_values('OBS_TYPE', header,
                    ['pointing', 'science', 'focus', 'skydip',
                     'flatfield', 'setup', 'noise'])

                someBAD |= self.check_values('SAM_MODE', header,
                    ['scan', 'stare'])

                someBAD |= self.check_values('SURVEY', header,
                    [pyfits.card.UNDEFINED,
                     'CLS', 'DDS', 'GBS', 'JPS', 'NGS', 'SASSY'])

            if isdefined('INSTRUME', header):
                frontend = header['INSTRUME']
            else:
                frontend = 'UNKNOWN'
            
            # Check some more detailed values by building instrument_keywords
            keyword_dict = {}
            if isdefined('SW_MODE', header):
                keyword_dict['switching_mode'] = header['SW_MODE']

            if isdefined('INBEAM', header):
                keyword_dict['inbeam'] = header['INBEAM']
            
            if isdefined('SCAN_PAT', header):
                keyword_dict['x_scan_pat'] = header['SCAN_PAT']
            
            if backend in ('ACSIS', 'DAS', 'AOS-C'):
                if isdefined('OBS_SB', header):
                    keyword_dict['sideband'] = header['OBS_SB']
                
                if isdefined('SB_MODE', header):
                    keyword_dict['sideband_filter'] = header['SB_MODE']
            
            thisBad, keyword_list = instrument_keywords('stdpipe',
                                                        frontend,
                                                        backend,
                                                        keyword_dict,
                                                        self.log)
            self.instrument_keywords = ' '.join(keyword_list)
            
            # verify membership headers are real observations
            max_release_date = None
            obstimes = {}

            self.log.file('Reading membership, OBSCNT = ' + str(obscnt),
                          logging.DEBUG)
            if obscnt > 0:
                for i in range(obscnt):
                    # Starlink records the obsid-subsysnr in OBSn to
                    # identify the input observation.  There is no ICD
                    # to parse the obsid_subsysnr; the FILES and ACSIS 
                    # tables provide the only valid translation from 
                    # obsid_subsysnr to obsid.  The COMMON table provides 
                    # the definitive source for raw data release dates,
                    # from which pipeline product release dates are 
                    # computed.
                    obskey = 'OBS' + str(i + 1)
                    obsn = header[obskey]
                    obsid = None
                    
                    if obsn in self.member_cache:
                        obsid, release_date, date_obs, date_end = \
                            self.member_cache[obsn]
                        self.log.file('fetch member metadata from cache '
                                      'for ' + obsn,
                                      logging.DEBUG)
                    else:
                        sqlcmd = '\n'.join([
                            'SELECT distinct f.obsid,',
                            '       c.release_date,',
                            '       c.date_obs,',
                            '       c.date_end',
                            'FROM ' + self.jcmt_db + 'FILES f',
                            '    INNER JOIN ' + self.jcmt_db + 'COMMON c',
                            '        ON f.obsid=c.obsid',
                            'WHERE f.obsid_subsysnr = "%s"' % (obsn,)])
                        result = self.conn.read(sqlcmd)
                        if len(result):
                            obsid, release_date, date_obs, date_end = \
                                result[0]
                            
                            # cache the membership metadata
                            self.member_cache[obsn] = \
                                (obsid, release_date, date_obs, date_end)
                            self.log.file('cache member metadata '
                                          'for ' + obsn,
                                          logging.DEBUG)
                                
                            # Also cache the productID's for each file
                            fdict = raw_product_id(backend,
                                                   'prod',
                                                   obsid,
                                                   self.conn,
                                                   self.log)
                            self.provenance_cache.update(fdict)
                            self.log.file('cache provenance metadata '
                                          'for ' + obsn,
                                          logging.DEBUG)

                        else:
                            self.warnings = True
                            self.log.console('Member key ' + obsn + ' is '
                                             'not in jcmtmd.dbo.FILES',
                                             logging.WARN)
                            someBAD = True
                        
                    if obsid:
                        # record the time interval
                        if ((algorithm != 'exposure'
                             or obsid == self.observationID)
                            and obsid not in obstimes):
                                obstimes[obsid] = (date_obs, date_end)

                        if max_release_date:
                            if max_release_date < release_date:
                                max_release_date = release_date
                        else:
                            max_release_date = release_date
                        
            if algorithm != 'exposure' and not obstimes:
                # It is an error if a composite has no members
                self.warnings = True
                self.log.console('No members in a composite '
                                 'observation: ' + self.observationID,
                                 logging.WARN)
                someBAD = True

            if not max_release_date:
                self.warnings = True
                self.log.console('Release date could not be '
                                 'calculated from membership: ' +
                                 self.observationID,
                                 logging.WARN)
                someBAD = True
            
            # Translate the PRV1..PRV<PRVCNT> headers into plane URIs
            product = header['PRODUCT']
            self.log.file('Reading provenance')
            prvprocset = set()
            prvrawset = set()
            self.log.file('provenance_cache: ' + repr(self.provenance_cache),
                                                      logging.DEBUG)

            if product not in ('rimg', 'rsp'):

                self.log.file('PRVCNT = ' + str(prvcnt))
                for i in range(prvcnt):
                    # Verify that files in provenance are being ingested
                    # or have already been ingested.
                    prvkey = 'PRV' + str(i + 1)
                    prvn = header[prvkey]
                    self.log.file(prvkey + ' = ' + prvn)
                    if (re.match(stdpipe.proc_acsis_regex, prvn) or
                        re.match(stdpipe.proc_scuba2_regex, prvn)):
                        # Does this look like a processed file?
                        # An existing problem is that some files include 
                        # themselves in their provenance, but are otherwise
                        # OK.
                        if prvn == file_id:
                            # add a warning and skip this entry
                            self.warnings = True
                            self.log.console(
                                'file_id = ' + file_id + ' includes itself'
                                ' in its provenance as ' + prvkey,
                                logging.WARN)
                            continue
                        prvprocset.add(prvn)

                    elif (re.match(stdpipe.raw_acsis_regex, prvn) or
                          re.match(stdpipe.raw_scuba2_regex, prvn)):
                        # Does it look like a raw file?
                        if algorithm == 'exposure':
                            if prvn in self.provenance_cache:
                                prv_obsID, prv_prodID = \
                                    self.provenance_cache[prvn]
                                self.log.file('fetch provenance metadata '
                                              'from cache for ' + prvn,
                                              logging.DEBUG)
                                if prv_obsID != self.observationID:
                                    continue
                            else:
                                self.errors = True
                                self.log.console('provenance and membership '
                                                 'headers list inconsistent '
                                                 'raw data:' +
                                                 prvn + ' is not in ' +
                                                 'provenance_cache constructed '
                                                 'from membership',
                                                 logging.ERROR)
                            
                        # Add the file if this is NOT an exposure,
                        # or if it is and the file is part of the same exposure
                        prvrawset.add(prvn)

                    else:
                        # There are many files with bad provenance.
                        # This should be an error, but it is prudent
                        # to report it as a warning until all of the
                        # otherwise valid recipes have been fixed.
                        self.warnings = True
                        self.log.console('In file "' + file_id + '", ' +
                                         prvkey + ' = ' + prvn + ' is '
                                         'neither processed nor raw',
                                         logging.WARN)
                        # Remove the comment character to make this 
                        # conditiuon an error.
                        # someBAD = True

        # Report any problems that have been encountered, including
        # the file name
        if someBAD:
            self.log.console('Bad headers in ' + file_id,
                             self.validitylevel)
        else:
            # In check mode, return immediately without attempting ingestion
            if self.switches.check:
                self.log.console('SUCCESS: header check passes for ' + file_id)
            else:
                self.log.console('PROGRESS: header check passes for ' + file_id)
        if self.switches.check:
            return
                
        #----------------------------------------------------------------------
        # Only get here if NOT in check mode
        # Begin real ingestion
        #----------------------------------------------------------------------
        # Determine whether this is a simple or composite observation
        self.add_to_plane_dict('algorithm.name', algorithm)
        if algorithm != 'exposure':
            for obsid in list(obstimes):
                obsnURI = self.observationURI(self.collection,
                                              obsid)

        # proposal - define this structure only if the proposal is unambiguous
        if isdefined('PROJECT', header):
            self.add_to_plane_dict('proposal.id', header['PROJECT'])

            if isdefined('SURVEY', header):
                self.add_to_plane_dict('proposal.project', header['SURVEY'])

            sqlcmd = '\n'.join([
                'SELECT ',
                '    ou.uname,',
                '    op.title',
                'FROM ' + self.omp_db + 'ompproj op',
                '    LEFT JOIN ' + self.omp_db + 'ompuser ou ON op.pi=ou.userid',
                'WHERE op.projectid="%s"' % (header['PROJECT'],)])
            answer = self.conn.read(sqlcmd)

            if len(answer):
                self.add_to_plane_dict('proposal.pi',
                                       answer[0][0])
                self.add_to_plane_dict('proposal.title',
                                       answer[0][1])
        
        # Instrument
        if isdefined('BACKEND', header):
            inbeam = ''
            if isdefined('INBEAM', header):
                inbeam = header['INBEAM']
            instrument = instrument_name(header['INSTRUME'],
                                         header['BACKEND'],
                                         inbeam,
                                         self.log)
            self.add_to_plane_dict('instrument.name', instrument)
            self.add_to_plane_dict('instrument.keywords',
                                   self.instrument_keywords)

        # Environment
        if isdefined('SEEINGST', header):
            self.add_to_plane_dict('environment.seeing',
                                   '%f' % (header['SEEINGST'],))

        if isdefined('HUMSTART', header):
            # Humity is reported in %, but should be scaled to [0.0, 1.0]
            if header['HUMSTART'] < 0.0:
                humidity = 0.0
            elif header['HUMSTART'] > 100.0:
                humidity = 100.0
            else:
                humidity = header['HUMSTART']
            self.add_to_plane_dict('environment.humidity',
                                   '%f' % (humidity,))

        if isdefined('ELSTART', header):
            self.add_to_plane_dict('environment.elevation',
                                   '%f' % (header['ELSTART'],))

        if isdefined('TAU225ST', header):
            self.add_to_plane_dict('environment.tau',
                                   '%f' % (header['TAU225ST'],))
            wave_tau = '%12.9f' % (stdpipe.speedOfLight/stdpipe.lambda_csotau)
            self.add_to_plane_dict('environment.wavelengthTau',
                                   wave_tau)

        if isdefined('ATSTART', header):
            self.add_to_plane_dict('environment.ambientTemp',
                                   '%f' % (header['ATSTART'],))


        # if they are unambiguous, calculate the observation type
        # from OBS_TYPE and SAM_MODE.
        obs_type = None
        if isdefined('OBS_TYPE', header):
            obs_type = header['OBS_TYPE'].strip()
            intent_val = intent(obs_type, header['BACKEND']).value
            self.add_to_plane_dict('obs.intent', intent_val)

            if isdefined('SAM_MODE', header):

                if obs_type == "science":
                    if header["SAM_MODE"] == "raster":
                        self.add_to_plane_dict('OBSTYPE',
                                               'scan')
                    else:
                        self.add_to_plane_dict('OBSTYPE',
                                               header['SAM_MODE'])
                else:
                    if obs_type not in ("phase", "ramp"):
                        self.add_to_plane_dict('OBSTYPE',
                                               obs_type)

        # Target
        if obs_type not in ('flatfield', 'noise', 'setup', 'skydip'):
            if isdefined('OBJECT', header):
                self.add_to_plane_dict('target.name',
                                       header['OBJECT'])
            
            if isdefined('STANDARD', header):
                    if header['STANDARD']:
                        self.add_to_plane_dict('STANDARD', 'TRUE')
                    else:
                        self.add_to_plane_dict('STANDARD', 'FALSE')

            if isdefined('OBSRA', header) or isdefined('OBSDEC', header):
                self.add_to_plane_dict('target.moving', 'TRUE')

                # fits2caom2 has trouble with some moving coordinate systems
                if (header['CTYPE1'][0:4] == 'OFLN'
                    and 'CTYPE1A' in header):
                    # Use the first alternate coordinate system
                    self.config = os.path.join(self.configpath, 
                                               'jcmt_stdpipe_a.config')
                    self.default = os.path.join(self.configpath, 
                                               'jcmt_stdpipe_a.default')
                    
                else:
                    self.add_to_plane_dict('target.moving', 'FALSE')
                    self.add_to_plane_dict('target_position.cval1',
                                           str(header['OBSRA']))
                    self.add_to_plane_dict('target_position.cval2',
                                           str(header['OBSDEC']))
                    self.add_to_plane_dict('target_position.radesys',
                                           'ICRS')
                    self.add_to_plane_dict('target_position.equinox',
                                           '2000.0')
                    
            if backend != 'SCUBA-2' and isdefined('ZSOURCE', header):
                
                self.add_to_plane_dict('target.redshift',
                                       str(header['ZSOURCE']))

        # Plane metadata
        product = header['PRODUCT']
        if isdefined('PRODID', header) and header['PRODID']:
            self.productID = header['PRODID']
            self.log.console('productID = ' + self.productID,
                             logging.DEBUG)
            if product == 'cube':
                self.add_to_plane_dict('plane.calibrationLevel',
                                       str(CalibrationLevel.RAW_STANDARD.value))
            elif product in['reduced', 'rsp', 'rimg']: 
                self.add_to_plane_dict('plane.calibrationLevel',
                                       str(CalibrationLevel.CALIBRATED.value))
            elif product in ['healpix', 'hpxrsp', 'hpxrimg', 
                             'pointcat', 'extendcat', 'peakcat', 'clumpcat']:
                self.add_to_plane_dict('plane.calibrationLevel',
                                       str(CalibrationLevel.PRODUCT.value))
            
        elif header['INSTRUME'] == 'SCUBA-2':
            self.productID = product_id('SCUBA-2', self.log,
                                        product=product,
                                        filter=str(header['FILTER']))
            self.add_to_plane_dict('plane.calibrationLevel',
                                   str(CalibrationLevel.CALIBRATED.value))
        else:  # ACSIS-like backends
            if product in ['reduced', 'rimg', 'rsp']:
                self.productID = \
                    product_id(header['BACKEND'], self.log,
                               product='reduced',
                               restfreq=float(header['RESTFRQ']),
                               bwmode=header['BWMODE'],
                               subsysnr=str(header['SUBSYSNR']))
            elif product in ['healpix', 'hpxrimg', 'hpxrsp']:
                self.productID = \
                    product_id(header['BACKEND'], self.log,
                               product='healpix',
                               restfreq=float(header['RESTFRQ']),
                               bwmode=header['BWMODE'],
                               subsysnr=str(header['SUBSYSNR']))
            elif product == 'cube':
                # like raw data files, cube files need to be grouped into
                # hybrid planes.  These are the same as ACSIS subsystems if
                # the observation does not include hybrid mode sybsystems.
                sqlcmd = '\n'.join(
                    ['SELECT min(a.subsysnr)',
                     'FROM ' + self.omp_db + 'ACSIS a',
                     '    INNER JOIN (',
                     '        SELECT aa.obsid,',
                     '               aa.restfreq,',
                     '               aa.iffreq,',
                     '               aa.ifchansp',
                     '        FROM ' + self.omp_db + 'ACSIS aa',
                     '        WHERE aa.obsid="%s" AND' % (header['OBSID'], ),
                     '              aa.subsysnr=%s) s' % (header['SUBSYSNR'], ),
                     '            ON a.obsid=s.obsid AND',
                     '               a.restfreq=s.restfreq AND',
                     '               a.iffreq=s.iffreq AND',
                     '               a.ifchansp=s.ifchansp',
                     'GROUP BY a.obsid,',
                     '         a.restfreq,',
                     '         a.iffreq,',
                     '         a.ifchansp'])
                result = self.conn.read(sqlcmd)
                if len(result):
                    self.productID = \
                        product_id(header['BACKEND'], self.log,
                                   product='cube',
                                   restfreq=float(header['RESTFRQ']),
                                   bwmode=header['BWMODE'],
                                   subsysnr='%d' % result[0])
                else:
                    self.errors = True
                    self.log.console('Could not generate productID for ' +
                                     file_id,
                                     logging.ERROR)
            else:
                self.productID = \
                    product_id(header['BACKEND'], self.log,
                               product=product,
                               restfreq=float(header['RESTFRQ']),
                               bwmode=header['BWMODE'],
                               subsysnr=str(header['SUBSYSNR']))

            if product == 'cube':
                self.add_to_plane_dict('plane.calibrationLevel',
                                       str(CalibrationLevel.RAW_STANDARD.value))
            else:
                self.add_to_plane_dict('plane.calibrationLevel',
                                       str(CalibrationLevel.CALIBRATED.value))

        # Define the productID, dataProductType and calibrationLevel
        # Axes are always in the order X, Y, Freq, Pol
        # but may be degenerate with length 1.  Only compute the 
        # dataProductType for science data.
        if product in ['reduced', 'cube']:
            if (header['NAXIS'] == 3 or
                (header['NAXIS'] == 4 and header['NAXIS4'] == 1)):
                if (header['NAXIS1'] == 1 and 
                    header['NAXIS2'] == 1):
                    dataProductType = 'spectrum'
                elif header['NAXIS3'] == 1:
                    dataProductType = 'image'
                else:
                    dataProductType = 'cube'
            else:
                # getting here is normally an error in data engineering, 
                # not a problem with the file
                self.errors = True
                self.log.console('unrecognized data array structure' +
                    ': NAXIS=' + str(header['NAXIS']) + ' ' +
                    ' '.join([na + '=' + str(header[na]) 
                              for na in ['NAXIS' + str(n + 1) 
                                         for n in range(header['NAXIS'])]]),
                                logging.ERROR)
                                                    
            self.add_to_plane_dict('plane.dataProductType',
                                   dataProductType)
        # Provenance
        if isdefined('RECIPE', header):
            self.add_to_plane_dict('provenance.name',
                                   header['RECIPE'])
            if product in ['reduced', 'cube']:
                self.add_to_plane_dict('provenance.project',
                                       'JCMT_STANDARD_PIPELINE')
            else:
                # healpix and catalogs are from the legacy project
                self.add_to_plane_dict('provenance.project',
                                       'JCMT_LEGACY_PIPELINE')
            if isdefined('REFERENC', header):
                self.add_to_plane_dict('provenance.reference',
                                       header['REFERENC'])

            # ENGVERS and PIPEVERS are mandatory
            self.add_to_plane_dict('provenance.version',
                                    'ENG:' + header['ENGVERS'][:25] + 
                                    ' PIPE:' + header['PIPEVERS'][:25])

            if isdefined('PRODUCER', header):
                self.add_to_plane_dict('provenance.producer',
                                       header['PRODUCER'])

            if isdefined('DPRCINST', header):
                # str(eval() converts hex values to arbitrary-size int
                # then back to decimal string holding identity_instance_id
                if isinstance(header['DPRCINST'], str):
                    dprcinst = str(eval(header['DPRCINST']))
                else:
                    dprcinst = str(header['DPRCINST'])
                self.add_to_plane_dict('provenance.runID', dprcinst)
                self.build_remove_dict(dprcinst)

            if isdefined('DPDATE', header):
                self.add_to_plane_dict('provenance.lastExecuted',
                                       header['DPDATE'])

            # Translate the PRV1..PRV<PRVCNT> headers into plane URIs
            self.log.file('Reading provenance')
            if prvprocset:
                for prvn in list(prvprocset):
                    # check if we have just ingested this file
                    prvnURI = self.fitsfileURI(self.archive,
                                               prvn,
                                               fits2caom2=False)
                    (c, o, p) = self.findURI(prvnURI)
                    if c:
                        self.planeURI(c, o, p)
                    else:
                        self.warnings = True
                        self.log.console('for file_id = ' + file_id + 
                                         ' processed file in provenance has not '
                                         'yet been ingested:' + prvn,
                                         logging.WARN)

            if prvrawset:
                for prvn in list(prvrawset):
                    # prvn only added to prvrawset if it is in provenance_cache
                    prv_obsID, prv_prodID = self.provenance_cache[prvn]
                    self.log.console('collection='+repr(self.collection) +
                                     '  prv_obsID=' + repr(prv_obsID) +  #####
                                     '  prv_prodID=' + repr(prv_prodID),
                                     logging.DEBUG)  
                    self.planeURI(self.collection,
                                  prv_obsID,
                                  prv_prodID)
                             
        if product in ['reduced', 'cube']:
            max_release_date_str = max_release_date.isoformat()
            self.add_to_plane_dict('obs.metaRelease',
                                   max_release_date_str)
            self.add_to_plane_dict('plane.metaRelease',
                                   max_release_date_str)
            self.add_to_plane_dict('plane.dataRelease',
                                   max_release_date_str)
        
        # Chunk
        if header['BACKEND'] in ('SCUBA-2',):
            if isdefined('FILTER', header):
                self.add_to_plane_dict('bandpassName', 
                                'SCUBA-2-' + str(header['FILTER']) + 'um')
        elif header['BACKEND'] in ('ACSIS',):
            if (isdefined('MOLECULE', header) and isdefined('TRANSITI', header)
                and header['MOLECULE'] != 'No Line'):
                self.add_to_plane_dict('energy.transition.species',
                                       header['MOLECULE'])
                self.add_to_plane_dict('energy.transition.transition',
                                       header['TRANSITI'])

        if product in ['cube', 'reduced']:
            primaryURI = self.fitsextensionURI(self.archive,
                                               file_id,
                                               [0])
            if intent_val == 'science':
                self.add_to_fitsuri_dict(primaryURI,
                                         'part.productType', 
                                         ProductType.SCIENCE.value)
            else:
                self.add_to_fitsuri_dict(primaryURI,
                                         'part.productType', 
                                         ProductType.CALIBRATION.value)

            varianceURI = self.fitsextensionURI(self.archive,
                                               file_id,
                                               [1])
            self.add_to_fitsuri_dict(varianceURI,
                                     'part.productType',
                                     ProductType.NOISE.value)

#            ignore_wcs_URI = self.fitsextensionURI(self.archive,
#                                               file_id,
#                                               [(3, 999)])
#            self.add_to_fitsuri_dict(ignore_wcs_URI,
#                                     'BITPIX',
#                                     '0')
            
            fileURI = self.fitsfileURI(self.archive,
                                       file_id)
            self.add_to_fitsuri_dict(fileURI,
                                     'part.productType',
                                     ProductType.AUXILIARY.value)

            # Record times for science products
            for key in sorted(obstimes, key=lambda t: t[1][0]):
                self.add_to_fitsuri_custom_dict(fileURI,
                                                key,
                                                obstimes[key])

        elif product in ['rsp', 'rimg']:
            fileURI = self.fitsfileURI(self.archive,
                                       file_id)
            self.add_to_fitsuri_dict(fileURI,
                                     'artifact.productType',
                                     ProductType.PREVIEW.value)
#            self.add_to_fitsuri_dict(fileURI,
#                                     'BITPIX',
#                                     '0')

        else:
            fileURI = self.fitsfileURI(self.archive,
                                       file_id)
            self.add_to_fitsuri_dict(fileURI,
                                     'artifact.productType',
                                     ProductType.AUXILIARY.value)
#            self.add_to_fitsuri_dict(fileURI,
#                                     'BITPIX',
#                                     '0')


    def build_fitsuri_custom(self,
                             xmlfile,
                             collection,
                             observationID,
                             planeID,
                             fitsuri):
        """
        Customize the xml file with fitsuri-specific metadata.  For
        jcmt2caom2proc, this comprises the time structure constructed from the
        OBSID for simple observations or list of OBSn values for composite
        observations.
        
        It has been found that fits2caom2 is generating false WCS structures 
        for JCMT files and that these fluff up the xml file to the point that
        fits2caom2 runs out of memory and crashes.  This is exacerbated by the
        large number of files that can be present in a JCMT observation, 
        especially multi-tiled obs products.  The kludge to work around this 
        problem is to check whether each part has product_type == 
        ProductType.SCIENCE, if so add the time to each chunk and if not
        to DELETE ALL CHUNKS IN TH PART!!!
        """
        thisCustom = \
            self.metadict[collection][observationID][planeID][fitsuri]['custom']
        if thisCustom:
            # if this dictionary is empty,skip processing
            self.log.console('custom processing for ' + fitsuri,
                             logging.DEBUG)
            
            observation = self.reader.read(xmlfile)
            # Check whether this is a part-specific uri.  We are only interested
            # in artifact-specific uri's.
            if not fitsuri in observation.planes[planeID].artifacts:
                self.log.console('skip custom processing because fitsuri does '
                                 'not point to an artifact',
                                 logging.DEBUG)
                return
            if (observation.algorithm != SimpleObservation._ALGORITHM and
                len(observation.members) > 1):
                # single exposure products have DATE-OBS and DATE-END, 
                # and are handled correctly by fits2caom2
                caomArtifact = observation.planes[planeID].artifacts[fitsuri]

                for part in caomArtifact.parts:
                    thisPart = caomArtifact.parts[part]
                    if thisPart.product_type in [ProductType.SCIENCE,
                                                 ProductType.NOISE]:
                        for chunk in thisPart.chunks:
                            if chunk.position:
                                # if the position WCS exists, add a time axis
                                time_axis = CoordAxis1D(Axis('TIME', 'd'))
#                                if len(thisCustom) == 1:
#                                    # time range
#                                    for key in thisCustom:
#                                        date_start, date_end = thisCustom[key]
#                                        mjdstart = utc2mjd(date_start)
#                                        mjdend = utc2mjd(date_end)
#                                        self.log.console(
#                                            'time range = %f, %f' %
#                                            (mjdstart, mjdend),
#                                            logging.DEBUG)
#
#                                        time_axis.range = CoordRange1D(
#                                            RefCoord(0.5, mjdstart),
#                                            RefCoord(1.5, mjdend))
#
#                                elif len(thisCustom) > 1:
                                if len(thisCustom):
                                    # time
                                    time_axis.bounds = CoordBounds1D()
                                    for key in thisCustom:
                                        date_start, date_end = thisCustom[key]
                                        mjdstart = utc2mjd(date_start)
                                        mjdend = utc2mjd(date_end)
                                        self.log.console(
                                            'time bounds = %f, %f' %
                                            (mjdstart, mjdend),
                                            logging.DEBUG)

                                        time_axis.bounds.samples.append(
                                            CoordRange1D(
                                                RefCoord(0.5, mjdstart),
                                                RefCoord(1.5, mjdend)))

                                else:
                                    self.warnings = True
                                    self.log.console('no time ranges defined '
                                                     ' for ' + fitsuri.uri,
                                                     logging.WARN)
                                
                                # if a temporalWCS already exists, use it but
                                # replace the CoordAxis1D
                                if chunk.time:
                                    chunk.time.axis = time_axis
                                    chunk.time.timesys = 'UTC'
                                else:
                                    chunk.time = TemporalWCS(time_axis)
                                    chunk.time.timesys = 'UTC'
                                self.log.console('temporal axis = ' + 
                                                 repr(chunk.time.axis.axis),
                                                 logging.DEBUG)
                                self.log.console('temporal WCS = ' + 
                                                 str(chunk.time),
                                                 logging.DEBUG)
    #                else:
                        # If not science, delete all chunks
                        # Once fits2caom2 is fixed, discard this clause
    #                    for chunk in thisPart.chunks:
    #                        thisPart.chunks.remove(chunk)

                with open(xmlfile, 'w') as XMLFILE:
                    self.writer.write(observation, XMLFILE)


    def build_plane_custom(self,
                           xmlfile,
                           collection,
                           observationID,
                           productID):
        """
        Implement the cleanup of planes that are no longer generated by this 
        recipe instance from observations that are.  It is only necessary to 
        remove planes from the current observation that are not already being 
        replaced by the new set of products.
        
        Arguments:
        xmlfile: current xmlfile
        collection: current collection
        observationID: current observationID
        productID: current productID NOT USED in this routine
        """
        if collection in self.remove_dict:
            if observationID in self.remove_dict[collection]:
                obs = self.reader.read(xmlfile)
                for prod in obs.planes.keys():
                    # logic is, this collection/observation/plane used to be
                    # genrated by this recipe instance, but is not part of the
                    # current ingestion and so is obsolete.
                    if prod in self.remove_dict[collection][observationID] and\
                        self.remove_dict[collection][observationID][prod] and\
                        prod not in self.metadict[collection][observationID]:
                        
                        uri = self.planeURI(collection, 
                                            observationID, 
                                            prod,
                                            input=False)
                        self.warnings = True
                        self.log.console('CLEANUP: remove obsolete plane:' + 
                                         uri.uri,
                                         logging.WARN)
                        del obs.planes[prod]
                        del self.remove_dict[collection][observationID][prod]
                        
                if not self.test:
                    with open(xmlfile, 'w') as XMLFILE:
                        self.writer.write(obs, XMLFILE)

    def build_observation_custom(self,
                                 xmlfile,
                                 collection,
                                 observationID):
        """
        Implement the cleanup of collections, observations, and planes that are 
        no longer generated by this recipe instance.  It is only necessary to 
        remove items that are not already being replaced by the new set of 
        products.  At this level, remove all collection, observations and planes
        from observations that are not generated by the current recipe instance. 
        
        Arguments:
        xmlfile: current xmlfile NOT USED in this routine
        collection: current collection NOT USED in this routine
        observationID: current observationID NOT USED in this routine
        """
        # log the contents of remove_dict
        for coll in self.remove_dict:
            for obsid in self.remove_dict[coll].keys():
                for prodid in self.remove_dict[coll][obsid].keys():
                    self.log.file('remove_dict ' + coll + ': ' + obsid +
                                  ': ' + prodid + '= ' + 
                                  str(self.remove_dict[coll][obsid][prodid]))
        
        for coll in self.remove_dict:
            if coll not in self.metadict:
                # Nothing in the existing collection still exists
                for obsid in self.remove_dict[coll].keys():
                    uri = self.observationURI(coll, obsid, member=False)
                    self.log.console('CLEANUP: remove ' + uri.uri)
                    if not self.test:
                        self.repository.remove(uri.uri)
                    del self.remove_dict[coll][obsid]
                del self.remove_dict[coll]
            
            else:
                for obsid in self.remove_dict[coll].keys():
                    uri = self.observationURI(coll, obsid, member=False)
                    if obsid not in self.metadict[coll]:
                        # Delete the whole observation or just some planes?
                        same = 1
                        for prodid in self.remove_dict[coll][obsid]:
                            same *= self.remove_dict[coll][obsid][prodid]
                        if same:
                            # all planes come from the same recipe instance
                            # so delete the whole observation
                            self.warnings = True
                            self.log.console('CLEANUP: remove obsolete '
                                             'observation: ' + uri.uri,
                                             logging.WARN)
                            if not self.test:
                                self.repository.remove(uri.uri)
                            del self.remove_dict[coll][obsid]
                        else:
                            with repository.process(uri) as badxmlfile:
                                if os.path.exists(badxmlfile):
                                    obs = self.reader.read(badxmlfile)
                                    for prod in self.remove_dict[coll][obsid].keys():
                                        if self.remove_dict[coll][obsid][prod]\
                                            and prod in obs.planes:
                                            uri = self.planeURI(coll, 
                                                                obsid, 
                                                                prod,
                                                                input=False)
                                            self.warnings = True
                                            self.log.console('CLEANUP: remove '
                                                'plane: ' + uri.uri,
                                                logging.WARN)
                                            
                                            del obs.planes[prod]
                                            del self.remove_dict[coll][obsid][prod]
                                if not self.test:
                                    with open(badxmlfile, 'w') as XMLFILE:
                                        self.writer.write(observation, XMLFILE)

    #************************************************************************
    # JSA cleanup
    #************************************************************************
    def cleanup(self):
        """
        Save the log file to the jsaops VOspace, then delete the log file from
        the logdir unless in debug mode or --keeplog was requested.
        
        Arguements:
        <none>
        """
        if self.collection == 'JCMT' and self.mode == 'ingest':
            self.voscopy = tovos.stdpipe_ingestion(vos.Client(),
                                                   self.vosroot)
            
            logcopy = self.logfile
            logsuffix = ''
            if self.errors:
                logsuffix = '_ERRORS'
            if self.warnings:
                logsuffix += '_WARNINGS'
                
            if logsuffix:
                logid, ext = os.path.splitext(self.logfile)
                logcopy = logid + logsuffix + ext
                shutil.copy(self.logfile, logcopy)

            self.voscopy.match(logcopy)
            self.voscopy.push()

            if logsuffix:
                os.remove(logcopy)
        
        ingest2caom2.cleanup(self)

    def check_acceptable_headers(self, head):
        """
        Check that every header is from the "acceptable" list, as a guard
        against bad files.  Note that this does not check whether particular
        keywords are present, only that the existing keywords are acceptable.
        """
        if self.debug:
            self.log.console('enter check_acceptable_headers: ' + 
                             datetime.datetime.now().isoformat(),
                             loglevel=logging.DEBUG)
        acceptable = [
                      '',
                    'AGENTID',
                    'ALIGN_DX',
                    'ALIGN_DY',
                    'ALT-OBS',
                    'AMEND',
                    'AMSTART',
                    'ARRAYID',
                    'ASN_CADC',
                    'ASN_ID',
                    'ASN_PROJ',
                    'ASN_TYPE',
                    'ASN_UT',
                    'ATEND',
                    'ATSTART',
                    'AZEND',
                    'AZSTART',
                    'BACKEND',
                    'BANDWID',
                    'BASEC1',
                    'BASEC2',
                    'BASETEMP',
                    'BBHEAT',
                    'BEDEGFAC',
                    'BITPIX',
                    'BKLEGTEN',
                    'BKLEGTST',
                    'BLANK',
                    'BMAJ',
                    'BMIN',
                    'BOLODIST',
                    'BPA',
                    'BPEND',
                    'BPSTART',
                    'BSCALE',
                    'BUNIT',
                    'BWMODE',
                    'BZERO',
                    'CD1_1',
                    'CD1_1A',
                    'CD1_2',
                    'CD1_2A',
                    'CD2_1',
                    'CD2_1A',
                    'CD2_2',
                    'CD2_2A',
                    'CD3_3',
                    'CD3_3A',
                    'CD4_4',
                    'CDELT3',
                    'CHECKSUM',
                    'CHOP_CRD',
                    'CHOP_FRQ',
                    'CHOP_PA',
                    'CHOP_THR',
                    'COMMENT',
                    'CRPIX1',
                    'CRPIX1A',
                    'CRPIX2',
                    'CRPIX2A',
                    'CRPIX3',
                    'CRPIX3A',
                    'CRPIX4',
                    'CRVAL1',
                    'CRVAL1A',
                    'CRVAL2',
                    'CRVAL2A',
                    'CRVAL3',
                    'CRVAL3A',
                    'CRVAL4',
                    'CTYPE1',
                    'CTYPE1A',
                    'CTYPE2',
                    'CTYPE2A',
                    'CTYPE3',
                    'CTYPE3A',
                    'CUNIT1',
                    'CUNIT1A',
                    'CUNIT2',
                    'CUNIT2A',
                    'CUNIT3',
                    'CUNIT3A',
                    'DARKHEAT',
                    'DATAMODE',
                    'DATASUM',
                    'DATE',
                    'DATE-END',
                    'DATE-OBS',
                    'DAZ',
                    'DEL',
                    'DETBIAS',
                    'DHSVER',
                    'DOPPLER',
                    'DPDATE',
                    'DPRCINST',
                    'DRGROUP',
                    'DRMWGHTS',
                    'DRRECIPE',
                    'DUT1',
                    'EFF_TIME',
                    'ELEND',
                    'ELSTART',
                    'ENGVERS',
                    'EQUINOX',
                    'EQUINOXA',
                    'ETAL',
                    'EXP_TIME',
                    'EXTEND',
                    'EXTLEVEL',
                    'EXTNAME',
                    'EXTNAMEF',
                    'EXTSHAPE',
                    'EXTTYPE',
                    'EXTVER',
                    'FCF',
                    'FFT_WIN',
                    'FILE_ID',
                    'FILEID',
                    'FILEPATH',
                    'FILTER',
                    'FLAT',
                    'FOCAXIS',
                    'FOCPOSN',
                    'FOCSTEP',
                    'FOCUS_DZ',
                    'FREQ',
                    'FREQ_THR',
                    'FRLEGTEN',
                    'FRLEGTST',
                    'FRQIMGHI',
                    'FRQIMGLO',
                    'FRQSIGHI',
                    'FRQSIGLO',
                    'FTS_IN',
                    'FTS_MODE',
                    'FTS_SH8C',
                    'FTS_SH8D',
                    'FTS_CNTR',
                    'GCOUNT',
                    'HDSNAME',
                    'HDSTYPE',
                    'HDUCLAS1',
                    'HDUCLAS2',
                    'HISTORY',
                    'HSTEND',
                    'HSTSTART',
                    'HUMEND',
                    'HUMSTART',
                    'IFCHANSP',
                    'IFFREQ',
                    'IMAGFREQ',
                    'INBEAM',
                    'INSTAP',
                    'INSTAP_X',
                    'INSTAP_Y',
                    'INSTREAM',
                    'INSTRUME',
                    'INT_TIME',
                    'JIGL_CNT',
                    'JIGL_NAM',
                    'JIG_CRD',
                    'JIG_PA',
                    'JIG_SCAL',
                    'JOS_MIN',
                    'JOS_MULT',
                    'LABEL',
                    'LAT',
                    'LAT-OBS',
                    'LBOUND1',
                    'LBOUND2',
                    'LBOUND3',
                    'LOCL_CRD',
                    'LOFREQE',
                    'LOFREQS',
                    'LONG',
                    'LONG-OBS',
                    'LONGSTRN',
                    'LONPOLE',
                    'LSTEND',
                    'LSTSTART',
                    'MAP_HGHT',
                    'MAP_PA',
                    'MAP_WDTH',
                    'MAP_X',
                    'MAP_Y',
                    'MEDTSYS',
                    'MIRPOS',
                    'MIXSETP',
                    'MJD-AVG',
                    'MJD-END',
                    'MJD-OBS',
                    'MOLECULE',
                    'MSBID',
                    'MSBTID',
                    'MSROOT',
                    'MUXTEMP',
                    'NAXIS',
                    'NAXIS1',
                    'NAXIS1A',
                    'NAXIS2',
                    'NAXIS2A',
                    'NAXIS3',
                    'NAXIS3A',
                    'NAXIS4',
                    'NBOLOEFF',
                    'NCALSTEP',
                    'NCHNSUBS',
                    'NDRKSTEP',
                    'NFOCSTEP',
                    'NFREQSW',
                    'NREFSTEP',
                    'NSUBBAND',
                    'NSUBSCAN',
                    'NUMTILES',
                    'NUM_CYC',
                    'NUM_NODS',
                    'N_MIX',
                    'OBJECT',
                    'OBSCNT',
                    'OBSDEC',
                    'OBSDECBL',
                    'OBSDECBR',
                    'OBSDECTL',
                    'OBSDECTR',
                    'OBSEND',
                    'OBSGEO-X',
                    'OBSGEO-Y',
                    'OBSGEO-Z',
                    'OBSID',
                    'OBSIDSS',
                    'OBSNUM',
                    'OBSRA',
                    'OBSRABL',
                    'OBSRABR',
                    'OBSRATL',
                    'OBSRATR',
                    'OBS_SB',
                    'OBS_TYPE',
                    'OCSCFG',
                    'ORIGIN',
                    'PCOUNT',
                    'PIPEVERS',
                    'PIXHEAT',
                    'POLANLIN',
                    'POLCALIN',
                    'POLWAVIN',
                    'POL_CONN',
                    'POL_CRD',
                    'POL_FAXS',
                    'POL_MODE',
                    'PROCVERS',
                    'PRODID',
                    'PRODUCER',
                    'PRODUCT',
                    'PROJECT',
                    'PROJ_ID',
                    'PRVCNT',
                    'PV1_3',
                    'RADESYS',
                    'RADESYSA',
                    'RECIPE',
                    'RECPTORS',
                    'REFCHAN',
                    'REFERENC',
                    'REFRECEP',
                    'RESTFRQ',
                    'RESTFRQA',
                    'RMTAGENT',
                    'ROTAFREQ',
                    'ROT_CRD',
                    'ROT_PA',
                    'SAM_MODE',
                    'SB_MODE',
                    'SCANDIR',
                    'SCANVEL',
                    'SCAN_CRD',
                    'SCAN_DY',
                    'SCAN_PA',
                    'SCAN_PAT',
                    'SCAN_VEL',
                    'SCUPIXSZ',
                    'SCUPROJ',
                    'SEEDATEN',
                    'SEEDATST',
                    'SEEINGEN',
                    'SEEINGST',
                    'SEQCOUNT',
                    'SEQEND',
                    'SEQSTART',
                    'SEQ_TYPE',
                    'SHUTTER',
                    'SIMPLE',
                    'SIMULATE',
                    'SIM_CORR',
                    'SIM_FTS',
                    'SIM_IF',
                    'SIM_POL',
                    'SIM_RTS',
                    'SIM_SMU',
                    'SIM_TCS',
                    'SKYANG',
                    'SKYREFX',
                    'SKYREFY',
                    'SPECSYS',
                    'SPECSYSA',
                    'SREFISA',
                    'SREF1A',
                    'SREF2A',
                    'SSYSOBS',
                    'SSYSSRC',
                    'SSYSSRCA',
                    'STANDARD',
                    'STARTIDX',
                    'STATUS',
                    'STBETCAL',
                    'STBETDRK',
                    'STBETREF',
                    'STEPDIST',
                    'STEPTIME',
                    'SUBARRAY',
                    'SUBBANDS',
                    'SUBREFP1',
                    'SUBREFP2',
                    'SUBSYSNR',
                    'SURVEY',
                    'SW_MODE',
                    'SYSTEM',
                    'TAU225EN',
                    'TAU225ST',
                    'TAUDATEN',
                    'TAUDATST',
                    'TAUSRC',
                    'TELESCOP',
                    'TEMPSCAL',
                    'TFIELDS',
                    'TILENUM',
                    'TRACKSYS',
                    'TRANSITI',
                    'TSPEND',
                    'TSPSTART',
                    'UAZ',
                    'UEL',
                    'UTDATE',
                    'VELOSYS',
                    'VELOSYSA',
                    'VERSION',
                    'WAVELEN',
                    'WCSNAME',
                    'WCSNAMEA',
                    'WNDDIREN',
                    'WNDDIRST',
                    'WNDSPDEN',
                    'WNDSPDST',
                    'WVMDATEN',
                    'WVMDATST',
                    'WVMTAUEN',
                    'WVMTAUST',
                    'XTENSION',
                    'ZSOURCE',
                    'ZSOURCEA']
        headercount = {'PRV': 'PRVCNT',
                       'OBS': 'OBSCNT',
                        'FILE_': 'OBSCNT', # Arbitrary number, but seems OK
                        'TCOMM': 'TFIELDS',
                        'TDIM': 'TFIELDS',
                        'TDISP': 'TFIELDS',
                        'TFORM': 'TFIELDS',
                        'TNULL': 'TFIELDS',
                        'TTYPE': 'TFIELDS'
                        }

        someBAD = False
        for key in head:
            keywordBAD = True
            if key in acceptable:
                keywordBAD = False
            else:
                # check if the key is a numbered header
                 m = re.match(r'^(?P<prefix>[A-Z]+)(?P<number>[1-9][0-9]*)$',
                              key)
                 if m:
                     pn = m.groupdict()
                     if pn["prefix"] in headercount:
                         n = int(pn["number"])
                         if 0 < n and n <= head[headercount[pn["prefix"]]]:
                             keywordBAD = False
            if keywordBAD:
                someBAD = True
                self.warnings = True
                self.log.console('Unexpected keyword: ' + key,
                                 logging.WARN)

        return someBAD

#************************************************************************
# if run as a main program, create an instance and exit
#************************************************************************
if __name__ == '__main__':
    myjcmt2caom2 = jcmt2caom2.stdpipe()
    myjcmt2caom2.run()
