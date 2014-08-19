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
from tools4caom2.caom2repo_wrapper import Repository
from tools4caom2.timezone import UTC
from tools4caom2.mjd import utc2mjd
from tools4caom2.utdate_string import UTDATE_REGEX
from tools4caom2.vos2caom2 import vos2caom2

from jcmt2caom2.jsa.instrument_keywords import instrument_keywords
from jcmt2caom2.jsa.instrument_name import instrument_name
from jcmt2caom2.jsa.intent import intent
from jcmt2caom2.jsa.product_id import product_id
from jcmt2caom2.jsa.raw_product_id import raw_product_id
from jcmt2caom2.jsa.target_name import target_name

from jcmt2caom2 import tovos 

from jcmt2caom2.__version__ import version as jcmt2caom2version

# Utility functions
def is_defined(key, header):
    """
    return True if key is in header and has a defined value, False otherwise
    This is useful for optional headers whose absence is not an error, or for
    metadata with more complicated logic than is supported using the prepackaged
    tests in delayed_error_warn.  Use the error() or warn() methods from that 
    package to report errors and warnings that affect ingestion.
    """
    return (key in header and header[key] != pyfits.card.UNDEFINED)
        
def is_blank(key, header):
    """
    return True if key is in header and has a defined value, False otherwise
    This is useful for optional headers whose absence is not an error, or for
    metadata with more complicated logic than is supported using the prepackaged
    tests in delayed_error_warn.  Use the error() or warn() methods from that 
    package to report errors and warnings that affect ingestion.
    """
    return (key in header and header[key] == pyfits.card.UNDEFINED)

# from caom2.caom2_enums import CalibrationLevel
# from caom2.caom2_enums import DataProductType
class jcmtvos2caom2(vos2caom2):
    """
    A derived class of vos2caom2 specialized to ingest externally generated
    products into the JSA.
    """
    speedOfLight = 2.9979250e8 # Speed of light in m/s
    lambda_csotau = 225.0e9 # Frequency of CSO tau meter in Hz
    proc_acsis_regex = \
        r'jcmth(20[\d]{2})(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])_' +\
        r'([\d]{5})_(0[0-4])_(cube[\d]{3}|reduced[\d]{3}|rimg|rsp|rvel|' + \
        r'linteg[\d]{3}|sp[\d]{3}|std)_(obs|nit|pro|pub)_([\d]{3})$'
    proc_scuba2_regex = \
        r'jcmts(20[\d]{2})(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])_' +\
        r'([\d]{5})_([48]50)_(reduced[\d]{3})_(obs|nit|pro|pub)_([\d]{3})$'

    def __init__(self):
        vos2caom2.__init__(self)
        self.archive = 'JCMT'
        self.stream = 'product'
        
        self.voscopy = None
        self.vosroot = 'vos:jsaops'
        
        # These defaults are for CADC use, but can be overriden in userconfig.

        # The server and cred_db are used to get database credentials at the CADC.
        # Other sites should supply cadc_id, cadc_key in the section [cadc] of
        # the userconfig file.
        self.userconfigpath = '~/.tools4caom2/tools4caom2.config'
        if self.sybase_defined:
            if not self.userconfig.has_section('database'):
                self.userconfig.add_section('database')
            self.userconfig.set('database', 'server', 'SYBASE')
            self.userconfig.set('database', 'cred_db', 'jcmt')
            self.userconfig.set('database', 'read_db', 'jcmt')
            self.userconfig.set('database', 'write_db', 'jcmt')

            # Set the site-dependent databases containing necessary tables
            if not self.userconfig.has_section('jcmt'):
                self.userconfig.add_section('jcmt')
            self.userconfig.set('jcmt', 'caom_db', 'jcmt')
            self.userconfig.set('jcmt', 'jcmt_db', 'jcmtmd')
            self.userconfig.set('jcmt', 'omp_db', 'jcmtmd')
        
        # This is needed for compatability with other uses of vos2caom2, but
        # should not be used for the JCMT.
        self.database = 'jcmt'
        self.collection_choices = ['JCMT', 'JCMTLS', 'JCMTUSER', 'SANDBOX']

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
        self.input_cache = {}
        self.remove_dict = {}
        self.remove_id = []
        self.repository = None
        
    #************************************************************************
    # Process the custom command line switchs
    #************************************************************************
    def processCommandLineSwitches(self):
        """
        Process some JSA-specific args

        Arguments:
        <none>
        """
        vos2caom2.processCommandLineSwitches(self)

        self.collection = self.args.collection
        
        self.caom_db = ''
        self.jcmt_db = ''
        self.omp_db = ''
        
        if self.sybase_defined and self.userconfig.has_section('jcmt'):
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
        Log the values of the command line args.

        Arguments:
        <none>
        """
        vos2caom2.logCommandLineSwitches(self)
        self.log.file('jcmt2caom2version    = ' + jcmt2caom2version)

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
            tapcmd = '\n'.join([
                'SELECT',
                '    Observation.collection,',
                '    Observation.observationID,',
                '    Observation.productID,',
                '    Plane.provenance_runID',
                'FROM',
                '    caom2.Observation AS Observation',
                '        INNER JOIN caom2.Plane AS Plane',
                '            ON Observation.obsID=Plane.obsID',
                '        INNER JOIN caom2.Plane AS Plane2',
                '            ON observation.obsID=Plane2.obsID',
                'WHERE',
                '    Plane2.provenance_runID=' + "'" + run_id + "'",
                'ORDER BY Observation.collection, ',
                '         Observation.observationID, ',
                '         Plane.productID'])
            result = self.tap.query(tapcmd)
            if result:
                for coll, obsid, prodid, run in result:
                    this_runID = run
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
        self.log.console('enter build_dict: ' + 
                         datetime.datetime.now().isoformat(),
                         loglevel=logging.DEBUG)
        if self.repository is None:
            # note that this is similar to the repository in vos2caom2, but 
            # that is not made a part of the structure - probably should be 
            self.repository = Repository(self.outdir, 
                                         self.log, 
                                         debug=self.debug,
                                         backoff=[10.0, 20.0, 40.0, 80.0])

        if 'file_id' not in header:
            self.errors = True
            self.log.console('No file_id in ' + repr(header),
                             logging.ERROR)
        file_id = header['file_id']
        filename = header['filepath']
        self.dew.sizecheck(filename)

        self.log.file('Starting ' + file_id)
        # Doing all the required checks here simplifies the code
        # farther down and ensures error reporting of these basic problems
        # even if the ingestion fails before reaching the place where the
        # header would be used.

        someBAD = False

        # Check that mandatory file headers exist that validate the FITS
        # file structure
        structural = ('BITPIX',
                      'CHECKSUM',
                      'DATASUM')
        for key in structural:
            self.dew.expect_keyword(filename, key, header, mandatory=True)

        # Observation metadata
        if self.collection == 'SANDBOX':
            self.dew.restricted_value(filename, 
                                      'INSTREAM', header, 
                                      self.collection_choices)
        else:
            self.dew.restricted_value(filename, 
                                      'INSTREAM', header, (self.collection,))
        
        if self.dew.expect_keyword(filename,
                                   'ASN_ID', header, mandatory=True):
            self.observationID = header['ASN_ID']
            # TAP query to find if the value is in use
            tapcmd = '\n'.join([
                "SELECT Observation.collection AS count",
                "FROM caom2.Observation AS Observation",
                "WHERE Observation.observationID='" + self.observationID + "'"])
            results = self.tap.query(tapcmd)
            if results:
                if self.mode == 'new' and self.collection in results[0]:
                    self.dew.error(filename,
                                   'observationID = "' + self.observationID +
                                   '" must be unique in collection = "' +
                                   self.collection + '"')
                elif self.mode == 'replace' and self.collection not in results[0]:
                    self.dew.error(filename,
                                   'observationID = "' + self.observationID +
                                   '" must be already in collection = "' +
                                   self.collection + '"')
                for coll in results[0]:
                    if (self.mode not in ('new', 'replace') or 
                        coll != self.collection): 
                        
                        self.dew.warn(filename,
                                'observationID = "' + self.observationID +
                                '" is found in collection = "' +
                                self.collection + '"')

        # Observation.algorithm
        algorithm = 'custom'
        if is_defined('ASN_TYPE', header):
            algorithm = header['ASN_TYPE']
        self.add_to_plane_dict('algorithm.name', algorithm)

        # Optional Observation.proposal
        proposal_id = None
        proposal_project = None
        proposal_pi = None
        proposal_title = None
        survey_acronyms = ('CLS', 'DDS', 'GBS', 'JPS', 'NGS', 'SASSY', 'SLS')
        if (is_defined('SURVEY', header) and
            self.dew.restricted_value(filename, 'SURVEY', header,
                                       survey_acronyms)):
            proposal_project = header['SURVEY']
        
        if is_defined('PROJECT', header):
            proposal_id = header['PROJECT']
            self.add_to_plane_dict('proposal_id', proposal_id)

            if proposal_project:
                self.add_to_plane_dict('proposal.project', proposal_project)

            if is_defined('PI', header):
                proposal_pi = header['PI']
                self.add_to_plane_dict('proposal.pi', proposal_pi)
                
            if is_defined('title', header):
                proposal_title = header['TITLE']
                self.add_to_plane_dict('proposal.title', proposal_title)
            
            if not (proposal_pi and proposal_title) and self.omp_db:
                sqlcmd = '\n'.join([
                    'SELECT ',
                    '    ou.uname,',
                    '    op.title',
                    'FROM ' + self.omp_db + 'ompproj op',
                    '    LEFT JOIN ' + self.omp_db + 'ompuser ou'
                    '        ON op.pi=ou.userid',
                    'WHERE op.projectid="%s"' % (header['PROJECT'],)])
                answer = self.conn.read(sqlcmd)

                if len(answer):
                    self.add_to_plane_dict('proposal.pi',
                                           answer[0][0])
                    self.add_to_plane_dict('proposal.title',
                                           answer[0][1])
        
        # Observation membership headers, which are optional
        earliest_utdate = None
        earliest_obs = None
        obstimes = {}

        if is_defined('MBRCNT', header):
            # Define membership using OBS headers
            mbrcnt = int(header['MBRCNT'])
            if mbrcnt > 0:
                for n in range(mbrcnt):
                    mbrkey = 'MBR' + str(n+1)
                    if self.dew.expect_keyword(filename, mbrkey, header):
                        obsn = header[obsn]
                    
                    if obsn in self.member_cache:
                        obsid, date_obs, date_end = self.member_cache[obsn]
                        self.log.file('fetch member metadata from cache '
                                      'for ' + obsn,
                                      logging.DEBUG)
                    else:
                        tapcmd = '\n'.join([
                            "SELECT Plane.productID, ",
                            "       Plane.time_bounds_cval1,",
                            "       Plane.time_bounds_cval2",
                            "FROM caom2.Observation as Observation",
                            "         INNER JOIN caom2.Plane AS Plane",
                            "             ON Observation.obsID=Plane.obsID",
                            "WHERE Observation.observationID='" + 
                            member + "''"
                            ])
                        results = self.tap.query(tapcmd)
                        if len(results):
                            (obsid, date_obs, date_end) = results[0]
                            # cache the membership metadata
                            self.member_cache[obsn] = \
                                (obsid, date_obs, date_end)
                            self.log.file('cache member metadata '
                                          'for ' + obsn,
                                          logging.DEBUG)

                    if obsid:
                        # record the time interval
                        if (obsid == self.observationID
                            and obsid not in obstimes):
                                obstimes[obsid] = (date_obs, date_end)
                        
        elif is_defined('OBSCNT', header) and self.conn.available():
            # Define membership using OBS headers
            # This REQUIRES database access because there is no other
            # reliable way to translate obsid_subsysnr into obsid than the 
            # JCMT-supplied tables.
            obscnt = int(header['OBSCNT'])
            if obscnt > 0:
                for n in range(obscnt):
                    obsn = 'OBS' + str(n+1)
                    self.dew.expect_keyword(filename, obsn, header)

            # verify membership headers are real observations
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
                        obsid, date_obs, date_end = \
                            self.member_cache[obsn]
                        self.log.file('fetch member metadata from cache '
                                      'for ' + obsn,
                                      logging.DEBUG)
                    else:
                        sqlcmd = '\n'.join([
                            'SELECT distinct f.obsid,',
                            '       c.date_obs,',
                            '       c.date_end',
                            'FROM ' + self.jcmt_db + 'FILES f',
                            '    INNER JOIN ' + self.jcmt_db + 'COMMON c',
                            '        ON f.obsid=c.obsid',
                            'WHERE f.obsid_subsysnr = "%s"' % (obsn,)])
                        result = self.conn.read(sqlcmd)
                        if len(result):
                            obsid, date_obs, date_end = \
                                result[0]
                            
                            if date_obs:
                                if (earliest_utdate is None or 
                                    date_obs < earliest_utdate):

                                    earliest_utdate = date_obs
                                    earliest_obs = obsid
                                    
                            # cache the membership metadata
                            self.member_cache[obsn] = \
                                (obsid, date_obs, date_end)
                            self.log.file('cache member metadata '
                                          'for ' + obsn,
                                          logging.DEBUG)
                                
                            # Also cache the productID's for each file
                            fdict = raw_product_id(backend,
                                                   'prod',
                                                   obsid,
                                                   self.conn,
                                                   self.log)
                            self.input_cache.update(fdict)
                            self.log.file('cache provenance metadata '
                                          'for ' + obsn,
                                          logging.DEBUG)

                        else:
                            self.dew.warning(filename,
                                             'Member key ' + obsn + ' is '
                                             'not in jcmtmd.dbo.FILES')
                        
                    if obsid:
                        # record the time interval
                        if ((algorithm != 'exposure'
                             or obsid == self.observationID)
                            and obsid not in obstimes):
                                obstimes[obsid] = (date_obs, date_end)

        # Add to membership
        for obsid in obstimes:
            obsnURI = self.observationURI(self.collection, obsid)
            
        # Calculate the observation type from OBS_TYPE and SAM_MODE,
        # if they are unambiguous.
        obs_type = None
        if is_defined('OBS_TYPE', header):
            if obs_type in ('flatfield', 'noise', 'setup', 'skydip'):
                self.dew.error(filename,
                               'observation types in (flatfield, noise, setup, '
                               'skydip) contain no astronomical data and cannot '
                               'be ingested')

            raw_obs_type = header['OBS_TYPE'].strip()
            obs_type = raw_obs_type
            if is_defined('SAM_MODE', header) and raw_obs_type == "science":
                if header["SAM_MODE"] == "raster":
                    obs_type = 'scan'
                else:
                    obs_type = header['SAM_MODE'].strip()
            self.add_to_plane_dict('OBSTYPE', obs_type)
        
        # Record the instrument configuration if it is unambiguous.
        # It is possible in principle to combine data from multiple backends 
        # (e.g. DAS and ACSIS for spectra and datacubes, or SCUBA-2 - ACSIS 
        # for line-corrected continuum images), so unambiguous instrument
        # metadata are not mandatory.
         
        # We need the backend and instrument to check various modes, so try to 
        # identify them now
        instrument_fullname = None
        instrument = None
        backend = None
        inbeam = None
        if is_defined('INSTNAME', header):
            instrument_fullname = header['INSTNAME'].strip().upper()
            if re.match(r'.*?SCUBA-2', instrument_fullname):
                instrument = 'SCUBA-2'
                backend = 'SCUBA-2'
            else:
                components = instrument_fullname.split('-')
                instrument = components[-2]
                backend = components[-1]
        else:
            # Try to define instrument_fullname from INSTRUME, INBEAM and BACKEND
            if is_defined('INSTRUME', header):
                instrument = header['INSTRUME'].strip().upper()
            if is_defined('INBEAM', header):
                inbeam = header['INBEAM'].strip().upper()
            if self.dew.restricted_value(filename,
                                         'BACKEND', header,
                                         ('SCUBA-2', 'ACSIS', 'DAS', 'AOSC')):
                backend = header['BACKEND'].strip().upper()

            instrument_fullname = instrument_name(instrument,
                                                  backend,
                                                  inbeam,
                                                  self.log)

        if instrument_fullname:
            self.add_to_plane_dict('instrument.name', instrument_fullname)

        # Only do these tests if the backend is OK
        if backend in ('ACSIS', 'DAS', 'AOS-C'):
            if inbeam and inbeam != 'POL':
                self.dew.error(filename, 'INBEAM can only be blank or POL '
                                         'for heterodyne observations')

            if is_defined('OBS_TYPE', header):
                self.dew.restricted_value(filename, 'OBS_TYPE', header,
                        ['pointing', 'science', 'focus', 'skydip'])

            if is_defined('SAM_MODE', header):
                self.dew.restricted_value(filename, 'SAM_MODE', header,
                        ['jiggle', 'grid', 'raster', 'scan'])

        elif backend == 'SCUBA-2':
            if is_defined('OBS_TYPE', header):
                self.dew.restricted_value(filename, 'OBS_TYPE', header,
                        ['pointing', 'science', 'focus', 'skydip',
                         'flatfield', 'setup', 'noise'])

            if is_defined('SAM_MODE', header):
                self.dew.restricted_value(filename, 'SAM_MODE', header,
                        ['scan', 'stare'])
            
        # Check some more detailed values by building instrument_keywords
        keyword_dict = {}
        if is_defined('SW_MODE', header):
            keyword_dict['switching_mode'] = header['SW_MODE']

        if inbeam:
            keyword_dict['inbeam'] = inbeam
        
        if is_defined('SCAN_PAT', header):
            keyword_dict['x_scan_pat'] = header['SCAN_PAT']
        
        if backend in ('ACSIS', 'DAS', 'AOS-C'):
            if is_defined('OBS_SB', header):
                keyword_dict['sideband'] = header['OBS_SB']
            
            if is_defined('SB_MODE', header):
                keyword_dict['sideband_filter'] = header['SB_MODE']
        
        
        thisBad, keyword_list = instrument_keywords('stdpipe',
                                                    instrument,
                                                    backend,
                                                    keyword_dict,
                                                    self.log)
        self.instrument_keywords = ''
        if thisBad:
            self.dew.error(filename, 
                           'instrument_keywords could not be '
                           'constructed from ' + repr(keyword_dict))
        else:
            self.instrument_keywords = ' '.join(keyword_list)
            self.add_to_plane_dict('instrument.keywords',
                                   self.instrument_keywords)

        # Telescope metadata. geolocation is optional.
        self.dew.restricted_value(filename, 'TELESCOP', header, ['JCMT'])
        
        # Target metadata
        if self.dew.expect_keyword(filename, 'OBJECT', header):
            self.add_to_plane_dict('target.name', header['OBJECT'])
        
        if backend != 'SCUBA-2' and is_defined('ZSOURCE', header):
                self.add_to_plane_dict('target.redshift',
                                       str(header['ZSOURCE']))

        target_type = None
        if is_defined('TARGTYPE', header):
            if self.dew.restricted_value(filename, 
                                         'TARGTYPE', header, ['FIELD', 'OBJECT']):
                target_type = header['TARGTYPE']
        
        standard_target = 'FALSE'
        if is_defined('STANDARD', header):
            standard_target = 'TRUE'
        self.add_to_plane_dict('STANDARD', standard_target)
        
        # Distinguish moving targets
        moving = 'FALSE'
        if (is_blank('MOVING', header) or 
            is_blank('OBSRA', header) or
            is_blank('OBSDEC', header)):
            moving = 'TRUE'
        self.add_to_plane_dict('target.moving', 'TRUE')

        if (moving == 'TRUE'and 
            header['CTYPE1'][0:4] == 'OFLN' and
            'CTYPE1A' in header):
                # Use the first alternate coordinate system
                self.config = os.path.join(self.configpath, 
                                           'jcmt_stdpipe_a.config')
                self.default = os.path.join(self.configpath, 
                                           'jcmt_stdpipe_a.default')
        elif is_defined('OBSRA', header) and is_defined('OBSDEC', header):
            # Record the nominal target position
            self.add_to_plane_dict('target_position.cval1',
                                   str(header['OBSRA']))
            self.add_to_plane_dict('target_position.cval2',
                                   str(header['OBSDEC']))
            self.add_to_plane_dict('target_position.radesys',
                                   'ICRS')
            self.add_to_plane_dict('target_position.equinox',
                                   '2000.0')
        intent_val = None
        if obs_type and backend:
            intent_val = intent(obs_type, backend).value
            self.add_to_plane_dict('obs.intent', intent_val)

        # Plane metadata
        # metadata needed to create productID
        product = None
        if self.dew.expect_keyword(filename, 'PRODUCT', header, mandatory=True):
            product = header['PRODUCT']
        
        self.productID = None
        filter = None
        restfreq = None
        subsysnr = None
        bwmode = None
        science_product = None
        if is_defined('PRODID', header):
            self.productID = header['PRODID']
        else:
            # Try to build the productID with the standard algorithm
            if backend == 'SCUBA-2':
                if self.dew.expect_keyword(filename, 'FILTER', header):
                    filter = str(header['FILTER'])
                    self.productID = \
                        product_id('SCUBA-2', 
                                    self.log,
                                    product=product,
                                    filter=filter)
            else:
                # ACSIS-like files must define either PRODID or 
                # the SUBSYSNR, RESTFRQ and BWMODE
                if self.dew.expect_keyword(filename, 'RESTFRQ', header):
                    restfreq = float(header['RESTFRQ'])
                if self.dew.expect_keyword(filename, 'SUBSYSNR', header):
                    subsysnr = str(header['SUBSYSNR'])
                if self.dew.expect_keyword(filename, 'BWMODE', header):
                    bwmode = header['BWMODE']
                if product in ['reduced', 'rimg', 'rsp']:
                    self.productID = \
                        product_id(backend, 
                                   self.log,
                                   product='reduced',
                                   restfreq=restfreq,
                                   bwmode=bwmode,
                                   subsysnr=subsysnr)
                elif product in ['healpix', 'hpxrimg', 'hpxrsp']:
                    self.productID = \
                        product_id(backend, 
                                   self.log,
                                   product='healpix',
                                   restfreq=restfreq,
                                   bwmode=bwmode,
                                   subsysnr=subsysnr)
                elif product and restfreq and bwmode and subsysnr:
                    self.productID = \
                        product_id(backend, 
                                   self.log,
                                   product=product,
                                   restfreq=restfreq,
                                   bwmode=bwmode,
                                   subsysnr=subsysnr)
        if self.productID:
            if re.search(r'-', self.productID):
                science_product = self.productID.split('-')[0]
            else:
                science_product = self.productID
        else:
            self.dew.error(filename, 'productID could not be determined')

        calibrationLevel = None
        if is_defined('CALLEVEL', header):
            if header['CALLEVEL'] == 'CALIBRATED':
                calibrationLevel = str(CalibrationLevel.CALIBRATED.value)
            elif header['CALLEVEL'] == 'PRODUCT':
                calibrationLevel = str(CalibrationLevel.PRODUCT.value)
        else:
            calibrationLevel = str(CalibrationLevel.CALIBRATED.value)
            if product in ('point-cat', 'extent-cat', 'peak-cat'):
                calibrationLevel = str(CalibrationLevel.PRODUCT.value)
        if calibrationLevel:
            self.add_to_plane_dict('plane.calibrationLevel', calibrationLevel)

        # Check for existence of provenance input headers, which are optional
        self.log.file('Reading provenance')
        prvprocset = set()
        prvrawset = set()
        self.log.file('input_cache: ' + repr(self.input_cache),
                      logging.DEBUG)
        
        if is_defined('PRVCNT', header):
            # Translate the PRV1..PRV<PRVCNT> headers into plane URIs
            prvcnt = int(header['PRVCNT'])
            if product and product == science_product and prvcnt > 0:
                self.log.file('PRVCNT = ' + str(prvcnt))
                for i in range(prvcnt):
                    # Verify that files in provenance are being ingested
                    # or have already been ingested.
                    prvkey = 'PRV' + str(i + 1)
                    prvn = header[prvkey]
                    self.dew.expect_keyword(filename, prvn, header)
                    self.log.file(prvkey + ' = ' + prvn,
                                  logging.DEBUG)
                    if (re.match(jcmtvos2caom2.proc_acsis_regex, prvn) or
                        re.match(jcmtvos2caom2.proc_scuba2_regex, prvn)):
                        # Does this look like a processed file?
                        # An existing problem is that some files include 
                        # themselves in their provenance, but are otherwise
                        # OK.
                        if prvn == file_id:
                            # add a warning and skip this entry
                            self.dew.warning(filename,
                                'file_id = ' + file_id + ' includes itself '
                                'in its provenance as ' + prvkey)
                            continue
                        prvprocset.add(prvn)

                    elif (re.match(stdpipe.raw_acsis_regex, prvn) or
                          re.match(stdpipe.raw_scuba2_regex, prvn)):
                        # Does it look like a raw file?
                        # Add the file if this is NOT an exposure,
                        # or if it is and the file is part of the same exposure
                        if prvn in self.input_cache:
                            prv_obsID, prv_prodID = self.input_cache[prvn]
                            self.log.file('fetch provenance metadata '
                                          'from cache for ' + prvn,
                                          logging.DEBUG)
                            if (algorithm == 'exposure' and 
                                prv_obsID != self.observationID):
                                continue
                            else:
                                self.dew.error(filename, 
                                               'provenance and membership '
                                               'header lists inconsistent '
                                               'raw data:' +
                                               prvn + ' is not in ' +
                                               'input_cache constructed '
                                               'from membership')
                            
                        prvrawset.add(prvn)

                    else:
                        # There are many files with bad provenance.
                        # This should be an error, but it is prudent
                        # to report it as a warning until all of the
                        # otherwise valid recipes have been fixed.
                        self.dew.warning(filename,
                                         prvkey + ' = ' + prvn + ' is '
                                         'neither processed nor raw',
                                         logging.WARN)

        elif is_defined('INPCNT', header):
            # Copy the INP1..INP<PRVCNT> headers as plane URIs
            inpcnt = int(header['INPCNT'])
            if product and product == science_product and inpcnt > 0:
                for n in range(inpcnt):
                    inpkey = 'INP' + str(i + 1)
                    inpn = header[inpkey]
                    self.dew.expect_keyword(filename, inpn, header)
                    self.log.file(inpkey + ' = ' + inpn,
                                  logging.DEBUG)
                    inpprocset.add(inpn)

        # Report the earliest UTDATE
        if earliest_utdate:
            rcinstprefix = 'caom-' + self.collection + '-' + earliest_obs
            self.log.file('Earliest utdate: ' + 
                          earliest_utdate.date().isoformat() +
                          ' for ' + rcinstprefix +
                          '_vlink-' + dprcinst)

        dataProductType = None
        if is_defined('DATAPROD', header):
            if not self.dew.restricted_value(filename, 'DATAPROD', header,
                                    ("image", "spectrum", "cube" and "catalog")):
                dataProductType = None
        elif product == science_product:
            # Assume these are standard pipeline products
            # Axes are always in the order X, Y, Freq, Pol
            # but may be degenerate with length 1.  Only compute the 
            # dataProductType for science data.
            if product in ['reduced', 'cube', 'healpix']:
                if (header['NAXIS'] == 3 or
                    (header['NAXIS'] == 4 and header['NAXIS4'] == 1)):
                    if (header['NAXIS1'] == 1 and 
                        header['NAXIS2'] == 1):
                        dataProductType = 'spectrum'
                    elif header['NAXIS3'] == 1:
                        dataProductType = 'image'
                    else:
                        dataProductType = 'cube'
                elif product in ('peak-cat', 'extent-cat', 'point-cat'):
                    dataProductType = 'catalog'
        if dataProductType:
            self.add_to_plane_dict('plane.dataProductType', dataProductType)

        # Provenance_name
        if self.dew.expect_keyword(filename, 'RECIPE', header):
            self.add_to_plane_dict('provenance.name', header['RECIPE'])
        
        # Provenance_project
        dpproject = None
        if is_defined('DPPROJ', header):
            dpproject = header['DPPROJ'].strip()
            self.add_to_plane_dict('provenance.name', dpproject)
        elif self.collection == 'JCMTLS' and proposal_project:
            dpproject = proposal_project
            self.add_to_plane_dict('provenance.name', proposal_project)
        else:
            self.dew.error(filename,
                           'data processing project name is undefined: '
                           'either DPPROJ or SURVEY must be defined')
       
        # Provenance_reference - likely to be overwritten
        if is_defined('REFERENC', header):
            self.add_to_plane_dict('provenance.reference',
                                   header['REFERENC'])

        # ENGVERS and PIPEVERS are optional
        if is_defined('PROCVERS', header):
            self.add_to_plane_dict('provenance.version',
                                   header['PROCVERS'])
        else:
            if is_defined('ENGVERS', header) and is_defined('PIPEVERS', header):
                self.add_to_plane_dict('provenance.version',
                                       'ENG:' + header['ENGVERS'][:25] + 
                                       ' PIPE:' + header['PIPEVERS'][:25])
                
        if is_defined('PRODUCER', header):
            self.add_to_plane_dict('provenance.producer',
                                   header['PRODUCER'])

        if self.dew.expect_keyword(filename, 'DPRCINST', header, mandatory=True):
            # DPRCINST is filled with the vos URI of the minor release directory
            dprcinst = header['DPRCINST']
            self.add_to_plane_dict('provenance.runID', dprcinst)

        if self.dew.expect_keyword(filename, 'DPDATE', header, mandatory=True):
            # DPDATE is a characteristic datetime when the data was processed
            dpdate = header['DPDATE']
            if isinstance(dpdate, datetime.datetime):
                dpdate = header['DPDATE'].isoformat()
            self.add_to_plane_dict('provenance.runID', dpdate)

        # Chunk
        bandpassName = None
        if backend == 'SCUBA-2' and filter:
            bandpassname = 'SCUBA-2-' + filter + 'um'
            self.add_to_plane_dict('bandpassName', bandpassName)
        elif backend in ('ACSIS', 'DAS', 'AOSC'):
            if (is_defined('MOLECULE', header) and is_defined('TRANSITI', header)
                and header['MOLECULE'] != 'No Line'):
                self.add_to_plane_dict('energy.transition.species',
                                       header['MOLECULE'])
                self.add_to_plane_dict('energy.transition.transition',
                                       header['TRANSITI'])

        if product == science_product:
            primaryURI = self.fitsextensionURI(self.archive,
                                               file_id,
                                               [0])
            self.add_to_fitsuri_dict(primaryURI,
                                     'part.productType', 
                                     ProductType.SCIENCE.value)
            if is_defined('PIPEVERS', header):
                # if PIPEVERS is defined, assume Starlink software was used 
                # to generate normal Strlink products
                varianceURI = self.fitsextensionURI(self.archive,
                                                   file_id,
                                                   [1])
                self.add_to_fitsuri_dict(varianceURI,
                                         'part.productType',
                                         ProductType.NOISE.value)

            fileURI = self.fitsfileURI(self.archive,
                                       file_id)
            self.add_to_fitsuri_dict(fileURI,
                                     'part.productType',
                                     ProductType.AUXILIARY.value)
        else:
            fileURI = self.fitsfileURI(self.archive,
                                       file_id)
            self.add_to_fitsuri_dict(fileURI,
                                     'artifact.productType',
                                     ProductType.AUXILIARY.value)

        if product == science_product:
            # Record times for science products
            for key in sorted(obstimes, key=lambda t: t[1][0]):
                self.add_to_fitsuri_custom_dict(fileURI,
                                                key,
                                                obstimes[key])


    def checkProvenanceInputs(self):
        """
        These are "gifted" from build_dict and seriously need to be reworked 
        """
        if True:
            pass
        else:
            if prvprocset:
                for prvn in prvprocset:
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
                    # prvn only added to prvrawset if it is in input_cache
                    prv_obsID, prv_prodID = self.input_cache[prvn]
                    self.log.console('collection='+repr(self.collection) +
                                     '  prv_obsID=' + repr(prv_obsID) +  #####
                                     '  prv_prodID=' + repr(prv_prodID),
                                     logging.DEBUG)  
                    self.planeURI(self.collection,
                                  prv_obsID,
                                  prv_prodID)


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
        
        vos2caom2.cleanup(self)


#************************************************************************
# if run as a main program, create an instance and exit
#************************************************************************
if __name__ == '__main__':
    myjcmtvos2caom2 = jcmtvos2caom2.jcmtvos2caom2()
    myjcmt2caom2.run()
