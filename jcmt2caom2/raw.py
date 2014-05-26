#!/usr/bin/env python2.7

__author__ = "Russell O. Redman"

import argparse
from ConfigParser import SafeConfigParser
import errno
import logging
import math
import os.path
import re
import sys
import traceback

from caom2.xml.caom2_observation_reader import ObservationReader
from caom2.xml.caom2_observation_writer import ObservationWriter

from caom2.caom2_simple_observation import SimpleObservation
from caom2.caom2_enums import CalibrationLevel
from caom2.caom2_enums import ObservationIntentType
from caom2.caom2_energy_transition import EnergyTransition
from caom2.caom2_environment import Environment
from caom2.caom2_instrument import Instrument
from caom2.caom2_proposal import Proposal
from caom2.caom2_target import Target
from caom2.caom2_target_position import TargetPosition
from caom2.caom2_telescope import Telescope
from caom2.caom2_observation_uri import ObservationURI
from caom2.caom2_plane import Plane
from caom2.caom2_artifact import Artifact
from caom2.caom2_part import Part
from caom2.caom2_chunk import Chunk
from caom2.types.caom2_point import Point
from caom2.wcs.caom2_axis import Axis
from caom2.wcs.caom2_spatial_wcs import SpatialWCS
from caom2.wcs.caom2_coord_axis2d import CoordAxis2D
from caom2.wcs.caom2_dimension2d import Dimension2D
from caom2.wcs.caom2_coord_polygon2d import CoordPolygon2D
from caom2.wcs.caom2_value_coord2d import ValueCoord2D
from caom2.wcs.caom2_ref_coord import RefCoord
from caom2.wcs.caom2_spectral_wcs import SpectralWCS
from caom2.wcs.caom2_coord_axis1d import CoordAxis1D
from caom2.wcs.caom2_coord_error import CoordError
from caom2.wcs.caom2_coord_bounds1d import CoordBounds1D
from caom2.wcs.caom2_coord_circle2d import CoordCircle2D
from caom2.wcs.caom2_coord_range1d import CoordRange1D
from caom2.wcs.caom2_coord_range2d import CoordRange2D
from caom2.wcs.caom2_temporal_wcs import TemporalWCS
from caom2.caom2_enums import ProductType

from tools4caom2.database import database
from tools4caom2.database import connection
from tools4caom2.caom2repo_wrapper import Repository
from tools4caom2.mjd import utc2mjd
from tools4caom2.logger import logger
import tools4caom2.__version__

from jcmt2caom2.jsa.quality import JCMT_QA
from jcmt2caom2.jsa.quality import JSA_QA
from jcmt2caom2.jsa.quality import quality

from jcmt2caom2.jsa.intent import intent
from jcmt2caom2.jsa.target_name import target_name
from jcmt2caom2.jsa.instrument_keywords import instrument_keywords
from jcmt2caom2.jsa.instrument_name import instrument_name
from jcmt2caom2.jsa.raw_product_id import raw_product_id
from jcmt2caom2.jsa.twod import TwoD
from jcmt2caom2.jsa.threed import ThreeD
from jcmt2caom2.jsa.utdate_string import utdate_string

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version

__doc__ = """
The raw class immplements methods to collect metadata from the database
to construct a caom2 observation.  Once completed, the observation is
serialized to a temporary xml file in outdir and push to the CAOM-2
repository.

This routine requires read access to the database, but does only reads.
It therefore always reads the metadata from SYBASE.
"""

class INGESTIBILITY(object):
    """
    Defines ingestion constants
    """
    GOOD = 0
    BAD = 1
    JUNK = 2
    
class raw(object):
    """
    Use pyCAOM2 to ingest raw JCMT raw data for a single observation using
    metadata from the COMMON, ACSIS, SCUBA2 and FILES tables in
    self.database + '.' + self.schema on SYBASE.

    This class requires direct access to the copies of these tables at the CADC.
    Only read access is required inside this routine to gather the metadata and
    create the CAOM-2 xml file for the observation.  The module
    pytools4caom2.tools4caom2.database is used to query the tables.

    The resulting xml file will be pushed back to the CAOM-2 repository to
    complete the put/update, and this must be separately configured.
    """

    # Allowed values for backend names in ACSIS
    BACKENDS = ['ACSIS', 'SCUBA-2', 'DAS', 'AOSC']

    # Fields to extract from COMMON
    COMMON = ('atstart',
              'backend',
              'date_end',
              'date_obs',
              'elstart',
              'humstart',
              'inbeam',
              'instrume',
              'object',
              'obsdec',
              'obsdecbl',
              'obsdecbr',
              'obsdectl',
              'obsdectr',
              'obsid',
              'obsgeo_x',
              'obsgeo_y',
              'obsgeo_z',
              'obsnum',
              'obsra',
              'obsrabl',
              'obsrabr',
              'obsratl',
              'obsratr',
              'obs_type',
              'project',
              'release_date',
              'sam_mode',
              'scan_pat',
              'seeingst',
              'standard',
              'survey',
              'sw_mode',
              'tau225st')

    # Fields to extract from ACSIS
    ACSIS = ('bwmode',
             'freq_sig_lower',
             'freq_sig_upper',
             'freq_img_lower',
             'freq_img_upper',
             'ifchansp',
             'obsid_subsysnr',
             'molecule',
             'obs_sb',
             'restfreq',
             'iffreq',
             'ifchansp',
             'sb_mode',
             'ssysobs',
             'ssyssrc',
             'subsysnr',
             'transiti',
             'zsource')

    # Fields to extract from SCUBA2
    SCUBA2 = ('obsid_subsysnr',
              'filter',
              'wavelen',
              'bandwid')

    MANDATORY = ('backend',
                 'obsgeo_x',
                 'obsgeo_y',
                 'obsgeo_z',
                 'obs_type',
                 'project',
                 'release_date',
                 'sam_mode',
                 'sw_mode')

    SpeedOfLight = 299792458.0 # m/s

    def __init__(self,
                 outdir='./'):
        """
        Create a jcmt2caom2.raw instance to ingest a single observation.

        Arguments:
        outdir:      working directory for output files
        """
        self.outdir = os.path.abspath(
                          os.path.expanduser(
                              os.path.expandvars(outdir)))
        self.server = 'SYBASE'
        self.database = None
        self.schema = None
        
        self.collection = None
        
        self.checkmode = None

        self.logdir = ''
        self.logfile = ''
        self.loglevel = logging.INFO
        self.log = None
        
        # Defaults are correct for CADC, but can be overriden in userconfig.
        # Other site should also supply cred_id, cred_key.
        self.userconfig = {'server': 'SYBASE',
                           'cred_db': 'jcmt',
                           'caom_db': 'jcmt',
                           'jcmt_db': 'jcmtmd',
                           'omp_db': 'jcmtmd',
                           'collection': 'JCMT'}
        
        self.userconfigpath = '~/.tools4caom2/jcmt2caom2.config'

        self.reader = ObservationReader(True)
        self.writer = ObservationWriter()
        self.conn = None

    def parse_command_line(self):
        """
        Parse command line arguments

        Arguments:
        <None>
        """
        ap = argparse.ArgumentParser()
        ap.add_argument('--userconfig',
                        default=self.userconfigpath,
                        help='Optional user configuration file '
                        '(default=' + self.userconfigpath + ')')
        ap.add_argument('--key',
            required=True,
            help='obsid, primary key in COMMON table')
        ap.add_argument('--outdir',
            help='working directory for output files')

        ap.add_argument('--server',
            default='SYBASE',
            help='logical name of Sybase server')
        ap.add_argument('--database',
            help='database containing COMMON, ACSIS, SCUVBA2 and FILES tables')
        ap.add_argument('--schema',
            default='dbo',
            help='database schema to use')

        ap.add_argument('--collection',
            choices=('JCMT', 'SANDBOX'),
            help='collection to use for ingestion')

        ap.add_argument('-c', '--check',
            action='store_true',
            dest='checkmode',
            help='Check the validity of metadata for this'
                 ' observation and file, then exit')
        ap.add_argument('--logdir',
            help='path to log file directory')
        ap.add_argument('--log',
            help='path to log file')
        ap.add_argument('-d', '--debug',
            dest='loglevel',
            action='store_const',
            const=logging.DEBUG)
        args = ap.parse_args()

        if os.path.isfile(self.userconfigpath):
            config_parser = SafeConfigParser()
            with open(userconfigpath) as UC:
                config_parser.readfp(UC)
        
            if config_parser.has_section('database'):
                for option in config_parser.options('database'):
                    self.userconfig[option] = config_parser.get('database', 
                                                                option)

        if args.server:
            self.userconfig['server'] = args.server
            self.server = args.server
        else:
            self.server = self.userconfig['server']
        
        if args.database:
            self.userconfig['jcmt_db'] = args.database
            self.userconfig['omp_db'] = args.database
            self.database = args.database
        elif 'jcmt_db' in self.userconfig:
            self.database = self.userconfig['jcmt_db']

        if args.collection:
            self.userconfig['collection'] = args.collection
            self.collection = args.collection
        else:
            self.collection = self.userconfig['collection']

        self.obsid = args.key
        self.schema = args.schema
        
        self.caom_db = self.userconfig['caom_db'] + '.' + self.schema + '.'
        self.jcmt_db = self.userconfig['jcmt_db'] + '.' + self.schema + '.'
        self.omp_db = self.userconfig['omp_db'] + '.' + self.schema + '.'
        
        if args.outdir:
            self.outdir = os.path.abspath(
                              os.path.expanduser(
                                  os.path.expandvars(args.outdir)))

        if args.logdir:
            self.logdir = os.path.abspath(
                               os.path.expanduser(
                                   os.path.expandvars(args.logdir)))
        
        if args.log:
            self.logfile = args.log

        if args.loglevel:
            self.loglevel = args.loglevel

        self.checkmode = args.checkmode

    def setup_logger(self):
        """
        Configure the logger

        Arguments:
        logfile:     log file name
        loglevel:    logging level for messages
        """
        if self.logfile:
            self.logfile = os.path.abspath(
                               os.path.expanduser(
                                   os.path.expandvars(self.logfile)))
        else:
            defaultlogname = '_'.join(['caom',
                                       self.collection,
                                       self.obsid,
                                       utdate_string()]) + '.log'
            if self.logdir:
                if not os.path.isdir(self.logdir):
                    raise RuntimeError('logdir = ' + self.logdir +
                                       ' is not a directory')
                defaultlogname = os.path.join(self.logdir, defaultlogname)
            else:
                defaultlogname = os.path.join(self.outdir, defaultlogname)
            self.logfile = os.path.abspath(
                               os.path.expanduser(
                                   os.path.expandvars(defaultlogname)))

    def logCommandLineSwitches(self):
        """
        Log the internal configuration.

        Arguments:
        <None>
        """
        self.log.file('jcmt2caom2version    = ' + jcmt2caom2version)
        self.log.file('tools4caom2version   = ' + tools4caom2version)
        for line in ['obsid = ' + self.obsid,
                     'server = ' + self.server,
                     'database = ' + self.database,
                     'schema = ' + self.schema,
                     'outdir = ' + self.outdir,
                     'loglevel = %d' % self.loglevel,
                     'checkmode = ' + str(self.checkmode)]:
            self.log.file(line)
        self.log.console('Logfile = ' + self.logfile)

    # Some custom read queries for particular applications
    def check_obsid(self):
        """
        Query the number of rows in COMMON matching obsid to check that
        the observation actually exists.

        Arguments:
        <None>
        
        Returns:
        1 if the observation exists in COMMON else 0
        """
        sqlcmd = '\n'.join(['SELECT',
                            '    count(obsid)',
                            'FROM ' + self.jcmt_db + 'COMMON',
                            'WHERE',
                            '    obsid = "%s"' % (self.obsid,)])
        return self.conn.read(sqlcmd)[0][0]

    def query_table(self,
                    table,
                    columns):
        """
        Query a specified table in self.db for a set of columns.

        Arguments:
        table      the name of the table to query
        columns    a list of column names in the table

        Returns:
        A list of dictionaries keyed on the column_name.
        If a value is null, a default value will be returned in its place
        that depends upon the data_type.
        """
        selection = ',\n'.join(['    ' + key
                                for key in columns])
        sqlcmd = '\n'.join(['SELECT',
                            '%s' % (selection,),
                            'FROM ' + table,
                            'WHERE obsid = "%s"' % (self.obsid,)])

        answer = self.conn.read(sqlcmd)
        rowlist = []
        for row in answer:
            rowdict = {}
            for key, value in zip(columns, row):
                rowdict[key] = value
            rowlist.append(rowdict)
        self.log.file(rowlist)

        return rowlist

    def get_proposal(self, obsid):
        """
        Get the PI name and proposal title for this obsid.

        Arguments:
        obsid: the observation identifier in COMMON for the observation
        """

        sqlcmd = '\n'.join([
            'SELECT ',
            '    ou.uname,',
            '    op.title',
            'FROM ' + self.jcmt_db + 'COMMON c',
            '    left join ' + self.omp_db + 'ompproj op on c.project=op.projectid',
            '    left join ' + self.omp_db + 'ompuser ou on op.pi=ou.userid',
            'WHERE c.obsid="%s"' % (obsid,)])
        answer = self.conn.read(sqlcmd)

        results = {}
        if len(answer):
            results['pi'] = answer[0][0]
            results['title'] = answer[0][1]
        return results

    def get_quality(self, obsid):
        """
        Get the JSA quality assessment for this proposal, with a default value
        of JSA_QA.GOOD if no assessment has been entered yet.

        Arguments:
        obsid: the observation identifier in COMMON for the observation
        
        Returns:
        a one-entry dictionary with the key 'quality' and a JSA quality
             as the value
        """
        sqlcmd = '\n'.join([
            'SELECT ',
            '    isnull(commentstatus, 0)',
            'FROM ' + self.omp_db + 'ompobslog',
            'WHERE obsid="%s"' % (obsid,),
            '    AND obsactive=1',
            '    AND commentstatus <= %d' % (JCMT_QA.JUNK),
            'GROUP BY obsid',
            'HAVING commentdate=max(commentdate)'])
        answer = self.conn.read(sqlcmd)

        results = {'quality': quality(JCMT_QA.GOOD, self.log)}
        if len(answer):
            results['quality'] = quality(answer[0][0], self.log)
        self.log.file('For %s JSA_QA = %s from ompobslog' %
                      (obsid, results['quality'].jsa_name()))
        return results

    def get_files(self, obsid):
        """
        Get the list of files in this observations, grouped obsid_subsysnr
        and sorted alphabetically.

        Arguments:
        obsid: the observation identifier for the observation
        """
        sqlcmd = '\n'.join([
            'SELECT ',
            '    obsid_subsysnr,',
            '    file_id',
            'FROM ' + self.jcmt_db + 'FILES',
            'WHERE obsid="%s"' % (obsid,),
            'ORDER BY obsid_subsysnr, file_id'])
        answer = self.conn.read(sqlcmd)

        results = {}
        if len(answer):
            for i in range(len(answer)):
                obsid_subsysnr = answer[i][0]
                if obsid_subsysnr not in results:
                    results[obsid_subsysnr] = []
                results[obsid_subsysnr].append(answer[i][1])
        else:
            self.log.console('No rows in ' + self.db + 'FILES for obsid = ' +
                             obsid,
                             logging.ERROR)
        return results

    def check_observation(self,
                          common,
                          subsystem):
        """
        Check the validity of the metadata for a single observation

        Arguments:
        common      dictionary containing fields common to the observation
        subsystem   dictionary containing fields from ACSIS or SCUBA2
        
        Returns:
         0 if observation is OK
         1 if observation is JUNK
        -1 if observation should be skipped
        """
        #-----------------------------------------------------------------
        # Validity checking for raw ACSIS and SCUBA-2 data
        #-----------------------------------------------------------------
        # Check that mandatory fields do not have NULL values
        nullvalues = []
        ingestibility = INGESTIBILITY.GOOD
        
        for field in raw.MANDATORY:
            if common[field] is None:
                nullvalues.append(field)
        if nullvalues:
            self.log.console('The following mandatory fields are NULL:\n' +
                             '\n'.join(sorted(nullvalues)),
                             logging.WARN)
            ingestibility = INGESTIBILITY.BAD
            
        if common['obs_type'] in ('phase', 'RAMP'):
            # do not ingest observations with bogus obs_type
            # this is not an error, but log a warning
            self.log.console('Observation ' + self.obsid +
                             ' is being skipped because obs_type = ' +
                             common['obs_type'],
                             logging.WARN)
            ingestibility = INGESTIBILITY.BAD
        
        # JUNK status trumps BAD, because a JUNK observation must be removed
        # from CAOM-2 if present, whereas a bad observation just cannot be
        # ingested.
        if common['quality'].jsa_value() == JSA_QA.JUNK:
            self.log.console('JUNK QUALITY ASSESSMENT for ' + self.obsid +
                             ' prevents it from being ingested in CAOM-2',
                             logging.WARN)
            ingestibility = INGESTIBILITY.JUNK
        
        # Check observation-level mandatory headers with restricted values
        # by creating the instrument keyword list
        keyword_dict = {}
        keyword_dict['switching_mode'] = common['sw_mode']
        if common['scan_pat']:
            keyword_dict['x_scan_pat'] = common['scan_pat']
        if common['inbeam']:
            keyword_dict['inbeam'] = common['inbeam']
        if common['backend'] in ('ACSIS', 'DAS', 'AOS-C'):
            # Although stored in ACSIS, the sideband properties belong to the
            # whole observation.  Fetch them using any subsysnr.
            subsysnr = subsystem.keys()[0]
            keyword_dict['sideband'] = subsystem[subsysnr]['obs_sb']
            keyword_dict['sideband_filter'] = subsystem[subsysnr]['sb_mode']
        someBad, keyword_list = instrument_keywords('raw', 
                                                    common['instrume'],
                                                    common['backend'],
                                                    keyword_dict, 
                                                    self.log)
        if someBad:
            ingestibility = INGESTIBILITY.JUNK
            self.instrument_keywords = []
        else:
            self.instrument_keywords = keyword_list            

        return ingestibility

    def build_observation(self,
                          observation,
                          common,
                          subsystem,
                          files):
        """
        Construct a simple observation from the available metadata

        Arguments:
        obsid       obsid from COMMON to be used as the observationID
        common      dictionary containing fields common to the observation
        subsystem   dictionary containing fields from ACSIS or SCUBA2
        files       dictionary containing the lists of artifact filenames
        """
        #------------------------------------------------------------
        # Build (or rebuild) a simple observation
        # Since we are dealing with raw data, the algorithm = "exposure"
        # by default, a change in notation for the JCMT.
        #------------------------------------------------------------
        collection = self.collection
        observationID = self.obsid
        self.log.console('PROGRESS: build observationID = ' + self.obsid,
                         logging.DEBUG)

        if observation is None:
            observation = SimpleObservation(collection,
                                            observationID)

        # Every ACSSIS and SCUBA2 observation has an obsnum in COMMON.
        observation.sequence_number = common['obsnum']

        observation.meta_release = common['release_date']

        # The observation type is derived from COMMON.obs_type and
        # COMMON.sam_mode
        if common['obs_type'] == "science":
            # raster is a synonym for scan
            if common['sam_mode'] == "raster":
                observation.obs_type = "scan"
            else:
                observation.obs_type = common['sam_mode']
        else:
            observation.obs_type = common['obs_type']

        # set the observation intent
        observation.intent = intent(common['obs_type'],
                                    common['backend'])

        proposal = Proposal(common['project'])
        if common['pi'] is not None:
            proposal.pi_name = common['pi']
        if common['survey'] is not None:
            proposal.project = common['survey']
        if common['title'] is not None:
            proposal.title = common['title']
        observation.proposal = proposal

        environment = Environment()
        if common['atstart'] is not None:
            make_env = True
            environment.ambient_temp = common['atstart']
        if common['elstart'] is not None:
            environment.elevation = common['elstart']
        if common['humstart'] is not None:
            if common['humstart'] < 0.0:
                environment.humidity = 0.0
            elif common['humstart'] > 100.0:
                environment.humidity = 100.0
            else:
                environment.humidity = common['humstart']
        if common['seeingst'] is not None:
            environment.seeing = common['seeingst']
        if common['tau225st'] is not None:
            environment.tau = common['tau225st']
            environment.wavelength_tau = raw.SpeedOfLight/225.0e9
        observation.environment = environment

        frontend = common['instrume'].upper()
        backend = common['backend'].upper()
        if common['inbeam']:
            inbeam = common['inbeam'].upper()
        else:
            inbeam = ''
        instrument = Instrument(instrument_name(frontend, 
                                                backend, 
                                                inbeam,
                                                self.log))
        instrument.keywords.extend(self.instrument_keywords)

        if backend in ['ACSIS', 'DAS', 'AOSC']:
            keys = sorted(subsystem.keys())
            hybrid = {}
            beamsize = 0.0
            for key in keys:
                productID = self.productID_dict[str(key)]
                # Convert restfreq from GHz to Hz
                restfreq = 1.0e9 * subsystem[key]['restfreq']
                iffreq = subsystem[key]['iffreq']
                ifchansp = subsystem[key]['ifchansp']
                if productID not in hybrid:
                    hybrid[productID] = {}
                    hybrid[productID]['restfreq'] = restfreq
                    hybrid[productID]['iffreq'] = iffreq
                    hybrid[productID]['ifchansp'] = ifchansp
                this_hybrid = hybrid[productID]

                if 'freq_sig_lower' in this_hybrid:
                    this_hybrid['freq_sig_lower'] = \
                        min(subsystem[key]['freq_sig_lower'],
                            this_hybrid['freq_sig_lower'])
                else:
                    this_hybrid['freq_sig_lower'] = \
                        subsystem[key]['freq_sig_lower']

                if 'freq_sig_upper' in this_hybrid:
                    this_hybrid['freq_sig_upper'] = \
                        max(subsystem[key]['freq_sig_upper'],
                            this_hybrid['freq_sig_upper'])
                else:
                    this_hybrid['freq_sig_upper'] = \
                        subsystem[key]['freq_sig_upper']

                if 'freq_img_lower' in this_hybrid:
                    this_hybrid['freq_img_lower'] = \
                        min(subsystem[key]['freq_img_lower'],
                            this_hybrid['freq_img_lower'])
                else:
                    this_hybrid['freq_img_lower'] = \
                        subsystem[key]['freq_img_lower']

                if 'freq_img_upper' in this_hybrid:
                    this_hybrid['freq_img_upper'] = \
                        max(subsystem[key]['freq_img_upper'],
                            this_hybrid['freq_img_upper'])
                else:
                    this_hybrid['freq_img_upper'] = \
                        subsystem[key]['freq_img_upper']

                this_hybrid['meanfreq'] = (this_hybrid['freq_sig_lower'] + 
                                           this_hybrid['freq_sig_upper'])/2.0

                # Compute maximum beam size for this observation in degrees  
                # frequencies are in GHz
                # The scale factor is:
                # 206264.8 ["/r] * sqrt(pi/2) * c [m GHz]/ 15 [m] 
                beamsize = max(beamsize, 1.435 / this_hybrid['meanfreq'])
        else:
            # Compute beam size in degrees for 850 micron array
            # filter is in microns
            # The scale factor is:
            # pi/180 * sqrt(pi/2) * 1e-6 * lambda [um]/ 15 [m] 
            beamsize = 4.787e-6 * 850.0
        observation.instrument = instrument

        if (observation.obs_type not in (
                'flatfield', 'noise', 'setup', 'skydip')
            and common['object']):
            # The target is not significant for the excluded kinds of 
            # observation, even if supplied in COMMON
            if common['object']:
                targetname = target_name(common['object'])
            target = Target(targetname)
            
            if common['obsra'] is None or common['obsdec'] is None:
                target.moving = True
                target_position = None
            else:
                target.moving = False
                target_position = TargetPosition(Point(common['obsra'],
                                                       common['obsdec']),
                                                 'ICRS',
                                                 2000.0)
            observation.target_position = target_position
            
            if common['standard'] is not None:
                target.standard = True if common['standard'] else False
                
            if backend != 'SCUBA-2':
                subsysnr = min(subsystem.keys())
                if subsystem[subsysnr]['zsource'] is not None:
                    target.redshift = subsystem[subsysnr]['zsource']
            
            observation.target = target

        telescope = Telescope('JCMT')
        telescope.geo_location_x = common['obsgeo_x']
        telescope.geo_location_y = common['obsgeo_y']
        telescope.geo_location_z = common['obsgeo_z']
        observation.telescope = telescope

        # Delete any existing raw planes, since we will construct
        # new ones from scratch
        for productID in observation.planes:
            if productID[0:3] == 'raw':
                del observation.planes[productID]

        # Use key for the numeric value of subsysnr here for brevity and
        # to distinguish it from the string representation that will be
        # named subsysnr in this section
        for key in sorted(subsystem.keys()):
            productID = self.productID_dict[str(key)]
            obsid_subsysnr = subsystem[key]['obsid_subsysnr']

            # This plane might already have been created in a hybrid-mode
            # observation, use it if it exists
            if productID not in observation.planes:
                observation.planes.add(Plane(productID))
                plane = observation.planes[productID]
                # set the release dates
                plane.meta_release = common['release_date']
                plane.data_release = common['release_date']

                # all JCMT raw data is in a non-FITS format
                plane.calibration_level = CalibrationLevel.RAW_INSTRUMENT
            else:
                plane = observation.planes[productID]

            # For JCMT raw data, all artifacts have the same WCS
            for jcmt_file_id in files[obsid_subsysnr]:
                file_id = os.path.splitext(jcmt_file_id)[0]
                uri = 'ad:JCMT/' + file_id
                artifact = Artifact(uri)
                if observation.intent == ObservationIntentType.SCIENCE:
                    artifact.product_type = ProductType.SCIENCE
                else:
                    artifact.product_type = ProductType.CALIBRATION

                artifact.meta_release = common['release_date']

                # There is only one part and one chunk for raw data
                artifact.parts.add(Part('0'))
                chunk = Chunk()

                artifact.meta_release = common['release_date']
                artifact.parts['0'].meta_release = common['release_date']
                chunk.meta_release = common['release_date']

                # Raw data does not have axes.
                # bounds and ranges can be specified

# Note that for single spectra the bl and tr corners have the same
# coordinates.  CAOM-2 does not accept a zero-area polygon, so pad the
# coordinates by the +/- 0.5 * beamsize.
# Also, a line map in X or Y grid coordinates will have zero area,
# so expand the box sideways by +/- 0.5 * beamsize.
# Finally, check for a bowtie polygon, where the corners were recorded
# in the wrong order.
                if (common['obs_type'] in ('science', 'pointing', 'focus')
                    and common['obsrabl'] is not None):
                    # Sky position makes no sense for other kinds of 
                    # observations, even if supplied in COMMON
                    
                    # Position axis bounds are in ICRS
                    # Check for various pathologies due to different
                    # observing strategies
                    # position accuracy is about 0.1 arcsec (in decimal degrees)
                    eps = 0.1 / 3600.0
                     
                    bl = TwoD(common['obsrabl'], common['obsdecbl'])
                    br = TwoD(common['obsrabr'], common['obsdecbr'])
                    tl = TwoD(common['obsratl'], common['obsdectl'])
                    tr = TwoD(common['obsratr'], common['obsdectr'])
                    self.log.file('initial bounds bl = ' + str(bl))
                    self.log.file('initial bounds br = ' + str(br))
                    self.log.file('initial bounds tr = ' + str(tr))
                    self.log.file('initial bounds tl = ' + str(tl))
                    halfbeam = beamsize /2.0

                    # The precomputed bounding box can be represented as a polgon
                    if ((bl - br).abs() < eps
                        and (bl - tl).abs() < eps 
                        and (tl - tr).abs() < eps):
                        # bounding "box" is a point, so expand to a box 
                        self.log.console('For observation ' + 
                                         common['obsid'] + 
                                         ' the bounds are a point')
                        
                        cosdec = math.cos(br.y * math.pi / 180.0)
                        offsetX = 0.5 * beamsize / cosdec
                        offsetY = 0.5 * beamsize
                        bl = bl + TwoD(-offsetX, -offsetY)
                        br = br + TwoD( offsetX, -offsetY)
                        tr = tr + TwoD( offsetX,  offsetY)
                        tl = tl + TwoD(-offsetX,  offsetY)

                    elif ((bl - br).abs() < eps
                          and (tl - tr).abs() < eps
                          and (bl - tl).abs() >= eps):
                        # bounding box is a line in y, so diff points to + Y
                        # and the perpendicular points along - X
                        self.log.console('For observation ' + 
                                         common['obsid'] + 
                                         ' the bounds are in a line in Y')
                        diff = tl - bl
                        mean = (tl + bl)/2.0
                        cosdec = math.cos(mean.y * math.pi / 180.0)
                        
                        unitX = TwoD(diff.y, -diff.x * cosdec)
                        unitX = unitX / unitX.abs()
                        offsetX = -halfbeam * TwoD(unitX.x / cosdec, unitX.y)
                        
                        unitY = TwoD(diff.x * cosdec, diff.y)
                        unitY = unitY / unitY.abs()
                        offsetY = halfbeam * TwoD(unitY.x / cosdec, unitY.y)
                        
                        bl = bl - offsetX - offsetY
                        tl = tl - offsetX + offsetY 
                        br = br + offsetX - offsetY 
                        tr = tr + offsetX + offsetY
                        
                    elif ((bl - tl).abs() < eps 
                          and (br - tr).abs() < eps 
                          and (bl - br).abs() >= eps):
                        # bounding box is a line in x
                        self.log.console('For observation ' + 
                                         common['obsid'] + 
                                         ' the bounds are in a line in X')
                        diff = br - bl
                        mean = (br + bl)/2.0
                        cosdec = math.cos(mean.y * math.pi / 180.0)

                        unitX = TwoD(diff.x * cosdec, diff.y)
                        unitX = unitX / unitX.abs()
                        offsetX = halfbeam * TwoD(unitX.x / cosdec, unitX.y)
                        
                        unitY = TwoD(diff.y, -diff.x * cosdec)
                        unitY = unitY / unitY.abs()
                        offsetY = halfbeam * TwoD(unitY.x / cosdec, unitY.y)

                        bl = bl - offsetX - offsetY
                        tl = tl - offsetX + offsetY 
                        br = br + offsetX - offsetY 
                        tr = tr + offsetX + offsetY
                        
                    else:
                        # Get here only if the box is not degenerate
                        bl3d = ThreeD(bl)
                        br3d = ThreeD(br)
                        tr3d = ThreeD(tr)
                        tl3d = ThreeD(tl)
                        
                        try:
                            sign1 = math.copysign(1,
                                ThreeD.included_angle(br3d, bl3d, tl3d))
                            sign2 = math.copysign(1, 
                                ThreeD.included_angle(tr3d, br3d, bl3d))
                            sign3 = math.copysign(1, 
                                ThreeD.included_angle(tl3d, tr3d, br3d))
                            sign4 = math.copysign(1,
                                ThreeD.included_angle(bl3d, tl3d, tr3d))
                        except ValueError as e:
                            self.log.console('The bounding box for obsid = ' +
                                             self.obsid + ' is degenerate',
                                             logging.ERROR)
                        
                        # If the signs are not all the same, the vertices
                        # were recorded in a bowtie order.  Swap any two.
                        if (sign1 != sign2 or sign2 != sign3 or sign3 != sign4):
                            self.log.console('For observation ' + 
                                             common['obsid'] + 
                                             ' the bounds are in a bowtie order',
                                             logging.WARN)
                            bl.swap(br)

                    self.log.file('final bounds bl = ' + str(bl))
                    self.log.file('final bounds br = ' + str(br))
                    self.log.file('final bounds tr = ' + str(tr))
                    self.log.file('final bounds tl = ' + str(tl))
                    bounding_box = CoordPolygon2D()
                    bounding_box.vertices.append(ValueCoord2D(bl.x, bl.y))
                    bounding_box.vertices.append(ValueCoord2D(br.x, br.y))
                    bounding_box.vertices.append(ValueCoord2D(tr.x, tr.y))
                    bounding_box.vertices.append(ValueCoord2D(tl.x, tl.y))

                    spatial_axes = CoordAxis2D(Axis('RA', 'deg'),
                                               Axis('DEC', 'deg'))
                    spatial_axes.bounds = bounding_box

                    chunk.position = SpatialWCS(spatial_axes)
                    chunk.position.coordsys = 'ICRS'
                    chunk.position.equinox = 2000.0

                # energy range, which can contain two subranges in DSB
                if backend == 'SCUBA-2':
                    energy_axis = CoordAxis1D(Axis('WAVE', 'm'))
                    wavelength = subsystem[key]['wavelen']
                    bandwidth = subsystem[key]['bandwid']
                    energy_axis.range = CoordRange1D(
                        RefCoord(0.5, wavelength - bandwidth/2.0),
                        RefCoord(1.5, wavelength + bandwidth/2.0))

                    spectral_axis = SpectralWCS(energy_axis, 'TOPOCENT')
                    spectral_axis.ssysobs = 'TOPOCENT'
                    spectral_axis.ssyssrc = 'TOPOCENT'
                    spectral_axis.resolving_power = abs(wavelength / bandwidth)
                    spectral_axis.bandpass_name = \
                        'SCUBA-2_' + subsystem[key]['filter'] + 'um'

                else:
                    this_hybrid = hybrid[productID]
                        
                    energy_axis = CoordAxis1D(Axis('FREQ', 'GHz'))
                    if subsystem[key]['sb_mode'] == 'DSB':
                        # These all correspond to "pixel" 1, so the pixel
                        # coordinate runs from [0.5, 1.5]
                        # Note that each artifact already records the frequency
                        # bounds correctly for that data in that file.  The
                        # aggregation to the plane will take care of overlapping
                        # energy bounds.
                        freq_bounds = CoordBounds1D()
                        freq_bounds.samples.append(CoordRange1D(
                            RefCoord(0.5, subsystem[key]['freq_sig_lower']),
                            RefCoord(1.5, subsystem[key]['freq_sig_upper'])))
                        freq_bounds.samples.append(CoordRange1D(
                            RefCoord(0.5, subsystem[key]['freq_img_lower']),
                            RefCoord(1.5, subsystem[key]['freq_img_upper'])))
                        energy_axis.bounds = freq_bounds
                    else:
                        energy_axis.range = CoordRange1D(
                            RefCoord(0.5, subsystem[key]['freq_sig_lower']),
                            RefCoord(1.5, subsystem[key]['freq_sig_upper']))

                    spectral_axis = SpectralWCS(energy_axis, 'BARYCENT')
                    spectral_axis.ssysobs = subsystem[key]['ssysobs']
                    spectral_axis.ssyssrc = subsystem[key]['ssyssrc']
                    spectral_axis.zsource = subsystem[key]['zsource']

                    # Recall that restfreq has been converted to Hz in 
                    # thishybrid so do not use the unconverted value from
                    # subsystem[key][['restfreq']
                    spectral_axis.restfrq = this_hybrid['restfreq']
                    meanfreq = float(this_hybrid['meanfreq'])
                    ifchansp = float(this_hybrid['ifchansp'])
                    spectral_axis.resolving_power = abs(1.0e9 * meanfreq / 
                                                        ifchansp)

                    spectral_axis.transition = EnergyTransition(
                        subsystem[key]['molecule'],
                        subsystem[key]['transiti'])

                chunk.energy = spectral_axis

                # time range
                time_axis = CoordAxis1D(Axis('TIME', 'd'))
                mjdstart = utc2mjd(common['date_obs'])
                mjdend = utc2mjd(common['date_end'])
                time_axis.range = CoordRange1D(
                    RefCoord(0.5, mjdstart),
                    RefCoord(1.5, mjdend))

                chunk.time = TemporalWCS(time_axis)
                chunk.time.timesys = 'UTC'
                chunk.time.exposure = \
                    (common['date_end'] - common['date_obs']).total_seconds()

                # Chunk is done, so append it to the part
                artifact.parts['0'].chunks.append(chunk)

                # and append the atrifact to the plane
                plane.artifacts.add(artifact)

        return observation

    def ingest(self):
        """
        Do the ingestion.
        First do all the checks,
        then build the caom2 structure,
        and persist to an xml file that is sent to the repository.
        
        Arguments:
        <none>
        """
        # Check that this is a valid observation
        if not self.check_obsid():
            self.log.console('There is no observation with '
                             'obsid = %s' % (self.obsid,),
                             logging.ERROR)

        # get the dictionary of common metadata
        common = self.query_table(self.jcmt_db + 'COMMON',
                                  raw.COMMON)
        if len(common):
            common = common[0]

        # Append the proposal metadata
        proposal = self.get_proposal(self.obsid)
        if proposal:
            common.update(proposal)

        # Append the quality assessment
        quality = self.get_quality(self.obsid)
        if quality:
            common.update(quality)

        # get a list of rows for the subsystems in this observation
        backend = common['backend']
        if backend in ['ACSIS', 'DAS', 'AOSC']:
            subsystemlist = self.query_table(self.jcmt_db + 'ACSIS',
                                             raw.ACSIS)
            # Convert the list of rows into a dictionary
            subsystem = {}
            for row in subsystemlist:
                subsysnr = row.pop('subsysnr')
                subsystem[subsysnr] = row

        elif backend == 'SCUBA-2':
            subsystemlist = self.query_table(self.jcmt_db + 'SCUBA2',
                                             raw.SCUBA2)
            # Convert the list of rows into a dictionary
            subsystem = {}
            for row in subsystemlist:
                subsysnr = int(row['filter'])
                subsystem[subsysnr] = row

        else:
            self.log.console('backend = "' + backend + '" is not one of '
                           '["ACSIS",  "DAS",  "AOSC",  "SCUBA",  '
                           '"SCUBA-2"]',
                           logging.WARN)

        # somewhat repetitive, but custom SQL is useful
        # get dictionary of productID's for each subsystem
        self.productID_dict = raw_product_id(backend,
                                             'raw',
                                             self.obsid,
                                             self.conn,
                                             self.log)
        
        ingestibility = self.check_observation(common, subsystem)
        if ingestibility == INGESTIBILITY.BAD:
            self.log.console('SERIOUS ERRORS were found in ' + self.obsid,
                             logging.ERROR)
        if self.checkmode:
            if ingestibility == INGESTIBILITY.GOOD:
                self.log.console('SUCCESS: Observation ' + self.obsid + 
                                 ' is ready for ingestion')
            # running in checkmode will NOT remove JUNK observations, 
            # and does NOT check whether they are currently in CAOM-2,
            # but will report that they are junk.
            return
        

        if self.loglevel == logging.DEBUG:
            repository = Repository(self.outdir, self.log)
        else:
            repository = Repository(self.outdir, self.log, debug=False)

        uri = 'caom:' + self.collection + '/' + common['obsid']
        if ingestibility == INGESTIBILITY.JUNK:
            self.log.console('     Remove non-ingestible observation ' + 
                             self.obsid)
            repository.remove(uri)
        else:
            # get the list of files for this observation
            files = self.get_files(self.obsid)

            with repository.process(uri) as xmlfile:
                orig_xmlfile = xmlfile
                observation = None
                if os.path.exists(xmlfile):
                    observation = self.reader.read(xmlfile)

                observation = \
                    self.build_observation(observation,
                                           common,
                                           subsystem,
                                           files)

                with open(xmlfile, 'w') as XMLFILE:
                    self.writer.write(observation, XMLFILE)

            self.log.console('SUCCESS: Observation ' + self.obsid + 
                 ' has been ingested')


    def run(self):
        """
        Fetch metadata, build a CAOM-2 object, and push it into the repository
        """
        self.parse_command_line()
        self.setup_logger()
        
        with logger(self.logfile, 
                    loglevel = self.loglevel).record() as self.log:
            try:
                self.logCommandLineSwitches()
                with connection(self.userconfig,
                                self.log) as self.conn:
                    self.ingest()
            except Exception as e:
                # Be sure that every error message is logged
                self.log.console('ERROR: ' + traceback.format_exc(),
                                 logging.ERROR)
