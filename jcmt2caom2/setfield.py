#!/usr/bin/env python2.7

__author__ = "Russell O. Redman"

import argparse
from ConfigParser import SafeConfigParser
import errno
import logging
import math
import os.path
import re
import shutil
import sys
import traceback
import vos

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
from tools4caom2.utdate_string import utdate_string
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

from jcmt2caom2 import tovos 

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version

__doc__ = """
The update class is used to update specific fields in a set of existing caom2 
observations, i.e. each observation will be read from the CAOM-2 repository,
updated, and written back to the repository.  Observations to be updated are 
identified through the provenance_runID.  Only a few fields can be
updated, based on values passed in through the command line arguments,
and the same value will be assigned to the specified field in every observation.
"""

class update(object):
    def __init__(self):
        """
        Create a jcmt2caom2.update instance to update specific fields in a set
        of observations.
        """
        self.exedir = os.path.abspath(os.path.dirname(sys.argv[0]))
        self.configpath = os.path.abspath(self.exedir + '/../config')

        # config object optionally contains a user configuration object
        # this can be left undefined at the CADC, but is needed at other sites
        self.userconfigpath = '~/.tools4caom2/tools4caom2.config'

        self.userconfig = SafeConfigParser()

        self.outdir = '.'
        
        self.collection = None
        self.collections = ('JCMT', 'JCMTLS', 'JCMRUSER', 'SANDBOX')
        
        self.logdir = ''
        self.logfile = ''
        self.loglevel = logging.INFO
        self.log = None
        self.errors = False
        self.warnings = False
        self.junk = False
        
        self.voscopy = None
        self.vosroot = 'vos:jsaops'
        
        self.reader = ObservationReader(True)
        self.writer = ObservationWriter()
        self.conn = None

    def parse_command_line(self):
        """
        Parse command line arguments

        Arguments:
        <None>
        
        Sets the release date and/or reference URL for a set of planes
        identified by their provenance_runID.  One or both of --releasedate
        and --reference must be specified.
        """
        ap = argparse.ArgumentParser()
        ap.add_argument('--outdir',
            default='.',
            help='working directory for output files')

        ap.add_argument('--collection',
            choices=self.collections,
            default='ALL',
            help='collection to use for ingestion')
        ap.add_argument('--runid',
            required=True,
            help='provenance_runID for the planes to be updated')
        ap.add_argument('--releasedate',
            help='release date to set for the planes and observations')
        ap.add_argument('--reference',
            help='reference URl to set for the planes and observations')
        
        ap.add_argument('--logdir',
            default='.',
            help='path to log file directory')
        ap.add_argument('--log',
            help='path to log file')

        ap.add_argument('--test',
            action='store_true',
            help='report observations and planes but do not execute commands')
        ap.add_argument('--debug',
            dest='loglevel',
            action='store_const',
            const=logging.DEBUG)
        args = ap.parse_args()

        if args.collection:
            self.collection = args.collection

        self.outdir = os.path.abspath(
                          os.path.expanduser(
                              os.path.expandvars(args.outdir)))

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
            defaultlogname = 'jcmt2caom2setfields_' + utdate_string() + '.log'
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
        self.log.file(sys.argv[0])
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

    def update(self):
        """
        Find all observations that match the provenance_runID, then 
        update the requested fields in each observation.
        
        Arguments:
        <none>
        """
        if self.loglevel == logging.DEBUG:
            repository = Repository(self.outdir, self.log)
        else:
            repository = Repository(self.outdir, self.log, debug=False)


        if run_id not in self.remove_id:
            self.remove_id.append(run_id)
            tapcmd = '\n'.join([
                'SELECT',
                '    Observation.collection,',
                '    Observation.observationID,',
                '    Plane.productID,',
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

        uri = 'caom:' + self.collection + '/' + common['obsid']
            with repository.process(uri) as xmlfile:
                try:
                    observation = self.reader.read(xmlfile)
                    # do something

                    with open(xmlfile, 'w') as XMLFILE:
                        self.writer.write(observation, XMLFILE)
                except:
                    self.log.console('Cannot get observation ' + uri,
                                     logging.ERROR)

            self.log.console('SUCCESS: Observation ' + self.obsid + 
                 ' has been ingested')


    def run(self):
        """
        Fetch metadata, build a CAOM-2 object, and push it into the repository
        """
        self.parse_command_line()
        self.setup_logger()
        
        prefix = ''
        
        with logger(self.logfile, 
                    loglevel = self.loglevel).record() as self.log:
            try:
                self.logCommandLineSwitches()
                self.update()
                self.log.console('DONE')
            except Exception as e:
                if not isinstance(e, logger.LoggerError):
                    # Be sure that every error message is logged
                    # Log this error, but pass because we are exitting anyways
                    try:
                        self.errors = True
                        self.log.console('ERROR: ' + traceback.format_exc(),
                                         logging.ERROR)
                    except Exception as p:
                        pass
