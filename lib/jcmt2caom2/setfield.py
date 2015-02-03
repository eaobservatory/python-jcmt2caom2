__author__ = "Russell O. Redman"

import argparse
from collections import OrderedDict
from datetime import datetime
import logging
import os.path
import re
import shutil
import sys
import traceback

from caom2.caom2_observation import Observation
from caom2.caom2_observation_uri import ObservationURI
from caom2.caom2_plane import Plane

from tools4caom2.caom2repo_wrapper import Repository
from tools4caom2.timezone import UTC
from tools4caom2.tapclient import tapclient
from tools4caom2.utdate_string import utdate_string
import tools4caom2.__version__

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version

__doc__ = """
The setfield class is used to update specific fields in a set of existing caom2
observations identified by the value of provenance_runID in at least one
of their planes.  Each observation will be read from the CAOM-2 repository,
the specified fields will be updated in the planes with the matching values of
provenance_runID, and the observations written back to the repository.  Only a
few fields can be set with this routine, selected by command line arguments,
and the same value will be assigned to the specified field in every matching
plane.
"""

logger = logging.getLogger()


class setfield(object):
    def __init__(self):
        """
        Create a jcmt2caom2.update instance to update specific fields in the
        matching planes of a set of observations.
        """
        if sys.argv[0] and sys.argv[0] != '-c':
            self.progname = os.path.basename(sys.argv[0])
        else:
            self.progname = 'setfield'

        if sys.path[0]:
            self.exedir = os.path.abspath(os.path.dirname(sys.path[0]))
        else:
            self.exedir = os.getcwd()

        self.configpath = os.path.abspath(self.exedir + '/../config')

        self.outdir = None

        self.collection = None
        self.collections = ('JCMT', 'JCMTLS', 'JCMRUSER', 'SANDBOX')

        self.runid = None
        self.releasedate = None
        self.reference = None

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
        ap.add_argument(
            '--proxy',
            default='~/.ssl/cadcproxy.pem',
            help='path to CADC proxy')

        ap.add_argument(
            '--outdir',
            default='.',
            help='working directory for output files')

        ap.add_argument(
            '--collection',
            choices=self.collections,
            default='ALL',
            help='collection to use for ingestion')
        ap.add_argument(
            '--runid',
            required=True,
            help='provenance_runID for the planes to be updated')
        ap.add_argument(
            '--releasedate',
            help='release date to set for the planes and observations')
        ap.add_argument(
            '--reference',
            help='reference URl to set for the planes and observations')

        ap.add_argument(
            '--test',
            action='store_true',
            help='report observations and planes but do not execute commands')
        ap.add_argument(
            '--debug',
            dest='loglevel',
            action='store_const',
            const=logging.DEBUG)
        self.args = ap.parse_args()

        self.proxy = os.path.abspath(
            os.path.expandvars(
                os.path.expanduser(self.args.proxy)))

        if self.args.collection == 'ALL':
            self.collection = self.collections
        else:
            self.collection = (self.args.collection,)
        self.runid = self.args.runid
        if self.args.releasedate:
            dt_string = self.args.releasedate
            if re.match(r'^\d{8}$',
                        self.args.releasedate):
                dt = ('-'.join([dt_string[0:4],
                                dt_string[4:6],
                                dt_string[6:8]]) + 'T00:00:00')
            elif re.match(r'^[^\d]*(\d{1,4}-\d{2}-\d{2})$', dt_string):
                dt = dt_string + 'T00:00:00'
            else:
                raise ValueError('the string "%s" does not match a utdate '
                                 'YYYYMMDD or ISO YYYY-MM-DD format:'
                                 % (dt_string))
            self.releasedate = datetime.strptime(dt,
                                                 '%Y-%m-%dT%H:%M:%S')
        elif self.args.reference:
            self.reference = self.args.reference
        else:
            raise RuntimeError('one of --releasedate or --reference '
                               'must be given')

        self.outdir = os.path.abspath(
            os.path.expanduser(
                os.path.expandvars(self.args.outdir)))

        if self.args.loglevel:
            logging.getLogger().setLevel(self.args.loglevel)

        self.test = self.args.test

    def logCommandLineSwitches(self):
        """
        Log the internal configuration.

        Arguments:
        <None>
        """
        logger.info(self.progname)
        logger.info('jcmt2caom2version    = %s', jcmt2caom2version)
        logger.info('tools4caom2version   = %s', tools4caom2version)
        for attr in dir(self.args):
            if attr != 'id' and attr[0] != '_':
                logger.info('%-15s= %s', attr, getattr(self.args, attr))
        logger.info('exedir = %s', self.exedir)
        logger.info('outdir = %s', self.outdir)

    def update(self):
        """
        Find all observations that match the provenance_runID, then
        update the requested fields in each observation.

        Arguments:
        <none>
        """
        repository = Repository()

        tapcmd = '\n'.join([
            'SELECT',
            '    Observation.collection,',
            '    Observation.observationID,',
            '    Plane.productID',
            'FROM',
            '    caom2.Observation AS Observation',
            '        INNER JOIN caom2.Plane AS Plane',
            '            ON Observation.obsID=Plane.obsID',
            'WHERE',
            '    Plane.provenance_runID=' + "'" + self.runid + "'",
            'ORDER BY Observation.collection, ',
            '         Observation.observationID, ',
            '         Plane.productID'])
        result = self.tap.query(tapcmd)
        result_dict = OrderedDict()

        if result:
            for coll, obsid, prodid in result:
                if coll not in result_dict:
                    result_dict[coll] = OrderedDict()
                if obsid not in result_dict[coll]:
                    result_dict[coll][obsid] = []
                if prodid not in result_dict[coll][obsid]:
                    result_dict[coll][obsid].append(prodid)

        for coll in result_dict:
            for obsid in result_dict[coll]:
                uri = 'caom:' + coll + '/' + obsid
                logger.info('PROGRESS: ' + uri)
                with repository.process(uri) as wrapper:
                    observation = wrapper.observation
                    try:
                        if self.releasedate:
                            observation.metaRelease = self.releasedate
                        for productID in observation.planes:
                            logger.debug('PROGRESS: %s/%s', uri, productID)
                            plane = observation.planes[productID]
                            if productID in result_dict[coll][obsid]:
                                if self.releasedate:
                                    plane.data_release = self.releasedate
                                    plane.meta_release = self.releasedate
                                if self.reference:
                                    plane.provenance_reference = self.reference

                        if self.test:
                            wrapper.observation = None

                    except:
                        logger.exception('Cannot process %s', uri)
                        raise

            logger.info('SUCCESS: Observation %s has been ingested', obsid)

    def run(self):
        """
        Fetch metadata, build a CAOM-2 object, and push it into the repository
        """
        self.parse_command_line()

        try:
            self.logCommandLineSwitches()
            self.tap = tapclient(self.proxy)
            self.update()
            logger.info('DONE')
        except Exception:
            self.errors = True
            logger.exception('ERROR')
