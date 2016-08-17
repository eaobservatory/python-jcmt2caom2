# Copyright (C) 2014-2015 Science and Technology Facilities Council.
# Copyright (C) 2015-2016 East Asian Observatory.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

__author__ = "Russell O. Redman"

import argparse
from collections import OrderedDict
from datetime import datetime
import logging
import os.path
import re
import sys

from tools4caom2.__version__ import version as tools4caom2version
from tools4caom2.caom2repo_wrapper import Repository
from tools4caom2.tapclient import tapclient

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

        self.collection = None
        self.collections = ('JCMT', 'JCMTLS', 'JCMTUSER', 'SANDBOX')

        self.runid = None
        self.releasedate = None
        self.reference = None

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

                        if self.dry_run:
                            wrapper.observation = None

                    except:
                        logger.exception('Cannot process %s', uri)
                        raise

            logger.info('SUCCESS: Observation %s has been ingested', obsid)

    def run(self):
        """
        Fetch metadata, build a CAOM-2 object, and push it into the repository
        """

        if sys.argv[0] and sys.argv[0] != '-c':
            progname = os.path.basename(sys.argv[0])
        else:
            progname = 'setfield'

        ap = argparse.ArgumentParser(progname)

        ap.add_argument(
            '--proxy',
            default='~/.ssl/cadcproxy.pem',
            help='path to CADC proxy')

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
            '--dry-run', '-n',
            action='store_true',
            dest='dry_run',
            help='report observations and planes but do not execute commands')
        ap.add_argument(
            '--verbose', '-v',
            dest='loglevel',
            action='store_const',
            const=logging.DEBUG)

        args = ap.parse_args()

        proxy = os.path.abspath(
            os.path.expandvars(
                os.path.expanduser(args.proxy)))

        if args.collection == 'ALL':
            self.collection = self.collections
        else:
            self.collection = (args.collection,)
        self.runid = args.runid
        if args.releasedate:
            dt_string = args.releasedate
            if re.match(r'^\d{8}$',
                        args.releasedate):
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
        elif args.reference:
            self.reference = args.reference
        else:
            raise RuntimeError('one of --releasedate or --reference '
                               'must be given')

        if args.loglevel:
            logging.getLogger().setLevel(args.loglevel)

        self.dry_run = args.dry_run

        logger.info(progname)
        logger.info('jcmt2caom2version  = %s', jcmt2caom2version)
        logger.info('tools4caom2version = %s', tools4caom2version)
        for attr in dir(args):
            if attr != 'id' and attr[0] != '_':
                logger.info('%-18s = %s', attr, getattr(args, attr))

        try:
            self.tap = tapclient(proxy)
            self.update()
            logger.info('DONE')
        except:
            logger.exception('ERROR')
            return False

        return True
