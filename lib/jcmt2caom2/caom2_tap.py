# Copyright (C) 2014-2015 Science and Technology Facilities Council.
# Copyright (C) 2015 East Asian Observatory.
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

from collections import namedtuple
from logging import getLogger

from tools4caom2.tapclient import tapclient

logger = getLogger(__name__)

ObsInfo = namedtuple('ObsInfo',
                     ('prod_id', 'date_obs', 'date_end', 'release',
                      'artifact_uri'))

PlaneInfo = namedtuple('PlaneInfo',
                       ('collection', 'obs_id', 'prod_id', 'run_id'))

FileInfo = namedtuple('FileInfo',
                      ('collection', 'obs_id', 'prod_id', 'artifact_uri'))


class CAOM2TAP(object):
    """
    Class for interacting with CAOM-2 via CADC's TAP srevice.
    """

    def __init__(self, *args, **kwargs):
        """
        Construct CAOM-2 TAP query object.

        Any additional arguments and keyword arguments are passed to
        the tools4caom2.tapclient constructor.
        """

        self.tap = tapclient(*args, **kwargs)

    def get_proposal_info(self, project_id):
        """
        Attempt to retrieve the PI and title of the given project.
        """

        pi = title = None

        answer = self.tap.query(
            "SELECT DISTINCT Observation.proposal_pi, "
            "Observation.proposal_title "
            "FROM caom2.Observation as Observation "
            "WHERE Observation.collection = 'JCMT' "
            "AND lower(Observation.proposal_id) = '{0}'".format(
                project_id.lower()))

        for row in answer:
            if (pi is None) and row[0]:
                pi = row[0]
            if (title is None) and row[1]:
                title = row[1]

        return (pi, title)

    def get_collections_with_obs_id(self, obs_id):
        """
        Get a list of collections which have an observation with the
        given observation ID.
        """

        result = []

        for row in self.tap.query(
                'SELECT DISTINCT Observation.collection '
                'FROM caom2.Observation AS Observation '
                'WHERE Observation.observationID=\'{0}\''.format(
                    obs_id)):

            result.append(row[0])

        return result

    def get_obs_info(self, obs_id):
        """
        Get information for the given observation.

        TODO: this tap query is as extracted from the "jcmt2caom2ingest"
        module.  It repeats all the plane information for every artifact
        in the plane.  Why not return only the observation info and make
        a second query for artifacts?
        """

        result = []

        for row in self.tap.query(
                'SELECT'
                ' Plane.productID,'
                ' Plane.time_bounds_cval1,'
                ' Plane.time_bounds_cval2,'
                ' Plane.dataRelease,'
                ' Artifact.uri '
                'FROM caom2.Observation as Observation'
                ' INNER JOIN caom2.Plane AS Plane'
                '  ON Observation.obsID=Plane.obsID'
                ' INNER JOIN caom2.Artifact AS Artifact'
                '  ON Plane.planeID=Artifact.planeID '
                'WHERE Observation.collection=\'JCMT\''
                ' AND Observation.observationID=\'{0}\''.format(obs_id)):

            result.append(ObsInfo(*row))

        return result

    def get_planes_for_obs_with_run_id(self, run_id):
        """
        Get information on observations which have a plane featuring the
        given (provenance) run ID.

        Returns all planes of the matching observations.
        """

        result = []

        for row in self.tap.query(
                'SELECT'
                ' Observation.collection,'
                ' Observation.observationID,'
                ' Plane.productID,'
                ' Plane.provenance_runID '
                'FROM'
                ' caom2.Observation AS Observation'
                ' INNER JOIN caom2.Plane AS Plane'
                '   ON Observation.obsID=Plane.obsID'
                ' INNER JOIN caom2.Plane AS Plane2'
                '   ON observation.obsID=Plane2.obsID '
                'WHERE'
                ' Plane2.provenance_runID=\'{0}\' '
                'ORDER BY'
                ' Observation.collection, '
                ' Observation.observationID, '
                ' Plane.productID'.format(
                    run_id)):

            result.append(PlaneInfo(*row))

        return result

    def get_artifacts_for_plane_with_artifact_uri(self, artifact_uri):
        """
        Get information on planes which have an artifact with the
        given URI.

        Gives results for all planes which include a matching
        artifact.
        """

        result = []

        for row in self.tap.query(
                'SELECT Observation.collection,'
                ' Observation.observationID,'
                ' Plane.productID,'
                ' Artifact.uri '
                'FROM caom2.Observation AS Observation'
                ' INNER JOIN caom2.Plane as Plane'
                '  ON Observation.obsID=Plane.obsID'
                ' INNER JOIN caom2.Artifact AS Artifact'
                '  ON Plane.planeID=Artifact.planeID'
                ' INNER JOIN caom2.Artifact AS Artifact2'
                '  ON Plane.planeID=Artifact2.planeID '
                'WHERE Artifact2.uri=\'{0}\''.format(artifact_uri)):

            result.append(FileInfo(*row))

        return result
