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

from codecs import ascii_decode
from collections import namedtuple
from logging import getLogger

from tools4caom2.tapclient import tapclient

logger = getLogger(__name__)

ObsInfo = namedtuple('ObsInfo',
                     ('prod_id', 'date_obs', 'date_end', 'release',
                      'artifact_uri'))

PlaneInfo = namedtuple('PlaneInfo',
                       ('obs_id', 'prod_id'))

FileInfo = namedtuple('FileInfo',
                      ('collection', 'obs_id', 'prod_id', 'artifact_uri'))


def _remove_file_extension(artifact_uri):
    # Assume that "." only appears in artifact URIs before a file extension
    # (appears true based on TAP queries performed to check).
    return artifact_uri.split('.', 1)[0]


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

        for (prod_id, date_obs, date_end, release, artifact_uri) in self.tap.query(
                'SELECT'
                ' Plane.productID,'
                ' Plane.time_bounds_lower,'
                ' Plane.time_bounds_upper,'
                ' Plane.dataRelease,'
                ' Artifact.uri '
                'FROM caom2.Observation as Observation'
                ' INNER JOIN caom2.Plane AS Plane'
                '  ON Observation.obsID=Plane.obsID'
                ' INNER JOIN caom2.Artifact AS Artifact'
                '  ON Plane.planeID=Artifact.planeID '
                'WHERE Observation.collection=\'JCMT\''
                ' AND Observation.observationID=\'{0}\''.format(obs_id)):

            artifact_uri = ascii_decode(artifact_uri)[0]

            result.append(ObsInfo(prod_id, date_obs, date_end, release, artifact_uri))

        return result

    def get_planes_with_run_id(self, collection, run_ids):
        """
        Get information on planes featuring a (provenance) run ID
        from the given list.
        """

        conditions = ['Observation.collection=\'{0}\'']
        params = [collection]

        run_id_conditions = []

        n = 1
        for run_id in run_ids:
            run_id_conditions.append('Plane.provenance_runID=\'{' + str(n) +
                                     '}\'')
            params.append(run_id)
            n += 1

        conditions.append('(' + ' OR '.join(run_id_conditions) + ')')

        result = []

        for row in self.tap.query((
                'SELECT'
                ' Observation.observationID,'
                ' Plane.productID '
                'FROM'
                ' caom2.Observation AS Observation'
                ' JOIN caom2.Plane AS Plane'
                '   ON Observation.obsID=Plane.obsID '
                'WHERE ' + ' AND '.join(conditions)).format(*params)):

            result.append(PlaneInfo(*row))

        return result

    def get_artifacts_for_plane_with_artifact_uri(self, artifact_uri):
        """
        Get information on planes which have an artifact with the
        given URI.

        Gives results for all planes which include a matching
        artifact.
        """

        # Temporarily remove extension and do a "LIKE" query to allow for
        # CAOM-2 records with and without extensions in artifact URIs.
        artifact_uri = _remove_file_extension(artifact_uri)

        result = []

        for (collection, obs_id, prod_id, artifact_uri) in self.tap.query(
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
                'WHERE Artifact2.uri LIKE \'{0}%\''.format(artifact_uri)):

            artifact_uri = ascii_decode(artifact_uri)[0]

            result.append(FileInfo(collection, obs_id, prod_id, artifact_uri))

        return result
