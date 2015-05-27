# Copyright (C) 2014-2015 Science and Technology Facilities Council.
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

from logging import getLogger

logger = getLogger(__name__)


def get_project_pi_title(project_id, conn, tap):
    """
    Retrieve the PI name and project title for a given JCMT project.

    For current projects and some older projects, the PI name will be
    available in the OMP.  Otherwise we must check the CAOM-2 for
    existing records which refer to this project.

    Returns a (project_pi, project_title) tuple, of which one or both
    elements may be None if the project as a whole, or the PI name,
    could not be found.
    """


    logger.debug('Fetching project "%s" details from OMP', project_id)
    (project_pi, project_title) = conn.get_project_pi_title(project_id)

    if (project_pi is None) or (project_title is None):
        # Some of the information was missing, so try a TAP query
        # to see if the project information is already in CAOM-2.
        logger.debug('Fetching project "%s" details from CAOM-2', project_id)

        (caom2_pi, caom2_title) = tap.get_proposal_info(project_id)

        if project_pi is None and caom2_pi is not None:
            project_pi = caom2_pi

        if project_title is None and caom2_title is not None:
            project_title = caom2_title

    return (project_pi, project_title)
