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

from caom2.observation import ObservationIntentType


def intent(obs_type, backend):
    """
    Generates values for the CAOM-2 field Observation.intent.
    Mostly, if the JCMT designated an observation as 'calibration' or 'science'
    that assessment is considered correct.  However, some calibration mode
    observations are perfectly usable for science if reasonable care is taken
    to assess the quality of the data and reduce it properly.

    Arguments:
    obs_type: the JCMT intent for the observation
    backend: one of ACSIS, DAS, AOS-C, SCUBA-2
    obsmode: observing mode for science observations

    Returns:
    ObservationIntentType.(SCIENCE|CALIBRATION)

    Usage:
    For a raw observation:
        observation.intent = intent(common['obs_type'],
                                    common['backend'])
    For processed data:
        self.add_to_plane_dict('obs.intent',
                               intent(header['OBS_TYPE'],
                                      header['BACKEND']).value)
    """
    intent_value = ObservationIntentType.CALIBRATION
    if (obs_type == 'science' or
            (obs_type == 'pointing' and backend == 'SCUBA-2')):

        intent_value = ObservationIntentType.SCIENCE

    return intent_value
