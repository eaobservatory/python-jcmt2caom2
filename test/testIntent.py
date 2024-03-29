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

from __future__ import absolute_import

import unittest

from jcmt2caom2.jsa.intent import intent
from caom2.observation import ObservationIntentType as OIT


class testIntent(unittest.TestCase):
    """
    Test cases for the function intent(obs_type, backend, sam_mode)
    """

    def testIntent(self):
        test_data = [['focus',    'ACSIS',   OIT.CALIBRATION],
                     ['pointing', 'ACSIS',   OIT.CALIBRATION],
                     ['science',  'ACSIS',   OIT.SCIENCE],
                     ['focus',    'DAS',     OIT.CALIBRATION],
                     ['pointing', 'DAS',     OIT.CALIBRATION],
                     ['science',  'DAS',     OIT.SCIENCE],
                     ['focus',    'AOS-C',   OIT.CALIBRATION],
                     ['pointing', 'AOS-C',   OIT.CALIBRATION],
                     ['science',  'AOS-C',   OIT.SCIENCE],
                     ['focus',    'SCUBA-2', OIT.CALIBRATION],
                     ['pointing', 'SCUBA-2', OIT.SCIENCE],
                     ['science',  'SCUBA-2', OIT.SCIENCE]]

        for obs_type, backend, retval in test_data:
            intentval = intent(obs_type, backend)
            self.assertEqual(intentval, retval,
                             'The value returned from intent("' + obs_type +
                             '", "' + backend +
                             '") was "' + str(intentval) +
                             '" but should have been "' + str(retval) + '"')
