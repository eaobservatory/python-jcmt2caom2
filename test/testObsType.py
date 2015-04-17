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

from __future__ import absolute_import

import unittest

from jcmt2caom2.jsa.obs_type import obs_type


class testTargetName(unittest.TestCase):
    """
    Test cases for the function target_name(object)
    """

    def test_target_name(self):
        test_data = [['science', 'grid', 'grid'],
                     ['science', 'jiggle', 'jiggle'],
                     ['science', 'raster', 'scan'],
                     ['science', 'scan', 'scan'],
                     ['pointing', 'anything', 'pointing'],
                     ['focus', 'anything', 'focus'],
                     ['skydip', 'anything', 'skydip'],
                     ['flatfield', 'anything', 'flatfield'],
                     ['setup', 'anything', 'setup'],
                     ['noise', 'anything', 'noise']]

        for jcmt_obs_type, sam_mode, retval in test_data:
            caom2_obs_type = obs_type(jcmt_obs_type, sam_mode)
            self.assertEqual(caom2_obs_type, retval,
                             'The value returned from obs_type("' +
                             jcmt_obs_type + '", "' +
                             sam_mode + '") was "' +
                             caom2_obs_type + '" but should have been "' +
                             retval + '"')
