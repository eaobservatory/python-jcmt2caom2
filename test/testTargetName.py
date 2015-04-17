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

from jcmt2caom2.jsa.target_name import target_name


class testTargetName(unittest.TestCase):
    """
    Test cases for the function target_name(object)
    """

    def test_target_name(self):
        test_data = [['venus', 'VENUS'],
                     ['Venus', 'VENUS'],
                     ['VENUS', 'VENUS'],
                     ['IRC+10216', 'IRC+10216'],
                     ['irc  +  10216', 'IRC + 10216'],
                     [' IRC +10216 ', 'IRC +10216']]

        for objectname, retval in test_data:
            target = target_name(objectname)
            self.assertEqual(target, retval,
                             'The value returned from target_name("' +
                             objectname + '") was "' + target +
                             '" but should have been "' + retval + '"')
