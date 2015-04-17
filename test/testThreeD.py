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

import math
import unittest

from jcmt2caom2.jsa.threed import ThreeD
from jcmt2caom2.jsa.twod import TwoD


class testThreeD(unittest.TestCase):
    def testThreeDArithmetic(self):
        """
        Test basic properties
        """
        origin = ThreeD(0.0, 0.0, 0.0)
        xunit = ThreeD(1.0, 0.0, 0.0)
        yunit = ThreeD(0.0, 1.0, 0.0)
        zunit = ThreeD(0.0, 0.0, 1.0)

        self.assertFalse(xunit == yunit)
        self.assertFalse(yunit == zunit)
        self.assertTrue(xunit != zunit)
        self.assertNotEqual(xunit, yunit)
        self.assertNotEqual(yunit, zunit)

        # conversions from TwoD as RA, Dec
        self.assertTrue((xunit - ThreeD(TwoD(0.0, 0.0))).abs() < 1.0e-9)
        self.assertTrue((yunit - ThreeD(TwoD(90.0, 0.0))).abs() < 1.0e-9)
        self.assertTrue((zunit - ThreeD(TwoD(0.0, 90.0))).abs() < 1.0e-9)

        self.assertEqual(ThreeD(1.0, 1.0, 0.0), xunit + yunit)
        self.assertEqual(ThreeD(0.0, 1.0, 1.0), yunit + zunit)
        self.assertEqual(ThreeD(1.0, 1.0, 1.0), xunit + yunit + zunit)

        self.assertEqual(ThreeD(1.0, 1.0, 0.0) - xunit, yunit)
        self.assertEqual(ThreeD(0.0, 1.0, 1.0) - yunit, zunit)
        self.assertEqual(ThreeD(1.0, 1.0, 1.0) - xunit, yunit + zunit)

        self.assertEqual(ThreeD(1.0, 1.0, 1.0) - xunit - yunit - zunit, origin)

        self.assertEqual(2.0 * xunit, ThreeD(2.0, 0.0, 0.0))
        self.assertEqual(2.0 * yunit, ThreeD(0.0, 2.0, 0.0))
        self.assertEqual(2.0 * zunit, ThreeD(0.0, 0.0, 2.0))

        self.assertEqual((xunit + yunit).abs(), math.sqrt(2.0))
        self.assertEqual((yunit + zunit).abs(), math.sqrt(2.0))
        self.assertEqual((zunit + xunit).abs(), math.sqrt(2.0))
        self.assertEqual((xunit + yunit + zunit).abs(), math.sqrt(3.0))

        self.assertEqual(zunit, ThreeD.cross(xunit, yunit))
        self.assertEqual(xunit, ThreeD.cross(yunit, zunit))
        self.assertEqual(yunit, ThreeD.cross(zunit, xunit))

        self.assertEqual(-1 * zunit, ThreeD.cross(yunit, xunit))
        self.assertEqual(-1 * xunit, ThreeD.cross(zunit, yunit))
        self.assertEqual(-1 * yunit, ThreeD.cross(xunit, zunit))

        self.assertEqual(0.0, ThreeD.dot(xunit, yunit))
        self.assertEqual(0.0, ThreeD.dot(yunit, zunit))
        self.assertEqual(0.0, ThreeD.dot(zunit, xunit))

        self.assertEqual(1.0, ThreeD.dot(xunit, xunit))
        self.assertEqual(1.0, ThreeD.dot(yunit, yunit))
        self.assertEqual(1.0, ThreeD.dot(zunit, zunit))

        self.assertEqual(1.0, xunit.abs())
        self.assertEqual(1.0, yunit.abs())
        self.assertEqual(1.0, zunit.abs())

        self.assertEquals(180.0,
                          ThreeD.included_angle(
                              ThreeD(1.0, 0.0, 1.0),
                              ThreeD(2.0, 0.0, 1.0),
                              ThreeD(3.0, 0.0, 1.0)))
        self.assertEquals(0.0,
                          ThreeD.included_angle(
                              ThreeD(1.0, 0.0, 1.0),
                              ThreeD(2.0, 0.0, 1.0),
                              ThreeD(-3.0, 0.0, 1.0)))
        self.assertEquals(180.0,
                          ThreeD.included_angle(
                              ThreeD(1.0, 0.0, 0.0),
                              ThreeD(0.0, 1.0, 0.0),
                              ThreeD(-1.0, 0.0, 0.0)))
        self.assertEquals(90.0,
                          ThreeD.included_angle(
                              ThreeD(0.0, 0.0, 1.0),
                              ThreeD(0.0, 1.0, 0.0),
                              ThreeD(1.0, 0.0, 0.0)))
        for ang in range(0, 90, 3):
            for iscale in range(0, 4):
                scale = 0.1 ** iscale
                angle = ang * math.pi / 180.0

                a = ThreeD(TwoD(ang,        90.0 - scale))
                b = ThreeD(TwoD(ang,        90.0))
                c = ThreeD(TwoD(ang + 90.0, 90.0 - scale))

                self.assertTrue(abs(90.0 - ThreeD.included_angle(a, b, c)) <
                                1.0e-9)
