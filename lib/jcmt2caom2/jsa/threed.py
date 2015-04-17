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

__author__ = "Russell O. Redman"

import math

from jcmt2caom2.jsa.twod import TwoD


class ThreeD(object):
    """
    Simple three-tuple vector
    """
    radiansPerDegree = math.pi / 180.0

    def __init__(self, x=None, y=None, z=None):
        """
        Create a three-tuple with coordinates (x, y, z)
        """
        __slots__ = ["x", "y", "z"]
        if isinstance(x, ThreeD):
            self.x = x.x
            self.y = x.y
            self.z = x.z
        elif isinstance(x, TwoD):
            self.x = (math.cos(ThreeD.radiansPerDegree * x.x) *
                      math.cos(ThreeD.radiansPerDegree * x.y))
            self.y = (math.sin(ThreeD.radiansPerDegree * x.x) *
                      math.cos(ThreeD.radiansPerDegree * x.y))
            self.z = math.sin(ThreeD.radiansPerDegree * x.y)
        elif isinstance(x, tuple) and len(x) == 2:
            self.x = float(x[0])
            self.y = float(x[1])
            self.z = float(x[2])
        elif isinstance(x, list) and len(x) == 2:
            self.x = float(x[0])
            self.y = float(x[1])
            self.z = float(x[2])
        elif x is not None and y is not None:
            self.x = float(x)
            self.y = float(y)
            self.z = float(z)
        else:
            self.x = None
            self.y = None
            self.z = None

    def __str__(self):
        """
        string representation
        """
        return '(%f, %f, %f)' % (self.x, self.y, self.z)

    def __eq__(self, t):
        """
        test equality between self and t

        Arguments:
        t: another twod
        """
        return (self.x == t.x and self.y == t.y and self.z == t.z)

    def __ne__(self, t):
        """
        test non-equality between self and t

        Arguments:
        t: another twod
        """
        return (self.x != t.x or self.y != t.y or self.z != t.z)

    def __add__(self, t):
        """
        add self and t

        Arguments:
        t: another twod
        """
        return ThreeD(self.x + t.x,
                      self.y + t.y,
                      self.z + t.z)

    def __sub__(self, t):
        """
        subtract T from self

        Arguments:
        t: another twod
        """
        return ThreeD(self.x - t.x,
                      self.y - t.y,
                      self.z - t.z)

    def __mul__(self, f):
        """
        multiply self by f

        Arguments:
        t: another twod
        """
        return ThreeD(f*self.x,
                      f*self.y,
                      f*self.z)

    def __rmul__(self, f):
        """
        multiply self by f

        Arguments:
        t: another twod
        """
        return ThreeD(f*self.x,
                      f*self.y,
                      f*self.z)

    def __div__(self, f):
        """
        divide self by f

        Arguments:
        t: another twod
        """
        return ThreeD(self.x / f,
                      self.y / f,
                      self.z / f)

    def abs(self):
        """
        length of self

        Arguments:
        <none>
        """
        return math.sqrt(self.x**2 + self.y**2 + self.z**2)

    @staticmethod
    def cross(a, b):
        return ThreeD(a.y * b.z - a.z * b.y,
                      a.z * b.x - a.x * b.z,
                      a.x * b.y - a.y * b.x)

    @staticmethod
    def dot(a, b):
        return (a.x * b.x + a.y * b.y + a.z * b.z)

    @staticmethod
    def included_angle(a, b, c):
        """
        Calculate the included angle between the planes defined by
        (origin a, b) and (origin, b, c) in degrees,
        checking for degenerate cases.
        """
        if (a == b or b == c or c == a):
            raise ValueError('The triangle a = ' + str(a) + ', b = ' +
                             str(b) + ', c = ' + str(c) + ' is degenerate')
        # Most triangles will be very small on the celestial sphere.
        # Subtracting b helps retain numerical significance.
        amb = ThreeD.cross(a - b, b)
        norm = amb.abs()
        if 0.0 == norm:
            raise ValueError('The origin, a = ' + str(a) + ', and '
                             'b = ' + str(b) + ' are colinear')
        amb = amb / norm

        cmb = ThreeD.cross(c - b, b)
        norm = cmb.abs()
        if 0.0 == norm:
            raise ValueError('The origin, b = ' + str(b) + ', and '
                             'c = ' + str(c) + ' are colinear')
        cmb = cmb / norm

        cosval = ThreeD.dot(amb, cmb)
        if cosval > 1.0:
            cosval = 1.0
        if cosval < -1.0:
            cosval = -1.0
        return math.acos(cosval) / ThreeD.radiansPerDegree
