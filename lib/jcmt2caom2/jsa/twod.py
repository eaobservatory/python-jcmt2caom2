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


class TwoD(object):
    """
    Simple two-tuple vector
    """

    def __init__(self, x=None, y=None):
        """
        Create a two-tuple with coordinates (x, y)
        """
        __slots__ = ["x", "y"]
        if isinstance(x, TwoD):
            self.x = x.x
            self.y = x.y
        elif isinstance(x, tuple) and len(x) == 2:
            self.x = float(x[0])
            self.y = float(x[1])
        elif isinstance(x, list) and len(x) == 2:
            self.x = float(x[0])
            self.y = float(x[1])
        elif x is not None and y is not None:
            self.x = float(x)
            self.y = float(y)
        else:
            self.x = None
            self.y = None

    def __str__(self):
        """
        string representation
        """
        return '(%f, %f)' % (self.x, self.y)

    def __eq__(self, t):
        """
        test equality between self and t

        Arguments:
        t: another twod
        """
        return (self.x == t.x and self.y == t.y)

    def __ne__(self, t):
        """
        test non-equality between self and t

        Arguments:
        t: another twod
        """
        return (self.x != t.x or self.y != t.y)

    def __add__(self, t):
        """
        add self and t

        Arguments:
        t: another twod
        """
        return TwoD(self.x + t.x, self.y + t.y)

    def __sub__(self, t):
        """
        subtract T from self

        Arguments:
        t: another twod
        """
        return TwoD(self.x - t.x, self.y - t.y)

    def __mul__(self, f):
        """
        multiply self by f

        Arguments:
        t: another twod
        """
        return TwoD(f*self.x, f*self.y)

    def __rmul__(self, f):
        """
        multiply self by f

        Arguments:
        t: another twod
        """
        return TwoD(f*self.x, f*self.y)

    def __div__(self, f):
        """
        divide self by f

        Arguments:
        t: another twod
        """
        return TwoD(self.x/f, self.y/f)

    def swap(self, t):
        """
        swap the values of self and t

        Arguments:
        t: another twod
        """
        store = self.x
        self.x = t.x
        t.x = store

        store = self.y
        self.y = t.y
        t.y = store

    def abs(self):
        """
        length of self

        Arguments:
        <none>
        """
        return math.sqrt(self.x**2 + self.y**2)

    @staticmethod
    def cross(v1, v2):
        return (v1.x*v2.y - v1.y*v2.x)
