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

import math
import re

import healpy
import numpy as np

from caom2.wcs import Axis
from caom2.wcs import CoordAxis2D
from caom2.wcs import CoordPolygon2D
from caom2.chunk import SpatialWCS
from caom2.wcs import ValueCoord2D

from tools4caom2.error import CAOMError

tilenum_comment = re.compile('\(Nside=(\d+)\)')


def jsa_tile_wcs(header):
    """
    Determine WCS information for a JSA tile.
    """

    # Find tile number and Nside.
    tile_number = header['TILENUM']
    match = tilenum_comment.search(header.comments['TILENUM'])
    if not match:
        raise CAOMError('Cannot find Nside in TILENUM comment')
    nside = int(match.group(1))

    # Get corner coordinates.
    (colatitude, longitude) = healpy.vec2ang(np.transpose(
        healpy.boundaries(nside, tile_number, nest=True)))

    assert len(colatitude) == 4
    assert len(longitude) == 4

    # Convert to a CAOM-2 polygon.  Note the ordering appears to be
    # the other way round from what CAOM-2 requires, hence iteration
    # over the corners backwards.
    tile = CoordPolygon2D()

    for i in range(3, -1, -1):
        tile.vertices.append(ValueCoord2D(
            180 * longitude[i] / math.pi,
            90 - (180 * colatitude[i] / math.pi)))

    spatial_axes = CoordAxis2D(Axis('RA', 'deg'),
                               Axis('DEC', 'deg'))
    spatial_axes.bounds = tile

    return SpatialWCS(spatial_axes,
                      coordsys='ICRS',
                      equinox=2000.0)
