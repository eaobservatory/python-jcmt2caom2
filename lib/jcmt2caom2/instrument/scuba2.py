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

from caom2.wcs.caom2_axis import Axis
from caom2.wcs.caom2_coord_axis1d import CoordAxis1D
from caom2.wcs.caom2_coord_range1d import CoordRange1D
from caom2.wcs.caom2_ref_coord import RefCoord
from caom2.wcs.caom2_spectral_wcs import SpectralWCS


def scuba2_spectral_wcs(header):
    energy_axis = CoordAxis1D(Axis('WAVE', 'm'))
    wavelength = header['wavelen']
    bandwidth = header['bandwid']
    energy_axis.range = CoordRange1D(
        RefCoord(0.5, wavelength - bandwidth / 2.0),
        RefCoord(1.5, wavelength + bandwidth / 2.0))

    spectral_axis = SpectralWCS(energy_axis, 'TOPOCENT')
    spectral_axis.ssysobs = 'TOPOCENT'
    spectral_axis.ssyssrc = 'TOPOCENT'
    spectral_axis.resolving_power = abs(wavelength / bandwidth)
    spectral_axis.bandpass_name = 'SCUBA-2-' + header['filter'] + 'um'

    return spectral_axis
