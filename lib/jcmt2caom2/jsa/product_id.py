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

from tools4caom2.error import CAOMError


def product_id(backend,
               product=None,
               restfreq=None,
               bwmode=None,
               subsysnr=None,
               filter=None):
    """
    Generates productID strings.

    Arguments:
    backend: one of ACSIS, DAS, AOS-C, SCUBA-2
    product: one of raw, raw-hybrid, or PRODUCT keyword
    restfreq: restfreq in Hz as a float
    bwmode: bandwidth mode as a string
    subsysnr: subsysnr for ACSIS-like observations as a string
    filter: wavelength in um for SCUBA-2 as a string

    Returns:
    productID string
    """
    separator = '-'
    if not product:
        raise CAOMError('product must be supplied to generate productID')

    if backend == 'SCUBA-2':
        if not filter:
            raise CAOMError(
                'filter must be supplied to generate SCUBA-2 productID')

        subsysnr_dict = {'450': '450um',
                         '850': '850um'}
        if filter in subsysnr_dict:
            productID = separator.join([product,
                                        subsysnr_dict[filter]])
        else:
            # Allow custom filters for special cases
            productID = separator.join([product,
                                        filter])

    else:
        if not restfreq or type(restfreq) != float:
            raise CAOMError(
                'restfreq must be supplied to generate heterodyne productID')

        if not bwmode or type(bwmode) != str:
            raise CAOMError(
                'bwmode must be supplied to generate heterodyne productID')

        if not subsysnr or type(subsysnr) != str:
            raise CAOMError(
                'subsysnr must be supplied to generate heterodyne productID')

        restfreqstr = '%.0fMHz' % (restfreq * 1.0e-6)

        productID = separator.join([product,
                                    restfreqstr,
                                    bwmode,
                                    subsysnr])

    return productID
