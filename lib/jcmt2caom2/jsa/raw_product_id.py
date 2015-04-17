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

import logging

from tools4caom2.error import CAOMError

from jcmt2caom2.jsa.product_id import product_id

logger = logging.getLogger(__name__)


def raw_product_id(backend, obsid, conn):
    """
    Generates raw (observationID, productID) values for an observation.

    Arguments:
    backend: one of ACSIS, DAS, AOS-C, SCUBA-2
    obsid: observation identifier, primary key in COMMON table
    conn: connection to database

    Returns:
    return a dictionary of productID keyed on
    subsysnr (filter for SCUBA-2)
    """
    if backend == 'SCUBA-2':
        subsysnr_dict = {'450': 'raw-450um',
                         '850': 'raw-850um'}

    else:
        subsysnr_dict = {}

        result = conn.get_heterodyne_product_info(backend, obsid)

        if result:
            for subsysnr, restfreq, bwmode, specid, hybrid in result:
                restfreqhz = 1.0e9 * float(restfreq)
                prefix = 'raw'
                if int(hybrid) > 1:
                    prefix = 'raw-hybrid'
                subsysnr_dict[str(subsysnr)] = product_id(backend,
                                                          product=prefix,
                                                          restfreq=restfreqhz,
                                                          bwmode=bwmode,
                                                          subsysnr=str(specid))
        else:
            raise CAOMError('no rows returned from ACSIS for obsid = ' + obsid)


    return subsysnr_dict
