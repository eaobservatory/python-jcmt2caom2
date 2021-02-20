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
import re

logger = logging.getLogger(__name__)

# global dictionary of permitted combinations of values by backend
frontends = {
    'ACSIS': ('HARP', 'RXA3', 'RXA3M', 'RXWB', 'RXWD2',
              'UU', 'ALAIHI', 'AWEOWEO'),
    'DAS': ('RXA', 'RXA2', 'RXA3', 'RXB', 'RXB2', 'RXB3I', 'RXB3',
            'RXC', 'RXC2', 'RXWCD', 'RXWD', 'MPIRXE'),
    'AOS-C': ('RXA', 'RXA2', 'RXB', 'RXB2', 'RXB3', 'RXC', 'RXC2')
}
continuum = ('SCUBA-2', 'SCUBA')


def instrument_name(frontend, backend, inbeam):
    """
    Generates an unambigous name for Instrument.name.

    Continuum instruments intended for sciencerather than calibration
    (e.g. SCUBA-2) combine the detection of photons and the conversion of
    the signal into binary data in a single package.  Heterodyne receivers, by
    contrast, divide this process between two components known as the
    "frontend" or receiver, and the "backend" or spectrometer.  The frontend
    converts the photons from the sky into one or more electrical signals.  The
    backend converts the electrical signal(s) into binary data.  The complete
    instrument is comprised of both parts, so the instrument name will be
    constructed by joining the frontend and backend names with a hyphen, e.g.:
        HARP-ACSIS
        RXA3-DAS
    Instrument names for the continuum detectors only need the one name, which
    will be taken from the frontend, e.g.:
        SCUBA-2

    Arguments:
    frontend: the receiver name
    backend: the spectrometer name
    inbeam: string containing list of subinstruments in the beam

    Returns a string giving the instrument name.
    """
    separator = '-'

    # Sanitize the frontend and backend names
    myFrontend = frontend.upper() if frontend else 'UNKNOWN'
    parts = []

    myBackend = backend.upper() if backend else 'UNKNOWN'

    myInbeam = inbeam.upper() if inbeam else ''

    if re.search(r'POL', myInbeam):
        if myFrontend == 'SCUBA-2':
            parts.append('POL2')
        else:
            parts.append('POL')

    if re.search(r'FTS2', myInbeam):
            parts.append('FTS2')

    parts.append(myFrontend)

    if myBackend in frontends:
        parts.append(myBackend)

        if myFrontend not in frontends[myBackend]:
            logger.warning('frontend = %s should be one of %s',
                           myFrontend,
                           repr(sorted(frontends[myBackend])))
    elif myFrontend not in continuum:
        logger.warning('frontend = %s should be one of %s',
                       myFrontend, repr(sorted(continuum)))

    instrument = separator.join(parts)

    return instrument
