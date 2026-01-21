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

from tools4caom2.error import CAOMError

logger = logging.getLogger(__name__)

# global dictionary of permitted combinations of values by backend
permitted = {
    'ACSIS': {
        'inbeam': ['WAVEPLATE'],
        'sideband': ["LSB", "USB"],
        'sideband_filter': ["DSB", "SSB", "2SB"],
        'switching_mode': ["CHOP", "FREQSW", "NONE", "PSSW"]
    },

    'DAS': {
        'inbeam': ["ROVER"],
        'sideband': ["LSB", "USB"],
        'sideband_filter': ["DSB", "SSB", "UNKNOWN"],
        'switching_mode': ["CHOP", "FREQSW", "NONE", "PSSW"]
    },

    'AOS-C': {
        'inbeam': [],
        'sideband': ["LSB", "USB"],
        'sideband_filter': ["DSB", "SSB", 'UNKNOWN'],
        'switching_mode': ["CHOP", "FREQSW", "NONE", "PSSW"]
    },

    'SCUBA-2': {
        'inbeam': ['BLACKBODY'],
        'switching_mode': ["NONE", "SELF", "SPIN"]
    }
}


def instrument_keywords(strictness, frontend, backend, keyword_dict):
    """
    Generates a list of keywords for the CAOM-2 field Instrument.keywords.

    Keywords to add are passed in through a dictionary, which allows special
    processing to be applied to particular keywords.

    Arguments:
    strictness: one of 'raw', 'stdpipe', or 'external', where raw means every
                violation of standards is reported as an error, 'stdpipe'
                allows some missing values that can legitimately be dropped
                during normal processing, and 'external' reports invalid values
                but ignores missing keywords does not raise exceptions.
    frontend: receiver name
    backend: spectrometer name for heterodyne observations
    keyword_dict: a dictionary containing candidate keywords.  Keys for the
                dictionary include:
        inbeam: list of optical devices in the optical path
        x_scan_pat: scan pattern (x makes it the last item in a sorted list)
        sideband: for heterodyne observations, the signal sideband (USB, LSB)
        sideband_mode: single or double sideband (SSB, DSB)
        switching_mode: the switching mode in use

    Returns:
    keywords: a list containing the keywords to be used

    Raises:
    CAOMError if an error is encountered.

    Usage: (omitting error checking)
    For a raw observation:
        frontend = common['instrume'].upper()
        backend = common['backend'].upper()
        keyword_dict = {}
        keyword_dict['switching_mode'] = common['sw_mode']
        keyword_dict['inbeam'] = common['inbeam']
        if backend in ('ACSIS', 'DAS', 'AOS-C'):
            keyword_dict['sideband'] = subsystem['obs_sb']
            keyword_dict['sieband_mode'] = subsystem['sb_mode']
        keywords = instrument_keywords('raw', keyword_dict)
    For processed data:
        keyword_dict = {}
        keyword_dict['frontend'] = header['INSTRUME']
        keyword_dict['backend'] = header['BACKEND']
        keyword_dict['switching_mode'] = header['SW_MODE']
        keyword_dict['inbeam'] = header['INBEAM']
        if header['BACKEND'] in ('ACSIS', 'DAS', 'AOS-C'):
            keyword_dict['sideband'] = header['OBS_SB']
            keyword_dict['sieband_filter'] = header['SB_MODE']
        keywords = instrument_keywords('stdpipe',
                                       frontend,
                                       backend,
                                       keyword_dict)
    """

    if strictness not in ('raw', 'stdpipe', 'external'):
        raise CAOMError('Unknown strictness "{}"'.format(strictness))

    # The backend is not mandatory for external data products, but the
    # rest of the backend-dependent validity checks must then be skipped

    # This first block of code just reports errors
    myBackend = backend.strip().upper()
    myFrontend = frontend.strip().upper()

    if myBackend not in permitted:
        raise CAOMError('instrument_keywords does not recognize ' +
                        '"%s" as a permitted backend' % backend)

    else:
        # The remaining checks only work if backend is permitted
        if myBackend in ('ACSIS', 'DAS', 'AOS-C'):
            if 'sideband' not in keyword_dict and strictness == 'raw':
                raise CAOMError('with strictness = %s'
                                ' backend = %s'
                                ' frontend = %s'
                                ' sideband is not defined'
                                % (strictness, backend, frontend))

            if 'sideband' in keyword_dict:
                sideband = keyword_dict['sideband'].strip().upper()
                if sideband not in permitted[myBackend]['sideband']:
                    raise CAOMError('sideband %s'
                                    ' is not in the list permited for %s: %r'
                                    % (sideband, myBackend,
                                       permitted[myBackend]['sideband']))

            if 'sideband_filter' not in keyword_dict:
                if strictness != 'external':
                    raise CAOMError('sideband_filter is not defined')

            else:
                sideband_filter = \
                    keyword_dict['sideband_filter'].strip().upper()

                # Sideband filter was not recorded before 1994-04-14 and is
                # stored as a blank '' in ACSIS for DAS and AOS-C data.  It can
                # be interpretted into DSB for every receiver except RXB3,
                # where a blank really means UNKNOWN, forcing the keyword to be
                # omitted.
                if sideband_filter == '' and frontend != 'RXB3':
                    sideband_filter = 'DSB'
                    keyword_dict['sideband_filter'] = sideband_filter

                elif sideband_filter == 'UNKNOWN' and backend in ['DAS',
                                                                  'AOS-C']:
                    del keyword_dict['sideband_filter']

                elif (sideband_filter not in
                        permitted[myBackend]['sideband_filter']):
                    raise CAOMError(
                        'sideband_filter %s'
                        ' is not in the list permited for %s: %r'
                        % (sideband_filter, backend,
                           permitted[myBackend]['sideband_filter']))

        else:
            if 'sideband' in keyword_dict:
                raise CAOMError(
                    'sideband is not permitted for %s' % backend)

            if 'sideband_filter' in keyword_dict:
                raise CAOMError(
                    'sideband_filter is not permitted for %s' % backend)

        if 'switching_mode' not in keyword_dict:
            if strictness == 'raw':
                raise CAOMError('switching_mode is not defined')

        else:
            switching_mode = keyword_dict['switching_mode'].strip().upper()

            # DAS observations often have 'FREQ' instead of 'FREQSW'
            if switching_mode == 'FREQ':
                switching_mode = 'FREQSW'
                keyword_dict['switching_mode'] = switching_mode

            if switching_mode not in permitted[myBackend]['switching_mode']:
                raise CAOMError('switching_mode %s'
                                ' is not in the list permited for %s: %r'
                                % (switching_mode, backend,
                                   permitted[myBackend]['switching_mode']))

    # If there were no actual errors, compose the keyword list
    keywords = []
    for key in sorted(keyword_dict.keys()):
        if key == 'inbeam':
            inbeam_list = re.split(r'\s+',
                                   keyword_dict['inbeam'].strip().upper())

            for item in inbeam_list:
                if re.search(r'POL|FTS|SHUTTER', item):
                    continue

                if item not in permitted[myBackend]['inbeam']:
                    raise CAOMError(
                        'inbeam entry "%s" is not permitted for %s: %r'
                        % (item, backend, permitted[myBackend]['inbeam']))

                keywords.append(item)

        else:
            keywords.append(keyword_dict[key].strip().upper())

    return keywords
