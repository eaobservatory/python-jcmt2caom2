#!/usr/bin/env python2.7

#################################
# Import required Python modules
#################################
import logging
import re

from jcmt2caom2.__version__ import version

logger = logging.getLogger(__name__)

# global dictionary of permitted combinations of values by backend
permitted = {
    'ACSIS': {
        'inbeam': ["POL"],
        'sideband': ["LSB", "USB"],
        'sideband_filter': ["DSB", "SSB"],
        'switching_mode': ["CHOP", "FREQSW", "NONE", "PSSW"]
    },

    'DAS': {
        'inbeam': ["ROVER"],
        'sideband': ["LSB", "USB"],
        'sideband_filter': ["DSB", "SSB", "UNKNOWN"],
        'switching_mode': ["CHOP", "FREQSW", "NONE", "PSSW"]
    },

    'AOS-C': {
        'sideband': ["LSB", "USB"],
        'sideband_filter': ["DSB", "SSB", 'UNKNOWN'],
        'switching_mode': ["CHOP", "FREQSW", "NONE", "PSSW"]
    },

    'SCUBA-2': {
        'inbeam': ["BLACKBODY", "FTS2", "POL", "POL2_CAL",
                   "POL2_WAVE", "POL2_ANA", "SHUTTER"],
        'switching_mode': ["NONE", "SELF", "SPIN"]
    }
}
receiver_sideband_modes = {'HARP': ['SSB'],
                           'RXA3': ['DSB', 'SSB'],
                           'RXWB': ['DSB', 'SSB'],
                           'RXWD2': ['DSB', 'SSB']}


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
                but ignores missing keywords and always returns bad=False.
    frontend: receiver name
    backend: spectrometer name for heterodyne observations
    keyword_dict: a dictionary containing candidate keywords.  Keys for the
                dictionary include:
        inbeam: list of optical devices in the optical path
        x_scan_pat: scan pattern (x makes it the last item in a sorted list)
        sideband: for heterodyne observations, the signal sideband (USB, LSB)
        sideband_mode: single or double sideband (SSB, DSB)
        swiching_mode: the switching mode in use

    Returns a tuple containing:
    bad: True if an error was encountered, False otherwise
    keywords: a list containing the keywords to be used

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
        mybad, keywords = instrument_keywords('raw', keyword_dict)
    For processed data:
        keyword_dict = {}
        keyword_dict['frontend'] = header['INSTRUME']
        keyword_dict['backend'] = header['BACKEND']
        keyword_dict['switching_mode'] = header['SW_MODE']
        keyword_dict['inbeam'] = header['INBEAM']
        if header['BACKEND'] in ('ACSIS', 'DAS', 'AOS-C'):
            keyword_dict['sideband'] = header['OBS_SB']
            keyword_dict['sieband_filter'] = header['SB_MODE']
        mybad, keywords = instrument_keywords('stdpipe',
                                              frontend,
                                              backend,
                                              keyword_dict)
    """
    bad = False

    # The backend is not mandatory for external data products, but the
    # rest of the backend-dependent validity checks must then be skipped

    # This first block of code just reports warnings and sets bad to T
    myBackend = backend.strip().upper()
    myFrontend = frontend.strip().upper()

    if myBackend not in permitted:
        logger.warning('instrument_keywords does not recognize ' +
                       '"%s" as a permitted backend', backend)
        bad = True
    else:
        # The remaining checks only work if backend is permitted
        if myBackend in ('ACSIS', 'DAS', 'AOS-C'):
            if 'sideband' not in keyword_dict and strictness == 'raw':
                logger.warning('with strictness = %s'
                               ' backend = %s'
                               ' frontend = %s'
                               ' sideband is not defined',
                               strictness, backend, frontend)
                bad = True
            if 'sideband' in keyword_dict:
                sideband = keyword_dict['sideband'].strip().upper()
                if sideband not in permitted[myBackend]['sideband']:
                    logger.warning('sideband %s'
                                   ' is not in the list permited for %s: %s',
                                   sideband, myBackend,
                                   repr(permitted[myBackend]['sideband']))
                    bad = True

            if ('sideband_filter' not in keyword_dict and
                    strictness != 'external'):

                logger.warning('sideband_filter is not defined')
                bad = True
            if 'sideband_filter' in keyword_dict:
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
                    logger.warning(
                        'sideband_filter %s'
                        ' is not in the list permited for %s: %s',
                        sideband_filter, backend,
                        repr(permitted[myBackend]['sideband_filter']))
                    bad = True
        else:
            if 'sideband' in keyword_dict:
                logger.warning('sideband is not permitted for %s',
                               backend)
                bad = True

            if 'sideband_filter' in keyword_dict:
                logger.warning('sideband_filter is not permitted for %s',
                               backend)
                bad = True

        if 'switching_mode' not in keyword_dict and strictness == 'raw':
            logger.warning('switching_mode is not defined')
            bad = True
        if 'switching_mode' in keyword_dict:
            switching_mode = keyword_dict['switching_mode'].strip().upper()
            # DAS observations often have 'FREQ' instead of 'FREQSW'
            if switching_mode == 'FREQ':
                switching_mode = 'FREQSW'
                keyword_dict['switching_mode'] = switching_mode

            if switching_mode not in permitted[myBackend]['switching_mode']:
                logger.warning('switching_mode %s'
                               ' is not in the list permited for %s: %s',
                               switching_mode, backend,
                               repr(permitted[myBackend]['switching_mode']))
                bad = True

    # If there were no actual errors, compose the keyword list
    keywords = []
    if not bad:
        for key in sorted(keyword_dict.keys()):
            if key == 'inbeam':
                inbeam_list = re.split(r'\s+',
                                       keyword_dict['inbeam'].strip().upper())
                for item in inbeam_list:
                    if not re.search(r'POL|FTS|SHUTTER', item):
                        keywords.append(item)
            else:
                keywords.append(keyword_dict[key].strip().upper())

    return (bad, keywords)
