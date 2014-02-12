#!/usr/bin/env python2.7

#################################
# Import required Python modules
#################################
import logging
import re

from tools4caom2.logger import logger

from jcmt2caom2.__version__ import version

# global dictionary of permitted combinations of values by backend
permitted = {'ACSIS': {'inbeam': ["POL"],
                       'sideband': ["LSB", "USB"],
                       'sideband_filter': ["DSB", "SSB"],
                       'switching_mode': ["CHOP", "FREQSW", "NONE", "PSSW"]
                      },
                       
             'DAS': {'inbeam': ["ROVER"],
                     'sideband': ["LSB", "USB"],
                     'sideband_filter': ["DSB", "SSB"],
                     'switching_mode': ["CHOP", "FREQSW", "NONE", "PSSW"]
                    },
             'AOS-C': {'sideband': ["LSB", "USB"],
                       'sideband_filter': ["DSB", "SSB"],
                       'switching_mode': ["CHOP", "FREQSW", "NONE", "PSSW"]
                      },
             'SCUBA-2': {'inbeam': ["BLACKBODY", "FTS2", "POL", "POL2_CAL", 
                                    "POL2_WAVE", "POL2_ANA", "SHUTTER"],
                         'switching_mode': ["NONE", "SELF"]
                        }
            }
receiver_sideband_modes = {'HARP': ['SSB'],
                           'RXA3': ['DSB', 'SSB'],
                           'RXWB': ['DSB', 'SSB'],
                           'RXWD2': ['DSB', 'SSB']}

def instrument_keywords(strictness, frontend, backend, keyword_dict, log):
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
        subsys_bwmode: bandwidth mode
    log: a tools4caom2.logger logger object
    
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
            keyword_dict['subsys_bwmode'] = subsystem['bwmode']
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
            keyword_dict['subsys_bwmode'] = header['BWMODE']
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
        log.console('instrument_keywords does not recognize ' + 
                    backend + ' as a permitted backend',
                    logging.WARN)
        bad = True
    else:
        # The remaining checks only work if backend is permitted
        if myBackend in ('ACSIS', 'DAS', 'AOS-C'):
            if 'sideband' not in keyword_dict and strictness == 'raw':
                log.console('with strictness = ' + str(strictness) +
                            ' backend = ' + backend +
                            ' frontend = ' + frontend +
                            ' sideband is not defined', logging.WARN)
                bad = True
            if 'sideband' in keyword_dict:
                sideband = keyword_dict['sideband'].strip().upper()
                if sideband not in permitted[myBackend]['sideband']:
                    log.console('sideband ' + sideband + 
                                ' is not in the list permited for ' +
                                myBackend + ': ' +
                                repr(permitted[myBackend]['sideband']), 
                                logging.WARN)
                    bad = True
                
            if ('sideband_filter' not in keyword_dict and 
                strictness != 'external'):
                    
                log.console('sideband_filter is not defined', logging.WARN)
                bad = True
            if 'sideband_filter' in keyword_dict:
                sideband_filter = \
                    keyword_dict['sideband_filter'].strip().upper()
                if sideband_filter not in permitted[myBackend]['sideband_filter']:
                    log.console('sideband_filter ' + sideband_fileter + 
                                ' is not in the list permited '
                                'for ' +
                                backend + ': ' +
                                repr(permitted[myBackend]['sideband_filter']), 
                                logging.WARN)
                    bad = True
        else:
            if 'sideband' in keyword_dict:
                log.console('sideband is not permitted for ' + 
                            backend, 
                            logging.WARN)
                bad = True
                
            if 'sideband_filter' in keyword_dict:
                log.console('sideband_filter is not permitted for ' + 
                            backend, 
                            logging.WARN)
                bad = True

            if 'subsys_bwmode' in keyword_dict:
                log.console('subsys_bwmode is not permitted for ' + 
                            backend, 
                            logging.WARN)
                bad = True

        if 'switching_mode' not in keyword_dict and strictness == 'raw':
            log.console('switching_mode is not defined', logging.WARN)
            bad = True
        if 'switching_mode' in keyword_dict:
            switching_mode = keyword_dict['switching_mode'].strip().upper()
            if switching_mode not in permitted[myBackend]['switching_mode']:
                log.console('switching_mode ' + switching_mode +
                            ' is not in the list permited '
                            'for ' +
                            backend + ': ' +
                            repr(permitted[myBackend]['switching_mode']), 
                            logging.WARN)
                bad = True
                        
    # If there were no actual errors, compose the keyword list
    keywords = []
    if not bad:
        for key in sorted(keyword_dict.keys()):
            if key == 'inbeam':
                inbeam_list = re.split(r'\s+', 
                                       keyword_dict['inbeam'].strip().upper())
                for item in inbeam_list:
                    if item != 'SHUTTER':
                        keywords.append(item)
            else:
                keywords.append(keyword_dict[key].strip().upper())
    
    return (bad, keywords)
