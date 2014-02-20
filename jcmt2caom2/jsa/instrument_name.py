#!/usr/bin/env python2.7

#################################
# Import required Python modules
#################################
import logging
import re

from tools4caom2.logger import logger

from jcmt2caom2.__version__ import version

# global dictionary of permitted combinations of values by backend
frontends = {'ACSIS': ('HARP', 'RXA3', 'RXWB','RXWD2'),
             'DAS': ('RXA', 'RXA2', 'RXA3', 'RXB', 'RXB2', 'RXB3I', 'RXB3', 
                     'RXC', 'RXC2', 'RXWCD', 'RXWD', 'MPIRXE'),
             'AOS-C': ('RXA', 'RXA2', 'RXB', 'RXB2', 'RXB3', 'RXC', 'RXC2')
            }
continuum = ('SCUBA-2', 'SCUBA')

def instrument_name(frontend, backend, log):
    """
    Generates an unambigous name for Instrument.name.

    Continuum instruments intended for sciencerather than calibration 
    (e.g. SCUBA-2) combine the detection of photons and the conversion of 
    the signal into binary data in a single package.  Heterodyne receivers, by 
    contrast, divide this process between two components known as the "frontend" 
    or receiver, and the "backend" or spectrometer.  The frontend converts the 
    photons from the sky into one or more electrical signals.  The backend 
    converts the electrical signal(s) into binary data.  The complete 
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
    log: a tools4caom2.logger logger object
    
    Returns a string giving the instrument name.
    """
    
    # Sanitize the frontend and backend names
    myFrontend = frontend.upper() if frontend else 'UNKNOWN'
    
    if myFrontend in continuum:
        instrument = myFrontend
    else:
        myBackend = backend.upper() if backend else 'UNKNOWN'
        if myBackend not in frontends:
            log.console('backend = ' + myBackend + ' should be one of ' + 
                        repr(sorted(frontends.keys())), 
                        logging.WARN)

        if myFrontend not in frontends[myBackend]:
            log.console('frontend = ' + myFrontend + ' should be one of ' + 
                        repr(sorted(frontends[myBackend])), 
                        logging.WARN)

        instrument = myFrontend + '-' + myBackend

    return instrument
            
    