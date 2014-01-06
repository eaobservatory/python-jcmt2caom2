#!/usr/bin/env python2.7

#################################
# Import required Python modules
#################################
import logging
from caom2.caom2_enums import ObservationIntentType

from jcmt2caom2.__version__ import version

def intent(obs_type, backend, sam_mode):
    """
    Generates values for the CAOM-2 field Observation.intent.
    Mostly, if the JCMT designated an observation as 'calibration' or 'science'
    that assessment is considered correct.  However, some calibration mode
    observations are perfectly usable for science if reasonable care is taken
    to assess the quality of the data and reduce it properly.
    
    Arguments:
    obs_type: the JCMT intent for the observation
    backend: one of ACSIS, DAS, AOS-C, SCUBA-2
    obsmode: observing mode for science observations
    
    Returns:
    ObservationIntentType.(SCIENCE|CALIBRATION)
    
    Usage:
    For a raw observation:
        myintent = intent(self.log)
        observation.intent = myintent(common['obs_type'],
                                      common['backend'],
                                      common['sam_mode'])
    For processed data:
        myintent = intent(self.log)
        self.add_to_plane_dict('obs.intent', 
                               myintent(header['OBS_TYPE'],
                                        header['BACKEND'],
                                        header['SAM_MODE']).value)
    """
    if obs_type == 'science':
        intent_value = ObservationIntentType.SCIENCE
    else: 
        intent_value = ObservationIntentType.CALIBRATION
    
    if backend == 'SCUBA-2' and sam_mode == 'pointing':
        intent_value = ObservationIntentType.SCIENCE

    return intent_value