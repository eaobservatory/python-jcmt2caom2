#################################
# Import required Python modules
#################################
from caom2.caom2_enums import ObservationIntentType

from jcmt2caom2.__version__ import version


def intent(obs_type, backend):
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
        observation.intent = intent(common['obs_type'],
                                    common['backend'])
    For processed data:
        self.add_to_plane_dict('obs.intent',
                               intent(header['OBS_TYPE'],
                                      header['BACKEND']).value)
    """
    intent_value = ObservationIntentType.CALIBRATION
    if (obs_type == 'science' or
            (obs_type == 'pointing' and backend == 'SCUBA-2')):

        intent_value = ObservationIntentType.SCIENCE

    return intent_value
