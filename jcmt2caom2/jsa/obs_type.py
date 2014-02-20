#!/usr/bin/env python2.7

#################################
# Import required Python modules
#################################

from jcmt2caom2.__version__ import version

def obs_type(jcmt_obs_type, sam_mode):
    """
    Standardizes the conventions for target names.  All names will be put 
    into upper case, leading and trailing white space will be stripped, and 
    multiple white space characters within the string will be converted to 
    single spaces.
    
    Arguments:
    jcmt_obs_type: the observation type (science or calibration mode)
    sam_mode: sampling mode, of interest for science observations
    
    Returns:
    string containing the CAOM-2 observation type
    
    Usage:
    For a raw observation:
        observation.target_name = target_name(common['obs_type'],
                                              common['sam_mode'])
    For processed data:
        self.add_to_plane_dict('OBSTYPE', 
                               obs_type(header['OBS_TYPE'],
                                        header['SAM_MODE']))
    """
    caom2_obs_type = jcmt_obs_type
    if jcmt_obs_type == "science":
        # raster is a synonym for scan
        if sam_mode == "raster":
            caom2_obs_type = "scan"
        else:
            caom2_obs_type = sam_mode

    return caom2_obs_type
    