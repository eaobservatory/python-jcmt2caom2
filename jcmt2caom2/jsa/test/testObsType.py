#!/usr/bin/env python2.7
from __future__ import absolute_import

import unittest

from tools4caom2.logger import logger
from jcmt2caom2.jsa.obs_type import obs_type

class testTargetName( unittest.TestCase):
    """
    Test cases for the function target_name(object)
    """

    def test_target_name(self):
        test_data = [['science', 'grid', 'grid'],
                     ['science', 'jiggle', 'jiggle'],
                     ['science', 'raster', 'scan'],
                     ['science', 'scan', 'scan'],
                     ['pointing', 'anything', 'pointing'],
                     ['focus', 'anything', 'focus'],
                     ['skydip', 'anything', 'skydip'],
                     ['flatfield', 'anything', 'flatfield'],
                     ['setup', 'anything', 'setup'],
                     ['noise', 'anything', 'noise']]
        
        for jcmt_obs_type, sam_mode, retval in test_data:
            caom2_obs_type = obs_type(jcmt_obs_type, sam_mode)
            self.assertEqual(caom2_obs_type, retval,
                             'The value returned from obs_type("' + 
                             jcmt_obs_type + '", "' +
                             sam_mode + '") was "' + 
                             caom2_obs_type + '" but should have been "' + 
                             retval + '"')                              
