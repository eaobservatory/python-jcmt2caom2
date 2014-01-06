#!/usr/bin/env python2.7
from __future__ import absolute_import

import os
import tempfile
import unittest

from jcmt2caom2.jsa.intent import intent
from caom2.caom2_enums import ObservationIntentType as OIT

class testIntent( unittest.TestCase):
    """
    Test cases for the function intent(obs_type, backend, sam_mode)
    """

    def testIntent(self):
        test_data = [['calibration', 'ACSIS',   'focus',    OIT.CALIBRATION],
                     ['calibration', 'ACSIS',   'pointing', OIT.CALIBRATION],
                     ['science',     'ACSIS',   'raster',   OIT.SCIENCE],
                     ['calibration', 'DAS',     'focus',    OIT.CALIBRATION],
                     ['calibration', 'DAS',     'pointing', OIT.CALIBRATION],
                     ['science',     'DAS',     'raster',   OIT.SCIENCE],
                     ['calibration', 'AOS-C',   'focus',    OIT.CALIBRATION],
                     ['calibration', 'AOS-C',   'pointing', OIT.CALIBRATION],
                     ['science',     'AOS-C',   'raster',   OIT.SCIENCE],
                     ['calibration', 'SCUBA-2', 'focus',    OIT.CALIBRATION],
                     ['calibration', 'SCUBA-2', 'pointing', OIT.SCIENCE],
                     ['science',     'SCUBA-2', 'raster',   OIT.SCIENCE]]
        
        for obs_type, backend, sam_mode, retval in test_data:
            intentval = intent(obs_type, backend, sam_mode)
            self.assertEqual(intentval, retval,
                             'The value returned from intent("' + obs_type +
                             '", "' + backend +
                             '", "' + sam_mode +
                             '") was "' + str(intentval) +
                             '" but should have been "' + str(retval) + '"')
