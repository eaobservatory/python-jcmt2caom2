#!/usr/bin/env python2.7
from __future__ import absolute_import

import os
import tempfile
import unittest

from jcmt2caom2.jsa.intent import intent
from caom2.caom2_enums import ObservationIntentType as OIT


class testIntent(unittest.TestCase):
    """
    Test cases for the function intent(obs_type, backend, sam_mode)
    """

    def testIntent(self):
        test_data = [['focus',    'ACSIS',   OIT.CALIBRATION],
                     ['pointing', 'ACSIS',   OIT.CALIBRATION],
                     ['science',  'ACSIS',   OIT.SCIENCE],
                     ['focus',    'DAS',     OIT.CALIBRATION],
                     ['pointing', 'DAS',     OIT.CALIBRATION],
                     ['science',  'DAS',     OIT.SCIENCE],
                     ['focus',    'AOS-C',   OIT.CALIBRATION],
                     ['pointing', 'AOS-C',   OIT.CALIBRATION],
                     ['science',  'AOS-C',   OIT.SCIENCE],
                     ['focus',    'SCUBA-2', OIT.CALIBRATION],
                     ['pointing', 'SCUBA-2', OIT.SCIENCE],
                     ['science',  'SCUBA-2', OIT.SCIENCE]]

        for obs_type, backend, retval in test_data:
            intentval = intent(obs_type, backend)
            self.assertEqual(intentval, retval,
                             'The value returned from intent("' + obs_type +
                             '", "' + backend +
                             '") was "' + str(intentval) +
                             '" but should have been "' + str(retval) + '"')
