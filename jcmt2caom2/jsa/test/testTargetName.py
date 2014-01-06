#!/usr/bin/env python2.7
from __future__ import absolute_import

import re
import unittest

from jcmt2caom2.jsa.target_name import target_name
from caom2.caom2_enums import ObservationIntentType as OIT

class testTargetName( unittest.TestCase):
    """
    Test cases for the function target_name(object)
    """

    def test_target_name(self):
        test_data = [['venus', 'VENUS'],
                     ['Venus', 'VENUS'],
                     ['VENUS', 'VENUS'],
                     ['IRC+10216', 'IRC+10216'],
                     ['irc  +  10216', 'IRC + 10216'],
                     [' IRC +10216 ', 'IRC +10216']]
        
        for objectname, retval in test_data:
            target = target_name(objectname)
            self.assertEqual(target, retval,
                             'The value returned from target_name("' + 
                             objectname + '") was "' + target +
                             '" but should have been "' + retval + '"')
