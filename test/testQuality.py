#!/usr/bin/env python2.7
from __future__ import absolute_import

import os
import tempfile
import unittest

from tools4caom2.error import CAOMError

import jcmt2caom2.jsa.quality as q


class testJsaQA(unittest.TestCase):
    def testJSA_QA(self):
        """
        Test basic properties
        """

        # Reject TIME GAP classification values
        self.assertEquals(len(q.JSA_NAMES), len(q.JSA_VALUES),
                          'lengths of JSA_VALUES and JSA_NAMES are different')
        self.assertEquals(
            len(q.JCMT_NAMES), len(q.JCMT_VALUES),
            'lengths of JCMT_VALUES and JCMT_NAMES are different')

        self.assertRaises(CAOMError,
                          q.quality, -1)
        self.assertEquals(q.JSA_QA.GOOD,
                          q.quality(0).jsa_value())
        self.assertEquals(q.JSA_QA.FAILED_QA,
                          q.quality(1).jsa_value())
        self.assertEquals(q.JSA_QA.BAD,
                          q.quality(2).jsa_value())
        self.assertEquals(q.JSA_QA.FAILED_QA,
                          q.quality(3).jsa_value())
        self.assertEquals(q.JSA_QA.JUNK,
                          q.quality(4).jsa_value())
        self.assertRaises(CAOMError,
                          q.quality, 5)

        self.assertRaises(CAOMError,
                          q.quality, 11)
        self.assertRaises(CAOMError,
                          q.quality, 12)
        self.assertRaises(CAOMError,
                          q.quality, 13)

        self.assertEquals(
            q.JSA_QA.GOOD,
            q.quality(q.JCMT_QA.GOOD).jsa_value())
        self.assertEquals(
            q.JSA_QA.FAILED_QA,
            q.quality(q.JCMT_QA.QUESTIONABLE).jsa_value())
        self.assertEquals(
            q.JSA_QA.BAD,
            q.quality(q.JCMT_QA.BAD).jsa_value())
        self.assertEquals(
            q.JSA_QA.FAILED_QA,
            q.quality(q.JCMT_QA.REJECTED).jsa_value())
        self.assertEquals(
            q.JSA_QA.JUNK,
            q.quality(q.JCMT_QA.JUNK).jsa_value())

        self.assertEquals('GOOD', q.quality(0).jsa_name())
        self.assertEquals('FAILED_QA', q.quality(1).jsa_name())
        self.assertEquals('BAD', q.quality(2).jsa_name())
        self.assertEquals('FAILED_QA', q.quality(3).jsa_name())
        self.assertEquals('JUNK', q.quality(4).jsa_name())

        for i in range(len(q.JCMT_VALUES)):
            self.assertEquals(
                q.JCMT_NAMES[i],
                q.quality(q.JCMT_VALUES[i]).jcmt_name())

if __name__ == '__main__':
    unittest.main()
