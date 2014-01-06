#!/usr/bin/env python2.7
from __future__ import absolute_import

import os
import tempfile
import unittest

import jcmt2caom2.jsa.quality as q
from tools4caom2.logger import logger


class testJsaQA( unittest.TestCase):
    def setUp(self):
        # self.log = logger(os.path.expanduser('~/temp.log'))
        fh, self.logfile = tempfile.mkstemp()
        os.close(fh)
        self.log = logger(self.logfile)
    
    def tearDown(self):
        os.remove(self.logfile)
        
    def testJSA_QA(self):
        """
        Test basic properties
        """
        self.assertEquals(len(q.JSA_NAMES), len(q.JSA_VALUES),
                          'lengths of JSA_VALUES and JSA_NAMES are different')
        self.assertEquals(len(q.JCMT_NAMES), len(q.JCMT_VALUES),
                          'lengths of JCMT_VALUES and JCMT_NAMES are different')
        
        self.assertRaises(logger.LoggerError, q.quality, -1, self.log)
        self.assertEquals(q.JSA_QA.GOOD,      
                          q.quality(  0, self.log).jsa_value())
        self.assertEquals(q.JSA_QA.FAILED_QA, 
                          q.quality(  1, self.log).jsa_value())
        self.assertEquals(q.JSA_QA.BAD,       
                          q.quality(  2, self.log).jsa_value())
        self.assertEquals(q.JSA_QA.FAILED_QA, 
                          q.quality(  3, self.log).jsa_value())
        self.assertEquals(q.JSA_QA.JUNK,      
                          q.quality(  4, self.log).jsa_value())
        self.assertRaises(logger.LoggerError, q.quality,  5, self.log)
        
        # Reject TIME GAP classification values
        self.assertRaises(logger.LoggerError, q.quality, 11, self.log)
        self.assertRaises(logger.LoggerError, q.quality, 12, self.log)
        self.assertRaises(logger.LoggerError, q.quality, 13, self.log)

        self.assertEquals(
            q.JSA_QA.GOOD, 
            q.quality(q.JCMT_QA.GOOD, self.log).jsa_value())
        self.assertEquals(
            q.JSA_QA.FAILED_QA, 
            q.quality(q.JCMT_QA.QUESTIONABLE, self.log).jsa_value())
        self.assertEquals(
            q.JSA_QA.BAD, 
            q.quality(q.JCMT_QA.BAD, self.log).jsa_value())
        self.assertEquals(
            q.JSA_QA.FAILED_QA, 
            q.quality(q.JCMT_QA.REJECTED, self.log).jsa_value())
        self.assertEquals(
            q.JSA_QA.JUNK, 
            q.quality(q.JCMT_QA.JUNK, self.log).jsa_value())
        
        self.assertEquals('GOOD', q.quality(0, self.log).jsa_name())
        self.assertEquals('FAILED_QA', q.quality(1, self.log).jsa_name())
        self.assertEquals('BAD', q.quality(2, self.log).jsa_name())
        self.assertEquals('FAILED_QA', q.quality(3, self.log).jsa_name())
        self.assertEquals('JUNK', q.quality(4, self.log).jsa_name())

        for i in range(len(q.JCMT_VALUES)):
            self.assertEquals(q.JCMT_NAMES[i], 
                              q.quality(q.JCMT_VALUES[i], self.log).jcmt_name())

if __name__ == '__main__':
    unittest.main()
    
