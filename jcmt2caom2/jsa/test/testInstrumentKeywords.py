#!/usr/bin/env python2.7
from __future__ import absolute_import

import os
import tempfile
import unittest

from tools4caom2.logger import logger
from jcmt2caom2.jsa.instrument_keywords import instrument_keywords

class testInstrumentKeywords( unittest.TestCase):
    """
    Test cases for the function intent(obs_type, backend, sam_mode)
    """
    def setUp(self):
        # self.log = logger(os.path.expanduser('~/temp.log'))
        fh, self.logfile = tempfile.mkstemp()
        os.close(fh)
        self.log = logger(self.logfile)
    
    def tearDown(self):
        os.remove(self.logfile)
        
    def testInstrumentKeywords(self):
        test_data = [[{'backend': 'ACSIS',
                       'frontend': 'RxA3',
                       'sideband': 'USB',
                       'sideband_filter': 'DSB',
                       'switching_mode': 'pssw'}, 
                      {'raw': False,
                       'stdpipe': False,
                       'external': False},
                      ['RXA3', 'USB', 'DSB', 'PSSW']],
                      
                     [{'backend': 'ACSIS',
                       'frontend': 'HARP',
                       'sideband': ' LSB',
                       'sideband_filter': 'SSB',
                       'switching_mode': 'pssw'}, 
                      {'raw': False,
                       'stdpipe': False,
                       'external': False},
                      ['HARP', 'LSB', 'SSB', 'PSSW']],

                     [{'backend': 'SCUBA-2',
                       'frontend': 'SCUBA-2',
                       'inbeam': 'FTS',
                       'switching_mode': 'self'}, 
                      {'raw': False,
                       'stdpipe': False,
                       'external': False},
                      ['SCUBA-2', 'FTS', 'SELF']],

                     [{'backend': 'SCUBA-2',
                       'frontend': 'SCUBA-2',
                       'inbeam': 'POL FTS',
                       'switching_mode': 'self'}, 
                      {'raw': False,
                       'stdpipe': False,
                       'external': False},
                      ['SCUBA-2', 'POL', 'FTS', 'SELF']],

                     [{'backend': 'Scuba-2',
                       'frontend': 'Scuba-2',
                       'inbeam': 'SHUTTER',
                       'switching_mode': 'self'}, 
                      {'raw': False,
                       'stdpipe': False,
                       'external': False},
                      ['SCUBA-2', 'SELF']],

                     [{'backend': 'scuba-2',
                       'frontend': 'scuba-2',
                       'inbeam': 'shutter pol',
                       'switching_mode': 'self'}, 
                      {'raw': False,
                       'stdpipe': False,
                       'external': False},
                      ['SCUBA-2', 'POL', 'SELF']],

                     [# processed data can mix sidebands
                      {'backend': 'ACSIS',
                       'frontend': 'HARP',
                       'sideband_filter': 'SSB',
                       'switching_mode': 'pssw'}, 
                      {'raw': True,
                       'stdpipe': False,
                       'external': False},
                      ['HARP', 'SSB', 'PSSW']],

                     [# Missing frontend
                      {'backend': 'ACSIS',
                       'sideband': ' LSB',
                       'sideband_filter': 'SSB',
                       'switching_mode': 'pssw'}, 
                      {'raw': True,
                       'stdpipe': True,
                       'external': False},
                      ['LSB', 'SSB', 'PSSW']],

                     [# HARP cannot have been used with the DAS
                      {'backend': 'DAS',
                       'frontend': 'HARP',
                       'sideband': ' LSB',
                       'sideband_filter': 'SSB',
                       'switching_mode': 'pssw'}, 
                      {'raw': True,
                       'stdpipe': True,
                       'external': True},
                      []],

                     [# RXB cannot have been used with ACSIS
                      {'backend': 'ACSIS',
                       'frontend': 'RXB',
                       'sideband': 'LSB',
                       'sideband_filter': 'SSB',
                       'switching_mode': 'pssw'}, 
                      {'raw': True,
                       'stdpipe': True,
                       'external': True},
                      []],
                       
                     [# Invalid frontend
                      {'backend': 'ACSIS',
                       'frontend': 'rxq',
                       'sideband': 'LSB',
                       'sideband_filter': 'SSB',
                       'switching_mode': 'freqsw'},
                      {'raw': True,
                       'stdpipe': True,
                       'external': True},
                      []]]
        
        for strictness in ('raw', 'stdpipe', 'external'):
            for keyword_dict, strict_dict, retval in test_data:
                status, keyword_list = instrument_keywords(strictness,
                                                    keyword_dict,
                                                    self.log)
                if strict_dict[strictness]:
                    self.assertEqual(status, True,
                                 'The status returned from '
                                 'instrument_keywords("' + strictness +
                                 '", "' + repr(keyword_dict) +
                                 '") was ' + str(status) +
                                 ' but should have been True')  
                else:
                    self.assertEqual(keyword_list, retval,
                                 'The value returned from '
                                 'instrument_keywords("' + strictness +
                                 '", "' + repr(keyword_dict) +
                                 '") was "' + repr(keyword_list) +
                                 '" but should have been "' + repr(retval) + 
                                 '"')
