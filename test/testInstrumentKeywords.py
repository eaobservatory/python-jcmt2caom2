#!/usr/bin/env python2.7
from __future__ import absolute_import

import unittest

from jcmt2caom2.jsa.instrument_keywords import instrument_keywords


class testInstrumentKeywords(unittest.TestCase):
    """
    Test cases for the function intent(obs_type, backend, sam_mode)
    """
    def testInstrumentKeywords(self):
        test_data = [['ACSIS',
                      'RxA3',
                      {'sideband': 'USB',
                       'sideband_filter': 'DSB',
                       'switching_mode': 'pssw'},
                      {'raw': False,
                       'stdpipe': False,
                       'external': False},
                      ['USB', 'DSB', 'PSSW']],

                     ['ACSIS',
                      'HARP',
                      {'sideband': ' LSB',
                       'sideband_filter': 'SSB',
                       'switching_mode': 'pssw'},
                      {'raw': False,
                       'stdpipe': False,
                       'external': False},
                      ['LSB', 'SSB', 'PSSW']],

                     ['SCUBA-2',
                      'SCUBA-2',
                      {'inbeam': 'FTS',
                       'switching_mode': 'self'},
                      {'raw': False,
                       'stdpipe': False,
                       'external': False},
                      ['SELF']],

                     ['SCUBA-2',
                      'SCUBA-2',
                      {'inbeam': 'POL FTS',
                       'switching_mode': 'self'},
                      {'raw': False,
                       'stdpipe': False,
                       'external': False},
                      ['SELF']],

                     ['SCUBA-2',
                      'SCUBA-2',
                      {'inbeam': 'SHUTTER',
                       'switching_mode': 'self'},
                      {'raw': False,
                       'stdpipe': False,
                       'external': False},
                      ['SELF']],

                     ['SCUBA-2',
                      'SCUBA-2',
                      {'inbeam': 'shutter pol',
                       'switching_mode': 'self'},
                      {'raw': False,
                       'stdpipe': False,
                       'external': False},
                      ['SELF']],

                     # processed data can mix sidebands
                     ['ACSIS',
                      'HARP',
                      {'sideband_filter': 'SSB',
                       'switching_mode': 'pssw'},
                      {'raw': True,
                       'stdpipe': False,
                       'external': False},
                      ['SSB', 'PSSW']]]

        for strictness in ('raw', 'stdpipe', 'external'):
            for backend, frontend, keyword_dict, strict_dict, retval \
                    in test_data:
                status, keyword_list = instrument_keywords(strictness,
                                                           frontend,
                                                           backend,
                                                           keyword_dict)
                if strict_dict[strictness]:
                    self.assertEqual(
                        status, True,
                        'The status returned from '
                        'instrument_keywords("' + strictness +
                        '", "' + repr(keyword_dict) +
                        '") was ' + str(status) +
                        ' but should have been True')
                else:
                    self.assertEqual(
                        keyword_list, retval,
                        'The value returned from '
                        'instrument_keywords("' + strictness +
                        '", "' + repr(keyword_dict) +
                        '") was "' + repr(keyword_list) +
                        '" but should have been "' + repr(retval) +
                        '"')
