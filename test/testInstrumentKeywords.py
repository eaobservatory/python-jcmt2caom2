# Copyright (C) 2014-2015 Science and Technology Facilities Council.
# Copyright (C) 2015 East Asian Observatory.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import absolute_import

import unittest

from tools4caom2.error import CAOMError

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

                     ['ACSIS',
                      'UU',
                      {'sideband': 'LSB',
                       'sideband_filter': '2SB',
                       'switching_mode': 'pssw',
                       'inbeam': 'WAVEPLATE'},
                      {'raw': False,
                       'stdpipe': False,
                       'external': False},
                       ['WAVEPLATE', 'LSB', '2SB', 'PSSW']],

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
                args = [strictness, frontend, backend, keyword_dict]

                if strict_dict[strictness]:
                    with self.assertRaises(CAOMError):
                        instrument_keywords(*args)

                else:
                    keyword_list = instrument_keywords(*args)

                    self.assertEqual(
                        keyword_list, retval,
                        'The value returned from '
                        'instrument_keywords("' + strictness +
                        '", "' + repr(keyword_dict) +
                        '") was "' + repr(keyword_list) +
                        '" but should have been "' + repr(retval) +
                        '"')
