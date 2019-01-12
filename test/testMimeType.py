# Copyright (C) 2019 East Asian Observatory
# All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful,but WITHOUT
# ANY # WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc.,51 Franklin
# Street, Fifth Floor, Boston, MA  02110-1301, USA

from __future__ import absolute_import

import unittest

from tools4caom2.error import CAOMError
from jcmt2caom2.mime import determine_mime_type


class testMimeType(unittest.TestCase):
    def testMimeType(self):
        with self.assertRaisesRegexp(
                CAOMError, 'Unknown file extension: "filename.docx"'):
            determine_mime_type('filename.docx')

        with self.assertRaisesRegexp(
                CAOMError, 'Unknown file extension: "filename.docx"'):
            determine_mime_type('filename.docx.gz')

        self.assertEquals(
            determine_mime_type('filename.sdf'),
            'application/octet-stream')
        self.assertEquals(
            determine_mime_type('filename.sdf.gz'),
            'application/octet-stream')
        self.assertEquals(
            determine_mime_type('filename.gsd'),
            'application/octet-stream')

        self.assertEquals(
            determine_mime_type('preview.png'),
            'image/png')

        self.assertEquals(
            determine_mime_type('cat.fits.gz'),
            'application/fits')
