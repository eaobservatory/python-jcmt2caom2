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

from tools4caom2.error import CAOMError


_mime_types = {
    '.sdf': 'application/octet-stream',
    '.gsd': 'application/octet-stream',
    '.fits': 'application/fits',
    '.png': 'image/png',
    '.txt': 'text/plain',
}


def determine_mime_type(filename):
    """
    Determine MIME type for a file in the JCMT archive.
    """

    if filename.endswith('.gz'):
        filename = filename[:-3]

    for (extension, type_) in _mime_types.items():
        if filename.endswith(extension):
            return type_

    raise CAOMError('Unknown file extension: "{}"'.format(filename))
