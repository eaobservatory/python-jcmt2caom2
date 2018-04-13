# Copyright (C) 2018 East Asian Observatory.
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

from codecs import ascii_encode
import json
import subprocess

from tools4caom2.error import CAOMError

EXIFTOOL_COMMAND = 'exiftool'


def read_png_keywords(filename):
    """
    Read EXIF data from a PNG file.  The list of keywords is then returned as
    a dictionary by assuming each entry is a JSA-style "key=value" pair.
    """

    exif_json = subprocess.check_output(
        [EXIFTOOL_COMMAND, '-j', filename],
        shell=False)

    exif_data = json.loads(exif_json)

    if len(exif_data) != 1:
        raise CAOMError('Did not get expected single item from exiftool')

    keywords = exif_data[0]['Keywords']

    return dict(ascii_encode(x)[0].split('=', 1) for x in keywords)
