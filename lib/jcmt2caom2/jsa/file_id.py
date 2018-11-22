# Copyright (C) 2018 East Asian Observatory
# All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc.,51 Franklin
# Street, Fifth Floor, Boston, MA  02110-1301, USA

import re

from tools4caom2.util import make_file_id

_pattern_acsis = re.compile(r'^a(\d{8})_(\d{5})_\d{2}_\d{4}\.sdf$')
_pattern_scuba2 = re.compile(r'^s[48][abcd](\d{8})_(\d{5})_\d{4}\.sdf$')

_non_gz = [
    'a20061112_00010_00_0001.sdf',
    'a20061112_00013_00_0001.sdf',
    's4a20140415_00082_0001.sdf',
    's4a20140729_00001_0001.sdf',
    's4b20140415_00082_0001.sdf',
    's4c20140415_00082_0001.sdf',
    's4d20140415_00082_0001.sdf',
]


def make_file_id_jcmt(filepath):
    """
    Determine file ID for use in the JCMT archive.

    This applies the `make_file_id` function from `tools4caom2.util` and
    then adds ".gz" if the file is a raw data file which we expect to be
    stored in the archive in gzipped form.
    """

    file_id = make_file_id(filepath)

    if _file_id_is_gz(file_id):
        file_id = '{}.gz'.format(file_id)

    return file_id


def _file_id_is_gz(file_id):
    """
    Determine whether we expect the given file ID to be gzipped,
    based on some date/observation ranges and a list of
    unexpectedly non-gzipped files.
    """

    (inst, date, obs) = _parse_raw_file_id(file_id)

    if inst is not None:
        if (
                (20060701 <= date <= 20150123)
                and not
                (20140116 <= date <= 20140122)
                and not
                (date == 20140115 and inst == 'SCUBA-2' and (38 <= obs <= 53))
                and
                (file_id not in _non_gz)):
            return True

    return False


def _parse_raw_file_id(file_id):
    """
    Attempt to parse a file ID as a raw data file for one of the
    current instruments.  If successful, return a (date, observation)
    tuple.
    """

    m = _pattern_acsis.search(file_id)
    if m:
        return ('ACSIS', int(m.group(1)), int(m.group(2)))

    m = _pattern_scuba2.search(file_id)
    if m:
        return ('SCUBA-2', int(m.group(1)), int(m.group(2)))

    return (None, None, None)
