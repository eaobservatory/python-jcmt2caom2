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

import re

from tools4caom2.error import CAOMError

obsidss_irregular = {
    'scuba2_18_20120703T075007_850': 'scuba2_00018_20120703T075008',
}

obsidss_pattern = re.compile(
    '^(scuba2|acsis|DAS|AOSC|scuba)_(\d+)_(\d{8})[tT](\d{6})_\d+$')

def obsidss_to_obsid(obsidss):
    """
    Attempt to guess the OBSID given the OBSIDSS.

    In principal we shouldn't need to do this, but in practise it may
    be necessary.  This function encodes date-based rules for different
    instruments and can also handle special cases where an explicit
    OBSID mapping is given by the obsidss_irregular dictionary.
    """

    if obsidss in obsidss_irregular:
        return obsidss_irregular[obsidss]

    m = obsidss_pattern.match(obsidss)

    if not m:
        raise CAOMError('format of obsidss not recognised: {0}'.format(obsidss))

    (inst, obs, date, time) = m.groups()
    obs = int(obs)

    if inst == 'scuba2':
        if date < '20091004':
            return '{0}_{1:d}_{2}T{3}'.format(inst, obs, date, time)
        else:
            return '{0}_{1:05d}_{2}T{3}'.format(inst, obs, date, time)

    elif inst == 'DAS':
        return '{0}_{1:05d}_{2}T{3}'.format(inst, obs, date, time)

    elif inst == 'acsis':
        if date >= '20061001' and date <= '20070521':
            return '{0}_{1:d}_{2}T{3}'.format(inst, obs, date, time)
        else:
            return '{0}_{1:05d}_{2}T{3}'.format(inst, obs, date, time)

    raise CAOMError('do not know how to format obsid for: {0}'.format(obsidss))
