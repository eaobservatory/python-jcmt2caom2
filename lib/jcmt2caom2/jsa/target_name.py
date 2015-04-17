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

import re


def target_name(object_name):
    """
    Standardizes the conventions for target names.  All names will be put
    into upper case, leading and trailing white space will be stripped, and
    multiple white space characters within the string will be converted to
    single spaces.

    Arguments:
    object_name: the JCMT object name

    Returns:
    string containing the standardized target name

    Usage:
    For a raw observation:
        observation.target_name = target_name(common['object'])
    For processed data:
        self.add_to_plane_dict('target.name',
                               target_name(header['OBJECT']))
    """
    return re.sub(r'\s+', r' ', object_name.strip().upper())
