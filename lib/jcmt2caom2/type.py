# Copyright (C) 2016 East Asian Observatory.
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

from collections import MutableMapping, OrderedDict
import logging

from tools4caom2.error import CAOMError

logger = logging.getLogger(__name__)


class OrderedStrDict(MutableMapping):
    """
    Class which acts like an OrderedDict but requires its values to be
    of type `str`.

    This differs from `caom2.util.caom2_util.TypedOrderedDict` in that it
    doesn't require its values to have a `key` attribute.  (And a fixed
    type of `str` is used.)
    """

    def __init__(self, iterable=()):
        self._data = OrderedDict()

        for (key, value) in iterable:
            self[key] = value

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        if not isinstance(value, str):
            logger.error("in the (key, value) pair ('%s', '%s'),"
                         " the value should have type 'str' but is %s",
                         key, repr(value), type(value))
            raise CAOMError('Non-str value being added to OrderedStrDict')

        self._data[key] = value

    def __delitem__(self, key):
        del self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __contains__(self, key):
        return key in self._data

    def __repr__(self):
        return 'OrderedStrDict({0!r})'.format(list(self._data.items()))
