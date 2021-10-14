# Copyright (C) 2014-2015 Science and Technology Facilities Council.
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

from distutils.core import setup
import os
import sys

sys.path.insert(0, 'lib')
from jcmt2caom2.__version__ import version

if 'CADC_ROOT' in os.environ:
    configdir = os.path.join(os.path.expandvars('$CADC_ROOT'), 'config')
else:
    configdir = os.path.join(sys.prefix, 'config')
configfiles = [os.path.join('config', f) for f in os.listdir('config')]

setup(
    name='jcmt2caom2',
    version=version,
    description='Ingest JCMT data into CAOM-2',
    author='Russell Redman',
    author_email='russell.o.redman@gmail.com',
    url='https://github.com/eaobservatory/python-jcmt2caom2',
    package_dir={'': 'lib'},
    packages=[
        'jcmt2caom2',
        'jcmt2caom2.instrument',
        'jcmt2caom2.jsa',
    ],
    package_data={'jcmt2caom2': [
        'data/ignoredobs/*.lis',
    ]},
    scripts=[
        'scripts/jsaingest',
        'scripts/jsaraw',
        'scripts/jsasetfield',
        'scripts/caomcheck',
        'scripts/remove_products',
    ],
    # config files are not package data and must be located
    # in ../config relative to the executables in scripts
    data_files=[(configdir, configfiles)],
    provides=['jcmt2caom2'],
    requires=[
        'astropy',
        'caom2',
        'docopt',
        'healpy',
        'tools4caom2',
    ]
)
