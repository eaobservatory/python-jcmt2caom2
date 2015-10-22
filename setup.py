# Copyright (C) 2014-2015 Science and Technology Facilities Council.
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

from distutils import debug
from setuptools import setup, find_packages
import os
import os.path
import sys

if sys.version_info[0] > 2:
    print 'The jcmt2caom2 package is only compatible with Python version 2.n'
    sys.exit(1)

if 'CADC_ROOT' in os.environ:
    configdir = os.path.join(os.path.expandvars('$CADC_ROOT'), 'config')
else:
    configdir = os.path.join(sys.prefix, 'config')
configfiles = [os.path.join('config', f) for f in os.listdir('config')]

setup(
    name="jcmt2caom2",
    version='1.2.6',
    description='Ingest JCMT data into CAOM-2',
    author='Russell Redman',
    author_email='russell.o.redman@gmail.com',
    package_dir={'': 'lib'},
    packages=find_packages(where='lib'),
    package_data={'jcmt2caom2': ['data/ignoredobs/*.lis']},
    scripts=['scripts/jsaingest',
             'scripts/jsaraw',
             'scripts/jsasetfield',
             'scripts/caomcheck',
             'scripts/remove_products'],
    # config files are not package data and must be located
    # in ../config relative to the executables in scripts
    data_files=[(configdir, configfiles)],
    provides=['jcmt2caom2'],
    install_requires=['distribute'],
    zip_safe=False
)
