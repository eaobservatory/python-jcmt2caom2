# Use "distribute"
from distutils import debug
from setuptools import setup, find_packages
import os
import os.path
import sys

if sys.version_info[0] > 2:
    print 'The jcmt2caom2 package is only compatible with Python version 2.n'
    sys.exit(-1)

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
    scripts=['scripts/jsaingest',
             'scripts/jsaraw',
             'scripts/jsarawlist',
             'scripts/jsasetfield'],
    # config files are not package data and must be located
    # in ../config relative to the executables in scripts
    data_files=[(configdir, configfiles)],
    provides=['jcmt2caom2'],
    install_requires=['distribute'],
    zip_safe=False
)
