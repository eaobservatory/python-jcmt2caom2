# Use "distribute"
from distutils import debug
from setuptools import setup, find_packages
import sys

if sys.version_info[0] > 2:
    print 'The jcmt2caom2 package is only compatible with Python version 2.n'
    sys.exit(-1)

setup(name="jcmt2caom2",
      version='1.1.0',
      description='Ingest JCMT data into CAOM-2',
      author='Russell Redman',
      author_email='russell.o.redman@gmail.com',
      license='This is for license.',
      data_files=[('scripts', ['scripts/jcmt2caom2raw', 
                               'scripts/jcmt2caom2proc'])],
      long_description='This is for description.',
      packages=find_packages(exclude='*.test'),
      provides=['jcmt2caom2'],
      install_requires=['distribute',
                        'tools4caom2 >= 1.1.0'],
      dependency_links = ['https://github.com/jac-h/python-tools4caom2.git'],
      zip_safe=False
)

