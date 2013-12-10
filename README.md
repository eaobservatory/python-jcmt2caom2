python-jcmt2caom2
=================

Python code to ingest JCMT data into the JCMT Science Archive at the CADC
This is a set of notes relevant to the development of the jcmt2caom2
package that will be used to ingest files into CAOM-2 for the JSA.

GIT:

To push modified code to the Linux-based git repository:
  cd <directory containing working copy>
  git add <list of new files>
  git commit -a -m 'message'
  git push origin master
 
To fetch the changes into the working copy on Linux
  cd <<directory containing working copy>
  git pull origin master

ANT:
Ant build actually does an install into the build subdirectory, so that the
CADC install/release scripts can copy the resulting files into their normal 
places by setting RPS correctly.  This process is very specific to the CADC;
Ant should NOT be used if it is intended that the software will be installed 
into the normal Python directories.  For the ant process to work properly, 
it is necessary to set the environment variable A (set by default on the CADC 
Linux systems, and usually to ~/ elsewhere)) and to have copies of the files
   ${A}/compilers/setup.ant.python.properties
   ${A}/compilers/setup.ant.python.targets.xml

  setenv A ~/
  cd <directory containing working copy>
  ant build

LOCAL INSTALLATION:
A normal distutils installation is possibel, provided the installed directory
in PYTHONPATH:
   setenv PYTHONPATH ~/lib/python2.7/site-packages
   python2.7 setup.py install --prefix=~
Beware that configuration files like ~/.pydistutils.cfg can change your 
installation configuration and may need to be renamed, deleted or stored in
another directory.

LOCAL OPERATION:
  setenv PYTHONPATH /home/cadc/redman/cadcgit/jcmt2caom2/build/lib/python2.7/site-packages:/home/cadc/redman/lib/python2.7/site-packages/
  setenv CADC_ROOT $RPS
