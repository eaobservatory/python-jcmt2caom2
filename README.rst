python-jcmt2caom2
=================

Python code to ingest JCMT data into the JCMT Science Archive at the CADC
This is a set of notes relevant to the development of the jcmt2caom2
package that will be used to ingest files into CAOM-2 for the JSA.

LOCAL INSTALLATION:
A normal distutils installation is possibel, provided the installed directory
in PYTHONPATH::

    setenv PYTHONPATH ~/lib/python2.7/site-packages
    python2.7 setup.py install --prefix=~

Beware that configuration files like ~/.pydistutils.cfg can change your 
installation configuration and may need to be renamed, deleted or stored in
another directory.
