__author__ = "Russell O. Redman"

import argparse
from ConfigParser import SafeConfigParser
from contextlib import contextmanager, closing
from collections import OrderedDict
import datetime
import logging
import os
import os.path
try:
    from astropy.io import fits as pyfits
except:
    import pyfits
import re
import subprocess
import sys

from vos.vos import Client

from caom2.caom2_composite_observation import CompositeObservation
from caom2.caom2_observation_uri import ObservationURI
from caom2.caom2_plane_uri import PlaneURI

from tools4caom2.caom2repo_wrapper import Repository
from tools4caom2.data_web_client import data_web_client
from tools4caom2.delayed_error_warning import delayed_error_warning
from tools4caom2.error import CAOMError
from tools4caom2.fits2caom2 import run_fits2caom2
from tools4caom2.container.adfile import adfile_container
from tools4caom2.container.filelist import filelist_container
from tools4caom2.tapclient import tapclient
from tools4caom2.utdate_string import utdate_string
from tools4caom2.util import make_file_id_no_ext
from tools4caom2.container.vos import vos_container

from tools4caom2.__version__ import version as tools4caom2version

__doc__ = """
Ingest processed data files into CAOM-2.  The caom2ingest module has been
cloned from ingest2caom2 and customized to find its lists of input files
in a directory that is either on disk or in a VOspace, or in an adfile.
This is intended to be a base class for archive-specific ingestion routines
with names like archive2caom2ingest.

By default, it runs a set of file verification tests, creates a report of
errors and warnings and exits.  This is referred to as "check mode" and anyone
can run caom2ingest in check mode.

Check mode implements several of the checks that would be done during a CADC
e-transfer operation, such as rejecting zero-length files, running fitsverify
on FITS files to verify that they do not generate error messages,
and verifying that names match the regex required for the archive.  Other
checks include metadata checks for mandatory keywords or keywords that must
have one of a restricted set of values, and verifying whether the file is
already present in the archive (sometimes forbidden, sometimes mandatory).

With the --store switch, caom2ingest will copy files from VOspace into archive
storage at the CADC.  This is a privileged operation; the account making the
request must have appropriate write permissions.

The --storemethod switch has one of two values, "pull" or "push" where "pull"
is the default.  The "pull" method uses CADC e-transfer to move the files into
ADS.  The "push" method uses the data web client to push the files into AD.

Either store method can be used for files in VOspace, provided the VOspace
etransfer directory has been configured in the [transfer] section of the
userconfig file.

Files on disk at the JAC can be transferred using the "push" method, but it
is likely that some other transfer mechanism will already be built into the
data processing system, rendering it unnecessary.

With the --ingest switch, caom2ingest will ingest the files into CAOM-2.
However it is managed, the transfer of files into AD must already have occurred
before --ingest is invoked.  In addition, all raw observations in the
membership must already have been successfully ingested.

The ingestion logic is mostly missing from this module, since it is extremely
archive-specific and must be implemented in a subclass derived from
caom2ingest.  This is a privileged operation; the CADC must have granted read
and write access for the CAOM-2 repository to the account requesting the
ingestion.

The caom2ingest module is intended to be used at sites remote from the CADC.
If metadata is available from a database, caom2ingest can be configured to
use it to improve performance and gather more authoritative metadata, but
otherwise it uses only generic methods that should work over the internet to
access and store files. Access to the VOspace uses the CADC-supplied vos
module.  Access to existing observations, planes and artifacts in CAOM-2 uses
the CADC TAP service, and the caom2repo.py comand line script supplied with the
CADC caom2repoClient package.

Original documentation from ingest2caom2:

The ingest2caom2 base class supplies a generic wrapper for fits2caom2.
For each new archive, derive a new class that customizes the methods:
 class archive2caom2(ingest2caom2):
 - __init__         : supply archive-specific default values
 - build_dict       : given the headers from a FITS file, define plane and
                      uri dependent data structures
Optionally, it may be useful to customize the methods:
 - build_observation_custom : modify the xml file after all fits2caom2
                              operations on an observation are complete
 - build_plane_custom : modify the xml file after each fits2caom2
                        operations is complete
The latter two calls allow, for example, the time bounds derived from raw
data to be added to the science chunks within a composite observation.

It might also be useful to define filter and comparison functions (outside
the class):
 - archivefilter(f)                : return True if f is a file to ingest,
                                            False otherwise

This can be used to initialize the field filterfunc in the __init__ method of
the derived class.  The tools4caom.container.util module supplies examples of
these functions that are adequate for mamny purposes:
 - fitsfilter(f)                   : return True if f is a FITS file,
                                            False otherwise
 - nofilter(f)                     : return True always, i.e. no filtering

It is sometimes also useful to supply a custom function
 - make_file_id(f)                 : given a file name, return an AD file_id

The commandLineSwitches method inherited from ingest2caom2 defines a common
command line interface that should be adequate for all but the most complex
archives.  By overriding this method, it is possible to add more switches
that can be queried in the build routines.
"""

logger = logging.getLogger(__name__)


class caom2ingest(object):
    """
    Base class to copy and ingest files from a VOspace into a CADC archive
    """

    def __init__(self):
        """
        Initialize the caom2ingest structure, especially the attributes
        storing default values for command line arguments.

        Arguments:
        <none>

        It is normally necessary to override __init__ in a derived class,
        supplying archive-specific values for some of the fields, e.g.
            def __init__(self):
                caom2ingest.__init__(self)
                self.archive  = <myarchive>
        """
        # config object optionally contains a user configuration object
        # this can be left undefined at the CADC, but is needed at other sites
        self.userconfig = SafeConfigParser()
        # userconfigpath can be overridden on the command line or in a
        # derived class
        self.userconfigpath = '~/.tools4caom2/tools4caom2.config'

        # -------------------------------------------
        # placeholders for command line switch values
        # -------------------------------------------
        # Command line interface for the ArgumentParser and arguments
        # Command line options
        self.progname = os.path.basename(os.path.splitext(sys.argv[0])[0])
        self.exedir = os.path.abspath(os.path.dirname(sys.path[0]))
        # Derive the config path from the script or bin directory path
        if 'CADC_ROOT' in os.environ:
            self.configpath = os.path.abspath(
                os.path.join(os.path.expandvars('$CADC_ROOT'), 'config'))
        else:
            self.configpath = os.path.join(self.exedir, 'config')

        # Argument parser
        self.ap = None
        self.args = None

        # Database defaults, normally filled from userconfig
        self.archive = None
        self.stream = None
        self.collection_choices = ['SANDBOX']
        self.external_collections = ['SANDBOX']

        # routine to convert filepaths into file_ids
        # The default routine supplied here should work for most archives.
        self.make_file_id = make_file_id_no_ext

        # temporary disk space for working files
        self.workdir = None

        # Ingestion parameters and structures
        self.debug = False
        self.prefix = ''         # ingestible files must start with this prefix
        self.indir = ''          # path to indir
        self.replace = False     # True if observations in JCMTLS or JCMTUSER
                                 # can replace each other
        self.big = False         # True to use larger memory for fits2caom2
        self.store = False       # True to store files from indir
        self.storemethod = None  # e-transfer or data web service
        self.ingest = False      # True to ingest files from indir into CAOM-2
        self.local = False       # True if files are on a local disk

        # Archive-specific fits2caom2 config and default file paths
        self.config = None
        self.default = None

        # Current vos container
        self.vosclient = Client()
        self.vos = None
        # dictionary of lists of compiles regex expressions, keyed by extension
        self.fileid_regex_dict = None

        # Working structures thatcollect metadata from each file to be saved
        # in self.metadict
        self.collection = None
        self.observationID = None
        self.productID = None
        self.plane_dict = OrderedDict()
        self.fitsuri_dict = OrderedDict()
        # The memberset contains member time intervals for this plane.
        # The member_cache is a dict keyed by the membership headers
        # MBR<n> or OBS<n> that contains the observationURI, date_obs, date_end
        # and release_date for each member.  This is preserved for the whole
        # container on the expectation that the same members will be used by
        # multiple files.
        self.memberset = set()
        self.member_cache = dict()
        # The inputset is the set of planeURIs that are inputs for a plane
        # The fileset is a set of input files that have not yet been confirmed
        # as belonging to any particular input plane.
        # The input cache is a dictionary giving the planeURI for each file_id
        # found in a member observation.
        self.inputset = set()
        self.fileset = set()
        self.input_cache = dict()

        # The metadata dictionary - fundamental structure for the entire class
        # For the detailed structure of metadict, see the help text for
        # fillMetadictFromFile()
        self.metadict = OrderedDict()

        # Lists of files to be stored, or to check that they are in storage
        # Data files are added to data_storage iff they report no errors
        self.data_storage = []
        # Preview candidates are added as they are encountered and removed
        # if they do not match any planes.
        self.preview_storage = []

        # list of containers for input files
        self.containerlist = []

        # Delayed reporting of errors and warnings discovered in files
        self.dew = None

        # TAP client
        self.tap = None

        # Prepare CAOM-2 repository client.
        self.repository = Repository()

        # A dictionary giving the number of parts which should be in each
        # artifact.  When we read a FITS file, the part count will be written
        # into this hash to allow us to identify and remove left-over
        # spurious parts from the CAOM-2 records.
        self.artifact_part_count = {}

    # ***********************************************************************
    # Define the standard command line interface.
    # Be sure to maintain consistency amongst defineCommandLineSwitches,
    # processCommandLineSwitches, and logCommandLineSwitches.
    # ***********************************************************************
    def defineCommandLineSwitches(self):
        """
        Generic routine to build the standard list of command line arguments.
        This routine has been split off from processing and logging to allow
        additional arguments to be defined for derived classes.

        Subclasses for specific archive can override this method to add new
        arguments, but should first call
           self.caom2ingest.defineCommandLineSwitches()
        to ensure that the standard arguments are always defined.

        Arguments:
        <none>

        # user config arguments
        --userconfig : path to user configuration file
        --proxy      : path to CADC proxy certificate

        # ingestion arguments
        --prefix     : (required) prefix for files to be stored/ingested
        --indir      : (required) directory or ad file containing the release
        --replace    : (optional) observations in JCMTLS or JCMTUSER can
                       replace existing observations
        --store      : (optional) store files in AD (requires CADC
                       authorization)
        --ingest     : (optional) ingest new files (requires CADC
                       authorization)

        # fits2caom2 arguments
        --collection : (required) collection to use for ingestion
        --config     : (optional) path to fits2caom2 config file
        --default    : (optional) path to fits2caom2 default file

        # File and directory options
        --workdir    : (optional) working directory (default = cwd)

        # debugging options
        --debug      : (optional) log all messages and retain temporary files
                       on error
        --test       : (optional) simulate operation of fits2caom2
        """

        # Optional user configuration
        if self.userconfigpath:
            self.ap.add_argument(
                '--userconfig',
                default=self.userconfigpath,
                help='Optional user configuration file '
                     '(default=' + self.userconfigpath + ')')

        self.ap.add_argument(
            '--proxy',
            default='~/.ssl/cadcproxy.pem',
            help='path to CADC proxy')

        # Ingestion modes
        self.ap.add_argument('--prefix',
                             help='file name prefix that identifies files '
                                  'to be ingested')
        self.ap.add_argument('--indir',
                             required=True,
                             help='path to release data (on disk, in vos, or '
                                  'an ad file')
        self.ap.add_argument('--replace',
                             action='store_true',
                             help='observations in JCMTLS and JCMTUSER can '
                                  'replace existing observations')
        self.ap.add_argument('--store',
                             action='store_true',
                             help='store in AD files that are ready for '
                                  'ingestion if there are no errors')
        self.ap.add_argument('--storemethod',
                             choices=['push', 'pull'],
                             default='pull',
                             help='use e-transfer (pull) or data web service '
                                  '(push) to store files in AD')
        self.ap.add_argument('--ingest',
                             action='store_true',
                             help='ingest from AD files that are ready for '
                                  'ingestion if there are no errors')

        # Basic fits2caom2 options
        # Optionally, specify explicit paths to the config and default files
        self.ap.add_argument(
            '--collection',
            required=True,
            choices=self.collection_choices,
            help='collection to use for ingestion')
        self.ap.add_argument(
            '--config',
            help='(optional) path to fits2caom2 config file')
        self.ap.add_argument(
            '--default',
            help='(optional) path to fits2caom2 default file')

        # Big jobs require extra memory
        self.ap.add_argument(
            '--big',
            action='store_true',
            help='(optional) request extra heap space and RAM')

        # output directory
        self.ap.add_argument(
            '--workdir',
            help='output directory, (default = current directory')

        # debugging options
        self.ap.add_argument(
            '--test',
            action='store_true',
            help='(optional) simulate operation of fits2caom2')
        self.ap.add_argument(
            '--debug',
            action='store_true',
            help='(optional) show all messages, pass --debug to fits2caom2,'
            ' and retain all xml and override files')

    def processCommandLineSwitches(self):
        """
        Generic routine to process the command line arguments
        and create workdir if necessary.  This will check the values of the
        standard arguments defined in defineCommandLineSwitches and will
        leave the additional arguments in self.args.

        Arguments:
        <None>

        Returns:
        The set of command line arguments is stored in self.args and the
        default arguments are interpreted and stored into individual
        attributes.
        """
        # If the user configuration file exists, read it.
        if 'userconfig' in self.args:
            self.userconfigpath = os.path.abspath(
                os.path.expanduser(
                    os.path.expandvars(
                        self.args.userconfig)))
        if self.userconfigpath and os.path.isfile(self.userconfigpath):
            with open(self.userconfigpath) as UC:
                self.userconfig.readfp(UC)

        self.proxy = os.path.abspath(
            os.path.expandvars(
                os.path.expanduser(self.args.proxy)))

        self.collection = self.args.collection

        if self.args.prefix:
            self.prefix = self.args.prefix
            file_id_regex = re.compile(self.prefix + r'.*')
            self.fileid_regex_dict = {'.fits': [file_id_regex],
                                      '.fit': [file_id_regex],
                                      '.log': [file_id_regex],
                                      '.txt': [file_id_regex]}
        else:
            self.fileid_regex_dict = {'.fits': [re.compile(r'.*')],
                                      '.fit': [re.compile(r'.*')]}

        # Save the values in self
        # A value on the command line overrides a default set in code.
        # Options with defaults are always defined by the command line.
        # It is not necessary to check for their existance.
        if self.args.big:
            self.big = self.args.big

        if self.args.config:
            self.config = os.path.abspath(
                os.path.expandvars(
                    os.path.expanduser(self.args.config)))
        if self.args.default:
            self.default = os.path.abspath(
                os.path.expandvars(
                    os.path.expanduser(self.args.default)))

        if self.args.workdir:
            self.workdir = os.path.abspath(
                os.path.expandvars(
                    os.path.expanduser(self.args.workdir)))
        else:
            self.workdir = os.getcwd()

        # Parse ingestion options
        if (re.match(r'vos:.*', self.args.indir)
                and self.vosclient.access(self.args.indir)
                and self.vosclient.isdir(self.args.indir)):

            self.indir = self.args.indir
            self.local = False
        else:
            indirpath = os.path.abspath(
                os.path.expandvars(
                    os.path.expanduser(self.args.indir)))
            # is this a local directorory on the disk?
            if os.path.isdir(indirpath):
                self.indir = indirpath
                self.local = True

            # is this an adfile?
            elif (os.path.isfile(indirpath) and
                  os.path.splitext(indirpath)[1] == '.ad'):
                self.indir = indirpath
                self.local = False

        if self.args.replace:
            self.replace = self.args.replace

        self.test = self.args.test

        if self.args.debug:
            logging.getLogger().setLevel(logging.DEBUG)
            self.debug = True

        # create workdir if it does not already exist
        if not os.path.exists(self.workdir):
            os.makedirs(self.workdir)

        if self.args.store:
            self.store = self.args.store
        self.storemethod = self.args.storemethod

        if self.args.ingest:
            self.ingest = self.args.ingest

    def logCommandLineSwitches(self):
        """
        Generic method to log the command line switch values

        Arguments:
        <none>
        """
        # Report switch values
        logger.info(self.progname)
        logger.info('*** Arguments for caom2ingest base class ***')
        logger.info('tools4caom2version = %s', tools4caom2version)
        logger.info('configpath = ' + self.configpath)
        for attr in dir(self.args):
            if attr != 'id' and attr[0] != '_':
                logger.info('%-15s= %s', attr, str(getattr(self.args, attr)))
        logger.info('workdir = %s', self.workdir)
        logger.info('local = %s', self.local)

        if self.collection in self.external_collections:
            if not self.prefix:
                errors = True
                logger.error('--prefix is mandatory if --collection '
                             'is in ' + repr(self.external_collections))
                raise CAOMError('error in command line options')

        if not self.indir:
            raise CAOMError('--indir = ' + self.args.indir + ' does not exist')

        self.tap = tapclient(self.proxy)
        if not os.path.exists(self.proxy):
            raise CAOMError('proxy does not exist: ' + self.proxy)

        if not os.path.isdir(self.workdir):
            raise CAOMError('workdir is not a directory: ' + self.workdir)

        if self.config and not os.path.isfile(self.config):
            raise CAOMError('config file does not exist: ' + str(self.config))

        if self.default and not os.path.isfile(self.default):
            raise CAOMError('default file does not exist: ' +
                            str(self.default))

    def getfilelist(self, rootdir, check):
        """
        Return a list of files in the directory tree rooted at dirpath
        for which check(f) is True.

        Arguments:
        rootdir: absolute path to the root of the directory tree
        check: function that checks whether to include the file in the list
        """
        mylist = []
        for dirpath, dirlist, filelist in os.walk(rootdir):
            for f in filelist:
                filepath = os.path.join(dirpath, f)
                if check(filepath):
                    mylist.append(filepath)
            for d in dirlist:
                mylist.extend(self.getfilelist(os.path.join(dirpath, d),
                                               check))
        return mylist

    def commandLineContainers(self):
        """
        Process the input directory.  Unlike previous versions of this code,
        caom2ingest handles only one container at a time.  This might revert
        to processing multiple containers again in the future, so the
        container list is retained.

        Arguments:
        <None>
        """
        # Find the list of containers to ingest.
        self.containerlist = []
        try:
            if os.path.isdir(self.indir):
                check = lambda f: (self.dew.namecheck(f, report=False)
                                   and self.dew.sizecheck(f))
                filelist = self.getfilelist(self.indir, check)
                self.containerlist.append(
                    filelist_container(
                        self.indir,
                        filelist,
                        lambda f: True,
                        self.make_file_id))

            elif os.path.isfile(self.indir):
                basename, ext = os.path.splitext(self.indir)
                if ext == '.ad':
                    # self.indir points to an ad file
                    self.containerlist.append(
                        adfile_container(
                            self.data_web,
                            self.indir,
                            self.workdir,
                            self.make_file_id))

                else:
                    raise CAOMError('indir is not a directory and: '
                                    'is not an ad file: ' +
                                    self.indir)

            else:
                # handle VOspace directories
                if (self.vosclient.access(self.indir)
                        and self.vosclient.isdir(self.indir)):

                    self.containerlist.append(
                        vos_container(self.indir,
                                      self.archive,
                                      self.ingest,
                                      self.workdir,
                                      self.dew,
                                      self.vosclient,
                                      self.data_web,
                                      self.make_file_id))
                else:
                    raise CAOMError('indir is not local and is not '
                                    'a VOspace directory: ' +
                                    self.indir)

        except Exception as e:
            logger.exception('Error configuring containers')
            raise CAOMError(str(e))

    # ***********************************************************************
    # Clear the local plane and artifact dictionaries
    # ***********************************************************************
    def clear(self):
        """
        Clear the local plane and artifact dictionaries before each file is
        read.

        Arguments:
        <none>
        """
        self.file_id = ''
        self.uri = ''
        self.observationID = None
        self.productID = None
        self.plane_dict.clear()
        self.fitsuri_dict.clear()
        self.memberset.clear()
        self.inputset.clear()
        self.override_items = 0

    # ***********************************************************************
    # Fill metadict using metadata from each file in the specified container
    # ***********************************************************************
    def fillMetadict(self, container):
        """
        Generic routine to fill the metadict structure by iterating over
        all containers, extracting the required metadata from each file
        in turn using fillMetadictFromFile().

        Arguments:
        container: a container of files to read
        """
        self.metadict.clear()
        self.data_storage = []
        self.preview_storage = []

        try:
            # sort the file_id_list
            file_id_list = sorted(container.file_id_list())
            logger.debug('in fillMetadict, file_id_list = %s',
                         repr(file_id_list))

            # Gather metadata from each file in the container
            for file_id in file_id_list:
                logger.debug('In fillMetadict, use %s', file_id)

                with container.use(file_id) as f:
                    self.fillMetadictFromFile(file_id, f, container)
        finally:
            container.close()

    def fillMetadictFromFile(self, file_id, filepath, container):
        """
        Generic routine to read metadata and fill the internal structure
        metadict (a nested set of dictionaries) that will be used to control
        sort and fill the override file templates.

        Arguments:
        file_id : must be added to the header
        filepath : absolute path to the file, must be added to the header
        """
        logger.info('fillMetadictFromFile: %s %s', file_id, filepath)

        # ****************************************************************
        # Call build_dict to fill plane_dict and fitsuri_dict
        # ****************************************************************
        self.clear()
        # If the file is not a FITS file or is in serious violation of the FITS
        # standard, substitute an empty dictionary for the headers.  This is
        # a silent replacement, not an error, to allow non-FITS files to be
        # ingested along with regular FITS files.
        if self.dew.namecheck(filepath, report=False):
            try:
                with closing(pyfits.open(filepath, mode='readonly')) as f:
                    head = f[0].header
                    self.artifact_part_count[self.fitsfileURI(
                        self.archive, file_id)] = len(f)

                head.update('file_id', file_id)
                head.update('filepath', filepath)
                if isinstance(container, vos_container):
                    head.update('VOSPATH', container.vosroot)
                    head.update('SRCPATH', container.uri(file_id))
                else:
                    head.update('SRCPATH', filepath)

                logger.debug('...got primary header from %s', filepath)

            except:
                head = {}
                head['file_id'] = file_id
                head['filepath'] = filepath
                logger.debug('...could not read primary header from ',
                             filepath)

            self.file_id = file_id
            if self.ingest:
                self.verifyFileInAD(filepath, file_id)

            self.build_dict(head)
            self.build_metadict(filepath)
            if (filepath not in self.dew.errors
                    or len(self.dew.errors[filepath]) == 0):

                self.data_storage.append(head['SRCPATH'])

#        else:
#            self.preview_storage.append(container.uri(file_id))

    # ***********************************************************************
    # Format an observation URI for composite members
    # ***********************************************************************
    def observationURI(self, collection, observationID):
        """
        Generic method to format an observation URI, i.e. the URI used to
        specify members in a composite observation.

        Arguments:
        collection : the collection containing observationID
        observationID : the observationID of the URI

        Returns:
        the value of the observationURI
        """
        mycollection = collection
        if collection is None:
            mycollection = ''
        myobservationID = observationID
        if observationID is None:
            myobservationID = ''

        uri = ObservationURI('caom:' +
                             mycollection + '/' +
                             myobservationID)
        return uri

    # ***********************************************************************
    # Format a plane URI for provenance inputs
    # ***********************************************************************
    def planeURI(self, collection, observationID, productID):
        """
        Generic method to format a plane URI, i.e. the URI used to access
        a plane in the data repository.

        Arguments:
        collection : the collection containing observationID
        observationID : the observationID containing productID
        productID : the productID of the URI

        Returns:
        the value of the planeURI
        """
        mycollection = collection
        if collection is None:
            mycollection = ''
        myobservationID = observationID
        if observationID is None:
            myobservationID = ''
        myproductID = productID
        if productID is None:
            myproductID = ''

        uri = PlaneURI('caom:' +
                       mycollection + '/' +
                       myobservationID + '/' +
                       myproductID)
        return uri

    # ***********************************************************************
    # Add this fitsuri (file or extewnsion) to the local fitsuri dictionary
    # ***********************************************************************
    def add_fitsuri_dict(self, uri):
        """
        Add a key, value pair to the local fitsuri dictionary.  The method
        will throw an exception if the value does not have a string type.

        Arguments:
        uri : the uri of this fits file or extension
        key : a key in a string.Template
        value : a string value to be substituted in a string.Template
        """
        if uri not in self.fitsuri_dict:
            self.fitsuri_dict[uri] = OrderedDict()
            self.fitsuri_dict[uri]['custom'] = OrderedDict()

    # ***********************************************************************
    # Format a URI for data access
    # ***********************************************************************
    def fitsfileURI(self,
                    archive,
                    file_id):
        """
        Generic method to format an artifact URI, i.e. the URI used to access
        a file in AD.

        Either fitsfileURI or fitsextensionURI must be called with
        fits2caom2=True for every file to be ingested.

        Arguments:
        archive : the archive within ad that holds the file
        file_id : file_id of the file in ad
        fits2caom2 : True => store uri for use with fits2caom2

        Returns:
        the value of the fitsfileURI
        """
        return ('ad:' + archive + '/' + file_id)

    # ***********************************************************************
    # Format a URI for data access
    # ***********************************************************************
    def fitsextensionURI(self,
                         archive,
                         file_id,
                         extension_list):
        """
        Generic method to format a part URI, i.e. the URI used to access
        one or more extensions from a FITS file in AD.

        Generating a fitsextensionURI calls fitsfileURI so it is not necessary
        to call both explicitly, but one or the other must be called with
        fits2caom2=True for every file that is ingested.

        Arguments:
        archive : the archive within ad that holds the file
        file_id : file_id of the file in ad
        extension_list : list (or tuple) of integers or tuples containing
                        integer pairs for the extensions to be ingested;
                        if omitted ingest all extensions
        fits2caom2 : True => store uri for use with fits2caom2

        Returns:
        the value of the fitsextensionURI
        """
        fileuri = self.fitsfileURI(archive, file_id)
        elist = []
        for e in extension_list:
            if isinstance(e, int):
                elist.append(str(e))
            elif (isinstance(e, tuple) and
                  len(e) == 2 and
                  isinstance(e[0], int) and
                  isinstance(e[1], int)):
                elist.append(str(e[0]) + '-' + str(e[1]))
            else:
                logger.error('extension_list must contain only integers '
                             'or tuples cntaining pairs of integers: %s',
                             repr(extension_list))
                raise CAOMError('invalid extension_list')

        if elist:
            fexturi = fileuri + '#[' + ','.join(elist) + ']'

        return fexturi

    # ***********************************************************************
    # Add a key-value pair to the local plane dictionary
    # ***********************************************************************
    def add_to_plane_dict(self, key, value):
        """
        Add a key, value pair to the local plane dictionary.  The method will
        throw an exception and exit if the value does not have a string type.

        Arguments:
        key : a key in a string.Template
        value : a string value to be substituted in a string.Template
        """
        if not isinstance(value, str):
            logger.error("in the (key, value) pair ('%s', '%s'),"
                         " the value should have type 'str' but is %s",
                         key, repr(value), type(value))
            raise CAOMError('non-str value being added to plane dict')

        self.plane_dict[key] = value
        self.override_items += 1

    # ***********************************************************************
    # Add a key-value pair to the local fitsuri dictionary
    # ***********************************************************************
    def add_to_fitsuri_dict(self, uri, key, value):
        """
        Add a key, value pair to the local fitsuri dictionary.  The method
        will throw an exception if the value does not have a string type.

        Arguments:
        uri : the uri of this fits file or extension
        key : a key in a string.Template
        value : a string value to be substituted in a string.Template
        """
        if not isinstance(value, str):
            logger.error("in the (key, value) pair ('%s', '%s'),"
                         " the value should have type 'str' but is %s",
                         key, repr(value), type(value))
            raise CAOMError('non-str value being added to fitsuri dict')

        if uri not in self.fitsuri_dict:
            logger.error('Create the fitsuri before adding '
                         'key,value pairs to the fitsuri_dict: '
                         '["%s"]["%s"] = "%s")', uri, key, value)
            raise CAOMError('trying to add pair for non-existent fitsuri')

        self.fitsuri_dict[uri][key] = value
        self.override_items += 1

    # ***********************************************************************
    # Add a key-value pair to the local fitsuri custom dictionary
    # ***********************************************************************
    def add_to_fitsuri_custom_dict(self, uri, key, value):
        """
        Add a key, value pair to the local fitsuri dictionary.  Unlike the
        other dictionaries, the fitsuri custom dictionary can hold arbitray
        dictionary values, since the values will be processed using custom
        code and do not necessary get written into the override file.

        Arguments:
        uri : the uri of this fits file or extension
        key : a key
        value : an arbitrary data type
        """
        if uri not in self.fitsuri_dict:
            logger.error('call fitfileURI before adding '
                         'key,value pairs to the fitsuri_dict: '
                         '["%s"]["%s"] = "%s")',
                         uri, key, repr(value))
            raise CAOMError('trying to add pair for non-existent fitsuri')

        self.fitsuri_dict[uri]['custom'][key] = value
        self.override_items += 1

    # ***********************************************************************
    # Fetch a previously entered value from a specified plane dictionary
    # ***********************************************************************
    def findURI(self, uri):
        """
        Generic routine to find which collection, observationID and productID
        contains a particular uri that has been previously ingested.

        Arguments:
        uri : an artifact URI to locate

        Returns:
        the tuple (collection, observationID, productID) describing the uri
        or (None, None, None)
        """
        for c in self.metadict:
            for o in self.metadict[c]:
                for p in self.metadict[c][o]:
                    if p not in ['memberset']:
                        if uri in self.metadict[c][o][p]['uri_dict']:
                            return (c, o, p)
        return (None, None, None)

    # ***********************************************************************
    # Fetch a previously entered value from a specified plane dictionary
    # ***********************************************************************
    def get_plane_value(self, collection, observationID, productID, key):
        """
        Return the value stored in the plane dictionary for a previously
        entered collection, productID, and key.

        Arguements:
        collection : the collection containing the productID
        productID : the productID containing the key in its plane_dict
        key : the key whose value is needed

        If any of collection, productID or key are not present, a
        KeyError exception will be raised.
        """
        return self.metadict[collection][observationID][productID
                                                        ]['plane_dict'][key]

    # ***********************************************************************
    # Fetch a previously entered value from a specified artifact dictionary
    # ***********************************************************************
    def get_artifact_value(self,
                           collection,
                           observationID,
                           productID,
                           uri,
                           key):
        """
        Return the value stored in the artifact dictionary for a previously
        entered collection, observationID, productID, uri, and key.

        Arguements:
        collection : the collection containing the productID
        productID : the productID containing the key in its plane_dict
        key : the key whose value is needed

        If any of collection, productID or key are not present, a
        KeyError exception will be raised.
        """
        return self.metadict[collection][observationID][productID
                                                        ]['fitsuri_dict'
                                                          ][uri][key]

    def build_metadict(self, filepath):
        """
        Generic routine to build the internal structure metadict (a nested set
        of ordered dictionaries) that will be used to control, sort and fill
        the override file templates.  The required metadata must already exist
        in the internal structures of caom2ingest.

        Arguments:
        filepath: path to file (may not exist if not local)
        local: True if the file is already on the disk

        The structure of metadict is a nested set of OrderedDict's and sets.
            [collection]
                [observationID]
                    ['memberset']
                    [productID]
                        ['uri_dict']
                        ['inputset']
                        ['fileset']
                        ['plane_dict']
                        [fitsuri]
                            ['custom']
        where:
            - The metadict is an OrderedDict of collections.
            - Each collection is an OrderedDict of observations.
            - Each observation is an OrderedDict of planes.
            - Each observation also contains an element called 'memberset'
              holding the set of members for the observation, which will be
              empty for a simple observation.
            - Each plane is an OrderedDict containing a set of fitsuri dicts.
            - Each plane contains an element 'uri_dict' that holds an
              OrderedDict of input URIs to pass to fits2caom2.  The uri is the
              key into the dictionary, where the value is the path to the file
              if it is local or None if it should be fetched from AD.
            - Each plane contains an element 'inputset' that holds a set of
              provenance input URIs for this plane, which can be empty.
            - Each plane also contains an element 'plane_dict' that is an
              OrderedDict holding items to add to the plane part of the
              override file.  The 'plane_dict' can be empty.
            - Each fitsuri dict is an OrderedDict containing items to include
              in the override file for that fitsuri.
            - The "custom" item inside the fitsuri is an OrderedDict of
              items that will be used to create archive-specific
              structures in the "science" chunks of an artifact.
              Archive-specific code should override the
              build_fitsuri_custom() method.
        """
        logger.debug('build_metadict')

        # In check mode, errors should not raise exceptions
        raise_exception = True
        if not (self.store or self.ingest):
            raise_exception = False

        # If the plane_dict is completely empty, skip further processing
        if self.override_items:
            # ****************************************************************
            # fetch the required keys from self.plane_dict
            # ****************************************************************
            if not self.collection:
                if raise_exception:
                    raise CAOMError(filepath + ' does not define the required'
                                    ' key "collection"')
                else:
                    return

            if not self.observationID:
                if raise_exception:
                    raise CAOMError(filepath + ' does not define the required'
                                    ' key "observationID"')
                else:
                    return

            if not self.productID:
                if raise_exception:
                    raise CAOMError(
                        filepath + ' does not define the required' +
                        ' key "productID"')
                else:
                    return

            if not self.uri:
                if raise_exception:
                    raise CAOMError(filepath + ' does not call fitsfileURI()'
                                    ' or fitsextensionURI()')
                else:
                    return

            logger.info(
                'PROGRESS: collection="%s" observationID="%s" productID="%s"',
                self.collection, self.observationID, self.productID)

            # ****************************************************************
            # Build the dictionary structure
            # ****************************************************************
            if self.collection not in self.metadict:
                self.metadict[self.collection] = OrderedDict()
            thisCollection = self.metadict[self.collection]

            if self.observationID not in thisCollection:
                thisCollection[self.observationID] = OrderedDict()
            thisObservation = thisCollection[self.observationID]

            # ****************************************************************
            # If memberset is not empty, the observation is a composite.
            # The memberset is the union of the membersets from all the
            # files in the observation.
            # ****************************************************************
            if 'memberset' not in thisObservation:
                thisObservation['memberset'] = set([])
            if self.memberset:
                thisObservation['memberset'] |= self.memberset

            # ****************************************************************
            # Create the plane-level structures
            # ****************************************************************
            if self.productID not in thisObservation:
                thisObservation[self.productID] = OrderedDict()
            thisPlane = thisObservation[self.productID]

            # ****************************************************************
            # Items in the plane_dict accumulate so a key will be defined for
            # the plane if it is defined by any file.  If a key is defined
            # by several files, the definition from the last file is used.
            # ****************************************************************
            if 'plane_dict' not in thisPlane:
                thisPlane['plane_dict'] = OrderedDict()
            if self.plane_dict:
                for key in self.plane_dict:
                    # Handle release_date as a special case
                    if (key == 'release_date' and key in thisPlane and
                            self.plane_dict[key] <=
                            thisPlane['plane_dict'][key]):
                        continue
                    thisPlane['plane_dict'][key] = self.plane_dict[key]

            # ****************************************************************
            # If inputset is not empty, the provenance should be filled.
            # The inputset is the union of the inputsets from all the files
            # in the plane.  Beware that files not yet classified into
            # inputURI's may still remain in fileset, and will be
            # resolved if possible in checkProvenanceInputs.
            # ****************************************************************
            if 'inputset' not in thisPlane:
                thisPlane['inputset'] = set([])
            if self.inputset:
                thisPlane['inputset'] |= self.inputset

            # ****************************************************************
            # The fileset is the set of input files that have not yet been
            # identified as being recorded in any plane yet.
            # ****************************************************************
            if 'fileset' not in thisPlane:
                thisPlane['fileset'] = set([])
            if self.fileset:
                thisPlane['fileset'] |= self.fileset

            # ****************************************************************
            # Record the uri and (optionally) the filepath
            # ****************************************************************
            if 'uri_dict' not in thisPlane:
                thisPlane['uri_dict'] = OrderedDict()
            if self.uri not in thisPlane['uri_dict']:
                if self.local:
                    thisPlane['uri_dict'][self.uri] = filepath
                else:
                    thisPlane['uri_dict'][self.uri] = None

            # ****************************************************************
            # Foreach fitsuri in fitsuri_dict, record the metadata
            # ****************************************************************
            for fitsuri in self.fitsuri_dict:
                # ********************************************************
                # Create the fitsuri-level structures
                # ********************************************************
                if fitsuri not in thisPlane:
                    thisPlane[fitsuri] = OrderedDict()
                    thisPlane[fitsuri]['custom'] = OrderedDict()
                thisFitsuri = thisPlane[fitsuri]

                # ********************************************************
                # Copy the fitsuri dictionary
                # ********************************************************
                for key in self.fitsuri_dict[fitsuri]:
                    if key == 'custom':
                        thisCustom = thisFitsuri[key]
                        for customkey in self.fitsuri_dict[fitsuri][key]:
                            thisCustom[customkey] = \
                                self.fitsuri_dict[fitsuri][key][customkey]
                    else:
                        thisFitsuri[key] = self.fitsuri_dict[fitsuri][key]

    def verifyFileInAD(self, filename, file_id):
        """
        Use the data_web client to verify that file_id is in self.archive
        """
        if not self.data_web.info(self.archive, file_id):
            self.dew.error(filename,
                           'file_id = ' + file_id +
                           ' has not yet been stored in ' + self.archive)

    def storeFiles(self):
        """
        If files approved for storage are in vos, move them into AD.
        If storemethod == 'pull', use the VOspace e-transfer protocol.
        If storemethod == 'push', copy the files into a local directory
        and push them into AD using the data web service.

        This does not check that the transfer completes successfully.
        """
        transfer_dir = None
        if (self.storemethod == 'pull'
                and not self.local
                and self.userconfig.has_section('vos')
                and self.userconfig.has_option('vos', 'transfer')):

            transfer_dir = self.userconfig.get('vos', 'transfer')
            if not self.vosclient.isdir(transfer_dir):
                raise CAOMError('transfer_dir = ' + transfer_dir +
                                ' does not exist')

            for filelist in (self.data_storage, self.preview_storage):
                for filepath in filelist:
                    basefile = os.path.basename(filepath)
                    file_id = self.make_file_id(basefile)
                    logger.info('LINK: %s', filepath)
                    if transfer_dir:
                        self.vosclient.link(filepath,
                                            transfer_dir + '/' + basefile)

        elif self.storemethod == 'push':
            for filelist in (self.data_storage, self.preview_storage):
                for filepath in filelist:
                    basefile = os.path.basename(filepath)
                    file_id = self.make_file_id(basefile)
                    logger.info('PUT: %s', filepath)
                    if self.local:
                        tempfile = filepath
                    else:
                        tempfile = os.path.join(self.workdir, basefile)
                        self.vosclient.copy(filepath, tempfile)
                    try:
                        if not self.data_web.put(tempfile,
                                                 self.archive,
                                                 file_id,
                                                 self.stream):
                            self.dew.error(filepath,
                                           'failed to push into AD using the '
                                           'data_web_client')
                    finally:
                        if not self.local and os.path.exists(tempfile):
                            os.remove(tempfile)
        else:
            raise CAOMError('storemethod = ' + self.storemethod +
                            'has not been implemented')

    def checkMembers(self):
        """
        Checking membership will be archive-specific
        """
        pass

    def checkProvenanceInputs(self):
        """
        Checking provenance inputs will be archive-specific
        """
        pass

    # ***********************************************************************
    # Prepare the override file for a plane
    # ***********************************************************************
    def prepare_override_info(self, collection, observationID, productID):
        """
        Prepare the information required in override files for a plane specified
        by the collection, observationID and productID.

        Arguments:
        collection : the collection containing observationID
        observationID : the observationID containing productID
        productID : productID for this plane

        Returns:
        A tuple (general, section) containing the global and URI-specific
        parts of the override information.
        """
        thisObservation = self.metadict[collection][observationID]
        thisPlane = thisObservation[productID]

        sections = OrderedDict()

        # Prepare artifact-specific overrides.  This involves filtering
        # the data structure to remove things which don't correpsond to
        # sections of the override file (e.g. "plane_dict") and things
        # which shouldn't appear in individual secions (e.g. "custom").
        for fitsuri in thisPlane:
            if fitsuri not in ('uri_dict',
                               'inputset',
                               'fileset',
                               'plane_dict'):
                thisFitsuri = thisPlane[fitsuri].copy()
                try:
                    del thisFitsuri['custom']
                except KeyError:
                    pass
                sections[fitsuri] = thisFitsuri

        return (thisPlane['plane_dict'], sections)

    # ***********************************************************************
    # Add members to the observation xml
    # ***********************************************************************
    def replace_members(self, thisObservation, thisPlane):
        """
        For the current plane, insert the full set of members in the
        plane_dict.  The memberset should contain only caom2.ObservationURI
        objects.

        Arguments:
        collection: the collection for this plane
        observationID: the observationID for this plane
        productID: the the productID for this plane
        """
        memberset = thisObservation['memberset']
        if 'algorithm.name' in thisPlane['plane_dict']:
            logger.debug('replace_members: algorithm.name = %s',
                         thisPlane['plane_dict']['algorithm.name'])
            logger.debug('memberset = %s',
                         repr([m.uri for m in list(memberset)]))

            if (memberset and
                    thisPlane['plane_dict']['algorithm.name'] != 'exposure'):

                thisPlane['plane_dict']['members'] = ' '.join(
                    sorted([m.uri for m in list(memberset)]))
            elif 'members' in thisPlane['plane_dict']:
                del thisPlane['plane_dict']['members']

    # ***********************************************************************
    # Add inputs to a plane in an observation xml
    # ***********************************************************************
    def replace_inputs(self, thisObservation, thisPlane):
        """
        For the current plane, insert the full set of inputs in the plane_dict

        Arguments:
        thisObservation: generic argument, not needed in this case
        thsPlane: the plane structire in metadict to update
        """
        # Need the provenance.name to create a provenance structure
        if 'provenance.name' in thisPlane['plane_dict']:
            inputset = thisPlane['inputset']
            logger.debug('replace_inputs: provenance.name = %s',
                         thisPlane['plane_dict']['provenance.name'])
            logger.debug('inputset = %s',
                         repr([i.uri for i in list(inputset)]))

            if inputset:
                thisPlane['plane_dict']['provenance.inputs'] = ' '.join(
                    sorted([i.uri for i in list(inputset)]))
            elif 'provenance.inputs' in thisPlane['plane_dict']:
                del thisPlane['plane_dict']['provenance.inputs']

    # ***********************************************************************
    # Ingest planes from metadict, tracking members and inputs
    # ***********************************************************************
    def ingestPlanesFromMetadict(self):
        """
        Generic routine to ingest the planes in metadict, keeping track of
        members and inputs.

        Arguments:
        <none>
        """

        for collection in self.metadict:
            thisCollection = self.metadict[collection]
            for observationID in thisCollection:
                obsuri = self.observationURI(collection,
                                             observationID)
                with self.repository.process(obsuri) as wrapper:
                    if wrapper.observation is not None:
                        self.remove_excess_parts(wrapper.observation)

                    thisObservation = thisCollection[observationID]
                    for productID in thisObservation:
                        if productID != 'memberset':
                            thisPlane = thisObservation[productID]

                            logger.info('PROGRESS ingesting collection="%s"  '
                                        'observationID="%s" productID="%s"',
                                        collection, observationID, productID)

                            self.replace_members(thisObservation,
                                                 thisPlane)

                            self.replace_inputs(thisObservation,
                                                thisPlane)

                            override = self.prepare_override_info(
                                collection, observationID, productID)

                            # *******************************************
                            # Run fits2caom2
                            # *******************************************
                            urilist = sorted(thisPlane['uri_dict'].keys())
                            if urilist:
                                if self.local:
                                    filepathlist = [thisPlane['uri_dict'][u]
                                                    for u in urilist]
                                else:
                                    filepathlist = None
                            else:
                                logger.error(
                                    'for %s/%s/%s, uri_dict is empty so '
                                    'there is nothing to ingest',
                                    collection, observationID, productID)
                                raise CAOMError('Nothing to ingest')

                            arg = thisPlane.get('fits2caom2_arg', None)

                            try:
                                wrapper.observation = run_fits2caom2(
                                    collection=collection,
                                    observationID=observationID,
                                    productID=productID,
                                    observation=wrapper.observation,
                                    override_info=override,
                                    file_uris=urilist,
                                    local_files=filepathlist,
                                    workdir=self.workdir,
                                    config_file=self.config,
                                    default_file=self.default,
                                    caom2_reader=self.repository.reader,
                                    caom2_writer=self.repository.writer,
                                    arg=arg,
                                    debug=self.debug,
                                    big=self.big,
                                    dry_run=self.test)
                                logger.info(
                                    'INGESTED: observationID=%s productID="%s"',
                                    observationID, productID)

                            except CAOMError:
                                # Transitional code: before run_fits2caom2 was
                                # extracted from this class, it set
                                # self.errors and raised this exception.
                                # TODO: remove self.errors and just use
                                # exception handling.
                                self.errors = True
                                raise

                            for fitsuri in thisPlane:
                                if fitsuri not in ('plane_dict',
                                                   'uri_dict',
                                                   'inputset',
                                                   'fileset'):

                                    self.build_fitsuri_custom(wrapper.observation,
                                                              collection,
                                                              observationID,
                                                              productID,
                                                              fitsuri)

                            self.build_plane_custom(wrapper.observation,
                                                    collection,
                                                    observationID,
                                                    productID)

                    self.build_observation_custom(wrapper.observation,
                                                  collection,
                                                  observationID)

                logger.info('SUCCESS observationID="%s"', observationID)

    def remove_excess_parts(self, observation, excess_parts=50):
        """
        Check for artifacts with excess parts from a previous
        ingestion run.

        Takes a CAOM-2 observation object and checks for any artifacts
        which have more parts than noted in self.artifact_part_count.
        Any excess parts will be removed.  This is necessary because
        fits2caom2 does not remove parts left over from previous
        ingestions which no longer correspond to FITS extensions
        which still exist.

        A warning will be issued for artifacts not mentioned in
        self.artifact_part_count with more than 'excess_parts'.
        """

        for plane in observation.planes.values():
            for artifact in plane.artifacts.values():
                uri = artifact.uri
                # Is this an artifact we are processing?  (i.e. we have a
                # part count for it)
                if uri in self.artifact_part_count:
                    part_count = self.artifact_part_count[uri]
                    n_removed = 0

                    # The JCMT archive currently only has integer part names
                    # but these are not stored in order.  We need to sort
                    # them (into numeric order) in order to be able to
                    # remove those for the later FITS extensions first.
                    part_names = list(artifact.parts.keys())
                    part_names.sort(cmp=lambda x, y: cmp(int(x), int(y)))

                    while len(part_names) > part_count:
                        artifact.parts.pop(part_names.pop())
                        n_removed += 1

                    if n_removed:
                        logger.info('Removed %i excess parts for %s',
                                    n_removed, uri)

                    else:
                        logger.debug('No excess parts for %s', uri)

                # Otherwise issue a warning if we seem to have an excessive
                # number of parts for the artifact.
                else:
                    if len(artifact.parts) > 50:
                        logger.warning('More than %i parts for %s',
                                       excess_parts, uri)

    # ***********************************************************************
    # placeholders for archive-specific customization
    # ***********************************************************************
    def build_fitsuri_custom(self,
                             observation,
                             collection,
                             observationID,
                             productID,
                             fitsuri):
        """
        Customize as required
        """
        pass

    def build_plane_custom(self,
                           observation,
                           collection,
                           observationID,
                           productID,
                           fitsuri):
        """
        Customize as required
        """
        pass

    def build_observation_custom(self,
                                 observation,
                                 collection,
                                 observationID,
                                 productID,
                                 fitsuri):
        """
        Customize as required
        """
        pass

    # ***********************************************************************
    # Run the program
    # ***********************************************************************
    def run(self):
        """
        Generic method to run the checks or persistence

        Returns True on success, False otherwise.
        """
        # metadict is the fundamental structure in the program, sorting
        # files by observation, plane and file, and holding all the relevant
        # metadata in a set of nested dictionaries.
        self.ap = argparse.ArgumentParser(self.progname)
        self.defineCommandLineSwitches()

        self.args = self.ap.parse_args()
        self.processCommandLineSwitches()

        try:
            self.logCommandLineSwitches()

            # Read list of files from VOspace and do things
            self.data_web = data_web_client(self.workdir)

            with delayed_error_warning(self.workdir,
                                       self.archive,
                                       self.fileid_regex_dict,
                                       self.make_file_id).gather() as self.dew:

                self.commandLineContainers()
                for c in self.containerlist:
                    logger.info('PROGRESS: container = %s', c.name)
                    self.fillMetadict(c)
                    self.checkMembers()
                    self.checkProvenanceInputs()
                    if self.dew.error_count() == 0:
                        if self.store:
                            self.storeFiles()
                        if self.ingest:
                            self.ingestPlanesFromMetadict()
                    else:
                        self.errors = True
                    if self.dew.warning_count():
                        self.warnings = True

            # declare we are DONE
            logger.info('DONE')

        except Exception as e:
            self.errors = True

            # Log this previously uncaught error, but let it pass
            logger.exception('Error during ingestion')

        return not self.errors
