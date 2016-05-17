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

"""
The jcmt2caom2ingest class immplements methods to collect metadata from a set
of FITS files and from the jcmt database that will be passed to fits2caom2 to
construct a caom2 observation.  Once completed, it is serialized to a temporary
xml file in workdir and copied to the CAOM-2 repository.

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

Original documentation from ingest2caom2:

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
"""

__author__ = "Russell O. Redman"

import argparse
from astropy.time import Time
from ConfigParser import SafeConfigParser
from collections import OrderedDict
from contextlib import closing
import datetime
import logging
import os
try:
    from astropy.io import fits
except:
    import pyfits as fits
import re
import shutil
import subprocess
import sys

from vos.vos import Client

from omp.db.part.arc import ArcDB

from caom2.caom2_chunk import Chunk
from caom2.caom2_composite_observation import CompositeObservation
from caom2.caom2_enums import CalibrationLevel
from caom2.caom2_enums import ObservationIntentType
from caom2.caom2_enums import ProductType
from caom2.caom2_observation_uri import ObservationURI
from caom2.caom2_plane_uri import PlaneURI
from caom2.caom2_simple_observation import SimpleObservation
from caom2.wcs.caom2_axis import Axis
from caom2.wcs.caom2_coord_axis1d import CoordAxis1D
from caom2.wcs.caom2_coord_bounds1d import CoordBounds1D
from caom2.wcs.caom2_coord_range1d import CoordRange1D
from caom2.wcs.caom2_ref_coord import RefCoord
from caom2.wcs.caom2_temporal_wcs import TemporalWCS

from tools4caom2.__version__ import version as tools4caom2version
from tools4caom2.caom2repo_wrapper import Repository
from tools4caom2.container.adfile import adfile_container
from tools4caom2.container.filelist import filelist_container
from tools4caom2.container.vos import vos_container
from tools4caom2.data_web_client import data_web_client
from tools4caom2.error import CAOMError
from tools4caom2.fits2caom2 import run_fits2caom2
from tools4caom2.mjd import utc2mjd
from tools4caom2.utdate_string import UTDATE_REGEX
from tools4caom2.utdate_string import utdate_string
from tools4caom2.util import make_file_id_no_ext
from tools4caom2.validation import CAOMValidation, CAOMValidationError

from jcmt2caom2.__version__ import version as jcmt2caom2version
from jcmt2caom2.caom2_tap import CAOM2TAP
from jcmt2caom2.jsa.instrument_keywords import instrument_keywords
from jcmt2caom2.jsa.instrument_name import instrument_name
from jcmt2caom2.instrument.scuba2 import scuba2_spectral_wcs
from jcmt2caom2.jsa.intent import intent
from jcmt2caom2.jsa.obsid import obsidss_to_obsid
from jcmt2caom2.jsa.product_id import product_id
from jcmt2caom2.jsa.target_name import target_name
from jcmt2caom2.jsa.tile import jsa_tile_wcs
from jcmt2caom2.project import get_project_pi_title

logger = logging.getLogger(__name__)


# Utility functions
def is_defined(key, header):
    """
    return True if key is in header and has a defined value, False otherwise
    This is useful for optional headers whose absence is not an error, or for
    metadata with more complicated logic than is supported using the
    prepackaged tests in CAOMValidation.
    """
    return (key in header and header[key] != fits.card.UNDEFINED)


def is_blank(key, header):
    """
    return True if key is in header and has an undefined value, False otherwise
    This is useful for optional headers whose presence or absence acts as a
    flag for some condition.
    """
    return (key in header and header[key] == fits.card.UNDEFINED)


def read_recipe_instance_mapping():
    """
    Read the recipe instance mapping file.

    Return a dictionary of JAC job names "jac-?????????" containing the
    old CADC recipe instance number as the value.
    """

    result = {}

    with open('/net/kamaka/export/data/jsa_proc/recipe-instance-mapping.txt') as f:
        for line in f:
            line = line.strip()
            if line.startswith('#') or not line:
                continue

            (cadc_rc_inst, jsa_proc_job, tag) = line.split(' ', 2)
            result['jac-{0:09d}'.format(int(jsa_proc_job))] = cadc_rc_inst

    return result


class jcmt2caom2ingest(object):
    """
    A class to ingest reduced data products into the JSA.
    """

    speedOfLight = 2.99792485e8  # Speed of light in m/s
    freq_csotau = 225.0e9  # Frequency of CSO tau meter in Hz
    lambda_csotau = '%12.9f' % (speedOfLight / freq_csotau)
    productType = {'cube': '0=science,1=noise,auxiliary',
                   'reduced': '0=science,1=noise,auxiliary',
                   'rsp': '0=preview,1=noise,auxiliary',
                   'rimg': '0=preview,1=noise,auxiliary',
                   'healpix': '0=science,1=noise,auxiliary',
                   'hpxrsp': '0=preview,1=noise,auxiliary',
                   'hpxrimg': '0=preview,1=noise,auxiliary',
                   'peak-cat': '1=science,auxiliary',
                   'extent-cat': '1=science,auxiliary',
                   'extent-mask': 'auxiliary',
                   'extent-moc': '1=science,auxiliary',
                   'tile-moc': '1=science,auxiliary',
                   }

    def __init__(self):
        """
        Initialize the caom2ingest structure, especially the attributes
        storing default values for command line arguments.
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

        # routine to convert filepaths into file_ids
        # The default routine supplied here should work for most archives.
        self.make_file_id = make_file_id_no_ext

        # temporary disk space for working files
        self.workdir = None

        # Ingestion parameters and structures
        self.verbose = False
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

        # Dictionary of explicit WCS infomation, by FITS URI.
        self.explicit_wcs = {}

        # Lists of files to be stored, or to check that they are in storage
        # Data files are added to data_storage iff they report no errors
        self.data_storage = []
        # Preview candidates are added as they are encountered and removed
        # if they do not match any planes.
        self.preview_storage = []

        # list of containers for input files
        self.containerlist = []

        # Validation object
        self.validation = None

        # TAP client
        self.tap = None

        # Prepare CAOM-2 repository client.
        self.repository = Repository()

        # A dictionary giving the number of parts which should be in each
        # artifact.  When we read a FITS file, the part count will be written
        # into this hash to allow us to identify and remove left-over
        # spurious parts from the CAOM-2 records.
        self.artifact_part_count = {}

        self.archive = 'JCMT'
        self.stream = 'product'

        self.collection_choices = ['JCMT', 'JCMTLS', 'JCMTUSER', 'SANDBOX']
        self.external_collections = ['JCMTLS', 'JCMTUSER']

        # set default locations for the fits2caom2 config files
        if not os.path.isdir(self.configpath):
            raise RuntimeError('The config directory ' + self.configpath +
                               ' does not exist')

        configpath = os.path.join(self.configpath, 'jcmt_stdpipe.config')
        if os.path.exists(configpath):
            self.config = configpath
        else:
            raise RuntimeError('The config file ' + configpath +
                               ' does not exist')

        defaultpath = os.path.join(self.configpath, 'jcmt_stdpipe.default')
        if os.path.exists(defaultpath):
            self.default = defaultpath
        else:
            raise RuntimeError('The default file ' + defaultpath +
                               ' does not exist')

        # Connection to database
        self.conn = None

        self.remove_dict = {}
        self.remove_id = []

        self.dprcinst = None

        # Read recipe instance mapping file.
        self.recipe_instance_mapping = read_recipe_instance_mapping()

        self.xmloutdir = None

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
        --xmloutdir  : (optional) directory into which to write the new/updated
                       CAOM-2 documents for debugging purposes

        # fits2caom2 arguments
        --collection : (required) collection to use for ingestion
        --config     : (optional) path to fits2caom2 config file
        --default    : (optional) path to fits2caom2 default file

        # File and directory options
        --workdir    : (optional) working directory (default = cwd)

        # debugging options
        --verbose, -v: (optional) log all messages and retain temporary files
                       on error
        --dry-run, -n: (optional) simulate operation of fits2caom2
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
            '--dry-run', '-n',
            action='store_true',
            dest='dry_run',
            help='(optional) simulate operation of fits2caom2')
        self.ap.add_argument(
            '--verbose', '-v',
            action='store_true',
            help='(optional) show all messages, pass --debug to fits2caom2,'
            ' and retain all xml and override files')
        self.ap.add_argument(
            '--xmloutdir',
            help='(optional) directory into which to write XML files')

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

        self.dry_run = self.args.dry_run

        if self.args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
            self.verbose = True

        # create workdir if it does not already exist
        if not os.path.exists(self.workdir):
            os.makedirs(self.workdir)

        if self.args.store:
            self.store = self.args.store
        self.storemethod = self.args.storemethod

        if self.args.ingest:
            self.ingest = self.args.ingest

        self.xmloutdir = self.args.xmloutdir

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

        self.tap = CAOM2TAP(self.proxy)
        if not os.path.exists(self.proxy):
            raise CAOMError('proxy does not exist: ' + self.proxy)

        if not os.path.isdir(self.workdir):
            raise CAOMError('workdir is not a directory: ' + self.workdir)

        if self.config and not os.path.isfile(self.config):
            raise CAOMError('config file does not exist: ' + str(self.config))

        if self.default and not os.path.isfile(self.default):
            raise CAOMError('default file does not exist: ' +
                            str(self.default))

        logger.info('jcmt2caom2version    = %s', jcmt2caom2version)

    def getfilelist(self, rootdir):
        """
        Return a list of valid files in the directory tree rooted at dirpath.

        Arguments:
        rootdir: absolute path to the root of the directory tree
        """
        mylist = []

        for dirpath, dirlist, filelist in os.walk(rootdir):
            for f in filelist:
                filepath = os.path.join(dirpath, f)

                try:
                    self.validation.check_name(filepath)
                    self.validation.check_size(filepath)
                except CAOMValidationError:
                    pass
                else:
                    mylist.append(filepath)

            for d in dirlist:
                mylist.extend(self.getfilelist(os.path.join(dirpath, d)))

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
                filelist = self.getfilelist(self.indir)
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
                                      self.validation,
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

    def clear(self):
        """
        Clear the local plane and artifact dictionaries before each file is
        read.

        Arguments:
        <none>
        """
        self.uri = ''
        self.observationID = None
        self.productID = None
        self.plane_dict.clear()
        self.fitsuri_dict.clear()
        self.memberset.clear()
        self.inputset.clear()
        self.override_items = 0

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

        self.clear()
        # If the file is not a FITS file or is in serious violation of the FITS
        # standard, substitute an empty dictionary for the headers.  This is
        # a silent replacement, not an error, to allow non-FITS files to be
        # ingested along with regular FITS files.
        try:
            self.validation.check_name(filepath)
        except CAOMValidationError:
            return

        try:
            with closing(fits.open(filepath, mode='readonly')) as f:
                head = f[0].header
                self.artifact_part_count[self.fitsfileURI(
                    self.archive, file_id)] = len(f)

            head['file_id'] = file_id
            head['filepath'] = filepath
            if isinstance(container, vos_container):
                head['VOSPATH'] = container.vosroot
                head['SRCPATH'] = container.uri(file_id)
            else:
                head['SRCPATH'] = filepath

            logger.debug('...got primary header from %s', filepath)

        except:
            head = {}
            head['file_id'] = file_id
            head['filepath'] = filepath
            logger.debug('...could not read primary header from ',
                         filepath)

        if self.ingest:
            self.validation.is_in_archive(filepath)

        self.build_dict(head)
        self.build_metadict(filepath)

        self.data_storage.append(head['SRCPATH'])

#        else:
#            self.preview_storage.append(container.uri(file_id))

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
            - The metadict is an OrderedDict of observations.
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
            # Fetch the required keys from self.plane_dict
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

            # Build the dictionary structure
            if self.observationID not in self.metadict:
                self.metadict[self.observationID] = OrderedDict()
            thisObservation = self.metadict[self.observationID]

            # If memberset is not empty, the observation is a composite.
            # The memberset is the union of the membersets from all the
            # files in the observation.
            if 'memberset' not in thisObservation:
                thisObservation['memberset'] = set([])
            if self.memberset:
                thisObservation['memberset'] |= self.memberset

            # Create the plane-level structures
            if self.productID not in thisObservation:
                thisObservation[self.productID] = OrderedDict()
            thisPlane = thisObservation[self.productID]

            # Items in the plane_dict accumulate so a key will be defined for
            # the plane if it is defined by any file.  If a key is defined
            # by several files, the definition from the last file is used.
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

            # If inputset is not empty, the provenance should be filled.
            # The inputset is the union of the inputsets from all the files
            # in the plane.  Beware that files not yet classified into
            # inputURI's may still remain in fileset, and will be
            # resolved if possible in checkProvenanceInputs.
            if 'inputset' not in thisPlane:
                thisPlane['inputset'] = set([])
            if self.inputset:
                thisPlane['inputset'] |= self.inputset

            # The fileset is the set of input files that have not yet been
            # identified as being recorded in any plane yet.
            if 'fileset' not in thisPlane:
                thisPlane['fileset'] = set([])
            if self.fileset:
                thisPlane['fileset'] |= self.fileset

            # Record the uri and (optionally) the filepath
            if 'uri_dict' not in thisPlane:
                thisPlane['uri_dict'] = OrderedDict()
            if self.uri not in thisPlane['uri_dict']:
                if self.local:
                    thisPlane['uri_dict'][self.uri] = filepath
                else:
                    thisPlane['uri_dict'][self.uri] = None

            # Foreach fitsuri in fitsuri_dict, record the metadata
            for fitsuri in self.fitsuri_dict:
                # Create the fitsuri-level structures
                if fitsuri not in thisPlane:
                    thisPlane[fitsuri] = OrderedDict()
                    thisPlane[fitsuri]['custom'] = OrderedDict()
                thisFitsuri = thisPlane[fitsuri]

                # Copy the fitsuri dictionary
                for key in self.fitsuri_dict[fitsuri]:
                    if key == 'custom':
                        thisCustom = thisFitsuri[key]
                        for customkey in self.fitsuri_dict[fitsuri][key]:
                            thisCustom[customkey] = \
                                self.fitsuri_dict[fitsuri][key][customkey]
                    else:
                        thisFitsuri[key] = self.fitsuri_dict[fitsuri][key]

    def build_remove_dict(self, run_id):
        """
        Discover planes to remove.

        If identity_instance_id has not already been checked, read back a
        complete list of existing collections, observations and planes,
        which will be deleted if they are not replaced or updated by the
        current recipe instance.

        Arguments:
        run_id: a run identifer as a string to be compared with
                Plane.provenance_runID
        """

        # Did we already check this run_id?
        if run_id in self.remove_id:
            return

        self.remove_id.append(run_id)

        # Does this job have alternate run_id values we must also check for?
        run_ids = [run_id]
        if run_id in self.recipe_instance_mapping:
            run_ids.append(self.recipe_instance_mapping[run_id])

        # Get all planes with one of these run IDs and store in
        # self.remove_dict, organized by observation.
        for result in self.tap.get_planes_with_run_id(self.collection,
                                                      run_ids):
            if result.obs_id not in self.remove_dict:
                self.remove_dict[result.obs_id] = [result.prod_id]

            elif result.prod_id not in self.remove_dict[result.obs_id]:
                self.remove_dict[result.obs_id].append(result.prod_id)

    def build_dict(self, header):
        '''Archive-specific code to read the common dictionary from the
               file header.
           The following keys must be defined:
               collection
               observationID
               productID
        '''

        if 'file_id' not in header:
            raise CAOMError('No file_id in ' + repr(header))

        file_id = header['file_id']
        filename = header['filepath']
        self.validation.check_size(filename)

        logger.info('Starting %s', file_id)
        # Doing all the required checks here simplifies the code
        # farther down and ensures error reporting of these basic problems
        # even if the ingestion fails before reaching the place where the
        # header would be used.

        # Is this a JSA catalog file?
        is_catalog = is_defined('PRODID', header) and (
            header['PRODID'].startswith('extent-') or
            header['PRODID'].startswith('peak-'))

        # Check that mandatory file headers exist that validate the FITS
        # file structure
        if not is_catalog:
            for key in ('BITPIX',
                        'CHECKSUM',
                        'DATASUM'):
                self.validation.expect_keyword(filename, key, header)

        # Observation metadata
        self.validation.restricted_value(filename, 'INSTREAM', header,
                                         (self.collection_choices
                                          if self.collection == 'SANDBOX'
                                          else (self.collection,)))

        instream = None
        if is_defined('INSTREAM', header):
            instream = header['INSTREAM']

        # Conditionally mandatory
        # Observation.algorithm
        algorithm = 'custom'
        if is_defined('ASN_TYPE', header):
            algorithm = header['ASN_TYPE']
        logger.info('PROGRESS: %s', header['SRCPATH'])

        if algorithm == 'obs':
            # Obs products can only be ingested into the JCMT collection
            # External data providers must choose a different grouping
            # algorithm
            self.validation.restricted_value(filename,
                                             'INSTREAM', header, ['JCMT'])
            self.validation.expect_keyword(filename,
                                           'OBSID',
                                           header)
            algorithm = 'exposure'
            self.observationID = header['OBSID']
        else:
            # any other value for algorithm indicates a composite observation
            self.validation.expect_keyword(filename,
                                           'ASN_ID',
                                           header)
            self.observationID = header['ASN_ID']

            # Check for duplicate observationIDs.
            # Do not do this for obs products, since the raw data can be
            # ingested before or after the processed data.
            # This is always OK in the SANDBOX.
            # In JCMT, --replace is never needed for observations in
            # the JCMT collection because replacement is expected.
            # Otherwise,  issue an error if --replace is not specified and
            # the observation exists in the collection, or if the
            # observation pre-exists in another collection.
            for coll in self.tap.get_collections_with_obs_id(
                    self.observationID):
                # Do not raise errors for ingestions into the SANDBOX
                # or into JCMT if coll is also JCMT.
                if coll == self.collection:
                    if self.collection in ('JCMTLS', 'JCMTUSER'):
                        if not self.replace:
                            # Raise an error if --replace not is
                            # specified but the observation already
                            # exists in the collection
                            raise CAOMError(
                                'file: {0}: Must specify --replace if'
                                ' observationID = "{1}" already exists'
                                ' in collection = "{2}"'.format(
                                    filename, self.observationID,
                                    self.collection))
                elif self.collection != 'SANDBOX':
                    # Complain if the observation matches
                    # an observation in a different collection
                    raise CAOMError(
                        'file: {0}, observationID = "{1}" is also in use'
                        ' in collection = "{2}"'.format(
                            filename, self.observationID, coll))

        self.add_to_plane_dict('algorithm.name', algorithm)

        # Optional Observation.proposal
        proposal_id = None
        proposal_project = None
        proposal_pi = None
        proposal_title = None
        survey_acronyms = ('CLS', 'DDS', 'GBS', 'JPS', 'NGS', 'SASSY', 'SLS')
        # We may need the proposal_project for the data processing project,
        # even if the PROJECT is ambiguous.
        if (is_defined('SURVEY', header) and
            self.validation.restricted_value(filename, 'SURVEY', header,
                                             survey_acronyms)):
            proposal_project = header['SURVEY']

        if algorithm == 'public':
            self.add_to_plane_dict(
                'proposal.id', 'JCMT-LR1')
            self.add_to_plane_dict(
                'proposal.pi', 'James Clerk Maxwell Telescope')
            self.add_to_plane_dict(
                'proposal.title', 'JCMT Legacy Release 1')

        elif is_defined('PROJECT', header):
            proposal_id = header['PROJECT']
            self.add_to_plane_dict('proposal.id', proposal_id)

            if proposal_project:
                self.add_to_plane_dict('proposal.project', proposal_project)

            if is_defined('PI', header):
                proposal_pi = header['PI']

            if is_defined('TITLE', header):
                proposal_title = header['TITLE']

            if not (proposal_pi and proposal_title):
                (proposal_pi, proposal_title) = get_project_pi_title(
                    header['PROJECT'], self.conn, self.tap)

            if proposal_pi is not None:
                self.add_to_plane_dict('proposal.pi', proposal_pi)
            if proposal_title is not None:
                self.add_to_plane_dict('proposal.title', proposal_title)

        # Observation membership headers, which are optional
        earliest_utdate = None
        if algorithm == 'exposure':
            if is_defined('DATE-OBS', header):
                earliest_utdate = Time(header['DATE-OBS']).mjd

        obscnt = None
        mbrcnt = None
        date_obs = None
        date_end = None
        release_date = None
        latest_release_date = None
        # obstimes records the (date_obs, date_end) for this file.
        # self.member_cache records these intervals for use with subsequent
        # files.
        obstimes = {}

        # The calculation of membership is long and ugly because there are
        # two different ways to do it, depending upon whether the files
        # supplied MBR or OBS headers.  The code is nearly the same
        # for both cases.
        if is_defined('MBRCNT', header):
            # Define membership using MBR headers
            # Each MBRn is a CAOM-2 URI for a member observation,
            # i.e. caom:<collection>/<observationID>
            # where <collection>=JCMT for observations recording raw data.
            mbrcnt = int(header['MBRCNT'])
            if mbrcnt > 0:
                for n in range(mbrcnt):
                    # verify that the expected membership headers are present
                    mbrkey = 'MBR' + str(n+1)
                    self.validation.expect_keyword(filename, mbrkey, header)
                    mbrn_str = header[mbrkey]
                    # mbrn contains a caom observation uri
                    mbr_coll, obsid = mbrn_str.split('/')
                    if mbr_coll != 'caom:JCMT':
                        raise CAOMError(
                            'file {0}: {1} must point to an observation in'
                            ' the JCMT collection: {2}'.format(
                                filename, mbrkey, mbrn_str))

                    mbrn = self.observationURI('JCMT', obsid)
                    mbr_date_obs = None
                    mbr_date_end = None

                    # Only get here if mbrn has a defined value
                    if mbrn in self.member_cache:
                        # Skip the query if this member has been cached
                        (this_mbrn,
                         mbr_date_obs,
                         mbr_date_end,
                         release_date) = self.member_cache[mbrn]
                        if (latest_release_date is None or
                                release_date > latest_release_date):

                            latest_release_date = release_date

                        logger.debug(
                            'fetch from member_cache[%s] = [%s, %s, %s, %s]',
                            mbrn, this_mbrn, date_obs, date_end, release_date)

                    else:
                        # Verify that the member header points to a real
                        # observation.
                        # Extract the start, end release times from the member.
                        # Also, do a nasty optimization for performance,
                        # caching useful information from the member for later
                        # re-use.

                        # To reduce the number of TAP queries, we will return
                        # all the files and planes in this observation, in the
                        # expectation that they will be part of the membership
                        # and provenance inputs for this release.

                        missing = True
                        for row in self.tap.get_obs_info(obsid):
                            if (not row.date_obs or
                                    not row.date_end or
                                    not row.release):
                                continue

                            # Only extract date_obs, date_end and release
                            # raw planes
                            if missing and re.match(r'raw.*', row.prod_id):
                                missing = False
                                if (latest_release_date is None or
                                        row.release >
                                        latest_release_date):

                                    latest_release_date = row.release
                                # cache mbrn, start, end and release
                                # caching mbrn is NOT needlessly repetitive
                                # because with obsn headers it will be
                                # different
                                logger.debug(
                                    'cache member_cache[%s] ='
                                    ' [%s, %s, %s, %s]',
                                    mbrn,
                                    mbrn, row.date_obs, row.date_end,
                                    row.release)
                                self.member_cache[mbrn] = (mbrn,
                                                           row.date_obs,
                                                           row.date_end,
                                                           row.release)
                                mbr_date_obs = row.date_obs
                                mbr_date_end = row.date_end

                            # Cache provenance input candidates
                            # Do NOT rewrite the file_id
                            if row.artifact_uri not in self.input_cache:
                                filecoll, this_file_id = \
                                    row.artifact_uri.split('/')
                                inURI = self.planeURI('JCMT',
                                                      obsid,
                                                      row.prod_id)
                                self.input_cache[this_file_id] = inURI
                                self.input_cache[inURI.uri] = inURI

                    # At this point we have mbrn, mbr_date_obs, mbr_date_end
                    # and release_date either from the member_cache or from
                    # the query
                    if mbr_date_obs:
                        if (earliest_utdate is None or
                                mbr_date_obs < earliest_utdate):

                            earliest_utdate = mbr_date_obs

                        if mbrn not in obstimes:
                            obstimes[mbrn] = (mbr_date_obs, mbr_date_end)

                        self.memberset.add(mbrn)

        elif is_defined('OBSCNT', header):
            obscnt = header['OBSCNT']
            if obscnt > 0:
                for n in range(obscnt):
                    mbrn = None
                    mbr_date_obs = None
                    mbr_date_end = None
                    obskey = 'OBS' + str(n+1)
                    # verify that the expected membership headers are present
                    self.validation.expect_keyword(filename, obskey, header)
                    # This is the obsid_subsysnr of a plane of raw data
                    obsn = header[obskey]

                    # Only get here if obsn has a defined value
                    if obsn in self.member_cache:
                        # Skip the query if this member has been cached
                        (obsid,
                         mbrn,
                         mbr_date_obs,
                         mbr_date_end,
                         release_date) = self.member_cache[obsn]
                        if (latest_release_date is None or
                                release_date > latest_release_date):

                            latest_release_date = release_date

                        logger.debug(
                            'fetch from member_cache[%s] = [%s, %s, %s, %s]',
                            obsn, mbrn.uri, date_obs, date_end, release_date)

                    else:
                        # Verify that the member header points to a real
                        # observation
                        # Extract the start, end release times from the member.
                        # Also, do a nasty optimization for performance,
                        # caching useful information from the member for later
                        # re-use.

                        # obsn contains an obsid_subsysnr
                        obsid_guess = obsidss_to_obsid(obsn)

                        for row in self.tap.get_obs_info(obsid_guess):
                            if (not row.date_obs or
                                    not row.date_end or
                                    not row.release):
                                continue

                            # Only cache member date_obs, date_end and
                            # release_date from raw planes
                            if re.match(r'raw.*', row.prod_id):
                                if (latest_release_date is None or
                                        row.release >
                                        latest_release_date):

                                    latest_release_date = row.release

                                mbrn = self.observationURI('JCMT',
                                                           obsid_guess)
                                # cache the members start and end times
                                logger.debug(
                                    'cache member_cache[%s] ='
                                    ' [%s, %s, %s, %s]',
                                    obsn, mbrn.uri, row.date_obs, row.date_end,
                                    row.release)
                                if mbrn not in self.member_cache:
                                    self.member_cache[obsn] = \
                                        (obsid_guess,
                                         mbrn,
                                         row.date_obs,
                                         row.date_end,
                                         row.release)
                                    mbr_date_obs = row.date_obs
                                    mbr_date_end = row.date_end

                            # Cache provenance input candidates
                            # Do NOT rewrite the file_id!
                            if row.artifact_uri not in self.input_cache:
                                filecoll, this_file_id = \
                                    row.artifact_uri.split('/')
                                inURI = self.planeURI('JCMT',
                                                      obsid_guess,
                                                      row.prod_id)
                                self.input_cache[this_file_id] = inURI
                                self.input_cache[inURI.uri] = inURI

                    if mbrn is None:
                        raise CAOMError('file {0}: {1} = {2}'
                                        ' is not present in the JSA'.format(
                                            filename, obskey, obsn))
                    else:
                        # At this point we have mbrn, date_obs, date_end and
                        # release_date either from the member_cache or from
                        # the query
                        if mbr_date_obs:
                            if (earliest_utdate is None or
                                    mbr_date_obs < earliest_utdate):

                                earliest_utdate = mbr_date_obs

                            if mbrn not in obstimes:
                                obstimes[mbrn] = (mbr_date_obs, mbr_date_end)
                            self.memberset.add(mbrn)

        # Only record the environment from single-member observations
        if algorithm == 'exposure' or (obscnt == 1 or mbrcnt == 1):
            # NB 'SEEINGST' is sometimes defined as an empty string which will
            # pass the >0.0 test
            if (is_defined('SEEINGST', header) and header['SEEINGST'] > 0.0 and
                    header['SEEINGST']):
                self.add_to_plane_dict('environment.seeing',
                                       '%f' % (header['SEEINGST'],))

            if is_defined('HUMSTART', header):
                # Humity is reported in %, but should be scaled to [0.0, 1.0]
                if header['HUMSTART'] < 0.0:
                    humidity = 0.0
                elif header['HUMSTART'] > 100.0:
                    humidity = 100.0
                else:
                    humidity = header['HUMSTART']
                self.add_to_plane_dict('environment.humidity',
                                       '%f' % (humidity,))

            if is_defined('ELSTART', header):
                self.add_to_plane_dict('environment.elevation',
                                       '%f' % (header['ELSTART'],))

            if is_defined('TAU225ST', header):
                # Some old data appears to have TAU225ST in string form
                # so convert to float in order to handle that data.
                self.add_to_plane_dict('environment.tau',
                                       '%f' % (float(header['TAU225ST']),))
                self.add_to_plane_dict('environment.wavelengthTau',
                                       jcmt2caom2ingest.lambda_csotau)

            if is_defined('ATSTART', header):
                self.add_to_plane_dict('environment.ambientTemp',
                                       '%f' % (header['ATSTART'],))

        # Calculate the observation type from OBS_TYPE and SAM_MODE,
        # if they are unambiguous.
        raw_obs_type = None
        obs_type = None
        if is_defined('OBS_TYPE', header):
            raw_obs_type = header['OBS_TYPE'].strip()

            obs_type = raw_obs_type
            if obs_type in ('flatfield', 'noise', 'setup', 'skydip'):
                raise CAOMError(
                    'file {0}: '
                    'observation types in (flatfield, noise, setup, '
                    'skydip) contain no astronomical data and cannot '
                    'be ingested'.format(filename))

            if is_defined('SAM_MODE', header) and raw_obs_type == "science":
                if header["SAM_MODE"] == "raster":
                    obs_type = 'scan'
                else:
                    obs_type = header['SAM_MODE'].strip()
            self.add_to_plane_dict('OBSTYPE', obs_type)

        # Record the instrument configuration if it is unambiguous.
        # It is possible in principle to combine data from multiple backends
        # (e.g. DAS and ACSIS for spectra and datacubes, or SCUBA-2 - ACSIS
        # for line-corrected continuum images), so unambiguous instrument
        # metadata are not mandatory.

        # We need the backend and instrument to check various modes, so try to
        # identify them now
        instrument_fullname = None
        instrument = None
        backend = None
        inbeam = None
        if is_defined('INSTNAME', header):
            instrument_fullname = header['INSTNAME'].strip().upper()
            if re.match(r'.*?SCUBA-2', instrument_fullname):
                instrument = 'SCUBA-2'
                backend = 'SCUBA-2'
            else:
                components = instrument_fullname.split('-')
                instrument = components[-2]
                backend = components[-1]
        else:
            # Try to define instrument_fullname from INSTRUME, INBEAM and
            # BACKEND
            if is_defined('INSTRUME', header):
                instrument = header['INSTRUME'].strip().upper()
            if is_defined('INBEAM', header):
                inbeam = header['INBEAM'].strip().upper()
            self.validation.restricted_value(
                filename, 'BACKEND', header,
                ('SCUBA-2', 'ACSIS', 'DAS', 'AOSC'))
            backend = header['BACKEND'].strip().upper()

            instrument_fullname = instrument_name(instrument,
                                                  backend,
                                                  inbeam)

        if instrument_fullname:
            self.add_to_plane_dict('instrument.name', instrument_fullname)

        # Only do these tests if the backend is OK
        if backend in ('ACSIS', 'DAS', 'AOS-C'):
            if inbeam and inbeam != 'POL':
                raise CAOMError('file {0}: INBEAM can only be blank or POL '
                                'for heterodyne observations'.format(filename))

            if is_defined('OBS_TYPE', header):
                self.validation.restricted_value(
                    filename, 'OBS_TYPE', header,
                    ['pointing', 'science', 'focus', 'skydip'])

            if is_defined('SAM_MODE', header):
                self.validation.restricted_value(
                    filename, 'SAM_MODE', header,
                    ['jiggle', 'grid', 'raster', 'scan'])

        elif backend == 'SCUBA-2':
            if is_defined('OBS_TYPE', header):
                self.validation.restricted_value(
                    filename, 'OBS_TYPE', header,
                    ['pointing', 'science', 'focus', 'skydip',
                        'flatfield', 'setup', 'noise'])

            if is_defined('SAM_MODE', header):
                self.validation.restricted_value(
                    filename, 'SAM_MODE', header,
                    ['scan', 'stare'])

        # Check some more detailed values by building instrument_keywords
        keyword_dict = {}
        if is_defined('SW_MODE', header):
            keyword_dict['switching_mode'] = header['SW_MODE']

        if inbeam:
            keyword_dict['inbeam'] = inbeam

        if is_defined('SCAN_PAT', header):
            keyword_dict['x_scan_pat'] = header['SCAN_PAT']

        if backend in ('ACSIS', 'DAS', 'AOS-C'):
            if is_defined('OBS_SB', header):
                keyword_dict['sideband'] = header['OBS_SB']

            if is_defined('SB_MODE', header):
                keyword_dict['sideband_filter'] = header['SB_MODE']

        thisBad, keyword_list = instrument_keywords('stdpipe',
                                                    instrument,
                                                    backend,
                                                    keyword_dict)
        self.instrument_keywords = ''
        if thisBad:
            raise CAOMError('instrument_keywords for file {0} could not be '
                            'constructed from {1!r}'.format(
                                filename, keyword_dict))
        else:
            self.instrument_keywords = ' '.join(keyword_list)
            self.add_to_plane_dict('instrument.keywords',
                                   self.instrument_keywords)

        # Telescope metadata. geolocation is optional.
        self.validation.restricted_value(filename, 'TELESCOP', header, ['JCMT'])

        # Target metadata
        self.validation.expect_keyword(filename, 'OBJECT', header)
        self.add_to_plane_dict('target.name', header['OBJECT'])

        if backend != 'SCUBA-2' and is_defined('ZSOURCE', header):
                self.add_to_plane_dict('target.redshift',
                                       str(header['ZSOURCE']))

        target_type = None
        if is_defined('TARGTYPE', header):
            self.validation.restricted_value(
                filename,
                'TARGTYPE', header, ['FIELD', 'OBJECT'])
            target_type = header['TARGTYPE']

        standard_target = 'FALSE'
        if is_defined('STANDARD', header) and header['STANDARD']:
            standard_target = 'TRUE'
        self.add_to_plane_dict('STANDARD', standard_target)

        moving = 'FALSE'
        # MOVING header is boolean
        if ((is_defined('MOVING', header) and header['MOVING']) or
                # Distinguish moving targets
                is_blank('OBSRA', header) or
                is_blank('OBSDEC', header)):
            moving = 'TRUE'
        self.add_to_plane_dict('target.moving', moving)

        if (moving == 'TRUE'and
                header['CTYPE1'][0:4] == 'OFLN' and
                'CTYPE1A' in header):
            # Use the first alternate coordinate system
            self.config = os.path.join(self.configpath,
                                       'jcmt_stdpipe_a.config')
            self.default = os.path.join(self.configpath,
                                        'jcmt_stdpipe_a.default')
        elif is_defined('OBSRA', header) and is_defined('OBSDEC', header):
            # Record the nominal target position
            self.add_to_plane_dict('target_position.cval1',
                                   str(header['OBSRA']))
            self.add_to_plane_dict('target_position.cval2',
                                   str(header['OBSDEC']))
            self.add_to_plane_dict('target_position.radesys',
                                   'ICRS')
            self.add_to_plane_dict('target_position.equinox',
                                   '2000.0')
        intent_val = None
        if obs_type and backend:
            intent_val = intent(raw_obs_type, backend).value
            self.add_to_plane_dict('obs.intent', intent_val)

        # Plane metadata
        # metadata needed to create productID
        self.validation.expect_keyword(filename, 'PRODUCT', header)
        product = header['PRODUCT']

        # The standard and legacy pipelines must have some standard keywords
        if (self.collection == 'JCMT' or instream == 'JCMT'):
            if backend == 'SCUBA-2':
                self.validation.expect_keyword(filename, 'FILTER', header)
            else:
                self.validation.expect_keyword(filename, 'RESTFRQ', header)
                self.validation.expect_keyword(filename, 'SUBSYSNR', header)
                self.validation.expect_keyword(filename, 'BWMODE', header)

        science_product = None
        filter = None
        restfreq = None
        subsysnr = None
        bwmode = None
        # Define productID as a string so it does not trigger later syntax
        # errors but will still test False.
        self.productID = ''

        if backend == 'SCUBA-2' and is_defined('FILTER', header):
            filter = str(header['FILTER'])
        else:
            # Allow RESTFREQ and RESTWAV as equivalents to RESTFRQ.
            if is_defined('RESTFREQ', header):
                restfreq = float(header['RESTFREQ'])
            elif is_defined('RESTWAV', header):
                restfreq = (jcmt2caom2ingest.speedOfLight /
                            float(header['RESTWAV']))
            elif is_defined('RESTFRQ', header):
                restfreq = float(header['RESTFRQ'])
            if is_defined('SUBSYSNR', header):
                subsysnr = str(header['SUBSYSNR'])
            if is_defined('BWMODE', header):
                bwmode = header['BWMODE']

        # Try to compute self.productID using the standard rules
        # First, determine the science_product
        if instream in self.external_collections:
            # Externally generated data products must define PRODID, which
            # will be used to fill productID and to define science_product
            # as the first dash-separated token in the string
            self.validation.expect_keyword(filename, 'PRODID', header)
            self.productID = header['PRODID']
            if re.search(r'-', self.productID):
                science_product = self.productID.split('-')[0]
            else:
                science_product = self.productID

        else:
            # Pipeline products must define the science_product as a function
            # of product, which is mandatory.
            # BEWARE that the same dictionary is used for both heterodyne and
            # continuum products.
            science_product_dict = {'reduced': 'reduced',
                                    'rsp': 'reduced',
                                    'rimg': 'reduced',
                                    'cube': 'cube',
                                    'healpix': 'healpix',
                                    'hpxrsp': 'healpix',
                                    'hpxrimg': 'healpix',
                                    'tile-moc': 'extent',
                                    'extent-moc': 'extent',
                                    'extent-mask': 'extent',
                                    'extent-cat': 'extent',
                                    'peak-cat': 'peak',
                                    }

            science_product = None
            if product in science_product_dict:
                science_product = science_product_dict[product]
            else:
                raise CAOMError('file: {0} product = "{1}" is not one of the'
                                ' pipeline products: {2!r}'.format(
                                    filename, product,
                                    sorted(science_product_dict.keys())))

            if filter:
                self.productID = product_id(backend,
                                            product=science_product,
                                            filter=filter)

            elif (restfreq and bwmode and subsysnr):
                if product in ['reduced', 'rimg', 'rsp', 'cube',
                               'healpix', 'hpxrsp', 'hpxrimg']:
                    self.productID = \
                        product_id(backend,
                                   product=science_product,
                                   restfreq=restfreq,
                                   bwmode=bwmode,
                                   subsysnr=subsysnr)

        # Is this the "main" product?  Originally this module just used
        # (product == science_product) but not all planes have a product
        # the name of which matches the product ID.  The name "science_product"
        # doesn't entirely make sense any more either -- some planes have
        # multiple products containing science data.  Also "tile-moc" isn't
        # really the "main" product, but it's the only one in its plane which
        # is guaranteed to exist (if the plane exists at all).
        is_main_product = ((product == science_product) or
                           (product == 'tile-moc') or
                           (product == 'peak-cat'))

        # Add this plane to the set of known file_id -> plane translations
        self.input_cache[file_id] = self.planeURI(self.collection,
                                                  self.observationID,
                                                  self.productID)

        # TODO: do we only need to do this for the "main" product?
        if instream == 'JCMT':
            if science_product in ['reduced', 'cube']:
                # Set release dates for non-healpix products
                if latest_release_date:
                    if algorithm != 'exposure':
                        # Don't set the Observation level release date for
                        # "exposures" because the raw data ingestion should
                        # do that.
                        self.add_to_plane_dict('obs.metaRelease',
                                               latest_release_date)
                    self.add_to_plane_dict('plane.metaRelease',
                                           latest_release_date)
                    self.add_to_plane_dict('plane.dataRelease',
                                           latest_release_date)
                else:
                    raise CAOMError('file {0}: '
                                    'Release date could not be '
                                    'calculated from membership: '.format(
                                        filename, self.observationID))
            else:
                # For "healpix" products (i.e. JSA legacy release) use a dummy
                # release date for now.
                if backend == 'SCUBA-2' and filter == '850':
                    legacy_release_date = '2016-04-01T00:00:00.000'
                elif backend == 'SCUBA-2' and filter == '450':
                    legacy_release_date = '2016-04-01T00:00:00.000'
                elif backend == 'ACSIS':
                    legacy_release_date = '2020-01-01T00:00:00.000'
                else:
                    raise CAOMError(
                        'Don\'t know release date for this "HEALPix" product')

                if algorithm != 'exposure':
                    # Don't set the Observation level release date for
                    # "exposures" because the raw data ingestion should
                    # do that.
                    self.add_to_plane_dict('obs.metaRelease', legacy_release_date)
                self.add_to_plane_dict('plane.metaRelease', legacy_release_date)
                self.add_to_plane_dict('plane.dataRelease', legacy_release_date)

        calibrationLevel = None
        # The calibration lelvel needs to be defined for all science products
        if is_main_product:
            if instream in self.external_collections:
                callevel_dict = \
                    {'calibrated': str(CalibrationLevel.CALIBRATED.value),
                     'product':    str(CalibrationLevel.PRODUCT.value)}
                self.validation.restricted_value(filename,
                                                 'CALLEVEL',
                                                 header,
                                                 sorted(callevel_dict))
                calibrationLevel = callevel_dict[header['CALLEVEL']]
            else:
                callevel_dict = \
                    {'cube':       str(CalibrationLevel.RAW_STANDARD.value),
                     'reduced':    str(CalibrationLevel.CALIBRATED.value),
                     'peak':       str(CalibrationLevel.PRODUCT.value),
                     'extent':     str(CalibrationLevel.PRODUCT.value),
                     }

                if science_product == 'healpix':
                    # We have "healpix" products both for individual
                    # observations and as co-adds -- only the latter should
                    # be level "PRODUCT".
                    if algorithm == 'public':
                        calibrationLevel = str(CalibrationLevel.PRODUCT.value)
                    else:
                        calibrationLevel = str(CalibrationLevel.CALIBRATED.value)
                elif science_product in callevel_dict:
                    calibrationLevel = callevel_dict[science_product]
                else:
                    raise CAOMError(
                        'file {0} '
                        'science product "{1}" is not in {2}!r'.format(
                            filename, science_product, (sorted(callevel_dict))))

            if calibrationLevel:
                self.add_to_plane_dict('plane.calibrationLevel',
                                       calibrationLevel)

        # Check for existence of provenance input headers, which are optional
        logger.info('Reading provenance')
        logger.debug('input_cache: %s',
                     ', '.join([str(k) + ': ' + repr(self.input_cache[k])
                                for k in sorted(self.input_cache.keys())]))

        if is_defined('INPCNT', header):
            planeURI_regex = r'^caom:([^\s/]+)/([^\s/]+)/([^\s/]+)$'
            # Copy the INP1..INP<PRVCNT> headers as plane URIs
            inpcnt = int(header['INPCNT'])
            if product and is_main_product and inpcnt > 0:
                for n in range(inpcnt):
                    inpkey = 'INP' + str(n + 1)
                    self.validation.expect_keyword(filename, inpkey, header)
                    inpn_str = header[inpkey]
                    logger.debug('%s = %s', inpkey, inpn_str)
                    pm = re.match(planeURI_regex, inpn_str)
                    if pm:
                        # inpn looks like a planeURI, so add it unconditionally
                        # here and check later that the plane exists
                        inpn = self.planeURI(pm.group(1),
                                             pm.group(2),
                                             pm.group(3))
                        self.inputset.add(inpn)
                    else:
                        raise CAOMError(
                            'file {0}: {1} = {2} does not '
                            'match the regex for a plane URI: {3}'.format(
                                filename, inpkey, inpn_str, planeURI_regex))

        elif is_defined('PRVCNT', header):
            # Translate the PRV1..PRV<PRVCNT> headers into plane URIs
            prvcnt = int(header['PRVCNT'])
            if product and is_main_product and prvcnt > 0:
                logger.info('PRVCNT = %s', prvcnt)
                for i in range(prvcnt):
                    # Verify that files in provenance are being ingested
                    # or have already been ingested.
                    prvkey = 'PRV' + str(i + 1)
                    self.validation.expect_keyword(filename, prvkey, header)
                    prvn = header[prvkey]
                    logger.debug('%s = %s', prvkey, prvn)

                    # jsawrapdr has left some "oractempXXXXXX" entries in the
                    # provenance headers.  While the correct thing to do is to
                    # correct jsawrapdr, there are still a lot of existing
                    # processed data which we need to be able to ingest
                    # efficiently.  Therefore skip over these files in
                    # the provenance.
                    if prvn.startswith('oractemp'):
                        logger.warning('provenance contains oractemp file')
                        continue

                    # An existing problem is that some files include
                    # themselves in their provenance, but are otherwise
                    # OK.
                    prvn_id = self.make_file_id(prvn)
                    if prvn_id == file_id:
                        # add a warning and skip this entry
                        logger.warning(
                            'file_id = %s includes itself '
                            'in its provenance as %s', file_id, prvkey)
                        continue
                    elif prvn_id in self.input_cache:
                        # The input cache should already have uri's for
                        # raw data
                        self.inputset.add(self.input_cache[prvn_id])
                    else:
                        # uri's for processed data are likely to be defined
                        # during this ingestion, but cannot be checked until
                        # metadata has been gathered from all the files.
                        # See checkProvenanceInputs.
                        self.fileset.add(prvn_id)

        dataProductType = None
        if is_defined('DATAPROD', header):
            self.validation.restricted_value(
                filename, 'DATAPROD', header,
                ('image', 'spectrum', 'cube', 'catalog'))
            dataProductType = header['DATAPROD']
        elif is_main_product:
            # Assume these are like standard pipeline products
            # Axes are always in the order X, Y, Freq, Pol
            # but may be degenerate with length 1.  Only compute the
            # dataProductType for science data.
            if product in ['reduced', 'cube', 'healpix']:
                if (header['NAXIS'] == 3 or
                        (header['NAXIS'] == 4 and header['NAXIS4'] == 1)):
                    if (header['NAXIS1'] == 1 and
                            header['NAXIS2'] == 1):
                        dataProductType = 'spectrum'
                    elif header['NAXIS3'] == 1:
                        dataProductType = 'image'
                    else:
                        dataProductType = 'cube'
            elif product in ('tile-moc', 'peak-cat'):
                dataProductType = 'catalog'
        if dataProductType:
            self.add_to_plane_dict('plane.dataProductType', dataProductType)

        # Provenance_name
        self.validation.expect_keyword(filename, 'RECIPE', header)
        self.add_to_plane_dict('provenance.name', header['RECIPE'])

        # Provenance_project
        dpproject = None
        if is_defined('DPPROJ', header):
            dpproject = header['DPPROJ'].strip()
        elif instream == 'JCMTLS' and proposal_project:
            dpproject = proposal_project
        elif instream == 'JCMT':
            standard_products = ['reduced', 'cube', 'rsp', 'rimg']
            legacy_products = ['healpix', 'hpxrsp', 'hpxrimg',
                               'peak-cat', 'extent-cat', 'extent-mask',
                               'extent-moc', 'tile-moc']
            if product in standard_products:
                # This is the complete list of standard pipeline FITS products
                dpproject = 'JCMT_STANDARD_PIPELINE'
            elif product in legacy_products:
                # healpix and catalogs are from the legacy project
                dpproject = 'JCMT_LEGACY_PIPELINE'
            else:
                raise CAOMError(
                    'file {0}: UNKNOWN PRODUCT in collection=JCMT: {1}'
                    ' must be one of {2!r}'.format(
                        filename, product,
                        (standard_products + legacy_products)))

        if dpproject:
            self.add_to_plane_dict('provenance.project', dpproject)
        else:
            raise CAOMError('file {0}: data processing project '
                            'is undefined'.format(filename))

        # Provenance_reference - likely to be overwritten
        if is_defined('REFERENC', header):
            self.add_to_plane_dict('provenance.reference',
                                   header['REFERENC'])

        # ENGVERS, PIPEVERS and PROCVERS are optional
        if (is_defined('ENGVERS', header) and
                is_defined('PIPEVERS', header)):
            self.add_to_plane_dict('provenance.version',
                                   'ENGINE:' + header['ENGVERS'][:20] +
                                       ' PIPELINE:' + header['PIPEVERS'][:20])
        elif is_defined('PROCVERS', header):
            self.add_to_plane_dict('provenance.version',
                                   header['PROCVERS'])

        if is_defined('PRODUCER', header):
            self.add_to_plane_dict('provenance.producer',
                                   header['PRODUCER'])

        self.dprcinst = None
        self.validation.expect_keyword(filename, 'DPRCINST', header)
        if isinstance(header['DPRCINST'], str):
            m = re.match(r'jac-([1-9][0-9]*)', header['DPRCINST'])
            if m:
                # dprcinst is a JAC recipe instance
                self.dprcinst = 'jac-%09d' % (eval(m.group(1)),)

            elif re.match(r'^0x[0-9a-fA-F]+$', header['DPRCINST']):
                # dprcinst is an old-style hex recipe_instance_id
                self.dprcinst = str(eval(header['DPRCINST']))
            else:
                # dprcinst is an arbitrary string; use without modification
                self.dprcinst = header['DPRCINST']
        else:
            # dprcisnt is an identity_instance_id integer; convert to
            # string
            self.dprcinst = str(header['DPRCINST'])

        if self.dprcinst:
            self.add_to_plane_dict('provenance.runID', self.dprcinst)
            self.build_remove_dict(self.dprcinst)
        else:
            raise CAOMError('could not calculate dprcinst')

        # Report the earliest UTDATE
        if earliest_utdate and self.dprcinst:
            logger.info(
                'Earliest utdate: %s for %s',
                Time(earliest_utdate, format='mjd', out_subfmt='date').iso,
                self.dprcinst)

        self.validation.expect_keyword(filename, 'DPDATE', header)
        # DPDATE is a characteristic datetime when the data was processed
        dpdate = header['DPDATE']
        if isinstance(dpdate, datetime.datetime):
            dpdate = header['DPDATE'].isoformat()
        self.add_to_plane_dict('provenance.lastExecuted', dpdate)

        # Chunk
        bandpassName = None
        if backend == 'SCUBA-2' and filter:
            bandpassName = 'SCUBA-2-' + filter + 'um'
            self.add_to_plane_dict('bandpassName', bandpassName)
        elif backend in ('ACSIS', 'DAS', 'AOSC'):
            if (is_defined('MOLECULE', header) and
                    is_defined('TRANSITI', header) and
                    header['MOLECULE'] != 'No Line'):
                self.add_to_plane_dict('energy.transition.species',
                                       header['MOLECULE'])
                self.add_to_plane_dict('energy.transition.transition',
                                       header['TRANSITI'])

        self.uri = self.fitsfileURI(self.archive, file_id)
        # Recall that the order self.add_fitsuri_dict is called is preserved
        # in the override file

        # Translate the PRODTYPE header into a list of (extension_number, type)
        # pairs, where the default with extension_number = None is always last
        prodtype = 'auxiliary'
        if is_defined('PRODTYPE', header):
            prodtype = header['PRODTYPE'].lower()
        elif product in jcmt2caom2ingest.productType:
            prodtype = jcmt2caom2ingest.productType[product]

        prodtype = re.sub(r'\s', '', prodtype)
        if ',' in prodtype:
            prodtype = re.sub(r',{2,}', ',', prodtype)
            prodtype_list = prodtype.split(',')
        else:
            prodtype_list = [prodtype]

        prodtype_default = None
        prodtypes = []
        prodtype_options = (r'(science|calibration|preview|' +
                            r'info|catalog|noise|weight|auxiliary)')
        for pt in prodtype_list:
            mpt = re.match(r'(\d+)=' + prodtype_options,
                           pt)
            if mpt:
                prodtypes.append((mpt.group(1), mpt.group(2)))
            else:
                if re.match(prodtype_options, pt):
                    prodtype_default = pt

        prodtypes = sorted(prodtypes, key=lambda t: t[0])
        if len(prodtypes):
            for (ext, pt) in prodtypes:
                extURI = self.fitsextensionURI(self.archive,
                                               file_id,
                                               [int(ext)])
                self.add_fitsuri_dict(extURI)
                self.add_to_fitsuri_dict(extURI,
                                         'part.productType',
                                         pt)
            if prodtype_default:
                self.add_fitsuri_dict(self.uri)
                self.add_to_fitsuri_dict(self.uri,
                                         'part.productType',
                                         prodtype_default)
        elif prodtype_default:
            self.add_fitsuri_dict(self.uri)
            self.add_to_fitsuri_dict(self.uri,
                                     'artifact.productType',
                                     prodtype_default)
        else:
            raise CAOMError(
                'file {0}: ProductType is not defined'.format(filename))

        if is_main_product and len(obstimes):
            self.add_fitsuri_dict(self.uri)
            # Record times for science products
            for key in sorted(obstimes, key=lambda t: obstimes[t][0]):
                self.add_to_fitsuri_custom_dict(self.uri,
                                                key,
                                                obstimes[key])

        # If this is a catalog file, generate explicit WCS information as
        # fits2caom2 may not be able to do it.  For now assume that all
        # tiles are of SCUBA-2 data.
        if is_catalog and self.uri not in self.explicit_wcs:
            self.explicit_wcs[self.uri] = {
                'spatial': jsa_tile_wcs(header),
                'spectral': scuba2_spectral_wcs(header),
            }

        # Temporary workaround for HEALPix co-adds for which
        # the CAOM-2 repository rejects the WCS information
        # written by fits2caom2, while awaiting a response to our
        # inquiries to CADC about this problem.
        is_healpix_850 = (is_defined('PRODID', header) and
            header['PRODID'] == 'healpix-850um')
        if (is_healpix_850 and (algorithm == 'public') and
                (self.uri not in self.explicit_wcs) and (header['TILENUM'] in [
                    3054,
                    3055,
                    3066,
                    3067,
                    3755,
                    14301,
                    14303,
                    14325,
                    14327,
                    15703,
                ])):
            self.explicit_wcs[self.uri] = {
                'spatial': jsa_tile_wcs(header),
            }
        # Also temporarily work around problems for HEALPix obs products.
        if (is_healpix_850 and (algorithm == 'exposure') and
                (self.uri not in self.explicit_wcs) and (header['OBSID'] in [
                    'scuba2_00013_20121214T051903',
                    'scuba2_00018_20121214T061802',
                    'scuba2_00019_20130621T144346',
                    'scuba2_00021_20120224T065458',  # IndexOutOfBoundsException
                    'scuba2_00021_20121214T071005',
                    'scuba2_00022_20130714T075458',  # IndexOutOfBoundsException
                    'scuba2_00024_20130112T072839',  # IndexOutofBoundsException
                    'scuba2_00024_20130420T134525',  # IndexOutOfBoundsException
                    'scuba2_00025_20130420T135344',
                    'scuba2_00026_20120221T101229',  # IndexOutOfBoundsException
                    'scuba2_00026_20130420T140153',  # IndexOutOfBoundsException
                    'scuba2_00027_20130420T140955',  # IndexOutOfBoundsException
                    'scuba2_00027_20130619T170927',
                    'scuba2_00028_20120821T091459',
                    'scuba2_00029_20120306T074028',  # IndexOutOfBoundsException
                    'scuba2_00031_20130103T080345',  # IndexOutOfBoundsException
                    'scuba2_00032_20120306T083508',  # IndexOutOfBoundsException
                    'scuba2_00035_20120123T102512',
                    'scuba2_00035_20120819T101300',
                    'scuba2_00037_20120821T103044',
                    'scuba2_00038_20120819T110220',
                    'scuba2_00039_20120322T081516',  # IndexOutOfBoundsException
                    'scuba2_00040_20120821T111756',
                    'scuba2_00041_20120707T130932',  # IndexOutOfBoundsException
                    'scuba2_00046_20120816T110937',
                    'scuba2_00047_20130520T160621',
                    'scuba2_00047_20130702T132200',  # IndexOutOfBoundsException
                    'scuba2_00049_20120829T140642',
                    'scuba2_00051_20120910T131905',
                    'scuba2_00051_20130601T151439',  # IndexOutOfBoundsException
                    'scuba2_00058_20130105T165836',  # getConvexHull: not implemented
                    'scuba2_00061_20120831T115647',
                    'scuba2_00069_20120831T134429',
                    'scuba2_00072_20120831T143630',
                    'scuba2_00016_20140120T063740',
                    'scuba2_00044_20140404T145801',
                    'scuba2_00056_20140404T164922',
                    'scuba2_00057_20140404T173424',
                    'scuba2_00059_20140404T182159',
                    'scuba2_00053_20140405T152903',
                    'scuba2_00062_20140405T171638',
                    'scuba2_00063_20140405T175819',
                    'scuba2_00033_20140406T121051',
                    'scuba2_00058_20140406T173801',
                    'scuba2_00083_20140408T174053',
                    'scuba2_00084_20140408T182202',
                    'scuba2_00069_20140409T172712',
                    'scuba2_00070_20140409T180824',
                    'scuba2_00073_20140410T170938',
                    'scuba2_00074_20140410T175148',
                    'scuba2_00064_20140415T165609',
                    'scuba2_00065_20140415T173746',
                    'scuba2_00069_20140415T183054',
                    'scuba2_00043_20140416T115821',
                    'scuba2_00044_20140416T123939',
                    'scuba2_00055_20140416T152412',
                    'scuba2_00056_20140416T160535',
                    'scuba2_00049_20140417T110036',
                    'scuba2_00069_20140417T164930',
                    'scuba2_00070_20140417T173058',
                    'scuba2_00076_20140417T184743',
                    'scuba2_00077_20140417T185315',
                    'scuba2_00035_20140511T112018',
                    'scuba2_00052_20140511T145911',
                    'scuba2_00073_20140511T184617',
                    'scuba2_00041_20140514T100741',
                    'scuba2_00045_20140514T110305',
                    'scuba2_00048_20140514T115528',
                    'scuba2_00058_20140514T142208',
                    'scuba2_00062_20140514T151730',
                    'scuba2_00044_20140516T124255',
                    'scuba2_00046_20140516T132913',
                    'scuba2_00049_20140516T142012',
                    'scuba2_00053_20140516T151456',
                    'scuba2_00047_20140517T101653',
                    'scuba2_00049_20140517T110311',
                    'scuba2_00054_20140517T115529',
                    'scuba2_00056_20140517T124138',
                    'scuba2_00059_20140517T133157',
                    'scuba2_00061_20140517T141819',
                    'scuba2_00068_20140517T152207',
                    'scuba2_00033_20140522T094555',
                    'scuba2_00044_20140529T131431',
                    'scuba2_00047_20140529T140219',
                    'scuba2_00053_20140529T150312',
                    'scuba2_00058_20140530T105813',
                    'scuba2_00060_20140530T114506',
                    'scuba2_00063_20140530T123640',
                    'scuba2_00065_20140530T132346',
                    'scuba2_00070_20140530T142337',
                    'scuba2_00030_20140531T092235',
                    'scuba2_00033_20140531T101531',
                    'scuba2_00038_20140531T111533',
                    'scuba2_00041_20140531T120402',
                    'scuba2_00045_20140531T130013',
                    'scuba2_00047_20140531T134748',
                    'scuba2_00052_20140531T144803',
                    'scuba2_00028_20140601T084348',
                    'scuba2_00030_20140601T092950',
                    'scuba2_00035_20140601T102920',
                    'scuba2_00036_20140601T111039',
                    'scuba2_00039_20140601T120220',
                    'scuba2_00021_20140606T075039',
                    'scuba2_00026_20140606T084536',
                    'scuba2_00033_20140606T094656',
                    'scuba2_00035_20140606T103240',
                    'scuba2_00041_20140606T113149',
                    'scuba2_00043_20140606T121737',
                    'scuba2_00046_20140606T130852',
                    'scuba2_00049_20140606T135523',
                    'scuba2_00052_20140606T144738',
                    'scuba2_00027_20140611T124811',
                    'scuba2_00023_20140614T074136',
                    'scuba2_00026_20140614T082759',
                    'scuba2_00029_20140614T091904',
                    'scuba2_00033_20140614T101234',
                    'scuba2_00037_20140614T110354',
                    'scuba2_00046_20140614T141021',
                    'scuba2_00020_20140615T075911',
                    'scuba2_00024_20140615T085118',
                    'scuba2_00022_20140618T073619',
                    'scuba2_00024_20140618T082312',
                    'scuba2_00027_20140618T091519',
                    'scuba2_00031_20140618T101121',
                    'scuba2_00043_20140618T133724',
                    'scuba2_00039_20140619T122434',
                    'scuba2_00041_20140619T131355',
                    'scuba2_00023_20140702T070607',
                    'scuba2_00017_20140703T063148',
                    'scuba2_00019_20140703T071828',
                    'scuba2_00023_20140703T081208',
                    'scuba2_00028_20140703T091141',
                    'scuba2_00030_20140703T095838',
                    'scuba2_00034_20140703T105607',
                    'scuba2_00015_20140704T063344',
                    'scuba2_00019_20140704T072834',
                    'scuba2_00022_20140704T081916',
                    'scuba2_00029_20140704T092224',
                    'scuba2_00031_20140704T100757',
                    'scuba2_00036_20140704T110417',
                    'scuba2_00038_20140704T114956',
                    'scuba2_00041_20140704T123745',
                    'scuba2_00015_20140705T055559',
                    'scuba2_00019_20140705T065617',
                    'scuba2_00022_20140705T074520',
                    'scuba2_00028_20140705T084718',
                    'scuba2_00030_20140705T093505',
                    'scuba2_00034_20140705T103104',
                    'scuba2_00036_20140705T111648',
                    'scuba2_00039_20140705T120801',
                    'scuba2_00019_20140706T074849',
                    'scuba2_00021_20140706T083426',
                    'scuba2_00032_20140706T111320',
                    'scuba2_00034_20140706T115908',
                    'scuba2_00020_20140726T051744',
                    'scuba2_00014_20140727T053000',
                    'scuba2_00015_20140727T061106',
                    'scuba2_00028_20140727T084036',
                    'scuba2_00029_20140727T092147',
                    'scuba2_00033_20140727T101521',
                    'scuba2_00034_20140727T105635',
                    'scuba2_00012_20140728T051427',
                    'scuba2_00013_20140728T055536',
                    'scuba2_00018_20140728T065209',
                    'scuba2_00029_20140728T091613',
                    'scuba2_00030_20140728T095724',
                    'scuba2_00035_20140728T105147',
                    'scuba2_00011_20140729T051631',
                    'scuba2_00012_20140729T055737',
                    'scuba2_00016_20140729T065042',
                    'scuba2_00017_20140729T073151',
                    'scuba2_00023_20140729T083357',
                    'scuba2_00024_20140729T091511',
                    'scuba2_00028_20140729T100833',
                    'scuba2_00029_20140729T104947',
                    'scuba2_00011_20140730T050309',
                    'scuba2_00012_20140730T054444',
                    'scuba2_00016_20140730T063810',
                    'scuba2_00017_20140730T071947',
                    'scuba2_00018_20140805T064356',
                    'scuba2_00022_20140805T074104',
                    'scuba2_00026_20140806T051514',
                    'scuba2_00029_20140806T060749',
                    'scuba2_00034_20140806T070750',
                    'scuba2_00037_20140806T075837',
                    'scuba2_00042_20140806T085900',
                    'scuba2_00045_20140806T094807',
                    'scuba2_00047_20140806T103658',
                    'scuba2_00018_20140815T061218',
                    'scuba2_00019_20140815T065330',
                    'scuba2_00023_20140815T074657',
                    'scuba2_00024_20140815T082808',
                    'scuba2_00029_20140815T092624',
                    'scuba2_00012_20140816T050447',
                    'scuba2_00014_20140816T054650',
                    'scuba2_00019_20140816T064027',
                    'scuba2_00020_20140816T072132',
                    'scuba2_00025_20140816T081527',
                    'scuba2_00026_20140816T085634',
                    'scuba2_00031_20140816T095427',
                    'scuba2_00013_20140817T064422',
                    'scuba2_00014_20140817T072534',
                    'scuba2_00019_20140817T082144',
                    'scuba2_00020_20140817T090258',
                    'scuba2_00024_20140817T095401',
                    'scuba2_00011_20140818T050402',
                    'scuba2_00012_20140818T054509',
                    'scuba2_00017_20140818T064202',
                    'scuba2_00018_20140818T072310',
                    'scuba2_00022_20140818T081631',
                    'scuba2_00023_20140818T085741',
                    'scuba2_00027_20140818T095110',
                    'scuba2_00011_20140819T045904',
                    'scuba2_00012_20140819T054009',
                    'scuba2_00018_20140819T064155',
                    'scuba2_00019_20140819T072307',
                    'scuba2_00023_20140819T081620',
                    'scuba2_00024_20140819T085729',
                    'scuba2_00028_20140819T094804',
                    'scuba2_00037_20140820T060030',
                    'scuba2_00050_20140820T085012',
                    'scuba2_00051_20140820T093128',
                    'scuba2_00029_20140821T055404',
                    'scuba2_00040_20140821T084705',
                    'scuba2_00018_20140822T053844',
                    'scuba2_00019_20140822T061952',
                    'scuba2_00026_20140822T072258',
                    'scuba2_00027_20140822T080408',
                    'scuba2_00030_20140822T085719',
                    'scuba2_00041_20140824T091240',
                    'scuba2_00029_20140826T085730',
                    'scuba2_00013_20140829T045128',
                    'scuba2_00014_20140829T053242',
                    'scuba2_00019_20140829T063247',
                    'scuba2_00021_20140829T071821',
                    'scuba2_00026_20140829T081717',
                    'scuba2_00027_20140829T085824',
                    'scuba2_00010_20140830T052704',
                    'scuba2_00014_20140830T062230',
                    'scuba2_00020_20140830T072507',
                    'scuba2_00022_20140830T081141',
                    'scuba2_00026_20140830T090706',
                    'scuba2_00010_20140902T050145',
                    'scuba2_00011_20140902T054250',
                    'scuba2_00016_20140902T064015',
                    'scuba2_00017_20140902T072123',
                    'scuba2_00023_20140902T081749',
                    'scuba2_00011_20140903T045616',
                    'scuba2_00013_20140903T053800',
                    'scuba2_00017_20140903T063226',
                    'scuba2_00018_20140903T071332',
                    'scuba2_00023_20140903T081051',
                    'scuba2_00020_20140904T060040',
                    'scuba2_00012_20140906T051301',
                    'scuba2_00013_20140906T055447',
                    'scuba2_00019_20140906T070054',
                    'scuba2_00020_20140906T074223',
                    'scuba2_00024_20140906T083726',
                    'scuba2_00018_20140908T061845',
                    'scuba2_00022_20140908T071357',
                    'scuba2_00025_20140908T080706',
                    'scuba2_00017_20140918T045304',
                    'scuba2_00018_20140918T053406',
                    'scuba2_00022_20140918T062746',
                    'scuba2_00023_20140918T070856',
                    'scuba2_00011_20140922T051230',
                    'scuba2_00014_20140922T060418',
                    'scuba2_00021_20141007T061904',
                    'scuba2_00019_20141013T060515',
                    'scuba2_00016_20141024T050057',
                    'scuba2_00010_20141026T051019',
                    'scuba2_00013_20141027T045729',
                    'scuba2_00011_20141028T044951',
                    'scuba2_00010_20141029T044557',
                    'scuba2_00012_20141030T050054',
                    'scuba2_00014_20141031T044815',
                    'scuba2_00079_20141122T205449',
                    'scuba2_00080_20141122T213658',
                    'scuba2_00083_20141127T203456',
                    'scuba2_00075_20141130T203525',
                    'scuba2_00041_20141206T195628',
                    'scuba2_00090_20141207T200211',
                    'scuba2_00068_20141213T190351',
                    'scuba2_00069_20141213T194517',
                    'scuba2_00076_20141219T183925',
                    'scuba2_00077_20141219T192051',
                    'scuba2_00083_20141219T203731',
                    'scuba2_00087_20141219T213631',
                    'scuba2_00061_20150110T182808',
                    'scuba2_00062_20150110T190914',
                    'scuba2_00068_20150110T201751',
                    'scuba2_00071_20150112T190125',
                    'scuba2_00064_20150117T165852',
                    'scuba2_00057_20150120T163804',
                    'scuba2_00075_20150122T165722',
                    'scuba2_00069_20150123T163823',
                    'scuba2_00072_20150123T172500',
                    'scuba2_00077_20150123T182018',
                    'scuba2_00083_20150123T192106',
                    'scuba2_00089_20150123T202205',
                    'scuba2_00057_20150124T164918',
                    'scuba2_00078_20150124T202717',
                    'scuba2_00084_20150124T213022',
                    'scuba2_00051_20150126T190511',
                    'scuba2_00056_20150126T200545',
                ])):
            self.explicit_wcs[self.uri] = {
                'spatial': jsa_tile_wcs(header),
            }

    def lookup_file_id(self, filename, file_id):
        """
        Given a file_id (and unnecessarily filename), return the URI
        from either the current ingestion or existing observation in the
        archive.  Cache the results from TAP queries for future reference.
        """
        inputURI = None
        if file_id in self.input_cache:
            inputURI = self.input_cache[file_id]
        else:
            # use TAP to find the collection, observation and plane
            # for all files in the observation containing file_id
            for row in self.tap.get_artifacts_for_plane_with_artifact_uri(
                    self.fitsfileURI(self.archive, file_id)):

                # Search for 'ad:<anything that isn't a slash>/'
                # and replace with nothing with in row.artifact_uri
                fid = re.sub(r'ad:[^/]+/', '', row.artifact_uri)

                if (row.collection in (self.collection,
                                       'JCMT',
                                       'JCMTLS',
                                       'JCMTUSER')):

                    # URI for this plane
                    thisInputURI = self.planeURI(row.collection, row.obs_id,
                                                 row.prod_id)

                    # If the ad URI is the same as file_id, set
                    # inputURI to thisInputURI
                    if fid == file_id:
                        inputURI = thisInputURI

                    # add to cache
                    self.input_cache[fid] = thisInputURI

                    logger.debug('inputs: %s: %s', fid, thisInputURI.uri)

        if inputURI is None:
            logger.warning(
                'file %s: provenance input is neither '
                'in the JSA already nor in the '
                'current release',
                filename)

        return inputURI

    def checkProvenanceInputs(self):
        """
        From the set of provenance input planeURIs or input files,
        build the list of provenance input URIs for each output plane,
        caching results to save time in the TAP queries.
        """
        for obs in self.metadict:
            for prod in self.metadict[obs]:
                if prod != 'memberset':
                    thisPlane = self.metadict[obs][prod]
                    planeURI = self.planeURI(self.collection, obs, prod)

                    for filename in thisPlane['fileset']:
                        file_id = self.make_file_id(filename)
                        inputURI = self.lookup_file_id(filename,
                                                       file_id)
                        if (inputURI and
                                inputURI.uri not in thisPlane['inputset']):

                            thisPlane['inputset'].add(inputURI)
                            logger.info('add %s to inputset for %s',
                                        inputURI.uri, planeURI.uri)

    def update_time_information(self,
                                observation,
                                observationID,
                                planeID,
                                fitsuri):
        """
        Customize the CAOM-2 observation with fitsuri-specific metadata.  For
        jsaingest, this comprises the time structure constructed from the
        OBSID for simple observations or list of OBSn values for composite
        observations.
        """
        thisCustom = self.metadict[observationID][planeID][fitsuri]['custom']
        if thisCustom:
            # if this dictionary is empty,skip processing
            logger.debug('custom processing for %s', fitsuri)

            # Check whether this is a part-specific uri.  We are only
            # interested in artifact-specific uri's.
            if fitsuri not in observation.planes[planeID].artifacts:
                logger.debug('skip custom processing because fitsuri does '
                             'not point to an artifact')
                return
            if (observation.algorithm != SimpleObservation._ALGORITHM and
                    len(observation.members) > 1):
                # single exposure products have DATE-OBS and DATE-END,
                # and are handled correctly by fits2caom2
                caomArtifact = observation.planes[planeID].artifacts[fitsuri]

                for part in caomArtifact.parts:
                    thisPart = caomArtifact.parts[part]
                    if thisPart.product_type in [ProductType.SCIENCE,
                                                 ProductType.NOISE]:
                        for chunk in thisPart.chunks:
                            if chunk.position:
                                # if the position WCS exists, add a time axis
                                time_axis = CoordAxis1D(Axis('TIME', 'd'))

                                if len(thisCustom):
                                    # time
                                    time_axis.bounds = CoordBounds1D()
                                    for key in thisCustom:
                                        date_start, date_end = thisCustom[key]
                                        # These are already MJDs
                                        # mjdstart = utc2mjd(date_start)
                                        # mjdend = utc2mjd(date_end)
                                        logger.debug('time bounds = %f, %f',
                                                     date_start, date_end)

                                        time_axis.bounds.samples.append(
                                            CoordRange1D(
                                                RefCoord(0.5, date_start),
                                                RefCoord(1.5, date_end)))

                                else:
                                    logger.warning(
                                        'no time ranges defined  for %s',
                                        fitsuri.uri)

                                # if a temporalWCS already exists, use it but
                                # replace the CoordAxis1D
                                if chunk.time:
                                    chunk.time.axis = time_axis
                                    chunk.time.timesys = 'UTC'
                                else:
                                    chunk.time = TemporalWCS(time_axis)
                                    chunk.time.timesys = 'UTC'
                                logger.debug('temporal axis = %r',
                                             chunk.time.axis.axis)
                                logger.debug('temporal WCS = %s',
                                             chunk.time)


    def set_explicit_wcs(self, observation, planeID):
        """
        Customize the CAOM-2 observation with explicit WCS values if any
        have been stored for its artifacts.
        """

        plane = observation.planes[planeID]

        for fitsuri in plane.artifacts:
            if fitsuri not in self.explicit_wcs:
                continue

            wcs = self.explicit_wcs[fitsuri]
            artifact = plane.artifacts[fitsuri]

            for partName in artifact.parts:
                part = artifact.parts[partName]

                if (not (part.product_type in [ProductType.SCIENCE,
                                               ProductType.NOISE]
                         or ('_extent-mask' in fitsuri and partName == '0'))):
                    continue

                if (len(part.chunks)) == 0:
                    chunk = Chunk()
                    part.chunks.append(chunk)

                elif len(part.chunks) != 1:
                    raise CAOMError(
                        'More than one chunk in explicit WCS part: %i',
                        len(part.chunks))
                else:
                    chunk = part.chunks[0]

                if 'spatial' in wcs:
                    chunk.position = wcs['spatial']

                if 'spectral' in wcs:
                    chunk.energy = wcs['spectral']


    def remove_old_planes(self,
                          observation,
                          observationID):
        """
        Implement the cleanup of planes in this observations
        that are no longer generated by this
        recipe instance from observations that are.  It is only necessary to
        remove planes from the current observation that are not already being
        replaced by the new set of products.

        Arguments:
        observation: CAOM-2 observation object to be updated
        observationID: current observationID
        """

        if observationID in self.remove_dict:
            for prod in observation.planes.keys():
                # logic is, this collection/observation/plane used to be
                # genrated by this recipe instance, but is not part of the
                # current ingestion and so is obsolete.
                if prod in self.remove_dict[observationID] and \
                        prod not in self.metadict[observationID]:

                    logger.warning(
                        'removing obsolete plane: %s',
                        self.planeURI(self.collection, observationID,
                                      prod).uri)

                    del observation.planes[prod]

            del self.remove_dict[observationID]

    def remove_old_observations_and_planes(self):
        """
        Implement the cleanup of observations and planes that are
        no longer generated by this recipe instance.  It is only necessary to
        remove items that are not already being replaced by the new set of
        products.  At this level, remove all observations and
        planes from observations that are not generated by the current recipe
        instance.
        """
        # log the contents of remove_dict
        for obsid in self.remove_dict:
            for prodid in self.remove_dict[obsid]:
                logger.info('remove_dict %s: %s', obsid, prodid)

        # Iterate over separate list of keys so that (in Python 3) it will
        # be safe to delete from the dictionary inside the loop.
        for obsid in list(self.remove_dict.keys()):
            # This method should not be processing observations which are
            # part of this ingestion.
            if obsid in self.metadict:
                logger.error(
                    'current ingestion observation "%s" still in remove_dict',
                    obsid)
                continue

            uri = self.observationURI(self.collection, obsid)

            # Process the observation, with the "allow_remove" flag
            # enabled in case we are removing everything from it.
            with self.repository.process(uri, allow_remove=True,
                                         dry_run=self.dry_run) as wrapper:
                if wrapper.observation is not None:
                    obs = wrapper.observation

                    for prod in self.remove_dict[obsid]:
                        if prod in obs.planes:
                            logger.warning(
                                'removing old plane: %s',
                                self.planeURI(self.collection, obsid, prod).uri)

                            del obs.planes[prod]

            del self.remove_dict[obsid]

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
                    if transfer_dir and not self.dry_run:
                        self.vosclient.link(filepath,
                                            transfer_dir + '/' + basefile)

        elif self.storemethod == 'push':
            for filelist in (self.data_storage, self.preview_storage):
                for filepath in filelist:
                    basefile = os.path.basename(filepath)
                    file_id = self.make_file_id(basefile)
                    logger.info('PUT: %s', filepath)
                    if not self.dry_run:
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
                                raise CAOMError(
                                    'failed to push {0} into AD using the '
                                    'data_web_client'.format(filepath))
                        finally:
                            if not self.local and os.path.exists(tempfile):
                                os.remove(tempfile)
        else:
            raise CAOMError('storemethod = ' + self.storemethod +
                            'has not been implemented')

    def prepare_override_info(self, observationID, productID):
        """
        Prepare the information required in override files for a plane specified
        by the collection, observationID and productID.

        Arguments:
        observationID : the observationID containing productID
        productID : productID for this plane

        Returns:
        A tuple (general, section) containing the global and URI-specific
        parts of the override information.
        """
        thisObservation = self.metadict[observationID]
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

    def ingestPlanesFromMetadict(self):
        """
        Generic routine to ingest the planes in metadict, keeping track of
        members and inputs.

        Arguments:
        <none>
        """

        for observationID in self.metadict:
            thisObservation = self.metadict[observationID]

            obsuri = self.observationURI(self.collection,
                                         observationID)

            with self.repository.process(obsuri, dry_run=self.dry_run) as wrapper:
                if wrapper.observation is not None:
                    self.remove_excess_parts(wrapper.observation)

                for productID in thisObservation:
                    if productID != 'memberset':
                        thisPlane = thisObservation[productID]

                        logger.info('PROGRESS ingesting collection="%s"  '
                                    'observationID="%s" productID="%s"',
                                    self.collection, observationID, productID)

                        self.replace_members(thisObservation,
                                             thisPlane)

                        self.replace_inputs(thisObservation,
                                            thisPlane)

                        override = self.prepare_override_info(
                            observationID, productID)

                        # Run fits2caom2
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
                                self.collection, observationID, productID)
                            raise CAOMError('Nothing to ingest')

                        arg = thisPlane.get('fits2caom2_arg', None)

                        wrapper.observation = run_fits2caom2(
                            collection=self.collection,
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
                            verbose=self.verbose,
                            retain=False,
                            big=self.big,
                            dry_run=False)
                        logger.info(
                            'INGESTED: observationID=%s productID="%s"',
                            observationID, productID)

                        for fitsuri in thisPlane:
                            if fitsuri not in ('plane_dict',
                                               'uri_dict',
                                               'inputset',
                                               'fileset'):

                                self.update_time_information(
                                    wrapper.observation,
                                    observationID, productID, fitsuri)

                        self.set_explicit_wcs(wrapper.observation, productID)

                logger.info('Removing old planes from this observation')
                self.remove_old_planes(wrapper.observation,
                                       observationID)

                if self.xmloutdir:
                    with open(os.path.join(self.xmloutdir, re.sub(
                            '[^-_A-Za-z0-9]', '_', observationID)) + '.xml',
                            'wb') as f:
                        self.repository.writer.write(wrapper.observation, f)

            logger.info('SUCCESS observationID="%s"', observationID)

        logger.info('Removing old observations and planes')
        self.remove_old_observations_and_planes()

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


    def run(self):
        """Perform ingestion.

        Returns True on success, False otherwise.
        """

        try:
            self.conn = ArcDB()

            # metadict is the fundamental structure in the program, sorting
            # files by observation, plane and file, and holding all the relevant
            # metadata in a set of nested dictionaries.
            self.ap = argparse.ArgumentParser(self.progname)
            self.defineCommandLineSwitches()

            self.args = self.ap.parse_args()
            self.processCommandLineSwitches()

            self.logCommandLineSwitches()

            # Read list of files from VOspace and do things
            self.data_web = data_web_client(self.workdir)

            # Construct validation object
            self.validation = CAOMValidation(self.workdir,
                                             self.archive,
                                             self.fileid_regex_dict,
                                             self.make_file_id)

            self.commandLineContainers()
            for c in self.containerlist:
                logger.info('PROGRESS: container = %s', c.name)
                self.fillMetadict(c)
                self.checkProvenanceInputs()
                if self.store:
                    self.storeFiles()
                if self.ingest:
                    self.ingestPlanesFromMetadict()

            # declare we are DONE
            logger.info('DONE')

        except CAOMError as e:
            logger.exception(str(e))
            return False

        except Exception:
            # Log this previously uncaught error, but let it pass
            logger.exception('Error during ingestion')
            return False

        finally:
            self.conn.close()

        return True
