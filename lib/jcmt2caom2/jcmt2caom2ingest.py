# Copyright (C) 2014-2015 Science and Technology Facilities Council.
# Copyright (C) 2015-2018 East Asian Observatory.
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

__author__ = "Russell O. Redman"

import argparse
from collections import defaultdict, namedtuple, OrderedDict
from contextlib import closing
import datetime
import logging
import os
import re
import shutil
import subprocess
import sys

from astropy.io import fits
from astropy.time import Time
from pymoc import MOC
from pymoc.io.fits import read_moc_fits_hdu

from omp.db.part.arc import ArcDB

from caom2.artifact import Artifact
from caom2.chunk import Chunk
from caom2.common import ChecksumURI
from caom2.observation import CompositeObservation
from caom2.plane import CalibrationLevel
from caom2.observation import ObservationIntentType
from caom2.chunk import ProductType
from caom2.artifact import ReleaseType
from caom2.common import ObservationURI
from caom2.plane import PlaneURI
from caom2.observation import SimpleObservation
from caom2.wcs import Axis
from caom2.wcs import CoordAxis1D
from caom2.wcs import CoordBounds1D
from caom2.wcs import CoordRange1D
from caom2.wcs import RefCoord
from caom2.chunk import TemporalWCS

from tools4caom2.__version__ import version as tools4caom2version
from tools4caom2.artifact_uri import extract_artifact_uri_filename, \
    make_artifact_uri
from tools4caom2.caom2repo_wrapper import Repository
from tools4caom2.error import CAOMError
from tools4caom2.fits2caom2 import run_fits2caom2
from tools4caom2.mjd import utc2mjd
from tools4caom2.validation import CAOMValidation, CAOMValidationError

from jcmt2caom2.__version__ import version as jcmt2caom2version
from jcmt2caom2.caom2_tap import CAOM2TAP
from jcmt2caom2.instrument.scuba2 import scuba2_spectral_wcs
from jcmt2caom2.jsa.file_id import make_file_id_jcmt
from jcmt2caom2.jsa.instrument_keywords import instrument_keywords
from jcmt2caom2.jsa.instrument_name import instrument_name
from jcmt2caom2.jsa.intent import intent
from jcmt2caom2.jsa.obsid import obsidss_to_obsid
from jcmt2caom2.jsa.product_id import product_id
from jcmt2caom2.jsa.target_name import target_name
from jcmt2caom2.jsa.tile import jsa_tile_wcs
from jcmt2caom2.md5sum import get_md5sum
from jcmt2caom2.mime import determine_mime_type
from jcmt2caom2.png_keywords import read_png_keywords
from jcmt2caom2.project import get_project_pi_title, truncate_string
from jcmt2caom2.type import OrderedDefaultDict, OrderedStrDict

logger = logging.getLogger(__name__)


FileInfo = namedtuple(
    'FileInfo',
    ('observationID', 'productID', 'uri',
     'plane', 'plane_custom', 'fitsuri', 'fitsuri_custom',
     'members', 'inputs'))


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


def read_fixed_object_names():
    """
    Read a file listing fixed object names.
    """

    result = {}

    with open('/net/kamaka/export/data/jsa_proc/fixed-object-names.txt') as f:
        for line in f:
            line = line.strip()
            if line.startswith('#') or not line:
                continue

            (_id, object_name) = line.split(' ', 1)
            result[_id] = object_name

    return result


def _ensure_file_extension(file_id):
    """
    Infer and add file extension, if missing.
    """

    if '.' not in file_id:
        if file_id.startswith('a') or file_id.startswith('s'):
            file_id = file_id + '.sdf'

            # Also apply routine which determines if we need to add .gz:
            file_id = make_file_id_jcmt(file_id)

        elif file_id.startswith('jcmts') or file_id.startswith('jcmth'):
            file_id = file_id + '.fits'

        else:
            logger.warning('Unexpected start to file_id %s', file_id)

    return file_id


class jcmt2caom2ingest(object):
    """
    A class to ingest reduced data products into the JSA.

    The jcmt2caom2ingest class implements methods to collect metadata from a
    set of FITS files and from the JCMT database that will be passed to
    fits2caom2 to construct a CAOM-2 observation.  Once completed, it is
    serialized as XML and uploaded to the CAOM-2 repository.
    """

    speedOfLight = 2.99792485e8  # Speed of light in m/s
    freq_csotau = 225.0e9  # Frequency of CSO tau meter in Hz
    lambda_csotau = '%12.9f' % (speedOfLight / freq_csotau)
    productType = {
        'reduced':     {0: 'science', 1: 'noise', None: 'auxiliary'},
        'rsp':         {None: 'auxiliary'},
        'rimg':        {None: 'auxiliary'},
        'healpix':     {0: 'science', 1: 'noise', None: 'auxiliary'},
        'hpxrsp':      {None: 'auxiliary'},
        'hpxrimg':     {None: 'auxiliary'},
        'peak-cat':    {1: 'science', None: 'auxiliary'},
        'extent-cat':  {1: 'science', None: 'auxiliary'},
        'extent-mask': {None: 'auxiliary'},
        'extent-moc':  {1: 'science', None: 'auxiliary'},
        'tile-moc':    {1: 'science', None: 'auxiliary'},
    }

    def __init__(self):
        """
        Initialize the caom2ingest structure, especially the attributes
        storing default values for command line arguments.
        """

        # Derive the config path from the script or bin directory path
        if 'CADC_ROOT' in os.environ:
            self.configpath = os.path.abspath(
                os.path.join(os.path.expandvars('$CADC_ROOT'), 'config'))
        else:
            exedir = os.path.abspath(os.path.dirname(sys.path[0]))
            self.configpath = os.path.join(exedir, 'config')

        # routine to convert filepaths into file_ids
        # The default routine supplied here should work for most archives.
        self.make_file_id = make_file_id_jcmt

        # temporary disk space for working files
        self.workdir = None

        # Ingestion parameters and structures
        self.verbose = False
        self.retain = False
        self.prefix = ''         # ingestible files must start with this prefix
        self.replace = False     # True if observations in JCMTLS or JCMTUSER
                                 # can replace each other
        self.big = False         # True to use larger memory for fits2caom2
        self.ingest = False      # True to ingest files from indir into CAOM-2

        # Archive-specific fits2caom2 config and default file paths
        self.config = None
        self.default = None

        # Working structures thatcollect metadata from each file to be saved
        # in self.metadict
        self.collection = None
        # The memberset contains member time intervals for this plane.
        # The member_cache is a dict keyed by the membership headers
        # MBR<n> or OBS<n> that contains the observationURI, date_obs, date_end
        # and release_date for each member.  This is preserved for the whole
        # container on the expectation that the same members will be used by
        # multiple files.
        self.member_cache = dict()
        # The inputset is the set of planeURIs that are inputs for a plane
        # The fileset is a set of input files that have not yet been confirmed
        # as belonging to any particular input plane.
        # The input cache is a dictionary giving the planeURI for each file_id
        # found in a member observation.
        self.fileset = set()
        self.input_cache = dict()

        # The metadata dictionary - fundamental structure for the entire class
        # For the detailed structure of metadict, see the help text for
        # fillMetadictFromFile()
        self.metadict = OrderedDict()

        # Dictionary of explicit WCS infomation, by FITS URI.
        self.explicit_wcs = {}

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

        # Read fixed object names file.
        self.fixed_object_names = read_fixed_object_names()

        self.xmloutdir = None

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

    def fillMetadict(self, files):
        """
        Generic routine to fill the metadict structure by iterating over
        the given files, extracting the required metadata from each file
        in turn using fillMetadictFromFile().

        :param files: list of files to read
        """
        self.metadict.clear()

        # Organise and sort files by file_id to preserve previous behavior
        # of this method, in case the order of processing matters.
        # (This is probably unnecessary.)
        files_by_id = {self.make_file_id(f): f for f in files}

        file_ids = sorted(files_by_id.keys())

        logger.debug('in fillMetadict, file_ids = %s',
                     repr(file_ids))

        # Gather metadata from each file.
        for file_id in file_ids:
            logger.debug('In fillMetadict, use %s', file_id)

            self.fillMetadictFromFile(file_id, files_by_id[file_id])

    def fillMetadictFromFile(self, file_id, filepath):
        """
        Generic routine to read metadata and fill the internal structure
        metadict (a nested set of dictionaries) that will be used to control
        sort and fill the override file templates.

        Arguments:
        file_id : must be added to the header
        filepath : absolute path to the file, must be added to the header
        """
        logger.info('fillMetadictFromFile: %s %s', file_id, filepath)

        # If the file is not a FITS file or is in serious violation of the FITS
        # standard, substitute an empty dictionary for the headers.  This is
        # a silent replacement, not an error, to allow non-FITS files to be
        # ingested along with regular FITS files.
        try:
            self.validation.check_name(filepath)
        except CAOMValidationError:
            return

        head = {}
        moc = None

        try:
            with closing(fits.open(filepath, mode='readonly')) as f:
                head = f[0].header
                self.artifact_part_count[self.fitsfileURI(
                    self.archive, file_id)] = len(f)

                # For some files, e.g. catalogs, the primary HDU does not
                # contain the main data and we may wish to extract information
                # from the first extension.
                first_extension = None
                try:
                    first_extension = f[1].header

                    # Does this look like a MOC file?
                    if ((head['NAXIS'] == 0)
                            and (first_extension['XTENSION'] == 'BINTABLE')
                            and (first_extension.get('PIXTYPE') == 'HEALPIX')):
                        moc = MOC()
                        read_moc_fits_hdu(moc, f[1],
                                          include_meta=True)

                except IndexError:
                    pass

            logger.debug('...got primary header from %s', filepath)

        except:
            logger.debug('...could not read primary header from ',
                         filepath)

        if self.ingest:
            self.validation.is_in_archive(filepath)

        self.build_metadict(
            filepath, self.read_file_info(
                file_id, filepath, head, first_extension, moc))

    def get_png_info(self, filenames):
        """
        Read headers from the given list of PNG files.

        Returns a dictionary structured like that built by build_metadict:

            observationID =>
                productID =>
                    preview => file_info
                    thumbnail => file_info

        Wheere each `file_info` dictionary contains the file_id, size and md5sum.
        """

        info = defaultdict(lambda: defaultdict(dict))

        for filename in filenames:
            if filename.endswith('preview_64.png'):
                continue
            elif filename.endswith('preview_256.png'):
                type_ = 'thumbnail'
            elif filename.endswith('preview_1024.png'):
                type_ = 'preview'
            else:
                logger.warning('Unexpected preview size: %s', filename)
                continue

            file_id = self.make_file_id(filename)
            keywords = read_png_keywords(filename)

            if keywords['jsa:asn_type'] == 'obs':
                observationID = keywords['jsa:obsid']

            else:
                observationID = keywords['jsa:asn_id']

            productID = keywords['jsa:productID']

            info[observationID][productID][type_] = {
                'file_id': file_id,
                'size': os.path.getsize(filename),
                'md5sum': get_md5sum(filename),
            }

        return info

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
        return make_artifact_uri(file_id, archive='JCMT')

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

    def build_metadict(self, filepath, file_info):
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
                    ['custom']
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
              key into the dictionary, where the value is the path to the file.
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
        if not self.ingest:
            raise_exception = False

        # If the plane_dict is completely empty, skip further processing
        if not (file_info.plane
                or file_info.fitsuri
                or file_info.fitsuri_custom):
            return

        # Fetch the required keys from plane_dict
        if not self.collection:
            if raise_exception:
                raise CAOMError(filepath + ' does not define the required'
                                ' key "collection"')
            else:
                return

        if not file_info.observationID:
            if raise_exception:
                raise CAOMError(filepath + ' does not define the required'
                                ' key "observationID"')
            else:
                return

        if not file_info.productID:
            if raise_exception:
                raise CAOMError(
                    filepath + ' does not define the required' +
                    ' key "productID"')
            else:
                return

        if not file_info.uri:
            if raise_exception:
                raise CAOMError(filepath + ' does not call fitsfileURI()'
                                ' or fitsextensionURI()')
            else:
                return

        logger.info(
            'PROGRESS: collection="%s" observationID="%s" productID="%s"',
            self.collection, file_info.observationID, file_info.productID)

        # Build the dictionary structure
        if file_info.observationID not in self.metadict:
            self.metadict[file_info.observationID] = OrderedDict()
        thisObservation = self.metadict[file_info.observationID]

        # If memberset is not empty, the observation is a composite.
        # The memberset is the union of the membersets from all the
        # files in the observation.
        if 'memberset' not in thisObservation:
            thisObservation['memberset'] = set([])
        if file_info.members:
            thisObservation['memberset'] |= file_info.members

        # Create the plane-level structures
        if file_info.productID not in thisObservation:
            thisObservation[file_info.productID] = OrderedDict()
        thisPlane = thisObservation[file_info.productID]

        # Items in the plane_dict accumulate so a key will be defined for
        # the plane if it is defined by any file.  If a key is defined
        # by several files, the definition from the last file is used.
        if 'plane_dict' not in thisPlane:
            thisPlane['plane_dict'] = OrderedDict()

        for (key, value) in file_info.plane.items():
            # Handle release_date as a special case
            if ((key == 'release_date')
                    and (key in thisPlane['plane_dict'])
                    and (value <= thisPlane['plane_dict'][key])):
                continue

            elif (key == 'metrics.sourceNumberDensity'
                    and (key in thisPlane['plane_dict'])
                    and (value == '0')):
                # Don't overwrite an existing
                # metrics.sourceNumberDensity value with 0.
                # This allows us to assume 0 when a "tile-moc" is
                # seen and have the real value from the "extent-cat"
                # always take precedence if present.
                continue

            thisPlane['plane_dict'][key] = value

        # Accumulate items for the custom plane information.
        if 'custom' not in thisPlane:
            thisPlane['custom'] = OrderedDict()

        thisPlane['custom'].update(file_info.plane_custom)

        # If inputset is not empty, the provenance should be filled.
        # The inputset is the union of the inputsets from all the files
        # in the plane.  Beware that files not yet classified into
        # inputURI's may still remain in fileset, and will be
        # resolved if possible in checkProvenanceInputs.
        if 'inputset' not in thisPlane:
            thisPlane['inputset'] = set([])
        if file_info.inputs:
            thisPlane['inputset'] |= file_info.inputs

        # The fileset is the set of input files that have not yet been
        # identified as being recorded in any plane yet.
        if 'fileset' not in thisPlane:
            thisPlane['fileset'] = set([])
        if self.fileset:
            thisPlane['fileset'] |= self.fileset

        # Record the uri and (optionally) the filepath
        if 'uri_dict' not in thisPlane:
            thisPlane['uri_dict'] = OrderedDict()
        if file_info.uri not in thisPlane['uri_dict']:
            thisPlane['uri_dict'][file_info.uri] = filepath

        # Foreach fitsuri in the fitsuri dict, record the metadata
        for (fitsuri, fitsuri_data) in file_info.fitsuri.items():
            # Create the fitsuri-level structures
            if fitsuri in thisPlane:
                thisFitsuri = thisPlane[fitsuri]
            else:
                thisFitsuri = thisPlane[fitsuri] = OrderedDict()
                thisFitsuri['custom'] = OrderedDict()

            # Copy the fitsuri dictionary
            for (key, value) in fitsuri_data.items():
                thisFitsuri[key] = value

        for (fitsuri, fitsuri_custom) in file_info.fitsuri_custom.items():
            # Create the fitsuri-level structures (repeat of above logic).
            if fitsuri in thisPlane:
                thisCustom = thisPlane[fitsuri]['custom']
            else:
                thisFitsuri = thisPlane[fitsuri] = OrderedDict()
                thisCustom = thisFitsuri['custom'] = OrderedDict()

            for (key, value) in fitsuri_custom.items():
                thisCustom[key] = value

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

    def read_file_info(self, file_id, filename, header,
                       first_extension=None, moc=None):
        """
        Given the headers from a FITS file, define plane and URI-dependent
        data structures.

        The following keys must be defined:

        * collection
        * observationID
        * productID
        """

        plane_dict = OrderedStrDict()
        plane_custom_dict = OrderedDict()
        fitsuri_dict = OrderedDefaultDict(OrderedStrDict)
        fitsuri_custom_dict = defaultdict(OrderedDict)
        memberset = set()
        inputset = set()

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
        logger.info('PROGRESS: %s', filename)

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
            observationID = header['OBSID']
        else:
            # any other value for algorithm indicates a composite observation
            self.validation.expect_keyword(filename,
                                           'ASN_ID',
                                           header)
            observationID = header['ASN_ID']

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
                    observationID):
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
                                    filename, observationID,
                                    self.collection))
                elif self.collection != 'SANDBOX':
                    # Complain if the observation matches
                    # an observation in a different collection
                    raise CAOMError(
                        'file: {0}, observationID = "{1}" is also in use'
                        ' in collection = "{2}"'.format(
                            filename, observationID, coll))

        plane_dict['algorithm.name'] = algorithm

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
            plane_dict['proposal.id'] = 'JCMT-LR'
            plane_dict['proposal.pi'] = 'James Clerk Maxwell Telescope'
            plane_dict['proposal.title'] = 'JCMT Legacy Release'

        elif is_defined('PROJECT', header):
            proposal_id = header['PROJECT']
            plane_dict['proposal.id'] = proposal_id

            if proposal_project:
                plane_dict['proposal.project'] = proposal_project

            if is_defined('PI', header):
                proposal_pi = header['PI']

            if is_defined('TITLE', header):
                proposal_title = header['TITLE']

            if not (proposal_pi and proposal_title):
                (proposal_pi, proposal_title) = get_project_pi_title(
                    header['PROJECT'], self.conn, self.tap)

            if proposal_pi is not None:
                plane_dict['proposal.pi'] = proposal_pi
            if proposal_title is not None:
                plane_dict['proposal.title'] = truncate_string(
                    proposal_title, 80)

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

                                # Temporarily adjust file_id values to re-add file extension.
                                this_file_id = _ensure_file_extension(this_file_id)

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

                        memberset.add(mbrn)

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

                                # Temporarily adjust file_id values to re-add file extension.
                                this_file_id = _ensure_file_extension(this_file_id)

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
                            memberset.add(mbrn)

        # Only record the environment from single-member observations
        if algorithm == 'exposure' or (obscnt == 1 or mbrcnt == 1):
            # NB 'SEEINGST' is sometimes defined as an empty string which will
            # fail the >0.0 test
            if (is_defined('SEEINGST', header) and header['SEEINGST'] and
                   not (isinstance(header['SEEINGST'], str)) and header['SEEINGST'] > 0.0):
                plane_dict['environment.seeing'] = '%f' % (header['SEEINGST'],)

            if is_defined('HUMSTART', header):
                # Humity is reported in %, but should be scaled to [0.0, 1.0]
                if header['HUMSTART'] < 0.0:
                    humidity = 0.0
                elif header['HUMSTART'] > 100.0:
                    humidity = 100.0
                else:
                    humidity = header['HUMSTART']
                plane_dict['environment.humidity'] = '%f' % (humidity,)

            if is_defined('ELSTART', header):
                elevation = header['ELSTART']
                if elevation > 90.0:
                    logger.warning('Elevation %f > 90', elevation)
                    elevation = 90.0
                plane_dict['environment.elevation'] = \
                    '%f' % (elevation,)

            if is_defined('TAU225ST', header):
                # Some old data appears to have TAU225ST in string form
                # so convert to float in order to handle that data.
                plane_dict['environment.tau'] = \
                    '%f' % (float(header['TAU225ST']),)
                plane_dict['environment.wavelengthTau'] = \
                    jcmt2caom2ingest.lambda_csotau

            if is_defined('ATSTART', header):
                plane_dict['environment.ambientTemp'] = \
                    '%f' % (header['ATSTART'],)

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
            plane_dict['OBSTYPE'] = obs_type

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
            plane_dict['instrument.name'] = instrument_fullname

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
            plane_dict['instrument.keywords'] = self.instrument_keywords

        # Telescope metadata. geolocation is optional.
        self.validation.restricted_value(
            filename, 'TELESCOP', header, ['JCMT'])

        # Target metadata
        try:
            self.validation.expect_keyword(filename, 'OBJECT', header)
            plane_dict['target.name'] = header['OBJECT']
        except CAOMValidationError:
            fixed_object_name = self.fixed_object_names.get(observationID)
            if fixed_object_name is None:
                raise
            else:
                logger.warning('Used fixed object for %s', observationID)
                plane_dict['target.name'] = fixed_object_name

        if backend != 'SCUBA-2' and is_defined('ZSOURCE', header):
                plane_dict['target.redshift'] = str(header['ZSOURCE'])

        target_type = None
        if is_defined('TARGTYPE', header):
            self.validation.restricted_value(
                filename,
                'TARGTYPE', header, ['FIELD', 'OBJECT'])
            target_type = header['TARGTYPE']

        standard_target = 'FALSE'
        if is_defined('STANDARD', header) and header['STANDARD']:
            standard_target = 'TRUE'
        plane_dict['STANDARD'] = standard_target

        moving = 'FALSE'
        # MOVING header is boolean
        if ((is_defined('MOVING', header) and header['MOVING']) or
                # Distinguish moving targets
                is_blank('OBSRA', header) or
                is_blank('OBSDEC', header)):
            moving = 'TRUE'
        plane_dict['target.moving'] = moving

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
            plane_dict['target_position.cval1'] = str(header['OBSRA'])
            plane_dict['target_position.cval2'] = str(header['OBSDEC'])
            plane_dict['target_position.radesys'] = 'ICRS'
            plane_dict['target_position.equinox'] = '2000.0'
        intent_val = None
        if obs_type and backend:
            intent_val = intent(raw_obs_type, backend).value
            plane_dict['obs.intent'] = str(intent_val)

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
        productID = ''

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

        # Try to compute productID using the standard rules
        # First, determine the science_product
        if instream in self.external_collections:
            # Externally generated data products must define PRODID, which
            # will be used to fill productID and to define science_product
            # as the first dash-separated token in the string
            self.validation.expect_keyword(filename, 'PRODID', header)
            productID = header['PRODID']
            if re.search(r'-', productID):
                science_product = productID.split('-')[0]
            else:
                science_product = productID

        else:
            # Pipeline products must define the science_product as a function
            # of product, which is mandatory.
            # BEWARE that the same dictionary is used for both heterodyne and
            # continuum products.
            science_product_dict = {'reduced': 'reduced',
                                    'rsp': 'reduced',
                                    'rimg': 'reduced',
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
                productID = product_id(backend,
                                       product=science_product,
                                       filter=filter)

            elif (restfreq and bwmode and subsysnr):
                if product in ['reduced', 'rimg', 'rsp',
                               'healpix', 'hpxrsp', 'hpxrimg']:
                    productID = \
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
                                                  observationID,
                                                  productID)

        # TODO: do we only need to do this for the "main" product?
        if instream == 'JCMT':
            if science_product in ['reduced']:
                # Set release dates for non-healpix products
                if latest_release_date:
                    if algorithm != 'exposure':
                        # Don't set the Observation level release date for
                        # "exposures" because the raw data ingestion should
                        # do that.
                        plane_dict['obs.metaRelease'] = latest_release_date
                    plane_dict['plane.metaRelease'] = latest_release_date
                    plane_dict['plane.dataRelease'] = latest_release_date
                else:
                    raise CAOMError('file {0}: '
                                    'Release date could not be '
                                    'calculated from membership: '.format(
                                        filename, observationID))
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
                    plane_dict['obs.metaRelease'] = legacy_release_date
                plane_dict['plane.metaRelease'] = legacy_release_date
                plane_dict['plane.dataRelease'] = legacy_release_date

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
                    {'reduced':    str(CalibrationLevel.CALIBRATED.value),
                     'peak':       str(CalibrationLevel.PRODUCT.value),
                     'extent':     str(CalibrationLevel.PRODUCT.value),
                     }

                if science_product == 'healpix':
                    # We have "healpix" products both for individual
                    # observations and as co-adds -- only the latter should
                    # be level "PRODUCT".
                    if algorithm == 'public':
                        calibrationLevel = str(
                            CalibrationLevel.PRODUCT.value)
                    else:
                        calibrationLevel = str(
                            CalibrationLevel.CALIBRATED.value)
                elif science_product in callevel_dict:
                    calibrationLevel = callevel_dict[science_product]
                else:
                    raise CAOMError(
                        'file {0} '
                        'science product "{1}" is not in {2}!r'.format(
                            filename, science_product,
                            (sorted(callevel_dict))))

            if calibrationLevel:
                plane_dict['plane.calibrationLevel'] = calibrationLevel

        # Check for existence of provenance input headers, which are optional
        logger.info('Reading provenance')
        logger.debug('input_cache:')
        for input_cache_key in sorted(self.input_cache.keys()):
            logger.debug('%s: %s', input_cache_key, repr(self.input_cache[input_cache_key]))

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
                        inputset.add(inpn)
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

                    # Adjust PRVN headers to re-add file extension.
                    # (Current data products should have the extension in PRVN
                    # but old products being reingested may not.)
                    prvn = _ensure_file_extension(prvn)

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
                        inputset.add(self.input_cache[prvn_id])
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
            if product in ['reduced', 'healpix']:
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
                dataProductType = 'measurements'
        if dataProductType:
            plane_dict['plane.dataProductType'] = dataProductType

        # Provenance_name
        self.validation.expect_keyword(filename, 'RECIPE', header)
        plane_dict['provenance.name'] = header['RECIPE']

        # Provenance_project
        dpproject = None
        if is_defined('DPPROJ', header):
            dpproject = header['DPPROJ'].strip()
        elif instream == 'JCMTLS' and proposal_project:
            dpproject = proposal_project
        elif instream == 'JCMT':
            standard_products = ['reduced', 'rsp', 'rimg']
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
            plane_dict['provenance.project'] = dpproject
        else:
            raise CAOMError('file {0}: data processing project '
                            'is undefined'.format(filename))

        # Provenance_reference - likely to be overwritten
        if is_defined('REFERENC', header):
            plane_dict['provenance.reference'] = header['REFERENC']

        # ENGVERS, PIPEVERS and PROCVERS are optional
        if (is_defined('ENGVERS', header) and
                is_defined('PIPEVERS', header)):
            plane_dict['provenance.version'] = (
                'ENGINE:' + header['ENGVERS'][:20]
                + ' PIPELINE:' + header['PIPEVERS'][:20])
        elif is_defined('PROCVERS', header):
            plane_dict['provenance.version'] = header['PROCVERS']

        if is_defined('PRODUCER', header):
            plane_dict['provenance.producer'] = header['PRODUCER']

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
            plane_dict['provenance.runID'] = self.dprcinst
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
        plane_dict['provenance.lastExecuted'] = dpdate

        # Chunk
        bandpassName = None
        if backend == 'SCUBA-2' and filter:
            bandpassName = 'SCUBA-2-' + filter + 'um'
            plane_dict['bandpassName'] = bandpassName
        elif backend in ('ACSIS', 'DAS', 'AOSC'):
            if (is_defined('MOLECULE', header) and
                    is_defined('TRANSITI', header) and
                    header['MOLECULE'] != 'No Line'):
                plane_dict['energy.transition.species'] = header['MOLECULE']

                # Tidy transition name.  (It appears this used to be done by
                # CAOM-2 itself, but now we need to do it here.)
                plane_dict['energy.transition.transition'] = re.sub(
                    r'\s\s+', ' ', header['TRANSITI'])

        uri = self.fitsfileURI(self.archive, file_id)
        # Recall that the order in fitsuri_dict is called is preserved
        # in the override file

        # Translate the PRODTYPE header into a (extension_number, type)
        # dictionary, where the default has extension_number = None.
        prodtypes = {None: 'auxiliary'}

        if is_defined('PRODTYPE', header):
            prodtypes[0] = header['PRODTYPE'].lower()
            if is_defined('PRODTYPE', first_extension):
                prodtypes[1] = first_extension['PRODTYPE'].lower()

        elif product in jcmt2caom2ingest.productType:
            prodtypes = jcmt2caom2ingest.productType[product]

        for prodtype in prodtypes.values():
            if prodtype not in [x.value for x in ProductType.__members__.values()]:
                raise CAOMError('file {0}: invalid ProductType "{1}"'.format(
                    filename, prodtype))

        prodtype_default = prodtypes.get(None)

        prodtype_exts = sorted(x for x in prodtypes.keys() if x is not None)

        if prodtype_exts:
            for ext in prodtype_exts:
                extURI = self.fitsextensionURI(self.archive, file_id, [ext])
                fitsuri_dict[extURI]['part.productType'] = prodtypes[ext]

            if prodtype_default:
                fitsuri_dict[uri]['part.productType'] = prodtype_default

        elif prodtype_default:
            fitsuri_dict[uri]['artifact.productType'] = prodtype_default

        else:
            raise CAOMError(
                'file {0}: ProductType is not defined'.format(filename))

        if is_main_product and len(obstimes):
            # Record times for science products
            for key in sorted(obstimes, key=lambda t: obstimes[t][0]):
                fitsuri_custom_dict[uri][key] = obstimes[key]

        # If this is a catalog file, generate explicit WCS information as
        # fits2caom2 may not be able to do it.  For now assume that all
        # tiles are of SCUBA-2 data.
        if is_catalog and uri not in self.explicit_wcs:
            self.explicit_wcs[uri] = {
                'spatial': jsa_tile_wcs(header),
                'spectral': scuba2_spectral_wcs(header),
            }

        # Temporary workaround for HEALPix co-adds for which
        # the CAOM-2 repository rejects the WCS information
        # written by fits2caom2, while awaiting a response to our
        # inquiries to CADC about this problem.
        is_healpix_850 = (is_defined('PRODID', header)
                          and header['PRODID'] == 'healpix-850um')
        if (is_healpix_850 and (algorithm == 'public') and
                (uri not in self.explicit_wcs) and (header['TILENUM'] in [
                    1399,   # job 318778
                    3054,
                    3055,
                    3066,
                    3067,
                    3755,
                    5597,   # job 319661
                    10990,  # job 320507
                    14301,
                    14303,
                    14325,
                    14327,
                    15358,  # job 321123
                    15359,
                    15703,
                    16042,
                    16043,  # job 321333
                    16298,  # job 321359
                ])):
            self.explicit_wcs[uri] = {
                'spatial': jsa_tile_wcs(header),
                'replace_only': True,
            }
        # Also temporarily work around problems for HEALPix obs products.
        if (is_healpix_850 and (algorithm == 'exposure') and
                (uri not in self.explicit_wcs)):
            need_explicit_wcs = False
            if (header['OBSID'] in [
                    'scuba2_00013_20121214T051903',
                    'scuba2_00018_20121214T061802',
                    'scuba2_00019_20130621T144346',
                    'scuba2_00021_20120224T065458',  # IndexOutOfBoundsException
                    'scuba2_00021_20121214T071005',
                    'scuba2_00022_20130714T075458',  # IndexOutOfBoundsException
                    'scuba2_00024_20130112T072839',  # IndexOutOfBoundsException
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
                    'scuba2_00014_20111215T061536',
                    'scuba2_00020_20200226T044208',
                    'scuba2_00046_20150505T083107',
                    ]):
                need_explicit_wcs = True

            else:
                for (bad_project, bad_object) in [
                        ('JCMTCAL', '0723-008'),
                        ('JCMTCAL', '1803+784'),
                        ('M16AN002', '1803+784'),
                        ('M17BL007', 'NEP-1'),
                        ('M17BL007', 'NEP-11'),
                        ('M17BL007', 'NEP-12'),
                        ('M17BL007', 'NEP-2'),
                        ('M17BL007', 'NEP-3'),
                        ('M17BL007', 'NEP-7'),
                        ('M17BL007', 'NEP-8'),
                        ('M17BL007', 'NEP-9'),
                        ('MJLSC02', 'Akari-NEP_PONG2700_850'),
                        ]:
                    if ((header['PROJECT'] == bad_project)
                            and (header['OBJECT'] == bad_object)):
                        need_explicit_wcs = True

            if need_explicit_wcs:
                self.explicit_wcs[uri] = {
                    'spatial': jsa_tile_wcs(header),
                    'replace_only': True,
                }
                logger.warning('Using explicit spatial WCS for this observation')

        # Determine plane metrics.
        if product in ['peak-cat', 'extent-cat']:
            if first_extension is None:
                logger.warning('Didn\'t get first exenstion for a catalog')
            elif not is_defined('NAXIS2', first_extension):
                logger.warning('Catalog has no NAXIS2 in first extension')
            else:
                plane_custom_dict['source_count'] = first_extension['NAXIS2']
        elif product in ['tile-moc']:
            if moc is None:
                logger.warning('Didn\'t get MOC object for a tile-moc')
            else:
                plane_custom_dict['area_covered'] = moc.area_sq_deg

        return FileInfo(
            observationID=observationID, productID=productID, uri=uri,
            plane=plane_dict, plane_custom=plane_custom_dict,
            fitsuri=fitsuri_dict, fitsuri_custom=fitsuri_custom_dict,
            members=memberset, inputs=inputset)

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
                fid = extract_artifact_uri_filename(
                    row.artifact_uri, archive='JCMT')

                # Temporarily adjust file_id values to re-add file extension.
                fid = _ensure_file_extension(fid)

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
                                inputURI not in thisPlane['inputset']):

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
            if (observation.algorithm.name != SimpleObservation._DEFAULT_ALGORITHM_NAME and
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

                # In replacement mode, replace existing bad WCS information
                # if it is present rather than attempting to determine
                # which parts should have it.  Note: we currently only
                # check for the presence of spatial WCS in this mode.
                if wcs.get('replace_only', False):
                    if ((len(part.chunks) == 0)
                            or (part.chunks[0].position is None)):
                        continue

                elif (not (
                        part.product_type in [ProductType.SCIENCE,
                                              ProductType.NOISE]
                        or ('_extent-mask' in fitsuri and (
                            partName == '0'
                            or (len(part.chunks) > 0 and part.chunks[0].position is not None))))):
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
            for prod in list(observation.planes.keys()):
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

    def add_pngs_to_plane(self, observation, planeID, pngs):
        """
        Add PNG images as preview and thumbnail to the plane.

        Also add these images to the raw image plane if appropriate.
        """

        planes = [observation.planes[planeID]]

        # If this is the "reduced" plane and there is an equivalent
        # "raw" plane, use these previews for it too.
        (product, product_suffix) = planeID.split('-', 1)
        if product == 'reduced':
            raw_planeID = '{}-{}'.format('raw', product_suffix)
            if raw_planeID in observation.planes:
                planes.append(observation.planes[raw_planeID])

        for plane in planes:
            for (type_name, product_type) in [
                        ('preview', ProductType.PREVIEW),
                        ('thumbnail', ProductType.THUMBNAIL),
                    ]:
                png_info = pngs.get(type_name)
                if png_info is None:
                    logger.warning('PNG %s not found', type_name)
                    continue

                png_id = png_info['file_id']

                logger.info(
                    'ADDING PNG %s to plane %s as %s',
                    png_id, plane.product_id, type_name)

                uri = self.fitsfileURI(self.archive, png_id)

                artifact = Artifact(
                    uri, product_type=product_type,
                    release_type=ReleaseType.META,
                    content_type=determine_mime_type(png_id),
                    content_length=png_info['size'],
                    content_checksum=ChecksumURI(
                        'md5:{}'.format(png_info['md5sum'])))

                plane.artifacts[uri] = artifact

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
                                'removing old plane: %s', self.planeURI(
                                    self.collection, obsid, prod).uri)

                            del obs.planes[prod]

            del self.remove_dict[obsid]

    def prepare_override_info(self, observationID, productID):
        """
        Prepare the information required in override files for a plane
        specified by the collection, observationID and productID.

        Arguments:
        observationID : the observationID containing productID
        productID : productID for this plane

        Returns:
        A tuple (general, section) containing the global and URI-specific
        parts of the override information.
        """
        thisObservation = self.metadict[observationID]
        thisPlane = thisObservation[productID]

        general = thisPlane['plane_dict'].copy()
        sections = OrderedDict()

        # Incorporate extra information into the general information.
        # This allows us to combine information which comes from multiple
        # input files.
        thisPlaneCustom = thisPlane['custom']
        source_count = thisPlaneCustom.get('source_count')
        area_covered = thisPlaneCustom.get('area_covered')
        if area_covered is None and productID.startswith('peak-'):
            # For "peak" catalogs, get the area covered from the "extent"
            # plane, since the area is defined there and we generate both
            # planes in a single data processing job.
            extPlane = thisObservation.get(
                'extent-' + productID.split('-', 1)[1])
            if extPlane is not None:
                area_covered = extPlane['custom'].get('area_covered')

        if area_covered is not None:
            if source_count is None:
                general['metrics.sourceNumberDensity'] = '0'
            else:
                general['metrics.sourceNumberDensity'] = '{0}'.format(
                    source_count / area_covered)
        elif source_count is not None:
            logger.warning('Source count is defined but area covered is not')

        # Prepare artifact-specific overrides.  This involves filtering
        # the data structure to remove things which don't correpsond to
        # sections of the override file (e.g. "plane_dict") and things
        # which shouldn't appear in individual secions (e.g. "custom").
        for fitsuri in thisPlane:
            if fitsuri not in ('uri_dict',
                               'inputset',
                               'fileset',
                               'plane_dict',
                               'custom'):
                thisFitsuri = thisPlane[fitsuri].copy()
                try:
                    del thisFitsuri['custom']
                except KeyError:
                    pass
                sections[fitsuri] = thisFitsuri

        return (general, sections)

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

    def ingestPlanesFromMetadict(self, png_info):
        """
        Generic routine to ingest the planes in metadict, keeping track of
        members and inputs.

        Arguments:
        png_info
        """

        for observationID in self.metadict:
            thisObservation = self.metadict[observationID]
            observation_png = png_info.get(observationID)

            obsuri = self.observationURI(self.collection,
                                         observationID)

            with self.repository.process(
                    obsuri, dry_run=self.dry_run) as wrapper:
                existing_artifacts = None
                if wrapper.observation is not None:
                    self.remove_excess_parts(wrapper.observation)
                    existing_artifacts = self.get_existing_artifacts(
                        wrapper.observation)

                for productID in thisObservation:
                    if productID != 'memberset':
                        thisPlane = thisObservation[productID]
                        plane_png = None if observation_png is None \
                            else observation_png.get(productID)

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
                            filepathlist = [thisPlane['uri_dict'][u]
                                            for u in urilist]
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
                            retain=self.retain,
                            big=self.big,
                            dry_run=False)
                        logger.info(
                            'INGESTED: observationID=%s productID="%s"',
                            observationID, productID)

                        for fitsuri in thisPlane:
                            if fitsuri not in ('plane_dict',
                                               'uri_dict',
                                               'inputset',
                                               'fileset',
                                               'custom'):

                                self.update_time_information(
                                    wrapper.observation,
                                    observationID, productID, fitsuri)

                        self.set_explicit_wcs(wrapper.observation, productID)

                        if plane_png is not None:
                            self.add_pngs_to_plane(
                                wrapper.observation, productID, plane_png)

                self._apply_fixes(wrapper.observation)

                logger.info('Removing old planes from this observation')
                self.remove_old_planes(wrapper.observation,
                                       observationID)

                if existing_artifacts is not None:
                    logger.info('Removing old version artifacts from this observation')
                    self.remove_old_artifacts(
                        wrapper.observation, existing_artifacts)

                if self.xmloutdir:
                    with open(os.path.join(self.xmloutdir, re.sub(
                            '[^-_A-Za-z0-9]', '_', observationID)) + '.xml',
                            'wb') as f:
                        self.repository.writer.write(wrapper.observation, f)

            logger.info('SUCCESS observationID="%s"', observationID)

        logger.info('Removing old observations and planes')
        self.remove_old_observations_and_planes()

    def fix_observation(self, observationID):
        obsuri = self.observationURI(self.collection, observationID)

        logger.info('URI: %s', str(obsuri))

        with self.repository.process(obsuri, dry_run=self.dry_run) as wrapper:
            if wrapper.observation is None:
                logger.error('NOT FOUND observationID="%s"', observationID)

                return False

            else:
                if self.xmloutdir:
                    with open(os.path.join(self.xmloutdir, re.sub(
                            '[^-_A-Za-z0-9]', '_', observationID)) + '_before.xml',
                            'wb') as f:
                        self.repository.writer.write(wrapper.observation, f)

                self.remove_excess_parts(wrapper.observation)

                self._apply_fixes(wrapper.observation)

                if self.xmloutdir:
                    with open(os.path.join(self.xmloutdir, re.sub(
                            '[^-_A-Za-z0-9]', '_', observationID)) + '_after.xml',
                            'wb') as f:
                        self.repository.writer.write(wrapper.observation, f)

        logger.info('SUCCESS observationID="%s"', observationID)

        return True

    def _apply_fixes(self, observation):
        for plane in observation.planes.values():
            for artifact in plane.artifacts.values():
                for (part_name, part) in artifact.parts.items():
                    for (chunk_num, chunk) in enumerate(part.chunks):
                        # Remove observable_axis
                        if chunk.observable_axis is not None:
                            chunk.observable_axis = None

                        # Remove weird position in extensions.
                        part_num = int(part_name)
                        if part_num > 2:
                            if chunk.position is not None:
                                if chunk.position.axis is not None:
                                    if chunk.position.axis.axis1.ctype.startswith('AZ') and chunk.position.coordsys == 'FK5':
                                        chunk.position = None
                                        logger.warning(
                                            'Position ctype AZ but coordsys FK5, removing spatial WCS for %s %s %s part %s chunk %i',
                                            observation.observation_id, plane.product_id, artifact.uri, part_name, chunk_num)

                        # If there is no position, we can't specify position_axis_1/2.
                        if chunk.position is None:
                            if chunk.position_axis_1 is not None:
                                chunk.position_axis_1 = None
                                logger.warning(
                                    'Position not present, removing position_axis_1 for %s %s %s part %s chunk %i',
                                    observation.observation_id, plane.product_id, artifact.uri, part_name, chunk_num)

                            if chunk.position_axis_2 is not None:
                                chunk.position_axis_2 = None
                                logger.warning(
                                    'Position not present, removing position_axis_2 for %s %s %s part %s chunk %i',
                                    observation.observation_id, plane.product_id, artifact.uri, part_name, chunk_num)

                        # If we don't identify the axes, remove naxis for now
                        # to avoid "bad request" error from CAOM-2 repository.
                        if chunk.naxis is not None:
                            naxis = 0
                            for axis in (
                                    chunk.position_axis_1,
                                    chunk.position_axis_2,
                                    chunk.energy_axis,
                                    chunk.time_axis,
                                    chunk.polarization_axis,
                                    chunk.custom_axis,
                                    ):
                                if axis is not None:
                                    naxis += 1
                            if chunk.naxis != naxis:
                                logger.warning(
                                    'Axes do not match (chunk.naxis = %i, axes defined = %i), removing naxis value for %s %s %s part %s chunk %i',
                                    chunk.naxis, naxis, observation.observation_id, plane.product_id, artifact.uri, part_name, chunk_num)
                                chunk.naxis = None

    def get_existing_artifacts(self, observation, is_existing=True):
        """
        Get a dictionary of existing artifacts in the observation.

        Keys are (planeID, versionless-URI) tuples, values are (full-URI,
        version string) tuples.

        For "is_existing" mode, give lowest version and warn of multiples.
        Otherwise give highest version.
        """

        version_res = [
            re.compile('_(\d\d\d)(\.fits?)$'),
            re.compile('_(\d\d\d)(_preview_\d+.png)$'),
        ]

        existing_artifacts = {}

        for (planeID, plane) in observation.planes.items():
            # Sort so that the version we want is first.
            artifactIDs = sorted(plane.artifacts.keys())
            if not is_existing:
                artifactIDs = reversed(artifactIDs)

            for artifactID in artifactIDs:
                for version_re in version_res:
                    m = version_re.search(artifactID)
                    if m:
                        version = m.group(1)

                        (versionless, count) = version_re.subn(
                            '_XXX\\2', artifactID, count=1)
                        assert(count == 1)

                        logger.debug(
                            'Inferred version %s: %s %s',
                            version, planeID, artifactID)

                        key = (planeID, versionless)
                        if key in existing_artifacts:
                            if is_existing:
                                logger.warning(
                                    'Multiple versions of %s %s present',
                                    planeID, artifactID)
                        else:
                            existing_artifacts[key] = (artifactID, version)

                        break

                else:
                    logger.warning(
                        'No version number found in %s', artifactID)

        return existing_artifacts

    def remove_old_artifacts(self, observation, previous_artifacts):
        """
        Compare the version number for artifacts in the observation
        matching those in the given dictionary of previous artifacts.

        Remove any artifacts for which a matching artifact with
        higher version number is now present.

        Raise an error if a lower version number is now present.

        Note: only deals with two versions being present (previous lowest
        and highest new version).  Any others will not be considered.
        """

        # Fetch with is_existing=False to get highest version and not warn
        # of duplicates.
        new_artifacts = self.get_existing_artifacts(
            observation, is_existing=False)

        for (id_, new_info) in new_artifacts.items():
            old_info = previous_artifacts.get(id_, None)
            if old_info is None:
                continue

            (planeID, versionless) = id_
            (new_uri, new_ver) = new_info
            (old_uri, old_ver) = old_info

            if new_ver < old_ver:
                raise CAOMError(
                    'Version of %s: %s reverted %s -> %s'
                    % (planeID, new_uri, old_ver, new_ver))

            if new_ver > old_ver:
                plane = observation.planes.get(planeID)
                if plane is None:
                    logger.warning(
                        'Plane %s vanished, could not remove old file',
                        planeID)
                    continue

                if old_uri not in plane.artifacts:
                    logger.warning(
                        'Old file already vanished %s %s',
                        planeID, old_uri)
                    continue

                logger.info('Removing old version artifact: %s', old_uri)
                del plane.artifacts[old_uri]

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
                    part_names.sort(key=lambda x: int(x))

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

        By default, it runs a set of file verification tests, creates a
        report of errors and warnings and exits.  This is referred to as
        "check mode" and anyone can run `jsaingest` in check mode.

        Check mode implements several of the checks that would be done
        during a CADC e-transfer operation, such as rejecting zero-length
        files, running fitsverify on FITS files to verify that they do not
        generate error messages, and verifying that names match the regex
        required for the archive.  Other checks include metadata checks for
        mandatory keywords or keywords that must have one of a restricted
        set of values, and verifying whether the file is already present
        in the archive (sometimes forbidden, sometimes mandatory).

        With the --ingest switch, caom2ingest will ingest the files into
        CAOM-2.  However it is managed, the transfer of files into AD must
        already have occurred before --ingest is invoked.  In addition,
        all raw observations in the membership must already have been
        successfully ingested.

        :return: True on success, False otherwise.
        """

        progname = os.path.basename(os.path.splitext(sys.argv[0])[0])

        ap = argparse.ArgumentParser(progname)

        ap.add_argument('--proxy',
                        default='~/.ssl/cadcproxy.pem',
                        help='path to CADC proxy')

        # Ingestion modes
        ap.add_argument('--prefix',
                        help='file name prefix that identifies files '
                             'to be ingested')
        ap.add_argument('--indir',
                        help='path to release data on disk')
        ap.add_argument('--replace',
                        action='store_true',
                        help='observations in JCMTLS and JCMTUSER can '
                             'replace existing observations')
        ap.add_argument('--ingest',
                        action='store_true',
                        help='ingest from AD files that are ready for '
                             'ingestion if there are no errors')

        ap.add_argument('--fix',
                        help='apply fixes to existing observation')

        # Basic fits2caom2 options
        # Optionally, specify explicit paths to the config and default files
        ap.add_argument('--collection',
                        required=True,
                        choices=self.collection_choices,
                        help='collection to use for ingestion')
        ap.add_argument('--config',
                        help='path to fits2caom2 config file')
        ap.add_argument('--default',
                        help='path to fits2caom2 default file')

        # Big jobs require extra memory
        ap.add_argument('--big',
                        action='store_true',
                        help='request extra heap space and RAM')

        # output directory
        ap.add_argument('--workdir',
                        help='output directory, (default = current directory')

        # debugging options
        ap.add_argument('--dry-run', '-n',
                        action='store_true',
                        dest='dry_run',
                        help='simulate operation of fits2caom2')
        ap.add_argument('--verbose', '-v',
                        action='store_true',
                        help='show all messages, pass --debug to fits2caom2')
        ap.add_argument('--retain',
                        action='store_true',
                        help='retain all fits2caom2 xml and override files')

        ap.add_argument('--quiet', '-q',
                        action='store_true',
                        help='show only warning and error messages')
        ap.add_argument('--xmloutdir',
                        help='directory into which to write XML files')
        ap.add_argument('--argus',
                        action='store_true',
                        help='use argus (public TAP service) instead of AMS')

        args = ap.parse_args()

        proxy = os.path.abspath(
            os.path.expandvars(
                os.path.expanduser(args.proxy)))

        self.collection = args.collection

        if args.prefix:
            self.prefix = args.prefix
            file_id_regexes = [
                re.compile(r'^' + self.prefix + r'.*\.fits$'),
                re.compile(r'^' + self.prefix + r'.*\.fit$'),
                re.compile(r'^' + self.prefix + r'.*\.log$'),
                re.compile(r'^' + self.prefix + r'.*\.txt$'),
            ]
        else:
            file_id_regexes = [
                re.compile(r'^.*\.fits$'),
                re.compile(r'^.*\.fit$'),
                re.compile(r'^.*\.png$'),
            ]

        if args.big:
            self.big = args.big

        if args.config:
            self.config = os.path.abspath(
                os.path.expandvars(
                    os.path.expanduser(args.config)))
        if args.default:
            self.default = os.path.abspath(
                os.path.expandvars(
                    os.path.expanduser(args.default)))

        if args.workdir:
            self.workdir = os.path.abspath(
                os.path.expandvars(
                    os.path.expanduser(args.workdir)))
        else:
            self.workdir = os.getcwd()

        if args.replace:
            self.replace = args.replace

        self.dry_run = args.dry_run

        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
            self.verbose = True
        elif args.quiet:
            logging.getLogger().setLevel(logging.WARNING)
        if args.retain:
            self.retain = True

        # create workdir if it does not already exist
        if not os.path.exists(self.workdir):
            os.makedirs(self.workdir)

        if args.ingest:
            self.ingest = args.ingest

        self.xmloutdir = args.xmloutdir

        # Report command line argument values.
        logger.info(progname)
        logger.info('jcmt2caom2version  = %s', jcmt2caom2version)
        logger.info('tools4caom2version = %s', tools4caom2version)
        logger.info('configpath         = ' + self.configpath)
        for attr in dir(args):
            if attr != 'id' and attr[0] != '_':
                logger.info('%-18s = %s', attr, getattr(args, attr))
        logger.info('workdir            = %s', self.workdir)

        try:
            if args.fix:
                return self.fix_observation(args.fix)

            if self.collection in self.external_collections:
                if not self.prefix:
                    logger.error('--prefix is mandatory if --collection '
                                 'is in ' + repr(self.external_collections))
                    raise CAOMError('error in command line options')

            if not args.indir:
                logger.error('--indir is mandatory if not in --fix mode')
                raise CAOMError('error in command line options')

            indirpath = os.path.abspath(
                os.path.expandvars(
                    os.path.expanduser(args.indir)))

            # is this a local directorory on the disk?
            if not os.path.isdir(indirpath):
                raise CAOMError('--indir = ' + args.indir + ' does not exist')

            if not os.path.exists(proxy):
                raise CAOMError('proxy does not exist: ' + proxy)

            self.tap = CAOM2TAP(proxy=proxy, ams=(not args.argus))

            if not os.path.isdir(self.workdir):
                raise CAOMError('workdir is not a directory: '
                                + self.workdir)

            if self.config and not os.path.isfile(self.config):
                raise CAOMError('config file does not exist: '
                                + str(self.config))

            if self.default and not os.path.isfile(self.default):
                raise CAOMError('default file does not exist: '
                                + str(self.default))

            self.conn = ArcDB()

            # Construct validation object
            self.validation = CAOMValidation(
                self.archive, file_id_regexes, self.make_file_id)


            files = self.getfilelist(indirpath)
            self.fillMetadict([x for x in files if not x.endswith('.png')])

            png_info = self.get_png_info([x for x in files if x.endswith('.png')])

            self.checkProvenanceInputs()
            if self.ingest:
                self.ingestPlanesFromMetadict(png_info=png_info)

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
            if self.conn is not None:
                self.conn.close()

        return True
