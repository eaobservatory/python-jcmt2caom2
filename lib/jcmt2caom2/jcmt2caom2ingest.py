"""
The jcmt2caom2ingest class immplements methods to collect metadata from a set
of FITS files and from the jcmt database that will be passed to fits2caom2 to
construct a caom2 observation.  Once completed, it is serialized to a temporary
xml file in workdir and copied to the CAOM-2 repository.

This routine requires read access to the jcmtmd database, but does only reads.
It should therefore access SYBASE rather than DEVSYBASE unless heavy loading
makes SYBASE access problematic.
"""

__author__ = "Russell O. Redman"

from astropy.time import Time
from ConfigParser import SafeConfigParser
from contextlib import contextmanager
import datetime
import logging
import os.path
try:
    from astropy.io import fits as pyfits
except:
    import pyfits
import re
import shutil
import string
import vos

from caom2.caom2_enums import CalibrationLevel
from caom2.caom2_enums import ObservationIntentType
from caom2.wcs.caom2_axis import Axis
from caom2.wcs.caom2_coord_axis1d import CoordAxis1D
from caom2.wcs.caom2_coord_bounds1d import CoordBounds1D
from caom2.wcs.caom2_coord_range1d import CoordRange1D
from caom2.wcs.caom2_ref_coord import RefCoord
from caom2.wcs.caom2_temporal_wcs import TemporalWCS
from caom2.caom2_enums import ProductType
from caom2.caom2_simple_observation import SimpleObservation

from tools4caom2.database import database
from tools4caom2.error import CAOMError
from tools4caom2.timezone import UTC
from tools4caom2.mjd import utc2mjd
from tools4caom2.utdate_string import UTDATE_REGEX
from tools4caom2.caom2ingest import caom2ingest

from jcmt2caom2.project import get_project_pi_title

from jcmt2caom2.jsa.instrument_keywords import instrument_keywords
from jcmt2caom2.jsa.instrument_name import instrument_name
from jcmt2caom2.jsa.intent import intent
from jcmt2caom2.jsa.product_id import product_id
from jcmt2caom2.jsa.raw_product_id import raw_product_id
from jcmt2caom2.jsa.target_name import target_name

from jcmt2caom2.__version__ import version as jcmt2caom2version

logger = logging.getLogger(__name__)


# Utility functions
def is_defined(key, header):
    """
    return True if key is in header and has a defined value, False otherwise
    This is useful for optional headers whose absence is not an error, or for
    metadata with more complicated logic than is supported using the
    prepackaged tests in delayed_error_warn.  Use the error() or warn() methods
    from that package to report errors and warnings that affect ingestion.
    """
    return (key in header and header[key] != pyfits.card.UNDEFINED)


def is_blank(key, header):
    """
    return True if key is in header and has an undefined value, False otherwise
    This is useful for optional headers whose presence or absence acts as a
    flag for some condition.
    """
    return (key in header and header[key] == pyfits.card.UNDEFINED)


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


class jcmt2caom2ingest(caom2ingest):
    """
    A derived class of caom2ingest specialized to ingest externally generated
    products into the JSA.
    """
    speedOfLight = 2.99792485e8  # Speed of light in m/s
    freq_csotau = 225.0e9  # Frequency of CSO tau meter in Hz
    lambda_csotau = '%12.9f' % (speedOfLight / freq_csotau)
    proc_acsis_regex = \
        r'jcmth(20[\d]{2})(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])_' +\
        r'([\d]{5})_(0[0-4])_(cube[\d]{3}|reduced[\d]{3}|rimg|rsp|rvel|' + \
        r'linteg[\d]{3}|sp[\d]{3}|std)_(obs|nit|pro|pub)_([\d]{3})$'
    proc_scuba2_regex = \
        r'jcmts(20[\d]{2})(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])_' +\
        r'([\d]{5})_([48]50)_(reduced[\d]{3})_(obs|nit|pro|pub)_([\d]{3})$'
    productType = {'cube': '0=science,1=noise,auxiliary',
                   'reduced': '0=science,1=noise,auxiliary',
                   'rsp': '0=preview,1=noise,auxiliary',
                   'rimg': '0=preview,1=noise,auxiliary',
                   'healpix': '0=science,1=noise,auxiliary',
                   'hpxrsp': '0=preview,1=noise,auxiliary',
                   'hpxrimg': '0=preview,1=noise,auxiliary',
                   'peak-cat': '0=catalog,auxiliary',
                   'extent-cat': '0=catalog,auxiliary',
                   'point-cat': '0=catalog,auxiliary'}

    def __init__(self):
        caom2ingest.__init__(self)
        self.archive = 'JCMT'
        self.stream = 'product'

        # Database credenials are set using cred_id, cred_key in the section
        # [database] of the userconfig file.
        self.userconfigpath = '~/.tools4caom2/tools4caom2.config'

        # This is needed for compatability with other uses of caom2ingest, but
        # should not be used for the JCMT.
        self.database = 'jcmt'
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

        self.UTC = UTC()

        # Connection to database
        self.conn = None

        self.member_cache = {}
        self.input_cache = {}
        self.remove_dict = {}
        self.remove_id = []

        # Are any errors or warnings recorded in this log file?
        self.errors = False
        self.warnings = False
        self.dprcinst = None

        # Read recipe instance mapping file.
        self.recipe_instance_mapping = read_recipe_instance_mapping()

    # ************************************************************************
    # Process the custom command line switchs
    # ************************************************************************
    def processCommandLineSwitches(self):
        """
        Process some JSA-specific args

        Arguments:
        <none>
        """
        caom2ingest.processCommandLineSwitches(self)

        self.collection = self.args.collection

        self.jcmt_db = ''
        self.omp_db = ''

        if self.sybase_defined and self.userconfig.has_section('jcmt'):
            if self.userconfig.has_option('jcmt', 'jcmt_db'):
                self.jcmt_db = (self.userconfig.get('jcmt', 'jcmt_db') + '.' +
                                self.schema + '.')
            if self.userconfig.has_option('jcmt', 'omp_db'):
                self.omp_db = (self.userconfig.get('jcmt', 'omp_db') + '.' +
                               self.schema + '.')

    # ************************************************************************
    # Include the custom command line switch in the log
    # ************************************************************************
    def logCommandLineSwitches(self):
        """
        Log the values of the command line args.

        Arguments:
        <none>
        """
        caom2ingest.logCommandLineSwitches(self)
        logger.info('jcmt2caom2version    = %s', jcmt2caom2version)

    # Discover observations and planes to remove
    def build_remove_dict(self, run_id):
        """
        If identity_instance_id has not already been checked, read back a
        complete list of existing collections, observations and planes,
        which will be deleted if they are not replaced or updated by the
        current recipe instance.

        Arguments:
        run_id: an identity_instance_id as a decimal string to be compared
                with Plane.provenance_runID
        """
        if run_id not in self.remove_id:
            self.remove_id.append(run_id)
            tapcmd = '\n'.join([
                'SELECT',
                '    Observation.collection,',
                '    Observation.observationID,',
                '    Plane.productID,',
                '    Plane.provenance_runID',
                'FROM',
                '    caom2.Observation AS Observation',
                '        INNER JOIN caom2.Plane AS Plane',
                '            ON Observation.obsID=Plane.obsID',
                '        INNER JOIN caom2.Plane AS Plane2',
                '            ON observation.obsID=Plane2.obsID',
                'WHERE',
                '    Plane2.provenance_runID=' + "'" + run_id + "'",
                'ORDER BY Observation.collection, ',
                '         Observation.observationID, ',
                '         Plane.productID'])
            result = self.tap.query(tapcmd)
            if result:
                for coll, obsid, prodid, run in result:
                    this_runID = run
                    eq = (1 if this_runID == run_id else 0)
                    # If this is a "new" JAC processing job number, check also
                    # whether the file came from a previous version of the job
                    # at CADC.
                    if run_id in self.recipe_instance_mapping:
                        if this_runID == self.recipe_instance_mapping[run_id]:
                            eq = 1
                    # Ignore entries in other collections
                    if coll == self.collection:
                        if coll not in self.remove_dict:
                            self.remove_dict[coll] = {}
                        if obsid not in self.remove_dict[coll]:
                            self.remove_dict[coll][obsid] = {}
                        if prodid not in self.remove_dict[coll][obsid]:
                            self.remove_dict[coll][obsid][prodid] = eq

    # ************************************************************************
    # archive-specific structures to write override files
    # ************************************************************************
    def build_dict(self, header):
        '''Archive-specific code to read the common dictionary from the
               file header.
           The following keys must be defined:
               collection
               observationID
               productID
        '''

        if 'file_id' not in header:
            self.errors = True
            raise CAOMError('No file_id in ' + repr(header))

        file_id = header['file_id']
        filename = header['filepath']
        self.dew.sizecheck(filename)

        logger.info('Starting %s', file_id)
        # Doing all the required checks here simplifies the code
        # farther down and ensures error reporting of these basic problems
        # even if the ingestion fails before reaching the place where the
        # header would be used.

        someBAD = False

        # Check that mandatory file headers exist that validate the FITS
        # file structure
        structural = ('BITPIX',
                      'CHECKSUM',
                      'DATASUM')
        for key in structural:
            self.dew.expect_keyword(filename, key, header, mandatory=True)

        # Observation metadata
        if self.collection == 'SANDBOX':
            self.dew.restricted_value(filename,
                                      'INSTREAM', header,
                                      self.collection_choices)
        else:
            self.dew.restricted_value(filename,
                                      'INSTREAM', header, (self.collection,))
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
            self.dew.restricted_value(filename,
                                      'INSTREAM', header, ['JCMT'])
            if self.dew.expect_keyword(filename,
                                       'OBSID',
                                       header,
                                       mandatory=True):
                algorithm = 'exposure'
                self.observationID = header['OBSID']
        else:
            # any other value for algorithm indicates a composite observation
            if self.dew.expect_keyword(filename,
                                       'ASN_ID',
                                       header,
                                       mandatory=True):
                self.observationID = header['ASN_ID']

                # TAP query to find if the observationID is in use.  Do not do
                # this for obs products, since the raw data can be ingested
                # before or after the processed data.
                tapcmd = '\n'.join([
                    "SELECT Observation.collection",
                    "FROM caom2.Observation AS Observation",
                    "WHERE Observation.observationID='" +
                    self.observationID + "'"])
                results = self.tap.query(tapcmd)
                if results:
                    # Check for duplicate observationIDs.
                    # This is always OK in the SANDBOX.
                    # In JCMT, --replace is never needed for observations in
                    # the JCMT collection because replacement is expected.
                    # Otherwise,
                    #  issue an error if --replace is not specified and
                    # the observation exists in the collection, or if --replace
                    # is specified and the observation does not already exist,
                    # or a warning if the observation pre-exists in another
                    # collection.
                    for (coll,) in results:
                        # Do not raise errors for ingestions into the SANDBOX
                        # or into JCMT if coll is also JCMT.
                        if coll == self.collection:
                            if self.collection in ('JCMTLS', 'JCMTUSER'):
                                if not self.replace:
                                    # Raise an error if --replace not is
                                    # specified but the observation already
                                    # exists in the collection
                                    self.dew.error(
                                        filename,
                                        'Must specify --replace if' +
                                        'observationID = "' +
                                        self.observationID +
                                        '" already exists in collection = "' +
                                        self.collection + '"')
                        elif self.collection != 'SANDBOX':
                            # Complain if the observation matches
                            # an observation in a different collection
                            self.dew.warning(
                                filename,
                                'observationID = "' + self.observationID +
                                '" is also in use in collection = "' +
                                coll + '"')

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
            self.dew.restricted_value(filename, 'SURVEY', header,
                                      survey_acronyms)):
            proposal_project = header['SURVEY']

        if is_defined('PROJECT', header):
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

            self.add_to_plane_dict('proposal.pi', proposal_pi)
            self.add_to_plane_dict('proposal.title', proposal_title)

        # Observation membership headers, which are optional
        earliest_utdate = None
        earliest_obs = None
        if algorithm == 'exposure':
            if is_defined('DATE-OBS', header):
                earliest_utdate = Time(header['DATE-OBS']).mjd
            if is_defined('OBSID', header):
                earliest_obs = header['OBSID']

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
                    if self.dew.expect_keyword(filename, mbrkey, header):
                        mbrn_str = header[mbrkey]
                        # mbrn contains a caom observation uri
                        mbr_coll, obsid = mbrn_str.split('/')
                        if mbr_coll != 'caom:JCMT':
                            self.dew.error(
                                filename,
                                mbrkey + ' must point to an '
                                'observation in the JCMT collection: ' +
                                mbrn_str)
                            continue
                    else:
                        continue
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
                        tapcmd = '\n'.join([
                            "SELECT"
                            "       Plane.productID, ",
                            "       Plane.time_bounds_cval1,",
                            "       Plane.time_bounds_cval2,",
                            "       Plane.dataRelease,",
                            "       Artifact.uri",
                            "FROM caom2.Observation as Observation",
                            "         INNER JOIN caom2.Plane AS Plane",
                            "             ON Observation.obsID=Plane.obsID",
                            "         INNER JOIN caom2.Artifact AS Artifact",
                            "             ON Plane.planeID=Artifact.planeID",
                            "WHERE Observation.collection='JCMT'",
                            "      AND Observation.observationID='" +
                            obsid + "'"
                            ])
                        answer = self.tap.query(tapcmd)
                        if len(answer) > 0 and len(answer[0]) > 0:
                            missing = True
                            for (prodid,
                                 date_obs,
                                 date_end,
                                 release,
                                 uri) in answer:

                                if (not date_obs or
                                        not date_end or
                                        not release):
                                    continue

                                # Only extract date_obs, date_end and release
                                # raw planes
                                if missing and re.match(r'raw.*', prodid):
                                    missing = False
                                    release_date = release
                                    if (latest_release_date is None or
                                            release_date >
                                            latest_release_date):

                                        latest_release_date = release_date
                                    # cache mbrn, start, end and release
                                    # caching mbrn is NOT needlessly repetitive
                                    # because with obsn headers it will be
                                    # different
                                    logger.debug(
                                        'cache member_cache[%s] ='
                                        ' [%s, %s, %s, %s]',
                                        mbrn,
                                        mbrn, date_obs, date_end, release_date)
                                    self.member_cache[mbrn] = (mbrn,
                                                               date_obs,
                                                               date_end,
                                                               release_date)
                                    mbr_date_obs = date_obs
                                    mbr_date_end = date_end

                                # Cache provenance input candidates
                                # Do NOT rewrite the file_id
                                if uri not in self.input_cache:
                                    filecoll, this_file_id = uri.split('/')
                                    inURI = self.planeURI('JCMT',
                                                          obsid,
                                                          prodid)
                                    self.input_cache[this_file_id] = inURI
                                    self.input_cache[inURI.uri] = inURI

                    # At this point we have mbrn, mbr_date_obs, mbr_date_end
                    # and release_date either from the member_cache or from
                    # the query
                    if mbr_date_obs:
                        if (earliest_utdate is None or
                                mbr_date_obs < earliest_utdate):

                            earliest_utdate = mbr_date_obs
                            earliest_obs = obsid

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
                    if self.dew.expect_keyword(filename, obskey, header):
                        # This is the obsid_subsysnr of a plane of raw data
                        obsn = header[obskey]
                    else:
                        continue

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
                        raw_regex = (r'(scuba2|acsis|DAS|AOSC|scuba)_'
                                     r'\d+_(\d{8}[tT]\d{6})_\d+')
                        m = re.match(raw_regex, obsn)
                        if m:
                            # obsid_pattern should match a single obsid,
                            # because the datetime in group(2) should be
                            # unique to each observation
                            obsid_pattern = m.group(1) + '%' + m.group(2)
                        else:
                            self.dew.error(
                                filename,
                                obskey + ' = "' + obsn + '" does not '
                                'match the pattern expected for the '
                                'observationID of a member: ' +
                                raw_regex)
                            continue

                        tapquery = '\n'.join([
                            "SELECT",
                            "       Observation.observationID,",
                            "       Plane.productID,",
                            "       Plane.time_bounds_cval1,",
                            "       Plane.time_bounds_cval2,",
                            "       Plane.dataRelease,",
                            "       Artifact.uri",
                            "FROM caom2.Observation as Observation",
                            "         INNER JOIN caom2.Plane AS Plane",
                            "             ON Observation.obsID=Plane.obsID",
                            "         INNER JOIN caom2.Artifact AS Artifact",
                            "             ON Plane.planeID=Artifact.planeID",
                            "WHERE Observation.observationID LIKE '" +
                            obsid_pattern + "'"])
                        answer = self.tap.query(tapquery)
                        logger.debug(repr(answer))
                        if len(answer) > 0 and len(answer[0]) > 0:
                            obsid_solitary = None
                            for (obsid,
                                 prodid,
                                 date_obs,
                                 date_end,
                                 release,
                                 uri) in answer:

                                if (not date_obs or
                                        not date_end or
                                        not release):
                                    continue

                                if obsid_solitary is None:
                                    obsid_solitary = obsid
                                    release_date = release
                                    if (latest_release_date is None or
                                            release_date >
                                            latest_release_date):

                                        latest_release_date = release_date

                                elif obsid != obsid_solitary:
                                    self.dew.error(
                                        obskey + ' = ' + obsn +
                                        ' with obsid_pattern = ' +
                                        obsid_pattern + ' matched ' +
                                        obsid_solitary + ' and ' +
                                        obsid)
                                    break

                                if re.match(r'raw.*', prodid):
                                    # Only cache member date_obs, date_end and
                                    # release_date from raw planes
                                    mbrn = self.observationURI('JCMT', obsid)
                                    # cache the members start and end times
                                    logger.debug(
                                        'cache member_cache[%s] ='
                                        ' [%s, %s, %s, %s]',
                                        obsn, mbrn.uri, date_obs, date_end,
                                        release_date)
                                    if mbrn not in self.member_cache:
                                        self.member_cache[obsn] = \
                                            (obsid,
                                             mbrn,
                                             date_obs,
                                             date_end,
                                             release_date)
                                        mbr_date_obs = date_obs
                                        mbr_date_end = date_end

                                # Cache provenance input candidates
                                # Do NOT rewrite the file_id!
                                if uri not in self.input_cache:
                                    filecoll, this_file_id = uri.split('/')
                                    inURI = self.planeURI('JCMT',
                                                          obsid,
                                                          prodid)
                                    self.input_cache[this_file_id] = inURI
                                    self.input_cache[inURI.uri] = inURI

                    if mbrn is None:
                        self.dew.error(filename,
                                       obskey + ' = ' + obsn +
                                       ' is not present in the JSA')
                    else:
                        # At this point we have mbrn, date_obs, date_end and
                        # release_date either from the member_cache or from
                        # the query
                        if mbr_date_obs:
                            if (earliest_utdate is None or
                                    mbr_date_obs < earliest_utdate):

                                earliest_utdate = mbr_date_obs
                                earliest_obs = obsid

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
                self.add_to_plane_dict('environment.tau',
                                       '%f' % (header['TAU225ST'],))
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
                self.dew.error(
                    filename,
                    'observation types in (flatfield, noise, setup, '
                    'skydip) contain no astronomical data and cannot '
                    'be ingested')

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
            if self.dew.restricted_value(filename,
                                         'BACKEND', header,
                                         ('SCUBA-2', 'ACSIS', 'DAS', 'AOSC')):
                backend = header['BACKEND'].strip().upper()

            instrument_fullname = instrument_name(instrument,
                                                  backend,
                                                  inbeam)

        if instrument_fullname:
            self.add_to_plane_dict('instrument.name', instrument_fullname)

        # Only do these tests if the backend is OK
        if backend in ('ACSIS', 'DAS', 'AOS-C'):
            if inbeam and inbeam != 'POL':
                self.dew.error(filename, 'INBEAM can only be blank or POL '
                                         'for heterodyne observations')

            if is_defined('OBS_TYPE', header):
                self.dew.restricted_value(
                    filename, 'OBS_TYPE', header,
                    ['pointing', 'science', 'focus', 'skydip'])

            if is_defined('SAM_MODE', header):
                self.dew.restricted_value(
                    filename, 'SAM_MODE', header,
                    ['jiggle', 'grid', 'raster', 'scan'])

        elif backend == 'SCUBA-2':
            if is_defined('OBS_TYPE', header):
                self.dew.restricted_value(
                    filename, 'OBS_TYPE', header,
                    ['pointing', 'science', 'focus', 'skydip',
                        'flatfield', 'setup', 'noise'])

            if is_defined('SAM_MODE', header):
                self.dew.restricted_value(
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
            self.dew.error(filename,
                           'instrument_keywords could not be '
                           'constructed from ' + repr(keyword_dict))
        else:
            self.instrument_keywords = ' '.join(keyword_list)
            self.add_to_plane_dict('instrument.keywords',
                                   self.instrument_keywords)

        # Telescope metadata. geolocation is optional.
        self.dew.restricted_value(filename, 'TELESCOP', header, ['JCMT'])

        # Target metadata
        if self.dew.expect_keyword(filename, 'OBJECT', header):
            self.add_to_plane_dict('target.name', header['OBJECT'])

        if backend != 'SCUBA-2' and is_defined('ZSOURCE', header):
                self.add_to_plane_dict('target.redshift',
                                       str(header['ZSOURCE']))

        target_type = None
        if is_defined('TARGTYPE', header):
            if self.dew.restricted_value(
                    filename,
                    'TARGTYPE', header, ['FIELD', 'OBJECT']):
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
        product = None
        if self.dew.expect_keyword(filename, 'PRODUCT', header,
                                   mandatory=True):
            product = header['PRODUCT']

        # The standard and legacy pipelines must have some standard keywords
        if (self.collection == 'JCMT' or instream == 'JCMT'):
            if backend == 'SCUBA-2':
                self.dew.expect_keyword(filename, 'FILTER', header,
                                        mandatory=True)
            else:
                self.dew.expect_keyword(filename, 'RESTFRQ', header,
                                        mandatory=True)
                self.dew.expect_keyword(filename, 'SUBSYSNR', header,
                                        mandatory=True)
                self.dew.expect_keyword(filename, 'BWMODE', header,
                                        mandatory=True)

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
            if self.dew.expect_keyword(filename, 'PRODID', header,
                                       mandatory=True):
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
                                    'peak-cat': 'peak-cat',
                                    'extent-cat': 'extent-cat',
                                    'point-cat': 'point-cat'}

            science_product = None
            if product in science_product_dict:
                science_product = science_product_dict[product]
            else:
                self.dew.error(filename, 'product = "' + product +
                               '" is not one of the pipeline products: ' +
                               repr(sorted(science_product_dict.keys())))

            if filter:
                self.productID = product_id(backend,
                                            product=science_product,
                                            filter=filter)

            elif (restfreq and bwmode and subsysnr):
                if product in ['reduced', 'rimg', 'rsp', 'cube']:
                    self.productID = \
                        product_id(backend,
                                   product=science_product,
                                   restfreq=restfreq,
                                   bwmode=bwmode,
                                   subsysnr=subsysnr)

        # Add this plane to the set of known file_id -> plane translations
        self.input_cache[file_id] = self.planeURI(self.collection,
                                                  self.observationID,
                                                  self.productID)

        if instream == 'JCMT':
            if product in ['reduced', 'cube']:
                # Do not set release dates for healpix products
                if latest_release_date:
                    self.add_to_plane_dict('obs.metaRelease',
                                           latest_release_date)
                    self.add_to_plane_dict('plane.metaRelease',
                                           latest_release_date)
                    self.add_to_plane_dict('plane.dataRelease',
                                           latest_release_date)
                else:
                    self.dew.error(filename,
                                   'Release date could not be '
                                   'calculated from membership: ' +
                                   self.observationID)

        calibrationLevel = None
        # The calibration lelvel needs to be defined for all science products
        if product == science_product:
            if instream in self.external_collections:
                callevel_dict = \
                    {'calibrated': str(CalibrationLevel.CALIBRATED.value),
                     'product':    str(CalibrationLevel.PRODUCT.value)}
                if self.dew.restricted_value(filename,
                                             'CALLEVEL',
                                             header,
                                             sorted(callevel_dict)):
                    calibrationLevel = callevel_dict[header['CALLEVEL']]
            else:
                callevel_dict = \
                    {'cube':       str(CalibrationLevel.RAW_STANDARD.value),
                     'reduced':    str(CalibrationLevel.CALIBRATED.value),
                     'healpix':    str(CalibrationLevel.CALIBRATED.value),
                     'point-cat':  str(CalibrationLevel.PRODUCT.value),
                     'extent-cat': str(CalibrationLevel.PRODUCT.value),
                     'peak-cat':   str(CalibrationLevel.PRODUCT.value)}
                if science_product in callevel_dict:
                    calibrationLevel = callevel_dict[science_product]
                else:
                    self.dew.error(
                        filename,
                        'science product "' + science_product +
                        '" is not in ' + repr(sorted(callevel_dict)))

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
            if product and product == science_product and inpcnt > 0:
                for n in range(inpcnt):
                    inpkey = 'INP' + str(n + 1)
                    if not self.dew.expect_keyword(filename, inpkey, header):
                        continue
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
                        self.dew.error(inpkey + ' = ' + inpn_str + ' does not '
                                       'match the regex for a plane URI: ' +
                                       planeURI_regex)

        elif is_defined('PRVCNT', header):
            # Translate the PRV1..PRV<PRVCNT> headers into plane URIs
            prvcnt = int(header['PRVCNT'])
            if product and product == science_product and prvcnt > 0:
                logger.info('PRVCNT = %s', prvcnt)
                for i in range(prvcnt):
                    # Verify that files in provenance are being ingested
                    # or have already been ingested.
                    prvkey = 'PRV' + str(i + 1)
                    if not self.dew.expect_keyword(filename, prvkey, header):
                        continue
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
                        self.dew.warning(
                            filename,
                            'file_id = ' + file_id + ' includes itself '
                            'in its provenance as ' + prvkey)
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
            if not self.dew.restricted_value(
                    filename, 'DATAPROD', header,
                    ('image', 'spectrum', 'cube', 'catalog')):
                dataProductType = None
        elif product == science_product:
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
                elif product in ('peak-cat', 'extent-cat', 'point-cat'):
                    dataProductType = 'catalog'
        if dataProductType:
            self.add_to_plane_dict('plane.dataProductType', dataProductType)

        # Provenance_name
        if self.dew.expect_keyword(filename, 'RECIPE', header,
                                   mandatory=True):
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
                               'peak-cat', 'extent-cat']
            if product in standard_products:
                # This is the complete list of standard pipeline FITS products
                dpproject = 'JCMT_STANDARD_PIPELINE'
            elif product in legacy_products:
                # healpix and catalogs are from the legacy project
                dpproject = 'JCMT_LEGACY_PIPELINE'
            else:
                self.dew.error(filename,
                               'UNKNOWN PRODUCT in collection=JCMT: ' +
                               product + ' must be one of ' +
                               repr(standard_products + legacy_products))

        if dpproject:
            self.add_to_plane_dict('provenance.project', dpproject)
        else:
            self.dew.error(filename,
                           'data processing project is undefined')

        # Provenance_reference - likely to be overwritten
        if is_defined('REFERENC', header):
            self.add_to_plane_dict('provenance.reference',
                                   header['REFERENC'])

        # ENGVERS and PIPEVERS are optional
        if is_defined('PROCVERS', header):
            self.add_to_plane_dict('provenance.version',
                                   header['PROCVERS'])
        else:
            if (is_defined('ENGVERS', header) and
                    is_defined('PIPEVERS', header)):
                self.add_to_plane_dict('provenance.version',
                                       'ENG:' + header['ENGVERS'][:25] +
                                       ' PIPE:' + header['PIPEVERS'][:25])

        if is_defined('PRODUCER', header):
            self.add_to_plane_dict('provenance.producer',
                                   header['PRODUCER'])

        self.dprcinst = None
        if self.dew.expect_keyword(filename, 'DPRCINST', header,
                                   mandatory=True):
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
            self.dew.error(filename, 'could not calculate dprcinst')

        # Report the earliest UTDATE
        if earliest_utdate and self.dprcinst:
            rcinstprefix = 'caom-' + self.collection + '-' + earliest_obs
            logger.info(
                'Earliest utdate: %s for %s_vlink-%s',
                Time(earliest_utdate, format='mjd', out_subfmt='date').iso,
                rcinstprefix,
                self.dprcinst)

        if self.dew.expect_keyword(filename, 'DPDATE', header, mandatory=True):
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
            self.dew.error(filename,
                           'ProductType is not defined')

        if product == science_product and len(obstimes):
            self.add_fitsuri_dict(self.uri)
            # Record times for science products
            for key in sorted(obstimes, key=lambda t: obstimes[t][0]):
                self.add_to_fitsuri_custom_dict(self.uri,
                                                key,
                                                obstimes[key])

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
            tapquery = '\n'.join([
                "SELECT Observation.collection,",
                "       Observation.observationID,",
                "       Plane.productID,",
                "       Artifact.uri",
                "FROM caom2.Observation AS Observation",
                "    INNER JOIN caom2.Plane as Plane",
                "        ON Observation.obsID=Plane.obsID",
                "    INNER JOIN caom2.Artifact AS Artifact",
                "        ON Plane.planeID=Artifact.planeID",
                "    INNER JOIN caom2.Artifact AS Artifact2",
                "        ON Plane.planeID=Artifact2.planeID",
                "WHERE Artifact2.uri like 'ad:%/" + file_id + "'"])
            answer = self.tap.query(tapquery)

            if answer and len(answer[0]):
                for row in answer:

                    # Collection, obsid, productid and uri
                    c, o, p, u = row

                    # Search for 'ad:<anything that isn't a slash>/'
                    # and replace with nothing with in u
                    fid = re.sub(r'ad:[^/]+/', '', u)

                    if (c in (self.collection,
                              'JCMT',
                              'JCMTLS',
                              'JCMTUSER')):

                        # URI for this plane
                        thisInputURI = self.planeURI(c, o, p)

                        # If the ad URI is the same as file_id, set
                        # inputURI to thisInputURI
                        if fid == file_id:
                            inputURI = thisInputURI

                        # add to cache
                        self.input_cache[fid] = thisInputURI

                        logger.debug('inputs: %s: %s', fid, thisInputURI.uri)

        if inputURI is None:
            self.dew.warning(
                filename,
                'provenance input is neither '
                'in the JSA already nor in the '
                'current release')

        return inputURI

    def checkProvenanceInputs(self):
        """
        From the set of provenance input planeURIs or input files,
        build the list of provenance input URIs for each output plane,
        caching results to save time in the TAP queries.
        """
        for coll in self.metadict:
            for obs in self.metadict[coll]:
                for prod in self.metadict[coll][obs]:
                    if prod != 'memberset':
                        thisPlane = self.metadict[coll][obs][prod]
                        planeURI = self.planeURI(coll, obs, prod)

                        for filename in thisPlane['fileset']:
                            file_id = self.make_file_id(filename)
                            inputURI = self.lookup_file_id(filename,
                                                           file_id)
                            if (inputURI and
                                    inputURI.uri not in thisPlane['inputset']):

                                thisPlane['inputset'].add(inputURI)
                                logger.info('add %s to inputset for %s',
                                            inputURI.uri, planeURI.uri)

    def build_fitsuri_custom(self,
                             observation,
                             collection,
                             observationID,
                             planeID,
                             fitsuri):
        """
        Customize the CAOM-2 observation with fitsuri-specific metadata.  For
        jsaingest, this comprises the time structure constructed from the
        OBSID for simple observations or list of OBSn values for composite
        observations.
        """
        thisCustom = \
            self.metadict[collection][observationID][planeID][fitsuri]['custom']
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
#                                if len(thisCustom) == 1:
#                                    # time range
#                                    for key in thisCustom:
#                                        date_start, date_end = thisCustom[key]
#                                        mjdstart = utc2mjd(date_start)
#                                        mjdend = utc2mjd(date_end)
#                                        self.log.console(
#                                            'time range = %f, %f' %
#                                            (mjdstart, mjdend),
#                                            logging.DEBUG)
#
#                                        time_axis.range = CoordRange1D(
#                                            RefCoord(0.5, mjdstart),
#                                            RefCoord(1.5, mjdend))
#
#                                elif len(thisCustom) > 1:
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
                                    self.warnings = True
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

    def build_plane_custom(self,
                           observation,
                           collection,
                           observationID,
                           productID):
        """
        Implement the cleanup of planes that are no longer generated by this
        recipe instance from observations that are.  It is only necessary to
        remove planes from the current observation that are not already being
        replaced by the new set of products.

        Arguments:
        observation: CAOM-2 observation object to be updated
        collection: current collection
        observationID: current observationID
        productID: current productID NOT USED in this routine
        """
        if collection in self.remove_dict:
            if observationID in self.remove_dict[collection]:
                for prod in observation.planes.keys():
                    # logic is, this collection/observation/plane used to be
                    # genrated by this recipe instance, but is not part of the
                    # current ingestion and so is obsolete.
                    if prod in self.remove_dict[collection][observationID] and \
                            self.remove_dict[collection][observationID][prod] and \
                            prod not in self.metadict[collection][observationID]:

                        uri = self.planeURI(collection,
                                            observationID,
                                            prod)
                        self.warnings = True
                        logger.warning('CLEANUP: remove obsolete plane: %s',
                                       uri.uri)

                        if not self.test:
                            del observation.planes[prod]
                        del self.remove_dict[collection][observationID][prod]

    def build_observation_custom(self,
                                 observation,
                                 collection,
                                 observationID):
        """
        Implement the cleanup of collections, observations, and planes that are
        no longer generated by this recipe instance.  It is only necessary to
        remove items that are not already being replaced by the new set of
        products.  At this level, remove all collection, observations and
        planes from observations that are not generated by the current recipe
        instance.

        Arguments:
        observation: CAOM-2 observation object NOT USED in this routine
        collection: current collection NOT USED in this routine
        observationID: current observationID NOT USED in this routine
        """
        # log the contents of remove_dict
        for coll in self.remove_dict:
            for obsid in self.remove_dict[coll].keys():
                for prodid in self.remove_dict[coll][obsid].keys():
                    logger.info('remove_dict %s: %s: %s = %s',
                                coll, obsid, prodid,
                                self.remove_dict[coll][obsid][prodid])

        for coll in self.remove_dict:
            if coll not in self.metadict:
                # Nothing in the existing collection still exists
                for obsid in self.remove_dict[coll].keys():
                    uri = self.observationURI(coll, obsid)
                    logger.info('CLEANUP: remove %s', uri.uri)
                    if not self.test:
                        self.repository.remove(uri.uri)
                    del self.remove_dict[coll][obsid]
                del self.remove_dict[coll]

            else:
                for obsid in self.remove_dict[coll].keys():
                    uri = self.observationURI(coll, obsid)
                    if obsid not in self.metadict[coll]:
                        # Delete the whole observation or just some planes?
                        same = 1
                        for prodid in self.remove_dict[coll][obsid]:
                            same *= self.remove_dict[coll][obsid][prodid]
                        if same:
                            # all planes come from the same recipe instance
                            # so delete the whole observation
                            self.warnings = True
                            logger.warning(
                                'CLEANUP: remove obsolete observation: %s',
                                uri.uri)
                            if not self.test:
                                self.repository.remove(uri.uri)
                            del self.remove_dict[coll][obsid]
                        else:
                            with repository.process(uri) as wrapper:
                                if wrapper.observation is not None:
                                    obs = wrapper.observation
                                    for prod in self.remove_dict[coll][obsid].keys():
                                        if self.remove_dict[coll][obsid][prod] \
                                                and prod in obs.planes:
                                            uri = self.planeURI(coll,
                                                                obsid,
                                                                prod)
                                            self.warnings = True
                                            logger.warning(
                                                'CLEANUP: remove plane: %s',
                                                uri.uri)

                                            del obs.planes[prod]
                                            del self.remove_dict[coll][obsid][prod]
                                if self.test:
                                    wrapper.observation = None
