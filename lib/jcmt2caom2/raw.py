# Copyright (C) 2014-2015 Science and Technology Facilities Council.
# Copyright (C) 2015-2017 East Asian Observatory.
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
import logging
import math
import os.path
import re
import sys

from omp.db.part.arc import ArcDB
from omp.obs.state import OMPState

from caom2.caom2_artifact import Artifact
from caom2.caom2_chunk import Chunk
from caom2.caom2_data_quality import DataQuality
from caom2.caom2_energy_transition import EnergyTransition
from caom2.caom2_enums import CalibrationLevel, ObservationIntentType, \
    ProductType, Quality, ReleaseType, Status
from caom2.caom2_environment import Environment
from caom2.caom2_instrument import Instrument
from caom2.caom2_part import Part
from caom2.caom2_plane import Plane
from caom2.caom2_proposal import Proposal
from caom2.caom2_requirements import Requirements
from caom2.caom2_simple_observation import SimpleObservation
from caom2.caom2_target import Target
from caom2.caom2_target_position import TargetPosition
from caom2.caom2_telescope import Telescope
from caom2.types.caom2_point import Point
from caom2.wcs.caom2_axis import Axis
from caom2.wcs.caom2_coord_axis1d import CoordAxis1D
from caom2.wcs.caom2_coord_axis2d import CoordAxis2D
from caom2.wcs.caom2_coord_bounds1d import CoordBounds1D
from caom2.wcs.caom2_coord_polygon2d import CoordPolygon2D
from caom2.wcs.caom2_coord_range1d import CoordRange1D
from caom2.wcs.caom2_ref_coord import RefCoord
from caom2.wcs.caom2_spatial_wcs import SpatialWCS
from caom2.wcs.caom2_spectral_wcs import SpectralWCS
from caom2.wcs.caom2_temporal_wcs import TemporalWCS
from caom2.wcs.caom2_value_coord2d import ValueCoord2D

from tools4caom2.__version__ import version as tools4caom2version
from tools4caom2.caom2repo_wrapper import Repository
from tools4caom2.error import CAOMError
from tools4caom2.mjd import utc2mjd

from jcmt2caom2.__version__ import version as jcmt2caom2version
from jcmt2caom2.caom2_tap import CAOM2TAP
from jcmt2caom2.instrument.scuba2 import scuba2_spectral_wcs
from jcmt2caom2.jsa.instrument_keywords import instrument_keywords
from jcmt2caom2.jsa.instrument_name import instrument_name
from jcmt2caom2.jsa.intent import intent
from jcmt2caom2.jsa.raw_product_id import raw_product_id
from jcmt2caom2.jsa.target_name import target_name
from jcmt2caom2.jsa.threed import ThreeD
from jcmt2caom2.jsa.twod import TwoD
from jcmt2caom2.project import get_project_pi_title, truncate_string

__doc__ = """
The raw class immplements methods to collect metadata from the database
to construct a caom2 observation.

This routine requires read access to the database, but does only reads.
It therefore always reads the metadata from SYBASE.
"""

logger = logging.getLogger(__name__)


class INGESTIBILITY(object):
    """
    Defines ingestion constants
    """
    GOOD = 0
    BAD = 1


class raw(object):
    """
    Use pyCAOM2 to ingest raw JCMT raw data for a single observation using
    metadata from the COMMON, ACSIS, SCUBA2 and FILES tables from the jcmt
    database and from the ompproj, ompuser, and ompobslog tables from the
    omp database.  This class requires direct access to a database server
    hosting copies of these tables.  The module tools4caom2.database is used
    to query the tables.

    Only read access is required inside this routine to gather the metadata and
    create the CAOM-2 xml file for the observation.

    The resulting xml file will be pushed back to the CAOM-2 repository to
    complete the put/update, and this must be separately configured.
    """

    # Allowed values for backend names in ACSIS
    BACKENDS = ['ACSIS', 'SCUBA-2', 'DAS', 'AOSC']

    # Instrumens for which we do not wish to ingest data.
    FORBIDDEN_INSTRUMENTS = ('GLT', 'GLT86', 'GLT230', 'GLT345')

    MANDATORY = ('backend',
                 'instrume',
                 'obsgeo_x',
                 'obsgeo_y',
                 'obsgeo_z',
                 'obs_type',
                 'project',
                 'release_date',
                 'sam_mode',
                 'sw_mode')

    SpeedOfLight = 299792458.0  # m/s

    def __init__(self):
        """
        Create a jcmt2caom2.raw instance to ingest a single observation.
        """

        self.collection = None
        self.obsid = None

        self.dry_run = None

        self.conn = None
        self.tap = CAOM2TAP()

        self.xmloutdir = None

    def get_proposal(self, project_id):
        """
        Get the PI name and proposal title for this project.
        """

        (proposal_pi, proposal_title) = get_project_pi_title(
            project_id, self.conn, self.tap)

        return {
            'pi': proposal_pi,
            'title': proposal_title,
        }

    def get_quality(self, obsid):
        """
        Get the JSA quality assessment for this proposal, with a default value
        of JSA_QA.GOOD if no assessment has been entered yet.

        Arguments:
        obsid: the observation identifier in COMMON for the observation

        Returns:
        a one-entry dictionary with the key 'quality' and a JSA quality
             as the value
        """

        status = self.conn.get_obsid_status(obsid)

        results = {'quality': OMPState.GOOD}
        if status is not None:
            if not OMPState.is_valid(status):
                raise CAOMError('Invalid OMP status: {0}'.format(status))
            results['quality'] = status
        logger.info('For %s state = %s from ompobslog',
                    obsid, OMPState.get_name(results['quality']))
        return results

    def check_observation(self,
                          common,
                          subsystem):
        """
        Check the validity of the metadata for a single observation

        Arguments:
        common      dictionary containing fields common to the observation
        subsystem   dictionary containing fields from ACSIS or SCUBA2

        Returns:
        INGESTIBILITY.GOOD if observation is OK
        INGESTIBILITY.BAD  if observation should be skipped
        """

        nullvalues = []
        ingestibility = INGESTIBILITY.GOOD

        # Check that mandatory fields do not have NULL values
        for field in raw.MANDATORY:
            if common[field] is None:
                nullvalues.append(field)
        if nullvalues:
            logger.warning('The following mandatory fields are NULL: %s',
                           ', '.join(sorted(nullvalues)))
            ingestibility = INGESTIBILITY.BAD

        if common['obs_type'] in ('phase', 'RAMP'):
            # do not ingest observations with bogus obs_type
            # this is not an error, but log a warning
            logger.warning(
                'Observation %s is being skipped because obs_type = %s',
                self.obsid, common['obs_type'])
            ingestibility = INGESTIBILITY.BAD

        # Check observation-level mandatory headers with restricted values
        # by creating the instrument keyword list
        keyword_dict = {}
        keyword_dict['switching_mode'] = common['sw_mode']
        if common['scan_pat']:
            keyword_dict['x_scan_pat'] = common['scan_pat']
        if common['inbeam']:
            keyword_dict['inbeam'] = common['inbeam']
        if common['backend'] in ('ACSIS', 'DAS', 'AOS-C'):
            # Although stored in ACSIS, the sideband properties belong to the
            # whole observation.  Fetch them using any subsysnr.
            subsysnr = subsystem.keys()[0]
            keyword_dict['sideband'] = subsystem[subsysnr]['obs_sb']
            keyword_dict['sideband_filter'] = subsystem[subsysnr]['sb_mode']
        someBad, keyword_list = instrument_keywords('raw',
                                                    common['instrume'],
                                                    common['backend'],
                                                    keyword_dict)

        if someBad:
            ingestibility = INGESTIBILITY.BAD
            self.instrument_keywords = []
        else:
            self.instrument_keywords = keyword_list

        return ingestibility

    def build_observation(self,
                          observation,
                          common,
                          subsystem,
                          files):
        """
        Construct a simple observation from the available metadata

        Since we are dealing with raw data, the algorithm = "exposure"
        by default, a change in notation for the JCMT.

        Arguments:
        obsid       obsid from COMMON to be used as the observationID
        common      dictionary containing fields common to the observation
        subsystem   dictionary containing fields from ACSIS or SCUBA2
        files       dictionary containing the lists of artifact filenames
        """

        collection = self.collection
        observationID = self.obsid
        logger.debug('PROGRESS: build observationID = %s', self.obsid)

        if observation is None:
            observation = SimpleObservation(collection,
                                            observationID)

        # Determine data quality metrics for this observation.
        data_quality = DataQuality(Quality.JUNK) \
            if OMPState.is_caom_junk(common['quality']) else None
        requirement_status = Requirements(Status.FAIL) \
            if OMPState.is_caom_fail(common['quality']) else None

        # "Requirements" is an observation-level attribute, so fill it in now.
        observation.requirements = requirement_status

        # Every ACSSIS and SCUBA2 observation has an obsnum in COMMON.
        observation.sequence_number = common['obsnum']

        observation.meta_release = common['release_date']

        # The observation type is derived from COMMON.obs_type and
        # COMMON.sam_mode
        if common['obs_type'] == "science":
            # raster is a synonym for scan
            if common['sam_mode'] == "raster":
                observation.obs_type = "scan"
            else:
                observation.obs_type = common['sam_mode']
        else:
            observation.obs_type = common['obs_type']

        # set the observation intent
        observation.intent = intent(common['obs_type'],
                                    common['backend'])

        proposal = Proposal(common['project'])
        if common['pi'] is not None:
            proposal.pi_name = common['pi']
        if common['survey'] is not None:
            proposal.project = common['survey']
        if common['title'] is not None:
            proposal.title = truncate_string(common['title'], 80)
        observation.proposal = proposal

        environment = Environment()
        if common['atstart'] is not None:
            make_env = True
            environment.ambient_temp = common['atstart']
        if common['elstart'] is not None:
            environment.elevation = common['elstart']
        if common['humstart'] is not None:
            if common['humstart'] < 0.0:
                environment.humidity = 0.0
            elif common['humstart'] > 100.0:
                environment.humidity = 100.0
            else:
                environment.humidity = common['humstart']
        if common['seeingst'] is not None and common['seeingst'] > 0.0:
            environment.seeing = common['seeingst']
        if common['tau225st'] is not None:
            environment.tau = common['tau225st']
            environment.wavelength_tau = raw.SpeedOfLight/225.0e9
        observation.environment = environment

        frontend = common['instrume'].upper()
        backend = common['backend'].upper()
        if common['inbeam']:
            inbeam = common['inbeam'].upper()
        else:
            inbeam = ''
        instrument = Instrument(instrument_name(frontend,
                                                backend,
                                                inbeam))
        instrument.keywords.extend(self.instrument_keywords)

        if backend in ['ACSIS', 'DAS', 'AOSC']:
            keys = sorted(subsystem.keys())
            hybrid = {}
            beamsize = 0.0
            for key in keys:
                productID = self.productID_dict[str(key)]
                # Convert restfreq from GHz to Hz
                restfreq = 1.0e9 * subsystem[key]['restfreq']
                iffreq = subsystem[key]['iffreq']
                ifchansp = subsystem[key]['ifchansp']
                if productID not in hybrid:
                    hybrid[productID] = {}
                    hybrid[productID]['restfreq'] = restfreq
                    hybrid[productID]['iffreq'] = iffreq
                    hybrid[productID]['ifchansp'] = ifchansp
                this_hybrid = hybrid[productID]

                if 'freq_sig_lower' in this_hybrid:
                    this_hybrid['freq_sig_lower'] = \
                        min(subsystem[key]['freq_sig_lower'],
                            this_hybrid['freq_sig_lower'])
                else:
                    this_hybrid['freq_sig_lower'] = \
                        subsystem[key]['freq_sig_lower']

                if 'freq_sig_upper' in this_hybrid:
                    this_hybrid['freq_sig_upper'] = \
                        max(subsystem[key]['freq_sig_upper'],
                            this_hybrid['freq_sig_upper'])
                else:
                    this_hybrid['freq_sig_upper'] = \
                        subsystem[key]['freq_sig_upper']

                if 'freq_img_lower' in this_hybrid:
                    this_hybrid['freq_img_lower'] = \
                        min(subsystem[key]['freq_img_lower'],
                            this_hybrid['freq_img_lower'])
                else:
                    this_hybrid['freq_img_lower'] = \
                        subsystem[key]['freq_img_lower']

                if 'freq_img_upper' in this_hybrid:
                    this_hybrid['freq_img_upper'] = \
                        max(subsystem[key]['freq_img_upper'],
                            this_hybrid['freq_img_upper'])
                else:
                    this_hybrid['freq_img_upper'] = \
                        subsystem[key]['freq_img_upper']

                this_hybrid['meanfreq'] = (this_hybrid['freq_sig_lower'] +
                                           this_hybrid['freq_sig_upper'])/2.0

                # Compute maximum beam size for this observation in degrees
                # frequencies are in GHz
                # The scale factor is:
                # 206264.8 ["/r] * sqrt(pi/2) * c [m GHz]/ 15 [m]
                beamsize = max(beamsize, 1.435 / this_hybrid['meanfreq'])
        else:
            # Compute beam size in degrees for 850 micron array
            # filter is in microns
            # The scale factor is:
            # pi/180 * sqrt(pi/2) * 1e-6 * lambda [um]/ 15 [m]
            beamsize = 4.787e-6 * 850.0
        observation.instrument = instrument

        if (observation.obs_type not in (
                'flatfield', 'noise', 'setup', 'skydip')
                and common['object']):
            # The target is not significant for the excluded kinds of
            # observation, even if supplied in COMMON
            if common['object']:
                targetname = target_name(common['object'])
            target = Target(targetname)

            if common['obsra'] is None or common['obsdec'] is None:
                target.moving = True
                target_position = None
            else:
                target.moving = False
                target_position = TargetPosition(Point(common['obsra'],
                                                       common['obsdec']),
                                                 'ICRS',
                                                 2000.0)
            observation.target_position = target_position

            if common['standard'] is not None:
                target.standard = True if common['standard'] else False

            if backend != 'SCUBA-2':
                subsysnr = min(subsystem.keys())
                if subsystem[subsysnr]['zsource'] is not None:
                    target.redshift = subsystem[subsysnr]['zsource']

            observation.target = target

        telescope = Telescope('JCMT')
        telescope.geo_location_x = common['obsgeo_x']
        telescope.geo_location_y = common['obsgeo_y']
        telescope.geo_location_z = common['obsgeo_z']
        observation.telescope = telescope

        # Delete any existing raw planes, since we will construct
        # new ones from scratch.  For all other planes, update the
        # "data quality" since this is a plane-level attribute.
        for productID in observation.planes:
            if productID[0:3] == 'raw':
                del observation.planes[productID]
            else:
                observation.planes[productID].quality = data_quality

        # Use key for the numeric value of subsysnr here for brevity and
        # to distinguish it from the string representation that will be
        # named subsysnr in this section
        for key in sorted(subsystem.keys()):
            productID = self.productID_dict[str(key)]
            obsid_subsysnr = subsystem[key]['obsid_subsysnr']

            logger.debug('Processing subsystem %s: %s, %s',
                         key, obsid_subsysnr, productID)

            # This plane might already have been created in a hybrid-mode
            # observation, use it if it exists
            if productID not in observation.planes:
                observation.planes.add(Plane(productID))
                plane = observation.planes[productID]

            else:
                plane = observation.planes[productID]

            # set the release dates
            plane.meta_release = common['release_date']
            plane.data_release = common['release_date']
            # all JCMT raw data is in a non-FITS format
            plane.calibration_level = CalibrationLevel.RAW_INSTRUMENT
            # set the plane data quality
            plane.quality = data_quality

            # For JCMT raw data, all artifacts have the same WCS
            for jcmt_file_id in files[obsid_subsysnr]:
                file_id = os.path.splitext(jcmt_file_id)[0]
                uri = 'ad:JCMT/' + file_id

                if observation.intent == ObservationIntentType.SCIENCE:
                    artifact_product_type = ProductType.SCIENCE
                else:
                    artifact_product_type = ProductType.CALIBRATION

                artifact = Artifact(uri, product_type=artifact_product_type,
                                    release_type=ReleaseType.DATA)

                artifact.meta_release = common['release_date']

                # There is only one part and one chunk for raw data
                artifact.parts.add(Part('0'))
                chunk = Chunk()

                artifact.meta_release = common['release_date']
                artifact.parts['0'].meta_release = common['release_date']
                chunk.meta_release = common['release_date']

                # Raw data does not have axes.
                # bounds and ranges can be specified

# Note that for single spectra the bl and tr corners have the same
# coordinates.  CAOM-2 does not accept a zero-area polygon, so pad the
# coordinates by the +/- 0.5 * beamsize.
# Also, a line map in X or Y grid coordinates will have zero area,
# so expand the box sideways by +/- 0.5 * beamsize.
# Finally, check for a bowtie polygon, where the corners were recorded
# in the wrong order.
                if (common['obs_type'] in ('science', 'pointing', 'focus')
                        and common['obsrabl'] is not None):
                    # Sky position makes no sense for other kinds of
                    # observations, even if supplied in COMMON

                    # Position axis bounds are in ICRS
                    # Check for various pathologies due to different
                    # observing strategies
                    # position accuracy is about 0.1 arcsec (in decimal
                    # degrees)
                    eps = 0.1 / 3600.0

                    bl = TwoD(common['obsrabl'], common['obsdecbl'])
                    br = TwoD(common['obsrabr'], common['obsdecbr'])
                    tl = TwoD(common['obsratl'], common['obsdectl'])
                    tr = TwoD(common['obsratr'], common['obsdectr'])
                    logger.info('initial bounds bl = %s', bl)
                    logger.info('initial bounds br = %s', br)
                    logger.info('initial bounds tr = %s', tr)
                    logger.info('initial bounds tl = %s', tl)
                    halfbeam = beamsize / 2.0

                    # The precomputed bounding box can be represented as a
                    # polgon
                    if ((bl - br).abs() < eps
                            and (bl - tl).abs() < eps
                            and (tl - tr).abs() < eps):
                        # bounding "box" is a point, so expand to a box
                        logger.info(
                            'For observation %s the bounds are a point',
                            common['obsid'])

                        cosdec = math.cos(br.y * math.pi / 180.0)
                        offsetX = 0.5 * beamsize / cosdec
                        offsetY = 0.5 * beamsize
                        bl = bl + TwoD(-offsetX, -offsetY)
                        br = br + TwoD(offsetX, -offsetY)
                        tr = tr + TwoD(offsetX, offsetY)
                        tl = tl + TwoD(-offsetX, offsetY)

                    elif ((bl - br).abs() < eps
                          and (tl - tr).abs() < eps
                          and (bl - tl).abs() >= eps):
                        # bounding box is a line in y, so diff points to + Y
                        # and the perpendicular points along - X
                        logger.info(
                            'For observation %s the bounds are in a line in Y',
                            common['obsid'])

                        diff = tl - bl
                        mean = (tl + bl)/2.0
                        cosdec = math.cos(mean.y * math.pi / 180.0)

                        unitX = TwoD(diff.y, -diff.x * cosdec)
                        unitX = unitX / unitX.abs()
                        offsetX = -halfbeam * TwoD(unitX.x / cosdec, unitX.y)

                        unitY = TwoD(diff.x * cosdec, diff.y)
                        unitY = unitY / unitY.abs()
                        offsetY = halfbeam * TwoD(unitY.x / cosdec, unitY.y)

                        bl = bl - offsetX - offsetY
                        tl = tl - offsetX + offsetY
                        br = br + offsetX - offsetY
                        tr = tr + offsetX + offsetY

                    elif ((bl - tl).abs() < eps
                          and (br - tr).abs() < eps
                          and (bl - br).abs() >= eps):
                        # bounding box is a line in x
                        logger.info(
                            'For observation %s the bounds are in a line in X',
                            common['obsid'])

                        diff = br - bl
                        mean = (br + bl)/2.0
                        cosdec = math.cos(mean.y * math.pi / 180.0)

                        unitX = TwoD(diff.x * cosdec, diff.y)
                        unitX = unitX / unitX.abs()
                        offsetX = halfbeam * TwoD(unitX.x / cosdec, unitX.y)

                        unitY = TwoD(diff.y, -diff.x * cosdec)
                        unitY = unitY / unitY.abs()
                        offsetY = halfbeam * TwoD(unitY.x / cosdec, unitY.y)

                        bl = bl - offsetX - offsetY
                        tl = tl - offsetX + offsetY
                        br = br + offsetX - offsetY
                        tr = tr + offsetX + offsetY

                    else:
                        # Get here only if the box is not degenerate
                        bl3d = ThreeD(bl)
                        br3d = ThreeD(br)
                        tr3d = ThreeD(tr)
                        tl3d = ThreeD(tl)

                        try:
                            sign1 = math.copysign(
                                1, ThreeD.included_angle(br3d, bl3d, tl3d))
                            sign2 = math.copysign(
                                1, ThreeD.included_angle(tr3d, br3d, bl3d))
                            sign3 = math.copysign(
                                1, ThreeD.included_angle(tl3d, tr3d, br3d))
                            sign4 = math.copysign(
                                1, ThreeD.included_angle(bl3d, tl3d, tr3d))
                        except ValueError as e:
                            raise CAOMError('The bounding box for obsid = ' +
                                            self.obsid + ' is degenerate')

                        # If the signs are not all the same, the vertices
                        # were recorded in a bowtie order.  Swap any two.
                        if (sign1 != sign2 or sign2 != sign3 or
                                sign3 != sign4):
                            logger.warning(
                                'For observation %s the bounds are in a'
                                ' bowtie order',
                                common['obsid'])
                            bl.swap(br)

                    logger.info('final bounds bl = ' + str(bl))
                    logger.info('final bounds br = ' + str(br))
                    logger.info('final bounds tr = ' + str(tr))
                    logger.info('final bounds tl = ' + str(tl))
                    bounding_box = CoordPolygon2D()
                    bounding_box.vertices.append(ValueCoord2D(bl.x, bl.y))
                    bounding_box.vertices.append(ValueCoord2D(br.x, br.y))
                    bounding_box.vertices.append(ValueCoord2D(tr.x, tr.y))
                    bounding_box.vertices.append(ValueCoord2D(tl.x, tl.y))

                    spatial_axes = CoordAxis2D(Axis('RA', 'deg'),
                                               Axis('DEC', 'deg'))
                    spatial_axes.bounds = bounding_box

                    chunk.position = SpatialWCS(spatial_axes)
                    chunk.position.coordsys = 'ICRS'
                    chunk.position.equinox = 2000.0

                # energy range, which can contain two subranges in DSB
                if backend == 'SCUBA-2':
                    spectral_axis = scuba2_spectral_wcs(subsystem[key])

                else:
                    this_hybrid = hybrid[productID]

                    energy_axis = CoordAxis1D(Axis('FREQ', 'GHz'))
                    if subsystem[key]['sb_mode'] == 'DSB':
                        # These all correspond to "pixel" 1, so the pixel
                        # coordinate runs from [0.5, 1.5]
                        # Note that each artifact already records the frequency
                        # bounds correctly for that data in that file.  The
                        # aggregation to the plane will take care of
                        # overlapping energy bounds.
                        freq_bounds = CoordBounds1D()
                        freq_bounds.samples.append(CoordRange1D(
                            RefCoord(0.5, subsystem[key]['freq_sig_lower']),
                            RefCoord(1.5, subsystem[key]['freq_sig_upper'])))
                        freq_bounds.samples.append(CoordRange1D(
                            RefCoord(0.5, subsystem[key]['freq_img_lower']),
                            RefCoord(1.5, subsystem[key]['freq_img_upper'])))
                        energy_axis.bounds = freq_bounds
                    else:
                        energy_axis.range = CoordRange1D(
                            RefCoord(0.5, subsystem[key]['freq_sig_lower']),
                            RefCoord(1.5, subsystem[key]['freq_sig_upper']))

                    spectral_axis = SpectralWCS(energy_axis, 'BARYCENT')
                    spectral_axis.ssysobs = subsystem[key]['ssysobs']
                    spectral_axis.ssyssrc = subsystem[key]['ssyssrc']
                    spectral_axis.zsource = subsystem[key]['zsource']

                    # Recall that restfreq has been converted to Hz in
                    # thishybrid so do not use the unconverted value from
                    # subsystem[key][['restfreq']
                    spectral_axis.restfrq = this_hybrid['restfreq']
                    meanfreq = float(this_hybrid['meanfreq'])
                    ifchansp = float(this_hybrid['ifchansp'])
                    spectral_axis.resolving_power = abs(1.0e9 * meanfreq /
                                                        ifchansp)

                    spectral_axis.transition = EnergyTransition(
                        subsystem[key]['molecule'],
                        subsystem[key]['transiti'])

                chunk.energy = spectral_axis

                # time range
                time_axis = CoordAxis1D(Axis('TIME', 'd'))
                mjdstart = utc2mjd(common['date_obs'])
                mjdend = utc2mjd(common['date_end'])
                time_axis.range = CoordRange1D(
                    RefCoord(0.5, mjdstart),
                    RefCoord(1.5, mjdend))

                chunk.time = TemporalWCS(time_axis)
                chunk.time.timesys = 'UTC'
                chunk.time.exposure = \
                    (common['date_end'] - common['date_obs']).total_seconds()

                # Chunk is done, so append it to the part
                artifact.parts['0'].chunks.append(chunk)

                # and append the atrifact to the plane
                plane.artifacts.add(artifact)

        return observation

    def ingest(self):
        """
        Do the ingestion.
        First do all the checks,
        then build the caom2 structure,
        and persist to an xml file that is sent to the repository.

        Arguments:
        <none>
        """

        # Check that this is a valid observation and
        # get the dictionary of common metadata
        common = self.conn.query_table('COMMON', self.obsid)
        if len(common):
            common = common[0]
        else:
            raise CAOMError('There is no observation with '
                            'obsid = %s' % (self.obsid,))

        # There are some instruments we wish to reject immediately.
        instrument = common['instrume'].upper()
        if instrument in self.FORBIDDEN_INSTRUMENTS:
            raise CAOMError('Forbidden instrument: %s', instrument)

        # Append the proposal metadata
        proposal = self.get_proposal(common['project'])
        if proposal:
            common.update(proposal)

        # Append the quality assessment
        quality = self.get_quality(self.obsid)
        if quality:
            common.update(quality)

        # get a list of rows for the subsystems in this observation
        backend = common['backend']
        if backend in ['ACSIS', 'DAS', 'AOSC']:
            subsystemlist = self.conn.query_table('ACSIS', self.obsid)
            # Convert the list of rows into a dictionary
            subsystem = {}
            for row in subsystemlist:
                subsysnr = row.pop('subsysnr')
                subsystem[subsysnr] = row

        elif backend == 'SCUBA-2':
            subsystemlist = self.conn.query_table('SCUBA2', self.obsid)
            # Convert the list of rows into a dictionary
            subsystem = {}
            for row in subsystemlist:
                subsysnr = int(row['filter'])
                subsystem[subsysnr] = row

        else:
            logger.warning(
                'backend = "%s" is not one of '
                '["ACSIS", "DAS", "AOSC", "SCUBA", "SCUBA-2"]',
                backend)

        # somewhat repetitive, but custom SQL is useful
        # get dictionary of productID's for each subsystem
        self.productID_dict = raw_product_id(backend,
                                             self.obsid,
                                             self.conn)
        logger.debug('query complete')

        ingestibility = self.check_observation(common, subsystem)
        if ingestibility == INGESTIBILITY.BAD:
            logger.error('SERIOUS ERRORS were found in %s', self.obsid)
            raise CAOMError('Serious errors found')

        repository = Repository()

        uri = 'caom:' + self.collection + '/' + common['obsid']
        # get the list of files for this observation
        files = self.conn.get_files(self.obsid)
        if files is None:
            raise CAOMError('No rows in FILES for obsid = ' + self.obsid)

        with repository.process(uri, dry_run=self.dry_run) as wrapper:
            wrapper.observation = self.build_observation(
                wrapper.observation, common, subsystem, files)

            if self.xmloutdir:
                with open(os.path.join(self.xmloutdir, re.sub(
                        '[^-_A-Za-z0-9]', '_', self.obsid)) + '.xml',
                        'wb') as f:
                    repository.writer.write(wrapper.observation, f)

        logger.info('SUCCESS: Observation %s has been ingested',
                    self.obsid)

    def run(self):
        """
        Fetch metadata, build a CAOM-2 object, and push it into the repository.

        Returns True on success, False otherwise.
        """

        ap = argparse.ArgumentParser()

        ap.add_argument(
            '--obsid',
            required=True,
            help='obsid, primary key in COMMON table')

        ap.add_argument(
            '--collection',
            choices=('JCMT', 'SANDBOX'),
            default='JCMT',
            help='collection to use for ingestion')

        ap.add_argument(
            '--dry-run', '-n',
            action='store_true',
            dest='dry_run',
            help='Check the validity of metadata for this'
                 ' observation and file, then exit')
        ap.add_argument(
            '--verbose', '-v',
            dest='loglevel',
            action='store_const',
            const=logging.DEBUG)
        ap.add_argument(
            '--xmloutdir',
            help='directory into which to write XML files')

        args = ap.parse_args()

        if args.collection:
            self.collection = args.collection

        self.obsid = args.obsid

        if args.loglevel:
            logging.getLogger().setLevel(args.loglevel)

        self.dry_run = args.dry_run

        self.xmloutdir = args.xmloutdir

        logger.info(sys.argv[0])
        logger.info('jcmt2caom2version    = %s', jcmt2caom2version)
        logger.info('tools4caom2version   = %s', tools4caom2version)
        logger.info('obsid                = %s', self.obsid)
        logger.info('dry run              = %s', self.dry_run)

        try:
            self.conn = ArcDB()

            self.ingest()

            logger.info('DONE')

        except:
            logger.exception('Error during ingestion')
            return False

        finally:
            self.conn.close()

        return True
