# Copyright (C) 2015 East Asian Observatory
# All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc.,51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA

# Author: SF Graves

import logging

from tools4caom2.caom2repo_wrapper import Repository
from tools4caom2.tapclient import tapclient

"""
This file contains miscellaneous functions for adjusting CAOM records.

Functions in thhis class are likely to be a slight hodge podge of
functions used by various scripts/one of programmes for fixing
up information in CAOM2 when changes are made.
"""

logger = logging.getLogger()


def remove_planes(productID, obsids, collection='JCMT',
                  dry_run=False, allow_remove=False):
    """

    Removes planes with given productID from a list of observations.

    args:
      productID (str): exact name of productIDs to be removed from CAOM2.

      obsids (list): list of observation IDs from which to remove planes.

      collection (opt, str): collection to remove planes from
      (defaults to JCMT)

      dry_run (opt, Bool): if True, then don't actually remove anything.

      allow_remove (opt, Bool): If True, allow removal of observations with
      no planes remaining.

    """

    logger.info('Removing planes with productID=%s', productID)
    repository = Repository()

    for obsid in obsids:
        logger.info('Attempting to remove plane from %s', obsid)
        uri = 'caom:' + collection + '/' + obsid
        with repository.process(
                uri, dry_run=dry_run, allow_remove=True) as wrapper:
            observation = wrapper.observation
            if observation and (productID in observation.planes):
                try:
                    del observation.planes[productID]
                except:
                    logger.exception('Cannot remove plane from  %s', uri)
                    raise

            elif not observation:
                logger.warning(
                    'Observation %s does not exist in CAOM DB', obsid)
            else:
                logger.warning(
                    'Observation %s has no %s plane to remove',
                    obsid, productID)

            if dry_run:
                logger.debug(
                    'No planes will be removed from %s as DRY RUN is enabled',
                    obsid)

        logger.info('Finished with observation %s', obsid)


def set_release_date(productID, obsids, releasedate, collection='JCMT',
                     dry_run=False):
    """
    Set the release date of given productIDs contained in a list of obsids.

    productID: string, productID at CADC.
    obsids: list of observationIDs at CADc.
    releasedate: datetime object


    """
    logger.info('Setting new releasedate to be %s.' % (str(releasedate)))
    logger.info('Updating releasedates for all planes in given observations'
                ' with productid=%s' % productID)
    if dry_run:
        logger.warning('DRY RUN mode enabled: CAOM will not be updated')
    repository = Repository()

    for obsid in obsids:
        logger.info('Attempting to update plane from %s', obsid)
        uri = 'caom:' + collection + '/' + obsid
        with repository.process(
                uri, dry_run=dry_run, allow_remove=True) as wrapper:
            observation = wrapper.observation
            if observation and (productID in observation.planes):
                plane = observation.planes[productID]
                plane.data_release = releasedate
                plane.meta_release = releasedate

            elif not observation:
                logger.warning(
                    'Observation %s does not exist in CAOM DB', obsid)
            else:
                logger.warning(
                    'Observation %s has no %s plane to remove',
                    obsid, productID)

            if dry_run:
                logger.debug(
                    'observation %s not updated as DRY RUN mode enabled',
                    obsid)

            else:
                logger.info(
                    'obsid: %s, plane: %s has a releasedate of %s',
                    obsid, productID, str(releasedate))
