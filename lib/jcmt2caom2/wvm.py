# Copyright (C) 2020-2023 East Asian Observatory
# All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful,but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc.,51 Franklin
# Street, Fifth Floor, Boston, MA  02110-1301, USA

from collections import namedtuple
from datetime import datetime, timedelta
import logging
import os
import re

from tools4caom2.tapclient import tapclient_luskan
from tools4caom2.artifact_uri import extract_artifact_uri_filename, \
    make_artifact_uri

from jcmt2caom2.md5sum import get_md5sum

WVMFileInfo = namedtuple('WVMFileInfo', ('path', 'name', 'size', 'md5sum'))

pattern_date = re.compile(r'^\d{8}$')
pattern_wvm_dir = re.compile(r'^\d{8}$')
pattern_wvm_file = re.compile(r'^(\d{8})\.wvm$')

logger = logging.getLogger(__name__)


def find_wvm_files(
        base_wvm_dir, date_start, date_end,
        with_size=False, with_md5sum=False):
    result = []

    logger.debug('Finding WVM files on disk')

    for dir_ in sorted(os.listdir(base_wvm_dir)):
        if not pattern_wvm_dir.search(dir_):
            continue

        if (date_start is not None) and (dir_ < date_start):
            continue

        if (date_end is not None) and (dir_ > date_end):
            continue

        path = os.path.join(base_wvm_dir, dir_)

        for file_ in sorted(os.listdir(path)):
            if not pattern_wvm_file.search(file_):
                continue

            filepath = os.path.join(path, file_)

            result.append(WVMFileInfo(
                path, file_,
                (os.stat(filepath).st_size if with_size else None),
                (get_md5sum(filepath) if with_md5sum else None)))

    return result


def get_archive_wvm_files(months):
    cadc_files = {}
    luskan = tapclient_luskan()

    logger.debug('Querying luskan TAP for WVM files')

    for month in months:
        for (uri, cadc_size, cadc_md5sum) in luskan.query(
                'SELECT uri, contentLength, contentChecksum '
                'FROM inventory.Artifact '
                'WHERE uri LIKE \'{}\''.format(make_artifact_uri(
                    '{}%.wvm'.format(month), archive='JCMT')),
                timeout=600):
            file_ = extract_artifact_uri_filename(uri, archive='JCMT')

            if cadc_md5sum.startswith('md5:'):
                cadc_md5sum = cadc_md5sum[4:]
            else:
                raise Exception('Unexpected contentChecksum format')

            cadc_files[file_] = WVMFileInfo(
                path=None, name=file_, size=cadc_size, md5sum=cadc_md5sum)

    return cadc_files


def make_months(date_start, date_end):
    """Construct a list of month prefixes for querying."""

    date = datetime.strptime(date_start, '%Y%m%d')
    if date_end is None:
        date_end = datetime.utcnow()
    else:
        date_end = datetime.strptime(date_end, '%Y%m%d')

    months = set()
    while date <= date_end:
        months.add(date.strftime('%Y%m'))
        date = date + timedelta(days=1)

    return sorted(months)
