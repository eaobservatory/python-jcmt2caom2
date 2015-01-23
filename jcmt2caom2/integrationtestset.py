#!/usr/bin/env python2.7

__author__ = "Russell O. Redman"

import argparse
import logging
import os.path
import re
import subprocess
import sys
import urllib2

from caom2.xml.caom2_observation_reader import ObservationReader
from caom2.xml.caom2_observation_writer import ObservationWriter

from tools4caom2.caom2repo_wrapper import Repository
from tools4caom2.logger import logger

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version


__doc__ = """
The integration Test Set is a selection of observations that illustrate some
aspect of the JCMT ingestion process.  It is important to be able to ingest
without error every memeber on the integration test set to demonstrate the
correct behaviour on the ingestion software.

The members of the test set are documented at:
https://wiki.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/caom2/index.php/IntegrationTestSet
"""


class integrationtestset(object):
    """
    Use jsaraw and jsaingest to ingest the integration test set
    into the SANDBOX collection and/or caom2repo.py to remove the the test
    set observations from the SANDBOX.

    Optionally, use pyCAOM2 to modify productID values in place.
    """

    def __init__(self):
        """
        Initialize the integration test set processor
        """
        # get xml file reader and writer, to allow insertion of the
        # time structures for chunks with WCS
        self.reader = ObservationReader(True)
        self.writer = ObservationWriter()
        self.logfile = None
        self.log = None
        self.loglevel = logging.INFO
        self.outdir = None
        self.rawlist = []
        self.proclist = []
        self.cleanlist = []
        self.keeplog = False
        self.debug = False
        self.args = None

        self.testset = {}

    def parse_command_line(self):
        """
        Define and pardse command line switches
        """
        ap = argparse.ArgumentParser()
        # logging options
        ap.add_argument('--log',
                        default='integrationtestset.log',
                        help='log file for integrationtestset')
        ap.add_argument(
            '--keeplog',
            action='store_true',
            help='(optional) keep log if successful (default is to delete)')
        ap.add_argument(
            '--debug',
            action='store_true',
            help='(optional) show all messages, pass --debug to fits2caom2,'
            ' and retain all xml and override files')

        ap.add_argument('--outdir',
                        default='.',
                        help='directory to be used for working files')

        ap.add_argument('--noraw',
                        action='store_false',
                        dest='raw',
                        help='skip raw reductions')
        ap.add_argument('--noproc',
                        action='store_false',
                        dest='proc',
                        help='skip raw reductions')
        ap.add_argument('--decorate',
                        action='store_true',
                        help='append -new to every productID')
        ap.add_argument('--skip',
                        action='store_true',
                        help='skip ingestion: implies --clean')
        ap.add_argument('--clean',
                        action='store_true',
                        help='remove observations from SANDBOX when done')

        ap.add_argument(
            'input',
            nargs='*',
            help='file(s) or container(s) to ingest')
        self.args = ap.parse_args()

        self.logfile = os.path.expanduser(
            os.path.expandvars(self.args.log))
        if self.args.keeplog:
            self.keeplog = self.args.keeplog
        if self.args.debug:
            self.debug = self.args.keeplog
            self.loglevel = logging.DEBUG

        self.outdir = os.path.abspath(self.args.outdir)

    def read_integrationtestset(self):
        """
        Read the integration test set from the wiki page
        """
        ITS = urllib2.urlopen(
            'https://wiki.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/caom2/index.php/'
            'IntegrationTestSet')
        title = ''
        criterion = 0

        for line in ITS:
            mt = re.match(r'^<h3>\s*<span[^>]*>([^<]+)(<.*)?$', line)
            if mt:
                title = mt.group(1)
            if title not in self.testset:
                self.testset[title] = {}
                self.log.console('TITLE = ' + title)
                criterion = 0

            mc = re.match(r'^[^#]*# CRITERIA:\s+(.*)$', line)
            if mc:
                criterion += 1
                self.log.console('CRITERION: %d: %s' % (criterion,
                                                        mc.group(1)))
                self.testset[title][criterion] = {}
                self.testset[title][criterion]['raw'] = []
                self.testset[title][criterion]['proc'] = []
                self.testset[title][criterion]['clean'] = []

            mr = re.match(r'^.*jsaraw.*--key=(\S+)\s*$', line)
            if mr:
                self.testset[title][criterion]['raw'].append(mr.group(1))
                self.log.file('  raw: ' + mr.group(1),
                              logging.DEBUG)

            mp = re.match(r'^.*jsaingest.*dp:(\S+)([#\s].*)$', line)
            if mp:
                self.testset[title][criterion]['proc'].append(mp.group(1))
                self.log.file(' proc: ' + mp.group(1),
                              logging.DEBUG)

            md = re.match(r'^.*caom2repo.*SANDBOX/(\S+)([#\s].*)$', line)
            if md:
                self.testset[title][criterion]['clean'].append(md.group(1))
                self.log.file('clean: ' + md.group(1),
                              logging.DEBUG)

    def log_command_line(self):
        """
        write startup configuration into the log
        """
        self.log.file(sys.argv[0])
        self.log.file('jcmt2caom2version    = ' + jcmt2caom2version)
        self.log.file('tools4caom2version   = ' + tools4caom2version)
        self.log.console('logfile = ' + self.logfile)
        self.log.file('keeplog = ' + str(self.args.keeplog))
        self.log.file('outdir = ' + self.outdir)
        self.log.file('debug = ' + str(self.args.debug))
        self.log.file('skip = ' + str(self.args.skip))
        self.log.file('clean = ' + str(self.args.clean))
        self.read_integrationtestset()
        for item in self.args.input:
            title, criterionstr = re.split(r':', item)
            criterion = int(criterionstr)
            if title in self.testset and criterion in self.testset[title]:
                self.rawlist.extend(self.testset[title][criterion]['raw'])
                self.proclist.extend(self.testset[title][criterion]['proc'])
                self.cleanlist.extend(self.testset[title][criterion]['clean'])
        for raw in self.rawlist:
            self.log.file('raw: ' + raw)
        for proc in self.proclist:
            self.log.file('proc: ' + proc)
        for clean in self.cleanlist:
            self.log.file('clean: ' + clean)

        if self.args.debug:
            self.repository = Repository(self.outdir, self.log)
        else:
            self.repository = Repository(self.outdir, self.log, debug=False)

    def ingest_raw(self):
        """
        Ingest the set of raw observations into SANDBOX using jsaraw
        """
        if self.args.raw:
            rawcmd = os.path.join(sys.path[0], 'jsaraw')
            rawcmd += ' --outdir=' + self.outdir
            rawcmd += ' --collection=SANDBOX'
            for raw in self.rawlist:
                cmd = rawcmd + ' --key=' + raw
                self.log.console(cmd)
                try:
                    output = subprocess.check_output(cmd,
                                                     shell=True,
                                                     stderr=subprocess.STDOUT)
                    self.log.file(output)
                except subprocess.CalledProcessError as e:
                    self.log.console('FAILED: ' + cmd)
                    self.log.file('FAILED: ' + e.output)

    def ingest_proc(self):
        """
        ingest the set of recipe instances into SANDBOX using jsaingest
        """
        if self.args.proc:
            proccmd = os.path.join(sys.path[0], 'jsaingest')
            proccmd += ' --outdir=' + self.outdir
            proccmd += ' --collection=SANDBOX'
            if self.args.debug:
                proccmd += ' --debug'
            elif self.args.keeplog:
                proccmd += ' --keeplog'

            for proc in self.proclist:
                cmd = proccmd + ' dp:' + proc
                self.log.console(cmd)
                try:
                    output = subprocess.check_output(cmd,
                                                     shell=True,
                                                     stderr=subprocess.STDOUT)
                    self.log.file(output)
                except subprocess.CalledProcessError as e:
                    self.log.console('FAILED: ' + cmd)
                    self.log.file('FAILED: ' + e.output)

    def decorate(self):
        """
        Modify the observations in SANDBOX as requested
        """
        for clean in self.cleanlist:
            uri = 'caom:SANDBOX/' + clean
            self.log.console('DECORATE: ' + uri)
            with self.repository.process(uri) as xmlfile:
                orig_xmlfile = xmlfile
                observation = None
                if os.path.exists(xmlfile):
                    observation = self.reader.read(xmlfile)

                for productID in observation.planes.keys():
                    plane = observation.planes[productID]
                    del observation.planes[productID]

                    new_productID = productID + '-new'
                    self.log.file('DECORATE: ' + productID + ' -> ' +
                                  new_productID)
                    plane.product_id = new_productID
                    observation.planes.add(plane)

                with open(xmlfile, 'w') as XMLFILE:
                    self.writer.write(observation, XMLFILE)

    def cleanup(self):
        """
        Remove from the SANDBOX all the observations in the requested set
        """
        for clean in self.cleanlist:
            cmd = 'caom2repo.py -r caom:SANDBOX/' + clean
            self.log.console(cmd)
            try:
                output = subprocess.check_output(cmd,
                                                 shell=True,
                                                 stderr=subprocess.STDOUT)
                self.log.file(cmd)
                self.log.file(output)
            except subprocess.CalledProcessError as e:
                self.log.console('FAILED: ' + cmd)
                self.log.file('FAILED: ' + e.output)

    def run(self):
        """
        Fetch metadata, build a CAOM-2 object, and push it into the repository
        """
        self.parse_command_line()
        with logger(self.logfile,
                    loglevel=self.loglevel).record() as self.log:
            self.log_command_line()
            if not self.args.skip:
                self.ingest_raw()
                self.ingest_proc()
                if self.args.decorate:
                    self.decorate()
            if self.args.clean or self.args.skip:
                self.cleanup()
