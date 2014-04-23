#!/usr/bin/env python2.7

import argparse
import commands
import datetime
import logging
import os.path
import re
import stat
import string

from tools4caom2.config import config
from tools4caom2.logger import logger
from tools4caom2.database import database
from tools4caom2.database import connection
from tools4caom2.gridengine import gridengine

from tools4caom2.__version__ import version as tools4caom2version
from jcmt2caom2.__version__ import version as jcmt2caom2version

def runcommand(identity_instance_id,
               basedir,
               qsub,
               debug,
               test,
               log,
               keeplog,
               mygridengine):
    """
    Format the command to ingest one identity_instance_id and
    either run directly or submit to gridengine.
    
    Aruments:
    identity_instance_id: primary key in dp_recipe_instance, as a string
    basedir: directory to hold csh and log files
    debug: run in debug mode if True, otherwise use verbose
    qsub: submit to gridengine if True
    log: an open tools4caom2.logger instance
    """
    cshdir = os.path.abspath(basedir)
    suffix = re.sub(r':', '-', datetime.datetime.utcnow().isoformat())
    
    rootfile = os.path.join(cshdir, '_'.join(['proc', 
                                              identity_instance_id, 
                                              suffix]))
    logfile = rootfile + '.log'
    
    proccmd = 'jcmt2caom2proc'
    proccmd += ' --outdir=${TMPDIR}'
    if debug:
        proccmd += ' --debug'
    if keeplog:
        proccmd += ' --keeplog'
    proccmd += ' --log=' + logfile
    proccmd += ' dp:' + identity_instance_id
    
    log.console('PROGRESS: "%s"' % (proccmd,))
    if qsub:
        cshfile = rootfile + '.csh'
        if not test:
            mygridengine.submit(proccmd, cshfile, logfile)

    else:
        if not test:
            status, output = commands.getstatusoutput(proccmd)
            if status:
                log.console(output,
                            logging.WARN)

        
def run():
    """
    The run() method for jcmtprocwrap.

    
    A range of id's can be entered one at a time, or as a range first-last, 
    where first defaults to 10894 and last defaults to the current maximum 
    identity_instance_id. Thus, the default range if nothing is specified 
    is 10895-, implying all currently valid recipe instances.

    Examples:
    jcmtprocwrap --debug 10895-10900
    jcmtprocwrap \
        12707 12744 12784 12795 12817 14782 14784 30550 30594 \
        30605 30736 30739 30745 30748 30863 31192 32084 32971
    jcmtprocwrap --qsub --algorithm=project
    jcmtprocwrap --qsub --backend=SCUBA-2 --existing
    """
    userconfig = None
    userconfigpath = '~/.tools4caom2/jcmt2caom2.config'

    ap = argparse.ArgumentParser('recipe_ad')
    ap.add_argument('--userconfig',
                    default=userconfigpath,
                    help='Optional user configuration file '
                    '(default=' + userconfigpath + ')')
    
    ap.add_argument('--log',
                    default='procrecipe.log',
                    help='(optional) name of log file')
    ap.add_argument('--keeplog',
                    action='store_true',
                    help='Pass --keeplog switch to jcmt2caom2proc')
    
    ap.add_argument('--qsub',
                    action='store_true',
                    help='rsubmit a job to gridengine for each recipe instance')
    ap.add_argument('--queue',
                    default='cadcproc',
                    help='gridengine queue to use if --qsub is set')
    ap.add_argument('--test',
                    action='store_true',
                    help='do not submit to gridengine or run commnands')
    # verbosity
    ap.add_argument('--debug', '-d',
                    action='store_true',
                    help='run ingestion commands in debug mode')
    # subset selection:
    # -- by date_processed
    ap.add_argument('--before',
                    help='insert a clause process_date < before')
    ap.add_argument('--after',
                    help='add a clause process_date >= after')
    # -- by backend
    ap.add_argument('--backend',
                    choices=['ACSIS',
                             'SCUBA-2',
                             'DAS'],
                    help='ingest only instances with inputs from backend')
    # -- by presence in CAOM-2
    ap.add_argument('--existing',
                    action='store_true',
                    help='ingest only if already present')
    ap.add_argument('--force',
                    action='store_true',
                    help='ingest even if already present')
    # -- by grouping algorithm
    ap.add_argument('--algorithm',
                    choices=['obs',
                             'night',
                             'project'],
                    help='ingest only instances with mode=algorithm')
    # -- by identity_instance_id range(s)
    ap.add_argument('id',
                    nargs='*',
                    help='list of identity_instance_id values or ranges')
    a = ap.parse_args()
    
    userconfig = config(a.userconfig)
    userconfig['server'] = 'SYBASE'
    userconfig['caom_db'] = 'jcmt'
    userconfig.read()
            
    log = logger(a.log, logging.INFO, True)
    log.file('jcmt2caom2version    = ' + jcmt2caom2version)
    log.file('tools4caom2version   = ' + tools4caom2version)
    for attr in dir(a):
        if attr != 'id' and attr[0] != '_':
            log.console('%-15s= %s' % (attr, getattr(a, attr)))

    mygridengine = gridengine(log, queue=a.queue)
    
    basedir = os.path.dirname(
                os.path.abspath(
                    os.path.expanduser(
                        os.path.expandvars(a.log))))
    idlist = []
    if a.id:
        for item in a.id:
            m = re.match(r'^(\d*)(-(\d*))?$', item)
            if m:
                g = m.groups()
                start = '10895'
                if g[0]:
                    start = g[0]

                end = None
                if g[1]:
                    if g[2]:
                        end = g[2]
                else:
                    end = start
                if start == end:
                    idlist.append(start)
                else:
                    idlist.append((start, end))
                
    else:
        idlist.append(('10895', None))
            
    log.console('%-15s= %s' % ('id', str(idlist)))

    retvals = None
    with connection(userconfig, log) as db:    
        selectclauses = ['SELECT convert(char(26), dri.identity_instance_id),'
                         '       count(cp.planeID)']
        fromclauses = ['FROM data_proc.dbo.dp_recipe_instance dri',
                       '    INNER JOIN data_proc.dbo.dp_recipe dr',
                       '        ON dri.recipe_id = dr.recipe_id',
                       '            AND dr.script_name="jsawrapdr"',
                       '    LEFT JOIN data_proc.dbo.dp_recipe_output dro',
                       '        ON dri.identity_instance_id=dro.identity_instance_id',
                       '            AND dro.dp_output like "%fits"',
                       '    LEFT JOIN jcmt.dbo.caom2_Plane cp',
                       '        ON dri.identity_instance_id = hextobigint(cp.provenance_runID)']
        if a.backend:
            fromclauses.extend([
                '    INNER JOIN jcmtmd.dbo.FILES f',
                '        on "ad:JCMT/"+f.file_id=dfi.dp_input+".sdf"',
                '    INNER JOIN jcmtmd.dbo.COMMON c',
                '        c.obsid=f.obsid'])
                
        whereclauses = ['WHERE dr.script_name = "jsawrapdr"',
                        '    dri.state="Y"']
        if a.before:
            whereclauses.append('    dri.date_processed < "' + a.before + '"')
        if a.after:
            whereclauses.append('    dri.date_processed >= "' + a.after + '"')
        
        if a.backend:
            whereclauses.append('    c.backend = "%s"' % (a.backend, ))

        if a.algorithm:
            whereclauses.append('    dr.parameters like "-mode="' +
                                "'" + a.algorithm + "'%")
        
        finalclauses = ['GROUP BY dri.identity_instance_id',
                        'HAVING count(dro.dp_output) > 0']
        if not a.force:
            finalclauses.append('    AND count(cp.planeID) = 0')
        elif a.existing:
            finalclauses.append('    AND count(cp.planeID) > 0')
        
        finalclauses.append('ORDER BY dri.identity_instance_id')       

        for id in idlist:
            if isinstance(id, str):
                whereclauses.append('    dri.identity_instance_id = ' + id)
            else:
                whereclauses.append('    dri.identity_instance_id >= ' + id[0])
                if id[1]:
                    whereclauses.append('    dri.identity_instance_id <= ' + id[1])
            
            sqlcmd = '\n'.join(selectclauses +
                               fromclauses +
                               [' AND\n'.join(whereclauses)] +
                               finalclauses)

            retvals = db.read(sqlcmd)
            log.console('count(identity_instance_id) = %d' %(len(retvals),))
        
            if retvals:
                dirnum = 0
                usedir = basedir
                if len(retvals) > 1000:
                    dirnum = 1
                    filecount = 0
                for idstr, count in retvals:
                    identity_instance_id = string.strip(idstr)
                    if a.force or count == 0:
                        if dirnum:
                            if filecount >= 1000:
                                filecount = 0
                                dirnum += 1
                            else:
                                filecount += 1
                            usedir = basedir + '/' + str(dirnum)
                            if not os.path.exists(usedir):
                                os.makedirs(usedir)
                        runcommand(identity_instance_id,
                                   usedir,
                                   a.qsub,
                                   a.debug,
                                   a.test,
                                   log,
                                   a.keeplog,
                                   mygridengine)
            else:
                log.console('no recipe instances for ' + str(id),
                            logging.WARN)
