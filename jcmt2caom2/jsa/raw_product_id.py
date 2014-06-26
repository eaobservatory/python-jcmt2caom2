#!/usr/bin/env python2.7

#################################
# Import required Python modules
#################################
import logging

from jcmt2caom2.__version__ import version
from jcmt2caom2.jsa.product_id import product_id

def raw_product_id(backend, context, obsid, conn, log):
    """
    Generates raw (observationID, productID) values for an observation.
    
    Arguments:
    backend: one of ACSIS, DAS, AOS-C, SCUBA-2
    context: one of "raw", "prod"
    obsid: observation identifier, primary key in COMMON table
    conn: connection to database
    
    Returns:
    if usage == "raw" return a dictionary of productID keyed on 
                      subsysnr (filter for SCUBA-2)
    elif usage == "prod" return a dictionary of productID keyed on
                         file_id (minus .sdf extension)
    """
    if backend == 'SCUBA-2':
        subsysnr_dict = {'450': 'raw-450um',
                         '850': 'raw-850um'}
        if context == 'prod':
            sqlcmd = '\n'.join([
                     'SELECT substring(f.file_id, 1, len(f.file_id)-4),',
                     '       s.filter',
                     'FROM jcmtmd.dbo.FILES f',
                     '    INNER JOIN jcmtmd.dbo.SCUBA2 s',
                     '        ON f.obsid_subsysnr=s.obsid_subsysnr',
                     'WHERE f.obsid = "%s"' % (obsid,)])
            result = conn.read(sqlcmd)
            
            fileid_dict = {}
            if result:
                for file_id, filter in result:
                    fileid_dict[file_id] = (obsid, 
                                            product_id(backend, log,
                                                       product='raw',
                                                       filter=str(filter)))
            else:
                log.console('no rows returned from FILES for obsid = ' + obsid,
                            logging.ERROR)
            
    else:
        subsysnr_dict = {}
        if backend == 'ACSIS':
            sqlcmd = '\n'.join([
                     'SELECT a.subsysnr,',
                     '       min(a.restfreq),',
                     '       min(a.bwmode),',
                     '       min(aa.subsysnr),',
                     '       count(aa.subsysnr)',
                     'FROM jcmtmd.dbo.ACSIS a',
                     '    INNER JOIN jcmtmd.dbo.ACSIS aa',
                     '        ON a.obsid=aa.obsid',
                     '        AND a.restfreq=aa.restfreq',
                     '        AND a.iffreq=aa.iffreq',
                     '        AND a.ifchansp=aa.ifchansp',
                     'WHERE a.obsid = "%s"' % (obsid,),
                     'GROUP BY a.subsysnr'])
        elif backend in ['DAS', 'AOS-C']:
            sqlcmd = '\n'.join([
                     'SELECT a.subsysnr,',
                     '       a.restfreq,',
                     '       a.bwmode,',
                     '       a.specid,',
                     '       count(aa.subsysnr)',
                     'FROM jcmtmd.dbo.ACSIS a',
                     '    INNER JOIN jcmtmd.dbo.ACSIS aa',
                     '        ON a.obsid=aa.obsid',
                     '        AND a.specid=aa.specid',
                     'WHERE a.obsid = "%s"' % (obsid,),
                     'GROUP BY a.subsysnr, a.restfreq, a.bwmode, a.specid'])
        else:
            log.console('backend = ' + backend + ' is not supported',
                        logging.ERROR)

        result = conn.read(sqlcmd)
        if result:
            for subsysnr, restfreq, bwmode, specid, hybrid in result:
                restfreqhz = 1.0e9 *float(restfreq)
                prefix = 'raw'
                if int(hybrid) > 1:
                    prefix = 'raw-hybrid'
                subsysnr_dict[str(subsysnr)] = product_id(backend, log,
                                                          product=prefix,
                                                          restfreq=restfreqhz,
                                                          bwmode=bwmode,
                                                          subsysnr=str(specid))
        else:
            log.console('no rows returned from ACSIS for obsid = ' + obsid,
                        logging.ERROR)

        if context == 'prod':
            sqlcmd = '\n'.join([
                     'SELECT substring(f.file_id, 1, len(f.file_id)-4),',
                     '       a.subsysnr',
                     'FROM jcmtmd.dbo.FILES f',
                     '    INNER JOIN jcmtmd.dbo.ACSIS a',
                     '        ON f.obsid_subsysnr=a.obsid_subsysnr',
                     'WHERE f.obsid = "%s"' % (obsid,)])
            result = conn.read(sqlcmd)
            
            fileid_dict = {}
            if result:
                for file_id, subsysnr in result:
                    fileid_dict[file_id] = (obsid, subsysnr_dict[str(subsysnr)])
                    log.file('file_id metadata: ' + file_id +
                             ', ' + obsid +
                             ', ' + subsysnr_dict[str(subsysnr)],
                             logging.DEBUG)
            else:
                log.console('no rows returned from FILES for obsid = ' + obsid,
                            logging.ERROR)
    
    if context == 'raw':
        return subsysnr_dict
    elif context == 'prod':
        return fileid_dict
    
