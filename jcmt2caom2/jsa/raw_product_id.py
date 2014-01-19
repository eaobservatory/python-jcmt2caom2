#!/usr/bin/env python2.7

#################################
# Import required Python modules
#################################
import logging

from jcmt2caom2.__version__ import version

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
        subsysnr_dict = {'450': 'raw_450',
                         '850': 'raw_850'}
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
                    fileid_dict[file_id] = (obsid, subsysnr_dict[str(filter)])
            else:
                log.console('no rows returned from FILES for obsid = ' + obsid,
                            logging.ERROR)
            
    else:
        subsysnr_dict = {}
        if backend == 'ACSIS':
            sqlcmd = '\n'.join([
                     'SELECT a.subsysnr, min(aa.subsysnr), count(aa.subsysnr)',
                     'FROM jcmtmd.dbo.ACSIS a',
                     '    INNER JOIN jcmtmd.dbo.ACSIS aa',
                     '        ON a.obsid=aa.obsid',
                     '        AND a.restfreq=aa.restfreq',
                     '        AND a.iffreq=aa.iffreq',
                     '        AND a.ifchansp=aa.ifchansp',
                     'WHERE a.obsid = "%s"' % (obsid,),
                     'GROUP BY a.subsysnr'])
            result = conn.read(sqlcmd)
            if result:
                for subsysnr, specid, hybrid in result:
                    prefix = 'raw_'
                    if int(hybrid) > 1:
                        prefix = 'raw_hybrid_'
                    subsysnr_dict[str(subsysnr)] = prefix + str(specid)
            else:
                log.console('no rows returned from ACSIS for obsid = ' + obsid,
                            logging.ERROR)
        elif backend in ['DAS', 'AOS-C']:
            sqlcmd = """
                     SELECT a.subsysnr, a.specid
                     FROM jcmtmd.dbo.ACSIS a
                     WHERE a.obsid = "%s"
                     """ % (obsid,)
            result = conn.read(sqlcmd)
            if result:
                for subsysnr, specid in result:
                    subsysnr_dict[str(subsysnr)] = 'raw_' + str(specid)
            else:
                log.console('no rows returned from ACSIS for obsid = ' + obsid,
                            logging.ERROR)
        else:
            log.console('backend = ' + backend + ' is not supported',
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
    
