#################################
# Import required Python modules
#################################
import logging

from tools4caom2.error import CAOMError

from jcmt2caom2.__version__ import version
from jcmt2caom2.jsa.product_id import product_id

logger = logging.getLogger(__name__)


def raw_product_id(backend, jcmt_db, obsid, conn):
    """
    Generates raw (observationID, productID) values for an observation.

    Arguments:
    backend: one of ACSIS, DAS, AOS-C, SCUBA-2
    jcmt_db: prefix fot the database and schem where ACSIS, FILES are located
    obsid: observation identifier, primary key in COMMON table
    conn: connection to database

    Returns:
    return a dictionary of productID keyed on
    subsysnr (filter for SCUBA-2)
    """
    if backend == 'SCUBA-2':
        subsysnr_dict = {'450': 'raw-450um',
                         '850': 'raw-850um'}

    else:
        subsysnr_dict = {}

        result = conn.get_heterodyne_product_info(backend, obsid)

        if result:
            for subsysnr, restfreq, bwmode, specid, hybrid in result:
                restfreqhz = 1.0e9 * float(restfreq)
                prefix = 'raw'
                if int(hybrid) > 1:
                    prefix = 'raw-hybrid'
                subsysnr_dict[str(subsysnr)] = product_id(backend,
                                                          product=prefix,
                                                          restfreq=restfreqhz,
                                                          bwmode=bwmode,
                                                          subsysnr=str(specid))
        else:
            raise CAOMError('no rows returned from ACSIS for obsid = ' + obsid)


    return subsysnr_dict
