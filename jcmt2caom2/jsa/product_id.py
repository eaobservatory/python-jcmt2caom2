#!/usr/bin/env python2.7

#################################
# Import required Python modules
#################################
import logging

from jcmt2caom2.__version__ import version

def product_id(backend, log, 
               product=None, 
               restfreq=None, 
               bwmode=None, 
               subsysnr=None,
               filter=None):
    """
    Generates productID strings.
    
    Arguments:
    backend: one of ACSIS, DAS, AOS-C, SCUBA-2
    product: one of raw, raw-hybrid, or PRODUCT keyword
    restfreq: restfreq in Hz as a float
    bwmode: bandwidth mode as a string
    subsysnr: subsysnr for ACSIS-like observations as a string
    filter: wavelength in um for SCUBA-2 as a string
    
    Returns:
    productID string
    """
    separator = '-'
    if not product:
        log.console('product must be supplied to generate productID',
                    logging.ERROR)

    if backend == 'SCUBA-2':
        if not filter:
            log.console('filter must be supplied to generate SCUBA-2 productID',
                        logging.ERROR)
        
        subsysnr_dict = {'450': '450um',
                         '850': '850um'}
        if filter in subsysnr_dict:
            productID = separator.join([product,
                                        subsysnr_dict[filter]])
        else:
            # Allow custom filters for special cases
            productID = separator.join([product,
                                        filter])

    else:
        if not restfreq or type(restfreq) != float:
            log.console('restfreq must be supplied to generate heterodyne productID',
                        logging.ERROR)
        if not bwmode or type(bwmode) != str:
            log.console('bwmode must be supplied to generate heterodyne productID',
                        logging.ERROR)
        if not subsysnr or type(subsysnr) != str :
            log.console('subsysnr must be supplied to generate heterodyne productID',
                        logging.ERROR)
        
        restfreqstr = '%.0fMHz' % (restfreq * 1.0e-6)
        
        productID = separator.join([product,
                                    restfreqstr,
                                    bwmode,
                                    subsysnr])
    
    return productID