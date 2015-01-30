#################################
# Import required Python modules
#################################
from tools4caom2.error import CAOMError

from jcmt2caom2.__version__ import version


def product_id(backend,
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
        raise CAOMError('product must be supplied to generate productID')

    if backend == 'SCUBA-2':
        if not filter:
            raise CAOMError(
                'filter must be supplied to generate SCUBA-2 productID')

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
            raise CAOMError(
                'restfreq must be supplied to generate heterodyne productID')

        if not bwmode or type(bwmode) != str:
            raise CAOMError(
                'bwmode must be supplied to generate heterodyne productID')

        if not subsysnr or type(subsysnr) != str:
            raise CAOMError(
                'subsysnr must be supplied to generate heterodyne productID')

        restfreqstr = '%.0fMHz' % (restfreq * 1.0e-6)

        productID = separator.join([product,
                                    restfreqstr,
                                    bwmode,
                                    subsysnr])

    return productID
