#!/usr/bin/env python2.7

#################################
# Import required Python modules
#################################
import logging
from collections import namedtuple

from tools4caom2.logger import logger

from jcmt2caom2.__version__ import version

JSA_NAMES = ('GOOD', 'FAILED_QA', 'BAD', 'JUNK')
JSA_VALUES = range(len(JSA_NAMES))

# The values possible for commentstatus have discontinuous ranges for different 
# purposes (quality assessment and classification of time gaps), but only
# the quality assessment values should be handled in this code.
JCMT_NAMES = ('GOOD', 'QUESTIONABLE', 'BAD', 'REJECTED', 'JUNK')
JCMT_VALUES = range(len(JCMT_NAMES))

JSA_QA  = namedtuple('JSA_Quality',  JSA_NAMES)._make(JSA_VALUES)
JCMT_QA = namedtuple('JCMT_Quality', JCMT_NAMES)._make(JCMT_VALUES)

JSA_TRANS = {JCMT_QA.GOOD:             JSA_QA.GOOD,
             JCMT_QA.QUESTIONABLE:     JSA_QA.FAILED_QA,
             JCMT_QA.BAD:              JSA_QA.BAD,
             JCMT_QA.REJECTED:         JSA_QA.FAILED_QA,
             JCMT_QA.JUNK:             JSA_QA.JUNK}

class quality(object):
    """
    Quality assessment class.  This does not store a quality assessment, 
    but provides transformations to character strings, and from JCMT_QA
    to JSA_QA.

    The intended public interface is
    quality.JSA_QA: named constants for JSA quality assessments
    quality.JCMT_QA: named constants for JCMT quality assessments
    quality.from_jcmt(value): convert JCMT_QA to JSA_QA value.
    
    This will need ajdustment when the CADC decides how to implement quality 
    assessment in CAOM-2.
    """    
    def __init__(self, jcmt_value, log):
        """
        A JSA quality assessment derived from the input JCMT quality assessment
        
        Arguments:
        jcmt_value: a JCMT_QA value
        log: a tools4caom2.logger to report errors
        """
        self.log = log

        if jcmt_value in JSA_TRANS:
            self._jcmt_value = jcmt_value
            self._jsa_value = JSA_TRANS[jcmt_value]
        else:
            self.log.console('jcmt_value = %d must be in ' % (jcmt_value) +
                             repr(JSA_TRANS.keys()) ,
                             logging.ERROR)
        
    def jsa_value(self):
        """
        Return the JSA_QA as a numerical value from JSA_VALUES
        """
        return self._jsa_value
    
    def jcmt_value(self):
        """
        Return the JSA_QA as a numerical value from JSA_VALUES
        """
        return self._jsa_value
    
    def __str__(self):
        """
        Convert the jsa_value to a string
        """
        return self.jsa_name()
    
    def jsa_name(self):
        """
        Convert a JSA_QA value to a string
        """
        if self._jsa_value in JSA_VALUES:
            return JSA_NAMES[JSA_VALUES.index(self._jsa_value)]
        else:
            self.log.console('jsa_value = %d must be in ' % (self._jsa_value) +
                             repr(JSA_VALUES),
                             logging.ERROR)
            
    def jcmt_name(self):
        """
        Convert the JCMT_QA value to a string
        """
        if self._jcmt_value in JCMT_VALUES:
            return JCMT_NAMES[JCMT_VALUES.index(self._jcmt_value)]
        else:
            self.log.console('jcmt_value = %d must be in ' % (self._jcmt_value) +
                             repr(JCMT_VALUES),
                             logging.ERROR)
            
            
        
    