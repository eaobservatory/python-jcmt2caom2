#!/usr/bin/env python2.7

#################################
# Import required Python modules
#################################
import re

from jcmt2caom2.__version__ import version


def target_name(object_name):
    """
    Standardizes the conventions for target names.  All names will be put
    into upper case, leading and trailing white space will be stripped, and
    multiple white space characters within the string will be converted to
    single spaces.

    Arguments:
    object_name: the JCMT object name

    Returns:
    string containing the standardized target name

    Usage:
    For a raw observation:
        observation.target_name = target_name(common['object'])
    For processed data:
        self.add_to_plane_dict('target.name',
                               target_name(header['OBJECT']))
    """
    return re.sub(r'\s+', r' ', object_name.strip().upper())
