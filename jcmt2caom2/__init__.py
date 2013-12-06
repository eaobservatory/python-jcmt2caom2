#!/usr/bin/env python
#/*+
#************************************************************************
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#*
#* (c) 2011  .                      (c) 2011
#* National Research Council        Conseil national de recherches
#* Ottawa, Canada, K1A 0R6          Ottawa, Canada, K1A 0R6
#* All rights reserved              Tous droits reserves
#*
#* NRC disclaims any warranties,    Le CNRC denie toute garantie
#* expressed, implied, or statu-    enoncee, implicite ou legale,
#* tory, of any kind with respect   de quelque nature que se soit,
#* to the software, including       concernant le logiciel, y com-
#* without limitation any war-      pris sans restriction toute
#* ranty of merchantability or      garantie de valeur marchande
#* fitness for a particular pur-    ou de pertinence pour un usage
#* pose.  NRC shall not be liable   particulier.  Le CNRC ne
#* in any event for any damages,    pourra en aucun cas etre tenu
#* whether direct or indirect,      responsable de tout dommage,
#* special or general, consequen-   direct ou indirect, particul-
#* tial or incidental, arising      ier ou general, accessoire ou
#* from the use of the software.    fortuit, resultant de l'utili-
#*                                  sation du logiciel.
#*
#************************************************************************
#*
#*   Script Name:    __init__.py (makes cadcpy2 into a package)
#*
#*   Purpose:
#+     makes cadcpy2 into a package
#*
#*   Classes:
#*
#*   Functions:
#*
#*   Field
#*    $Revision: 123 $
#*    $Date: 2012-07-20 12:02:13 -0700 (Fri, 20 Jul 2012) $
#*    $Author: redman $
#*
#*
#*   Modification History:
#*
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#* special or general, consequen-   direct ou indirect, particul-
#* tial or incidental, arising      ier ou general, accessoire ou
#* from the use of the software.    fortuit, resultant de l'utili-
#*                                  sation du logiciel.
#*
#************************************************************************
#*
#*   Script Name:    __init__.py (makes jcmt2caom2 into a package)
#*
#*   Purpose:
#+     makes jcmt2caom2 into a package
#*
#*   Classes:
#*
#*   Functions:
#*
#*   Field
#*    $Revision: 123 $
#*    $Date: 2012-07-20 12:02:13 -0700 (Fri, 20 Jul 2012) $
#*    $Author: redman $
#*
#*
#*   Modification History:
#*
#****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
#************************************************************************
#-*/
"""
The jcmt2caom2 package is a set of Python 2.7 modules providing tools to ingest
JCMT data files into CAOM-@.  
"""

__all__ = ['database',
           'discovery',
           'discovery_state',
           'received_state',
           'raw',
           'stdpipe']
