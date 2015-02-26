# TODO: insert copyright statement here.
# Contains code extracted from jcmt2caom2.raw and jcmt2caom2.jcmt2caom2ingest.

from logging import getLogger

logger = getLogger(__name__)


def get_project_pi_title(project_id, conn, tap):
    """
    Retrieve the PI name and project title for a given JCMT project.

    For current projects and some older projects, the PI name will be
    available in the OMP.  Otherwise we must check the CAOM-2 for
    existing records which refer to this project.

    Returns a (project_pi, project_title) tuple, of which one or both
    elements may be None if the project as a whole, or the PI name,
    could not be found.
    """

    project_pi = None
    project_title = None

    logger.debug('Fetching project "%s" details from OMP', project_id)
    sqlcmd = '\n'.join([
        'SELECT ',
        '    ou.uname,',
        '    op.title',
        'FROM omp..ompproj op',
        '    LEFT JOIN omp..ompuser ou'
        '        ON op.pi=ou.userid AND ou.obfuscated=0',
        'WHERE op.projectid="%s"' % (project_id,)])
    answer = conn.read(sqlcmd)

    if len(answer):
        project_pi = answer[0][0]
        project_title = answer[0][1]

    if (project_pi is None) or (project_title is None):
        # Some of the information was missing, so try a TAP query
        # to see if the project information is already in CAOM-2.
        logger.debug('Fetching project "%s" details from CAOM-2', project_id)
        tapcmd = '\n'.join([
            "SELECT DISTINCT Observation.proposal_pi, ",
            "                Observation.proposal_title",
            "FROM caom2.Observation as Observation",
            "WHERE Observation.collection = 'JCMT'",
            "      AND Observation.proposal_id = '" +
            project_id + "'"])
        answer = tap.query(tapcmd)

        if answer and len(answer[0]) > 0:
            if (project_pi is None) and answer[0][0]:
                project_pi = answer[0][0]
            if (project_title is None) and answer[0][1]:
                project_title = answer[0][1]

    return (project_pi, project_title)
