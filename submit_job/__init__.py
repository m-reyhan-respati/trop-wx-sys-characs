__name__ = "submit_job"
__all__ = ["submit", "check"]
__doc__ = '''This module contains the functions required to submit PBS jobs to queue and check their status reports.'''

from . import submit
from . import check
