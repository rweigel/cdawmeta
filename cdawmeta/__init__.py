__all__ = [
            'additions',
            'attrib',
            'CONFIG',
            'cli',
            'db',
            'dependencies',
            'DATA_DIR',
            'error',
            'write_errors',
            'generate',
            'ids',
            'io',
            'logger',
            'metadata',
            'restructure',
            'table',
            'util'
          ]

# TODO: Put the following imports in a loop over __all__.

import utilrsw as util

from cdawmeta import attrib
from cdawmeta import db
from cdawmeta import io
from cdawmeta import restructure

from cdawmeta.additions import additions
from cdawmeta.cli import cli
from cdawmeta.config import CONFIG
from cdawmeta.config import DATA_DIR
from cdawmeta.error import error
from cdawmeta.error import write_errors
from cdawmeta.generate import generate
from cdawmeta._generate import dependencies
from cdawmeta.logger import logger
from cdawmeta.metadata import ids
from cdawmeta.metadata import metadata
from cdawmeta.table import table

