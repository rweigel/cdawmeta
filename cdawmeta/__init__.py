__all__ = [
            'additions',
            'attrib',
            'CONFIG',
            'cli',
            'db',
            'dependencies',
            'DATA_DIR',
            'error',
            'exception',
            'write_errors',
            'generate',
            'generators',
            'ids',
            'io',
            'logger',
            'metadata',
            'reports',
            'restructure',
            'table',
            'trace',
            'util'
          ]

import utilrsw as util

from cdawmeta import attrib
from cdawmeta import db
from cdawmeta import io
from cdawmeta import reports
from cdawmeta import restructure
from cdawmeta import generators

from cdawmeta.generators import dependencies

from cdawmeta.additions import additions
from cdawmeta.cli import cli
from cdawmeta.config import CONFIG
from cdawmeta.config import DATA_DIR
from cdawmeta.error import trace
from cdawmeta.error import error
from cdawmeta.error import write_errors
from cdawmeta.error import exception
from cdawmeta.generate import generate
from cdawmeta.logger import logger
from cdawmeta.metadata import ids
from cdawmeta.metadata import metadata

from cdawmeta.table import table