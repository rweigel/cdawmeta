import os
import tempfile
if os.path.exists('/tmp'):
  DATA_DIR = '/tmp/cdawmeta-data'
else:
  DATA_DIR = os.path.join(tempfile.gettempdir(), 'cdawmeta-data')
INFO_DIR = os.path.join(DATA_DIR, 'hapi', 'info')
del tempfile

from cdawmeta import util
try:
  CONFIG = util.read(os.path.join(os.path.dirname(__file__), 'config.json'))
  del os
except Exception as e:
  print(f"Error reading config file: {os.path.join(os.path.dirname(__file__), 'config.json')}")
  raise e

from cdawmeta import attrib
from cdawmeta import io
from cdawmeta.logger import logger
from cdawmeta.cli import cli
from cdawmeta.hapi import hapi
from cdawmeta.metadata import metadata
from cdawmeta.metadata import ids
from cdawmeta.table import table
