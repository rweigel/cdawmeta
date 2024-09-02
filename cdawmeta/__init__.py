__all__ = ['attrib', 'cli', 'error', 'generate', 'hapi', 'ids', 'io', 'logger', 'metadata', 'table', 'util']

from cdawmeta import attrib
from cdawmeta import io
from cdawmeta import util
from cdawmeta import generate

from cdawmeta.cli import cli
from cdawmeta.error import error
from cdawmeta.metadata import ids
from cdawmeta.logger import logger
from cdawmeta.metadata import metadata
from cdawmeta.table import table

def config():
  import os
  from . import util
  try:
    CONFIG = util.read(os.path.join(os.path.dirname(__file__), 'config.json'))
  except Exception as e:
    print(f"Error reading config file: {os.path.join(os.path.dirname(__file__), 'config.json')}")
    raise e
  return CONFIG

def data_dir():
  import os
  import tempfile
  if os.path.exists('/tmp'):
    return '/tmp/cdawmeta-data'
  return os.path.join(tempfile.gettempdir(), 'cdawmeta-data')

CONFIG = config()
del config

DATA_DIR = data_dir()
del data_dir
