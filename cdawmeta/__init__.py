import os
import tempfile
if os.path.exists('/tmp'):
  DATA_DIR = '/tmp/cdawmeta-data'
else:
  DATA_DIR = os.path.join(tempfile.gettempdir(), 'cdawmeta-data')
INFO_DIR = os.path.join(DATA_DIR, 'hapi', 'info')
del tempfile

from cdawmeta import attrib
from cdawmeta import util
CONFIG = util.read(os.path.join(os.path.dirname(__file__), 'config.json'))
del os

def set(key, val):
  keys = ['DATA_DIR']
  if not key in keys:
    raise ValueError(f"Invalid key: {key}. Allowed keys: {keys}")

  import os
  import cdawmeta
  if key == 'DATA_DIR':
    cdawmeta.DATA_DIR = os.path.abspath(val)

from cdawmeta.io.f2c_specifier import f2c_specifier
from cdawmeta.io.write_csv import write_csv
from cdawmeta.io.read_cdf import read_cdf
from cdawmeta.io.read_cdf import read_cdf_meta
from cdawmeta.io.read_cdf import read_cdf_depend_0s
from cdawmeta.io.read_ws import read_ws

from cdawmeta.logger import logger
from cdawmeta.cli import cli
from cdawmeta.hapi import hapi
from cdawmeta.metadata import metadata
from cdawmeta.metadata import ids
from cdawmeta.table import table
