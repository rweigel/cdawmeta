import os
import tempfile

if os.path.exists('/tmp'):
  DATA_DIR = '/tmp/cdawmeta-data'
else:
  DATA_DIR = os.path.join(tempfile.gettempdir(), 'cdawmeta-data')
del os, tempfile

from cdawmeta import util

from cdawmeta.cli import cli

from cdawmeta.io.f2c_specifier import f2c_specifier
from cdawmeta.io.write_csv import write_csv
from cdawmeta.io.read_cdf import read_cdf
from cdawmeta.io.read_cdf import read_cdf_meta
from cdawmeta.io.read_cdf import read_cdf_depend_0s
from cdawmeta.io.read_ws import read_ws

from cdawmeta.metadata import metadata

from cdawmeta.hapi import hapi

