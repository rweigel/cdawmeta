import os
import glob
import tempfile

import cdawmeta

CONFIG = {}

parts = cdawmeta.util.file_parts(__file__)

pattern = os.path.join(f"{parts['dir']}/*.json")
files = glob.glob(pattern)
for file in files:
  try:
    parts = cdawmeta.util.file_parts(file)
    config = parts['root']
    CONFIG[config] = cdawmeta.util.read(file)
    if '_comment' in CONFIG[config]:
      del CONFIG[config]['_comment']
  except Exception as e:
    print(f"Error reading config file: {file}")
    raise e

DATA_DIR = os.path.join(tempfile.gettempdir(), 'cdawmeta-data')
if os.path.exists('/tmp'):
  DATA_DIR = '/tmp/cdawmeta-data'
