import os
import tempfile

import cdawmeta

CONFIG = {}
for config in ['table', 'logger', 'metadata', 'hapi']:
  try:
    file = os.path.join(os.path.dirname(__file__), f'{config}.json')
    CONFIG[config] = cdawmeta.util.read(file)
    del CONFIG[config]['_comment']
  except Exception as e:
    print(f"Error reading config file: {file}")
    raise e

DATA_DIR = os.path.join(tempfile.gettempdir(), 'cdawmeta-data')
if os.path.exists('/tmp'):
  DATA_DIR = '/tmp/cdawmeta-data'
