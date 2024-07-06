# Usage: python cdaweb.py --help

import os
import tempfile

import cdawmeta

def cli():
  clkws = {
    "id": {
      "help": "Pattern for dataset IDs to include, e.g., '^A|^B' (default: .*)"
    },
    "max_workers": {
      "type": int,
      "help": "Number of threads to use for downloading",
      "default": 4
    },
    "expire_after": {
      "type": int,
      "help": "Expire cache after this many seconds",
      "default": 0
    },
    "update": {
      "action": "store_true",
      "help": "Update existing files",
      "default": False
    },
    "no_orig_data": {
      "action": "store_true",
      "help": "Exclude _orig_data in catalog.json (_sample_{file,url} is also not created)",
      "default": False
    },
    "no_spase": {
      "action": "store_true",
      "help": "Exclude _spase in catalog.json",
      "default": False
    },
    "diffs": {
      "action": "store_true",
      "help": "Compute response diffs",
      "default": False
    },
    "data_dir": {
      "help": "Directory for data files",
      "default": None
    }
  }

  import argparse
  parser = argparse.ArgumentParser()
  for k, v in clkws.items():
    parser.add_argument(f'--{k}', **v)
  return parser.parse_args()

args = cli()

timeouts = {
  'allxml': 30,
  'master': 30,
  'spase': 30,
  'orig_data': 120
}

if args.id is None:
  partial_ext = ''
  partial_dir = ''
else:
  partial_ext = '.partial'
  partial_dir = 'partial'

if args.data_dir is None:
  if os.path.exists('/tmp'):
    args.data_dir = '/tmp/cdaweb'
  else:
    args.data_dir = os.path.join(tempfile.gettempdir(), 'cdaweb')

base_name = f'cdaweb{partial_ext}'
file_out = os.path.join(args.data_dir, partial_dir, f'{base_name}.json')
log_config = {
  'file_log': os.path.join(args.data_dir, partial_dir, f'{base_name}.log'),
  'file_error': os.path.join(args.data_dir, partial_dir, f'{base_name}.errors.log'),
  'format': '%(message)s',
  'rm_string': args.data_dir + '/'
}
logger = cdawmeta.util.logger(**log_config)

kwargs = {
  **vars(args),
  "timeouts": timeouts,
  "logger": logger,
}

import json
metadata_ = cdawmeta.metadata(**kwargs)

issues_file = os.path.join(os.path.dirname(__file__), 'hapi', "hapi-nl-issues.json")
try:
  issues = cdawmeta.util.read(issues_file, logger=logger)
except Exception as e:
  exit(f"Error: Could not read {issues_file} file: {e}")


rest = cdawmeta.hapi(metadata_, issues, data_dir=args.data_dir, logger=logger)
print(json.dumps(rest, indent=2))
exit()
#print(json.dumps(metadata_, indent=2))
#logger.info(f'# of datasets in all.xml: {create_datasets.n}')
#logger.info(f'# of datasets handled:    {len(datasets)}')

try:
  cdawmeta.util.write(file_out, metadata_, logger=logger)
except Exception as e:
  logger.error(f"Error writing {file_out}: {e}")
  exit(1)
