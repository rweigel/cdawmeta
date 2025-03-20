def cli(script, defs=False):

  import os

  import cdawmeta

  meta_types = cdawmeta.dependencies['all']

  # Define the custom type function
  def meta_types_list(value):
    if value == '':
      return ''

    items = value.split(',')
    for item in items:
      if item not in meta_types:
        raise argparse.ArgumentTypeError(f"Invalid meta-type: '{item}'")
    return items

  clkws = {
    "id": {
      "help": "ID or pattern for dataset IDs to include (prefix with ^ to use pattern match, e.g., '^A|^B') (default: ^.*)",
      "_used_by_all": True,
    },
    "id-skip": {
      "metavar": "ID",
      "help": "ID or pattern for dataset IDs to exclude (prefix with ^ to use pattern match, e.g., '^A|^B') (default: None)",
      "default": cdawmeta.CONFIG['hapi']['id_skip'],
      "_used_by_all": True,
    },
    "meta-type": {
      #"help": "Type of metadata to generate. Default is to generate all types. May be repeated.",
      "help": "Type of metadata to generate. Default is to generate all types.",
      "default": None,
      "choices": meta_types,
      #"action": 'append',
      "_used_by": ['metadata.py']
    },
    "write-catalog": {
      "action": "store_true",
      "help": "Write catalog-all.json files (and catalog.json for HAPI metadata)",
      "default": False,
      "_used_by": ['metadata.py']
    },
    "max-workers": {
      "metavar": "N",
      "type": int,
      "help": "Number of threads to use for downloading (default: %(default)s).",
      "default": 3,
      "_used_by_all": True,
    },
    "embed-data": {
      "action": "store_true",
      "help": "Embed data in returned dict",
      "default": False,
      "_used_by": ['metadata.py']
    },
    "update": {
      "action": "store_true",
      "help": "Update existing cached HTTP responses and regenerate computed metadata.",
      "default": False,
      "_used_by_all": True
    },
    "update-skip": {
      "help": "Comma separated list of meta-types to not regenerate.",
      "default": '',
      "type": meta_types_list,
      "_used_by_all": True
    },
    "regen": {
      "action": "store_true",
      "help": "Regenerate computed metadata. Use for testing computed metadata code changes.",
      "default": False,
      "_used_by_all": True
    },
    "regen-skip": {
      "help": "Comma separated list of meta-types to not regenerate.",
      "default": '',
      "type": meta_types_list,
      "_used_by_all": True
    },
    "exit-on-exception": {
      "action": "store_true",
      "help": "Exit on unhandled exception",
      "default": False,
      "_used_by_all": True
    },
    "log-level": {
      "help": "Log level",
      "default": 'info',
      "choices": ['debug', 'info', 'warning', 'error', 'critical'],
      "_used_by_all": True
    },
    "debug": {
      "action": "store_true",
      "help": "Same as --log-level debug",
      "default": False,
      "_used_by_all": True
    },
    "diffs": {
      "action": "store_true",
      "help": "Compute response diffs if --update",
      "default": False,
      "used_by": ['metadata.py']
    },
    "data-dir": {
      "metavar": "DIR",
      "help": "Directory to save files",
      "default": './data',
      "_used_by_all": True
    },

    "report-name": {
      "help": "Name of report to execute (default: all reports)",
      "default": None,
      "choices": cdawmeta.reports.__all__,
      "_used_by": ['report.py']
    },
    "table-name": {
      "help": "Name of table to create (default: all tables)",
      "default": None,
      "choices": list(cdawmeta.CONFIG['table']['tables'].keys()),
      "_used_by": ['table.py']
    },
    "collection-name": {
      "help": "Name of MongoDB collection to create (default: all collections)",
      "default": None,
      "choices": list(cdawmeta.CONFIG['table']['mongo']['dbs'].keys()),
      "_used_by": ['query.py']
    },
    "mongod-binary": {
      "help": "Path to mongod binary",
      "default": os.path.expanduser("~/mongodb/bin/mongod"),
      "_used_by": ['query.py']
    },
    "filter": {
      "help": "Filter to apply to MongoDB collection (default: {})",
      "default": "{}",
      "_used_by": ['query.py']
    },
    "port": {
      "metavar": "PORT",
      "type": int,
      "help": "Serve table as a web page at http://localhost:port. Must specify --table_name",
      "default": None,
      "_used_by": ['table.py', 'query.py']
    }

  }

  for key, val in clkws.copy().items():
    keep = '_used_by_all' in val and val['_used_by_all']
    keep = keep or ('_used_by' in val and script in val['_used_by'])
    if not keep:
      del clkws[key]
    if '_used_by_all' in val:
      del val['_used_by_all']
    if '_used_by' in val:
      del val['_used_by']

  if defs:
    return clkws

  import argparse
  parser = argparse.ArgumentParser()
  for k, v in clkws.items():
    parser.add_argument(f'--{k}', **v)

  # Note that hyphens are converted to underscores when parsing
  args = vars(parser.parse_args())

  if args['debug']:
    args['log_level'] = 'debug'
  del args['debug']

  if args['data_dir']:
    cdawmeta.DATA_DIR = args['data_dir']
  del args['data_dir']

  return args
