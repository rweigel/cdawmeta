def cli(script, defs=False):

  import cdawmeta

  meta_types = cdawmeta.dependencies['all']

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
      "help": "Number of threads to use for downloading",
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
      "help": "Update existing cached HTTP responses and regenerate computed metadata except cadence.",
      "default": False,
      "_used_by_all": True
    },
    "update-skip": {
      "help": "Comma separated list of meta-types to not regenerate.",
      "default": '',
      "choices": meta_types,
      "_used_by_all": True
    },
    "regen": {
      "action": "store_true",
      "help": "Regenerate computed metadata except cadence. Use for testing computed metadata code changes.",
      "default": False,
      "_used_by_all": True
    },
    "regen-skip": {
      "help": "Comma separated list of meta-types to not regenerate.",
      "default": '',
      "choices": meta_types,
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
