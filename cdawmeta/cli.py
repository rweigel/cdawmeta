def cli(script):

  import cdawmeta
  meta_types_generated = cdawmeta.generators

  clkws = {
    "meta-type": {
      "help": "Type of metadata to generate. Default is to generate all types.",
      "default": None,
      "choices": ['master', 'orig_data', 'spase', *meta_types_generated]
    },
    "id": {
      "help": "ID or pattern for dataset IDs to include (prefix with ^ to use pattern match, e.g., '^A|^B') (default: ^.*)"
    },
    "skip": {
      "metavar": "ID",
      "default": "AIM_CIPS_SCI_3A",
      "help": "ID or pattern for dataset IDs to exclude (prefix with ^ to use pattern match, e.g., '^A|^B') (default: None)"
    },
    "write-catalog": {
      "action": "store_true",
      "help": "Write catalog-all.json files (and catalog.json for HAPI metadata)",
      "default": False
    },
    "max-workers": {
      "metavar": "N",
      "type": int,
      "help": "Number of threads to use for downloading",
      "default": 3
    },
    "embed-data": {
      "action": "store_true",
      "help": "Embed data in returned dict",
      "default": False
    },
    "update": {
      "action": "store_true",
      "help": "Update existing cached HTTP responses and regenerate computed metadata.",
      "default": False
    },
    "regen": {
      "action": "store_true",
      "help": "Regenerate computed metadata. Use for testing computed metadata code changes.",
      "default": False
    },
    "log-level": {
      "choices": ['debug', 'info', 'warning', 'error', 'critical'],
      "help": "Log level",
      "default": 'info'
    },
    "diffs": {
      "action": "store_true",
      "help": "Compute response diffs if --update",
      "default": False
    },
    "data-dir": {
      "metavar": "DIR",
      "help": "Directory to save files",
      "default": './data'
    },
    "table-name": {
      "help": "Name of table to create (default: all tables)",
      "default": None
    },
    "query-name": {
      "help": "Name of query to execute (default: all queries)",
      "default": None
    },
    "port": {
      "metavar": "PORT",
      "type": int,
      "help": "Serve table as a web page at http://localhost:port. Must specify --table_name",
      "default": None
    }
  }

  if script != 'table.py':
    del clkws['port']
    del clkws['table-name']

  if script != 'query.py':
    del clkws['query-name']

  if script in ['hapi.py', 'soso.py', 'cadence.py']:
    del clkws['diffs']
    del clkws['embed-data']
    del clkws['max-workers']

  if script == 'table.py':
    del clkws['diffs']
    del clkws['meta-type']
    del clkws['write-catalog']

  if script == 'query.py':
    del clkws['diffs']
    del clkws['meta-type']
    del clkws['write-catalog']

  import argparse
  parser = argparse.ArgumentParser()
  for k, v in clkws.items():
    parser.add_argument(f'--{k}', **v)

  args = vars(parser.parse_args())

  # Hyphens are converted to underscores when parsing
  if args['data_dir']:
    import cdawmeta
    cdawmeta.DATA_DIR = args['data_dir']
  del args['data_dir']

  return args
