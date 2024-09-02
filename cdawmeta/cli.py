def cli(script):

  clkws = {
    "id": {
      "help": "ID or pattern for dataset IDs to include (prefix with ^ to use pattern match, e.g., '^A|^B') (default: ^.*)"
    },
    "skip": {
      "default": "AIM_CIPS_SCI_3A",
      "help": "ID or pattern for dataset IDs to exclude (prefix with ^ to use pattern match, e.g., '^A|^B') (default: None)"
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
      "help": "Update existing cached HTTP responses",
      "default": False
    },
    "regen": {
      "action": "store_true",
      "help": "Regenerate computed metadata",
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
      "help": "Directory for data files",
      "default": './data'
    },
    "table-name": {
      "help": "Name of table to create (default: all tables)",
      "default": None
    },
    "port": {
      "metavar": "PORT",
      "type": int,
      "help": "Serve table as a web page at http://localhost:port. Must specify --table_name",
      "default": None
    }
  }

  if script == 'cdaweb.py':
    del clkws['table-name']
    del clkws['port']

  if script in ['hapi.py', 'soso.py', 'cadence.py']:
    del clkws['max-workers']
    del clkws['embed-data']
    del clkws['port']
    del clkws['table-name']

  if script == 'table.py':
    del clkws['diffs']

  if script == 'query.py':
    del clkws['diffs']
    del clkws['port']
    del clkws['table-name']

  import argparse
  parser = argparse.ArgumentParser()
  for k, v in clkws.items():
    parser.add_argument(f'--{k}', **v)

  args = vars(parser.parse_args())

  # Hyphens are converted to underscores
  if args['data_dir']:
    import cdawmeta
    cdawmeta.DATA_DIR = args['data_dir']
  del args['data_dir']

  return args
