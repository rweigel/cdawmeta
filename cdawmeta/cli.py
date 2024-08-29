def cli(script):

  clkws = {
    "id": {
      "help": "ID or pattern for dataset IDs to include (prefix with ^ to use pattern match, e.g., '^A|^B') (default: ^.*)"
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
    "no-orig-data": {
      "action": "store_true",
      "help": "Exclude orig_data object in catalog.json (sample_{file,url,plot} is also not created)",
      "default": False
    },
    "no-spase": {
      "action": "store_true",
      "help": "Exclude spase in catalog.json",
      "default": False
    },
    "diffs": {
      "action": "store_true",
      "help": "Compute response diffs",
      "default": False
    },
    "data-dir": {
      "metavar": "DIR",
      "help": "Directory for data files",
      "default": './data'
    },
    "table": {
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
    del clkws['table']
    del clkws['port']

  if script == 'hapi.py':
    del clkws['table']
    del clkws['port']
    del clkws['no-spase']
    del clkws['embed-data']

  if script == 'table.py':
    del clkws['embed-data']
    del clkws['no-orig-data']
    del clkws['no-spase']
    del clkws['diffs']

  if script == 'query.py':
    del clkws['table']
    del clkws['port']
    del clkws['diffs']
    del clkws['no-spase']
    del clkws['no-orig-data']

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
