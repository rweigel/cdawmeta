def cli(script):

  clkws = {
    "id": {
      "help": "ID or pattern for dataset IDs to include (prefix with ^ to use pattern match, e.g., '^A|^B') (default: ^.*)"
    },
    "max_workers": {
      "type": int,
      "help": "Number of threads to use for downloading",
      "default": 3
    },
    "embed_data": {
      "action": "store_true",
      "help": "Embed data in returned dict",
      "default": False
    },
    "update": {
      "action": "store_true",
      "help": "Update existing cached HTTP responses",
      "default": False
    },
    "no_orig_data": {
      "action": "store_true",
      "help": "Exclude orig_data object in catalog.json (sample_{file,url,plot} is also not created)",
      "default": False
    },
    "no_spase": {
      "action": "store_true",
      "help": "Exclude spase in catalog.json",
      "default": False
    },
    "diffs": {
      "action": "store_true",
      "help": "Compute response diffs",
      "default": False
    },
    "data_dir": {
      "help": "Directory for data files",
      "default": './data'
    },
    "table_name": {
      "help": "Name of table to create (default: all tables)",
      "default": None
    },
    "port": {
      "help": "Serve table as a web page at http://localhost:port. Must specify --table_name",
      "default": None
    }
  }

  if script == 'cdaweb.py':
    del clkws['table_name']
    del clkws['port']

  if script == 'hapi.py':
    del clkws['table_name']
    del clkws['port']
    del clkws['no_spase']
    del clkws['embed_data']

  if script == 'table.py':
    del clkws['embed_data']
    del clkws['no_orig_data']
    del clkws['no_spase']
    del clkws['diffs']

  if script == 'query.py':
    del clkws['table_name']
    del clkws['port']
    del clkws['diffs']
    del clkws['no_spase']
    del clkws['no_orig_data']

  import argparse
  parser = argparse.ArgumentParser()
  for k, v in clkws.items():
    parser.add_argument(f'--{k}', **v)

  args = vars(parser.parse_args())

  import cdawmeta
  if args['data_dir']:
    cdawmeta.set('DATA_DIR', args['data_dir'])
  del args['data_dir']

  return args
