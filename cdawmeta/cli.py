def cli(script):

  clkws = {
    "id": {
      "help": "Pattern for dataset IDs to include, e.g., '^A|^B' (default: .*)"
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

  if script == 'hapi.py':
    del clkws['no_spase']
    del clkws['embed_data']

  import argparse
  parser = argparse.ArgumentParser()
  for k, v in clkws.items():
    parser.add_argument(f'--{k}', **v)

  args = vars(parser.parse_args())

  return args
