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
