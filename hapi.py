# Usage: python hapi.py --help

import cdawmeta

args = cdawmeta.cli('hapi.py')

metadata = cdawmeta.hapi(**args)

if args['id'] is not None:
  import json
  print(json.dumps(metadata, indent=2))