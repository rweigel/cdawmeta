# Usage: python hapi.py --help

import cdawmeta

args = cdawmeta.cli('hapi.py')

metadata = cdawmeta.generate.hapi(**args)

if args['id'] is not None and not args['id'].startswith('^'):
  import json
  print(json.dumps(metadata, indent=2))