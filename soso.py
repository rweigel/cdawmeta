# Usage: python soso.py --help

import cdawmeta

args = cdawmeta.cli('soso.py')

metadata = cdawmeta.soso(**args)

if args['id'] is not None and not args['id'].startswith('^'):
  import json
  print(json.dumps(metadata, indent=2))