# Usage: python cdaweb.py --help

import cdawmeta

args = cdawmeta.cli('cdaweb.py')

metadata = cdawmeta.metadata(**args)

if args['id'] is not None:
  import json
  print(json.dumps(metadata, indent=2))
