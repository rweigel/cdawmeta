# Usage: python metadata.py --help

import cdawmeta

args = cdawmeta.cli('cdaweb.py')
metadata = cdawmeta.metadata(**args)

if args['id'] is not None and not args['id'].startswith('^'):
  import json
  #print(json.dumps(metadata[args['id']]['hapi'], indent=2))
  #print(json.dumps(metadata[args['id']]['soso'], indent=2))
  print(json.dumps(metadata[args['id']]['hapi'], indent=2))
  #print(json.dumps(metadata, indent=2))
