# Usage: python metadata.py --help

import cdawmeta

args = cdawmeta.cli('cdaweb.py')
metadata = cdawmeta.metadata(**args)

if args['id'] is not None and not args['id'].startswith('^'):
  import json
  if args['meta_type'] == 'all':
    print(json.dumps(metadata[args['id']], indent=2))
  else:
    for meta_type in args['meta_type'].split(','):
      print(json.dumps(metadata[args['id']][meta_type], indent=2))
