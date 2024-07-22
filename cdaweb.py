# Usage: python cdaweb.py --help

import cdawmeta

args = cdawmeta.cli('cdaweb.py')

metadata = cdawmeta.metadata(**args)

if args['id'] is not None:
  #print(metadata)
  #pass
  import json
  print(json.dumps(metadata, indent=2))
  #_master = metadata[args['id']]['master']['data']
  #cdawmeta.restructure_master(_master)
  #print(json.dumps(_master, indent=2))