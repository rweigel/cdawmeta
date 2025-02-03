# Usage: python metadata.py --help

import cdawmeta

args = cdawmeta.cli('metadata.py')

if args['id'] is not None and not args['id'].startswith('^'):
  args['embed_data'] = True
metadata = cdawmeta.metadata(**args)
exit()
if args['id'] is not None and not args['id'].startswith('^'):
  cdawmeta.util.print_dict(metadata[args['id']], style='json')
