# Usage: python metadata.py --help

import cdawmeta

args = cdawmeta.cli('metadata.py')

if args['id'] is not None and not args['id'].startswith('^'):
  args['embed_data'] = True
metadata = cdawmeta.metadata(**args)

if args['id'] is not None and not args['id'].startswith('^'):
  if args['meta_type'] is None:
    cdawmeta.util.print_dict(metadata[args['id']], style='json')
  else:
    for meta_type in args['meta_type'].split(','):
      cdawmeta.util.print_dict(metadata[args['id']][meta_type], style='json')
