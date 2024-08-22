# Usage: python table.py --help

import cdawmeta

args = cdawmeta.cli('table.py')

kwargs = {
  'id': args['id'],
  'data_dir': args['data_dir'],
  'update': args['update'],
  'max_workers': args['max_workers']
}

headers, bodies = cdawmeta.table(**kwargs)
