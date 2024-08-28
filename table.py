# Usage: python table.py --help

import cdawmeta

args = cdawmeta.cli('table.py')

if args['port'] is not None and args['table_name'] is None:
  raise ValueError("Must specify --table_name when --port is specified")

port = args['port']
del args['port']

info = cdawmeta.table(**args)

if port:
  import os
  script_path = os.path.dirname(os.path.realpath(cdawmeta.__file__))
  path = os.path.normpath(os.path.join(script_path, '..'))
  cmd = f"python {path}/table/table-ui/ajax/server.py {port} '{info['header_file']}' '{info['sql_file']}'"
  print(f"Executing: Using os.system() to execute\n  {cmd}")
  os.system(cmd)
