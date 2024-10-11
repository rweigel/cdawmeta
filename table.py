# Usage: python table.py --help

import cdawmeta
args = cdawmeta.cli('table.py')

if args['port'] is not None and args['table_name'] is None:
  raise ValueError("Must specify --table_name when --port is specified")

port = args['port']
del args['port']

info = cdawmeta.table(**args)
cdawmeta.util.print_dict(info, style='json')

if port:
  print("To serve the table:")
  print("cd table")
  print("git clone https://github.com/rweigel/table-ui")
  print("cd table-ui; pip install -e .")
  print("cd ../../")
  print(f"python table/table-ui/serve.py --port {port} --sql {info['sql']}")
