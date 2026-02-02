# Usage:
#   python table.py --help
#
# To test a single dataset table with tableui server:
#   python table.py --id AC_OR_SSC --port 9991
# Note that any changes to table/conf/cdaweb.json will not be reflected unless
# the server is re-started. This is due to the fact that the configuration is
# passed as a dict instead of a filename to tableui.app(). This is needed because
# the content of table/conf/cdaweb.json must be modified.

import cdawmeta
args = cdawmeta.cli('table.py')

port = args['port']

# port is not an argument for cdawmeta.table, so remove it.
del args['port']

info = cdawmeta.table(**args)
cdawmeta.util.print_dict(info, style='json')

if port:
  import utilrsw.uvicorn

  # Create app configuration for tableui app.
  # Defaults
  configs = {
    'server': {'--host': '0.0.0.0', '--port': 5001, '--workers': 1},
    'app': {'config': 'conf/demo.json', 'debug': False, 'log_level': None}
  }


  dbs = {}
  # Get list of available table names from info dict and map to URL path that
  # is used in table/conf/cdaweb.json
  for table_name in info.keys():
    path = table_name.replace('cdaweb.', '').replace('.', '/')
    dbs[path] = info[table_name]['sql']

  # Read the cdaweb.json app configuration
  import utilrsw
  app_config = utilrsw.read('table/conf/cdaweb.json')

  # Remove entries from app configuration corresponding to tables not created
  app_config_reduced = []
  for entry in app_config:
    if entry['path'] in dbs:
      app_config_reduced.append(entry)
      entry['sqldb'] = dbs[entry['path']]
      entry['dataTables'] = './table/conf/default.json'
      entry['dataTablesAdditions']['renderFunctions'] = './table/conf/render.js'
      utilrsw.print_dict(entry, style='json')

  # Update app configuration in configs
  configs['app']['config'] = app_config_reduced

  utilrsw.uvicorn.run("tableui.app", configs)
