# Usage: python table.py --help

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
