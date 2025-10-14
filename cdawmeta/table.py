import os
import copy

import cdawmeta
import utilrsw

logger = None

def table(id=None,
          id_skip=None,
          table_name=None,
          embed_data=False,
          update=False,
          update_skip='',
          regen=False,
          regen_skip='',
          max_workers=3,
          log_level='info'):

  # Capture kwargs for passing to metadata()
  kwargs = locals()
  # Remove args not passed to metadata()
  del kwargs['table_name']
  del kwargs['embed_data']

  table_configs = cdawmeta.CONFIG['table']['tables']
  table_names = list(table_configs.keys())

  if table_name is None:
    # Create all tables
    info = {}
    for table_name in table_names:
      kwargs['table_name'] = table_name
      info[table_name] = table(**kwargs)
    return info

  if table_name is not None:
    if table_name not in table_names:
      emsg = f"table_name='{table_name}' not in {table_names} in config.json"
      raise ValueError(emsg)

  global logger
  if logger is None:
    logger = cdawmeta.logger('table')
    logger.setLevel(log_level.upper())

  # Get metadata needed for the requested table.
  if table_name.startswith('spase'):
    meta_type = ['spase']
  elif table_name.startswith('cdaweb'):
    meta_type = ['allxml', 'master']
  elif table_name.startswith('hapi'):
    meta_type = ['hapi']

  datasets = cdawmeta.metadata(**kwargs, meta_type=meta_type)

  if table_name.startswith('cdaweb'):
    for dsid in datasets.keys():
      if table_name == 'cdaweb.dataset':
        dataset = datasets[dsid].get("allxml", None)
        if dataset is not None:
          # {"a": {"b": "c"}, ...} -> {"a/b": "c"}
          datasets[dsid]["allxml"] = utilrsw.flatten_dicts(dataset)
          datasets[dsid]["allxml"]['datasetID'] = dsid

      if table_name == 'cdaweb.variable':
        variables_array = []
        p = ['master','data', 'CDFVariables']
        variables = utilrsw.get_path(datasets[dsid], p)
        if variables is not None:
          for vid in variables:
            if variables[vid] is None:
              continue
            variables[vid] = utilrsw.flatten_dicts(variables[vid], simplify=True)
            # Put VariableName and datasetID at the top of the dict
            variables[vid] = {'datasetID': dsid, 'VariableName': vid, **variables[vid]}
            variables_array.append(variables[vid])
        datasets[dsid]['master']['data']['CDFVariables'] = variables_array

  if table_name.startswith('spase'):
    for dsid in datasets.keys():
      p = ['spase', 'data', 'Spase', 'NumericalData', 'Parameter']
      parameters = utilrsw.get_path(datasets[dsid], p)
      if parameters is None:
        continue
      if isinstance(parameters, dict):
        # If only one parameter, make it a list of one parameter.
        parameters = [parameters]
      for i in range(0, len(parameters)):
        parameters[i]['datasetID'] = dsid
        parameters[i] = utilrsw.flatten_dicts(parameters[i])
      datasets[dsid]['spase']['data']['Spase']['NumericalData']['Parameter'] = parameters

  if table_name.startswith('hapi'):
    datasets_expanded = {}
    for dsid in datasets.keys():
      sub_datasets = datasets[dsid]['hapi']['data']
      if sub_datasets is None:
        logger.warning(f"No hapi datasets for {dsid}. Skipping.")
        continue
      if isinstance(sub_datasets, dict):
        sub_datasets = [sub_datasets]
      for sub_dataset in sub_datasets:
        sdsid = sub_dataset['id']
        sub_dataset = cdawmeta.restructure.hapi(sub_dataset, simplify_bins=True)
        sub_dataset['id'] = sdsid # Add id back in
        datasets_expanded[sdsid] = {'id': sdsid, 'hapi': {'data': sub_dataset}}
        # Convert parameters object to an array.
        p = ['hapi', 'data', 'parameters']
        parameters = utilrsw.get_path(datasets_expanded[sdsid], p)
        parameters_new = []
        if parameters:
          for key in parameters:
            parameters[key]['id'] = dsid
            parameters_new.append(parameters[key])
          datasets_expanded[sdsid]['hapi']['data']['parameters'] = parameters_new
    datasets = datasets_expanded

  table_config = table_configs[table_name]

  logger.info(40*"-")
  logger.info(f"Creating table '{table_name}'")
  header, body, attribute_counts = _table(datasets, table_config)

  if len(body) > 0 and len(header) != len(body[0]):
    emsg = f"len(header) == {len(header)} != len(body[0]) = {len(body[0])}"
    raise Exception(emsg)

  if len(body) == 0:
    raise Exception(f"No rows in {table_name} table for id='{id}'")

  data_dir = os.path.join(cdawmeta.DATA_DIR, 'table')
  if id is not None:
    logger.warning("Using id to create subdirectory for table files. If id is a regex, expect trouble.")
    data_dir = os.path.join(data_dir, 'partial', id)

  info = _write_files(table_name, table_config, data_dir, header, body, attribute_counts)

  if embed_data:
    info['header'] = header
    info['body'] = body
    if attribute_counts is not None:
      info['counts'] = {}
      for count in attribute_counts:
        info['counts'][count[0]] = count[1]

  return info

def _table(datasets, table_config):

  attributes = {}
  paths = table_config['paths']
  for path in paths:
    attributes[path] = table_config['paths'][path]

  path_type = table_config.get('path_type', 'dict')
  if path_type == 'list':
    if len(paths) != 1:
      emsg = "Error: If path_type = 'list', only one path may be given."
      raise Exception(emsg)
    path = list(paths.keys())[0]
    attributes[path] = utilrsw.flatten_dicts(paths[path], simplify=True)
    paths[path] = utilrsw.flatten_dicts(paths[path], simplify=True)

  attribute_counts = None
  if table_config['use_all_attributes']:
    # Modify attributes dict to include all unique attributes found in all
    # variables. If an attribute is misspelled, it is mapped to the correct
    # spelling and placed in the attributes dict if there is a fixes for in
    # table_name config.json. The return value of attributes_all is a list 
    # of all uncorrected attribute names encountered.
    import collections
    attributes_all = _table_walk(datasets, attributes, table_config, path_type, mode='attributes')
    attribute_counts = collections.Counter(attributes_all)
    attribute_counts = sorted(attribute_counts.items(), key=lambda i: i[0].lower())

  # Create table header based on attributes dict.
  header = _table_header(attributes, table_config.get('id_name', None))

  logger.info("Creating table row(s)")
  table = _table_walk(datasets, attributes, table_config, path_type, mode='rows')
  logger.info(f"Created {len(table)} table row(s)")

  return header, table, attribute_counts

def _table_walk(datasets, attributes, table_config, path_type, mode='attributes'):

  """
  If mode='attributes', returns a dictionary of attributes found across all
  datasets and paths starting with the given attributes. If the attribute
  is misspelled, it is mapped to the correct spelling.

  If mode='rows', returns a list of rows for the table. Each row contains the
  value of the associated attribute (accounting for misspellings) in the given
  attributes dictionary. If the path does not have the attribute, an empty
  string is used for the associated column.
  """

  assert mode in ['attributes', 'rows']

  omit_attributes = table_config.get('omit_attributes', None)

  fixes = None
  if 'fix_attributes' in table_config:
    if table_config['fix_attributes']:
      if 'fixes' in table_config:
        if mode == 'attributes':
          logger.info("Using fixes found in config")
        fixes = table_config['fixes']
      else:
        msg = "Error: 'fix_attributes' = True, but 'fixes' in config."
        logger.error(msg)

  if mode == 'attributes':
    attribute_names = []
  else:
    table = []
    row = []

  n_cols_last = None
  datasets = copy.deepcopy(datasets)

  paths = attributes.keys()

  for id, dataset in datasets.items():
    logger.info(f"Computing {mode} for id = '{id}'")

    if path_type == 'dict':

      if mode == 'rows':
        row = []

      for path in paths:

        logger.info(f"  Reading {path}")

        data = utilrsw.get_path(dataset, path.split('/'))

        if data is None:
          if mode == 'rows':
            logger.info(f"    Path '{path}' not found. Inserting '?' for all attribute values.")
            # Insert "?" for all attributes
            n_attribs = len(attributes[path])
            fill = n_attribs*"?".split()
            row = [*row, *fill]
          continue

        if mode == 'attributes':
          _add_attributes(data, attributes[path], attribute_names, fixes, id + "/" + path, omit_attributes)
        else:
          _append_columns(data, attributes[path], row, fixes, omit_attributes)

      if mode == 'rows':
        logger.debug(f"  {len(row)} columns in row {len(table)}")
        if n_cols_last is not None and len(row) != n_cols_last:
          loc = id + "/" + path
          emsg = f"In {loc}, number of columns changed from"
          emsg += f"{n_cols_last} to {len(row)}."
          raise Exception(emsg)
        n_cols_last = len(row)
        table.append(row)

    else:

      # Get first (only) path
      path = next(iter(paths))
      data = utilrsw.get_path(dataset, path.split('/'))

      if data is None:
        logger.debug(f"  Path '{path}' not found in dataset '{id}'.")
        continue

      for variable in data:

        if mode == 'rows':
          row = []

        if mode == 'attributes':
          _add_attributes(variable, attributes[path], attribute_names, fixes, id + "/" + path, omit_attributes)
        else:
          _append_columns(variable, attributes[path], row, fixes, omit_attributes)

        # Add row for variable
        if mode == 'rows':
          logger.debug(f"  row #{len(table)}: {len(row)} columns")
          if n_cols_last is not None and len(row) != n_cols_last:
            loc = id + "/" + path
            emsg = f"In {loc}, number of columns changed from"
            emsg += f"{n_cols_last} to {len(row)}."
            raise Exception(emsg)
          n_cols_last = len(row)
          table.append(row)

  if mode == 'attributes':
    return attribute_names
  else:
    return table

def _table_header(attributes, id_name):

  header = []
  for path in attributes.keys():
    for attribute in attributes[path]:
      header.append(attribute)

  return header

def _append_columns(data, attributes, row, fixes, omit_attributes):

  for attribute in attributes:

    if omit_attributes is not None and attribute in omit_attributes:
      logger.info(f"  Skipping {attribute}")
      continue

    if fixes is not None:
      for fix in fixes:
        if fix in data:
          data[fixes[fix]] = data[fix]
          del data[fix]

    if attribute in data:
      val = data[attribute]
      if isinstance(val, str) and val == " ":
        val = val.replace(' ', '⎵')
      row.append(val)
    else:
      row.append("")

def _add_attributes(data, attributes, attribute_names, fixes, path, omit_attributes):

  for attribute_name in data:

    if omit_attributes is not None and attribute_name in omit_attributes:
      logger.info(f"  Skipping {attribute_name}")
      continue

    attribute_names.append(attribute_name)
    if fixes is None or attribute_name not in fixes:
      attributes[attribute_name] = None
    else:
      logger.warning(f"  Fixing attribute name: {path}/{attribute_name} -> {fixes[attribute_name]}")
      attributes[fixes[attribute_name]] = None


def _table_metadata(table_name, config, header, files):
  import datetime

  creationDate = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
  columnDefinitions = config.get('column_definitions', {})
  table_metadata = {
    "description": config.get('description', ""),
    "creationDate": creationDate,
    "columnDefinitions": columnDefinitions
  }

  for column_name in header:
    if column_name not in columnDefinitions:
      logger.warning(f"   Column name '{column_name}' not in column_definitions for table '{table_name}'")
      table_metadata["columnDefinitions"][column_name] = None

  return table_metadata

def _write_files(table_name, table_config, data_dir, header, body, counts):

  files = {
    'meta': f'{table_name}.meta.json',
    'header': f'{table_name}.head.json',
    'body': f'{table_name}.body.json',
    'csv': f'{table_name}.csv',
    'sql': f'{table_name}.sql',
    'counts': f'{table_name}.attribute_counts.csv'
  }

  metadata = _table_metadata(table_name, table_config, header, files)

  for key in files:
    print(data_dir, files[key])
    files[key] = os.path.join(data_dir, files[key])

  if counts is None:
    del files['counts']
  else:
    logger.info(f"Writing: {files['counts']}")
    utilrsw.write(files['counts'], [["attribute", "count"], *counts])

  logger.info(f"Writing: {files['meta']}")
  utilrsw.write(files['meta'], metadata)

  logger.info(f"Writing: {files['header']}")
  utilrsw.write(files['header'], header)

  logger.info(f"Writing: {files['body']}")
  utilrsw.write(files['body'], body)

  logger.info(f"Writing: {files['csv']}")
  utilrsw.write(files['csv'], [header, *body])

  logger.info(f"Writing: {files['sql']}")
  _write_sqldb(table_name, header, body, f"{files['sql']}", metadata)

  return files

def _write_sqldb(name, header, body, file, metadata):
  indent = "   "
  import sqlite3

  if os.path.exists(file):
    logger.info(f"{indent}Removing existing SQLite database file '{file}'")
    os.remove(file)

  header, body = _sql_prep(header, body)

  for hidx, colname in enumerate(header):
    header[hidx] = f"`{colname}`"

  column_names = f"({', '.join(header)})"
  column_spec  = f"({', '.join(header)} TEXT)"
  column_vals  = f"({', '.join(len(header)*['?'])})"

  create  = f'CREATE TABLE `{name}` {column_spec}'
  execute = f'INSERT INTO `{name}` {column_names} VALUES {column_vals}'

  logger.debug(f"{indent}Creating and connecting to file '{file}'")
  conn = sqlite3.connect(file)
  logger.debug(f"{indent}Created and connecting to file '{file}'")

  logger.info(f"{indent}Getting cursor from connection to '{file}'")
  cursor = conn.cursor()
  logger.info(f"{indent}Got cursor from connection to '{file}'")

  logger.debug(f"{indent}Creating index using cursor.execute('{create}')")
  cursor.execute(create)
  logger.debug(f"{indent}Done")

  logger.info(f"{indent}Inserting rows")
  logger.debug(f"{indent}using cursor.executemany('{execute}', body)")
  cursor.executemany(execute, body)
  logger.info(f"{indent}Done")

  logger.debug(f"{indent}Executing: connection.commit()")
  conn.commit()
  logger.debug(f"{indent}Done")

  if header is not None:
    index = f"CREATE INDEX idx0 ON `{name}` ({header[0]})"
    logger.debug(f"{indent}Creating index using cursor.execute('{index}')")
    cursor.execute(index)
    logger.debug(f"{indent}Done")

    logger.debug(f"{indent}Executing: commit()")
    conn.commit()
    logger.debug(f"{indent}Done")

  conn.close()

  conn = sqlite3.connect(file)
  cursor = conn.cursor()
  name_desc = f'{name}.metadata'
  logger.info(f"{indent}Creating table {name_desc} with table metadata stored as a JSON string")

  spec = "(TableName TEXT NOT NULL, Metadata TEXT)"
  execute = f"CREATE TABLE `{name_desc}` {spec}"
  logger.debug(f"{indent}Executing: {execute}")
  conn.execute(execute)
  logger.debug(f"{indent}Done")

  import json
  metadata = json.dumps(metadata)
  metadata = metadata.replace("'","''")
  values = f"('{name_desc}', '{metadata}')"
  insert = f'INSERT INTO `{name_desc}` ("TableName", "Metadata") VALUES {values}'
  logger.debug(f"{indent}Executing: connection.execute('{insert})'")
  conn.execute(insert)
  logger.debug(f"{indent}Done.")
  conn.commit()
  conn.close()

def _sql_prep(header, body):
  import time

  indent = "   "

  def unique(header):

    headerlc = [val.lower() for val in header]
    headeru = header.copy()
    for val in header:
      indices = [i for i, x in enumerate(headerlc) if x == val.lower()]
      if len(indices) > 1:
        dups = [header[i] for i in indices]
        logger.warning(f"{indent}Duplicate column names when cast to lower case: {str(dups)}.")
        logger.warning(f"{indent}Renaming duplicates by appending _$DUPLICATE_NUMBER$ to the column name.")
        for r, idx in enumerate(indices):
          if r > 0:
            newname = header[idx] + "_$" + str(r) + "$"
            logger.info(f"{indent}Renaming {header[idx]} to {newname}")
            headeru[idx] = newname
    return headeru


  logger.info(f"{indent}Renaming non-unique column names")
  header = unique(header)
  logger.info(f"{indent}Renamed non-unique column names")

  logger.info(f"{indent}Casting table elements to str.")
  start = time.time()
  for i, row in enumerate(body):
    for j, _ in enumerate(row):
      body[i][j] = str(body[i][j])

  dt = "{:.2f} [s]".format(time.time() - start)
  logger.info(f"{indent}Casted table elements to str in {len(body)} rows and {len(header)} columns in {dt}")

  return header, body
