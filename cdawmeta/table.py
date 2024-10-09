import os
import copy

import cdawmeta

logger = None

def table(id=None, id_skip=None, table_name=None, embed_data=False,
          update=False, update_skip='',
          regen=False, regen_skip='',
          max_workers=3, log_level='info'):

  kwargs = locals()

  table_names = list(cdawmeta.CONFIG['table']['tables'].keys())

  if table_name is None:
    info = {}
    for table_name in table_names:
      kwargs['table_name'] = table_name
      info[table_name] = table(**kwargs)
    return info

  if table_name is not None:
    if table_name not in table_names:
      raise ValueError(f"table_name='{table_name}' not in {table_names} in config.json")

  global logger
  if logger is None:
    logger = cdawmeta.logger('table')
    logger.setLevel(log_level.upper())

  if table_name.startswith('spase'):
    meta_type = ['spase']
  elif table_name.startswith('cdaweb'):
    meta_type = ['allxml', 'master']
  elif table_name.startswith('hapi'):
    meta_type = ['hapi']

  datasets = cdawmeta.metadata(id=id, meta_type=meta_type)

  if table_name.startswith('spase'):
    for dsid in datasets.keys():
      logger.debug(f"{dsid}: Restructuring SPASE")
      datasets[dsid]['spase']['data'] = cdawmeta.restructure.spase(datasets[dsid]['spase']['data'])

  if table_name.startswith('hapi'):
    datasets_expanded = {}
    for dsid in datasets.keys():
      sub_datasets = datasets[dsid]['hapi']['data']
      if isinstance(sub_datasets, dict):
        sub_datasets = [sub_datasets]
      for sub_dataset in sub_datasets:
        sdsid = sub_dataset['id']
        sub_dataset = cdawmeta.restructure.hapi(sub_dataset, simplify_bins=True)
        datasets_expanded[sdsid] = {'id': sdsid, 'hapi': {'data': sub_dataset}}
    datasets = datasets_expanded


  logger.info(40*"-")
  logger.info(f"Creating table '{table_name}'")
  header, body, attribute_counts = _table(datasets, table_name=table_name)

  if len(body) > 0 and len(header) != len(body[0]):
    raise Exception(f"len(header) == {len(header)} != len(body[0]) = {len(body[0])}")

  if len(body) == 0:
    raise Exception(f"No rows in {table_name} table for id='{id}'")

  info = _write_files(table_name, header, body, attribute_counts, id=id)

  if embed_data:
    info['header'] = header
    info['body'] = body
    if attribute_counts is not None:
      info['counts'] = {}
      for count in attribute_counts:
        info['counts'][count[0]] = count[1]

  return info

def _table(datasets, table_name='cdaweb.dataset'):

  attributes = {}
  table_config = cdawmeta.CONFIG['table']['tables'][table_name]
  paths = table_config['paths']
  for path in paths:
    attributes[path] = table_config['paths'][path]

  attribute_counts = None
  if table_config['use_all_attributes']:
    # Modify attributes dict to include all unique attributes found in all variables. If
    # an attribute is misspelled, it is mapped to the correct spelling and placed
    # in the attributes dict if there is a fixes for table_name in config.json.
    # The return value of attributes_all is a list of all uncorrected attribute
    # names encountered.
    import collections
    attributes_all = _table_walk(datasets, attributes, table_name, mode='attributes')
    attribute_counts = collections.Counter(attributes_all)
    attribute_counts = sorted(attribute_counts.items(), key=lambda i: i[0].lower())

  # Create table header based on attributes dict.
  header = _table_header(attributes, table_name)

  logger.info(f"Creating {table_name} table row(s)")
  table = _table_walk(datasets, attributes, table_name, mode='rows')
  logger.info(f"Created {len(table)} {table_name} table row(s)")

  return header, table, attribute_counts

def _table_walk(datasets, attributes, table_name, mode='attributes'):

  """
  If mode='attributes', returns a dictionary of attributes found across all
  datasets and variables starting with the given attributes. If the attribute
  is misspelled, it is mapped to the correct spelling.

  If mode='rows', returns a list of rows for the table. Each row contains the
  value of the associated attribute (accounting for misspellings) in the given
  attributes dictionary. If the variable does not have the attribute, an empty
  string is used for the column.
  """

  assert mode in ['attributes', 'rows']

  table_config = cdawmeta.CONFIG['table']['tables'][table_name]
  omit_attributes = table_config.get('omit_attributes', None)

  fixes = None
  if 'fix_attributes' in table_config:
    if table_config['fix_attributes']:
      if 'fixes' in table_config:
        fixes_file = os.path.join(os.path.dirname(cdawmeta.__file__), 'config.json')
        if mode == 'attributes':
          logger.info(f"Using fixes for {table_name} found in ")
          logger.info(f"  {fixes_file}")
        fixes = table_config['fixes']
      else:
        msg = f"Error: cdawmeta.CONFIG['table']['tables'][{table_name}]['fix_attributes'] = True, "
        msg += "but no file cdawmeta.CONFIG['table']['tables'][{table_name}]['fixes'] set."
        logger.error(msg)

  if mode == 'attributes':
    attribute_names = []
  else:
    table = []
    row = []

  n_cols_last = None
  datasets = copy.deepcopy(datasets)

  for id, dataset in datasets.items():
    logger.info(f"Computing {mode} for {table_name}/{id}")

    if table_name.endswith('dataset'):

      if mode == 'rows':
        row = [dataset['id']]

      for path in attributes.keys():

        logger.info(f"  Reading {path}")

        data = cdawmeta.util.get_path(dataset, path.split('/'))

        if table_name == 'cdaweb.dataset' and path == 'allxml':
          # {"a": {"b": "c"}, ...} -> {"a/b": "c"}
          data = cdawmeta.util.flatten_dicts(data)

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
          raise Exception(f"Number of columns changed from {n_cols_last} to {len(row)} for {id}")
        n_cols_last = len(row)
        table.append(row)

    else:

      for path in attributes.keys():

        data = cdawmeta.util.get_path(dataset, path.split('/'))
        if data is None:
          logger.debug(f"  Path '{path}' not found in dataset '{id}'.")
          continue

        for variable_name, variable in data.items():

          if mode == 'rows':
            row = [dataset['id']]
            if table_name == 'cdaweb.variable':
              row = [dataset['id'], variable_name]

          if table_name == 'spase.parameter' or table_name == 'hapi.parameter':
            for key in variable.copy():
              # Drop attribute if value is list
              if table_name == 'spase.parameter' and isinstance(variable[key], list):
                del variable[key]
            if mode == 'attributes':
              _add_attributes(variable, attributes[path], attribute_names, fixes, id + "/" + path, omit_attributes)
            else:
              _append_columns(variable, attributes[path], row, fixes, omit_attributes)
          else:
            for subpath in attributes[path]:
              if subpath in data[variable_name]:
                variable_ = data[variable_name][subpath]
                if mode == 'attributes':
                  _add_attributes(variable_, attributes[path][subpath], attribute_names, fixes, f"{id}/{path}/{variable_name}/{subpath}", omit_attributes)
                else:
                  _append_columns(variable_, attributes[path][subpath], row, fixes, omit_attributes)
              else:
                if mode == 'rows':
                  # Insert "?" for all attributes
                  n_attribs = len(attributes[path][subpath])
                  fill = n_attribs*"?".split()
                  row = [*row, *fill]

          # Add row for variable
          if mode == 'rows':
            logger.debug(f"  row #{len(table)}: {len(row)} columns")
            if n_cols_last is not None and len(row) != n_cols_last:
              raise Exception(f"Number of columns changed from {n_cols_last} to {len(row)} for {id}/{variable_name}")
            n_cols_last = len(row)
            table.append(row)

  if mode == 'attributes':
    return attribute_names
  else:
    return table

def _table_header(attributes, table_name):

  header = ['datasetID']
  if table_name == 'cdaweb.variable':
    header = ['datasetID', 'VariableName']
  if table_name == 'hapi.dataset':
    header = ['id']

  for path in attributes.keys():
    if table_name == 'cdaweb.variable':
      for subpath in attributes[path]:
        for subattribute in attributes[path][subpath]:
          header.append(subattribute)
    else:
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


def _files(table_name, id=None):
  data_dir = cdawmeta.DATA_DIR

  subdir = ''
  if id is not None:
    logger.warning("Using id to create subdirectory for table files. If id is a regex, expect trouble.")
    subdir = os.path.join('partial', id)
  files = {
    'header': os.path.join(data_dir, 'table', subdir, f'{table_name}.head.json'),
    'body': os.path.join(data_dir, 'table', subdir, f'{table_name}.body.json'),
    'csv': os.path.join(data_dir, 'table', subdir, f'{table_name}.csv'),
    'sql': os.path.join(data_dir, 'table', subdir, f'{table_name}.sql'),
    'counts': os.path.join(data_dir, 'table', subdir, f'{table_name}.attribute_counts.csv')
  }

  return files

def _table_metadata(table_name, header, files):
  import datetime
  creationDate = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

  data_dir = cdawmeta.DATA_DIR
  path = files['csv'].replace(data_dir, "data").replace("csv", "")

  config = cdawmeta.CONFIG['table']
  meta_link = f'<a href="{config["url"]}{path}meta.json">{table_name}.meta.json</a>'
  db_links = []
  for ext in ["csv", "sql", "json"]:
    href = f"{config['url']}{path}{ext}"
    db_link = f'<a href="{href}">{ext}</a>'
    db_links.append(db_link)
  db_links = " | ".join(db_links)

  description = cdawmeta.CONFIG['table']['description'].format(db_links=db_links, meta_link=meta_link)
  db_links = " | ".join(db_links)

  table_config = cdawmeta.CONFIG['table']['tables'][table_name]
  description = f"{description} {table_config['description']}"
  table_metadata = {
    "description": description,
    "creationDate": creationDate,
    "column_definitions": {}
  }
  column_defs = table_config['column_definitions']
  for column_name in header:
    if column_name not in column_defs:
      logger.warning(f"   Column name '{column_name}' not in column_definitions for table '{table_name}'")
    table_metadata["column_definitions"][column_name] = None

  return table_metadata

def _write_files(table_name, header, body, counts, id=id):

  files = _files(table_name, id=id)

  table_metadata = _table_metadata(table_name, header, files)

  if counts is None:
    del files['counts']
  else:
    logger.info(f"Writing: {files['counts']}")
    cdawmeta.util.write(files['counts'], [["attribute", "count"], *counts])

  file = files['header'].replace("head", "meta")
  logger.info(f"Writing: {file}")
  cdawmeta.util.write(file, table_metadata)

  logger.info(f"Writing: {files['header']}")
  cdawmeta.util.write(files['header'], header)
  logger.info(f"Writing: {files['body']}")
  cdawmeta.util.write(files['body'], body)

  logger.info(f"Writing: {files['csv']}")
  cdawmeta.util.write(files['csv'], [header, *body])

  logger.info(f"Writing: {files['sql']}")
  _write_sqldb(table_name, header, body, f"{files['sql']}", table_metadata)

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
