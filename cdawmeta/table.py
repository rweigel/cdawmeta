import os
import copy

import cdawmeta

logger = None

def table(id=None, table_name=None, embed_data=False, skip=None, update=False, regen=False, max_workers=1, log_level='info'):

  global logger
  if logger is None:
    logger = cdawmeta.logger('table')
    logger.setLevel(log_level.upper())

  table_names = list(cdawmeta.CONFIG['table'].keys())
  if table_name is not None:
    if table_name not in table_names:
      raise ValueError(f"table_name='{table_name}' not in {table_names} in config.json")
    table_names = [table_name]

  datasets = cdawmeta.metadata(id=id, skip=skip, update=update, regen=regen, embed_data=True, diffs=False, max_workers=max_workers)
  info = {}
  for table_name in table_names:
    logger.info(f"Creating table '{table_name}'")
    header, body, attribute_counts = _table(datasets, table_name=table_name)
    if len(body) > 0 and len(header) != len(body[0]):
      raise Exception(f"len(header) == {len(header)} != len(body[0]) = {len(body[0])}")
    if len(body) == 0:
      raise Exception(f"No rows in {table_name} table for id='{id}'")

    info[table_name] = _write_files(table_name, header, body, attribute_counts, id=id)

    if embed_data:
      info[table_name]['header'] = header
      info[table_name]['body'] = body
      if attribute_counts is not None:
        info[table_name]['counts'] = attribute_counts

  if len(table_names) == 1:
    return info[table_names[0]]
  return info

def _table(datasets, table_name='cdaweb.dataset'):


  attributes = {}
  paths = cdawmeta.CONFIG['table'][table_name]['paths']
  for path in paths:
    attributes[path] = cdawmeta.CONFIG['table'][table_name]['paths'][path]


  attribute_counts = None
  if cdawmeta.CONFIG['table'][table_name]['use_all_attributes']:
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

  logger.info(f"Creating {table_name} table rows")
  table = _table_walk(datasets, attributes, table_name, mode='rows')
  logger.info(f"Created {len(table)} {table_name} table rows")

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

  omit_attributes = cdawmeta.CONFIG['table'][table_name].get('omit_attributes', None)

  fixes = None
  if 'fix_attributes' in cdawmeta.CONFIG['table'][table_name]:
    if cdawmeta.CONFIG['table'][table_name]['fix_attributes']:
      if 'fixes' in cdawmeta.CONFIG['table'][table_name]:
        fixes_file = os.path.join(os.path.dirname(cdawmeta.__file__), 'config.json')
        logger.info(f"with fixes for {table_name} found in {fixes_file}")
        fixes = cdawmeta.CONFIG['table'][table_name]['fixes']
      else:
        msg = f"Error: cdawmeta.CONFIG['table'][{table_name}]['fix_attributes'] = True, "
        msg += "but no file cdawmeta.CONFIG['table'][{table_name}]['fixes'] set."
        logger.error(msg)

  if mode == 'attributes':
    attribute_names = []
  else:
    table = []
    row = []

  n_cols_last = None
  for id, dataset in datasets.items():
    logger.info(f"Computing {mode} for {table_name}/{id}")
    if omit_attributes is not None:
      dataset = copy.deepcopy(dataset)

    if table_name == 'cdaweb.dataset' or table_name == 'spase.dataset':
      if mode == 'rows':
        row = [dataset['id']]

      for path in attributes.keys():

        logger.info(f"  Reading {path}")

        data = cdawmeta.util.get_path(dataset, path.split('/'))

        if table_name == 'cdaweb.dataset' and path == 'allxml':
          data = cdawmeta.util.flatten_dicts(data)

        if data is None:
          if mode == 'rows':
            logger.info(f"    Path '{path}' not found. Inserting '?' for all attribute values.")
            # Insert "?" for all attributes
            n_attribs = len(attributes[path])
            fill = n_attribs*"?".split()
            row = [*row, *fill]
          continue

        if omit_attributes is not None:
          for key in list(data.keys()):
            for omit in omit_attributes:
              cdawmeta.util.rm_path(data, omit.split('/'))

        if mode == 'attributes':
          _add_attributes(data, attributes[path], attribute_names, fixes, id + "/" + path)
        else:
          _append_columns(data, attributes[path], row, fixes)

      if mode == 'rows':
        logger.info(f"  {len(row)} columns")
        if n_cols_last is not None and len(row) != n_cols_last:
          raise Exception(f"Number of columns changed from {n_cols_last} to {len(row)} for {id}")
        n_cols_last = len(row)
        table.append(row)

    if table_name == 'cdaweb.variable' or table_name == 'spase.parameter':

      for path in attributes.keys():

        data = cdawmeta.util.get_path(dataset, path.split('/'))
        if data is None:
          continue

        for variable_name, variable in data.items():

          if mode == 'rows':
            row = [dataset['id']]
            if table_name == 'cdaweb.variable':
              row = [dataset['id'], variable_name]

          if table_name == 'spase.parameter':
            for key in variable.copy():
              # Drop attribute if value is list
              if isinstance(variable[key], list):
                del variable[key]
            if mode == 'attributes':
              _add_attributes(variable, attributes[path], attribute_names, fixes, id + "/" + path)
            else:
              _append_columns(variable, attributes[path], row, fixes)

          else:
            for subpath in attributes[path]:
              if subpath in data[variable_name]:
                variable_ = data[variable_name][subpath]
                if mode == 'attributes':
                  _add_attributes(variable_, attributes[path][subpath], attribute_names, fixes, f"{id}/{path}/{variable_name}/{subpath}")
                else:
                  _append_columns(variable_, attributes[path][subpath], row, fixes)
              else:
                if mode == 'rows':
                  # Insert "?" for all attributes
                  n_attribs = len(attributes[path][subpath])
                  fill = n_attribs*"?".split()
                  row = [*row, *fill]

          # Add row for variable
          if mode == 'rows':
            logger.info(f"  {len(row)} columns")
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

  for path in attributes.keys():
    if table_name == 'cdaweb.dataset' or table_name.startswith('spase'):
      for attribute in attributes[path]:
        header.append(attribute)
    else:
      for subpath in attributes[path]:
        for subattribute in attributes[path][subpath]:
          header.append(subattribute)
  return header

def _append_columns(data, attributes, row, fixes):

  for attribute in attributes:

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

def _add_attributes(data, attributes, attribute_names, fixes, path):

  for attribute_name in data:
    attribute_names.append(attribute_name)
    if fixes is None or attribute_name not in fixes:
      attributes[attribute_name] = None
    else:
      logger.error(f"Fixing attribute name: {path}/{attribute_name} -> {fixes[attribute_name]}")
      attributes[fixes[attribute_name]] = None


def _files(table_name, id=None):
  data_dir = cdawmeta.DATA_DIR

  subdir = ''
  if id is not None:
    logger.warning("Using id to create subdirectory for table files. If id is a regex, expect trouble.")
    subdir = id
  files = {
    'header': os.path.join(data_dir, 'table', subdir, f'{table_name}.head.json'),
    'body': os.path.join(data_dir, 'table', subdir, f'{table_name}.body.json'),
    'csv': os.path.join(data_dir, 'table', subdir, f'{table_name}.csv'),
    'sql': os.path.join(data_dir, 'table', subdir, f'{table_name}.sql'),
    'counts': os.path.join(data_dir, 'table', subdir, f'{table_name}.attribute_counts.csv')
  }

  return files

def _write_files(table_name, header, body, counts, id=id):

  files = _files(table_name, id=id)

  if counts is None:
    del files['counts']
  else:
    logger.info(f"Writing: {files['counts']}")
    cdawmeta.util.write(files['counts'], counts)

  logger.info(f"Writing: {files['header']}")
  cdawmeta.util.write(files['header'], header)
  logger.info(f"Writing: {files['body']}")
  cdawmeta.util.write(files['body'], body)

  logger.info(f"Writing: {files['csv']}")
  cdawmeta.util.write(files['csv'], [header, *body])

  logger.info(f"Writing: {files['sql']}")
  if os.path.exists(files['sql']):
    logger.info(f"  Removing existing SQLite database file '{files['sql']}'")
    os.remove(files['sql'])
  _write_sqldb(header, body, file=f"{files['sql']}", name=table_name)

  return files

def _write_sqldb(header, body, file="table1.db", name="table1"):

  import sqlite3

  header, body = _sql_prep(header, body)

  for hidx, colname in enumerate(header):
    header[hidx] = f"`{colname}`"

  column_names = f"({', '.join(header)})"
  column_spec  = f"({', '.join(header)} TEXT)"
  column_vals  = f"({', '.join(len(header)*['?'])})"

  create  = f'CREATE TABLE `{name}` {column_spec}'
  execute = f'INSERT INTO `{name}` {column_names} VALUES {column_vals}'

  logger.info(f"  Creating and connecting to file '{file}'")
  conn = sqlite3.connect(file)
  logger.info(f"  Created and connecting to file '{file}'")

  logger.info(f"  Getting cursor from connection to '{file}'")
  cursor = conn.cursor()
  logger.info(f"  Got cursor from connection to '{file}'")

  logger.info(f"  Creating index using cursor.execute('{create}')")
  cursor.execute(create)
  logger.info("  Done")

  logger.info(f"  Inserting rows using cursor.executemany('{execute}', body)")
  cursor.executemany(execute, body)
  logger.info("  Done")

  logger.info("  Executing: commit()")
  conn.commit()
  logger.info("  Done")

  if header is not None:
    index = f"CREATE INDEX idx0 ON `{name}` ({header[0]})"
    logger.info(f"  Creating index using cursor.execute('{index}')")
    cursor.execute(index)
    logger.info("  Done")

    logger.info("  Executing: commit()")
    conn.commit()
    logger.info("  Done")

  conn.close()

def _sql_prep(header, body):
  import time

  def unique(header):

    headerlc = [val.lower() for val in header]
    headeru = header.copy()
    for val in header:
      indices = [i for i, x in enumerate(headerlc) if x == val.lower()]
      if len(indices) > 1:
        dups = [header[i] for i in indices]
        logger.error(f"Warning: Duplicate column names when cast to lower case: {str(dups)}.")
        logger.error("         Renaming duplicates by appending _$DUPLICATE_NUMBER$ to the column name.")
        for r, idx in enumerate(indices):
          if r > 0:
            newname = header[idx] + "_$" + str(r) + "$"
            logger.info(f"Renaming {header[idx]} to {newname}")
            headeru[idx] = newname
    return headeru

  logger.info("  Casting table elements to str.")
  start = time.time()

  logger.info("  Renaming non-unique column names")
  header = unique(header)
  logger.info("  Renamed non-unique column names")

  for i, row in enumerate(body):
    for j, _ in enumerate(row):
      body[i][j] = str(body[i][j])

  dt = "{:.2f} [s]".format(time.time() - start)
  logger.info(f"Casted table elements to str in {len(body)} rows and {len(header)} columns in {dt}")

  return header, body
