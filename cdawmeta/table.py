import os
import json

# TODO: Should be passed to table()
options = {
  'dataset': {
    'use_all_attributes': True,
    'fix_attributes': True,
  },
  'variable': {
    'use_all_attributes': True,
    'fix_attributes': True,
  }
}

def table(id=None, table_name=None, data_dir=None, update=False, max_workers=1):

  import cdawmeta

  if data_dir is None:
    from . import DATA_DIR as data_dir

  datasets = cdawmeta.metadata(id=id, data_dir=data_dir, update=update, embed_data=True, max_workers=max_workers)

  if table_name is None:
    table_names = ['dataset', 'variable']
  else:
    table_names = [table_name]

  headers = {}
  bodies = {}
  for table_name in table_names:
    print(f"Creating {table_name} attribute table")
    headers[table_name], bodies[table_name] = _table(datasets, data_dir, table_name=table_name)
    print(f"Creating {table_name} attribute table with {len(headers[table_name])} columns and {len(bodies[table_name])} rows")
    assert len(headers[table_name]) == len(bodies[table_name][0])
    files = _files(table_name, data_dir)
    _write_table(headers[table_name], files['header'], bodies[table_name], files['body'])

  if len(table_names) == 1:
    return headers[table_names[0]], bodies[table_names[0]]
  else:
    return headers, bodies

def _table(datasets, data_dir, table_name='dataset'):

  attribute_names = []
  attributes = _attributes()[table_name]
  attribute_cats = list(attributes.keys())

  fixes = _read_fixes(table_name, data_dir)

  if options[table_name]['use_all_attributes'] == True:
    for _, dataset in datasets.items():
      if table_name == 'dataset':
        _add_attribute(attributes, attribute_names, attribute_cats, dataset['master']['data'], dataset['id'], None, fixes)
      else:
        for name, variable in dataset['master']['data']['CDFVariables'].items():
          _add_attribute(attributes, attribute_names, attribute_cats, variable, dataset['id'], name, fixes)

  _write_counts(attribute_names, file=_files(table_name, data_dir)['counts'])

  # Manually set first two columns
  if table_name == 'dataset':
    header = ['datasetID']
  else:
    header = ['datasetID', 'VariableName']

  for attribute_cat in attribute_cats:
    for attribute in attributes[attribute_cat]:
      header.append(attribute)

  table = []
  row = []
  print(f"Creating {table_name} table rows")
  for id, dataset in datasets.items():

    if _omit(id) == True:
      continue

    if table_name == 'dataset':
      row = [dataset['id']]
      for attribute_cat in attribute_cats:
        _append_columns(row, attributes, attribute_cat, dataset['master']['data'], fixes)
      table.append(row)
    else:
      for name, variable in dataset['master']['data']['CDFVariables'].items():
        row = [dataset['id'], name]
        for attribute_cat in attribute_cats:
          _append_columns(row, attributes, attribute_cat, variable, fixes)
        table.append(row)

  print(f"Created {table_name} table rows")

  return header, table

def _omit(id):
  return False
  if not id.startswith('A'):
    return True
  return False

def _files(table_name, data_dir):
  script_dir = os.path.dirname(__file__)
  files = {
    'header': os.path.join(data_dir, f'cdaweb.table.{table_name}.head.json'),
    'body': os.path.join(data_dir, f'cdaweb.table.{table_name}.body.json'),
    'counts': os.path.join(data_dir, f'cdaweb.table.{table_name}_attributes.counts.csv'),
    'fixes': os.path.join(script_dir, f'table.fixes.json')
  }

  if not options[table_name]['fix_attributes']:
    files['fixes'] = None

  return files

def _attributes():
  # This is used to set the ordering of certain attributes. Attributes not
  # in this list will be added to the end of the list.
  ret = {
    'dataset': {
      "CDFFileInfo": {
        "FileName": None,
        "FileVersion": None,
        "Format": None,
        "Majority": None,
        "Encoding": None
      },
      "CDFglobalAttributes": {
        "TITLE": None,
        "Project": None,
        "Discipline": None,
        "Source_name": None,
        "Data_version": None,
        "ADID_ref": None,
        "Logical_file_id": None,
        "Data_type": None,
        "Descriptor": None,
        "TEXT": None,
        "MODS": None,
        "Logical_source": None,
        "Logical_source_description": None,
        "PI_name": None,
        "PI_affiliation": None,
        "Mission_group": None,
        "Instrument_type": None,
        "TEXT_supplement_1": None,
        "Generation_date": None,
        "Acknowledgement": None,
        "Rules_of_use": None,
        "Generated_by": None,
        "Time_resolution": None,
        "Link_text": None,
        "Link_title": None,
        "HTTP_Link": None,
        "alt_logical_source": None,
        "spase_DatasetResourceID": None
      }
    },
    'variable': {
      'VarDescription': {
        "PadValue": None,
        "RecVariance": None,
        "NumDims": None,
        "DataType": None,
        "DimVariances": None,
        "NumElements": None
      },
      'VarAttributes': {
        "FIELDNAM": None,
        'VAR_TYPE': None,
        'DICT_KEY': None,
        'DEPEND_0': None,
        'DEPEND_1': None,
        'DEPEND_2': None,
        'DEPEND_3': None,
        'DELTA_PLUS_VAR': None,
        'DELTA_MINUS_VAR': None,
        'FORMAT': None,
        'FORM_PTR': None,
        'BIN_LOCATION': None,
        'LABLAXIS': None,
        'LABL_PTR_1': None,
        'LABL_PTR_2': None,
        'LABL_PTR_3': None,
        'VAR_NOTES': None,
        'VARIABLE_PURPOSE': None,
        'AVG_TYPE': None,
        'FILLVAL': None,
        'UNITS': None,
        'UNITS_PTR': None,
        'SI_CONVERSION': None,
        'COORDINATE_SYSTEM': None,
        'VIRTUAL': None,
        'FUNCT': None,
        'FUNCTION': None,
        'SCALETYP': None,
        'SCAL_PTR': None,
        'VALID_MIN': None,
        'VALID_MAX': None,
      }
    }
  }
  return ret

def _append_columns(row, attributes, attribute_type, variable, fixes):

  if not attribute_type in variable:
    for attribute in attributes[attribute_type]:
      row.append("?") # No VarDescription or VarAttributes
    return row

  for attribute in attributes[attribute_type]:

    if fixes is not None:
      for fix in fixes:
        if fix in variable[attribute_type]:
          variable[attribute_type][fixes[fix]] = variable[attribute_type][fix]
          del variable[attribute_type][fix]

    if attribute in variable[attribute_type]:
      val = variable[attribute_type][attribute]
      if isinstance(val, str) and val == " ":
        val = val.replace(' ', '⎵')
      row.append(val)
    else:
      row.append("")

def _add_attribute(attributes, attribute_names, attribute_types, variable, id, name, fixes):
  for attribute_type in attribute_types:
    if not attribute_type in variable:
      if name is None:
        print(f"Missing {attribute_type} in {id}")
      else:
        print(f"Missing {attribute_type} in {name} in {id}")
      continue
    for attribute_name in variable[attribute_type]:
      attribute_names.append(attribute_name)
      if fixes is not None:
        if attribute_name not in fixes:
          attributes[attribute_type][attribute_name] = None
      else:
        attributes[attribute_type][attribute_name] = None

def _write_counts(attribute_names, file=None):
  import collections
  counts = dict(collections.Counter(attribute_names))
  counts_sorted = sorted(counts.items(), key=lambda i: i[0].lower())
  print(f'Writing: {file}')
  with open(file, 'w', encoding='utf-8') as f:
    f.write("Attribute, Count\n")
    for count in counts_sorted:
      f.write(f"{count[0]}, {count[1]}\n")
  print(f'Wrote: {file}')

def _write_table(header, file_header, body, file_body):

  print(f'Writing: {file_header}')
  os.makedirs(os.path.dirname(file_header), exist_ok=True)
  with open(file_header, 'w', encoding='utf-8') as f:
    json.dump(header, f, indent=2)
    print(f'Wrote: {file_body}')

  print(f'Writing: {file_body}')
  os.makedirs(os.path.dirname(file_body), exist_ok=True)
  with open(file_body, 'w', encoding='utf-8') as f:
    json.dump(body, f, indent=2)
    print(f'Wrote: {file_body}')

def _read_fixes(table_name, data_dir):
  file = _files(table_name, data_dir)['fixes']
  if file is not None:
    print(f"Reading: {file}")
    with open(file) as f:
      fixes = json.load(f)
    print(f"Read: {file}")
  return fixes[table_name]
