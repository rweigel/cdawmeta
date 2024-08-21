# Put links in header to, e.g.,
# https://spdf.gsfc.nasa.gov/istp_guide/vattributes.html#FILLVAL

def omit(id):
  return False
  if not id.startswith('A'):
    return True
  return False

import os
import json

use_all_attributes = True
fix_dataset_attributes = True
fix_variable_attributes = True

root_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
table_dir = os.path.join(root_dir, 'data', 'table')
report_dir = os.path.join(os.path.dirname(__file__), 'report')

files = {
  'dataset': {
    'header': os.path.join(table_dir, 'cdaweb.table.dataset.head.json'),
    'body': os.path.join(table_dir, 'cdaweb.table.dataset.body.json'),
    'counts': os.path.join(report_dir, 'cdaweb.table.dataset_attributes.counts.csv'),
    'fixes': os.path.join(report_dir, 'cdaweb.table.dataset_attributes.fixes.json')
  }
}
files['variable'] = {}
for key, value in files['dataset'].items():
  files['variable'][key] = value.replace("dataset", "variable")

if not fix_dataset_attributes:
  files['dataset']['fixes'] = None
if not fix_variable_attributes:
  files['variable']['fixes'] = None

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

def attribute_table(datasets, table_name='dataset'):

  attribute_names = []
  attributes = _attributes()[table_name]
  attribute_cats = list(attributes.keys())

  fixes = read_fixes(files[table_name]['fixes'])

  if use_all_attributes == True:
    for _, dataset in datasets.items():
      if table_name == 'dataset':
        add_attribute(attributes, attribute_names, attribute_cats, dataset['master']['data'], dataset['id'], None, fixes)
      else:
        for name, variable in dataset['master']['data']['CDFVariables'].items():
          add_attribute(attributes, attribute_names, attribute_cats, variable, dataset['id'], name, fixes)

  write_counts(attribute_names, file=files[table_name]['counts'])

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

    if omit(id) == True:
      continue

    if table_name == 'dataset':
        row = [dataset['id']]
        for attribute_cat in attribute_cats:
          append_row(row, attributes, attribute_cat, dataset['master']['data'], fixes)
    else:
      for name, variable in dataset['master']['data']['CDFVariables'].items():
        row = [dataset['id'], name]
        for attribute_cat in attribute_cats:
          append_row(row, attributes, attribute_cat, variable, fixes)

    table.append(row)

  print(f"Created {table_name} table rows")

  return header, table

def append_row(row, attributes, attribute_type, variable, fixes):

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
  return row

def add_attribute(attributes, attribute_names, attribute_types, variable, id, name, fixes):
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

def write_counts(attribute_names, file=None):
  import collections
  counts = dict(collections.Counter(attribute_names))
  counts_sorted = sorted(counts.items(), key=lambda i: i[0].lower())
  print(f'Writing: {file}')
  with open(file, 'w', encoding='utf-8') as f:
    f.write("Attribute, Count\n")
    for count in counts_sorted:
      f.write(f"{count[0]}, {count[1]}\n")
  print(f'Wrote: {file}')

def write_table(file_header, file_body):
  print(f'Writing: {file_header}')
  os.makedirs(os.path.dirname(file_header), exist_ok=True)
  with open(file_header, 'w', encoding='utf-8') as f:
    json.dump(header, f, indent=2)
    print(f'Wrote: {file_body}')

  print(f'Writing: {file_header}')
  os.makedirs(os.path.dirname(file_body), exist_ok=True)
  with open(file_body, 'w', encoding='utf-8') as f:
    json.dump(table, f, indent=2)
    print(f'Wrote: {file_body}')

def read_fixes(file):
  if file is not None:
    print(f"Reading: {file}")
    with open(file) as f:
      fixes = json.load(f)
    print(f"Read: {file}")
  return fixes

import cdawmeta
datasets = cdawmeta.metadata(id="^A", data_dir='../data', update=False, embed_data=True)
#datasets = cdawmeta.metadata(data_dir='../data', update=False, embed_data=True)

for table_name in ['dataset', 'variable']:
  print(f"Creating {table_name} attribute table")
  header, table = attribute_table(datasets, table_name=table_name)
  assert len(header) == len(table[0])
  print(f"Creating {table_name} attribute table")
  write_table(files[table_name]['header'], files[table_name]['body'])
