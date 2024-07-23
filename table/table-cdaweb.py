# Put links in header to, e.g.,
# https://spdf.gsfc.nasa.gov/istp_guide/vattributes.html#FILLVAL

def omit(id):
  return False
  if not id.startswith('A'):
    return True
  return False

import os
import json

root_dir    = os.path.join(os.path.dirname(__file__), '..')
base_dir    = os.path.join(root_dir, 'data')
file_body   = os.path.join(base_dir, 'tables/cdaweb.table.body.json')
file_header = os.path.join(base_dir, 'tables/cdaweb.table.head.json')
file_counts = os.path.join(os.path.dirname(__file__), 'table-cdaweb.fixes.counts.csv')
file_fixes  = os.path.join(os.path.dirname(__file__), 'table-cdaweb.fixes.json')

apply_fixes = True
use_all_attributes = True

if apply_fixes:
  print(f'Reading: {file_fixes}')
  with open(file_fixes) as f:
    fixes = json.load(f)
  print(f'Read: {file_fixes}')

def write_counts(attribute_names):
  import collections
  counts = dict(collections.Counter(attribute_names))
  counts_sorted = sorted(counts.items(), key=lambda i: i[0].lower())
  print(f'Writing: {file_counts}')
  with open(file_counts, 'w', encoding='utf-8') as f:
    f.write("Attribute, Count\n")
    for count in counts_sorted:
      f.write(f"{count[0]}, {count[1]}\n")
  print(f'Wrote: {file_counts}')

def all_attribute_table(datasets):

  def all_attributes(datasets):

    attribute_names = []

    attributes = {
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

    if use_all_attributes == True:
      for _, dataset in datasets.items():
        for name, variable in dataset['master']['data']['CDFVariables'].items():
          for attribute_type in ['VarDescription', 'VarAttributes']:
            if not attribute_type in variable:
              print("Missing " + attribute_type + " in " + name + " in " + dataset['id'])
              continue
            for attribute in variable[attribute_type]:
              attribute_names.append(attribute)
              if apply_fixes:
                if attribute not in fixes:
                  attributes[attribute_type][attribute] = None
              else:
                attributes[attribute_type][attribute] = None

    write_counts(attribute_names)
    return attributes

  attributes = all_attributes(datasets)

  header = ['datasetID', 'VariableName']
  for attribute in attributes['VarDescription']:
    header.append(attribute)
  for attribute in attributes['VarAttributes']:
    header.append(attribute)

  table = []
  for id, dataset in datasets.items():
    if omit(id) == True:
      continue
    for name, variable in dataset['master']['data']['CDFVariables'].items():
      row = [dataset['id'], name]
      for attribute_type in ['VarDescription', 'VarAttributes']:

        if not attribute_type in variable:
          for attribute in attributes[attribute_type]:
            row.append("?") # No VarDescription or VarAttributes
          continue

        for attribute in attributes[attribute_type]:

          if apply_fixes:
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

      table.append(row)

  return header, table

import cdawmeta
datasets = cdawmeta.metadata(data_dir='../data', update=False, embed_data=True)

print("Creating table")
header, table = all_attribute_table(datasets)
print("Created table")

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
