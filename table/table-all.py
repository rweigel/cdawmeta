# Put links in header to, e.g., 
# https://spdf.gsfc.nasa.gov/istp_guide/vattributes.html#FILLVAL

def omit(id):
  return False
  if not id.startswith('A'):
    return True
  return False

import os

base_dir = os.path.join(os.path.dirname(__file__), '../data')
all_input   = os.path.join(base_dir, 'all-resolved.restructured.json')
file_body   = os.path.join(base_dir, 'tables/all.table.body.json')
file_header = os.path.join(base_dir, 'tables/all.table.head.json')

def all_attribute_table(datasets):

  def all_attributes(datasets):

    if False:
      attributes = {'VarDescription': set(), 'VarAttributes': set()}
      for dataset in datasets:
        for name, variable in dataset['_variables'].items():
          for attribute_type in ['VarDescription', 'VarAttributes']:
            if not attribute_type in variable:
              print("Missing " + attribute_type + " in " + name + " in " + dataset['id'])
              continue
            for attribute in variable[attribute_type]:
              attributes[attribute_type].add(attribute)

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
                    'FILLVAL': None,
                    'UNITS': None,
                    'UNITS_PTR': None,
                    'VAR_TYPE': None,
                    'VIRTUAL': None,
                    'FUNCT': None,
                    'FUNCTION': None,
                  }
                }

    return attributes

  attributes = all_attributes(datasets)

  header = ['datasetID','varName']
  for attribute in attributes['VarDescription']:
    header.append(attribute)
  for attribute in attributes['VarAttributes']:
    header.append(attribute)

  table = []
  for dataset in datasets:
    print(dataset['id'])
    if omit(dataset['id']) == True:
      continue
    for name, variable in dataset['_variables'].items():
      row = [dataset['id'], name]
      for attribute_type in ['VarDescription', 'VarAttributes']:
        if not attribute_type in variable:
          for attribute in attributes[attribute_type]:
            row.append("?") # No VarDescription or VarAttributes
          continue
        for attribute in attributes[attribute_type]:
          if attribute in variable[attribute_type]:
            val = variable[attribute_type][attribute]
            #if isinstance(val,str): val = val.replace(' ', '⎵')
            row.append(val)
          else:
            row.append("")

      table.append(row)

  return header, table

import json
with open(all_input, 'r', encoding='utf-8') as f:
  datasets = json.load(f)

header, table = all_attribute_table(datasets)

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
