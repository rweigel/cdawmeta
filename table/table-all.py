# Put links in header to, e.g., 
# https://spdf.gsfc.nasa.gov/istp_guide/vattributes.html#FILLVAL

def omit(id):
  if not id.startswith('A'):
    return True
  return False

import os
base_dir = os.path.join(os.path.dirname(__file__), '../data')
all_input   = os.path.join(base_dir, 'hapi-bw.tmp.json')
file_body   = os.path.join(base_dir, 'tables/all.table.body.json')
file_header = os.path.join(base_dir, 'tables/all.table.header.json')

def all_attribute_table(datasets):

  def all_attributes(datasets):
    attributes = {
      'VarDescription': set(),
      'VarAttributes': set()
    }
    for dataset in datasets:
      for name, variable in dataset['_variables'].items():
        for attribute_type in ['VarDescription', 'VarAttributes']:
          for attribute in variable[attribute_type]:
            attributes[attribute_type].add(attribute)
    return attributes

  attributes = all_attributes(datasets)

  header = ['datasetID/varName']
  for attribute in attributes['VarDescription']:
    header.append(attribute)
  for attribute in attributes['VarAttributes']:
    header.append(attribute)

  table = []
  for dataset in datasets:
    if omit(dataset['id']) == True:
      continue
    for name, variable in dataset['_variables'].items():
      row = [dataset['id'] + "/" + name]
      for attribute_type in ['VarDescription', 'VarAttributes']:
        for attribute in attributes[attribute_type]:
          if attribute in variable[attribute_type]:
            val = variable[attribute_type][attribute]
            if isinstance(val,str):
              val = val.replace(' ', '⎵')
            row.append(val)
          else:
            row.append("")

      table.append(row)

  return header, table

import json
with open(all_input, 'r', encoding='utf-8') as f:
  datasets = json.load(f)

header, table = all_attribute_table(datasets)

os.makedirs(os.path.dirname(file_header), exist_ok=True)
with open(file_header, 'w', encoding='utf-8') as f:
  json.dump(header, f, indent=2)
  print(f'Wrote: {file_header}')

os.makedirs(os.path.dirname(file_body), exist_ok=True)
with open(file_body, 'w', encoding='utf-8') as f:
  json.dump(table, f, indent=2)
  print(f'Wrote: {file_body}')
