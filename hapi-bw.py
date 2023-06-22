def omit(id):
  if not id.startswith('A'):
    return True
  return False

import os
import json

base_dir = os.path.dirname(__file__)
all_file = os.path.join(base_dir, 'data/all.json')
tmp_file = os.path.join(base_dir, 'data/hapi-bw.tmp.json')
out_file = os.path.join(base_dir, 'data/hapi-bw.json')

def cdf2hapitype(cdf_type):

  if cdf_type in ['CDF_CHAR', 'CDF_UCHAR']:
    return 'string'

  if cdf_type.startswith('CDF_EPOCH') or cdf_type.startswith('CDF_TIME'):
    return 'isotime'

  if cdf_type.startswith('CDF_INT') or cdf_type.startswith('CDF_UINT') or cdf_type.startswith('CDF_BYTE'):
    return 'integer'

  if cdf_type in ['CDF_FLOAT', 'CDF_DOUBLE', 'CDF_REAL4', 'CDF_REAL8']:
    return 'double'

  print("Unhandled CDF type: " + cdf_type)
  exit(1)

def add_variables(datasets):

  """
  Convert dict with arrays of objects to objects with objects. For example
  { "Epoch": [ 
      {"VarDescription": [{"DataType": "CDF_TIME_TT2000"}, ...] },
      {"VarAttributes": [{"CATDESC": "Default time"}, ...] }
    ]
  }
  // is converted to
    {
      "Epoch": {
        "VarDescription": {
          "DataType": "CDF_TIME_TT2000",
          ...
        },
        "VarAttributes": {
          "CATDESC": "Default time",
          ...
        }
      }
    }
  
  Returns all unique variable attributes and description keys, e.g.
  { 
    "VarDescription": ["DataType", ...],
    "VarAttributes": ["CATDESC", ...]
  }  
  """

  def sort_keys(obj):
    return {key: obj[key] for key in sorted(obj)}

  def array_to_dict(array):
    obj = {}
    for element in array:
      key = list(element.keys())[0]
      obj[key] = element[key]
    return obj

  for dataset in datasets:

    file = list(dataset['_master'].keys())[0]

    variables = dataset['_master'][file]['CDFVariables']
    variables_new = {}

    depend_0s = {}
    for variable in variables:

      variable_keys = list(variable.keys())
      if len(variable_keys) > 1:
        print(dataset["id"] + ": Expected only one variable key in variable object.")
        exit(0)

      variable_name = variable_keys[0]
      variable_array = variable[variable_name]
      variable_dict = array_to_dict(variable_array)

      for key, value in variable_dict.items():

        if key == 'VarData':
          variable_dict[key] = value
        else:
          variable_dict[key] = sort_keys(array_to_dict(value))

      variables_new[variable_name] = variable_dict

    dataset['_variables'] = variables_new

def add_variables_split(datasets):

  for dataset in datasets:

    depend_0s = {}
    print(dataset['id'] + ": building HAPI /info")
    names = dataset['_variables'].keys()
    for name in names:

      variable_meta = dataset['_variables'][name]

      if 'VarAttributes' not in variable_meta:
        print(f'  variable "{name}" has no VarAttributes')
        continue

      if 'VAR_TYPE' not in variable_meta['VarAttributes']:
        print(f'  variable "{name}" has no VAR_TYPE')
        continue

      VAR_TYPE = variable_meta['VarAttributes']['VAR_TYPE']
      if VAR_TYPE == 'support_data':
        continue

      if 'VIRTUAL' in variable_meta['VarAttributes']:
        if variable_meta['VarAttributes']['VIRTUAL'].lower() == 'true':
          print("  VIRTUAL variable: " + name)

      if 'DEPEND_0' in variable_meta['VarAttributes']:
        depend_0 = variable_meta['VarAttributes']['DEPEND_0']
        if depend_0 not in depend_0s:
          depend_0s[depend_0] = {}
        depend_0s[depend_0][name] = variable_meta

    depend_0_names = list(depend_0s.keys())
    if len(depend_0_names) > 1:
      print(f'  {len(depend_0_names)} DEPEND_0s')

    dataset['_variables_split'] = depend_0s

def variables2parameters(variables):

  # TODO: Determine if length can be obtained based on DEPEND_0 type.
  # Here I set it to 30, which assumes server will always return to ns precision.
  parameters = [{'name': 'Time',
                 'type': 'isotime',
                 'units': 'UTC',
                 'length': 30,
                 'fill': None}]

  #print(json.dumps(variables, indent=2))
  for key, variable in variables.items():

    type = cdf2hapitype(variable['VarDescription']['DataType'])

    parameter = {
      "name": key,
      "type": type
    }

    if 'VIRTUAL' in variable['VarAttributes']:
      parameter['_VIRTUAL'] = variable['VarAttributes']['VIRTUAL']

    if 'DimSizes' in variable['VarDescription']:
      size = variable['VarDescription']['DimSizes']
      if len(size) == 1:
        size = size[0]
      parameter['size'] = size

    description = ""
    if 'CATDESC' in variable['VarAttributes']:
      description = variable['VarAttributes']['CATDESC']
    parameter['description'] = description

    fill = None
    if 'FILLVAL' in variable['VarAttributes']:
      fill = variable['VarAttributes']['FILLVAL']
    if fill is not None:
      parameter['fill'] = fill

    units = None
    if 'UNITS' in variable['VarAttributes']:
      _units = variable['VarAttributes']['UNITS']
      if _units.strip() != '':
        units = _units
    if units is not None:
      parameter['units'] = units

    parameters.append(parameter)

  return parameters

def subset_and_transform(datasets):

  datasets_new = []
  for dataset in datasets:
    n = 0
    depend_0s = dataset['_variables_split'].items()
    for depend_0_name, variables in depend_0s:
      #print(depend_0_name)
      subset = ''
      if len(depend_0s) > 1:
        subset = '@' + str(n)
      dataset_new = {
        'id': dataset['id'] + subset,
        'info': {
          **dataset['info'],
          'parameters': variables2parameters(variables)
        }
      }
      datasets_new.append(dataset_new)
      n = n + 1

  return datasets_new

print(f'Reading: {all_file}')
with open(all_file, 'r', encoding='utf-8') as f:
  datasets = json.load(f)

for idx, dataset in enumerate(datasets):

  if '_master' not in dataset:
    print(f'No _master in {dataset["id"]}. Omitting dataset.')
    datasets[idx] = None
    continue

  if omit(dataset['id']) == True:
    print(f'Omitting {dataset["id"]}.')
    datasets[idx] = None
    continue

datasets = [i for i in datasets if i is not None]

# Add _variables element to each dataset
add_variables(datasets)
# Add _variables_split element to each dataset
add_variables_split(datasets)

# Split datasets into HAPI datasets using _variables_split element.
datasets_hapi = subset_and_transform(datasets)

with open(out_file, 'w', encoding='utf-8') as f:
  json.dump(datasets_hapi, f, indent=2)
print(f'Wrote: {out_file}')

# For debugging, save result passed to subset_and_transform().
with open(tmp_file, 'w', encoding='utf-8') as f:
  json.dump(datasets, f, indent=2)
print(f'Wrote: {tmp_file}')