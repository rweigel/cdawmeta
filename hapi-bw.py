def omit(id):
  return False

import os
import json

base_dir = os.path.dirname(__file__)
all_file_restructured = os.path.join(base_dir, 'data/all-resolved.restructured.json')
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

  return None

def split_variables(datasets):

  for dataset in datasets:

    depend_0_dict = {}

    names = dataset['_variables'].keys()
    for name in names:

      variable_meta = dataset['_variables'][name]

      if 'VarAttributes' not in variable_meta:
        print(dataset['id'])
        print(f'  Error: Dropping dataset b/c variable "{name}" has no VarAttributes')
        continue

      if 'VAR_TYPE' not in variable_meta['VarAttributes']:
        print(dataset['id'])
        print(f'  Error: Dropping dataset b/c variable "{name}" has no VAR_TYPE')
        continue

      if 'DEPEND_0' in variable_meta['VarAttributes']:
        depend_0_name = variable_meta['VarAttributes']['DEPEND_0']

        if depend_0_name not in dataset['_variables']:
          print(dataset['id'])
          print(f'  Error: Dropping variable "{name}" because it has a DEPEND_0 "{depend_0_name}" that is not in dataset')
          continue

        if depend_0_name not in depend_0_dict:
          depend_0_dict[depend_0_name] = {}
        depend_0_dict[depend_0_name][name] = variable_meta

    dataset['_variables_split'] = depend_0_dict

def cdftimelen(cdf_type):

  # Based on table at https://spdf.gsfc.nasa.gov/istp_guide/vattributes.html
  # Could also get from PadValue or FillValue, but they are not always present (!).
  if cdf_type == 'CDF_EPOCH':
    return len('0000-01-01:00:00:00.000Z')
  if cdf_type == 'CDF_TIME_TT2000':
    return len('0000-01-01:00:00:00.000000000Z')
  if cdf_type == 'CDF_EPOCH16':
    return len('0000-01-01:00:00:00.000000000000Z')

  return None

def variables2parameters(depend_0_variable, depend_0_variables, all_variables):

  cdf_type = depend_0_variable['VarDescription']['DataType']
  length = cdftimelen(cdf_type)

  if length == None:
    print("  Unhandled DEPEND_0 type: " + cdf_type)
    return None

  parameters = [
                  {
                    'name': 'Time',
                    'type': 'isotime',
                    'units': 'UTC',
                    'length': length,
                    'fill': None
                  }
                ]

  #print(json.dumps(variables, indent=2))

  for name, variable in depend_0_variables.items():

    VAR_TYPE = variable['VarAttributes']['VAR_TYPE']

    type = cdf2hapitype(variable['VarDescription']['DataType'])
    if type == None:
      print(f"  Error: Unhandled DataType: {variable['VarDescription']['DataType']}")
      return None

    if VAR_TYPE == 'data' and type == 'string':
      #print(variable['VarDescription']['PadValue'])
      #print(variable['VarAttributes']['FILLVAL'])
      # Would need to determine string length and need to handle
      # case where PadValue and FillValue are not present, so length
      # cannot be determined. (PadValue and FillValue are not always
      # present for DEPEND_0 variables; see note in cdftimelen()).
      print(f"  Dropping {name} because string parameter with VAR_TYPE='data' is not supported.")
      continue

    parameter = {
      "name": name,
      "type": type
    }

    if 'VIRTUAL' in variable['VarAttributes']:
      parameter['_VIRTUAL'] = variable['VarAttributes']['VIRTUAL']
    if 'DEPEND_0' in variable['VarAttributes']:
      parameter['_DEPEND_0'] = variable['VarAttributes']['DEPEND_0']

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

    if 'DEPEND_1' in variable['VarAttributes']:

      DEPEND_1_NAME = variable['VarAttributes']['DEPEND_1']
      if not DEPEND_1_NAME in all_variables:
        # Could just drop variable
        print(f"  Error: DEPEND_1 '{DEPEND_1_NAME}' for variable '{name}' is not a variable. Omitting {name}.")
        continue

      DEPEND_1 = all_variables[DEPEND_1_NAME]
      hapitype = cdf2hapitype(DEPEND_1['VarDescription']['DataType'])

      RecVariance = "NOVARY"
      if "RecVariance" in DEPEND_1['VarDescription']:
        RecVariance = DEPEND_1['VarDescription']["RecVariance"]
        #print("DEPEND_1 has RecVariance = " + RecVariance)

      if hapitype == 'integer' or hapitype == 'double':
        if RecVariance == "VARY":
          # Nand does not create bins for this case
          continue
        else:
          # TODO: Check for multi-dimensional
          units = ""
          if "UNITS" in DEPEND_1['VarAttributes']:
            units = DEPEND_1['VarAttributes']["UNITS"]
          else:
            DEPEND_1_VAR_TYPE = DEPEND_1['VarAttributes']['VAR_TYPE']
            if cdf2hapitype(DEPEND_1_VAR_TYPE) in ['data', 'support_data']:
              if not "UNIT_PTR" in DEPEND_1['VarAttributes']:
                print(f"  Error: No UNITS or UNIT_PTR for data for DEPEND_1 variable '{DEPEND_1_NAME}' with VAR_TYPE '{DEPEND_1_VAR_TYPE}'")
          if 'VarData' in DEPEND_1:
            bins = [{
              "name": DEPEND_1_NAME,
              "units": units,
              "centers": DEPEND_1["VarData"]
            }]
            parameter["bins"] = bins
          else:
            print(f"  Not including bin centers for {DEPEND_1_NAME} no VarData (probably VIRTUAL)")
      else:
        # TODO: Use for labels
        pass

    if VAR_TYPE == 'data':
      parameters.append(parameter)

  return parameters

def subset_and_transform(datasets):

  datasets_new = []
  for dataset in datasets:

    print(dataset['id'] + ": subsetting and creating /info")
    n = 0
    depend_0s = dataset['_variables_split'].items()
    print(f"  {len(depend_0s)} DEPEND_0s")

    # First pass - drop DEPEND_0 datasets with problems
    depend_0_names = []
    for depend_0_name, depend_0_variables in depend_0s:

      if depend_0_name not in dataset['_variables'].keys():
        print(f"  Error: DEPEND_0 = '{depend_0_name}' is referenced by a variable, but it is not a variable. Omitting variables that have this DEPEND_0.")
        continue

      DEPEND_0_VAR_TYPE = dataset['_variables'][depend_0_name]['VarAttributes']['VAR_TYPE']

      VAR_TYPES = []
      for depend_0_variable in depend_0_variables.values():
        VAR_TYPES.append(depend_0_variable['VarAttributes']['VAR_TYPE'])
      VAR_TYPES = set(VAR_TYPES)

      print(f"  DEPEND_0 ID/VAR_TYPE: '{depend_0_name}'/'{DEPEND_0_VAR_TYPE}'; dependent VAR_TYPES {VAR_TYPES}")

      if DEPEND_0_VAR_TYPE == 'ignore_data':
        print(f"  Not creating dataset for DEPEND_0 = '{depend_0_name}' because it has VAR_TYPE='ignore_data'.")
        continue

      if 'data' not in VAR_TYPES:
        print(f"  Not creating dataset for DEPEND_0 = '{depend_0_name}' because none of its variables have VAR_TYPE='data'.")
        continue

      depend_0_variable = dataset['_variables'][depend_0_name]
      parameters = variables2parameters(depend_0_variable, depend_0_variables, dataset['_variables'])
      if parameters == None:
        dataset['_variables_split'][depend_0_name] = None
        if len(depend_0s) == 1:
          print(f"  Due to last error, omitting dataset with DEPEND_0 = {depend_0_name}")
        else:
          print(f"  Due to last error, omitting sub-dataset with DEPEND_0 = {depend_0_name}")
        continue

      depend_0_names.append(depend_0_name)

    for depend_0_name in depend_0_names:

      depend_0_variable = dataset['_variables'][depend_0_name]
      depend_0_variables = dataset['_variables_split'][depend_0_name]

      parameters = variables2parameters(depend_0_variable, depend_0_variables, dataset['_variables'])

      subset = ''
      if len(depend_0_names) > 1:
        subset = '@' + str(n)

      dataset_new = {
        'id': dataset['id'] + subset,
        'info': {
          **dataset['info'],
          'parameters': parameters
        }
      }
      datasets_new.append(dataset_new)
      n = n + 1

  return datasets_new

print(f'Reading: {all_file_restructured}')
with open(all_file_restructured, 'r', encoding='utf-8') as f:
  datasets = json.load(f)
print(f'Read: {all_file_restructured}')

# Create _variables_split dict to each dataset. Each key is the name of
# the DEPEND_0 variable and all variables that reference that DEPEND_0 are
# under it.
split_variables(datasets)

# Split datasets with more than one DEPEND_0 variable into datasets with
# only one DEPEND_0.
datasets_hapi = subset_and_transform(datasets)

print(f'Writing: {out_file}')
with open(out_file, 'w', encoding='utf-8') as f:
  json.dump(datasets_hapi, f, indent=2)
print(f'Wrote: {out_file}')
