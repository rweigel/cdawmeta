# Put links in header to, e.g., 
# https://spdf.gsfc.nasa.gov/istp_guide/vattributes.html#FILLVAL

def omit(id):
  return False
  if not id.startswith('A'):
    return True
  return False

import os

base_dir    = os.path.join(os.path.dirname(__file__), '../data')
all_input   = os.path.join(base_dir, 'all-resolved.restructured.json')
file_body   = os.path.join(base_dir, 'tables/all.table.body.json')
file_header = os.path.join(base_dir, 'tables/all.table.head.json')

apply_fixes = True
use_all_attributes = True

def all_attribute_table(datasets):

  fixes = {
    "MonoTon": "MONOTON",
    "Bin_Location": "BIN_LOCATION",
    "Time_Base": "TIME_BASE",
    "Time_Scale": "TIME_SCALE",
    "Calib_input": "CALIB_INPUT",
    "Calib_software": "CALIB_SOFTWARE",
    "SI_conv": "SI_CONVERSION",
    "SI_conversion": "SI_CONVERSION",
    "long_name": "Long_Name",
    "valid_range": "VALID_RANGE",
    "Resolution": "RESOLUTION",
    "LABl_PTR_1": "LABL_PTR_1",
    "SC_id": "SC_ID",
    "Description": "DESCRIP",
    "description": "DESCRIP",
    "units": "UNITS",
    "Bin_Location": "BIN_LOCATION",
    "Bin_location": "BIN_LOCATION",
    "SCALETYPE": "SCALETYP",
    "ScaleType": "SCALETYP",
  }

  def all_attributes(datasets):

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
      for dataset in datasets:
        for name, variable in dataset['_variables'].items():
          for attribute_type in ['VarDescription', 'VarAttributes']:
            if not attribute_type in variable:
              print("Missing " + attribute_type + " in " + name + " in " + dataset['id'])
              continue
            for attribute in variable[attribute_type]:
              if apply_fixes:
                if attribute not in fixes:
                  attributes[attribute_type][attribute] = None
              else:
                attributes[attribute_type][attribute] = None

    return attributes

  attributes = all_attributes(datasets)

  header = ['datasetID','VariableName']
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

          if apply_fixes:
            for fix in fixes:
              if fix in variable[attribute_type]:
                variable[attribute_type][fixes[fix]] = variable[attribute_type][fix]
                del variable[attribute_type][fix]

          if attribute in variable[attribute_type]:
            val = variable[attribute_type][attribute]
            #if isinstance(val, str): val = val.replace(' ', '⎵')
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
