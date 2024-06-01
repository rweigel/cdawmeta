import os
import json

from cdawmeta.write_json import write_json

base_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
in_file  = os.path.join(base_dir, 'cdaweb.json')
out_file = os.path.join(os.path.join(base_dir, 'hapi'), 'catalog-all.json')

cat_file = os.path.join(os.path.dirname(__file__), '..', 'data', 'hapi', 'catalog.json')

issues_file = os.path.join(os.path.dirname(__file__), "hapi-nl-issues.json")
with open(issues_file) as f:
  issues = json.load(f)

def order_depend0s(id, depend0_names):

  if id not in issues['depend0Order'].keys():
    return depend0_names

  order_wanted = issues['depend0Order'][id]
  order_given = depend0_names

  # Next two ifs are similar to the ones in order_variables()
  # TODO: Refactor?
  if len(order_wanted) != len(order_wanted):
    print(f'Error: {id}\n  Number of depend0s in new order list ({len(order_wanted)}) does not match number found in dataset ({len(order_given)})')
    print(f'  New order:   {order_wanted}')
    print(f'  Given order: {list(order_given)}')
    print(f'  Exiting with code 1')
    exit(1)

  if sorted(order_wanted) != sorted(order_wanted):
    print(f'Error: {id}\n  Mismatch in depend0 names between new order list and dataset')
    print(f'  New order:   {order_wanted}')
    print(f'  Given order: {list(order_given)}')
    print(f'  Exiting with code 1')
    exit(1)

  return order_wanted

def order_variables(id, variables):

  if id not in issues['variableOrder'].keys():
    return variables

  order_wanted = issues['variableOrder'][id]
  order_given = variables.keys()
  if len(order_wanted) != len(order_wanted):
    print(f'Error: {id}\n  Number of variables in new order list ({len(order_wanted)}) does not match number found in dataset ({len(order_given)})')
    print(f'  New order:   {order_wanted}')
    print(f'  Given order: {list(order_given)}')
    print(f'  Exiting with code 1')
    exit(1)

  if sorted(order_wanted) != sorted(order_wanted):
    print(f'Error: {id}\n  Mismatch in variable names between new order list and dataset')
    print(f'  New order:   {order_wanted}')
    print(f'  Given order: {list(order_given)}')
    print(f'  Exiting with code 1')
    exit(1)

  return {k: variables[k] for k in order_wanted}

def keep_dataset(id, depend_0=None):
  if id in issues['keepSubset'].keys() and depend_0 == issues['keepSubset'][id]:
    print(id)
    print(f"  Warning: Keeping dataset associated with \"{depend_0}\" b/c it is in Nand's list")
    return True
  return False

def omit_dataset(id, depend_0=None):

  if depend_0 is None:
    if id in issues['omitAll'].keys():
      print(id)
      print(f"  Warning: Dropping dataset b/c it is not in Nand's list")
      return True
  else:
    if id in issues['omitSubset'].keys() and depend_0 in issues['omitSubset'][id]:
      print(f"  Warning: Dropping dataset associated with \"{depend_0}\" b/c it is not in Nand's list")
      return True
  return False

def omit_variable(id, variable_name):

  for key in list(issues['omitVariables'].keys()):
    # Some keys of issues['omitVariables'] are ids with @subset_number"
    # The @subset_number is not needed, but kept for reference.
    # Here we concatenate all variables with common dataset base
    # name (variable names are unique within a dataset, so this works).
    newkey = key.split("@")[0]
    if newkey != key:
      if newkey not in issues['omitVariables'].keys():
        issues['omitVariables'][newkey] = issues['omitVariables'][key]
      else:
        # Append new list to existing list
        issues['omitVariables'][newkey] += issues['omitVariables'][key]
      del issues['omitVariables'][key]

  if id in issues['omitVariables'].keys() and variable_name in issues['omitVariables'][id]:
    print(id)
    print(f"  Warning: Dropping variable \"{variable_name}\" b/c it is not in Nand's list")
    return True
  return False

def add_sample_start_stop(datasets):

  def extract_sample_start_stop(file_list):

    if isinstance(file_list["FileDescription"], dict):
      file_list["FileDescription"] = [file_list["FileDescription"]]

    num_files = len(file_list["FileDescription"])
    if num_files == 0:
      sampleFile = None
    if num_files == 1:
      sampleFile = file_list["FileDescription"][0]
    elif num_files == 2:
      sampleFile = file_list["FileDescription"][1]
    else:
      sampleFile = file_list["FileDescription"][-2]

    if sampleFile is not None:
      sampleStartDate = sampleFile["StartTime"]
      sampleStopDate = sampleFile["EndTime"]

    range = {
              "sampleStartDate": sampleStartDate,
              "sampleStopDate": sampleStopDate
            }

    return range

  for dataset in datasets:

    if not "_file_list" in dataset:
      print("No _file_list for " + dataset["id"])
      continue

    with open(dataset['_file_list'], 'r', encoding='utf-8') as f:
      dataset['_file_list_data'] = json.load(f)["_decoded_content"]

    if not "FileDescription" in dataset["_file_list_data"]:
      print("No file list for " + dataset["id"])
      continue

    range = extract_sample_start_stop(dataset["_file_list_data"])
    dataset["info"]["sampleStartDate"] = range["sampleStartDate"]
    dataset["info"]["sampleStopDate"] = range["sampleStopDate"]

    del dataset["_file_list_data"]

def add_info(datasets):

  for dataset in datasets:

    _allxml = dataset['_allxml']

    startDate = _allxml['@timerange_start'].replace(' ', 'T') + 'Z';
    stopDate = _allxml['@timerange_stop'].replace(' ', 'T') + 'Z';

    contact = ''
    if 'data_producer' in _allxml:
      if '@name' in _allxml['data_producer']:
        contact = _allxml['data_producer']['@name']
      if '@affiliation' in _allxml['data_producer']:
        contact = contact + " @ " + _allxml['data_producer']['@affiliation']


    dataset['info'] = {
        'startDate': startDate,
        'stopDate': stopDate,
        'resourceURL': f'https://cdaweb.gsfc.nasa.gov/misc/Notes{dataset["id"][0]}.html#{dataset["id"]}',
        'contact': contact
    }


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

def variables2parameters(depend_0_name, depend_0_variables, all_variables, print_info=False):
  depend_0_variable = all_variables[depend_0_name]

  cdf_type = depend_0_variable['VarDescription']['DataType']
  length = cdftimelen(cdf_type)

  if length == None:
    print(f"  Unhandled DEPEND_0 type: '{cdf_type}'. Dropping variables associated with DEPEND_0 '{depend_0_name}'")
    return None

  parameters = [
                  {
                    'name': 'Time',
                    'type': 'isotime',
                    'units': 'UTC',
                    'length': length,
                    'fill': None,
                    'x_cdf_depend_0_name': depend_0_name
                  }
                ]

  #print(json.dumps(variables, indent=2))

  for name, variable in depend_0_variables.items():

    type = cdf2hapitype(variable['VarDescription']['DataType'])
    if type == None:
      print(f"  Error: Unhandled DataType: {variable['VarDescription']['DataType']}")
      return None

    VAR_TYPE = variable['VarAttributes']['VAR_TYPE']
    if VAR_TYPE != 'data':
      continue

    length = None
    if VAR_TYPE == 'data' and type == 'string':

      PadValue = None
      if 'PadValue' in variable['VarDescription']:
        PadValue = variable['VarDescription']['PadValue']

      FillValue = None
      if 'FillValue' in variable['VarDescription']:
        FillValue = variable['VarDescription']['FillValue']

      NumElements = None
      if 'NumElements' in variable['VarDescription']:
        NumElements = variable['VarDescription']['NumElements']

      if PadValue is None and FillValue is None and NumElements is None:
        print(f'  Error: Dropping variable "{name}" because string parameter and no PadValue, FillValue, or NumElements given to allow length to be determined.')
        continue

      if NumElements is None:
        if PadValue != None and FillValue != None and PadValue != FillValue:
          print(f'  Error: Dropping variable "{name}" because PadValue and FillValue lengths differ.')
          continue

      if PadValue != None:
        length = len(PadValue)
      if FillValue != None:
        length = len(FillValue)
      if NumElements != None:
        length = int(NumElements)

    parameter = {
      "name": name,
      "type": type
    }

    if length is not None:
      parameter['length'] = length

    if 'VIRTUAL' in variable['VarAttributes']:
      parameter['x_cdf_is_virtual'] = variable['VarAttributes']['VIRTUAL'].lower()

    if 'DEPEND_0' in variable['VarAttributes']:
      parameter['x_cdf_depend_0'] = variable['VarAttributes']['DEPEND_0']

    if 'DimSizes' in variable['VarDescription']:
      parameter['size'] = variable['VarDescription']['DimSizes']


    CATDESC = ""
    if 'CATDESC' in variable['VarAttributes']:
      CATDESC = variable['VarAttributes']['CATDESC']

    VAR_NOTES = ""
    if 'VAR_NOTES' in variable['VarAttributes']:
      VAR_NOTES = variable['VarAttributes']['VAR_NOTES']

    if VAR_NOTES == CATDESC:
      parameter['description'] = f"{CATDESC}"
    elif CATDESC != "" and VAR_NOTES == "":
      parameter['description'] = f"{CATDESC}"
    elif VAR_NOTES != "" and CATDESC == "":
      parameter['description'] = f"{CATDESC}"
    elif CATDESC != "" and VAR_NOTES != "":
      parameter['description'] = CATDESC
      parameter['x_description'] = f"CATDESC: {CATDESC}; VAR_NOTES: {VAR_NOTES}"

    # TODO: parameter['description'] = parameter['description'].strip()
    #       Not done now to reduce number of mismatches with Nand.

    def trim(label):
      if isinstance(label, str):
        return label.strip()
      for i in range(0, len(label)):
        label[i] = label[i].strip()
      return label

    if 'size' in parameter:
      label = []
      for i in range(0, len(parameter['size'])):
        label.append([])
        labl_ptr_name = f'LABL_PTR_{i+1}'
        if labl_ptr_name in variable['VarAttributes']:
          labl_ptr_name = variable['VarAttributes'][labl_ptr_name]
          if labl_ptr_name in all_variables:
            #print(all_variables[labl_ptr_name])
            if 'VarData' in all_variables[labl_ptr_name]:
              label[i] = trim(all_variables[labl_ptr_name]['VarData'])
      parameter['x_label'] = label
      if len(parameter['size']) == 1:
        parameter['x_label'] = label[0]

    if 'LABLAXIS' in variable['VarAttributes']:
      parameter['x_label'] = trim(variable['VarAttributes']['LABLAXIS'])

    fill = None
    if 'FILLVAL' in variable['VarAttributes']:
      fill = variable['VarAttributes']['FILLVAL']
    if fill is not None:
      parameter['fill'] = fill

    parameter['units'] = None
    if 'UNITS' in variable['VarAttributes']:
      parameter['units'] = variable['VarAttributes']['UNITS']

    parameter['bins'] = []
    DEPEND_xs = ['DEPEND_1','DEPEND_2','DEPEND_3']
    for DEPEND_x in DEPEND_xs:
      if DEPEND_x in variable['VarAttributes']:
        DEPEND_x_NAME = variable['VarAttributes'][DEPEND_x]
        if not DEPEND_x_NAME in all_variables:
          print(f"  Error: {DEPEND_x} '{DEPEND_x_NAME}' for variable '{name}' is not a variable. Omitting {name}.")
          continue

        bin_object = bins(DEPEND_x_NAME, all_variables[DEPEND_x_NAME])
        if bin_object is not None:
          parameter['bins'].append(bin_object)

    if len(parameter['bins']) == 0:
      del parameter['bins']
    else:
      for bins_object in parameter['bins']:
        if bins_object is None:
          #print(f"  Warning: One of the bins objects for {name} is None.")
          del parameter['bins']

      if 'size' in parameter and len(parameter['size']) != len(parameter['bins']):
        #print(f"  Warning: number of bins objects that could be created ({len(parameter['bins'])}) for {name} does not match len(size) = len({parameter['size']}).")
        del parameter['bins']

      if 'size' not in parameter and 'bins' in parameter:
        del parameter['bins']
        # TODO: This is not always an error. If the variable is virtual,
        # then DimSizes can't be written into the CDF file for that variable
        # (because the variable has no associated data). In this case,
        # we need to get the DimSizes from the DEPEND variable.
        print(f"  Error: Omitting bins for parameter {name} because no DimSizes attribute found.")

    if print_info:
      virtual = parameter.get('x_cdf_is_virtual', '')
      if virtual != '':
        virtual = f'({virtual})'
      print(f"  {parameter['name']} {virtual}")
      print('   size = {}'.format(parameter.get('size', None)))
      print('   x_label = {}'.format(parameter.get('x_label', None)))

    parameters.append(parameter)

  return parameters

def bins(DEPEND_x_NAME, DEPEND_x):

  hapitype = cdf2hapitype(DEPEND_x['VarDescription']['DataType'])

  RecVariance = "NOVARY"
  if "RecVariance" in DEPEND_x['VarDescription']:
    RecVariance = DEPEND_x['VarDescription']["RecVariance"]
    #print("DEPEND_1 has RecVariance = " + RecVariance)

  if not (hapitype == 'integer' or hapitype == 'double'):
    # TODO: Use for labels
    return None

  if RecVariance == "VARY":
    # Nand does not create bins for this case
    return None
  else:
    # TODO: Check for multi-dimensional
    units = ""
    if "UNITS" in DEPEND_x['VarAttributes']:
      units = DEPEND_x['VarAttributes']["UNITS"]
    else:
      if 'VAR_TYPE' in DEPEND_x['VarAttributes']:
        DEPEND_x_VAR_TYPE = DEPEND_x['VarAttributes']['VAR_TYPE']
      else:
        print(f"  Error: No VAR_TYPE for depend variable '{DEPEND_x_NAME}'")
        return None

      if cdf2hapitype(DEPEND_x_VAR_TYPE) in ['data', 'support_data']:
        if not "UNIT_PTR" in DEPEND_x['VarAttributes']:
          print(f"  Error: No UNITS or UNIT_PTR for depend variable '{DEPEND_x_NAME}' with VAR_TYPE '{DEPEND_x_VAR_TYPE}'")

    if 'VarData' in DEPEND_x:
      bins_object = {
                      "name": DEPEND_x_NAME,
                      "units": units,
                      "centers": DEPEND_x["VarData"]
                    }
      return bins_object
    else:
      print(f"  Warning: Not including bin centers for {DEPEND_x_NAME} b/c no VarData (is probably VIRTUAL)")
      return None

def split_variables(datasets):
  """
  Create _variables_split dict. Each key is the name of the DEPEND_0
  variable. Each value is a dict of variables that reference that DEPEND_0
  """

  for dataset in datasets:

    depend_0_dict = {}

    names = dataset['_master_restructured']['_variables'].keys()
    for name in names:

      variable_meta = dataset['_master_restructured']['_variables'][name]

      if 'VarAttributes' not in variable_meta:
        print(dataset['id'])
        print(f'  Error: Dropping variable "{name}" b/c it has no VarAttributes')
        continue

      if 'VAR_TYPE' not in variable_meta['VarAttributes']:
        print(dataset['id'])
        print(f'  Error: Dropping variable "{name}" b/c it has no has no VAR_TYPE')
        continue

      if omit_variable(dataset['id'], name):
        continue

      if 'DEPEND_0' in variable_meta['VarAttributes']:
        depend_0_name = variable_meta['VarAttributes']['DEPEND_0']

        if depend_0_name not in dataset['_master_restructured']['_variables']:
          print(dataset['id'])
          print(f'  Error: Dropping variable "{name}" because it has a DEPEND_0 "{depend_0_name}" that is not in dataset')
          continue

        if depend_0_name not in depend_0_dict:
          depend_0_dict[depend_0_name] = {}
        depend_0_dict[depend_0_name][name] = variable_meta

    dataset['_variables_split'] = depend_0_dict


def create_infos(datasets):

  from cdawmeta.restructure_master import add_master_restructured
  datasets = add_master_restructured(datasets)

  add_info(datasets)
  add_sample_start_stop(datasets)

  # Add _variables_split dict to each dataset.
  split_variables(datasets)

  datasets_new = []
  for dataset in datasets:

    if omit_dataset(dataset['id']):
      continue

    print(dataset['id'] + ": subsetting and creating /info")
    n = 0
    depend_0s = dataset['_variables_split'].items()
    plural = "s" if len(depend_0s) > 1 else ""
    print(f"  {len(depend_0s)} DEPEND_0{plural}")

    # First pass - drop datasets with problems and create list of DEPEND_0 names
    depend_0_names = []
    for depend_0_name, depend_0_variables in depend_0s:

      if omit_dataset(dataset['id'], depend_0=depend_0_name):
        continue

      if depend_0_name not in dataset['_master_restructured']['_variables'].keys():
        print(f"  Error: DEPEND_0 = '{depend_0_name}' is referenced by a variable, but it is not a variable. Omitting variables that have this DEPEND_0.")
        continue

      DEPEND_0_VAR_TYPE = dataset['_master_restructured']['_variables'][depend_0_name]['VarAttributes']['VAR_TYPE']

      VAR_TYPES = []
      for name, variable in depend_0_variables.items():
        VAR_TYPES.append(variable['VarAttributes']['VAR_TYPE'])
      VAR_TYPES = set(VAR_TYPES)

      print(f"  DEPEND_0 Name/VAR_TYPE: '{depend_0_name}'/'{DEPEND_0_VAR_TYPE}'; dependent VAR_TYPES {VAR_TYPES}")

      if DEPEND_0_VAR_TYPE == 'ignore_data':
        print(f"  Not creating dataset for DEPEND_0 = '{depend_0_name}' because it has VAR_TYPE='ignore_data'.")
        continue

      if 'data' not in VAR_TYPES and not keep_dataset(dataset['id'], depend_0=depend_0_name):
        # In general, Nand drops these, but not always
        print(f"  Not creating dataset for DEPEND_0 = '{depend_0_name}' because none of its variables have VAR_TYPE='data'.")
        continue

      all_variables = dataset['_master_restructured']['_variables']
      parameters = variables2parameters(depend_0_name, depend_0_variables, all_variables, print_info=False)
      if parameters == None:
        dataset['_variables_split'][depend_0_name] = None
        if len(depend_0s) == 1:
          print(f"  Due to last error, omitting dataset with DEPEND_0 = {depend_0_name}")
        else:
          print(f"  Due to last error, omitting sub-dataset with DEPEND_0 = {depend_0_name}")
        continue

      depend_0_names.append(depend_0_name)

    #print(depend_0_names)
    depend_0_names = order_depend0s(dataset['id'], depend_0_names)
    #print(depend_0_names)

    for depend_0_name in depend_0_names:

      depend_0_variables = dataset['_variables_split'][depend_0_name]

      subset = ''
      if len(depend_0_names) > 1:
        subset = '@' + str(n)

      depend_0_variables = order_variables(dataset['id'] + subset, depend_0_variables)

      all_variables = dataset['_master_restructured']['_variables']
      parameters = variables2parameters(depend_0_name, depend_0_variables, all_variables, print_info=True)

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

def create_catalog(datasets):
  # Create catalog.json
  catalog = []
  for dataset in datasets:
    id = dataset['id']
    if dataset['_allxml'].get('description') and dataset['_allxml']['description'].get('@short'):
      catalog.append({'id': id, 'description': dataset['_allxml']['description'].get('@short')})
    else:
      catalog.append({'id': id})
  return catalog


print(f'Reading: {in_file}')
with open(in_file, 'r', encoding='utf-8') as f:
  datasets = json.load(f)
print(f'Read: {in_file}')

write_json(create_catalog(datasets), cat_file)

write_json(create_infos(datasets), out_file)
