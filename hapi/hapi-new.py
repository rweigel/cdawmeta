import os
import re
import json

import cdawmeta

# Set to True to omit datasets that are not in Nand's metadata
omit_datasets = False

# Set to false to reduce number of mismatch warnings
strip_description = False

# Remove "--->" in description
remove_arrows = False

root_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
base_dir = os.path.join(root_dir, 'data')
info_dir = os.path.join(base_dir, 'hapi', 'info')

def cli():
  clkws = {
    "partial": {
      "action": "store_true",
      "help": "Read catalog.partial.json instead of catalog.json",
      "default": False
    }
  }

  import argparse
  parser = argparse.ArgumentParser()
  for k, v in clkws.items():
    parser.add_argument(f'--{k}', **v)
  return parser.parse_args()

def set_error(id, name, msg):
  if not id in set_error.errors:
    set_error.errors[id] = {}
  if name is None:
    set_error.errors[id] = msg.lstrip()
  else:
    if not name in set_error.errors[id]:
      set_error.errors[id][name] = []
    set_error.errors[id][name].append(msg.lstrip())
set_error.errors = {}

def order_depend0s(id, depend0_names, issues):

  if id not in issues['depend0Order'].keys():
    return depend0_names

  order_wanted = issues['depend0Order'][id]

  for depend0_name in order_wanted:
    if not depend0_name in depend0_names:
      logger.error(f'Error: {id}\n  DEPEND_0 {depend0_name} in new order list is not a depend0 in dataset ({depend0_names})')
      logger.error(f'  Exiting with code 1')
      exit(1)

  if False:
    # Eventually we will want to use this when we are not trying to match
    # Nand's metadata exactly.
    # Append depend0s not in order_wanted to the end of the list
    final = order_wanted.copy()
    for i in depend0_names:
      if not i in order_wanted:
        final.append(i)

  return order_wanted

def order_variables(id, variables, issues):

  if id not in issues['variableOrder'].keys():
    return variables

  order_wanted = issues['variableOrder'][id]
  order_given = variables.keys()
  if len(order_wanted) != len(order_wanted):
    logger.error(f'Error: {id}\n  Number of variables in new order list ({len(order_wanted)}) does not match number found in dataset ({len(order_given)})')
    logger.error(f'  New order:   {order_wanted}')
    logger.error(f'  Given order: {list(order_given)}')
    logger.error(f'  Exiting with code 1')
    exit(1)

  if sorted(order_wanted) != sorted(order_wanted):
    logger.error(f'Error: {id}\n  Mismatch in variable names between new order list and dataset')
    logger.error(f'  New order:   {order_wanted}')
    logger.error(f'  Given order: {list(order_given)}')
    logger.error(f'  Exiting with code 1')
    exit(1)

  return {k: variables[k] for k in order_wanted}

def keep_dataset(id, issues, depend_0=None):
  if id in issues['keepSubset'].keys() and depend_0 == issues['keepSubset'][id]:
    logger.info(id)
    logger.info(f"  Warning: Keeping dataset associated with \"{depend_0}\" b/c it is in Nand's list")
    return True
  return False

def omit_dataset(id, issues, depend_0=None):

  if depend_0 is None:
    if id in issues['omitAll'].keys():
      if omit_datasets:
        logger.info(id)
        logger.info(f"    Warning: Dropping dataset {id} b/c it is not in Nand's list")
        return True
      else:
        logger.info(id)
        logger.info(f"    Warning: Keeping dataset {id} even though it is not in Nand's list")
        return False
    for pattern in issues['omitAllPattern']:
      if re.search(pattern, id):
        if omit_datasets:
          logger.info(id)
          logger.info(f"    Warning: Dropping dataset {id} b/c it is not in Nand's list")
          return True
        else:
          logger.info(id)
          logger.info(f"    Warning: Keeping dataset {id} even though it is not in Nand's list")
          return False
  else:
    if id in issues['omitSubset'].keys() and depend_0 in issues['omitSubset'][id]:
      logger.info(f"    Warning: Dropping variables associated with DEPEND_0 = \"{depend_0}\" b/c this DEPEND_0 is not in Nand's list")
      return True
  return False

def omit_variable(id, variable_name, issues):

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
    logger.info(id)
    logger.info(f"  Warning: Dropping variable \"{variable_name}\" b/c it is not in Nand's list")
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
      logger.info("No _file_list for " + dataset["id"])
      continue

    with open(dataset['_file_list'], 'r', encoding='utf-8') as f:
      dataset['_file_list_data'] = json.load(f)["_decoded_content"]

    if not "FileDescription" in dataset["_file_list_data"]:
      logger.info("No file list for " + dataset["id"])
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

def variables2parameters(depend_0_name, depend_0_variables, all_variables, dsid, print_info=False):
  depend_0_variable = all_variables[depend_0_name]

  cdf_type = depend_0_variable['VarDescription']['DataType']
  length = cdftimelen(cdf_type)

  if length == None:
    msg = f"    Warning: DEPEND_0 variable '{dsid}'/{depend_0_name} has unhandled type: '{cdf_type}'. "
    msg += f"Dropping variables associated with it"
    logger.info(msg)
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
      msg = f"    Error: '{name}' has unhandled DataType: {variable['VarDescription']['DataType']}. Dropping variable."
      set_error(dsid, name, msg)
      logger.error(msg)
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
        msg = "    Error: Dropping '{name}' because cdf2hapitype(VAR_TYPE) returns string but no PadValue, FillValue, or NumElements given to allow length to be determined."
        logger.error(msg)
        set_error(dsid, name, msg)
        continue

      if NumElements is None:
        if PadValue != None and FillValue != None and PadValue != FillValue:
          msg = f"    Error: Dropping '{name}' because PadValue and FillValue lengths differ."
          logger.error(msg)
          set_error(dsid, name, msg)
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

    if strip_description:
      parameter['description'] = parameter['description'].strip()

    if remove_arrows:
      parameter['description'] = parameter['description'].replace('--->', '')

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
              #print(labl_ptr_name)
              #print(all_variables[labl_ptr_name]['VarData'])
              label[i] = trim(str(all_variables[labl_ptr_name]['VarData']))
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

    if 'UNITS_PTR' in variable['VarAttributes']:
      if print_info:
        # Seems this is never used.
        logger.info(f"    Warning: Not implemented: UNITS_PTR = '{variable['VarAttributes']['UNITS_PTR']}' not used")

    if print_info:
      virtual = parameter.get('x_cdf_is_virtual', False)
      virtual = f' (virtual: {virtual})'
      logger.info(f"    {parameter['name']}{virtual}")
      logger.info('     size = {}'.format(parameter.get('size', None)))
      logger.info('     x_label = {}'.format(parameter.get('x_label', None)))
      check_display_type(dsid, name, variable, print_info=True)

    if 'DimSizes' in variable['VarDescription']:
      bins_object = bins(name, variable, all_variables, dsid, print_info=print_info)
      if bins_object is not None:
        parameter['bins'] = bins_object
      if print_info:
        if bins_object is not None:
          for idx, bin in enumerate(bins_object):
            bin_copy = bin.copy()
            if 'centers' in bin and len(bin['centers']) > 10:
              bin_copy['centers'] = f'{bin["centers"][0]} ... {bin["centers"][-1]}'  
            logger.info(f"     bins[{idx}] = {bin_copy}")

    parameters.append(parameter)

  return parameters


def check_display_type(dsid, name, variable, print_info=False):

  valid = False
  if 'DISPLAY_TYPE' in variable['VarAttributes']:
    DISPLAY_TYPE = variable['VarAttributes']['DISPLAY_TYPE']
    DISPLAY_TYPE = DISPLAY_TYPE.split(">")[0]

    display_types_known = ['time_series','spectrogram','stack_plot','image','no_plot','orbit', 'plasmagram', 'skymap']

    if DISPLAY_TYPE not in display_types_known:
      if print_info:
        msg = f"     Error: DISPLAY_TYPE = '{DISPLAY_TYPE}' is not in {display_types_known}. Will attempt to infer."
        logger.error(msg)
        set_error(dsid, name, msg)
    if print_info:
      if DISPLAY_TYPE == ' ':
        logger.info(f"     Warning: DISPLAY_TYPE = '{DISPLAY_TYPE}'")
      elif DISPLAY_TYPE.strip() == '':
        logger.info(f"     Warning: DISPLAY_TYPE.strip() = ''")

    found = False
    for display_type in display_types_known:
      if DISPLAY_TYPE.lower().startswith(display_type):
        found = True
        valid = True
        if print_info:
          logger.info(f"     DISPLAY_TYPE = '{DISPLAY_TYPE}'")
        break
    if not found and print_info:
      logger.info(f"     Warning: DISPLAY_TYPE.lower() = '{DISPLAY_TYPE}' does not start with one of {display_types_known}")
  elif variable['VarAttributes'].get('VAR_TYPE') == 'data':
    if print_info:
      logger.error('     Error: No DISPLAY_TYPE attribute for variable with VAR_TYPE = data')
    valid = False

  return valid

def bins(name, variable, all_variables, dsid, print_info=False):

  NumDims = variable['VarDescription'].get('NumDims', None)
  DimSizes = variable['VarDescription'].get('DimSizes', None)
  DimVariances = variable['VarDescription'].get('DimVariances', None)

  if print_info:
    logger.info(f"     NumDims: {NumDims}")
    logger.info(f"     DimSizes: {DimSizes}")
    logger.info(f"     DimVariances: {DimVariances}")

  x_error = None
  xs = {'DEPEND': [], 'LABL_PTR': []}
  for prefix in ['DEPEND', 'LABL_PTR']:
    for x in [1, 2, 3]:
      if f'{prefix}_{x}' in variable['VarAttributes']:
        x_NAME = variable['VarAttributes'][f'{prefix}_{x}']
        xs[prefix].append(x_NAME)
        if not x_NAME in all_variables:
          x_error = "Bad DEPEND reference"
          if print_info:
            msg = f"Error: Bad DEPEND reference: '{name}' has {prefix}_{x} named '{x_NAME}', which is not a variable. Not creating bins."
            logger.error(f"     {msg}")
            set_error(dsid, name, msg)

    if len(xs[prefix]) > len(DimSizes):
      if print_info:
        msg = f"     Error: Too many DEPENDs: '{name}' has a {prefix}_{x} and len(DimSizes) = {len(DimSizes)}."
        logger.error(msg)
        set_error(dsid, name, msg)
      else:
        xs[prefix].append(None)

  def n_not_none(xs):
    return len([x for x in xs if x is not None])

  if print_info:
    for prefix in ['DEPEND', 'LABL_PTR']:
      if n_not_none(xs[prefix]) > 0:
        logger.info("     " + f"{prefix}" + "_{1,2,3}: " + f"{xs[prefix]}")

    #else:
      #if n_vary > 0:
      #logger.error("Vary OK")
      #set_error(dsid, name, "Vary OK")

  if NumDims != len(DimSizes):
    if print_info:
      msg = f"     Error: DimSizes mismatch: NumDims = {NumDims} != len(DimSizes) = {len(DimSizes)}"
      logger.error(msg)
      set_error(dsid, name, msg)

  if len(DimSizes) != len(DimVariances):
    if print_info:
      msg = f"     Error: DimVariances mismatch: len(DimSizes) = {DimSizes} != len(DimVariances) = {len(DimVariances)}"
      logger.error(msg)
      set_error(dsid, name, msg)

  n_vary = DimVariances.count('VARY')
  if n_vary != n_not_none(xs['DEPEND']):
    if print_info:
      msg = f"     Error: DimVariances has {n_vary} VARY elements != number of DEPEND_{{1,2,3}} ({n_not_none(xs['DEPEND'])})"
      logger.error(msg)
      set_error(dsid, name, msg)
    return None

  if x_error is not None:
    if print_info:
      msg = f"     Error: Not creating bins because of {x_error}"
      logger.error(msg)
      set_error(dsid, name, msg)
    return None

  if n_not_none(xs['DEPEND']) != len(DimSizes):
    if print_info:
      logger.info(f"     Warning: Not implemented: Not creating bins because number of DEPENDs ({n_not_none(xs['DEPEND'])}) != len(DimSizes) ({len(DimSizes)})")
    return None


  bins_objects = []
  for x in range(len(xs['DEPEND'])):
    DEPEND_x_NAME = xs['DEPEND'][x]
    if DEPEND_x_NAME is not None:
      hapitype = cdf2hapitype(all_variables[DEPEND_x_NAME]['VarDescription']['DataType'])
      if not (hapitype == 'integer' or hapitype == 'double'):
        msg = f"DEPEND_{x} = '{DEPEND_x_NAME}' DataType is not an integer or float. Omitting bins for {name}."
        if print_info:
          logger.info(f"     Warning: Not implemented: {msg}")
        return None

      bins_object = create_bins(dsid, name, x, DEPEND_x_NAME, all_variables[DEPEND_x_NAME], print_info=print_info)
      if bins_object is None:
        return None
      bins_objects.append(bins_object)

  return bins_objects

def create_bins(dsid, name, x, DEPEND_x_NAME, DEPEND_x, print_info=False):

  RecVariance = "NOVARY"
  if "RecVariance" in DEPEND_x['VarDescription']:
    RecVariance = DEPEND_x['VarDescription']["RecVariance"]
    #print("DEPEND_1 has RecVariance = " + RecVariance)

  if RecVariance == "VARY":
    # Nand does not create bins for this case
    if print_info:
      logger.info(f"     Warning: Not implemented: DEPEND_{x} = {DEPEND_x_NAME} has RecVariance = 'VARY' . Not creating bins b/c Nand does not for this case.")
    return None
  else:
    # TODO: Check for multi-dimensional
    units = ""
    if "UNITS" in DEPEND_x['VarAttributes']:
      units = DEPEND_x['VarAttributes']["UNITS"]
    else:
      if "UNIT_PTR" in DEPEND_x['VarAttributes']:
        if print_info:
          msg = f"     Error: Not implemented: DEPEND_{x} = '{DEPEND_x_NAME}' has UNIT_PTR = '{DEPEND_x['VarAttributes']['UNIT_PTR']}'. Not using."
          logger.error(msg)
          set_error(dsid, name, msg)

      if 'VAR_TYPE' in DEPEND_x['VarAttributes']:
        DEPEND_x_VAR_TYPE = DEPEND_x['VarAttributes']['VAR_TYPE']
      else:
        if print_info:
          msg = f"     Error: DEPEND_{x} = '{DEPEND_x_NAME}' has no VAR_TYPE. Not creating bins."
          logger.error(msg)
          set_error(dsid, name, msg)
        return None

      if DEPEND_x_VAR_TYPE in ['data', 'support_data']:
        if not "UNIT_PTR" in DEPEND_x['VarAttributes']:
          if print_info:
            msg = f"     Error: DEPEND_{x} = '{DEPEND_x_NAME}' has VAR_TYPE '{DEPEND_x_VAR_TYPE}' and no UNITS or UNIT_PTR."
            logger.error(msg)
            set_error(dsid, name, msg)

    if 'VarData' in DEPEND_x:
      bins_object = {
                      "name": DEPEND_x_NAME,
                      "units": units,
                      "centers": DEPEND_x["VarData"]
                    }
      return bins_object
    else:
      if print_info:
        logger.info(f"     Warning: Not including bin centers for {DEPEND_x_NAME} b/c no VarData (is probably VIRTUAL)")
      return None

def split_variables(datasets, issues):
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
        logger.error(dataset['id'])
        msg = f"  Error: Dropping variable '{name}' b/c it has no VarAttributes"
        logger.error(msg)
        continue

      if 'VAR_TYPE' not in variable_meta['VarAttributes']:
        logger.error(dataset['id'])
        msg = f"  Error: Dropping variable '{name}' b/c it has no has no VAR_TYPE"
        logger.error(msg)
        set_error(dataset['id'], name, msg)
        continue

      if omit_variable(dataset['id'], name, issues):
        continue

      if 'DEPEND_0' in variable_meta['VarAttributes']:
        depend_0_name = variable_meta['VarAttributes']['DEPEND_0']

        if depend_0_name not in dataset['_master_restructured']['_variables']:
          logger.error(dataset['id'])
          msg = f"  Error: Dropping '{name}' b/c it has a DEPEND_0 ('{depend_0_name}') that is not in dataset"
          logger.error(msg)
          set_error(dataset['id'], name, msg)
          continue

        if depend_0_name not in depend_0_dict:
          depend_0_dict[depend_0_name] = {}
        depend_0_dict[depend_0_name][name] = variable_meta

    dataset['_variables_split'] = depend_0_dict


def create_catalog_all(datasets, issues):

  from cdawmeta.restructure_master import add_master_restructured
  datasets = add_master_restructured(datasets, set_error)

  add_info(datasets)
  add_sample_start_stop(datasets)

  # Add _variables_split dict to each dataset.
  split_variables(datasets, issues)

  catalog_all = []
  for dataset in datasets:

    if omit_dataset(dataset['id'], issues):
      continue

    logger.info(dataset['id'] + ": subsetting and creating /info")
    n = 0
    depend_0s = dataset['_variables_split'].items()
    plural = "s" if len(depend_0s) > 1 else ""
    logger.info(f"  {len(depend_0s)} DEPEND_0{plural}")

    # First pass - drop datasets with problems and create list of DEPEND_0 names
    depend_0_names = []
    for depend_0_name, depend_0_variables in depend_0s:

      logger.info(f"  Checking DEPEND_0: '{depend_0_name}'")

      if omit_dataset(dataset['id'], issues, depend_0=depend_0_name):
        continue

      if depend_0_name not in dataset['_master_restructured']['_variables'].keys():
        logger.error(dataset['id'])
        msg = f"    Error: DEPEND_0 = '{depend_0_name}' is referenced by a variable, but it is not a variable. Omitting variables that have this DEPEND_0."
        logger.error(msg)
        set_error(dataset['id'], depend_0_name, msg)
        continue

      DEPEND_0_VAR_TYPE = dataset['_master_restructured']['_variables'][depend_0_name]['VarAttributes']['VAR_TYPE']

      VAR_TYPES = []
      for name, variable in depend_0_variables.items():
        VAR_TYPES.append(variable['VarAttributes']['VAR_TYPE'])
      VAR_TYPES = set(VAR_TYPES)

      logger.info(f"    VAR_TYPE: '{DEPEND_0_VAR_TYPE}'; dependent VAR_TYPES {VAR_TYPES}")

      if DEPEND_0_VAR_TYPE == 'ignore_data':
        logger.info(f"    Not creating dataset for DEPEND_0 = '{depend_0_name}' because it has VAR_TYPE='ignore_data'.")
        continue

      if 'data' not in VAR_TYPES and not keep_dataset(dataset['id'], issues, depend_0=depend_0_name):
        # In general, Nand drops these, but not always
        logger.info(f"    Not creating dataset for DEPEND_0 = '{depend_0_name}' because none of its variables have VAR_TYPE='data'.")
        continue

      all_variables = dataset['_master_restructured']['_variables']
      parameters = variables2parameters(depend_0_name, depend_0_variables, all_variables, dataset['id'], print_info=False)
      if parameters == None:
        dataset['_variables_split'][depend_0_name] = None
        if len(depend_0s) == 1:
          logger.info(f"    Due to last error, omitting dataset with DEPEND_0 = {depend_0_name}")
        else:
          logger.info(f"    Due to last error, omitting sub-dataset with DEPEND_0 = {depend_0_name}")
        continue

      depend_0_names.append(depend_0_name)

    #print(depend_0_names)
    depend_0_names = order_depend0s(dataset['id'], depend_0_names, issues)
    #print(depend_0_names)

    for depend_0_name in depend_0_names:

      logger.info(f"  Creating HAPI dataset for DEPEND_0: '{depend_0_name}'")

      depend_0_variables = dataset['_variables_split'][depend_0_name]

      subset = ''
      if len(depend_0_names) > 1:
        subset = '@' + str(n)

      depend_0_variables = order_variables(dataset['id'] + subset, depend_0_variables, issues)

      all_variables = dataset['_master_restructured']['_variables']
      parameters = variables2parameters(depend_0_name, depend_0_variables, all_variables, dataset['id'], print_info=True)

      dataset_new = {
        'id': dataset['id'] + subset,
        'description': None,
        'info': {
          **dataset['info'],
          'parameters': parameters
        }
      }

      if dataset['_allxml'].get('description') and dataset['_allxml']['description'].get('@short'):
        dataset_new['description'] = dataset['_allxml']['description'].get('@short')
      else:
        del dataset_new['description']

      catalog_all.append(dataset_new)
      n = n + 1

  return catalog_all


def write_infos(catalog_all, info_dir):
  if not os.path.exists(info_dir):
    logger.info(f'Creating {info_dir}')
    os.makedirs(info_dir, exist_ok=True)

  for catalog in catalog_all:
    file_name = catalog['id'] + '.json'
    file_name = os.path.join(info_dir, file_name)
    cdawmeta.util.write(file_name, catalog['info'])

args = cli()

if args.partial == False:
  partial = ''
  partial_dir = ''
else:
  partial = '.partial'
  partial_dir = 'partial'

in_file  = os.path.join(base_dir, partial_dir, f'cdaweb{partial}.json')

base_name = f'catalog{partial}'
catalog_file = os.path.join(base_dir, 'hapi', partial_dir, f'{base_name}.json')
catalog_err_file = os.path.join(base_dir, 'hapi', partial_dir, f'{base_name}.errors.txt')
catalog_all_file = os.path.join(base_dir, 'hapi', partial_dir, f'catalog-all{partial}.json')

log_config = {
  'file_log': os.path.join(base_dir, 'hapi', partial_dir, f'{base_name}.log'),
  'file_error': False,
  'format': '%(message)s',
  'rm_string': root_dir + '/'
}
logger = cdawmeta.util.logger(**log_config)

issues_file = os.path.join(os.path.dirname(__file__), "hapi-nl-issues.json")
logger.info(f'Reading: {issues_file}')
try:
  with open(issues_file) as f:
    issues = json.load(f)
    logger.info(f'Read: {issues_file}')
except Exception as e:
  exit(f"Error: Could not read {issues_file} file: {e}")

logger.info(f'Reading: {in_file}')
with open(in_file, 'r', encoding='utf-8') as f:
  datasets = json.load(f)
logger.info(f'Read: {in_file}')
n_cdaweb = len(datasets)

logger.info(f'Creating HAPI catalog-all from {n_cdaweb} CDAWeb datasets')
catalog_all = create_catalog_all(datasets, issues)
logger.info(f'Created catalog-all')
logger.info(f"Created {len(catalog_all)} HAPI datasets from {n_cdaweb} CDAWeb datasets")

cdawmeta.util.write(catalog_all_file, catalog_all, logger=logger)

logger.info(f"Writing {len(catalog_all)} /info files to {info_dir}")
write_infos(catalog_all, info_dir)
logger.info(f"Wrote {len(catalog_all)} /info files to {info_dir}")

# Remove 'info' key from each dataset
for dataset in catalog_all:
  del dataset['info']
cdawmeta.util.write(catalog_file, catalog_all, logger=logger)

errors = ""
for did, vars in set_error.errors.items():
  if type(vars) == str:
    errors += f"{did}: {vars}\n"
    continue
  for vid, msg in vars.items():
    msg = "\n".join(msg)
    errors += f"{did}: {msg}\n"

cdawmeta.util.write(catalog_err_file, errors, logger=logger)
