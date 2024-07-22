import os
import re

import cdawmeta

# Set to True to omit datasets that are not in Nand's metadata
omit_datasets = False

# Set to false to reduce number of warnings due to mismatch with Nand's metadata
strip_description = False

# Remove "--->" in description
remove_arrows = False

log_display_type_issues = False

log_delta_issues = False

from . import util

# These are set in hapi()
DATA_DIR = None
INFO_DIR = None
logger = None

def logger_config():

  config = {
    'name': 'hapi.py',
    'file_log': os.path.join(DATA_DIR, 'hapi', f'cdaweb2hapi.log'),
    'file_error': False,
    'format': '%(message)s',
    'rm_string': DATA_DIR + '/'
  }

  return config

def hapi(id=None, update=True, diffs=None, max_workers=None, no_orig_data=False):

  global DATA_DIR
  global INFO_DIR
  global logger
  from . import DATA_DIR
  INFO_DIR = os.path.join(DATA_DIR, 'hapi', 'info')
  logger = util.logger(**logger_config())

  if id is not None:
    file_name = os.path.join(INFO_DIR, f'{id}.json')
    if update == False and os.path.exists(file_name):
      logger.info(f'Using cache because update = False and found cached file {file_name}')
      return cdawmeta.util.read(file_name, logger=logger)

  metadata_cdaweb = cdawmeta.metadata(id=id, update=update, diffs=diffs, max_workers=max_workers, no_orig_data=no_orig_data)

  # Loop over metadata_cdaweb and call _hapi() for each id
  metadata_hapi = []
  for dsid in metadata_cdaweb.keys():
    if dsid.startswith('AIM'):
      continue
    datasets = _hapi(metadata_cdaweb[dsid], no_orig_data=no_orig_data)
    for dataset in datasets:
      metadata_hapi.append(dataset)

  write_errors()

  if id is None:

    # Write catalog-all.json and catalog.json
    fname = os.path.join(DATA_DIR, 'hapi', 'catalog-all.json')
    cdawmeta.util.write(fname, metadata_hapi, logger=logger)
    from copy import deepcopy
    metadata_hapi_copy = deepcopy(metadata_hapi)
    for metadatum in metadata_hapi_copy:
      del metadatum['info']
    fname = os.path.join(DATA_DIR, 'hapi', 'catalog.json')
    cdawmeta.util.write(fname, metadata_hapi_copy, logger=logger)

  return metadata_hapi

def _hapi(metadatum, no_orig_data=False):

  id = metadatum['id']

  issues = _issues()
  if omit_dataset(id, issues):
    return None

  sample = None
  if no_orig_data == False:
    sample = extract_sample_start_stop(metadatum)

  metadatum_cdaweb = cdawmeta.metadata(id=id, embed_data=True, update=False, no_orig_data=True)
  master = metadatum_cdaweb[id]["master"]['data']

  file = list(master.keys())[0]
  variables = master[file]['CDFVariables']
  # Split variables to be under their DEPEND_0
  vars_split = split_variables(id, variables, issues)

  logger.info(id + ": subsetting and creating /info")

  n = 0
  depend_0s = vars_split.items()
  plural = "s" if len(depend_0s) > 1 else ""
  logger.info(f"  {len(depend_0s)} DEPEND_0{plural}")

  # First pass - drop datasets with problems and create list of DEPEND_0 names
  depend_0_names = []
  for depend_0_name, depend_0_variables in depend_0s:

    logger.info(f"  Checking DEPEND_0: '{depend_0_name}'")

    if omit_dataset(id, issues, depend_0=depend_0_name):
      continue

    DEPEND_0_VAR_TYPE = variables[depend_0_name]['VarAttributes']['VAR_TYPE']

    VAR_TYPES = []
    for name, variable in depend_0_variables.items():
      VAR_TYPES.append(variable['VarAttributes']['VAR_TYPE'])
    VAR_TYPES = set(VAR_TYPES)

    logger.info(f"    VAR_TYPE: '{DEPEND_0_VAR_TYPE}'; dependent VAR_TYPES {VAR_TYPES}")

    if DEPEND_0_VAR_TYPE == 'ignore_data':
      logger.info(f"    Not creating dataset for DEPEND_0 = '{depend_0_name}' because it has VAR_TYPE='ignore_data'.")
      continue

    if 'data' not in VAR_TYPES and not keep_dataset(id, issues, depend_0=depend_0_name):
      # In general, Nand drops these, but not always
      logger.info(f"    Not creating dataset for DEPEND_0 = '{depend_0_name}' because none of its variables have VAR_TYPE = 'data'.")
      continue

    parameters = variables2parameters(depend_0_name, depend_0_variables, variables, id, print_info=False)
    if parameters == None:
      vars_split[depend_0_name] = None
      if len(depend_0s) == 1:
        logger.info(f"    Due to last error, omitting dataset with DEPEND_0 = {depend_0_name}")
      else:
        logger.info(f"    Due to last error, omitting sub-dataset with DEPEND_0 = {depend_0_name}")
      continue

    depend_0_names.append(depend_0_name)

  depend_0_names = order_depend0s(id, depend_0_names, issues)

  catalog = []
  for depend_0_name in depend_0_names:

    logger.info(f"  Creating HAPI dataset for DEPEND_0 = '{depend_0_name}'")

    depend_0_variables = vars_split[depend_0_name]

    subset = ''
    if len(depend_0_names) > 1:
      subset = '@' + str(n)

    depend_0_variables = order_variables(id + subset, depend_0_variables, issues)

    parameters = variables2parameters(depend_0_name, depend_0_variables, variables, id, print_info=True)

    dataset_new = {
      'id': id + subset,
      'description': None,
      'info': {
        **info_head(metadatum),
        "sampleStartDate": None,
        "sampleStopDate": None,
        'parameters': parameters
      }
    }

    if sample is not None:
      dataset_new['info']['sampleStartDate'] = sample['sampleStartDate']
      dataset_new['info']['sampleStopDate'] = sample['sampleStopDate']
    else:
      del dataset_new['info']['sampleStartDate']
      del dataset_new['info']['sampleStopDate']

    if metadatum['allxml'].get('description') and metadatum['allxml']['description'].get('@short'):
      dataset_new['description'] = metadatum['allxml']['description'].get('@short')
    else:
      del dataset_new['description']

    file_name = os.path.join(INFO_DIR, f'{id}.json')
    cdawmeta.util.write(file_name, dataset_new['info'], logger=logger)
    file_name = os.path.join(INFO_DIR, f'{id}.pkl')
    cdawmeta.util.write(file_name, dataset_new['info'], logger=logger)

    catalog.append(dataset_new)
    n = n + 1

  return catalog

def info_head(master):

  id = master['id']
  allxml = master['allxml']

  startDate = allxml['@timerange_start'].replace(' ', 'T') + 'Z';
  stopDate = allxml['@timerange_stop'].replace(' ', 'T') + 'Z';

  contact = ''
  if 'data_producer' in allxml:
    if '@name' in allxml['data_producer']:
      contact = allxml['data_producer']['@name']
    if '@affiliation' in allxml['data_producer']:
      contact = contact + " @ " + allxml['data_producer']['@affiliation']

  info = {
      'startDate': startDate,
      'stopDate': stopDate,
      'resourceURL': f'https://cdaweb.gsfc.nasa.gov/misc/Notes{id[0]}.html#{id}',
      'contact': contact
  }

  return info

def _issues():
  if _issues.issues is not None:
    #print(_issues.issues)
    return _issues.issues
  issues_file = os.path.join(os.path.dirname(__file__), "hapi-nl-issues.json")
  try:
    _issues.issues = cdawmeta.util.read(issues_file, logger=logger)
  except Exception as e:
    exit(f"Error: Could not read {issues_file} file: {e}")
  return _issues.issues
_issues.issues = None

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

  for name, variable in depend_0_variables.items():

    VAR_TYPE = extract_var_type(dsid, name, variable, x=None, print_info=print_info)

    if VAR_TYPE == None:
      # Should not happen because variable will be dropped in split_variables
      continue

    if VAR_TYPE != 'data':
      continue

    virtual = 'VIRTUAL' in variable['VarAttributes']
    if print_info:
      virtual_txt = f' (virtual: {virtual})'
      logger.info(f"    {name}{virtual_txt}")

    type = cdf2hapitype(variable['VarDescription']['DataType'])
    if type == None:
      msg = f"    Error: '{name}' has unhandled DataType: {variable['VarDescription']['DataType']}. Dropping variable."
      set_error(dsid, name, msg)
      logger.error(msg)
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
      "type": type,
      "units": extract_units(dsid, name, variable, x=None, print_info=print_info),
      "x_cdf_is_virtual": virtual
    }

    ptrs = extract_ptrs(dsid, name, all_variables, print_info=print_info)
    parameter['description'] = extract_description(dsid, name, variable, print_info=print_info)
    parameter['x_label'] = extract_label(dsid, name, variable, ptrs, print_info=print_info)
    parameter['x_cdf_display_type'] = extract_display_type(dsid, name, variable, print_info=log_display_type_issues)

    # TODO: Finish.
    #deltas = extract_deltas(dsid, name, variable, print_info=log_delta_issues)
    #parameter = {**parameter, **deltas}

    if length is not None:
      parameter['length'] = length

    if 'DEPEND_0' in variable['VarAttributes']:
      parameter['x_cdf_depend_0'] = variable['VarAttributes']['DEPEND_0']

    if 'DimSizes' in variable['VarDescription']:
      parameter['size'] = variable['VarDescription']['DimSizes']

    fill = None
    if 'FILLVAL' in variable['VarAttributes']:
      fill = str(variable['VarAttributes']['FILLVAL'])
    if fill is not None:
      parameter['fill'] = fill

    n_values = 0
    if ptrs['DEPEND_VALUES'] is not None:
      parameter['x_cdf_depend_values'] = ptrs['DEPEND_VALUES']
      n_values = len([x for x in ptrs["DEPEND_VALUES"] if x is not None])

    if ptrs['DEPEND'] is not None and n_values != 0:
      if print_info:
        msg = '     Error: NotImplemented: Some DEPEND_VALUES are not None and ignored.'
        logger.error(msg)
        set_error(dsid, name, msg)

    bins_object = None
    if ptrs['DEPEND'] is not None and n_values == 0:
      bins_object = bins(dsid, name, all_variables, ptrs['DEPEND'], print_info=print_info)
      if bins_object is not None:
        parameter['bins'] = bins_object

    if print_info:
      for key, value in parameter.items():
        logger.info(f"       {key} = {value}")
      if bins_object is not None:
        for idx, bin in enumerate(bins_object):
          bin_copy = bin.copy()
          if 'centers' in bin and len(bin['centers']) > 10:
            bin_copy['centers'] = f'{bin["centers"][0]} ... {bin["centers"][-1]}'  
          logger.info(f"      bins[{idx}] = {bin_copy}")

    parameters.append(parameter)

  return parameters

def bins(dsid, name, all_variables, depend_xs, print_info=False):

  variable = all_variables[name]
  NumDims = variable['VarDescription'].get('NumDims', 0)
  DimSizes = variable['VarDescription'].get('DimSizes', [])
  DimVariances = variable['VarDescription'].get('DimVariances', [])

  if print_info:
    logger.info(f"     NumDims: {NumDims}")
    logger.info(f"     DimSizes: {DimSizes}")
    logger.info(f"     DimVariances: {DimVariances}")

  if NumDims != len(DimSizes):
    if print_info:
      msg = f"     Error: DimSizes mismatch: NumDims = {NumDims} "
      msg += "!= len(DimSizes) = {len(DimSizes)}"
      logger.error(msg)
      set_error(dsid, name, msg)
    return None

  if len(DimSizes) != len(DimVariances):
    if print_info:
      msg = f"     Error: DimVariances mismatch: len(DimSizes) = {DimSizes} "
      msg += "!= len(DimVariances) = {len(DimVariances)}"
      logger.error(msg)
      set_error(dsid, name, msg)
    return None

  bins_objects = []
  for x in range(len(depend_xs)):
    DEPEND_x_NAME = depend_xs[x]
    if DEPEND_x_NAME is not None:
      hapitype = cdf2hapitype(all_variables[DEPEND_x_NAME]['VarDescription']['DataType'])
      if hapitype in ['integer', 'double']:
        bins_object = create_bins(dsid, name, x, DEPEND_x_NAME, all_variables, print_info=print_info)
      else:
        msg = f"DEPEND_{x} = '{DEPEND_x_NAME}' DataType is not an integer or float. Omitting bins for {name}."
        if print_info:
          logger.info(f"     Warning: NotImplemented[2]: {msg}")
          logger.info(f"     Warning: NotImplemented[2]: {DEPEND_x_NAME}['VarData'] = {all_variables[DEPEND_x_NAME]['VarData']}")
        return None
        #bins_object = {'label': all_variables[DEPEND_x_NAME]['VarData']}

      if bins_object is None:
        return None
      bins_objects.append(bins_object)

  return bins_objects

def create_bins(dsid, name, x, DEPEND_x_NAME, all_variables, print_info=False):

  # TODO: Check for multi-dimensional DEPEND_x
  DEPEND_x = all_variables[DEPEND_x_NAME]
  RecVariance = "NOVARY"
  if "RecVariance" in DEPEND_x['VarDescription']:
    RecVariance = DEPEND_x['VarDescription']["RecVariance"]
    if print_info:
      logger.info(f"     DEPEND_{x} has RecVariance = " + RecVariance)

  if RecVariance == "VARY":
    if print_info:
      logger.info(f"     Warning: NotImplemented[3]: DEPEND_{x} = {DEPEND_x_NAME} has RecVariance = 'VARY'. Not creating bins b/c Nand does not for this case.")
    return None

  VAR_TYPE = extract_var_type(dsid, name, DEPEND_x, x=x, print_info=print_info)
  if VAR_TYPE is None:
    if print_info:
      logger.error(f"     Error: ISTP: DEPEND_{x} = {DEPEND_x_NAME} has no VAR_TYPE. Not creating bins.")
    return None

  ptrs = extract_ptrs(dsid, DEPEND_x_NAME, all_variables, print_info=print_info)

  if 'VarData' in DEPEND_x:
    bins_object = {
                    "name": DEPEND_x_NAME,
                    "description": extract_description(dsid, name, DEPEND_x, x=x, print_info=print_info),
                    "x_label": extract_label(dsid, name, DEPEND_x, ptrs, x=x, print_info=print_info),
                    "units": extract_units(dsid, name, DEPEND_x, x=x, print_info=print_info),
                    "centers": DEPEND_x["VarData"]
                  }
    return bins_object
  else:
    if print_info:
      logger.info(f"     Warning: Not including bin centers for {DEPEND_x_NAME} b/c no VarData (is probably VIRTUAL)")
    return None

def order_depend0s(id, depend0_names, issues):

  if id not in issues['depend0Order'].keys():
    return depend0_names

  order_wanted = issues['depend0Order'][id]

  for depend0_name in order_wanted:
    if not depend0_name in depend0_names:
      if logger:
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
    if logger:
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
    if logger:
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
    if logger:
      logger.info(id)
      logger.info(f"  Warning: Dropping variable \"{variable_name}\" b/c it is not in Nand's list")
    return True
  return False

def extract_sample_start_stop(metadatum):

  if "orig_data" not in metadatum:
    logger.info("No orig_data for " + metadatum["id"])
    return None
  if "data-cache" not in metadatum['orig_data']:
    logger.info("No orig_data['data-cache'] for " + metadatum["id"])
    return None

  if not 'data' in metadatum["orig_data"]:
    fname = os.path.join(cdawmeta.DATA_DIR, metadatum["orig_data"]['data-cache'].replace(".json", ".pkl"))
    orig_data = cdawmeta.util.read(fname, logger=logger)['data']
  else:
    orig_data = metadatum["orig_data"]['data']

  if not "FileDescription" in orig_data:
    logger.info("No orig_data for " + metadatum["id"])
    return None

  if isinstance(orig_data["FileDescription"], dict):
    orig_data["FileDescription"] = [orig_data["FileDescription"]]

  num_files = len(orig_data["FileDescription"])
  if num_files == 0:
    sampleFile = None
  if num_files == 1:
    sampleFile = orig_data["FileDescription"][0]
  elif num_files == 2:
    sampleFile = orig_data["FileDescription"][1]
  else:
    sampleFile = orig_data["FileDescription"][-2]

  if sampleFile is not None:
    sampleStartDate = sampleFile["StartTime"]
    sampleStopDate = sampleFile["EndTime"]

  range = {
            "sampleStartDate": sampleStartDate,
            "sampleStopDate": sampleStopDate
          }

  return range

def extract_deltas(dsid, name, variable, print_info=False):

  not_implemented = ['DELTA_PLUS', 'DELTA_MINUS',
                    'DELTA_PLUS_VAR', 'DELTA_MINUS_VAR'
                    'DELTA_PLUS_VARx', 'DELTA_MINUS_VARx']

  deltas = {}
  for attrib in not_implemented:
    if attrib in variable['VarAttributes']:
      attrib_val = variable['VarAttributes'][attrib]
      deltas[attrib] = attrib_val
      if print_info:
        msg =f"    Error: NotImplemented[DELTA]: {attrib} = '{attrib_val}' not used"
        logger.error(msg)
        set_error(dsid, name, msg)
  return deltas

def extract_display_type(dsid, name, variable, print_info=False):

  if not 'DISPLAY_TYPE' in variable['VarAttributes']:
    if print_info:
      logger.info(f"     No DISPLAY_TYPE for variable '{name}'")
    if variable['VarAttributes'].get('VAR_TYPE') == 'data':
      if print_info:
        msg = f"     Error: ISTP[DISPLAY_TYPE]: No attribute for variable '{name}' with VAR_TYPE = data'"
        logger.error(msg)
        set_error(dsid, name, msg)
    return None

  DISPLAY_TYPE = variable['VarAttributes']['DISPLAY_TYPE']
  DISPLAY_TYPE = DISPLAY_TYPE.split(">")[0]

  if DISPLAY_TYPE == ' ':
    if print_info:
      msg = f"     Error: ISTP[DISPLAY_TYPE] = '{DISPLAY_TYPE}'"
      logger.error(msg)
      set_error(dsid, name, msg)
    return False

  if DISPLAY_TYPE.strip() == '':
    if print_info:
      msg = f"     Error: ISTP[DISPLAY_TYPE]: DISPLAY_TYPE.strip() = ''"
      logger.error(msg)
      set_error(dsid, name, msg)
    return False

  display_types_known = [
    'time_series',
    'spectrogram',
    'stack_plot',
    'image',
    'no_plot',
    'orbit',
    'plasmagram',
    'skymap'
  ]

  if DISPLAY_TYPE not in display_types_known:
    if print_info:
      msg = f"     Error: ISTP[DISPLAY_TYPE]: DISPLAY_TYPE = '{DISPLAY_TYPE}' is not in "
      msg += f"{display_types_known}. Will attempt to infer."
      logger.error(msg)
      set_error(dsid, name, msg)

  found = False
  for display_type in display_types_known:
    if DISPLAY_TYPE.lower().startswith(display_type):
      found = True
      if print_info:
        logger.info(f"     DISPLAY_TYPE = '{DISPLAY_TYPE}'")
      break
  if not found and print_info:
    msg = f"     Error: ISTP[DISPLAY_TYPE]: DISPLAY_TYPE.lower() = "
    msg += "'{DISPLAY_TYPE}' does not start with one of {display_types_known}"
    logger.error(msg)
    set_error(msg)

  return DISPLAY_TYPE

def extract_description(dsid, name, variable, x=None, print_info=False):

  # TODO: This was written to match Nand's logic and reduce number of mis-matches.
  #       This should be modified to use FIELDNAM.
  desc = ""

  CATDESC = ""
  if 'CATDESC' in variable['VarAttributes']:
    CATDESC = variable['VarAttributes']['CATDESC']
    if isinstance(CATDESC, list):
      CATDESC = '\n'.join(CATDESC)

  VAR_NOTES = ""
  if 'VAR_NOTES' in variable['VarAttributes']:
    VAR_NOTES = variable['VarAttributes']['VAR_NOTES']
    if isinstance(VAR_NOTES, list):
      VAR_NOTES = '\n'.join(VAR_NOTES)

  FIELDNAM = ""
  if 'FIELDNAM' in variable['VarAttributes']:
    FIELDNAM = variable['VarAttributes']['FIELDNAM']
    if isinstance(FIELDNAM, list):
      FIELDNAM = '\n'.join(FIELDNAM)

  if VAR_NOTES == CATDESC:
    desc = f"{CATDESC}"
  elif CATDESC.strip() != "" and VAR_NOTES.strip() == "":
    desc = f"{CATDESC}"
  elif VAR_NOTES.strip() != "" and CATDESC.strip() == "":
    desc = f"{CATDESC}"
  elif CATDESC.strip() != "" and VAR_NOTES.strip() != "":
    desc = CATDESC
    xdesc = f"CATDESC: {CATDESC}; VAR_NOTES: {VAR_NOTES}"

  if strip_description:
    desc = desc.strip()

  if remove_arrows:
    desc = desc.replace('--->', '')

  return desc

def extract_ptrs(dsid, name, all_variables, print_info=False):

  variable = all_variables[name]
  DimSizes = variable['VarDescription'].get('DimSizes', [])
  ptrs = {}
  for prefix in ['DEPEND', 'LABL_PTR', 'COMPONENT']:
    ptrs[prefix] = [None, None, None]
    ptrs[prefix+"_VALID"] = [None, None, None]
    ptrs[prefix+"_VALUES"] = [None, None, None]
    for x in [1, 2, 3]:
      if f'{prefix}_{x}' in variable['VarAttributes']:
        x_NAME = variable['VarAttributes'][f'{prefix}_{x}']
        if not x_NAME in all_variables:
          ptrs[prefix+"_VALID"][x-1] = False
          if print_info:
            msg = f"Error: Bad {prefix} reference: '{name}' has {prefix}_{x} "
            msg += f"named '{x_NAME}', which is not a variable."
            logger.error(f"     {msg}")
            set_error(dsid, name, msg)
        elif prefix == 'LABL_PTR' or (prefix == 'DEPEND' and 'string' == cdf2hapitype(all_variables[x_NAME]['VarDescription']['DataType'])):
          if 'VarData' in all_variables[x_NAME]:
            ptrs[prefix+"_VALID"][x-1] = True
            ptrs[prefix][x-1] = x_NAME
            values = trim(all_variables[x_NAME]['VarData'])
            ptrs[prefix+"_VALUES"][x-1] = values
            if print_info:
              logger.info(f"     {prefix}_{x}: {x_NAME}")
              logger.info(f"     {prefix}_{x} trimmed values: {values}")
          else:
            ptrs[prefix+"_VALID"][x-1] = False
            if print_info:
              if prefix == 'LABL_PTR':
                msg = f"Error: {x_NAME} has no VarData"
              else:
                msg = f"Error: {x_NAME} is a string type but has no VarData"
              logger.error(f"     {msg}")
              set_error(dsid, name, msg)
        else:
          ptrs[prefix+"_VALID"][x-1] = True
          ptrs[prefix][x-1] = x_NAME
          if print_info:
            logger.info(f"     {prefix}_{x}: {x_NAME}")

    n_valid = len([x for x in ptrs[prefix+"_VALID"] if x is True])
    n_invalid = len([x for x in ptrs[prefix+"_VALID"] if x is False])
    n_found = len([x for x in ptrs[prefix+"_VALID"] if x is not None])
    if n_invalid > 0:
      ptrs[prefix] = None
      msg = f"Error: '{name}' has {n_invalid} invalid elements."
      logger.error(f"     {msg}")
      set_error(dsid, name, msg)
    elif prefix != 'COMPONENT':
      if n_valid != len(DimSizes):
        ptrs[prefix] = None
        if False and print_info:
          # Not necessarily an error for DEPEND. Depends on DISPLAY_TYPE.
          # https://spdf.gsfc.nasa.gov/istp_guide/vattributes.html
          msg = f"ISTP Error: '{name}' has {n_valid} valid elements {prefix}_{{1,2,3}}, but need "
          msg += f"len(DimSizes) = {len(DimSizes)}."
          logger.error(f"     {msg}")
          set_error(dsid, name, msg)
      if n_found != 0 and n_found != len(DimSizes):
        ptrs[prefix] = None
        if False and print_info:
          # Not necessarily an error for DEPEND. Depends on DISPLAY_TYPE.
          # https://spdf.gsfc.nasa.gov/istp_guide/vattributes.html
          msg = f"ISTP Error: Wrong number of {prefix}s: '{name}' has {n_found} of "
          msg += f"{prefix}_{{1,2,3}} and len(DimSizes) = {len(DimSizes)}."
          logger.error(f"     {msg}")
          set_error(dsid, name, msg)

    if n_found == 0:
      ptrs[prefix] = None
      ptrs[prefix+"_VALUES"] = None

    if ptrs[prefix] is not None:
      ptrs[prefix] = ptrs[prefix][0:len(DimSizes)]
      ptrs[prefix+"_VALUES"] = ptrs[prefix+"_VALUES"][0:len(DimSizes)]

      if len(ptrs[prefix]) == 0:
        ptrs[prefix] = None
        ptrs[prefix+"_VALUES"] = None

  return ptrs

def extract_label(dsid, name, variable, ptrs, x=None, print_info=False):

  if 'LABLAXIS' in variable['VarAttributes']:
    msgx = ""
    if x is not None:
      msgx = f"DEPEND_{x} "
    if print_info:
      logger.info(f"     {msgx}LABLAXIS = {variable['VarAttributes']['LABLAXIS']}")
    if ptrs['LABL_PTR'] is not None:
      if print_info:
        msg = f"Warning: For {msgx}variable '{name}', LABL_PTR = {ptrs['LABL_PTR']} and LABLAXIS = "
        msg += f"{variable['VarAttributes']['LABLAXIS']}. Using LABLAXIS."
        logger.info(f"     {msg}")
    return trim(variable['VarAttributes']['LABLAXIS'])

  if ptrs['LABL_PTR'] is not None:
    if len(ptrs['LABL_PTR_VALUES']) == 1:
      ptrs['LABL_PTR_VALUES'] = ptrs['LABL_PTR_VALUES'][0]
    return ptrs['LABL_PTR_VALUES']

  return None

def extract_var_type(dsid, name, variable, x=None, print_info=None):

  if 'VAR_TYPE' in variable['VarAttributes']:
    return variable['VarAttributes']['VAR_TYPE']
  else:
    if print_info:
      msgx = ""
      if x is not None:
        msgx = f"DEPEND_{x} "
      msg = f"     Error: ISTP: {msgx}variable has no VAR_TYPE."
      logger.error(msg)
      set_error(dsid, name, msg)
    return None

def extract_units(dsid, name, variable, x=None, print_info=None):

  msgx = ""
  if x is not None:
    msgx = f"DEPEND_{x} "

  units = None
  if "UNITS" in variable['VarAttributes']:
    units = variable['VarAttributes']["UNITS"]
  else:
    if "UNIT_PTR" in variable['VarAttributes']:
      if print_info:
        msg = f"     Error: NotImplemented: {msgx}variable '{name}'"
        msg += f"has UNIT_PTR = '{variable['VarAttributes']['UNIT_PTR']}'. Not using."
        logger.error(msg)
        set_error(dsid, name, msg)

    VAR_TYPE = extract_var_type(dsid, name, variable, x=x, print_info=print_info)
    if VAR_TYPE is not None and VAR_TYPE in ['data', 'support_data']:
      if not "UNIT_PTR" in variable['VarAttributes']:
        if print_info:
          msg = f"     Error: ISTP: {msgx}variable '{name}' has VAR_TYPE "
          msg += f"'{VAR_TYPE}' and no UNITS or UNIT_PTR."
          logger.error(msg)
          set_error(dsid, name, msg)

  return units

def split_variables(id, variables, issues):
  """
  Create _variables_split dict. Each key is the name of the DEPEND_0
  variable. Each value is a dict of variables that reference that DEPEND_0
  """

  depend_0_dict = {}

  names = variables.keys()
  for name in names:

    variable_meta = variables[name]

    if 'VarAttributes' not in variable_meta:
      logger.error(id)
      msg = f"  Error: Dropping variable '{name}' b/c it has no VarAttributes"
      logger.error(msg)
      continue

    if 'VAR_TYPE' not in variable_meta['VarAttributes']:
      msg = f"  Error: Dropping variable '{name}' b/c it has no has no VAR_TYPE"
      logger.error(id)
      logger.error(msg)
      set_error(id, name, msg)
      continue

    if omit_variable(id, name, issues):
      continue

    if 'DEPEND_0' in variable_meta['VarAttributes']:
      depend_0_name = variable_meta['VarAttributes']['DEPEND_0']

      if depend_0_name not in variables:
        msg = f"  Error: Dropping '{name}' b/c it has a DEPEND_0 ('{depend_0_name}') that is not in dataset"
        logger.error(id)
        logger.error(msg)
        set_error(id, name, msg)
        continue

      if depend_0_name not in depend_0_dict:
        depend_0_dict[depend_0_name] = {}
      depend_0_dict[depend_0_name][name] = variable_meta

  return depend_0_dict

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

def write_errors():
  # Write all errors to a single file if all datasets were requested. Errors
  # were already written to log file, but here we need to do additional formatting
  # that is more difficult if errors were written as they occur.
  errors = ""
  for dsid, vars in set_error.errors.items():
    if type(vars) == str:
      errors += f"{dsid}: {vars}\n"
      continue
    errors += f"{dsid}:\n"
    for vid, msgs in vars.items():
      errors += f"  {vid}:\n"
      for msg in msgs:
        errors += f"    {msg}\n"
  cdawmeta.util.write(os.path.join(DATA_DIR, 'hapi', 'cdaweb2hapi.errors.log'), errors)

def trim(label):
  if isinstance(label, str):
    return label.strip()
  for i in range(0, len(label)):
    label[i] = str(label[i]).strip()
  return label

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
