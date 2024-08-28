import os
import re

import cdawmeta

logger = None
fixes = None

def hapi(id=None, update=True, diffs=None, max_workers=None, no_orig_data=False):

  global logger
  if logger is None:
    logger = cdawmeta.logger('hapi')

  global fixes
  fixes = cdawmeta.CONFIG['hapi']['fixes']

  if id is not None:
    file_name = os.path.join(cdawmeta.INFO_DIR, f'{id}.json')
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

  _write_errors()

  if id is None:

    # Write catalog-all.json and catalog.json
    fname = os.path.join(cdawmeta.DATA_DIR, 'hapi', 'catalog-all.json')
    cdawmeta.util.write(fname, metadata_hapi, logger=logger)
    from copy import deepcopy
    metadata_hapi_copy = deepcopy(metadata_hapi)
    for metadatum in metadata_hapi_copy:
      del metadatum['info']
    fname = os.path.join(cdawmeta.DATA_DIR, 'hapi', 'catalog.json')
    cdawmeta.util.write(fname, metadata_hapi_copy, logger=logger)

  return metadata_hapi

def _hapi(metadatum, no_orig_data=False):

  id = metadatum['id']

  if _omit_dataset(id):
    return None

  sample = None
  if no_orig_data == False:
    sample = _sample_start_stop(metadatum)

  metadatum_cdaweb = cdawmeta.metadata(id=id, update=False, embed_data=True, no_spase=True, no_orig_data=True)
  master = metadatum_cdaweb[id]["master"]['data']

  variables = master['CDFVariables']
  # Split variables to be under their DEPEND_0 (Keys are DEPEND_0 names), e.g.,
  # {'Epoch': {'VAR1': {...}, 'VAR2': {...}}, 'Epoch2': {'VAR3': {...}, 'VAR4': {...}}}
  vars_split = _split_variables(id, variables)

  logger.info(id + ": subsetting and creating /info")

  n = 0
  depend_0s = vars_split.items()
  # depend_0s = dict_items([('Epoch', {'VAR1': {}, ...}), ('Epoch2', {'VAR3': {}, ...})])

  plural = "s" if len(depend_0s) > 1 else ""
  logger.info(f"  {len(depend_0s)} DEPEND_0{plural}")

  # First pass - drop datasets with problems and create list of DEPEND_0 names
  depend_0_names = []
  for depend_0_name, depend_0_variables in depend_0s:

    logger.info(f"  Checking DEPEND_0: '{depend_0_name}'")

    if _omit_dataset(id, depend_0=depend_0_name):
      continue

    DEPEND_0_VAR_TYPE = variables[depend_0_name]['VarAttributes']['VAR_TYPE']

    VAR_TYPES = []
    for _, variable in depend_0_variables.items():
      VAR_TYPES.append(variable['VarAttributes']['VAR_TYPE'])
    VAR_TYPES = set(VAR_TYPES) # Unique VAR_TYPES of DEPEND_0 variables

    logger.info(f"    VAR_TYPE: '{DEPEND_0_VAR_TYPE}'; dependent VAR_TYPES {VAR_TYPES}")

    if DEPEND_0_VAR_TYPE == 'ignore_data':
      logger.info(f"    Not creating dataset for DEPEND_0 = '{depend_0_name}' because it has VAR_TYPE='ignore_data'.")
      continue

    if 'data' not in VAR_TYPES and not _keep_dataset(id, depend_0=depend_0_name):
      # In general, Nand drops these, but not always
      logger.info(f"    Not creating dataset for DEPEND_0 = '{depend_0_name}' because none of its variables have VAR_TYPE = 'data'.")
      continue

    parameters = _variables2parameters(depend_0_name, depend_0_variables, variables, id, print_info=False)
    if parameters == None:
      vars_split[depend_0_name] = None
      if len(depend_0s) == 1:
        logger.info(f"    Due to last error, omitting dataset with DEPEND_0 = {depend_0_name}")
      else:
        logger.info(f"    Due to last error, omitting sub-dataset with DEPEND_0 = {depend_0_name}")
      continue

    depend_0_names.append(depend_0_name)

  depend_0_names = _order_depend0s(id, depend_0_names)

  catalog = []
  for depend_0_name in depend_0_names:

    logger.info(f"  Creating HAPI dataset for DEPEND_0 = '{depend_0_name}'")

    depend_0_variables = vars_split[depend_0_name]

    subset = ''
    if len(depend_0_names) > 1:
      subset = '@' + str(n)

    depend_0_variables = _order_variables(id + subset, depend_0_variables)

    parameters = _variables2parameters(depend_0_name, depend_0_variables, variables, id, print_info=True)

    dataset_new = {
      'id': id + subset,
      'description': None,
      'info': {
        **_info_head(metadatum),
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

    file_name = os.path.join(cdawmeta.INFO_DIR, f'{id}.json')
    cdawmeta.util.write(file_name, dataset_new['info'], logger=logger)
    file_name = os.path.join(cdawmeta.INFO_DIR, f'{id}.pkl')
    cdawmeta.util.write(file_name, dataset_new['info'], logger=logger)

    catalog.append(dataset_new)
    n = n + 1

  return catalog

def _info_head(metadatum):

  id = metadatum['id']
  allxml = metadatum['allxml']

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

def _variables2parameters(depend_0_name, depend_0_variables, all_variables, dsid, print_info=False):

  depend_0_variable = all_variables[depend_0_name]

  if 'DataType' not in depend_0_variable['VarDescription']:
    msg = f"    Error: DEPEND_0 variable '{dsid}'/{depend_0_name} has no DataType attribute. Dropping variables associated with it"
    _error(dsid, name, msg)
    return None

  DEPEND_0_DataType = depend_0_variable['VarDescription']['DataType']
  DEPEND_0_length = cdftimelen(DEPEND_0_DataType)

  if DEPEND_0_length == None:
    msg = f"    Warning: DEPEND_0 variable '{dsid}'/{depend_0_name} has unhandled type: '{DEPEND_0_DataType}'. "
    msg += f"Dropping variables associated with it"
    logger.info(msg)
    return None

  parameters = [
                  {
                    'name': 'Time',
                    'type': 'isotime',
                    'units': 'UTC',
                    'length': DEPEND_0_length,
                    'fill': None,
                    'x_cdf_depend_0_name': depend_0_name
                  }
                ]

  for name, variable in depend_0_variables.items():

    VAR_TYPE, emsg = cdawmeta.attrib.VAR_TYPE(dsid, name, variable, x=None)
    if emsg is not None:
      # Should not happen because variable will be dropped in _split_variables
      _error(dsid, name, emsg)
      continue

    if VAR_TYPE != 'data':
      continue

    virtual = 'VIRTUAL' in variable['VarAttributes']
    if print_info:
      virtual_txt = f' (virtual: {virtual})'
      logger.info(f"    {name}{virtual_txt}")

    type = cdf2hapitype(variable['VarDescription']['DataType'])
    if type == None and print_info:
      msg = f"    Error: '{name}' has unhandled DataType: {variable['VarDescription']['DataType']}. Dropping variable."
      _error(dsid, name, msg)
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
        _error(dsid, name, msg)
        continue

      if NumElements is None:
        if PadValue != None and FillValue != None and PadValue != FillValue:
          msg = f"    Error: Dropping '{name}' because PadValue and FillValue lengths differ."
          _error(dsid, name, msg)
          continue

      if PadValue != None:
        length = len(PadValue)
      if FillValue != None:
        length = len(FillValue)
      if NumElements != None:
        length = int(NumElements)

    UNITS, emsg = cdawmeta.attrib.UNITS(dsid, name, all_variables, x=None)
    if emsg is not None and print_info:
      _error(dsid, name, emsg)

    parameter = {
      "name": name,
      "type": type,
      "units": UNITS
    }

    parameter['description'] = _description(dsid, name, variable, print_info=print_info)

    fill = None
    if 'FILLVAL' in variable['VarAttributes']:
      fill = str(variable['VarAttributes']['FILLVAL'])
    if fill is not None:
      parameter['fill'] = fill

    ptrs = _pointers(dsid, name, all_variables, print_info=print_info)

    LABLAXIS, emsg = cdawmeta.attrib.LABLAXIS(dsid, name, variable, ptrs)
    if LABLAXIS is not None and cdawmeta.CONFIG['hapi']['keep_label']:
      parameter['label'] = LABLAXIS
    else:
      parameter['x_label'] = LABLAXIS

    FORMAT, emsg = cdawmeta.attrib.FORMAT(dsid, name, all_variables)
    if FORMAT is not None:
      parameter['x_format'] = FORMAT
      if emsg is not None and print_info:
        _error(dsid, name, emsg)

    parameter["x_cdf_is_virtual"] = True
    DISPLAY_TYPE, emsg = cdawmeta.attrib.DISPLAY_TYPE(dsid, name, variable)
    if DISPLAY_TYPE is not None:
      parameter['x_cdf_display_type'] = DISPLAY_TYPE
      if print_info:
        logger.info(f"     DISPLAY_TYPE = {DISPLAY_TYPE}")
    if emsg is not None and print_info:
      if cdawmeta.CONFIG['hapi']['log_display_type_issues']:
        _error(dsid, name, emsg)

    # TODO: Finish.
    #deltas = _extract_deltas(dsid, name, variable, print_info=cdawmeta.CONFIG['hapi']['log_delta_issues'])
    #parameter = {**parameter, **deltas}

    if length is not None:
      parameter['length'] = length

    if 'DEPEND_0' in variable['VarAttributes']:
      parameter['x_cdf_depend_0'] = variable['VarAttributes']['DEPEND_0']

    if 'DimSizes' in variable['VarDescription']:
      parameter['size'] = variable['VarDescription']['DimSizes']

    if 'DataType' not in variable['VarDescription']:
      msg = f"    Error: variable '{dsid}'/{depend_0_name} has no DataType attribute. Dropping variables associated with it"
      _error(dsid, name, msg)
      return None

    n_depend_values = 0
    if ptrs['DEPEND_VALUES'] is not None:
      #parameter['x_cdf_depend_values'] = ptrs['DEPEND_VALUES']
      n_depend_values = len([x for x in ptrs["DEPEND_VALUES"] if x is not None])

    n_label_values = 0
    if ptrs['LABL_PTR_VALUES'] is not None:
      n_label_values = len([x for x in ptrs["LABL_PTR_VALUES"] if x is not None])

    if ptrs['DEPEND'] is not None and n_depend_values != 0:
      differ = False
      if n_label_values != n_depend_values:
        differ = True
      else:
        for x in range(n_depend_values):
          if ptrs['LABL_PTR_VALUES'][x] != ptrs['DEPEND_VALUES'][x]:
            differ = True
            if differ and print_info:
              msg = f'     Warning: NotImplemented[RedundantDependValues]: DEPEND_{x} has is string type and LABL_PTR_{x} given. They differ; using LABL_PTR_{x} for HAPI label attribute.'
              logger.info(msg)
              break

    bins_object = None
    if ptrs['DEPEND'] is not None and n_depend_values == 0:
      bins_object = _bins(dsid, name, all_variables, ptrs['DEPEND'], print_info=print_info)
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

def _bins(dsid, name, all_variables, depend_xs, print_info=False):

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
      msg = f"     Error: ISTP: DimSizes mismatch: NumDims = {NumDims} "
      msg += "!= len(DimSizes) = {len(DimSizes)}"
      logger.error(msg)
      _error(dsid, name, msg)
    return None

  if len(DimSizes) != len(DimVariances):
    if print_info:
      msg = f"     Error: DimVariances mismatch: len(DimSizes) = {DimSizes} "
      msg += "!= len(DimVariances) = {len(DimVariances)}"
      _error(dsid, name, msg)
    return None

  bins_objects = []
  for x in range(len(depend_xs)):
    DEPEND_x_NAME = depend_xs[x]
    if DEPEND_x_NAME is not None:
      hapitype = cdf2hapitype(all_variables[DEPEND_x_NAME]['VarDescription']['DataType'])
      if hapitype in ['integer', 'double']:
        # Other cases are handled in _variables2parameters
        bins_object = _create_bins(dsid, name, x, DEPEND_x_NAME, all_variables, print_info=print_info)

      if bins_object is None:
        return None
      bins_objects.append(bins_object)

  return bins_objects

def _create_bins(dsid, name, x, DEPEND_x_NAME, all_variables, print_info=False):

  # TODO: Check for multi-dimensional DEPEND_x
  DEPEND_x = all_variables[DEPEND_x_NAME]
  RecVariance = "NOVARY"
  if "RecVariance" in DEPEND_x['VarDescription']:
    RecVariance = DEPEND_x['VarDescription']["RecVariance"]
    if print_info:
      logger.info(f"     DEPEND_{x} has RecVariance = " + RecVariance)

  if RecVariance == "VARY":
    if print_info:
      logger.info(f"     Warning: NotImplemented[TimeVaryingBins]: DEPEND_{x} = {DEPEND_x_NAME} has RecVariance = 'VARY'. Not creating bins b/c Nand does not for this case.")
    return None

  _, emsg = cdawmeta.attrib.VAR_TYPE(dsid, name, DEPEND_x, x=x)
  if emsg is not None:
    if print_info:
      emsg = emsg +  " Not creating bins."
      _error(dsid, name, emsg)
    return None

  ptrs = _pointers(dsid, DEPEND_x_NAME, all_variables, print_info=print_info)

  UNITS, emsg = cdawmeta.attrib.UNITS(dsid, DEPEND_x_NAME, all_variables, x=x)
  if emsg is not None and print_info:
    _error(dsid, DEPEND_x_NAME, emsg)

  LABLAXIS, emsg = cdawmeta.attrib.LABLAXIS(dsid, name, DEPEND_x, ptrs, x=x)
  if emsg is not None and print_info:
    _error(dsid, name, emsg)

  if 'VarData' in DEPEND_x:
    bins_object = {
                    "name": DEPEND_x_NAME,
                    "description": _description(dsid, name, DEPEND_x, x=x, print_info=print_info),
                    "units": UNITS,
                    "centers": DEPEND_x["VarData"],
                    "x_label": LABLAXIS
                  }
    return bins_object
  else:
    if print_info:
      logger.info(f"     Warning: Not including bin centers for {DEPEND_x_NAME} b/c no VarData (is probably VIRTUAL)")
    return None

def _order_depend0s(id, depend0_names):

  if id not in fixes['depend0Order'].keys():
    return depend0_names

  order_wanted = fixes['depend0Order'][id]

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

def _order_variables(id, variables):

  if id not in fixes['variableOrder'].keys():
    return variables

  order_wanted = fixes['variableOrder'][id]
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

def _split_variables(id, variables):
  """
  Create _variables_split dict. Each key is the name of the DEPEND_0
  variable. Each value is a dict of variables that reference that DEPEND_0
  """

  depend_0_dict = {}

  names = variables.keys()
  for name in names:

    variable_meta = variables[name]

    if 'VarAttributes' not in variable_meta:
      msg = f"  Error: ISTP: Dropping variable '{name}' b/c it has no VarAttributes"
      _error(id, name, msg)
      continue

    if 'VAR_TYPE' not in variable_meta['VarAttributes']:
      msg = f"  Error: ISTP: Dropping variable '{name}' b/c it has no has no VAR_TYPE"
      _error(id, name, msg)
      continue

    if _omit_variable(id, name):
      continue

    if 'DEPEND_0' in variable_meta['VarAttributes']:
      depend_0_name = variable_meta['VarAttributes']['DEPEND_0']

      if depend_0_name not in variables:
        msg = f"  Error: ISTP: Dropping '{name}' b/c it has a DEPEND_0 ('{depend_0_name}') that is not in dataset"
        _error(id, name, msg)
        continue

      if depend_0_name not in depend_0_dict:
        depend_0_dict[depend_0_name] = {}
      depend_0_dict[depend_0_name][name] = variable_meta

  return depend_0_dict

def _keep_dataset(id, depend_0=None):
  if id in fixes['keepSubset'].keys() and depend_0 == fixes['keepSubset'][id]:
    if logger:
      logger.info(id)
      logger.info(f"  Warning: Keeping dataset associated with \"{depend_0}\" b/c it is in Nand's list")
    return True
  return False

def _omit_dataset(id, depend_0=None):

  omit_datasets = cdawmeta.CONFIG['hapi']['omit_datasets']
  if depend_0 is None:
    if id in fixes['omitAll'].keys():
      if omit_datasets:
        logger.info(id)
        logger.info(f"    Warning: Dropping dataset {id} b/c it is not in Nand's list")
        return True
      else:
        logger.info(id)
        logger.info(f"    Warning: Keeping dataset {id} even though it is not in Nand's list")
        return False
    for pattern in fixes['omitAllPattern']:
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
    if id in fixes['omitSubset'].keys() and depend_0 in fixes['omitSubset'][id]:
      logger.info(f"    Warning: Dropping variables associated with DEPEND_0 = \"{depend_0}\" b/c this DEPEND_0 is not in Nand's list")
      return True
  return False

def _omit_variable(id, variable_name):

  for key in list(fixes['omitVariables'].keys()):
    # Some keys of fixes['omitVariables'] are ids with @subset_number"
    # The @subset_number is not needed, but kept for reference.
    # Here we concatenate all variables with common dataset base
    # name (variable names are unique within a dataset, so this works).
    newkey = key.split("@")[0]
    if newkey != key:
      if newkey not in fixes['omitVariables'].keys():
        fixes['omitVariables'][newkey] = fixes['omitVariables'][key]
      else:
        # Append new list to existing list
        fixes['omitVariables'][newkey] += fixes['omitVariables'][key]
      del fixes['omitVariables'][key]

  if id in fixes['omitVariables'].keys() and variable_name in fixes['omitVariables'][id]:
    if logger:
      logger.info(id)
      logger.info(f"  Warning: Dropping variable \"{variable_name}\" b/c it is not in Nand's list")
    return True
  return False

def _description(dsid, name, variable, x=None, print_info=False):

  # TODO: This was written to match Nand's logic and reduce number of mis-matches.
  #       This should be modified to use FIELDNAM.
  desc = ""

  CATDESC, emsg = cdawmeta.attrib.CATDESC(dsid, name, variable)
  if CATDESC is None:
    CATDESC = ""
  if emsg is not None and print_info:
    _error(dsid, name, emsg)

  VAR_NOTES, emsg = cdawmeta.attrib.VAR_NOTES(dsid, name, variable)
  if VAR_NOTES is None:
    VAR_NOTES = ""
  if emsg is not None and print_info:
    _error(dsid, name, emsg)

  FIELDNAM, emsg = cdawmeta.attrib.FIELDNAM(dsid, name, variable)
  if FIELDNAM is None:
    FIELDNAM = ""
  if emsg is not None and print_info:
    _error(dsid, name, emsg)

  if VAR_NOTES == CATDESC:
    desc = f"{CATDESC}"
  elif CATDESC.strip() != "" and VAR_NOTES.strip() == "":
    desc = f"{CATDESC}"
  elif VAR_NOTES.strip() != "" and CATDESC.strip() == "":
    desc = f"{CATDESC}"
  elif CATDESC.strip() != "" and VAR_NOTES.strip() != "":
    desc = CATDESC
    xdesc = f"CATDESC: {CATDESC}; VAR_NOTES: {VAR_NOTES}"

  if cdawmeta.CONFIG['hapi']['strip_description']:
    desc = desc.strip()

  if cdawmeta.CONFIG['hapi']['remove_arrows']:
    desc = desc.replace('--->', '')

  return desc

def _sample_start_stop(metadatum):

  cdawmeta.util.print_dict(metadatum)
  if "orig_data" not in metadatum:
    logger.info("No orig_data for " + metadatum['id'])
    return None
  if "data-cache" not in metadatum['orig_data']:
    logger.info("No orig_data['data'] for " + metadatum['id'])
    return None

  orig_data = metadatum["orig_data"]['data-cache']

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

def _pointers(dsid, name, all_variables, print_info=False):

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
            msg = f"Error: ISTP[BadReference]: Bad {prefix} reference: '{name}' has {prefix}_{x} "
            msg += f"named '{x_NAME}', which is not a variable."
            msg += f"     {msg}"
            _error(dsid, name, msg)
        elif prefix == 'LABL_PTR' or (prefix == 'DEPEND' and 'string' == cdf2hapitype(all_variables[x_NAME]['VarDescription']['DataType'])):
          if 'VarData' in all_variables[x_NAME]:
            ptrs[prefix+"_VALID"][x-1] = True
            ptrs[prefix][x-1] = x_NAME
            values = cdawmeta.util.trim(all_variables[x_NAME]['VarData'])
            ptrs[prefix+"_VALUES"][x-1] = values
            if print_info:
              logger.info(f"     {prefix}_{x}: {x_NAME}")
              logger.info(f"     {prefix}_{x} trimmed values: {values}")
          else:
            ptrs[prefix+"_VALID"][x-1] = False
            if print_info:
              if prefix == 'LABL_PTR':
                msg = f"Error: ISTP[Pointer]: {x_NAME} has no VarData"
              else:
                msg = f"Error: ISTP[Pointer]: {x_NAME} is a string type but has no VarData"
              msg += f"     {msg}"
              _error(dsid, name, msg)
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
      if False and print_info:
        s = ""
        if n_valid > 1:
          s = "s"
        msg = f"Error: ISTP: '{name}' has {n_invalid} invalid element{s}."
        if print_info:
          _error(dsid, name, f"     {msg}")
    elif prefix != 'COMPONENT':
      if n_valid != len(DimSizes):
        ptrs[prefix] = None
        if False and print_info:
          # Not necessarily an error for DEPEND. Depends on DISPLAY_TYPE.
          # https://spdf.gsfc.nasa.gov/istp_guide/vattributes.html
          msg = f"Error: ISTP: '{name}' has {n_valid} valid elements {prefix}_{{1,2,3}}, but need "
          msg += f"len(DimSizes) = {len(DimSizes)}."
          if print_info:
            _error(dsid, name, f"     {msg}")
      if n_found != 0 and n_found != len(DimSizes):
        ptrs[prefix] = None
        if False and print_info:
          # Not necessarily an error for DEPEND. Depends on DISPLAY_TYPE.
          # https://spdf.gsfc.nasa.gov/istp_guide/vattributes.html
          msg = f"Error: ISTP: Wrong number of {prefix}s: '{name}' has {n_found} of "
          msg += f"{prefix}_{{1,2,3}} and len(DimSizes) = {len(DimSizes)}."
          if print_info:
            _error(dsid, name, f"     {msg}")

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

def _error(id, name, msg):
  if not id in _error.errors:
    _error.errors[id] = {}
  if name is None:
    _error.errors[id] = msg.lstrip()
  else:
    if not name in _error.errors[id]:
      _error.errors[id][name] = []
    _error.errors[id][name].append(msg.lstrip())
_error.errors = {}

def _write_errors():
  # Write all errors to a single file if all datasets were requested. Errors
  # were already written to log file, but here we need to do additional formatting
  # that is more difficult if errors were written as they occur.
  errors = ""
  for dsid, vars in _error.errors.items():
    if type(vars) == str:
      errors += f"{dsid}: {vars}\n"
      continue
    errors += f"{dsid}:\n"
    for vid, msgs in vars.items():
      errors += f"  {vid}:\n"
      for msg in msgs:
        errors += f"    {msg}\n"
  cdawmeta.util.write(os.path.join(cdawmeta.DATA_DIR, 'hapi', 'cdaweb2hapi.errors.log'), errors, logger=logger)

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
