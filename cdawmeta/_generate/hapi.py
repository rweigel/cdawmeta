import re

import datetime
import timedelta_isoformat

import cdawmeta

logger = None

dependencies = ['master', 'cadence', 'sample_start_stop']

def hapi(metadatum, _logger):
  global logger
  logger = _logger

  id = metadatum['id']

  if 'data' not in metadatum['master']:
    msg = f"{id}: Not creating dataset for {id} b/c it has no 'data' key"
    cdawmeta.error('hapi', id, None, msg, logger)
    return None

  master = metadatum["master"]['data']

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
      logger.info(f"    Not creating dataset for {id} with variable having DEPEND_0 = '{depend_0_name}'")
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
    if parameters is None:
      vars_split[depend_0_name] = None
      if len(depend_0s) == 1:
        logger.info(f"    Due to last error, omitting dataset with DEPEND_0 = {depend_0_name}")
      else:
        logger.info(f"    Due to last error, omitting sub-dataset with DEPEND_0 = {depend_0_name}")
      continue

    depend_0_names.append(depend_0_name)

  #if len(depend_0_names) == 0:
  #  msg = f"  No datasets could be created for {id}"
  #  cdawmeta.error('hapi', id, None, msg, logger)
  #  return None

  depend_0_names = _order_depend0s(id, depend_0_names)

  catalog = []
  for depend_0_name in depend_0_names:

    logger.info(f"  Creating HAPI dataset for DEPEND_0 = '{depend_0_name}'")

    info_head = _info_head(metadatum, depend_0_name)

    dataset_new = {
      'id': None,
      'description': None,
      'info': info_head
    }

    if metadatum['allxml'].get('description') and metadatum['allxml']['description'].get('@short'):
      dataset_new['description'] = metadatum['allxml']['description'].get('@short')
    else:
      del dataset_new['description']

    depend_0_variables = vars_split[depend_0_name]

    subset = ''
    if len(depend_0_names) > 1:
      subset = '@' + str(n)
    dataset_new['id'] = id + subset

    depend_0_variables = _order_variables(dataset_new['id'], depend_0_variables)

    parameters = _variables2parameters(depend_0_name, depend_0_variables, variables, id, print_info=True)

    dataset_new['info']['parameters'] = parameters

    catalog.append(dataset_new)
    n = n + 1

  return catalog

def _info_head(metadatum, depend_0_name):

  id = metadatum['id']
  allxml = metadatum['allxml']

  startDate = allxml['@timerange_start'].replace(' ', 'T') + 'Z'
  stopDate = allxml['@timerange_stop'].replace(' ', 'T') + 'Z'

  contact = ''
  if 'data_producer' in allxml:
    if '@name' in allxml['data_producer']:
      contact = allxml['data_producer']['@name']
    if '@affiliation' in allxml['data_producer']:
      contact = contact + " @ " + allxml['data_producer']['@affiliation']

  info = {
      'startDate': startDate,
      'stopDate': stopDate,
      'sampleStartDate': None,
      'sampleStopDate': None,
      'cadence': None,
      'x_cadence_fraction': None,
      'x_cadence_note': None,
      'maxRequestDuration': 'P10D',
      'resourceURL': f'https://cdaweb.gsfc.nasa.gov/misc/Notes{id[0]}.html#{id}',
      'contact': contact
  }

  if 'sample_start_stop' in metadatum:
    sample_start_stop = metadatum['sample_start_stop']['data']
    info['sampleStartDate'] = sample_start_stop['sampleStartDate']
    info['sampleStopDate'] = sample_start_stop['sampleStopDate']
  else:
    logger.warn(f"  Warning: No sample_start_stop for {id}")
    info['sampleStartDate']
    info['sampleStopDate']

  no_cadence_msg = None
  if 'cadence' in metadatum:
    if 'error' in metadatum['cadence']:
      #no_cadence_msg = f"  Error: {id}: No cadence information available due to: {metadatum['cadence']['error']}"
      no_cadence_msg = "  Error: No cadence information available"
    else:
      counts = cdawmeta.util.get_path(metadatum, ['cadence', 'data', depend_0_name, 'counts'])
      note = cdawmeta.util.get_path(metadatum, ['cadence', 'data', depend_0_name, 'note'])
      if counts is not None:
        cadence = counts[0]['duration_iso8601']
        fraction = counts[0]['fraction']
        info['cadence'] = cadence
        info['x_cadence_fraction'] = fraction
        info['x_cadence_note'] = note
      else:
        no_cadence_msg = f"  Error: {id}: No cadence information available due to no counts object."

  if no_cadence_msg is not None:
    cdawmeta.error('hapi', id, None, no_cadence_msg, logger)
    del info['cadence']
    del info['x_cadence_fraction']
    del info['x_cadence_note']

  # sample{Start,Stop}Date is based on time range of 1 file
  # If sample{Start,Stop}Date available max duration is span of n_files files
  n_files = 50
  # If sample{Start,Stop}Date not available max duration is 1000*cadence
  n_cadence = 1000
  if 'sampleStartDate' in info and 'sampleStopDate' in info:
    try:
      stop = datetime.datetime.fromisoformat(info['sampleStopDate'][0:-1])
      start = datetime.datetime.fromisoformat(info['sampleStartDate'][0:-1])
      delta = stop - start
      td = timedelta_isoformat.timedelta(milliseconds=n_files*1000*delta.total_seconds())
      info['maxRequestDuration'] = td.isoformat()
      logger.info(f"  maxRequestDuration = {td.isoformat()} (based on sample{{Start,Stop}}Date)")
    except Exception as e:
      cdawmeta.error('hapi', id, None, "  Calculation of maxRequestDuration from sample{{Start,Stop}}Date failed.", logger)
  elif 'cadence' in info:
    counts = cdawmeta.util.get_path(metadatum, ['cadence', 'data', depend_0_name, 'counts'])
    if counts is not None:
      try:
        td = timedelta_isoformat.timedelta(milliseconds=n_cadence*counts[0]['duration_ms'])
        info['maxRequestDuration'] = td.isoformat()
        logger.info(f"  maxRequestDuration = {td.isoformat()} (based on cadence)")
      except Exception as e:
        cdawmeta.error('hapi', id, None, "  Calculation of maxRequestDuration from cadence failed.", logger)

  return info

def _variables2parameters(depend_0_name, depend_0_variables, all_variables, dsid, print_info=False):

  depend_0_variable = all_variables[depend_0_name]

  if 'DataType' not in depend_0_variable['VarDescription']:
    msg = f"Error: CDF[MissingDataType]: DEPEND_0 variable '{dsid}'/{depend_0_name} has no DataType attribute. Dropping variables associated with it"
    cdawmeta.error('hapi', dsid, depend_0_name, "    " + msg, logger)
    return None

  DEPEND_0_DataType = depend_0_variable['VarDescription']['DataType']
  DEPEND_0_length = _cdftimelen(DEPEND_0_DataType)

  if DEPEND_0_length is None:
    msg = f"Error: DEPEND_0 variable '{dsid}'/{depend_0_name} has unhandled type: '{DEPEND_0_DataType}'. "
    msg += "Dropping variables associated with it"
    logger.info("    " + msg)
    return None

  x_description = _description(dsid, depend_0_name, all_variables[depend_0_name], x=None, print_info=print_info)
  parameters = [
                  {
                    'name': 'Time',
                    'type': 'isotime',
                    'units': 'UTC',
                    'length': DEPEND_0_length,
                    'fill': None,
                    'x_description': x_description,
                    'x_cdf_NAME': depend_0_name,
                    'x_cdf_DataType': DEPEND_0_DataType,
                  }
                ]

  for name, variable in depend_0_variables.items():

    VAR_TYPE, emsg = cdawmeta.attrib.VAR_TYPE(dsid, name, variable, x=None)
    if emsg is not None:
      # Should not happen because variable will be dropped in _split_variables
      cdawmeta.error('hapi', dsid, name, "    " + emsg, logger)
      continue

    if VAR_TYPE != 'data':
      continue

    virtual = 'VIRTUAL' in variable['VarAttributes']
    if print_info:
      virtual_txt = f' (virtual: {virtual})'
      logger.info(f"    {name}{virtual_txt}")

    type = CDFDataType2HAPItype(variable['VarDescription']['DataType'])
    if type is None and print_info:
      msg = f"Error: HAPI[NotImplemented]: Variable '{name}' has unhandled DataType: {variable['VarDescription']['DataType']}. Dropping variable."
      cdawmeta.error('hapi', dsid, name, "      " + msg, logger)
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
        emsg = "Error: CDF[MissingInfo]: Dropping '{name}' because CDFDataType2HAPItype(VAR_TYPE) returns string but no PadValue, FillValue, or NumElements given to allow length to be determined."
        cdawmeta.error('hapi', dsid, name, "      " + emsg, logger)
        continue

      if NumElements is None:
        if PadValue is not None and FillValue is not None and PadValue != FillValue:
          emsg = f"Error: CDF[LengthMismatch]: Dropping '{name}' because PadValue and FillValue lengths differ."
          cdawmeta.error('hapi', dsid, name, "      " + emsg, logger)
          continue

      if PadValue is not None:
        length = len(PadValue)
      if FillValue is not None:
        length = len(FillValue)
      if NumElements is not None:
        length = int(NumElements)

    parameter = {
      "name": name,
      "type": type,
      "x_cdf_DataType": variable['VarDescription']['DataType']
    }

    parameter['description'] = _description(dsid, name, variable, print_info=print_info)

    fill = None
    if 'FILLVAL' in variable['VarAttributes']:
      fill = str(variable['VarAttributes']['FILLVAL'])
    if fill is not None:
      parameter['fill'] = fill

    UNITS, emsg = cdawmeta.attrib.UNITS(dsid, name, all_variables, x=None)
    parameter['units'] = UNITS
    if emsg is not None and print_info:
      cdawmeta.error('hapi', dsid, name, "      " + emsg, logger)

    LABLAXIS, emsg = cdawmeta.attrib.LABLAXIS(dsid, name, all_variables, x=None)
    if LABLAXIS is not None and cdawmeta.CONFIG['hapi']['keep_label']:
      parameter['label'] = LABLAXIS
    else:
      parameter['x_label'] = LABLAXIS

    format_f, emsg = cdawmeta.attrib.FORMAT(dsid, name, all_variables, c_specifier=False)
    if format_f is not None:
      parameter['x_cdf_FORMAT'] = format_f
    parameter['x_cdf_FORMAT'] = format_f
    if format_f is not None:
      format_c, emsg = cdawmeta.attrib.FORMAT(dsid, name, all_variables, c_specifier=True)
      if format_c is not None:
        parameter['x_format'] = format_c
        parameter['x_fractionDigits'] = ''.join(d for d in format_c if d.isdigit())
      if emsg is not None and print_info:
        cdawmeta.error('hapi', dsid, name, "    " + emsg, logger)

    if 'DataType' in variable['VarDescription']:
      parameter['x_cdf_DataType'] = variable['VarDescription']['DataType']

    FIELDNAM, emsg = cdawmeta.attrib.FIELDNAM(dsid, name, variable)
    if FIELDNAM is not None:
      parameter['x_cdf_FIELDNAM'] = FIELDNAM

    parameter["x_cdf_VIRTUAL"] = virtual
    DISPLAY_TYPE, emsg = cdawmeta.attrib.DISPLAY_TYPE(dsid, name, variable)
    if DISPLAY_TYPE is not None:
      parameter['x_cdf_DISPLAY_TYPE'] = DISPLAY_TYPE
    if emsg is not None and print_info:
      if cdawmeta.CONFIG['hapi']['log_display_type_issues']:
        cdawmeta.error('hapi', dsid, name, "    " + emsg, logger)

    # TODO: Finish.
    #deltas = _extract_deltas(dsid, name, variable, print_info=cdawmeta.CONFIG['hapi']['log_delta_issues'])
    #parameter = {**parameter, **deltas}

    if length is not None:
      parameter['length'] = length

    if 'DEPEND_0' in variable['VarAttributes']:
      parameter['x_cdf_DEPEND_0'] = variable['VarAttributes']['DEPEND_0']

    if 'DimSizes' in variable['VarDescription']:
      parameter['size'] = variable['VarDescription']['DimSizes']

    if 'DataType' not in variable['VarDescription']:
      msg = f"Error: CDF[MissingDataType]: Variable '{dsid}'/{depend_0_name} has no DataType attribute. Dropping variables associated with it"
      cdawmeta.error('hapi', dsid, name, "      " + msg, logger)
      return None

    ptr_names = ['DEPEND','LABL_PTR']
    ptrs = cdawmeta.attrib._resolve_ptrs(dsid, name, all_variables, ptr_names=ptr_names)
    for ptr_name in ptr_names:
      if ptrs[ptr_name] is not None and ptrs[ptr_name] is not None:
        for x in range(len(ptrs[ptr_name])):
          emsg = ptrs[ptr_name+'_ERROR'][x]
          if emsg is not None:
            cdawmeta.error('hapi', dsid, name, f'      {emsg}', logger)

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
              msg = f'      Warning: NotImplemented[RedundantDependValues]: DEPEND_{x} has is string type and LABL_PTR_{x} given. They differ; using LABL_PTR_{x} for HAPI label attribute.'
              logger.warning(msg)
              break

    bins_object = None
    if ptrs['DEPEND'] is not None and n_depend_values == 0:
      bins_object = _bins(dsid, name, all_variables, ptrs['DEPEND'], print_info=print_info)
      if bins_object is not None:
        parameter['bins'] = bins_object

    if print_info:
      for key, value in parameter.items():
        logger.info(f"      {key} = {value}")
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
    logger.info(f"      NumDims: {NumDims}")
    logger.info(f"      DimSizes: {DimSizes}")
    logger.info(f"      DimVariances: {DimVariances}")

  if NumDims != len(DimSizes):
    if print_info:
      msg = f"Error: CDF[DimSizes]: DimSizes mismatch: NumDims = {NumDims} "
      msg += "!= len(DimSizes) = {len(DimSizes)}"
      cdawmeta.error('hapi', dsid, name, "      " + msg, logger)
    return None

  if len(DimSizes) != len(DimVariances):
    if print_info:
      msg = f"Error: CDF[DimVariance]: DimVariances mismatch: len(DimSizes) = {DimSizes} "
      msg += "!= len(DimVariances) = {len(DimVariances)}"
      cdawmeta.error('hapi', dsid, name, "      " + msg, logger)
    return None

  bins_objects = []
  for x in range(len(depend_xs)):
    DEPEND_x_NAME = depend_xs[x]
    if DEPEND_x_NAME is not None:
      hapitype = CDFDataType2HAPItype(all_variables[DEPEND_x_NAME]['VarDescription']['DataType'])
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
      logger.info(f"      DEPEND_{x} has RecVariance = " + RecVariance)

  if RecVariance == "VARY":
    if print_info:
      logger.info(f"      Warning: NotImplemented[TimeVaryingBins]: DEPEND_{x} = {DEPEND_x_NAME} has RecVariance = 'VARY'. Not creating bins b/c Nand does not for this case.")
    return None

  _, emsg = cdawmeta.attrib.VAR_TYPE(dsid, name, DEPEND_x, x=x)
  if emsg is not None:
    if print_info:
      emsg = emsg +  " Not creating bins."
      cdawmeta.error('hapi', dsid, name, "      " + emsg, logger)
    return None

  UNITS, emsg = cdawmeta.attrib.UNITS(dsid, DEPEND_x_NAME, all_variables, x=x)
  if emsg is not None and print_info:
    cdawmeta.error('hapi', dsid, DEPEND_x_NAME, "      " + emsg, logger)

  LABLAXIS, emsg = cdawmeta.attrib.LABLAXIS(dsid, DEPEND_x_NAME, all_variables, x=x)
  if emsg is not None and print_info:
    cdawmeta.error('hapi', dsid, name, "      " + emsg, logger)

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
      logger.info(f"      Warning: HAPI: Not including bin centers for {DEPEND_x_NAME} b/c no VarData (is probably VIRTUAL)")
    return None

def _order_depend0s(id, depend0_names):

  fixes = cdawmeta.CONFIG['hapi']['fixes']

  if id not in fixes['depend0Order'].keys():
    return depend0_names

  order_wanted = fixes['depend0Order'][id]

  for depend0_name in order_wanted:
    if depend0_name not in depend0_names:
      logger.error(f'Error[HAPI]: {id}\n  DEPEND_0 {depend0_name} in new order list is not a depend0 in dataset ({depend0_names})')
      logger.error('  Exiting with code 1')
      exit(1)

  if False:
    # Eventually we will want to use this when we are not trying to match
    # Nand's metadata exactly.
    # Append depend0s not in order_wanted to the end of the list
    final = order_wanted.copy()
    for i in depend0_names:
      if i not in order_wanted:
        final.append(i)

  return order_wanted

def _order_variables(id, variables):

  fixes = cdawmeta.CONFIG['hapi']['fixes']

  if id not in fixes['variableOrder'].keys():
    return variables

  order_wanted = fixes['variableOrder'][id]
  order_given = variables.keys()
  if len(order_wanted) != len(order_wanted):
    logger.error(f'Error[HAPI]: {id}\n  Number of variables in new order list ({len(order_wanted)}) does not match number found in dataset ({len(order_given)})')
    logger.error(f'  New order:   {order_wanted}')
    logger.error(f'  Given order: {list(order_given)}')
    logger.error('  Exiting with code 1')
    exit(1)

  if sorted(order_wanted) != sorted(order_wanted):
    logger.error(f'Error[HAPI]: {id}\n  Mismatch in variable names between new order list and dataset')
    logger.error(f'  New order:   {order_wanted}')
    logger.error(f'  Given order: {list(order_given)}')
    logger.error('  Exiting with code 1')
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
      msg = f"  Error: ISTP[NoVarAttributes]: Dropping variable '{name}' b/c it has no VarAttributes"
      cdawmeta.error('hapi', id, name, msg, logger)
      continue

    if 'VAR_TYPE' not in variable_meta['VarAttributes'] and cdawmeta.CONFIG['hapi']['log_missing_var_type']:
      msg = f"  Error: ISTP[NoVAR_TYPE]: Dropping variable '{name}' b/c it has no has no VAR_TYPE"
      cdawmeta.error('hapi', id, name, msg, logger)
      continue

    if _omit_variable(id, name):
      continue

    if 'DEPEND_0' in variable_meta['VarAttributes']:
      depend_0_name = variable_meta['VarAttributes']['DEPEND_0']

      if depend_0_name not in variables:
        msg = f"  Error: CDF[MissingDEPEND_0]: Dropping '{name}' b/c it has a DEPEND_0 ('{depend_0_name}') that is not in dataset"
        cdawmeta.error('hapi', id, name, msg, logger)
        continue

      if depend_0_name not in depend_0_dict:
        depend_0_dict[depend_0_name] = {}
      depend_0_dict[depend_0_name][name] = variable_meta

  return depend_0_dict

def _keep_dataset(id, depend_0=None):
  fixes = cdawmeta.CONFIG['hapi']['fixes']
  if id in fixes['keepSubset'].keys() and depend_0 == fixes['keepSubset'][id]:
    if logger:
      logger.info(id)
      logger.info(f"  Warning: Keeping dataset associated with \"{depend_0}\" b/c it is in Nand's list")
    return True
  return False

def _omit_dataset(id, depend_0=None):

  if id is None:
    return None

  fixes = cdawmeta.CONFIG['hapi']['fixes']
  omit_datasets = cdawmeta.CONFIG['hapi']['omit_datasets']
  if depend_0 is None:
    if id in fixes['omitAll'].keys():
      if omit_datasets:
        logger.info(id)
        logger.info(f"  Warning: Dropping dataset {id} b/c it is not in Nand's list")
        return True
      else:
        logger.info(id)
        logger.info(f"  Warning: Keeping dataset {id} even though it is not in Nand's list")
        return False
    for pattern in fixes['omitAllPattern']:
      if re.search(pattern, id):
        if omit_datasets:
          logger.info(id)
          logger.info(f"  Warning: Dropping dataset {id} b/c it is not in Nand's list")
          return True
        else:
          logger.info(id)
          logger.info(f"  Warning: Keeping dataset {id} even though it is not in Nand's list")
          return False
  else:
    if id in fixes['omitSubset'].keys() and depend_0 in fixes['omitSubset'][id]:
      logger.info(f"  Warning: Dropping variables associated with DEPEND_0 = \"{depend_0}\" b/c this DEPEND_0 is not in Nand's list")
      return True
  return False

def _omit_variable(id, variable_name):
  fixes = cdawmeta.CONFIG['hapi']['fixes']

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
    cdawmeta.error('hapi', dsid, name, "      " + emsg, logger)

  VAR_NOTES, emsg = cdawmeta.attrib.VAR_NOTES(dsid, name, variable)
  if VAR_NOTES is None:
    VAR_NOTES = ""
  if emsg is not None and print_info:
    cdawmeta.error('hapi', dsid, name, "      " + emsg, logger)

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

def _cdftimelen(cdf_type):

  # Based on table at https://spdf.gsfc.nasa.gov/istp_guide/vattributes.html
  # Could also get from PadValue or FillValue, but they are not always present (!).
  if cdf_type == 'CDF_EPOCH':
    return len('0000-01-01:00:00:00.000Z')
  if cdf_type == 'CDF_TIME_TT2000':
    return len('0000-01-01:00:00:00.000000000Z')
  if cdf_type == 'CDF_EPOCH16':
    return len('0000-01-01:00:00:00.000000000000Z')

  return None

def CDFDataType2HAPItype(cdf_type):

  if cdf_type in ['CDF_CHAR', 'CDF_UCHAR']:
    return 'string'

  if cdf_type.startswith('CDF_EPOCH') or cdf_type.startswith('CDF_TIME'):
    return 'isotime'

  if cdf_type.startswith('CDF_INT') or cdf_type.startswith('CDF_UINT') or cdf_type.startswith('CDF_BYTE'):
    return 'integer'

  if cdf_type in ['CDF_FLOAT', 'CDF_DOUBLE', 'CDF_REAL4', 'CDF_REAL8']:
    return 'double'

  return None

# Used here and in soso.py
def flatten_parameters(hapi):

  if isinstance(hapi, list):
    parameters = {}
    for _, ds in enumerate(hapi):
      return [*parameters, *ds['info']['parameters']]

  return hapi['info']['parameters']
