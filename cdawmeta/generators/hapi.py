import re
import datetime

import timedelta_isoformat

import cdawmeta

logger = None

#dependencies = ['master', 'cadence', 'start_stop']
dependencies = ['master_resolved', 'cadence', 'start_stop']

def hapi(metadatum, _logger):
  global logger
  logger = _logger

  id = metadatum['id']

  master = metadatum['master_resolved'].get('data', None)

  if master is None:
    emsg = f"{id}: Not creating dataset for {id} b/c it has no 'data' key (or data=None) in master_resolved"
    cdawmeta.error('hapi', id, None, "ISTP.NoMaster", emsg, logger)
    return {"error": emsg}

  master = metadatum['master_resolved']['data']

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

    start_stop = metadatum['start_stop'].get('data', None)
    if start_stop is None:
      cdawmeta.error('hapi', id, None, "CDAWeb.NoStartStop", "No start/stop info. Omitting dataset.", logger)
      continue

    if 'startDate' not in start_stop:
      cdawmeta.error('hapi', id, None, "CDAWeb.NoStartDate", "No startDate info. Omitting dataset.", logger)
      continue
    if 'stopDate' not in start_stop:
      cdawmeta.error('hapi', id, None, "CDAWeb.NoStopDate", "No stopDate info. Omitting dataset.", logger)
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

  if len(depend_0_names) == 0:
    msg = f"{id}: No datasets could be created due errors in source metadata."
    cdawmeta.error('hapi', id, None, "CDF.NoDatasets", "  " + msg, logger)
    return {"error": msg}

  # Second pass
  depend_0_names = _order_depend0s(id, depend_0_names)

  catalog = []
  for depend_0_name in depend_0_names:

    logger.info(f"  Computing header for HAPI dataset for DEPEND_0 = '{depend_0_name}'")

    info_head = _info_head(metadatum, depend_0_name)

    logger.info("  Computing parameters")

    dataset_new = {
      'id': None,
      'title': None,
      'info': info_head
    }

    if metadatum['allxml'].get('description') and metadatum['allxml']['description'].get('@short'):
      dataset_new['title'] = metadatum['allxml']['description'].get('@short')
    else:
      del dataset_new['title']

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

  contact = ''
  if 'data_producer' in allxml:
    if '@name' in allxml['data_producer']:
      contact = allxml['data_producer']['@name']
    if '@affiliation' in allxml['data_producer']:
      contact = contact + " @ " + allxml['data_producer']['@affiliation']

  info = {
      'startDate': None,
      'stopDate': None,
      'sampleStartDate': None,
      'sampleStopDate': None,
      'cadence': None,
      'x_cadence_fraction': None,
      'x_cadence_note': None,
      'maxRequestDuration': None,
      'resourceURL': f'https://cdaweb.gsfc.nasa.gov/misc/Notes{id[0]}.html#{id}',
      'contact': contact
  }

  cadence_info, emsg = _cadence(id, depend_0_name, metadatum)

  if emsg is not None:
    cdawmeta.error('hapi', id, None, "HAPI.NoCadence", "    " + emsg, logger)
    del info['cadence']
    del info['x_cadence_fraction']
    del info['x_cadence_note']
  else:
    info.update(cadence_info)

  start_stop = metadatum['start_stop'].get('data', None)
  if start_stop is not None:
    start_stop = metadatum['start_stop']['data']
    info['startDate'] = start_stop['startDate']
    info['stopDate'] = start_stop['stopDate']
    if 'sampleStartDate' in start_stop:
      info['sampleStartDate'] = start_stop['sampleStartDate']
      info['sampleStopDate'] = start_stop['sampleStopDate']
  else:
    logger.warn(f"    No start_stop for {id}")
    del info['sampleStartDate']
    del info['sampleStopDate']

  maxRequestDuration, emsg = _max_request_duration(depend_0_name, metadatum, info)
  if emsg is not None:
    logger.info(f"    Using default maxRequestDuration = {info['maxRequestDuration']}")
    info["maxRequestDuration"] = cdawmeta.CONFIG['hapi']['maxRequestDurationDefault']
  if maxRequestDuration is not None:
    info["maxRequestDuration"] = maxRequestDuration

  return info

def _variables2parameters(depend_0_name, depend_0_variables, all_variables, dsid, print_info=False):

  depend_0_variable = all_variables[depend_0_name]

  if 'DataType' not in depend_0_variable['VarDescription']:
    msg = f"DEPEND_0 variable '{dsid}'/{depend_0_name} has no DataType attribute. Dropping variables associated with it"
    cdawmeta.error('hapi', dsid, depend_0_name, "CDF.MissingDataType.", "    " + msg, logger)
    return None

  DEPEND_0_DataType = depend_0_variable['VarDescription']['DataType']
  DEPEND_0_length = _cdftimelen(DEPEND_0_DataType)

  if DEPEND_0_length is None:
    msg = f"Error: DEPEND_0 variable '{dsid}'/{depend_0_name} has unhandled type: '{DEPEND_0_DataType}'. "
    msg += "Dropping variables associated with it"
    cdawmeta.error('hapi', dsid, depend_0_name, "HAPI.NotImplementedDataType.", "    " + msg, logger)
    return None

  if print_info:
    logger.info(f"    {depend_0_name} (variable associated with HAPI Time parameter)")

  x_description = _description(dsid, depend_0_name, all_variables[depend_0_name],
                               x=None, print_info=print_info)

  if print_info:
    logger.info(f"      x_description: {x_description}")
    logger.info(f"      x_cdf_DataType: {DEPEND_0_DataType}")

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

    VAR_TYPE, emsg, etype = cdawmeta.attrib.VAR_TYPE(dsid, name, variable, x=None)
    if etype is not None:
      # Should not happen because variable will be dropped in _split_variables
      cdawmeta.error('hapi', dsid, name, etype, "    " + emsg, logger)
      continue

    if VAR_TYPE != 'data':
      continue

    type_ = _to_hapi_type(variable['VarDescription']['DataType'])
    if type_ is None and print_info:
      msg = f"Variable '{name}' has unhandled DataType: {variable['VarDescription']['DataType']}. Dropping variable."
      cdawmeta.error('hapi', dsid, name, "HAPI.NotImplemented", "      " + msg, logger)
      continue

    length = None
    if VAR_TYPE == 'data' and type_ == 'string':

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
        emsg = "Dropping '{name}' because _to_hapi_type(VAR_TYPE) returns string but no PadValue, FillValue, or NumElements given to allow length to be determined."
        cdawmeta.error('hapi', dsid, name, "CDF.MissingMetadata", "      " + emsg, logger)
        continue

      if NumElements is None:
        if PadValue is not None and FillValue is not None and PadValue != FillValue:
          emsg = f"Dropping '{name}' because PadValue and FillValue lengths differ."
          cdawmeta.error('hapi', dsid, name, "CDF.LengthMismatch", "      " + emsg, logger)
          continue

      if PadValue is not None:
        length = len(PadValue)
      if FillValue is not None:
        length = len(FillValue)
      if NumElements is not None:
        length = int(NumElements)

    parameter = {
      "name": name,
      "type": type_,
      "x_cdf_DataType": variable['VarDescription']['DataType']
    }

    parameter['description'] = _description(dsid, name, variable, print_info=print_info)

    fill = None
    if 'FILLVAL' in variable['VarAttributes']:
      fill = variable['VarAttributes']['FILLVAL']
    else:
      emsg = f"No FILLVAL for '{name}'"
      cdawmeta.error('hapi', dsid, name, "CDF.MissingFillValue", "      " + emsg, logger)
      # Could use _default_fill()

    if fill is not None:
      if isinstance(fill, str) and fill.lower() != 'nan':
        if not type_ == 'integer' or type_ == 'double':
          emsg = f"FILLVAL = '{fill}' is string but data type is not integer or double. Setting fill to None."
          cdawmeta.error('hapi', dsid, name, "CDF.WrongFillType", "      " + emsg, logger)
          # TODO: Drop this variable?
          fill = None
    if fill is not None:
      fill = str(fill)
    parameter['fill'] = fill

    parameter = {**parameter, **_units(variable)}
    parameter = {**parameter, **_labels(variable)}

    format_f, emsg, etype = cdawmeta.attrib.FORMAT(dsid, name, all_variables, c_specifier=False)
    if etype is not None and print_info:
      cdawmeta.error('hapi', dsid, name, etype, "    " + emsg, logger)
    if format_f is not None:
      format_c, emsg, etype = cdawmeta.attrib.FORMAT(dsid, name, all_variables, c_specifier=True)
      if etype is not None and print_info:
        cdawmeta.error('hapi', dsid, name, etype, "    " + emsg, logger)
      if format_c is not None:
        parameter['x_format'] = format_c
        parameter['x_fractionDigits'] = ''.join(d for d in format_c if d.isdigit())
      parameter['x_cdf_FORMAT'] = format_f

    if 'DataType' in variable['VarDescription']:
      parameter['x_cdf_DataType'] = variable['VarDescription']['DataType']

    FIELDNAM, emsg, etype = cdawmeta.attrib.FIELDNAM(dsid, name, variable)
    if etype is not None and print_info:
      cdawmeta.error('hapi', dsid, name, etype, "    " + emsg, logger)
    if FIELDNAM is not None:
      parameter['x_cdf_FIELDNAM'] = FIELDNAM

    virtual = 'VIRTUAL' in variable['VarAttributes']
    if print_info:
      virtual_txt = f' (virtual: {virtual})'
      logger.info(f"    {name}{virtual_txt}")

    parameter["x_cdf_VIRTUAL"] = virtual
    if virtual:
      parameter["x_cdf_FUNCT"] = variable['VarAttributes']['FUNCT']
      parameter["x_cdf_COMPONENTS"] = variable['VarAttributes']['COMPONENTS']
      parameter['description'] += f". This variable is a 'virtual' variable that is computed using the function {parameter['x_cdf_FUNCT']} (see https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/source/virtual_funcs.pro) on the with inputs of the variables {parameter['x_cdf_COMPONENTS']}."
      parameter['description'] += " Note that not all COMPONENTS are time series and so their values are not available from the HAPI interface. They are accessible from the raw CDF files, however."

    DISPLAY_TYPE, emsg, etype = cdawmeta.attrib.DISPLAY_TYPE(dsid, name, variable)
    if etype is not None and print_info:
      cdawmeta.error('hapi', dsid, name, etype, "      " + emsg, logger)
    if DISPLAY_TYPE is not None:
      parameter['x_cdf_DISPLAY_TYPE'] = DISPLAY_TYPE

    # TODO: Finish.
    #deltas = _extract_deltas(dsid, name, variable, print_info=print_info)
    #parameter = {**parameter, **deltas}

    if length is not None:
      parameter['length'] = length

    if 'DEPEND_0' in variable['VarAttributes']:
      parameter['x_cdf_DEPEND_0'] = variable['VarAttributes']['DEPEND_0']

    if 'DimSizes' in variable['VarDescription']:
      parameter['size'] = variable['VarDescription']['DimSizes']

    if 'DataType' not in variable['VarDescription']:
      msg = f"Variable '{dsid}'/{depend_0_name} has no DataType attribute. Dropping variables associated with it"
      cdawmeta.error('hapi', dsid, name, "CDF.MissingDataType", "      " + msg, logger)
      return None

    ptr_names = ['DEPEND','LABL_PTR']
    ptrs = cdawmeta.attrib._resolve_ptrs(dsid, name, all_variables, ptr_names=ptr_names)
    for ptr_name in ptr_names:
      if ptrs[ptr_name] is not None and ptrs[ptr_name] is not None:
        for x in range(len(ptrs[ptr_name])):
          emsg = ptrs[ptr_name+'_ERROR'][x]
          if emsg is not None:
            cdawmeta.error('hapi', dsid, name, "CDF.PTR", f'      {emsg}', logger)

    n_depend_values = 0
    if ptrs['DEPEND_VALUES'] is not None:
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
              msg = f'      NotImplemented[RedundantDependValues]: DEPEND_{x} is string type and LABL_PTR_{x} given. They differ; using LABL_PTR_{x} for HAPI label attribute.'
              logger.warn(msg)
              break

    bins_object = None
    if ptrs['DEPEND'] is not None and n_depend_values == 0:
      bins_object = _bins(dsid, name, all_variables, ptrs['DEPEND'], print_info=print_info)
      if bins_object is not None:
        parameter['bins'] = bins_object

    if print_info:
      for key, value in parameter.items():
        if key != 'bins':
          logger.info(f"      {key} = {value}")
      if bins_object is not None:
        for idx, bin in enumerate(bins_object):
          logger.info(f"      bins[{idx}]")
          for bin_key, bin_value in bin.items():
            if bin_key != 'centers':
              logger.info(f"        {bin_key} = {bin_value}")
            else:
              centers = bin['centers']
              if len(bin['centers']) > 10:
                tmp = f'[{centers[0]}, ..., {centers[-1]}]'
                logger.info(f"        {bin_key} = {tmp}")

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
      msg = f"DimSizes mismatch: NumDims = {NumDims} "
      msg += "!= len(DimSizes) = {len(DimSizes)}"
      cdawmeta.error('hapi', dsid, name, "CDF.DimSizes", "      " + msg, logger)
    return None

  if len(DimSizes) != len(DimVariances):
    if print_info:
      msg = f"DimVariances mismatch: len(DimSizes) = {DimSizes} "
      msg += "!= len(DimVariances) = {len(DimVariances)}"
      cdawmeta.error('hapi', dsid, name, "CDF.DimVariance", "      " + msg, logger)
    return None

  bins_objects = []
  for x in range(len(depend_xs)):
    DEPEND_x_NAME = depend_xs[x]
    if DEPEND_x_NAME is not None:
      hapitype = _to_hapi_type(all_variables[DEPEND_x_NAME]['VarDescription']['DataType'])
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
      logger.warn(f"      NotImplemented[TimeVaryingBins]: DEPEND_{x} = {DEPEND_x_NAME} has RecVariance = 'VARY'. Not creating bins b/c Nand does not for this case.")
    return None

  _, emsg, etype = cdawmeta.attrib.VAR_TYPE(dsid, name, DEPEND_x, x=x)
  if etype is not None:
    if print_info:
      emsg = emsg +  " Not creating bins."
      cdawmeta.error('hapi', dsid, name, etype, "      " + emsg, logger)
    return None

  if 'VarData' in DEPEND_x:
    bins_object = {
                    "name": DEPEND_x_NAME,
                    "description": _description(dsid, name, DEPEND_x, x=x, print_info=print_info),
                    "centers": DEPEND_x["VarData"],
                    **_labels(DEPEND_x),
                    **_units(DEPEND_x)
                  }
    return bins_object
  else:
    if print_info:
      msg = f"HAPI: Not including bin centers for {DEPEND_x_NAME}"
      msg += " b/c no VarData (is probably VIRTUAL)"
      logger.warn(f"      {msg}")
    return None

def _labels(variable):

  LABLAXIS = variable['VarAttributes'].get('x_LABLAXIS', None)
  LABLAXES = variable['VarAttributes'].get('x_LABLAXES', None)
  labels = {}
  if LABLAXIS is not None:
    if cdawmeta.CONFIG['hapi']['keep_label']:
      labels['label'] = LABLAXIS
    else:
      labels['x_label'] = LABLAXIS
  if LABLAXES is not None:
    if LABLAXIS is None:
      if len(LABLAXES) == 1:
        LABLAXES = LABLAXES[0]
      if cdawmeta.CONFIG['hapi']['keep_label']:
        labels['label'] = LABLAXES
      else:
        labels['x_label'] = LABLAXES
    else:
      labels['x_labelComponents'] = LABLAXES
      if isinstance(LABLAXIS, str) and len(set(LABLAXES[0])) == 1:
        if LABLAXES[0][0] == LABLAXIS:
          # Catches case where (i.e., C1_CP_STA_PPP)
          # LABLAXIS = "F"
          # LABLAXES = [["F", "F", ...]]
          # => remove labelComponents b/c redundant
          # TODO: Does not handle
          # labels = ["F", "G"],
          # labelComponents = [["F", "F", ...], ["G", "G", ...]]
          del labels['x_labelComponents']

  return labels

def _units(variable):
  UNITS = variable['VarAttributes'].get('x_UNITS', None)
  UNITS_VO = variable['VarAttributes'].get('x_UNITS_VO', None)
  units = {}
  if UNITS_VO is not None:
    units['units'] = UNITS_VO
    units['unitsSchema'] = 'VOUnits1.1'
    units['x_unitsOriginal'] = UNITS
  else:
    units['units'] = UNITS

  if cdawmeta.CONFIG['hapi']['strip_units']:
    if 'units' in units:
      units['units'] = cdawmeta.util.trim(units)

  if isinstance(units, list) and isinstance(units[0], str):
    # Catch case where units = ["A", "A", ...] => units = "A"
    if len(set(units)) == 1:
      units = units[0]

  return units

def _max_request_duration(depend_0_name, metadatum, info):

  # sample{Start,Stop}Date is based on time range of 1 file
  # If sample{Start,Stop}Date available max duration is span of n_files files
  n_files = 50
  # If sample{Start,Stop}Date not available max duration is 1000*cadence
  n_cadence = 1000

  if 'sampleStartDate' in info and 'sampleStopDate' in info:
    logger.info("    Calculating maxRequestDuration from sample{Start,Stop}Date")
    try:
      start = datetime.datetime.fromisoformat(info['startDate'][0:-1])
      stop = datetime.datetime.fromisoformat(info['stopDate'][0:-1])
      sampleStart = datetime.datetime.fromisoformat(info['sampleStartDate'][0:-1])
      sampleStop = datetime.datetime.fromisoformat(info['sampleStopDate'][0:-1])

      logger.info(f"    startDate       = {info['startDate']}")
      logger.info(f"    stopDate        = {info['stopDate']}")
      logger.info(f"    sampleStartDate = {info['sampleStartDate']}")
      logger.info(f"    sampleStopDate  = {info['sampleStopDate']}")

      delta_sample = sampleStop - sampleStart
      delta_datset = stop - start

      total_seconds_sample = delta_sample.total_seconds()
      total_seconds_datset = delta_datset.total_seconds()
      logger.info(f"    sample length  = {total_seconds_sample} [s]")
      logger.info(f"    dataset length = {total_seconds_datset} [s]")

      total_seconds = min(total_seconds_datset, n_files*total_seconds_sample)
      logger.info(f"    min(dataset, {n_files}*sample) = {total_seconds} [s]")

      td = timedelta_isoformat.timedelta(milliseconds=1000*total_seconds)
      logger.info(f"    maxRequestDuration = {td.isoformat()}")
      maxRequestDuration = td.isoformat()
      logger.info(f"    maxRequestDuration = {td.isoformat()} (based on sample{{Start,Stop}}Date)")

      return maxRequestDuration, None

    except Exception as e:
      msg = f"Calculation of maxRequestDuration from sample{{Start,Stop}}Date failed: {e}"
      cdawmeta.error('hapi', id, None, "HAPI.UnHandledException", "  " + msg, logger)

  if 'cadence' in info:
    counts = cdawmeta.util.get_path(metadatum, ['cadence', 'data', depend_0_name, 'counts'])
    if counts is not None:
      logger.info("    No sample{Start,Stop}Date. Calculating maxRequestDuration from cadence")
      try:
        td = timedelta_isoformat.timedelta(milliseconds=n_cadence*counts[0]['duration_ms'])
        maxRequestDuration = td.isoformat()
        logger.info(f"    maxRequestDuration = {td.isoformat()} (based on cadence)")
        return maxRequestDuration, None
      except Exception as e:
        msg = f"    Calculation of maxRequestDuration from cadence failed: {e}"
        cdawmeta.error('hapi', id, None, "HAPI.UnHandledException", "  " + msg, logger)
        return None, msg

  return None, None

def _cadence(id, depend_0_name, metadatum):

  if 'cadence' not in metadatum:
    return None, f"{id}/{depend_0_name}: No cadence information available."

  if 'cadence' in metadatum:
    if 'error' in metadatum['cadence'] or 'data' not in metadatum['cadence']:
      return None, f"{id}/{depend_0_name}: No cadence information available."
    else:
      cadence_dict = cdawmeta.util.get_path(metadatum, ['cadence', 'data'])
      if 'error' in cadence_dict:
        return None, f"{id}/{depend_0_name}: No cadence information available due to {cadence_dict['error']}."

      depend_0_cadence_dict = cdawmeta.util.get_path(cadence_dict, [depend_0_name])
      if depend_0_cadence_dict is None:
        return None, f"{id}/{depend_0_name}: No cadence information available."

      if 'error' in depend_0_cadence_dict:
        return None, f"{id}/{depend_0_name}: No cadence information available due to {depend_0_cadence_dict['error']}."

      counts = cdawmeta.util.get_path(metadatum, ['cadence', 'data', depend_0_name, 'counts'])
      if counts is not None and len(counts) > 0:
        note = cdawmeta.util.get_path(metadatum, ['cadence', 'data', depend_0_name, 'note'])
        cadence = counts[0]['duration_iso8601']
        fraction = counts[0]['fraction']
        cadence_info = {'cadence': cadence, 'x_cadence_fraction': fraction, 'x_cadence_note': note}
        return cadence_info, None
      else:
        return None, f"{id}/{depend_0_name}: No cadence information available due to unspecified error."

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
      msg = f"Dropping variable '{name}' b/c it has no VarAttributes"
      cdawmeta.error('hapi', id, name, "ISTP.NoVarAttributes", "  " + msg, logger)
      continue

    if 'VAR_TYPE' not in variable_meta['VarAttributes']:
      msg = f"Dropping variable {id}/{name} b/c it has no has no VAR_TYPE"
      cdawmeta.error('hapi', id, name, "ISTP.VAR_TYPE", "  " + msg, logger)
      continue

    if _omit_variable(id, name):
      continue

    if 'DEPEND_0' in variable_meta['VarAttributes']:
      depend_0_name = variable_meta['VarAttributes']['DEPEND_0']

      if depend_0_name not in variables:
        msg = f"Dropping {id}/{name} b/c it has a DEPEND_0 ('{depend_0_name}') that is not in dataset"
        cdawmeta.error('hapi', id, name, "CDF.MissingDEPEND_0", "  " + msg, logger)
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
    if id in fixes['omitDataset'].keys():
      if omit_datasets:
        logger.info(id)
        logger.warn(f"  Dropping dataset {id} b/c it is not in Nand's list")
        return True
      else:
        logger.info(id)
        logger.info(f"  Keeping dataset {id} even though it is not in Nand's list")
        return False
    for pattern in fixes['omitDatasetPattern']:
      if re.search(pattern, id):
        if omit_datasets:
          logger.info(id)
          logger.warn(f"  Dropping dataset {id} b/c it is not in Nand's list")
          return True
        else:
          logger.info(id)
          logger.warn(f"  Keeping dataset {id} even though it is not in Nand's list")
          return False
  else:
    if id in fixes['omitDatasetSubset'].keys() and depend_0 in fixes['omitDatasetSubset'][id]:
      logger.warn(f"  Dropping {id} variables associated with DEPEND_0 = \"{depend_0}\" b/c this DEPEND_0 is not in Nand's list")
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

  CATDESC, emsg, etype = cdawmeta.attrib.CATDESC(dsid, name, variable)
  if etype is not None and print_info:
    cdawmeta.error('hapi', dsid, name, etype, "      " + emsg, logger)
  if CATDESC is None:
    CATDESC = ""

  VAR_NOTES, emsg, etype = cdawmeta.attrib.VAR_NOTES(dsid, name, variable)
  if etype is not None and print_info:
    cdawmeta.error('hapi', dsid, name, etype, "      " + emsg, logger)
  if VAR_NOTES is None:
    VAR_NOTES = ""

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

def _default_fill(cdf_type):

  if cdf_type == 'CDF_EPOCH':
    return '9999-12-31T23:59:59.999'

  if cdf_type == 'CDF_EPOCH16':
    return '9999-12-31T23:59:59.999999999999'

  if cdf_type == 'CDF_TT2000':
    return '9999-12-31T23:59:59.999999999'

  if cdf_type in ['CDF_FLOAT', 'CDF_DOUBLE', 'CDF_REAL4', 'CDF_REAL8']:
    return "-1.0e31"

  if cdf_type == 'CDF_BYTE':
    return -128

  if cdf_type == 'CDF_INT1':
    return -128

  if cdf_type == 'CDF_UINT1':
    return 255

  if cdf_type == 'CDF_INT2':
    return -32768

  if cdf_type == 'CDF_UINT2':
    return 65535

  if cdf_type == 'CDF_INT4':
    return -2147483648

  if cdf_type == 'CDF_UINT4':
    return 4294967295

  if cdf_type == 'CDF_INT8':
    return -9223372036854775808

  return None

def _to_hapi_type(cdf_type):

  if cdf_type in ['CDF_CHAR', 'CDF_UCHAR']:
    return 'string'

  if cdf_type.startswith('CDF_EPOCH') or cdf_type.startswith('CDF_TIME'):
    return 'isotime'

  if cdf_type.startswith('CDF_INT') or cdf_type.startswith('CDF_UINT') or cdf_type.startswith('CDF_BYTE'):
    return 'integer'

  if cdf_type in ['CDF_FLOAT', 'CDF_DOUBLE', 'CDF_REAL4', 'CDF_REAL8']:
    return 'double'

  return None
