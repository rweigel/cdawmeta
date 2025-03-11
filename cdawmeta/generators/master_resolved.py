import cdawmeta

dependencies = ['master']

indent = "    "

def master_resolved(metadatum, logger):

  id = metadatum['id']

  if 'data' not in metadatum['master']:
    msg = f"{id}: Not creating dataset for {id} b/c it has no 'data' attribute"
    cdawmeta.error('master_resolved', id, None, "ISTP.NoMaster", msg, logger)
    return {"error": msg}

  master = metadatum['master']['data'].copy()

  if 'CDFVariables' not in master:
    msg = f"{id}: Not creating dataset for {id} b/c it has no 'CDFVariables' attribute"
    cdawmeta.error('master_resolved', id, None, "CDF.NoCDFVariables", msg, logger)
    return {"error": msg}

  additions = cdawmeta.additions(logger)

  variables = master['CDFVariables']
  variable_names = list(variables.keys())
  logger.info(f"{id}")

  variables_removed = []
  logger.info("- Start check for variables to drop")
  for variable_name in variable_names.copy():
    removed_variable = _check_variable(id, variable_name, variables, logger)
    if removed_variable is not None:
      variables_removed.append(removed_variable)
  logger.info("- End check for variables to drop")

  for variable_name in variable_names.copy():

    logger.info(f"  {variable_name}")
    if variable_name in variables_removed:
      logger.info("Skipping b/c removed.")
      continue

    variable = variables[variable_name]

    VAR_TYPE = variable['VarAttributes'].get('VAR_TYPE', None)
    logger.info(f"{indent}VAR_TYPE: '{VAR_TYPE}'")

    NumDims = variable['VarDescription'].get('NumDims', None)
    logger.info(f"{indent}NumDims = {NumDims}")

    DimSizes = variable['VarDescription'].get('DimSizes', None)
    logger.info(f"{indent}DimSizes = {DimSizes}")

    DimVariances = variable['VarDescription'].get('DimVariances', None)
    logger.info(f"{indent}DimVariances = {DimVariances}")

    NumElements = variable['VarDescription'].get('NumElements', None)
    logger.info(f"{indent}NumElements = {NumElements}")

    RecVariance = variable['VarDescription'].get('RecVariance', None)
    logger.info(f"{indent}RecVariance = {RecVariance}")

    variable['UNITS'] = _UNITS(id, variable_name, variables, variables_removed, logger)

    UNITS_VO = _UNITS_VO(id, variable_name, variable['UNITS'], additions, logger)
    if UNITS_VO is not None:
      variable['UNITS_VO'] = UNITS_VO

    if 'LABLAXIS' in variable['VarAttributes']:
      LABLAXIS = variable['VarAttributes']['LABLAXIS']
      logger.info(f"    LABELAXIS given:    {LABLAXIS}")
      if cdawmeta.CONFIG['hapi']['strip_labelaxis']:
        LABLAXIS_RESOLVED = cdawmeta.util.trim(LABLAXIS)
        variable['VarAttributes']['LABLAXIS'] = LABLAXIS_RESOLVED
      logger.info(f"    LABELAXIS resolved: {variable['VarAttributes']['LABLAXIS']}")

    LABL_PTR_RESOLVED = _LABL_PTR_RESOLVED(id, variable_name, variables, variables_removed, logger)
    if LABL_PTR_RESOLVED is not None:
      variable['LABL_PTR_RESOLVED'] = LABL_PTR_RESOLVED
      logger.info(f"    LABL_PTR_RESOLVED: {LABL_PTR_RESOLVED}")

    DEPEND_RESOLVED = _DEPEND_RESOLVED(id, variable_name, variables, variables_removed, logger)
    if DEPEND_RESOLVED is not None:
      variable['DEPEND_RESOLVED'] = DEPEND_RESOLVED
      logger.info(f"    DEPEND_RESOLVED: {DEPEND_RESOLVED}")

  return [master]

def _resolve_ptr(id, variable_name, variables, variables_removed, logger, ptr_name=None):

  # This will replace _resolve_ptrs.py

  ptr_var = variables[variable_name]['VarAttributes'].get(ptr_name, None)
  if ptr_var is None:
    # No pointer to resolve
    return None

  if ptr_var not in variables:
    emsg = f"{indent}{id}/{variable_name} has {ptr_name} = '{ptr_var}' which is not a variable in dataset"
    if ptr_var in variables_removed:
      emsg += " because it was removed due to an error."
    else:
      emsg += "."
    cdawmeta.error('master_resolved', id, variable_name, "CDF.InvalidPtrReference", emsg, logger)
    return None

  msgo = f"{indent}{id}/{variable_name} has {ptr_name} = '{ptr_var}' "

  if 'VarDescription' not in variables[ptr_var]:
    emsg = f"{msgo} with no VarDescription."
    cdawmeta.error('master_resolved', id, ptr_var, "CDF.NoVarAttributes", emsg, logger)
    return None

  if 'DataType' not in variables[ptr_var]['VarDescription']:
    emsg = f"{msgo} with no DataType."
    cdawmeta.error('master_resolved', id, ptr_var, "CDF.NoDataType", emsg, logger)
    return None

  if 'VarData' not in variables[ptr_var]:
    if ptr_name == 'UNIT_PTR' or ptr_name.startswith('LABL_PTR'):
      emsg = f"{msgo} with no VarData."
      cdawmeta.error('master_resolved', id, ptr_var, "CDF.NoVarData", emsg, logger)
      return None
    if ptr_name.startswith('DEPEND'):
      # TODO: Check that dims match variable that references
      logger.info(f"{indent}{ptr_name} Does not have VarData, so not resolving values.")
      return {'variable_name': ptr_var, 'values': None, 'values_trimmed': None}

  cdf_string_types = ['CDF_CHAR', 'CDF_UCHAR']
  DataType = variables[ptr_var]['VarDescription']['DataType']
  if DataType not in cdf_string_types:
    if ptr_name.startswith('DEPEND'):
      # TODO: Check that dims match variable that references
      logger.info(f"{indent}{ptr_name} has VarData, but values are not strings, so not resolving.")
      return {'variable_name': ptr_var, 'values': None, 'values_trimmed': None}
    else:
      emsg = f"{msgo}with DataType = '{DataType}' which is not of type {cdf_string_types}."
      cdawmeta.error('master_resolved', id, ptr_var, "CDF.InvalidUnitPtrDataType", emsg, logger)
      return None

  values = variables[ptr_var]['VarData']
  values_trimmed = cdawmeta.util.trim(values)

  # TODO: Check that dims match variable that references

  DimSizes = variables[ptr_var]['VarDescription'].get('DimSizes', None)
  if DimSizes is None and len(values_trimmed) > 0:
    emsg = f"{msgo}with {len(values_trimmed)} values, but "
    emsg += f"'{variable_name}' has no DimSizes."
    cdawmeta.error('master_resolved', id, ptr_var, "CDF.PtrSizeMismatch", emsg, logger)
    return None

  if len(values_trimmed) > 1 and len(values_trimmed) != DimSizes[0]:
    emsg = f"{msgo}with {len(values_trimmed)} values, but "
    emsg += f"'{variable_name}' has DimSizes[0] = {DimSizes[0]}."
    cdawmeta.error('master_resolved', id, ptr_var, "CDF.PtrSizeMismatch", emsg, logger)
    return None

  logger.info(f"{indent}{ptr_name} name:   {ptr_var}")
  logger.info(f"{indent}{ptr_name} values: {values}")
  if "".join(values) != "".join(values_trimmed):
    logger.info(f"{indent}{ptr_name} values_trimmed: {values_trimmed}")

  return {'variable_name': ptr_var, 'values': values, 'values_trimmed': values_trimmed}

def _check_variable(id, variable_name, variables, logger):

  removing = "Removing it from dataset."
  variable = variables[variable_name]

  if 'VarAttributes' not in variable:
    emsg = f"{indent}{variable_name}No VarAttributes. {removing}"
    cdawmeta.error('master_resolved', id, variable_name, "CDF.NoVarAttributes", emsg, logger)
    del variables[variable_name]
    return variable_name

  var_types = ['data', 'support_data', 'metadata', 'ignore_data']
  VAR_TYPE = variable['VarAttributes'].get('VAR_TYPE', None)
  #logger.info(f"{indent}VAR_TYPE: '{VAR_TYPE}'")
  if VAR_TYPE is None:
    emsg = f"{indent}{variable_name}No VAR_TYPE. {removing}"
    cdawmeta.error('master_resolved', id, variable_name, "CDF.NoVAR_TYPE", emsg, logger)
    del variables[variable_name]
    return variable_name

  if VAR_TYPE not in var_types:
    emsg = f"{indent}{variable_name}VAR_TYPE = '{VAR_TYPE}' which is not in {var_types}."
    cdawmeta.error('master_resolved', id, variable_name, "CDF.NoVarAttributes", emsg, logger)
    del variables[variable_name]
    return variable_name

  if 'VarDescription' not in variable:
    emsg = f"{indent}{variable_name}No VarDescription. {removing}"
    cdawmeta.error('master_resolved', id, variable_name, "CDF.VarDescription", emsg, logger)
    del variables[variable_name]
    return variable_name

  NumDims = variable['VarDescription'].get('NumDims', None)
  #logger.info(f"{indent}NumDims: {NumDims}")

  DimSizes = variable['VarDescription'].get('DimSizes', None)
  #logger.info(f"{indent}DimSizes: {DimSizes}")
  if DimSizes is None:
    DimSizes = []

  DimVariances = variable['VarDescription'].get('DimVariances', None)
  #logger.info(f"{indent}DimVariances: {DimVariances}")
  if DimVariances is None:
    DimVariances = []

  if NumDims != len(DimSizes):
    emsg = f"{indent}{variable_name}DimSizes mismatch: NumDims = {NumDims} "
    emsg += f"!= len(DimSizes) = {len(DimSizes)}. {removing}"
    cdawmeta.error('hapi', id, variable_name, "CDF.DimSizes", emsg, logger)
    del variables[variable_name]
    return variable_name

  if len(DimSizes) != len(DimVariances):
    emsg = f"{indent}{variable_name}DimVariances mismatch: len(DimSizes) = {DimSizes} "
    emsg += f"!= len(DimVariances) = {len(DimVariances)}. {removing}"
    cdawmeta.error('hapi', id, variable_name, "CDF.DimVariance", emsg, logger)
    del variables[variable_name]
    return variable_name

  return None

def _summary(original, resolved, attribute_name, logger):
  indent = "    "

  msg = f"{indent}{attribute_name} given:    "
  if original is None:
    logger.info(f"{msg}{original}")
  else:
    logger.info(f"{msg}'{original}'")

  if type(original) is type(resolved):
    if isinstance(original, list):
      if "".join(original) == "".join(resolved):
        return
    if original == resolved:
      return

  msg = f"{indent}{attribute_name} resolved: "
  if isinstance(resolved, list):
    logger.info(f"{msg}{resolved}")
  else:
    if resolved is None:
      logger.info(f"{msg}{resolved}")
    else:
      logger.info(f"{msg}'{resolved}'")

def _DEPEND_RESOLVED(id, variable_name, variables, variables_removed, logger):
  depend = []

  found = False
  for i in [1, 2, 3]:
    depend_resolved = _resolve_ptr(id, variable_name, variables, variables_removed, logger, ptr_name=f'DEPEND_{i}')
    if depend_resolved is not None:
      found = True
      if depend_resolved['values_trimmed'] is not None:
        depend.append(depend_resolved['values_trimmed'])
      else:
        depend.append(depend_resolved['variable_name'])

  if not found:
    depend = None

  return depend

def _LABL_PTR_RESOLVED(id, variable_name, variables, variables_removed, logger):

  labl_ptrs = []
  found = False
  for i in [1, 2, 3]:
    labl_ptr_resolved = _resolve_ptr(id, variable_name, variables, variables_removed,logger, ptr_name=f'LABL_PTR_{i}')
    if labl_ptr_resolved is not None:
      found = True
      labl_ptrs.append(labl_ptr_resolved['values_trimmed'])

  if not found:
    labl_ptrs = None

  return labl_ptrs

def _UNITS(id, variable_name, variables, variables_removed, logger):

  indent = "    "
  units = None

  units_ptr_resolved = _resolve_ptr(id, variable_name, variables, variables_removed, logger, ptr_name='UNIT_PTR')

  variable = variables[variable_name]
  units_o = variable['VarAttributes'].get("UNITS", None)
  VAR_TYPE = variable['VarAttributes'].get("VAR_TYPE", None)

  if units_o is not None:

    units = units_o

    if not units.isprintable():
      msg = f"{indent}UNITS with non-printable characters. Setting UNITS = None"
      cdawmeta.error('master_resolved', id, variable_name, "CDF.UNITS", msg, logger)
      units = None
    elif units.strip() == "":
      msg = f"{indent}UNITS.strip() = ''"
      if VAR_TYPE is not None and VAR_TYPE in ['data', 'support_data']:
        # Catch case where empty string or whitespace string used for UNITS,
        # presumably to "satisfy" ISTP requirements that UNITS be present.
        msg = f"{indent}VAR_TYPE = '{VAR_TYPE}' and UNITS.strip() = ''. Setting UNITS = None"
        cdawmeta.error('master_resolved', id, variable_name, "ISTP.UNITS", msg, logger)
        units = None
    else:
      units = units_o.strip()

    if units_ptr_resolved is not None:
      msg = f"{indent}Both UNIT_PTR and UNITS attributes found. Using UNITS."
      cdawmeta.error('master_resolved', id, variable_name, "ISTP.UNITS", msg, logger)

  if units_o is None and units_ptr_resolved is not None:
    units = units_ptr_resolved['values_trimmed']
    if len(units) == 1:
      units = units[0]

  if "UNIT_PTR" not in variable['VarAttributes'] and 'UNITS' not in variable['VarAttributes']:
    units_required = VAR_TYPE is not None and VAR_TYPE in ['data', 'support_data']
    if units_required:
      msg = f"{indent}VAR_TYPE = '{VAR_TYPE}' and no UNITS or UNIT_PTR."
      cdawmeta.error('master_resolved', id, variable_name, "ISTP.UNITS", msg, logger)

  _summary(units_o, units, 'UNITS', logger)

  return units

def _UNITS_VO(id, variable_name, UNITS, additions, logger):

  if UNITS is None:
    return None

  indent = "    "
  UNITS_VO = None

  UNITS_VO = []
  if isinstance(UNITS, str):
    UNITS = [UNITS]

  for unit in UNITS:
    if unit not in additions['Units']:
      if unit.strip() == "":
        return None
      msg = f"{indent}  Could not mapping from CDAWeb unit = '{unit}' to VO_UNIT in additions['Units']"
      cdawmeta.error('master_resolved', id, variable_name, "VOUnits.NotFound", msg, logger)
      return None
    units_vo = additions['Units'][unit]
    if unit in additions['Units'] and units_vo is not None:
      logger.info(f"{indent}Found UNITS_VO = '{units_vo}' for UNITS = '{unit}'")
      UNITS_VO.append(units_vo)

  if len(UNITS_VO) > 1 and len(UNITS_VO) != len(UNITS):
    logger.info(f"{indent}Could not determine UNITS_VO for all units. Not creating UNITS_VO.")
    return None

  if len(UNITS_VO) == 0:
    return None

  if UNITS_VO is not None:
    if len(UNITS_VO) == 1:
      UNITS_VO = UNITS_VO[0]

    if isinstance(UNITS_VO, list):
      logger.info(f"{indent}UNITS_VO:        {UNITS_VO}")
    else:
      logger.info(f"{indent}UNITS_VO:       '{UNITS_VO}'")

  return UNITS_VO