import cdawmeta

dependencies = ['master']

def master_resolved(metadatum, logger):

  id = metadatum['id']

  if 'data' not in metadatum['master']:
    msg = f"{id}: Not creating dataset for {id} b/c it has no 'data' key"
    cdawmeta.error('master_resolved', id, None, "ISTP.NoMaster", msg, logger)
    return {"error": msg}

  master = metadatum['master']['data'].copy()

  if 'CDFVariables' not in master:
    msg = f"{id}: Not creating dataset for {id} b/c it has no 'CDFVariables' key"
    cdawmeta.error('master_resolved', id, None, "CDF.NoCDFVariables", msg, logger)
    return {"error": msg}

  additions = cdawmeta.additions(logger)

  variables = master['CDFVariables']
  variable_names = list(variables.keys())
  logger.info(f"{id}")

  for variable_name in variable_names.copy():

    logger.info(f"  {variable_name}")
    variable = variables[variable_name]

    if 'VarAttributes' not in variable:
      emsg = "No VarAttributes. Removing it from dataset."
      cdawmeta.error('master_resolved', id, variable_name, "CDF.NoVarAttributes", "    " + emsg, logger)
      del variables[variable_name]
      continue

    VAR_TYPE, etype, emsg = _VAR_TYPE(variable)
    if etype is not None:
      emsg = f"{emsg} Removing it from dataset."
      cdawmeta.error('master_resolved', id, variable_name, etype, "    " + emsg, logger)
      del variables[variable_name]
      continue

    variable['UNITS'] = _UNITS(id, variable_name, variables, VAR_TYPE, logger)

    UNITS_VO = _UNITS_VO(variable['UNITS'], additions, logger)
    if UNITS_VO is not None:
      variable['UNITS_VO'] = UNITS_VO

    variable['LABLAXIS'] = _LABLAXIS(id, variable_name, variables, VAR_TYPE, logger)

    variable['DEPEND'] = _DEPEND(id, variable_name, variables, VAR_TYPE, logger)

  return [master]

def _resolve_ptr(id, variable_name, variables, logger, ptr_name=None):

  # This will replace _resolve_ptrs.py
  indent = "    "

  ptr_var = variables[variable_name]['VarAttributes'].get(ptr_name, None)
  if ptr_var is None:
    # No pointer to resolve
    return None

  if ptr_var not in variables:
    emsg = f"{indent}{id}/{variable_name} has {ptr_name} = '{ptr_var}' which is not a variable in dataset."
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

  DimSizes = variables[ptr_var]['VarDescription'].get('DimSizes', [])
  # TODO: Check that dims match variable that references

  if len(values_trimmed) > 1 and len(values_trimmed) != DimSizes[0]:
    msg = f"{msgo}with {len(values_trimmed)} values, but "
    msg += f"'{variable_name}' has DimSizes[0] = {DimSizes[0]}."
    cdawmeta.error('master_resolved', id, ptr_var, "CDF.PtrSizeMismatch", emsg, logger)
    return None

  logger.info(f"{indent}{ptr_name} variable name: '{ptr_var}'")
  logger.info(f"{indent}{ptr_name} values:         {values}")
  if "".join(values) != "".join(values_trimmed):
    logger.info(f"{indent}{ptr_name} values_trimmed: {values_trimmed}")

  return {'variable_name': ptr_var, 'values': values, 'values_trimmed': values_trimmed}

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

def _VAR_TYPE(variable):

  indent = "    "

  var_types = ['data', 'support_data', 'metadata', 'ignore_data']
  VAR_TYPE = variable['VarAttributes'].get('VAR_TYPE', None)

  if VAR_TYPE is not None:
    if VAR_TYPE not in var_types:
      msg = f"{indent}VAR_TYPE = '{VAR_TYPE}' which is not in {var_types}."
      return None, "CDF.InvalidVarType", msg
    return variable['VarAttributes']['VAR_TYPE'], None, None
  else:
    return None, "CDF.NoVarType", f"{indent}No VAR_TYPE."

def _DEPEND(id, variable_name, variables, VAR_TYPE, logger):
  indent = "    "
  depend = []

  variable = variables[variable_name]

  for i in [1, 2, 3]:
    depend_resolved = _resolve_ptr(id, variable_name, variables, logger, ptr_name=f'DEPEND_{i}')
    if depend_resolved is not None:
      if depend_resolved['values_trimmed'] is not None:
        depend.append(depend_resolved['values_trimmed'])
      else:
        depend.append(depend_resolved['variable_name'])

  #_summary(lablaxis_o, lablaxis, 'LABLAXIS', logger)

  return depend

def _LABLAXIS(id, variable_name, variables, VAR_TYPE, logger):
  indent = "    "
  lablaxis = []

  variable = variables[variable_name]
  lablaxis_o = variable['VarAttributes'].get("LABLAXIS", None)

  found_ptr = False
  for i in [1, 2, 3]:
    lablaxis_resolved = _resolve_ptr(id, variable_name, variables, logger, ptr_name=f'LABL_PTR_{i}')
    if lablaxis_resolved is not None:
      found_ptr = True
      lablaxis.append(lablaxis_resolved['values_trimmed'])

  if lablaxis_o is not None and found_ptr:
    msg = f"{indent}LABLAXIS and LABL_PTRs found. Using LABL_PTRs"
    cdawmeta.error('master_resolved', id, variable_name, "CDF.LABLAXIS", msg, logger)
  else:
    lablaxis = lablaxis_o

  _summary(lablaxis_o, lablaxis, 'LABLAXIS', logger)

  return lablaxis

def _UNITS(id, variable_name, variables, VAR_TYPE, logger):

  indent = "    "
  units = None

  units_ptr_resolved = _resolve_ptr(id, variable_name, variables, logger, ptr_name='UNIT_PTR')

  variable = variables[variable_name]
  units_o = variable['VarAttributes'].get("UNITS", None)

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

def _UNITS_VO(UNITS, additions, logger):

  if UNITS is None:
    return None

  indent = "    "
  UNITS_VO = None

  UNITS_VO = []
  if isinstance(UNITS, str):
    UNITS = [UNITS]

  for unit in UNITS:
    units_vo = additions['Units'][unit]
    if unit in additions['Units'] and units_vo is not None:
      logger.info(f"{indent}  Found UNITS_VO = '{units_vo}' for UNITS = '{unit}'")
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