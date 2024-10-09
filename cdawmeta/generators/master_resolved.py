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

  return [master]

def _resolve_ptr(id, variable_name, all_variables, logger, ptr_type=None):

  # This will replace the _resolve_ptrs.py
  indent = "    "

  pointer_name = all_variables[variable_name]['VarAttributes'].get('UNIT_PTR', None)

  if pointer_name is None:
    return None

  if pointer_name not in all_variables:
    emsg = f"{indent}{id}/{variable_name} has UNIT_PTR = '{pointer_name}' which is not a variable in dataset."
    cdawmeta.error('master_resolved', id, variable_name, "CDF.InvalidPtrReference", emsg, logger)
    return None

  msgo = f"{indent}{id}/{variable_name} has UNIT_PTR = '{pointer_name}' "

  if 'VarDescription' not in all_variables[pointer_name]:
    emsg = f"{msgo} with no VarDescription."
    cdawmeta.error('master_resolved', id, pointer_name, "CDF.NoVarAttributes", emsg, logger)
    return None

  if 'DataType' not in all_variables[pointer_name]['VarDescription']:
    emsg = f"{msgo} with no DataType."
    cdawmeta.error('master_resolved', id, pointer_name, "CDF.NoDataType", emsg, logger)
    return None

  cdf_string_types = ['CDF_CHAR', 'CDF_UCHAR']

  DataType = all_variables[pointer_name]['VarDescription']['DataType']
  if DataType not in cdf_string_types:
    emsg = f"{msgo} with DataType = '{DataType}' which is not of type {cdf_string_types}."
    cdawmeta.error('master_resolved', id, pointer_name, "CDF.InvalidUnitPtrDataType", emsg, logger)
    return None

  if 'VarData' not in all_variables[pointer_name]:
    emsg = f"{msgo} with no VarData."
    cdawmeta.error('master_resolved', id, pointer_name, "CDF.NoVarData", emsg, logger)
    return None

  pointer_values_o = all_variables[pointer_name]['VarData']
  pointer_values = cdawmeta.util.trim(pointer_values_o)

  DimSizes = all_variables[pointer_name]['VarDescription'].get('DimSizes', [])

  if len(pointer_values) > 1 and len(pointer_values) != DimSizes[0]:
    msg = f"{msgo}with {len(pointer_values)} values, but "
    msg += f"'{variable_name}' has DimSizes[0] = {DimSizes[0]}."
    cdawmeta.error('master_resolved', id, pointer_name, "CDF.PtrSizeMismatch", emsg, logger)
    return None

  return {'UNIT_PTR': pointer_name, 'values': pointer_values, 'values_given': pointer_values_o}

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

def _UNITS(id, variable_name, all_variables, VAR_TYPE, logger):

  indent = "    "
  units = None

  variable = all_variables[variable_name]

  unit_ptr = _resolve_ptr(id, variable_name, all_variables, logger, ptr_type='UNIT_PTR')
  if unit_ptr is not None:
    logger.info(f"{indent}UNIT_PTR:       '{unit_ptr['UNIT_PTR']}'")
    logger.info(f"{indent}UNIT_PTR vals_o: {unit_ptr['values_given']}")
    logger.info(f"{indent}UNIT_PTR vals:   {unit_ptr['values']}")

  units_o = variable['VarAttributes'].get("UNITS", None)
  units = None

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

    if unit_ptr is not None:
      msg = f"{indent}UNIT_PTR = '{unit_ptr['UNIT_PTR']}' and UNITS. Using UNITS."
      cdawmeta.error('master_resolved', id, variable_name, "ISTP.UNITS", msg, logger)

  if units_o is None and unit_ptr is not None:
    units = unit_ptr['values']
    if len(units) == 1:
      units = units[0]

  if "UNIT_PTR" not in variable['VarAttributes'] and 'UNITS' not in variable['VarAttributes']:
    units_required = VAR_TYPE is not None and VAR_TYPE in ['data', 'support_data']
    if units_required:
      msg = f"{indent}VAR_TYPE = '{VAR_TYPE}' and no UNITS or UNIT_PTR."
      cdawmeta.error('master_resolved', id, variable_name, "ISTP.UNITS", msg, logger)

  if units_o is None:
    logger.info(f"{indent}UNITS given:     {units_o}")
  else:
    logger.info(f"{indent}UNITS given:    '{units_o}'")

  if isinstance(units, list):
    logger.info(f"{indent}UNITS resolved:  {units}")
  else:
    if units is None:
      logger.info(f"{indent}UNITS resolved:  {units}")
    else:
      logger.info(f"{indent}UNITS resolved: '{units}'")

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