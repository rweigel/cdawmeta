import cdawmeta

dependencies = ['master']

indent = "    "

def master_resolved(metadatum, logger):

  id = metadatum['id']

  master = metadatum['master'].get('data', None)
  if master is None:
    msg = f"{id}: Not creating dataset for {id} b/c it has no 'data' attribute"
    cdawmeta.error('master_resolved', id, None, "ISTP.NoMaster", msg, logger)
    return {"error": msg}

  master = master.copy()

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

  logger.info("- Start fixing of attributes in master CDF")
  _fix_attributes(metadatum, logger)
  logger.info("- End fixing of attributes in master CDF")

  for variable_name in variable_names.copy():

    logger.info(f"  {variable_name}")
    if variable_name in variables_removed:
      logger.info("Skipping b/c removed.")
      continue

    variable = variables[variable_name]

    VAR_TYPE = variable['VarAttributes'].get('VAR_TYPE', None)
    logger.info(f"{indent}VAR_TYPE: '{VAR_TYPE}'")

    DataType = variables[variable_name]['VarDescription']['DataType']
    logger.info(f"{indent}DataType = {DataType}")

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

    if VAR_TYPE == 'metadata' and DataType not in ['CDF_CHAR', 'CDF_UCHAR']:
      emsg = f"{indent}CDF VAR_TYPE = 'metadata' and DataType not one of ['CDF_CHAR', 'CDF_UCHAR']"
      cdawmeta.error('master_resolved', id, variable_name, "ISTP.DataTypeWrong", emsg, logger)

    variable['VarAttributes']['x_UNITS'] = _UNITS(id, variable_name, variables, variables_removed, logger)

    UNITS_VO = _UNITS_VO(id, variable_name, variable['VarAttributes']['x_UNITS'], additions, logger)
    if UNITS_VO is not None:
      variable['VarAttributes']['x_UNITS_VO'] = UNITS_VO

    LABLAXIS = _LABLAXIS(id, variable, logger)
    if LABLAXIS is not None:
      variable['VarAttributes']['x_LABLAXIS'] = LABLAXIS
      logger.info(f"{indent}x_LABLAXIS: {LABLAXIS}")

    LABL_PTR = _LABL_PTR(id, variable_name, variables, variables_removed, logger)
    if LABL_PTR is not None:
      variable['VarAttributes']['x_LABLAXES'] = LABL_PTR
      logger.info(f"{indent}x_LABLAXES: {LABL_PTR}")

    if VAR_TYPE in ['data', 'support_data']:
      if LABLAXIS is None and LABL_PTR is None:
        emsg = f"{indent}For VAR_TYPE = 'data' or 'support_data', if no LABLAXIS, LABL_PTR_i is required."
        cdawmeta.error('master_resolved', id, variable_name, "CDF.MissingLABL_PTR", emsg, logger)

    DEPEND = _DEPEND(id, variable_name, variables, variables_removed, logger)
    if DEPEND is not None:
      variable['VarAttributes']['x_DEPEND'] = DEPEND
      logger.info(f"{indent}x_DEPEND: {DEPEND}")

    if DEPEND is not None and DEPEND == LABL_PTR:
      emsg = f"{indent}DEPEND == LABL_PTR. Removing redundant DEPEND and DEPEND_{{1,2,3}}"
      cdawmeta.error('master_resolved', id, None, "CDF.DEPENDsEqualLABL_PTR", emsg, logger)
      del variable['VarAttributes']['x_DEPEND']
      for i in [1, 2, 3]:
        # TODO: This could create a metadata variable that is not referenced.
        variable['VarAttributes'].pop(f'DEPEND_{i}', None)

    v = variables[variable_name]['VarAttributes'].get('VIRTUAL', None)
    if v is not None and v == 'true':
      logger.info(f"{indent}VIRTUAL: true")
      funct = variables[variable_name]['VarAttributes'].get('FUNCT', None)
      if funct is not None:
        logger.info(f"{indent}FUNCT: {funct}")

      COMPONENTS = []
      for i in [0, 1, 2, 3]:
        c = variables[variable_name]['VarAttributes'].get(f'COMPONENT_{i}', None)
        if c is not None:
          COMPONENTS.append(c)
          logger.info(f'{indent}COMPONENT_{i}: {c}')
      if len(COMPONENTS) > 0:
        variable['VarAttributes']['COMPONENTS'] = COMPONENTS
        logger.info(f"{indent}COMPONENTS: {COMPONENTS}")

  return [master]

def _fix_attributes(metadatum, logger):
  id = metadatum['id']
  table_config = cdawmeta.CONFIG['table']['tables']['cdaweb.dataset']
  CDFglobalAttributes = metadatum['master']['data']['CDFglobalAttributes']
  for attribute in list(CDFglobalAttributes.keys()).copy():
    if attribute in table_config['fixes']:
      emsg = f"{indent}Renaming CDFglobalAttribute '{attribute}' to '{table_config['fixes'][attribute]}'"
      cdawmeta.error('master_resolved', id, None, "CDF.AttributeNameError", emsg, logger)
      fixed_attribute = table_config['fixes'][attribute]
      CDFglobalAttributes[fixed_attribute] = CDFglobalAttributes.pop(attribute)

  table_config = cdawmeta.CONFIG['table']['tables']['cdaweb.variable']
  CDFVariables = metadatum['master']['data']['CDFVariables']
  for variable_name in CDFVariables.keys():
    variable = CDFVariables[variable_name]
    for attribute in list(variable['VarAttributes'].keys()).copy():
      if attribute in table_config['fixes']:
        emsg = f"{indent}{variable_name}: Renaming VarAttributes attribute '{attribute}' to '{table_config['fixes'][attribute]}'"
        cdawmeta.error('master_resolved', id, variable_name, "CDF.AttributeNameError", emsg, logger)
        fixed_attribute = table_config['fixes'][attribute]
        variable['VarAttributes'][fixed_attribute] = variable['VarAttributes'].pop(attribute)

    VIRTUAL = _VIRTUAL(id, variable_name, variable, logger)
    if VIRTUAL is not None:
      variable['VarAttributes']['VIRTUAL'] = VIRTUAL

    FUNCT = _FUNCT(id, variable_name, variable, VIRTUAL, logger)
    if FUNCT is not None:
      variable['VarAttributes']['FUNCT'] = FUNCT

    DISPLAY_TYPE = _DISPLAY_TYPE(id, variable_name, variable, logger)
    if DISPLAY_TYPE is not None:
      variable['VarAttributes']['DISPLAY_TYPE'] = DISPLAY_TYPE

def _resolve_ptr(id, variable_name, variables, variables_removed, logger, ptr_name=None):

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

  msgo = f"{indent}Variable has {ptr_name} = '{ptr_var}' "

  if 'VarData' not in variables[ptr_var]:
    if ptr_name == 'UNIT_PTR' or ptr_name.startswith('LABL_PTR'):
      emsg = f"{msgo} with no VarData."
      cdawmeta.error('master_resolved', id, ptr_var, "CDF.NoVarData", emsg, logger)
      return None
    if ptr_name.startswith('DEPEND') or ptr_name.startswith('COMPONENT'):
      # TODO: Check that dims match variable that references
      logger.info(f"{indent}{ptr_name} Does not have VarData, so not resolving values.")
      return {'variable_name': ptr_var, 'values': None, 'values_trimmed': None}

  cdf_string_types = ['CDF_CHAR', 'CDF_UCHAR']
  DataType = variables[ptr_var]['VarDescription']['DataType']
  if DataType not in cdf_string_types:
    if ptr_name.startswith('DEPEND') or ptr_name.startswith('COMPONENT'):
      # TODO: Check that dims match variable that references
      RecVariance = variables[ptr_var]['VarDescription'].get('RecVariance', None)
      logger.info(f"{indent}{ptr_name} has VarData (RecVariance = {RecVariance}), but values are not strings, so not resolving.")
      return {'variable_name': ptr_var, 'values': None, 'values_trimmed': None}
    else:
      emsg = f"{msgo}with DataType = '{DataType}' which is not of type {cdf_string_types}."
      cdawmeta.error('master_resolved', id, ptr_var, "CDF.InvalidUnitPtrDataType", emsg, logger)
      return None

  values = variables[ptr_var]['VarData']
  values_trimmed = cdawmeta.util.trim(values)

  DimSizes = variables[ptr_var]['VarDescription'].get('DimSizes', None)
  DimSizesParent = variables[variable_name]['VarDescription'].get('DimSizes', None)

  VAR_TYPE = variables[variable_name]['VarAttributes'].get('VAR_TYPE', None)
  if ptr_name.startswith('DEPEND') and VAR_TYPE != 'support_data':
    emsg = f"{msgo}with VAR_TYPE = '{VAR_TYPE}' which is not 'support_data'."
    cdawmeta.error('master_resolved', id, ptr_var, "CDF.InvalidDependPtrVarType", emsg, logger)

  if ptr_name.startswith('DEPEND') or ptr_name.startswith('COMPONENT') or ptr_name.startswith('LABL_PTR'):
    ptr_name_end = ptr_name.split('_')[-1]
    if ptr_name_end.isdigit():
      ptr_idx = int(ptr_name_end) - 1
      if DimSizesParent is None:
        emsg = f"{msgo}with ptr_idx = {ptr_idx} having DimsSizes = {DimSizes}, "
        emsg += "but referencing variable has DimSizes = None."
        cdawmeta.error('master_resolved', id, ptr_var, "CDF.PtrSizeMismatch", emsg, logger)
        return None
      if ptr_idx + 1 > len(DimSizesParent):
        emsg = f"{msgo}with ptr_idx = {ptr_idx} which is greater than "
        emsg += f"len(DimSizes) = {len(DimSizesParent)} of referencing variable."
        cdawmeta.error('master_resolved', id, ptr_var, "CDF.PtrSizeMismatch", emsg, logger)
        return None
      if len(values_trimmed) > 1 and len(values_trimmed) != DimSizesParent[ptr_idx]:
        emsg = f"{msgo}with {len(values_trimmed)} values, but referencing variable "
        emsg += f"has DimSizes[{ptr_idx}] = {DimSizesParent[ptr_idx]}."
        cdawmeta.error('master_resolved', id, ptr_var, "CDF.PtrSizeMismatch", emsg, logger)
        return None
    else:
      emsg = f"{msgo}with ptr_name = '{ptr_name}' does not end with an integer."
  else:
    if DimSizesParent is None:
      emsg = f"{msgo}with DimsSizes = {DimSizes}, but referencing variable "
      emsg += "has DimSizes = None."
      cdawmeta.error('master_resolved', id, ptr_var, "CDF.PtrSizeMismatch", emsg, logger)
      return None
    if len(values_trimmed) != DimSizesParent[0]:
      emsg = f"{msgo}with {len(values_trimmed)} values, but "
      emsg += f"of referencing variable '{variable_name}' has DimSizes[0] = {DimSizesParent[0]}."
      cdawmeta.error('master_resolved', id, ptr_var, "CDF.PtrSizeMismatch", emsg, logger)
      return None

  if not isinstance(values, list):
    values = [values]
  if not isinstance(values_trimmed, list):
    values_trimmed = [values_trimmed]

  logger.info(f"{indent}{ptr_name} name:   {ptr_var}")
  logger.info(f"{indent}{ptr_name} values: {values}")
  if "".join(values) != "".join(values_trimmed):
    logger.info(f"{indent}{ptr_name} values_trimmed: {values_trimmed}")

  return {'variable_name': ptr_var, 'values': values, 'values_trimmed': values_trimmed}

def _check_variable(id, variable_name, variables, logger):

  removing = "Removing variable from dataset."
  variable = variables[variable_name]

  if 'VarAttributes' not in variable:
    emsg = f"{indent}  {variable_name}No VarAttributes. {removing}"
    cdawmeta.error('master_resolved', id, variable_name, "CDF.NoVarAttributes", emsg, logger)
    del variables[variable_name]
    return variable_name

  var_types = ['data', 'support_data', 'metadata', 'ignore_data']
  VAR_TYPE = variable['VarAttributes'].get('VAR_TYPE', None)

  if VAR_TYPE is None:
    emsg = f"{indent}  {variable_name} No VAR_TYPE. {removing}"
    cdawmeta.error('master_resolved', id, variable_name, "CDF.NoVAR_TYPE", emsg, logger)
    del variables[variable_name]
    return variable_name

  if VAR_TYPE not in var_types:
    emsg = f"{indent}  {variable_name} VAR_TYPE = '{VAR_TYPE}' which is not in {var_types}."
    cdawmeta.error('master_resolved', id, variable_name, "CDF.NoVarAttributes", emsg, logger)
    del variables[variable_name]
    return variable_name

  if 'VarDescription' not in variable:
    emsg = f"{indent}  {variable_name} No VarDescription. {removing}"
    cdawmeta.error('master_resolved', id, variable_name, "CDF.VarDescription", emsg, logger)
    del variables[variable_name]
    return variable_name

  if 'DataType' not in variables[variable_name]['VarDescription']:
    emsg = f"{indent}  {variable_name} No DataType. {removing}"
    cdawmeta.error('master_resolved', id, variable_name, "CDF.NoDataType", emsg, logger)
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
    emsg = f"{indent}  {variable_name}DimSizes mismatch: NumDims = {NumDims} "
    emsg += f"!= len(DimSizes) = {len(DimSizes)}. {removing}"
    cdawmeta.error('hapi', id, variable_name, "CDF.DimSizes", emsg, logger)
    del variables[variable_name]
    return variable_name

  if len(DimSizes) != len(DimVariances):
    emsg = f"{indent}  {variable_name}DimVariances mismatch: len(DimSizes) = {DimSizes} "
    emsg += f"!= len(DimVariances) = {len(DimVariances)}. {removing}"
    cdawmeta.error('hapi', id, variable_name, "CDF.DimVariance", emsg, logger)
    del variables[variable_name]
    return variable_name

  virtual = variable['VarAttributes'].get('VIRTUAL', None)
  if virtual is not None:
    virtual = virtual.lower().strip()
  else:
    virtual = 'false'

  funct = variable['VarAttributes'].get('FUNCT', None)
  function = variable['VarAttributes'].get('FUNCTION', None)

  if virtual == 'true' and virtual is True and (function is None and funct is None):
    emsg = f"{indent}{variable_name} VIRTUAL = 'true', but no FUNCTION or FUNCT. {removing}"
    cdawmeta.error('master_resolved', id, variable_name, "CDF.VirtualButNoFunctAttribute", emsg, logger)
    del variables[variable_name]
    return variable_name

  components = []
  MAX_COMPONENTS = 4
  for i in range(0, MAX_COMPONENTS):
    components.append(False)
    c = variables[variable_name]['VarAttributes'].get(f'COMPONENT_{i}', None)
    if c is not None:
      components[i] = True
      if c not in variables:
        if virtual:
          emsg = f"{indent}{variable_name} is VIRTUAL and has COMPONENT_{i} = '{c}' which is not a variable in dataset. {removing}"
          cdawmeta.error('master_resolved', id, variable_name, "CDF.InvalidComponentReference", emsg, logger)
          del variables[variable_name]
          return variable_name
        else:
          emsg = f"{indent}{variable_name} has COMPONENT_{i} = '{c}' which is not a variable in dataset. Not removing variable b/c variable is not virtual."
          cdawmeta.error('master_resolved', id, variable_name, "CDF.InvalidComponentReference", emsg, logger)

    for i in range(1, len(components)):
      if components[i] and not components[i-1]:
        if virtual:
          emsg = f"{indent}{variable_name} is VIRTUAL and has COMPONENT_{i} but no COMPONENT_{i-1}. {removing}"
          cdawmeta.error('master_resolved', id, variable_name, "CDF.MissingComponent", emsg, logger)
          del variables[variable_name]
          return variable_name
        else:
          emsg = f"{indent}{variable_name} has COMPONENT_{i} but no COMPONENT_{i-1}. Not removing variable b/c variable is not virtual."
          cdawmeta.error('master_resolved', id, variable_name, "CDF.MissingComponent", emsg, logger)

  return None

def _FUNCT(id, variable_name, variable, VIRTUAL, logger):
  FUNCT = variable['VarAttributes'].get('FUNCT', None)
  if VIRTUAL == 'false' and FUNCT is not None:
    emsg = f"{indent}{variable_name} VIRTUAL = 'false' and FUNCT = '{FUNCT}'. Removing attribute FUNCT."
    cdawmeta.error('master_resolved', id, variable_name, "CDF.AttributeError", emsg, logger)
    del variable['VarAttributes']['FUNCT']
    return None

  if FUNCT is not None and FUNCT == 'ALTERNATE_VIEW':
    emsg = f"{indent}{variable_name} FUNCT with value 'ALTERNATE_VIEW'; Renaming value to 'alternate_view'."
    cdawmeta.error('master_resolved', id, variable_name, "CDF.AttributeValueError", emsg, logger)
    FUNCT = 'alternate_view'

  return FUNCT

def _VIRTUAL(id, variable_name, variable, logger):
  VIRTUAL = variable['VarAttributes'].get('VIRTUAL', None)
  if VIRTUAL is not None:
    if VIRTUAL.lower() != VIRTUAL:
      emsg = f"{indent}{variable_name} VIRTUAL with value '{VIRTUAL}' which is not all lowercase; Renaming value to be all lowercase."
      cdawmeta.error('master_resolved', id, variable_name, "CDF.AttributeValueError", emsg, logger)
      VIRTUAL = VIRTUAL.lower()
    if VIRTUAL.strip() != VIRTUAL:
      emsg = f"{indent}{variable_name} VIRTUAL with value '{VIRTUAL}' which has leading or trailing whitespace; Renaming value to be stripped."
      cdawmeta.error('master_resolved', id, variable_name, "CDF.AttributeValueError", emsg, logger)
      VIRTUAL = VIRTUAL.strip()
    if VIRTUAL not in ['true', 'false']:
      emsg = f"{indent}{variable_name} VIRTUAL with value '{VIRTUAL}' which is not 'true' or 'false' after stripping whitespace and setting to all lowercase. Removing attribute VIRTUAL."
      cdawmeta.error('master_resolved', id, variable_name, "CDF.AttributeValueError", emsg, logger)
      del variable['VarAttributes']['VIRTUAL']
      return None
    variable['VarAttributes']['VIRTUAL'] = VIRTUAL

  return VIRTUAL

def _DISPLAY_TYPE(dsid, variable_name, variable, logger):

  if 'DISPLAY_TYPE' not in variable['VarAttributes']:
    if variable['VarAttributes'].get('VAR_TYPE') == 'data':
      emsg = f"{indent}{variable_name} DISPLAY_TYPE for variable with VAR_TYPE = 'data'."
      cdawmeta.error('master_resolved', id, variable_name, "ISTP.NoDISPLAY_TYPE", emsg, logger)
    return None

  display_type = variable['VarAttributes']['DISPLAY_TYPE']
  display_type_parts = display_type.split(">")
  display_type = display_type_parts[0]

  display_type_attributes = ""
  if len(display_type_parts) > 1:
    display_type_attributes = display_type_parts[1:]
    display_type_attributes = ">".join(display_type_attributes)

  if display_type.strip() == '':
    emsg = f"{indent}{variable_name} DISPLAY_TYPE.strip() = ''. Removing attribute DISPLAY_TYPE."
    cdawmeta.error('master_resolved', id, variable_name, "ISTP.EmptyDISPLAY_TYPE", emsg, logger)
    del variable['VarAttributes']['DISPLAY_TYPE']

  display_types_known = cdawmeta.CONFIG['master_resolved']['DISPLAY_TYPES']
  #import pdb; pdb.set_trace()

  if display_type not in display_types_known:
    emsg = f"{indent}{variable_name} DISPLAY_TYPE = '{display_type}' is not in "
    emsg += f"{display_types_known}. Will attempt to infer."
    cdawmeta.error('master_resolved', id, variable_name, "ISTP.UnknownDISPLAY_TYPE", emsg, logger)

  found = False
  for display_type in display_types_known:
    if display_type.strip().lower().startswith(display_type):
      found = True
      display_type = display_type.strip().lower()
      break

  if not found:
    emsg += "{indent}{variable_name}'DISPLAY_TYPE.strip().lower() = "
    emsg += f"'{display_type}' does not start with one of {display_types_known}"
    cdawmeta.error('master_resolved', id, variable_name, "ISTP.UnknownDISPLAY_TYPE", emsg, logger)
    return None

  return display_type + display_type_attributes

def _DEPEND(id, variable_name, variables, variables_removed, logger):
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

def _LABLAXIS(id, variable, logger):

  # TODO: There are rules for when LABLAXIS and LABL_PTR_1, LABL_PTR_2,
  # LABL_PTR_3 are required depending on DISPLAY_TYPE

  LABLAXIS = variable['VarAttributes'].get('LABLAXIS', None)
  if LABLAXIS is None:
    return None

  logger.info(f"    LABLAXIS given: {LABLAXIS}")
  if cdawmeta.CONFIG['hapi']['strip_labelaxis']:
    if isinstance(LABLAXIS, str):
      LABLAXIS = cdawmeta.util.trim(LABLAXIS)
    else:
      emsg = f"{indent}LABLAXIS = {LABLAXIS} is not a string. Casting to string."
      cdawmeta.error('master_resolved', id, None, "CDF.LABLAXISNotString", emsg, logger)
      LABLAXIS = cdawmeta.util.trim(str(LABLAXIS))

def _LABL_PTR(id, variable_name, variables, variables_removed, logger):

  labl_ptrs = []
  found = False
  for i in [1, 2, 3]:
    labl_ptr_resolved = _resolve_ptr(id, variable_name, variables, variables_removed, logger, ptr_name=f'LABL_PTR_{i}')
    if labl_ptr_resolved is not None:
      found = True
      labl_ptrs.append(labl_ptr_resolved['values_trimmed'])

  if not found:
    labl_ptrs = None

  return labl_ptrs

def _UNITS(id, variable_name, variables, variables_removed, logger):

  def _summary(original, resolved, attribute_name, logger):
    indent = "    "

    msg = f"{indent}{attribute_name} given: "
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

    msg = f"{indent}x_{attribute_name}: "
    if isinstance(resolved, list):
      logger.info(f"{msg}{resolved}")
    else:
      if resolved is None:
        logger.info(f"{msg}{resolved}")
      else:
        logger.info(f"{msg}'{resolved}'")

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
      units = units_o

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
      msg = f"{indent}Did not find mapping from CDAWeb unit = '{unit}' to VO_UNIT in additions['Units']"
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