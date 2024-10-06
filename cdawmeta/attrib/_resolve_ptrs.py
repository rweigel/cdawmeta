import cdawmeta

def _resolve_ptrs(dsid, name, all_variables, ptr_names=None):

  cdf_string_types = ['CDF_CHAR', 'CDF_UCHAR']
  variable = all_variables[name]
  DimSizes = variable['VarDescription'].get('DimSizes', [])

  if isinstance(ptr_names, str):
    ptr_names = [ptr_names]
  if ptr_names is None:
    ptr_names = ['DEPEND', 'LABL_PTR', 'COMPONENT', 'UNIT_PTR']

  ptrs = {}

  if 'UNIT_PTR' in ptr_names and 'UNIT_PTR' in variable['VarAttributes']:
    ptr_name = variable['VarAttributes']['UNIT_PTR']
    ptrs['UNIT_PTR'] = ptr_name
    ptrs['UNIT_PTR_VALID'] = False
    ptrs['UNIT_PTR_VALUES'] = None
    ptrs['UNIT_PTR_ERROR'] = None
    msgo = f"Variable '{name}' has UNIT_PTR = '{ptr_name}' "
    if ptr_name in all_variables:
      DataType = all_variables[ptr_name]['VarDescription']['DataType']
      if DataType in cdf_string_types:
        if 'VarData' in all_variables[ptr_name]:
          ptrs['UNIT_PTR_VALID'] = True
          values = cdawmeta.util.trim(all_variables[ptr_name]['VarData'])
          ptrs['UNIT_PTR_VALUES'] = values
        else:
          msg = f"{msgo} with no VarData."
      else:
        msg = f"{msgo} that is not a string type variable."
    else:
      msg = f"{msgo} which is not a variable in dataset."

    if not ptrs['UNIT_PTR_VALID']:
      ptrs['UNIT_PTR_ERROR'] = msg

    del ptr_names[ptr_names.index('UNIT_PTR')]

  for prefix in ptr_names:
    ptrs[prefix] = [None, None, None]
    ptrs[prefix+"_VALID"] = [None, None, None]
    ptrs[prefix+"_VALUES"] = [None, None, None]
    ptrs[prefix+"_ERROR"] = [None, None, None]
    for x in [1, 2, 3]:
      if f'{prefix}_{x}' in variable['VarAttributes']:
        x_NAME = variable['VarAttributes'][f'{prefix}_{x}']
        DataType = all_variables[x_NAME]['VarDescription']['DataType']
        if x_NAME not in all_variables:
          ptrs[prefix+"_VALID"][x-1] = False
          msg = f"Error: CDF[BadReference]: Bad {prefix} reference: '{name}' has {prefix}_{x} "
          msg += f"named '{x_NAME}', which is not a variable in dataset."
          ptrs[prefix+"_ERROR"][x-1] = msg
        elif prefix == 'LABL_PTR' or (prefix == 'DEPEND' and DataType in cdf_string_types):
          if 'VarData' in all_variables[x_NAME]:
            ptrs[prefix+"_VALID"][x-1] = True
            ptrs[prefix][x-1] = x_NAME
            values = cdawmeta.util.trim(all_variables[x_NAME]['VarData'])
            ptrs[prefix+"_VALUES"][x-1] = values
          else:
            ptrs[prefix+"_VALID"][x-1] = False
            if prefix == 'LABL_PTR':
              msg = f"ISTP.LABL_PTR: {x_NAME} has no VarData"
            else:
              msg = f"ISTP.LABL_PTR: {x_NAME} is a string type but has no VarData"
            ptrs[prefix+"_ERROR"][x-1] = msg
        else:
          ptrs[prefix+"_VALID"][x-1] = True
          ptrs[prefix][x-1] = x_NAME

    n_valid = len([x for x in ptrs[prefix+"_VALID"] if x is True])
    n_invalid = len([x for x in ptrs[prefix+"_VALID"] if x is False])
    n_found = len([x for x in ptrs[prefix+"_VALID"] if x is not None])
    if n_invalid > 0:
      ptrs[prefix] = None
    elif prefix != 'COMPONENT':
      if n_valid != len(DimSizes):
        ptrs[prefix] = None
        if False:
          # Not necessarily an error for DEPEND. Depends on DISPLAY_TYPE, which is often missing.
          # https://spdf.gsfc.nasa.gov/istp_guide/vattributes.html
          msg = f"Error: ISTP: '{name}' has {n_valid} valid elements {prefix}_{{1,2,3}}, but need "
          msg += f"len(DimSizes) = {len(DimSizes)}."
      if n_found != 0 and n_found != len(DimSizes):
        ptrs[prefix] = None
        if False:
          # Not necessarily an error for DEPEND. Depends on DISPLAY_TYPE.
          # https://spdf.gsfc.nasa.gov/istp_guide/vattributes.html
          msg = f"Error: ISTP: Wrong number of {prefix}s: '{name}' has {n_found} of "
          msg += f"{prefix}_{{1,2,3}} and len(DimSizes) = {len(DimSizes)}."

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
