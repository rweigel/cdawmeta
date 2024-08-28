import re
import cdawmeta

def FORMAT(dsid, name, all_variables, c_specifier=True):

  variable = all_variables[name]

  if c_specifier:
    if 'DataType' in variable['VarDescription']:
      DataType = variable['VarDescription']['DataType']
    else:
      return None

  msg = None
  format = None
  if 'FORMAT' in variable['VarAttributes']:
    format = variable['VarAttributes']['FORMAT']
    if c_specifier:
      format = _f2c_specifier(format, DataType)
  else:
    if 'FORM_PTR' not in variable['VarAttributes']:
      msg = f"     Error: ISTP[FORMAT]: Variable '{name}' does not have a FORMAT or FORM_PTR attribute."

  if 'FORMAT' in variable['VarAttributes'] and 'FORM_PTR' in variable['VarAttributes']:
    msg = f"     Error: ISTP[FORMAT]: Variable '{name}' has both FORMAT and FORM_PTR attributes. Using FORMAT."
    if format is not None:
      return format, msg

  if 'FORM_PTR' in variable['VarAttributes']:
    form_ptr = variable['VarAttributes']['FORM_PTR']
    if not form_ptr in all_variables:
      msg = f"     Error: ISTP[FORM_PTR]: Variable '{name}' has FORM_PTR = '{form_ptr}' but no such variable exists."
    else:
      variable_ptr = all_variables[form_ptr]
      if 'VarData' in variable_ptr:

        var_DimSizes = cdawmeta.util.get_path(variable, ['VarDescription', 'DimSizes'])
        if var_DimSizes is None:
          msg = f"     Error: ISTP[FORM_PTR]: Variable '{name}' does not have DimSizes."
          return None, msg

        ptr_DimSizes = cdawmeta.util.get_path(variable_ptr, ['VarDescription', 'DimSizes'])
        if ptr_DimSizes is None:
          msg = f"     Error: ISTP[FORM_PTR]: Variable '{name}' pointed to by FORM_PTR = '{form_ptr}' does not have DimSizes."
          return None, msg

        if var_DimSizes != ptr_DimSizes:
          msg = f"     Error: ISTP[FORM_PTR]: Variable '{name}' has FORM_PTR = '{form_ptr}' with DimSizes = {ptr_DimSizes} that does not match the variable's DimSizes = {var_DimSizes}."
          return None, msg

        format = variable_ptr['VarData']
        if c_specifier:
          format = _f2c_specifier(format, DataType)
        if format is not None and format[0][0:2] == '10':
          format = None
          # https://github.com/rweigel/cdawmeta/issues/11
          msg = f"     Error: ISTP[FORM_PTR]: Variable '{name}' has FORM_PTR = '{form_ptr}' with VarData = {format} that does not appear to be valid."

  if isinstance(format, list) and len(set(format)) == 1:
    # If all elements are the same, return a single string instead of list.
    format = format[0]
  return format, msg

def _f2c_specifier(f_template, DataType):
  """Extract precision part of a Fortran format string found as a FORMAT value.

  See also
  * https://github.com/rweigel/CDAWlib/blob/952a28b08658413081e75714bd3b9bd3ba9167b9/cdfjson__define.pro#L132
  * https://git.smce.nasa.gov/spdf/hapi-nand/-/blob/main/src/java/org/hapistream/hapi/server/cdaweb/CdawebUtil.java?ref_type=heads#L23
  * https://github.com/rweigel/cdawmeta/issues/10
  """

  def default(DataType):
    default_float = cdawmeta.CONFIG['hapi']['default_float_format']
    default_double = cdawmeta.CONFIG['hapi']['default_double_format']
    defaults = {
      'CDF_FLOAT': default_float,
      'CDF_DOUBLE': default_double,
      'CDF_REAL4': default_float,
      'CDF_REAL8': default_double
    }
    if DataType in defaults:
      return defaults[DataType]
    return None

  if isinstance(f_template, list):
    fmts = []
    for f in f_template:
      fmt = _f2c_specifier(f, DataType)
      if fmt is None:
        # If any not valid, don't use any
        return None
      else:
        fmts.append(fmt)
    return fmts

  f_template = f_template.lower().strip()
  if f_template == "":
    return default(DataType)

  # No precision.
  if "." not in f_template:
    return default(DataType)

  # Drop any string or integer related format string.
  fmt = re.sub(r".*[a|s|c|z|b|i].*", "", f_template)
  if fmt == "":
    return default(DataType)

  # e.g., E11.4 => .4e, F8.1 => .1f
  fmt = re.sub(r".*([f|e|g])([0-9].*)\.([0-9].*)", r".\3\1", f_template)

  # Test the format string
  try:
    templ = "{:" + fmt + "}"
    templ.format(1.0)
  except:
    return default(DataType)

  return fmt

if __name__ == '__main__':
  print(_f2c_specifier('E11.4', 'CDF_FLOAT'))
  print(_f2c_specifier(['E11.4','E11.4'], 'CDF_FLOAT'))
  print(_f2c_specifier([['E11.4','E11.4'], ['E11.4','E11.4']], 'CDF_FLOAT'))