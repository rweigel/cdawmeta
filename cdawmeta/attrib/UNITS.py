import cdawmeta

def UNITS(dsid, name, all_variables, x=None):

  units = None
  variable = all_variables[name]
  ptrs = cdawmeta.attrib._resolve_ptrs(dsid, name, all_variables, ptr_names=['UNIT_PTR'])

  msg = ""
  msgx = ""
  if x is not None:
    msgx = f"DEPEND_{x} "
  msgo = f"Error: ISTP[UNITS]: {msgx}variable '{name}' has "

  VAR_TYPE, _ = cdawmeta.attrib.VAR_TYPE(dsid, name, variable, x=x)

  if 'UNITS' in variable['VarAttributes']:
    units = variable['VarAttributes']["UNITS"]

    if not units.isprintable():
      msg = f"{msgo} UNITS with non-printable characters. Setting UNITS=''"
      units = ""
      return units, msg

    if units.strip() == "":
      msg = f"{msgo} UNITS.strip() = ''"
      if VAR_TYPE is not None and VAR_TYPE in ['data', 'support_data']:
        # Catch case where empty string or whitespace string used for UNITS,
        # presumably to "satisfy" ISTP requirements that UNITS be present.
        msg = f"{msgo} VAR_TYPE = '{VAR_TYPE}' and UNITS.strip() = ''"
        return units, msg

    if ptrs['UNIT_PTR'] is not None:
      msg = f"Error: ISTP[UNIT_PTR]: For {msgx}variable '{name}', UNIT_PTR = {ptrs['UNIT_PTR']} and UNITS = "
      msg += f"{variable['VarAttributes']['UNITS']}. Using UNITS."

    return units, None

  if ptrs['UNIT_PTR'] is None:
    if VAR_TYPE is not None and VAR_TYPE in ['data', 'support_data']:
      if "UNIT_PTR" not in variable['VarAttributes']:
        msg = f"{msgo} VAR_TYPE = '{VAR_TYPE}' and no UNITS or UNIT_PTR."
        return units, msg
  else:
    units = ptrs['UNIT_PTR_VALUES']
    if len(units) == 1:
      units = units[0]

  return units, None