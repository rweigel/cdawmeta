import cdawmeta
from cdawmeta._generate.hapi import CDFDataType2HAPItype

def UNITS(dsid, name, all_variables, x=None):

  variable = all_variables[name]

  msgx = ""
  if x is not None:
    msgx = f"DEPEND_{x} "

  msg = None
  units = None
  if "UNITS" in variable['VarAttributes']:
    units = variable['VarAttributes']["UNITS"]
    if not units.isprintable():
      msg = f"Error: ISTP[UNITS]: {msgx}variable '{name}' has UNITS with non-printable characters. Setting UNITS=''"
      units = ''
  else:
    if "UNIT_PTR" in variable['VarAttributes']:
      ptr_name = variable['VarAttributes']['UNIT_PTR']
      if ptr_name in all_variables:
          if 'string' == CDFDataType2HAPItype(all_variables[ptr_name]['VarDescription']['DataType']):
            if 'VarData' in all_variables[ptr_name]:
              units = all_variables[ptr_name]['VarData']
            else:
              msg = f"Error: ISTP[UNIT_PTR]: {msgx}variable '{name}' has UNIT_PTR = '{ptr_name}', "
              msg += "but it has no VarData."
          else:
            msg = f"Error: ISTP[UNIT_PTR]: {msgx}variable '{name}' has UNIT_PTR = '{ptr_name}', "
            msg += "but it is not a string type."
      else:
        msg = f"Error: ISTP[UNIT_PTR]: {msgx}variable '{name}'"
        msg += f" has UNIT_PTR = '{variable['VarAttributes']['UNIT_PTR']}', which is not a variable in dataset."

    VAR_TYPE, _ = cdawmeta.attrib.VAR_TYPE(dsid, name, variable, x=x)
    if VAR_TYPE is not None and VAR_TYPE in ['data', 'support_data']:
      if "UNIT_PTR" not in variable['VarAttributes']:
        msg = f"Error: ISTP[UNITS]: {msgx}variable '{name}' has VAR_TYPE "
        msg += f"'{VAR_TYPE}' and no UNITS or UNIT_PTR."

  return units, msg
