import cdawmeta

def LABLAXIS(dsid, name, variable, ptrs, x=None):
  """Get LABLAXIS or resolved LABEL_PTR for a variable."""

  msg = ""
  lablaxis = None
  if 'LABLAXIS' in variable['VarAttributes']:
    msgx = ""
    if x is not None:
      msgx = f"DEPEND_{x} "
      msg = f"     {msgx}LABLAXIS = {variable['VarAttributes']['LABLAXIS']}"

    if ptrs['LABL_PTR'] is None:
      msg = f"Error: ISTP[LABELAXIS]: For {msgx}variable '{name}' does not have LABL_PTR or LABLAXIS."
    else:
      msg = f"Error: ISTP[LABELAXIS]: For {msgx}variable '{name}', LABL_PTR = {ptrs['LABL_PTR']} and LABLAXIS = "
      msg += f"{variable['VarAttributes']['LABLAXIS']}. Using LABLAXIS."
      lablaxis = cdawmeta.util.trim(variable['VarAttributes']['LABLAXIS'])

  if ptrs['LABL_PTR'] is not None:
    label = ptrs['LABL_PTR_VALUES']
    if len(label) == 1:
      label =  label[0]

  return lablaxis, msg
