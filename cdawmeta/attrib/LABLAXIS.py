import cdawmeta

def LABLAXIS(dsid, name, all_variables, x=None):
  """Get LABLAXIS or resolved LABEL_PTR for a variable."""

  variable = all_variables[name]
  ptrs = cdawmeta.attrib._resolve_ptrs(dsid, name, all_variables, ptr_names=['LABL_PTR'])

  msg = ""
  lablaxis = None
  if 'LABLAXIS' in variable['VarAttributes']:
    msgx = ""
    if x is not None:
      msgx = f"DEPEND_{x} "
      msg = f"     {msgx}LABLAXIS = {variable['VarAttributes']['LABLAXIS']}"

    if ptrs['LABL_PTR'] is not None:
      msg = f"Error: ISTP[LABELAXIS]: For {msgx}variable '{name}', LABL_PTR = {ptrs['LABL_PTR']} and LABLAXIS = "
      msg += f"{variable['VarAttributes']['LABLAXIS']}. Using LABLAXIS."
      lablaxis = cdawmeta.util.trim(variable['VarAttributes']['LABLAXIS'])

  if ptrs['LABL_PTR'] is not None:
    label = ptrs['LABL_PTR_VALUES']
    if len(label) == 1:
      label = label[0]

  return lablaxis, msg
