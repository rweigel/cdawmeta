import cdawmeta

def LABLAXIS(dsid, name, all_variables, x=None):
  """Get LABLAXIS or resolved LABEL_PTR for a variable."""

  variable = all_variables[name]
  ptrs = cdawmeta.attrib._resolve_ptrs(dsid, name, all_variables, ptr_names=['LABL_PTR'])

  msg = ""
  etype = None
  lablaxis = None
  if 'LABLAXIS' in variable['VarAttributes']:
    lablaxis = cdawmeta.util.trim(variable['VarAttributes']['LABLAXIS'])

    if ptrs['LABL_PTR'] is not None:
      msgx = ""
      if x is not None:
        msgx = f"DEPEND_{x} "
      etype = "ISTP.LABLAXIS"
      msg = f"For {msgx}variable '{name}', LABL_PTR = {ptrs['LABL_PTR']} and LABLAXIS = "
      msg += f"'{lablaxis}'. Using LABLAXIS."

  if ptrs['LABL_PTR'] is not None:
    label = ptrs['LABL_PTR_VALUES']
    if len(label) == 1:
      label = label[0]

  return lablaxis, msg, etype
