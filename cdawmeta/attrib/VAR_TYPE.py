def VAR_TYPE(dsid, name, variable, x=None):

  if 'VAR_TYPE' in variable['VarAttributes']:
    return variable['VarAttributes']['VAR_TYPE'], None
  else:
    msgx = ""
    if x is not None:
      msgx = f"DEPEND_{x} "
    msg = f"Error: ISTP[VAR_TYPE]: {msgx} for '{name}' has no VAR_TYPE."
    return None, msg
