def VAR_TYPE(dsid, name, variable, x=None):

  if 'VAR_TYPE' in variable['VarAttributes']:
    return variable['VarAttributes']['VAR_TYPE'], None, None
  else:
    msgx = ""
    if x is not None:
      msgx = f"DEPEND_{x} for "
    msg = f"{msgx}variable '{name}' has no VAR_TYPE."
    return None, msg, "ISTP.VAR_TYPE"
