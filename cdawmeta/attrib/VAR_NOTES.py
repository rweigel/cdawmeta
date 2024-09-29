def VAR_NOTES(dsid, name, variable):

  var_notes = None
  msg = None
  etype = None
  if 'VAR_NOTES' in variable['VarAttributes']:
    var_notes = variable['VarAttributes']['VAR_NOTES']
    if isinstance(var_notes, list):
      var_notes = '\n'.join(var_notes)
  else:
    etype = "ISTP.VAR_NOTES"
    msg = f"No VAR_NOTES attribute for variable '{name}'"

  return var_notes, msg, etype