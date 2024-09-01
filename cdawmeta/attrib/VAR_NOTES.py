def VAR_NOTES(dsid, name, variable):

  msg = None
  var_notes = None
  if 'VAR_NOTES' in variable['VarAttributes']:
    var_notes = variable['VarAttributes']['VAR_NOTES']
    if isinstance(var_notes, list):
      var_notes = '\n'.join(var_notes)
  else:
    msg = f"     Error: ISTP[VAR_NOTES]: No VAR_NOTES attribute for variable '{name}'"

  return var_notes, msg