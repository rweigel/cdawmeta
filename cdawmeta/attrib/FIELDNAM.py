def FIELDNAM(dsid, name, variable):

  msg = None
  fieldnam = None
  if 'FIELDNAM' in variable['VarAttributes']:
    fieldnam = variable['VarAttributes']['FIELDNAM']
    if isinstance(fieldnam, list):
      fieldnam = '\n'.join(fieldnam)
  else:
    msg = f"     Warning: ISTP[VAR_NOTES]: No VAR_NOTES attribute for variable '{name}'"

  return fieldnam, msg
