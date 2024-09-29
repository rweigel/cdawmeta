def CATDESC(dsid, name, variable):

  msg = None
  etype = None
  catdesc = None
  if 'CATDESC' in variable['VarAttributes']:
    catdesc = variable['VarAttributes']['CATDESC']
    if isinstance(catdesc, list):
      catdesc = '\n'.join(catdesc)
  else:
    etype = "ISTP.CATDESC"
    msg = f"Variable '{name}' does not have a CATDESC attribute."

  return catdesc, msg, etype
