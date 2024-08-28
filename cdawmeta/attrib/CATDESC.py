
def CATDESC(dsid, name, variable):

  msg = None
  catdesc = None
  if 'CATDESC' in variable['VarAttributes']:
    catdesc = variable['VarAttributes']['CATDESC']
    if isinstance(catdesc, list):
      catdesc = '\n'.join(catdesc)
  else:
    msg = f"     Error: ISTP[DISPLAY_TYPE]: Variable '{name}' does not have a CATDESC attribute."

  return catdesc, msg
