def SI_CONVERSION(variable):
  emsg = None
  etype = None
  si_conversion = None

  if 'SI_CONVERSION' in variable['VarAttributes']:
    si_conversion = variable['VarAttributes']['SI_CONVERSION'].strip()

  # https://github.com/rweigel/cdawmeta/issues/14
  if 'SI_conversion' in variable['VarAttributes']:
    si_conversion = variable['VarAttributes']['SI_conversion'].strip()
    etype = "ISTP.SI_CONVERSION"
    emsg = "SI_conversion attribute should be named SI_CONVERSION"
  if 'SI_conv' in variable['VarAttributes']:
    etype = "ISTP.SI_CONVERSION"
    emsg = "SI_conv attribute should be named SI_CONVERSION"
    si_conversion = variable['VarAttributes']['SI_conv'].strip()

  return si_conversion, etype, emsg
