def DELTA(dsid, name, variable):

  not_implemented = [
    'DELTA_PLUS', 'DELTA_MINUS',
    'DELTA_PLUS_VAR', 'DELTA_MINUS_VAR'
    'DELTA_PLUS_VARx', 'DELTA_MINUS_VARx'
  ]

  deltas = {}
  for attrib in not_implemented:
    if attrib in variable['VarAttributes']:
      attrib_val = variable['VarAttributes'][attrib]
      deltas[attrib] = attrib_val
      msg =f"Error: NotImplemented[DELTA]: {attrib} = '{attrib_val}' not used"
  return deltas, msg
