def DISPLAY_TYPE(dsid, name, variable):

  if 'DISPLAY_TYPE' not in variable['VarAttributes']:
    if variable['VarAttributes'].get('VAR_TYPE') == 'data':
      msg = f"No DISPLAY_TYPE for variable '{name}' with VAR_TYPE = data'"
      return None, msg, "ISTP.DISPLAY_TYPE"

  display_type = variable['VarAttributes']['DISPLAY_TYPE']
  display_type = display_type.split(">")[0]

  if display_type.strip() == '':
    msg = "DISPLAY_TYPE.strip() = ''"
    return None, msg, "ISTP.DISPLAY_TYPE"

  display_types_known = [
    'time_series',
    'spectrogram',
    'stack_plot',
    'image',
    'no_plot',
    'orbit',
    'plasmagram',
    'skymap'
  ]

  msg = None
  etype = None

  if display_type not in display_types_known:
    etype = "ISTP.DISPLAY_TYPE"
    msg = f"DISPLAY_TYPE = '{display_type}' is not in "
    msg += f"{display_types_known}. Will attempt to infer."

  found = False
  for display_type in display_types_known:
    if display_type.lower().startswith(display_type):
      found = True
      break

  if not found:
    display_type = None
    etype = "ISTP.DISPLAY_TYPE"
    msg += "DISPLAY_TYPE.lower() = "
    msg += "'{DISPLAY_TYPE}' does not start with one of {display_types_known}"

  return display_type, msg, etype
