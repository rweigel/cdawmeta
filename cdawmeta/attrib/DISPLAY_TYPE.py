import cdawmeta

def DISPLAY_TYPE(dsid, name, variable):

  if 'DISPLAY_TYPE' not in variable['VarAttributes']:
    if variable['VarAttributes'].get('VAR_TYPE') == 'data':
      msg = f"     Error: ISTP[DISPLAY_TYPE]: No attribute for variable '{name}' with VAR_TYPE = data'"
      return None, msg

  msg = ""
  display_type = variable['VarAttributes']['DISPLAY_TYPE']
  display_type = display_type.split(">")[0]

  if display_type.strip() == '':
    msg = "     Error: ISTP[DISPLAY_TYPE]: DISPLAY_TYPE.strip() = ''"
    return None, msg

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

  if display_type not in display_types_known:
    msg = f"     Error: ISTP[DISPLAY_TYPE]: DISPLAY_TYPE = '{DISPLAY_TYPE}' is not in "
    msg += f"{display_types_known}. Will attempt to infer."

  found = False
  for display_type in display_types_known:
    if display_type.lower().startswith(display_type):
      found = True
      break
  if not found:
    display_type = None
    msg += "     Error: ISTP[DISPLAY_TYPE]: DISPLAY_TYPE.lower() = "
    msg += "'{DISPLAY_TYPE}' does not start with one of {display_types_known}"

  return display_type, msg
