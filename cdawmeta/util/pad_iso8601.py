import re
from datetime import datetime

def pad_iso8601(t):
  """Pad a restricted ISO 8601 time string t picoseconds."""

  allowed = ['yyyy-mm-ddThh:mm:ss.mmmuuunnnppp[Z]',
              'yyyy-mm-ddThh:mm:ss.mmmuuunnn[Z]',
              'yyyy-mm-ddThh:mm:ss.mmmuuu[Z]',
              'yyyy-mm-ddThh:mm:ss.mmm[Z]',
              'yyyy-mm-ddThh:mm:ss.[Z]',
              'yyyy-mm-ddThh:mm:ss[Z]',
              'yyyy-mm-ddThh:mm[Z]',
              'yyyy-mm-ddThh[Z]',
              'yyyy-mm-dd[Z]',
              'yyyy-mm[Z]',
              'yyyy[Z]'
            ]

  Z = ''
  if t.endswith('Z'):
    Z = 'Z'
    t = t[0:-1]

  if len(t) == 32: # yyyy-mm-ddThh:mm:ss.mmmuuunnnppp
    fmt = '%Y-%m-%dT%H:%M:%S.%f'
    n = 26
    if not re.match(r"[0-9]{6}", t[n:]):
      raise ValueError(f"For this length of string, last 6 characters of time string '{t}' must be digits.")
  elif len(t) == 29: # yyyy-mm-ddThh:mm:ss.mmmuuunnn
    t = t + '000'
    fmt = '%Y-%m-%dT%H:%M:%S.%f'
    n = 23
    if not re.match(r"[0-9]{6}", t[n:]):
      raise ValueError(f"For this length of string, last 6 characters of time string '{t}' must be digits.")
  elif len(t) == 26: # yyyy-mm-ddThh:mm:ss.mmmuuu
    t = t + '000000'
    n = 23
    fmt = '%Y-%m-%dT%H:%M:%S.%f'
  elif len(t) == 23: # yyyy-mm-ddThh:mm:ss.mmm
    t = t + '000000000'
    n = 23
    fmt = '%Y-%m-%dT%H:%M:%S.%f'
  elif len(t) == 20: # yyyy-mm-ddThh:mm:ss.
    t = t + '000000000000'
    n = 21
    fmt = '%Y-%m-%dT%H:%M:%S.%f'
  elif len(t) == 19: # yyyy-mm-ddThh:mm:ss
    t = t + '.000000000000'
    n = 19
    fmt = '%Y-%m-%dT%H:%M:%S'
  elif len(t) == 16: # yyyy-mm-ddThh:mm
    t = t + ':00.000000000000'
    n = 17
    fmt = '%Y-%m-%dT%H:%M'
  elif len(t) == 13: # yyyy-mm-ddThh
    t = t + ':00:00.000000000000'
    n = 14
    fmt = '%Y-%m-%dT%H'
  elif len(t) == 10: # yyyy-mm-dd
    t = t + 'T00:00:00.000000000000'
    n = 10
    fmt = '%Y-%m-%d'
  elif len(t) == 7: # yyyy-mm
    t = t + '-01T00:00:00.000000000000'
    n = 7
    fmt = '%Y-%m'
  elif len(t) == 4: # yyyy
    t = t + '-01-01T00:00:00.000000000000'
    n = 4
    fmt = '%Y'
  else:
    raise ValueError(f"Time string length does not match length of one of {allowed}")

  try:
    datetime.strptime(t[0:n], fmt)
  except:
    raise ValueError(f"Could not parse given time string '{t[0:n]}' with inferred format '{fmt}'.")

  return t + Z
