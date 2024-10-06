import copy

import cdawmeta

def master(master, master_url, logger=None):

  """
  Restructure JSON representation^* of Master CDF to match structure returned
  by cdawmeta.io.read_cdf_meta().

  ^* https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/

  lists of the form [{"0", value0}, {"1", value1}, ...]
  converted to lists of the form [value0, value1, ...].

  dict with arrays of objects to objects with objects. For example,
    { "Epoch": [ 
        {"VarDescription": [{"DataType": "CDF_TIME_TT2000"}, ...] },
        {"VarAttributes": [{"CATDESC": "Default time"}, ...] }
      ],
      ...
    }
  is converted to
    {
      "Epoch": {
        "VarDescription": {
          "DataType": "CDF_TIME_TT2000",
          ...
        },
        "VarAttributes": {
          "CATDESC": "Default time",
          ...
        }
      }
    }
  """

  def sort_keys(obj):
    return {key: obj[key] for key in sorted(obj)}

  master = copy.deepcopy(master)

  # TODO: Check for only one key.
  file = list(master.keys())[0]

  fileinfo_r = cdawmeta.util.array_to_dict(master[file]['CDFFileInfo'])

  variables = master[file]['CDFVariables']
  variables_r = {}

  for variable in variables:

    variable_keys = list(variable.keys())
    if len(variable_keys) > 1:
      msg = "Expected only one variable key in variable object. Exiting witih code 1."
      logger.error(msg)
      exit(1)

    variable_name = variable_keys[0]
    variable_array = variable[variable_name]
    variable_dict = cdawmeta.util.array_to_dict(variable_array)

    for key, value in variable_dict.items():

      if key == 'VarData':
        variable_dict[key] = value
      else:
        variable_dict[key] = sort_keys(cdawmeta.util.array_to_dict(value))

    variables_r[variable_name] = variable_dict

  # Why do they use lower-case G? Inconsistent with CDFVariables.
  globals = master[file]['CDFglobalAttributes']
  globals_r = {}

  for _global in globals:
    gkey = list(_global.keys())
    if len(gkey) > 1:
      if logger is not None:
        msg = "Expected only one key in _global object."
        logger.error(msg)
    gvals = _global[gkey[0]]
    text = []
    for gval in gvals:
      line = gval[list(gval.keys())[0]]
      text.append(str(line))

    globals_r[gkey[0]] = "\n".join(text)

  master = {
              'CDFFileInfo': {'File': file, 'FileURL': master_url, **fileinfo_r},
              'CDFglobalAttributes': globals_r,
              'CDFVariables': variables_r
            }

  return master
