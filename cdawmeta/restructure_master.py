def sort_keys(obj):
  return {key: obj[key] for key in sorted(obj)}

def array_to_dict(array):
  obj = {}
  for element in array:
    key = list(element.keys())[0]
    obj[key] = element[key]
  return obj

def add_master_restructured(root_dir, datasets, logger, set_error):
  for idx, dataset in enumerate(datasets):

    if '_master' not in dataset:
      msg = f'Error: No _master in {dataset["id"]}. Omitting dataset.'
      logger.error(msg)
      set_error(dataset["id"], None, msg)
      datasets[idx] = None
      continue

    _master_restructured = restructure_master(root_dir, dataset, logger, set_error)
    if _master_restructured is None:
      msg = f'Error: Could not restructure variables for {dataset["id"]}. Omitting dataset.'
      logger.error(msg)
      set_error(dataset["id"], None, msg)
      datasets[idx] = None
      continue

    datasets[idx]["_master_restructured"] = _master_restructured

  return [i for i in datasets if i is not None]

def restructure_master(root_dir, dataset, logger, set_error):

  """
  Convert dict with arrays of objects to objects with objects. For example
    { "Epoch": [ 
        {"VarDescription": [{"DataType": "CDF_TIME_TT2000"}, ...] },
        {"VarAttributes": [{"CATDESC": "Default time"}, ...] }
      ]
    }
  is converted and written to _variables as
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
  import os
  import json

  try:
    master_file = os.path.join(root_dir, dataset['_master'])
    with open(master_file, 'r', encoding='utf-8') as f:
      _master_data = json.load(f)["_decoded_content"]
  except:
    msg = f"Error: Could not open {dataset['id']} master file."
    logger.error(msg)
    set_error(dataset["id"], None, msg)
    return None

  _master_restructured = {
    'globals': restructure_globals(dataset["id"], _master_data, logger, set_error)
  }

  file = list(_master_data.keys())[0]

  variables = _master_data[file]['CDFVariables']
  variables_new = {}

  for variable in variables:

    variable_keys = list(variable.keys())
    if len(variable_keys) > 1:
      msg = "Expected only one variable key in variable object. Exiting witih code 1."
      logger.error(msg)
      set_error(dataset["id"], None, msg)
      exit(1)

    variable_name = variable_keys[0]
    variable_array = variable[variable_name]
    variable_dict = array_to_dict(variable_array)

    for key, value in variable_dict.items():

      if key == 'VarData':
        variable_dict[key] = value
      else:
        variable_dict[key] = sort_keys(array_to_dict(value))

    variables_new[variable_name] = variable_dict

  _master_restructured['_variables'] = variables_new

  return _master_restructured

def restructure_globals(dsid, _master_data, logger, set_error):

  file = list(_master_data.keys())[0]

  globals = _master_data[file]['CDFglobalAttributes']
  globals_r = {}

  for _global in globals:
    gkey = list(_global.keys())
    if len(gkey) > 1:
      msg = "Expected only one key in _global object."
      logger.error(msg)
      set_error(dsid, None, msg)
    gvals = _global[gkey[0]]
    text = []
    for gval in gvals:
      line = gval[list(gval.keys())[0]];
      text.append(str(line))

    globals_r[gkey[0]] = "\n".join(text)

  return globals_r
