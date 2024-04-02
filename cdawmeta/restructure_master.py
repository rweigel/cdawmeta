def add_master_restructured(datasets):
  for idx, dataset in enumerate(datasets):

    if '_master' not in dataset:
      print(f'Error: No _master in {dataset["id"]}. Omitting dataset.')
      datasets[idx] = None
      continue

    _master_restructured = restructure_master(dataset)
    if _master_restructured is None:
      print(f'Error: Could not restructure variables for {dataset["id"]}. Omitting dataset.')
      datasets[idx] = None
      continue

    datasets[idx]["_master_restructured"] = _master_restructured

  return [i for i in datasets if i is not None]


def restructure_master(dataset):

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
  import json

  try:
    with open(dataset['_master'], 'r', encoding='utf-8') as f:
      _master_data = json.load(f)["_decoded_content"]
  except:
    print("Error: Could not open " + dataset["id"] + " master file.")
    return None

  _master_restructured = {'globals': restructure_globals(_master_data)}

  file = list(_master_data.keys())[0]

  variables = _master_data[file]['CDFVariables']
  variables_new = {}

  for variable in variables:

    variable_keys = list(variable.keys())
    if len(variable_keys) > 1:
      print(dataset["id"] + ": Expected only one variable key in variable object.")
      exit(0)

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


def sort_keys(obj):
  return {key: obj[key] for key in sorted(obj)}


def array_to_dict(array):
  obj = {}
  for element in array:
    key = list(element.keys())[0]
    obj[key] = element[key]
  return obj


def restructure_globals(_master_data):

  file = list(_master_data.keys())[0]

  globals = _master_data[file]['CDFglobalAttributes']
  globals_r = {}

  for _global in globals:
    gkey = list(_global.keys())
    if len(gkey) > 1:
      print("Expected only one key in _global object.")
      exit()
    gvals = _global[gkey[0]]
    text = []
    for gval in gvals:
      line = gval[list(gval.keys())[0]];
      text.append(str(line))

    globals_r[gkey[0]] = "\n".join(text)

  return globals_r
